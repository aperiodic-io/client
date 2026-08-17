"""Tests for ordering of downloaded parquet files.

Data up to 2026-07-31 is served as one parquet per month; from 2026-08-01 the
API returns one parquet per day, so a single response can contain many files
sharing the same ``year``/``month``. These tests pin the client's ordering
guarantee: files are concatenated chronologically regardless of the order their
downloads complete in.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from io import BytesIO
from unittest import mock

import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa
import pyarrow.parquet as pq

from aperiodic._compat import HAS_POLARS
from aperiodic.client import AperiodicDataError
from aperiodic.endpoints.utils import _get_files_from_bucket_async

OUTPUT = "polars" if HAS_POLARS else "pandas"

FETCH_PARAMS = {
    "api_key": "test-key",
    "bucket": "ohlcv",
    "timestamp": "exchange",
    "interval": "1d",
    "exchange": "binance-futures",
    "symbol": "perpetual-BTC-USDT:USDT",
    "output": OUTPUT,
}


def _parquet_bytes(marker: int, *, extra_columns: bool = False) -> bytes:
    """A one-row parquet whose ``marker`` column identifies the source file.

    ``extra_columns`` reproduces the August 2026 backfill defect: a stored object that
    carries DuckDB's synthesized hive partition columns, so it is wider than its siblings
    and holds a VARCHAR column named ``timestamp``.
    """
    columns = {"marker": [marker]}
    if extra_columns:
        columns |= {"timestamp": ["exchange"], "exchange": ["binance-futures"]}
    buf = BytesIO()
    pq.write_table(pa.table(columns), buf)
    return buf.getvalue()


def _ts(value: str) -> datetime:
    """A naive timestamp, matching how the parquets store `time`."""
    return datetime.fromisoformat(value)


def _markers(frame) -> list[int]:
    column = frame["marker"]
    return column.to_list() if HAS_POLARS else column.tolist()


def _run(files, *, completion_order=None, show_progress=True, wide_markers=()):
    """Fetch ``files`` through the client, controlling download completion order.

    ``completion_order`` lists markers in the order their downloads finish; any
    file not named there resolves immediately.

    ``show_progress`` defaults to True to match the client's own default, and
    because that path collects results via ``asyncio.as_completed`` — so the
    downloads really are handed back out of order, which is what makes these
    assertions meaningful.

    ``wide_markers`` names files that come back carrying extra columns.
    """
    payload = [
        {**file, "url": f"https://r2.example/{file['marker']}.parquet"}
        for file in files
    ]
    markers = {entry["url"]: entry["marker"] for entry in payload}
    delays = (
        {marker: i * 0.01 for i, marker in enumerate(completion_order)}
        if completion_order
        else {}
    )

    async def fake_download(url, headers, *, year, month, semaphore, **kwargs):
        marker = markers[url]
        await asyncio.sleep(delays.get(marker, 0))
        return year, month, _parquet_bytes(marker, extra_columns=marker in wide_markers)

    response = {
        "files": [
            {k: v for k, v in entry.items() if k != "marker"} for entry in payload
        ]
    }

    with (
        mock.patch(
            "aperiodic.endpoints.utils._fetch_presigned_urls",
            new=mock.AsyncMock(return_value=response),
        ),
        mock.patch(
            "aperiodic.endpoints.utils.download_parquet_bytes",
            new=mock.AsyncMock(side_effect=fake_download),
        ),
    ):
        return asyncio.run(
            _get_files_from_bucket_async(
                **FETCH_PARAMS,
                start_date=date(2026, 8, 1),
                end_date=date(2026, 9, 1),
                show_progress=show_progress,
            )
        )


class TestDailyFileOrdering:
    def test_daily_files_concatenate_in_day_order(self):
        result = _run(
            [
                {"year": 2026, "month": 8, "day": 11, "marker": 11},
                {"year": 2026, "month": 8, "day": 12, "marker": 12},
                {"year": 2026, "month": 8, "day": 13, "marker": 13},
            ],
            # Downloads finish newest-first; the result must still be ordered.
            completion_order=[13, 12, 11],
        )
        assert _markers(result) == [11, 12, 13]

    def test_ordering_holds_without_the_progress_bar(self):
        result = _run(
            [
                {"year": 2026, "month": 8, "day": 11, "marker": 11},
                {"year": 2026, "month": 8, "day": 12, "marker": 12},
                {"year": 2026, "month": 8, "day": 13, "marker": 13},
            ],
            completion_order=[12, 13, 11],
            show_progress=False,
        )
        assert _markers(result) == [11, 12, 13]

    def test_daily_files_order_across_a_month_boundary(self):
        result = _run(
            [
                {"year": 2026, "month": 8, "day": 30, "marker": 1},
                {"year": 2026, "month": 8, "day": 31, "marker": 2},
                {"year": 2026, "month": 9, "day": 1, "marker": 3},
            ],
            completion_order=[3, 1, 2],
        )
        assert _markers(result) == [1, 2, 3]

    def test_monthly_file_sorts_before_the_same_month_daily_files(self):
        # The changeover falls on a month boundary, so the API does not mix the
        # two layouts within one month today. Pinned anyway: this ordering is
        # what the missing-`day` fallback means, and the client should not
        # silently scramble if a month is ever served both ways.
        result = _run(
            [
                {"year": 2026, "month": 8, "marker": 1},
                {"year": 2026, "month": 8, "day": 11, "marker": 2},
                {"year": 2026, "month": 8, "day": 12, "marker": 3},
            ],
            completion_order=[3, 2, 1],
        )
        assert _markers(result) == [1, 2, 3]

    def test_monthly_only_responses_are_unaffected(self):
        result = _run(
            [
                {"year": 2026, "month": 6, "marker": 1},
                {"year": 2026, "month": 7, "marker": 2},
                {"year": 2026, "month": 8, "marker": 3},
            ],
            completion_order=[2, 3, 1],
        )
        assert _markers(result) == [1, 2, 3]

    def test_empty_file_list_returns_empty_frame(self):
        assert len(_run([])) == 0


class TestTimeOrdering:
    """The trailing sort runs on `time`, the column the parquets actually carry.

    It used to look for a column named `timestamp` — a constant shared with the
    `timestamp=` query parameter — which no well-formed object has. The guard was
    therefore always false and the sort never ran, leaving concatenation order as the
    only thing establishing row order.
    """

    def _run_timed(self, rows_by_marker, *, completion_order):
        """Like ``_run``, but each file carries real ``time`` values."""
        files = [
            {"year": 2026, "month": 8, "day": day, "marker": day}
            for day in rows_by_marker
        ]

        def timed_parquet(marker):
            buf = BytesIO()
            pq.write_table(
                pa.table(
                    {
                        "time": pa.array(
                            rows_by_marker[marker], type=pa.timestamp("us")
                        ),
                        "marker": [marker] * len(rows_by_marker[marker]),
                    }
                ),
                buf,
            )
            return buf.getvalue()

        payload = [
            {**f, "url": f"https://r2.example/{f['marker']}.parquet"} for f in files
        ]
        markers = {e["url"]: e["marker"] for e in payload}
        delays = {m: i * 0.01 for i, m in enumerate(completion_order)}

        async def fake_download(url, headers, *, year, month, semaphore, **kwargs):
            marker = markers[url]
            await asyncio.sleep(delays.get(marker, 0))
            return year, month, timed_parquet(marker)

        response = {
            "files": [{k: v for k, v in e.items() if k != "marker"} for e in payload]
        }

        with (
            mock.patch(
                "aperiodic.endpoints.utils._fetch_presigned_urls",
                new=mock.AsyncMock(return_value=response),
            ),
            mock.patch(
                "aperiodic.endpoints.utils.download_parquet_bytes",
                new=mock.AsyncMock(side_effect=fake_download),
            ),
        ):
            return asyncio.run(
                _get_files_from_bucket_async(
                    **FETCH_PARAMS,
                    start_date=date(2026, 8, 1),
                    end_date=date(2026, 8, 3),
                    show_progress=True,
                )
            )

    def test_rows_come_back_sorted_by_time(self):
        # Day 2's rows are reversed *inside* the file. Ordering the files by day key —
        # which is all the client did before — cannot fix that; only a real sort on
        # `time` can. Nothing guarantees row order within a stored object.
        result = self._run_timed(
            {
                1: [_ts("2026-08-01 00:00"), _ts("2026-08-01 12:00")],
                2: [_ts("2026-08-02 12:00"), _ts("2026-08-02 00:00")],
                3: [_ts("2026-08-03 00:00"), _ts("2026-08-03 12:00")],
            },
            completion_order=[3, 1, 2],
        )

        times = result["time"].to_list() if HAS_POLARS else result["time"].tolist()
        assert times == sorted(times)
        assert len(times) == 6

    def test_rows_outside_the_requested_range_are_trimmed(self):
        # The filter shares the guard with the sort, so it was dead in the same way.
        result = self._run_timed(
            {
                1: [_ts("2026-08-01 00:00")],
                2: [_ts("2026-08-02 00:00")],
                3: [_ts("2026-08-03 00:00"), _ts("2026-08-04 00:00")],
            },
            completion_order=[1, 2, 3],
        )

        assert len(result) == 3


DRIFT_FILES = [
    {"year": 2026, "month": 8, "day": 1, "marker": 1},
    {"year": 2026, "month": 8, "day": 2, "marker": 2},
    {"year": 2026, "month": 8, "day": 3, "marker": 3},
]


class TestSchemaDrift:
    """Files whose columns disagree must fail with the offending file named.

    The August 2026 backfill wrote days 1-9 with four extra hive partition columns. The
    client surfaced that as `ShapeError: unable to append to a DataFrame of width 17 with
    a DataFrame of width 13` - true, but it names neither the file nor the columns, so it
    reads as a client bug rather than a bad object.
    """

    FILES = DRIFT_FILES

    def test_extra_columns_raise_an_error_naming_the_file(self):
        with pytest.raises(AperiodicDataError) as excinfo:
            _run(self.FILES, wide_markers={2})

        message = str(excinfo.value)
        assert "2026-08-02" in message
        assert "timestamp" in message
        assert "exchange" in message

    def test_the_error_distinguishes_extra_from_missing_columns(self):
        # The first file sets the expectation, so a *narrow* later file must report its
        # columns as missing rather than as unexpected.
        with pytest.raises(AperiodicDataError) as excinfo:
            _run(self.FILES, wide_markers={1, 2})

        message = str(excinfo.value)
        assert "2026-08-03" in message
        assert "missing: ['exchange', 'timestamp']" in message

    def test_uniform_files_are_unaffected(self):
        assert _markers(_run(self.FILES)) == [1, 2, 3]

    def test_a_single_wide_file_is_not_an_error(self):
        # Nothing to disagree with. Whether one object is well-formed is not a question
        # concatenation can answer.
        assert len(_run(self.FILES[:1], wide_markers={1})) == 1
