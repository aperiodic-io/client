"""Tests for ordering of downloaded parquet files.

Data up to 2026-07-31 is served as one parquet per month; from 2026-08-01 the
API returns one parquet per day, so a single response can contain many files
sharing the same ``year``/``month``. These tests pin the client's ordering
guarantee: files are concatenated chronologically regardless of the order their
downloads complete in.
"""

from __future__ import annotations

import asyncio
from datetime import date
from io import BytesIO
from unittest import mock

import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa
import pyarrow.parquet as pq

from aperiodic._compat import HAS_POLARS
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


def _parquet_bytes(marker: int) -> bytes:
    """A one-row parquet whose ``marker`` column identifies the source file."""
    buf = BytesIO()
    pq.write_table(pa.table({"marker": [marker]}), buf)
    return buf.getvalue()


def _markers(frame) -> list[int]:
    column = frame["marker"]
    return column.to_list() if HAS_POLARS else column.tolist()


def _run(files, *, completion_order=None, show_progress=True):
    """Fetch ``files`` through the client, controlling download completion order.

    ``completion_order`` lists markers in the order their downloads finish; any
    file not named there resolves immediately.

    ``show_progress`` defaults to True to match the client's own default, and
    because that path collects results via ``asyncio.as_completed`` — so the
    downloads really are handed back out of order, which is what makes these
    assertions meaningful.
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
        return year, month, _parquet_bytes(marker)

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
