"""Offline tests for the raw data endpoints (get_raw, download_raw, coverage).

The API and the presigned file URLs are mocked with respx, so the real
transport code runs: chunking long ranges, deduplicating monthly files,
re-requesting a URL after a 403, streaming to disk and skipping files that are
already there.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from io import BytesIO

import httpx
import pytest
import respx

pytest.importorskip("pyarrow")

import pyarrow as pa
import pyarrow.parquet as pq

import aperiodic
from aperiodic import APIError, DownloadError
from aperiodic._compat import HAS_POLARS
from aperiodic.endpoints import raw as raw_module
from aperiodic.endpoints.raw import (
    download_raw_async,
    get_raw_async,
    get_raw_coverage_async,
    raw_file_path,
)

BASE = "https://api.test/api/v1"
OUTPUT = "polars" if HAS_POLARS else "pandas"
SYMBOL = "perpetual-BTC-USDT:USDT"
PARAMS = {
    "api_key": "key-1",
    "dataset": "trades",
    "exchange": "binance-futures",
    "symbol": SYMBOL,
    "base_url": BASE,
    "show_progress": False,
}


def _parquet(times: list[str]) -> bytes:
    """A trades-shaped file with one row per exchange time."""
    stamps = [datetime.fromisoformat(t).replace(tzinfo=UTC) for t in times]
    table = pa.table(
        {
            "exchange_timestamp": pa.array(stamps, pa.timestamp("us", tz="UTC")),
            "local_timestamp": pa.array(stamps, pa.timestamp("us", tz="UTC")),
            "id": [str(i) for i in range(len(stamps))],
            "side": ["buy"] * len(stamps),
            "price": [100.0 + i for i in range(len(stamps))],
            "amount": [1.0] * len(stamps),
        }
    )
    buffer = BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


FILES = {
    "2025-05": _parquet(["2025-05-31T23:59:00"]),
    "2025-06": _parquet(
        ["2025-06-01T00:00:01", "2025-06-15T12:00:00", "2025-06-30T23:00:00"]
    ),
    "2025-07": _parquet(["2025-07-01T00:00:00", "2025-07-02T10:00:00"]),
}


def _file(period: str, version: int = 1) -> dict:
    return {
        "period": period,
        "url": f"https://r2.test/aperiodic-raw-trades/{period}.parquet?v={version}",
        "size": len(FILES[period]),
    }


def _listing(periods: list[str], version: int = 1) -> dict:
    return {
        "dataset": "trades",
        "exchange": "binance-futures",
        "symbol": SYMBOL,
        "schema_version": 1,
        "expires_in": 3600,
        "files": [_file(p, version) for p in periods],
        "missing_periods": [],
    }


def _serve_files(router, *, forbidden_versions: frozenset[int] = frozenset()):
    """Serve every file; URLs with a forbidden ?v= answer 403 like an expired URL."""

    def handler(request: httpx.Request) -> httpx.Response:
        period = request.url.path.rsplit("/", 1)[-1].removesuffix(".parquet")
        if int(request.url.params["v"]) in forbidden_versions:
            return httpx.Response(403, text="<Error><Code>AccessDenied</Code></Error>")
        return httpx.Response(200, content=FILES[period])

    return router.get(url__startswith="https://r2.test/").mock(side_effect=handler)


def _column(frame, name: str) -> list:
    column = frame[name]
    return column.to_list() if HAS_POLARS else column.tolist()


@pytest.fixture
def router():
    with respx.mock(assert_all_called=False) as mock:
        yield mock


# ---------------------------------------------------------------------------
# get_raw
# ---------------------------------------------------------------------------


async def test_get_raw_trims_monthly_files_to_the_range(router):
    api = router.get(f"{BASE}/data/raw/trades").mock(
        return_value=httpx.Response(200, json=_listing(["2025-06", "2025-07"]))
    )
    _serve_files(router)

    frame = await get_raw_async(
        **PARAMS, start_date=date(2025, 6, 15), end_date=date(2025, 7, 1), output=OUTPUT
    )

    assert _column(frame, "price") == [101.0, 102.0, 100.0]
    first = _column(frame, "exchange_timestamp")[0]
    assert first.utcoffset() is not None
    request = api.calls.last.request
    assert request.headers["X-API-KEY"] == "key-1"
    assert request.url.params["start_date"] == "2025-06-15"
    assert request.url.params["end_date"] == "2025-07-01"


async def test_get_raw_splits_long_ranges_and_dedupes_months(router):
    listings = iter(
        [
            _listing(["2025-05", "2025-06"]),
            _listing(["2025-06", "2025-07"]),
        ]
    )
    api = router.get(f"{BASE}/data/raw/trades").mock(
        side_effect=lambda _: httpx.Response(200, json=next(listings))
    )
    files = _serve_files(router)

    frame = await get_raw_async(
        **PARAMS, start_date=date(2024, 7, 1), end_date=date(2025, 7, 31), output=OUTPUT
    )

    windows = [
        (c.request.url.params["start_date"], c.request.url.params["end_date"])
        for c in api.calls
    ]
    assert windows == [("2024-07-01", "2025-07-01"), ("2025-07-02", "2025-07-31")]
    # 2025-06 was listed twice but downloaded once.
    assert files.call_count == 3
    assert len(_column(frame, "price")) == 6


async def test_get_raw_requests_a_fresh_url_after_a_403(router):
    listings = iter(
        [_listing(["2025-06"], version=1), _listing(["2025-06"], version=2)]
    )
    api = router.get(f"{BASE}/data/raw/trades").mock(
        side_effect=lambda _: httpx.Response(200, json=next(listings))
    )
    _serve_files(router, forbidden_versions=frozenset({1}))

    frame = await get_raw_async(
        **PARAMS, start_date=date(2025, 6, 1), end_date=date(2025, 6, 30), output=OUTPUT
    )

    assert len(_column(frame, "price")) == 3
    assert api.call_count == 2
    # The refresh asks for just that month.
    refresh = api.calls.last.request.url.params
    assert (refresh["start_date"], refresh["end_date"]) == ("2025-06-01", "2025-06-30")


async def test_get_raw_gives_up_after_one_refresh(router):
    router.get(f"{BASE}/data/raw/trades").mock(
        return_value=httpx.Response(200, json=_listing(["2025-06"]))
    )
    files = _serve_files(router, forbidden_versions=frozenset({1}))

    with pytest.raises(DownloadError) as info:
        await get_raw_async(
            **PARAMS,
            start_date=date(2025, 6, 1),
            end_date=date(2025, 6, 30),
            output=OUTPUT,
        )

    assert info.value.status_code == 403
    assert (info.value.year, info.value.month, info.value.day) == (2025, 6, None)
    # Two URLs tried, each once: a 403 isn't retried with backoff.
    assert files.call_count == 2


async def test_get_raw_surfaces_raw_not_in_plan(router):
    router.get(f"{BASE}/data/raw/trades").mock(
        return_value=httpx.Response(
            403,
            json={
                "error": "Raw data is not included in the prime plan.",
                "code": "raw_not_in_plan",
                "upgrade_url": "https://aperiodic.io/pricing",
            },
        )
    )

    with pytest.raises(APIError) as info:
        await get_raw_async(
            **PARAMS,
            start_date=date(2025, 6, 1),
            end_date=date(2025, 6, 30),
            output=OUTPUT,
        )

    assert info.value.status_code == 403
    assert info.value.code == "raw_not_in_plan"
    assert "not included" in info.value.message


async def test_get_raw_preview_uses_the_demo_key(router):
    api = router.get(f"{BASE}/data/raw/preview/trades").mock(
        return_value=httpx.Response(200, json=_listing(["2025-06"]))
    )
    _serve_files(router)

    params = {**PARAMS, "api_key": None}
    frame = await get_raw_async(
        **params,
        start_date=date(2025, 6, 1),
        end_date=date(2025, 6, 30),
        output=OUTPUT,
        preview=True,
    )

    assert len(_column(frame, "price")) == 3
    request = api.calls.last.request
    assert request.headers["X-API-KEY"] == "DEMO-KEY"
    assert dict(request.url.params) == {"exchange": "binance-futures", "symbol": SYMBOL}


async def test_get_raw_warns_on_large_ranges(router, monkeypatch):
    monkeypatch.setattr(raw_module, "LARGE_RAW_BYTES", 10)
    router.get(f"{BASE}/data/raw/trades").mock(
        return_value=httpx.Response(200, json=_listing(["2025-06"]))
    )
    _serve_files(router)

    with pytest.warns(UserWarning, match="download_raw"):
        await get_raw_async(
            **PARAMS,
            start_date=date(2025, 6, 1),
            end_date=date(2025, 6, 30),
            output=OUTPUT,
        )


# ---------------------------------------------------------------------------
# download_raw
# ---------------------------------------------------------------------------


async def test_download_raw_writes_the_bucket_layout(router, tmp_path):
    router.get(f"{BASE}/data/raw/trades").mock(
        return_value=httpx.Response(200, json=_listing(["2025-06", "2025-07"]))
    )
    _serve_files(router)

    paths = await download_raw_async(
        **PARAMS,
        start_date=date(2025, 6, 1),
        end_date=date(2025, 7, 31),
        output_dir=tmp_path,
    )

    assert [p.relative_to(tmp_path).as_posix() for p in paths] == [
        f"trades/exchange=binance-futures/symbol={SYMBOL}/year=2025/month=06/data.parquet",
        f"trades/exchange=binance-futures/symbol={SYMBOL}/year=2025/month=07/data.parquet",
    ]
    assert paths[0].read_bytes() == FILES["2025-06"]
    assert not list(tmp_path.rglob("*.part"))


async def test_download_raw_skips_files_with_the_right_size(router, tmp_path):
    router.get(f"{BASE}/data/raw/trades").mock(
        return_value=httpx.Response(200, json=_listing(["2025-06", "2025-07"]))
    )
    files = _serve_files(router)
    kept = raw_file_path(
        tmp_path,
        dataset="trades",
        exchange="binance-futures",
        symbol=SYMBOL,
        period="2025-06",
    )
    kept.parent.mkdir(parents=True)
    kept.write_bytes(FILES["2025-06"])
    stale = raw_file_path(
        tmp_path,
        dataset="trades",
        exchange="binance-futures",
        symbol=SYMBOL,
        period="2025-07",
    )
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"truncated")

    paths = await download_raw_async(
        **PARAMS,
        start_date=date(2025, 6, 1),
        end_date=date(2025, 7, 31),
        output_dir=tmp_path,
    )

    assert files.call_count == 1
    assert paths[1].read_bytes() == FILES["2025-07"]

    await download_raw_async(
        **PARAMS,
        start_date=date(2025, 6, 1),
        end_date=date(2025, 7, 31),
        output_dir=tmp_path,
        overwrite=True,
    )
    assert files.call_count == 3


async def test_download_raw_refreshes_an_expired_url(router, tmp_path):
    listings = iter(
        [_listing(["2025-06"], version=1), _listing(["2025-06"], version=2)]
    )
    router.get(f"{BASE}/data/raw/trades").mock(
        side_effect=lambda _: httpx.Response(200, json=next(listings))
    )
    _serve_files(router, forbidden_versions=frozenset({1}))

    paths = await download_raw_async(
        **PARAMS,
        start_date=date(2025, 6, 1),
        end_date=date(2025, 6, 30),
        output_dir=tmp_path,
    )

    assert paths[0].read_bytes() == FILES["2025-06"]


def test_raw_file_path_daily_and_windows(monkeypatch, tmp_path):
    daily = raw_file_path(
        tmp_path,
        dataset="quotes",
        exchange="okx-perps",
        symbol=SYMBOL,
        period="2026-08-10",
    )
    assert daily.relative_to(tmp_path).as_posix() == (
        f"quotes/exchange=okx-perps/symbol={SYMBOL}/year=2026/month=08/day=10/data.parquet"
    )

    monkeypatch.setattr(raw_module, "_ESCAPE_COLON", True)
    windows = raw_file_path(
        tmp_path,
        dataset="quotes",
        exchange="okx-perps",
        symbol=SYMBOL,
        period="2025-06",
    )
    assert "symbol=perpetual-BTC-USDT%3AUSDT" in windows.as_posix()


# ---------------------------------------------------------------------------
# coverage, types, errors
# ---------------------------------------------------------------------------


async def test_get_raw_coverage_needs_no_key(router):
    body = {"datasets": [{"id": "trades"}], "summary": [], "coverage": {"datasets": {}}}
    api = router.get(f"{BASE}/metadata/raw").mock(
        return_value=httpx.Response(200, json=body)
    )

    assert await get_raw_coverage_async(base_url=BASE) == body
    assert "X-API-KEY" not in api.calls.last.request.headers


def test_public_exports():
    for name in [
        "RawDataset",
        "get_raw",
        "get_raw_async",
        "download_raw",
        "download_raw_async",
        "get_raw_coverage",
        "get_raw_coverage_async",
    ]:
        assert name in aperiodic.__all__
        assert hasattr(aperiodic, name)


def test_download_error_names_the_day():
    error = DownloadError(2026, 8, RuntimeError("boom"), day=10)
    assert "2026-08-10" in str(error)
    assert json.dumps({"day": error.day}) == '{"day": 10}'
