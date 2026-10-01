"""Raw data against a live API, end to end: what a user of the client gets.

Runs against production, or APERIODIC_API_URL (CI sets staging before a
release). The files are the June 2025 BTC perpetuals, in the raw buckets since
the proof of concept. The paid tests need APERIODIC_API_KEY to be on the
Prime + Raw plan.

Whole files are fetched only for derivative datasets: a month of trades or
quotes runs to hundreds of MB or more, too much for every job in the matrix
(the CLI's live tests download the trades file).
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from typing import get_args

import pytest

import aperiodic
from aperiodic import APIError
from aperiodic._compat import HAS_POLARS
from aperiodic.config import DEFAULT_BASE_URL
from aperiodic.types import RawDataset

API_KEY = os.environ.get("APERIODIC_API_KEY")
OUTPUT = "polars" if HAS_POLARS else "pandas"

PARAMS = {
    "exchange": "binance-futures",
    "symbol": "perpetual-BTC-USDT:USDT",
    "start_date": date(2025, 6, 1),
    "end_date": date(2025, 6, 2),
    "base_url": DEFAULT_BASE_URL,
    "show_progress": False,
}

LEADING_COLUMNS = ["exchange_timestamp", "local_timestamp", "local_timestamp_kind"]


def _column(frame, name: str) -> list:
    column = frame[name]
    return column.to_list() if HAS_POLARS else column.tolist()


def test_coverage_lists_the_six_datasets():
    body = aperiodic.get_raw_coverage(base_url=DEFAULT_BASE_URL)

    assert [d["id"] for d in body["datasets"]] == list(get_args(RawDataset))
    btc = body["coverage"]["datasets"]["trades"]["binance-futures"][PARAMS["symbol"]]
    assert btc["first"] <= "2025-06-01"
    assert btc["last"] >= "2025-06-30"


def test_preview_needs_no_key():
    frame = aperiodic.get_raw(
        dataset="funding_rate", output=OUTPUT, preview=True, **PARAMS
    )

    assert len(frame) > 0
    assert list(frame.columns)[:3] == LEADING_COLUMNS


def test_demo_key_is_refused_outside_the_preview():
    with pytest.raises(APIError) as info:
        aperiodic.get_raw("DEMO-KEY", dataset="trades", output=OUTPUT, **PARAMS)
    assert info.value.status_code == 401


def test_get_raw_trims_the_june_file_to_the_range():
    frame = aperiodic.get_raw(API_KEY, dataset="funding_rate", output=OUTPUT, **PARAMS)

    assert len(frame) > 0
    assert list(frame.columns)[:3] == LEADING_COLUMNS
    timestamps = _column(frame, "exchange_timestamp")
    assert min(timestamps) >= datetime(2025, 6, 1, tzinfo=UTC)
    assert max(timestamps) < datetime(2025, 6, 3, tzinfo=UTC)


def test_download_raw_writes_then_skips_the_june_file(tmp_path):
    params = {**PARAMS, "dataset": "mark_price", "output_dir": tmp_path}

    [path] = aperiodic.download_raw(API_KEY, **params)

    assert path.name == "data.parquet"
    assert "year=2025/month=06" in path.as_posix()
    content = path.read_bytes()
    assert content[:4] == content[-4:] == b"PAR1"

    written = path.stat().st_mtime_ns
    assert aperiodic.download_raw(API_KEY, **params) == [path]
    assert path.stat().st_mtime_ns == written
