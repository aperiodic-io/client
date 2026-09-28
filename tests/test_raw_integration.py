"""Raw data against a live API (staging by default), end to end.

Skipped unless APERIODIC_RAW_API_KEY holds a Prime + Raw key. Point it at
staging with APERIODIC_API_URL=https://staging.aperiodic.io/api/v1. The files
are the June 2025 proof-of-concept months in the shared raw buckets.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

import aperiodic
from aperiodic import APIError
from aperiodic._compat import HAS_POLARS
from aperiodic.config import DEFAULT_BASE_URL

API_KEY = os.environ.get("APERIODIC_RAW_API_KEY")
OUTPUT = "polars" if HAS_POLARS else "pandas"

pytestmark = pytest.mark.skipif(
    not API_KEY, reason="APERIODIC_RAW_API_KEY (a Prime + Raw key) is not set"
)

PARAMS = {
    "exchange": "binance-futures",
    "symbol": "perpetual-BTC-USDT:USDT",
    "start_date": date(2025, 6, 1),
    "end_date": date(2025, 6, 2),
    "base_url": DEFAULT_BASE_URL,
    "show_progress": False,
}


@pytest.mark.parametrize("dataset", ["trades", "quotes", "funding_rate"])
def test_get_raw_returns_the_schema(dataset):
    frame = aperiodic.get_raw(API_KEY, dataset=dataset, output=OUTPUT, **PARAMS)

    assert len(frame) > 0
    assert list(frame.columns)[:3] == [
        "exchange_timestamp",
        "local_timestamp",
        "local_timestamp_kind",
    ]


def test_download_raw_writes_the_june_file(tmp_path):
    paths = aperiodic.download_raw(
        API_KEY, dataset="trades", output_dir=tmp_path, **PARAMS
    )

    assert [p.name for p in paths] == ["data.parquet"]
    assert "year=2025/month=06" in paths[0].as_posix()
    assert paths[0].stat().st_size > 0


def test_coverage_lists_six_datasets():
    body = aperiodic.get_raw_coverage(base_url=DEFAULT_BASE_URL)
    assert len(body["datasets"]) == 6


def test_demo_key_is_refused_outside_the_preview():
    with pytest.raises(APIError) as info:
        aperiodic.get_raw("DEMO-KEY", dataset="trades", output=OUTPUT, **PARAMS)
    assert info.value.status_code == 401
