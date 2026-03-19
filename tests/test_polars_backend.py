"""Test that aperiodic works with polars pre-installed (no [polars] extra needed).

When polars is already present in the environment, a plain ``pip install aperiodic``
should auto-detect it and return polars DataFrames.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

pl = pytest.importorskip("polars")

from aperiodic import get_metrics  # noqa: E402

API_KEY = os.environ.get("APERIODIC_API_KEY")
requires_api_key = pytest.mark.skipif(
    API_KEY is None, reason="APERIODIC_API_KEY environment variable not set"
)


@requires_api_key
def test_get_metrics_returns_polars_dataframe():
    result = get_metrics(
        api_key=API_KEY,
        metric="volume",
        timestamp="exchange",
        interval="1d",
        exchange="binance-futures",
        symbol="perpetual-BTC-USDT:USDT",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        show_progress=False,
    )
    assert isinstance(result, pl.DataFrame)
    assert len(result) > 0
