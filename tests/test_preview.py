"""Tests for the preview endpoint (/api/v1/data/preview/:metric_id).

The preview endpoint is open to any authenticated user (no subscription required),
but only for a curated whitelist of exact parameter combinations. Requests that
do not match a whitelist entry return HTTP 400.

These tests cover:
  - Invalid API key → 401 (auth fires before whitelist check)
  - Exact whitelist params → real data returned (ohlcv + l2_imbalance)
  - Date ranges that are structurally impossible to whitelist → 400
    - Spans greater than 2 years (preview is intended as a small sample)
    - Recent windows wider than 3 months (preview windows are narrow slices)
"""

import os
from datetime import date

import pytest

from aperiodic import APIError, get_metrics, get_ohlcv
from aperiodic._compat import HAS_POLARS, DataFrame

API_KEY = os.environ.get("APERIODIC_API_KEY")

# Canonical whitelist window — must stay in sync with preview-whitelist.config.ts.
PREVIEW_PARAMS = {
    "exchange": "binance-futures",
    "symbol": "perpetual-BTC-USDT:USDT",
    "interval": "5m",
    "timestamp": "exchange",
    "start_date": date(2025, 5, 1),
    "end_date": date(2025, 5, 31),
    "show_progress": False,
    "output": "polars" if HAS_POLARS else "pandas",
}


class TestPreview:
    def test_invalid_api_key_raises_401(self):
        """Auth check fires before whitelist check — even valid params → 401."""
        with pytest.raises(APIError) as exc_info:
            get_ohlcv(api_key="invalid-key", preview=True, **PREVIEW_PARAMS)
        assert exc_info.value.status_code == 401

    def test_ohlcv_returns_dataframe(self):
        result = get_ohlcv(api_key=API_KEY, preview=True, **PREVIEW_PARAMS)
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        for col in ["open", "high", "low", "close", "volume"]:
            assert col in result.columns
        if HAS_POLARS:
            assert result["time"].is_sorted()
        else:
            assert result["time"].is_monotonic_increasing

    def test_l2_imbalance_returns_dataframe(self):
        result = get_metrics(
            api_key=API_KEY, metric="l2_imbalance", preview=True, **PREVIEW_PARAMS
        )
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        assert "time" in result.columns
        if HAS_POLARS:
            assert result["time"].is_sorted()
        else:
            assert result["time"].is_monotonic_increasing

    @pytest.mark.parametrize(
        ("start_date", "end_date", "reason"),
        [
            # > 2 years — preview windows are small samples, never multi-year
            (date(2020, 1, 1), date(2022, 6, 1), "2.5-year span"),
            (date(2018, 1, 1), date(2021, 1, 1), "3-year span"),
            # Recent + > 3 months wide, with no overlap with any whitelist window.
            # The server clamps overlapping requests, so these must sit entirely
            # outside the whitelisted window (2025-05-01 → 2025-05-31).
            (date(2025, 6, 1), date(2025, 12, 31), "7-month span after whitelist window"),
            (date(2025, 1, 1), date(2025, 4, 30), "4-month span before whitelist window"),
            (date(2024, 1, 1), date(2024, 12, 31), "12-month span (2024)"),
        ],
    )
    def test_non_whitelisted_date_range_raises_400(self, start_date, end_date, reason):
        """Date ranges that are structurally outside any preview window → 400."""
        with pytest.raises(APIError) as exc_info:
            get_ohlcv(
                api_key=API_KEY,
                preview=True,
                **{**PREVIEW_PARAMS, "start_date": start_date, "end_date": end_date},
            )
        assert exc_info.value.status_code == 400, (
            f"Expected 400 for {reason} ({start_date} to {end_date}), "
            f"got {exc_info.value.status_code}"
        )
