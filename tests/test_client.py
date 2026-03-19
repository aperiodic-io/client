import os
from datetime import date
from typing import get_args

import pytest

from aperiodic import (
    APIError,
    get_derivative_metrics,
    get_metrics,
    get_ohlcv,
    get_symbols,
    get_twap,
    get_vwap,
)
from aperiodic._compat import HAS_POLARS, DataFrame
from aperiodic.types import (
    DerivativeMetric,
    L1Metric,
    L2Metric,
    TradeMetric,
)

API_KEY = os.environ.get("APERIODIC_API_KEY")
requires_api_key = pytest.mark.skipif(
    API_KEY is None, reason="APERIODIC_API_KEY environment variable not set"
)

# Shared defaults for all data fetch calls
COMMON_PARAMS = {
    "timestamp": "exchange",
    "interval": "1d",
    "exchange": "binance-futures",
    "symbol": "perpetual-BTC-USDT:USDT",
    "start_date": date(2024, 1, 1),
    "end_date": date(2024, 2, 1),
    "show_progress": False,
}


class TestGetOhlcv:
    def test_invalid_api_key_raises_401(self):
        with pytest.raises(APIError) as exc_info:
            get_ohlcv(api_key="invalid-key", **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    def test_inverted_date_range_raises_error(self):
        with pytest.raises(APIError) as exc_info:
            get_ohlcv(
                api_key="test-key",
                **{
                    **COMMON_PARAMS,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 1, 1),
                },
            )
        assert exc_info.value.status_code in [400, 401]

    @requires_api_key
    def test_returns_dataframe_with_ohlcv_columns(self):
        result = get_ohlcv(api_key=API_KEY, **COMMON_PARAMS)
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        for col in ["open", "high", "low", "close", "volume"]:
            assert col in result.columns
        assert result["time"].min().date() >= COMMON_PARAMS["start_date"]
        if HAS_POLARS:
            assert result["time"].is_sorted()
        else:
            assert result["time"].is_monotonic_increasing


class TestGetVwap:
    def test_invalid_api_key_raises_401(self):
        with pytest.raises(APIError) as exc_info:
            get_vwap(api_key="invalid-key", **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    def test_returns_dataframe(self):
        result = get_vwap(api_key=API_KEY, **COMMON_PARAMS)
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        assert "time" in result.columns
        if HAS_POLARS:
            assert result["time"].is_sorted()
        else:
            assert result["time"].is_monotonic_increasing


class TestGetTwap:
    def test_invalid_api_key_raises_401(self):
        with pytest.raises(APIError) as exc_info:
            get_twap(api_key="invalid-key", **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    def test_returns_dataframe(self):
        result = get_twap(api_key=API_KEY, **COMMON_PARAMS)
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        assert "time" in result.columns
        if HAS_POLARS:
            assert result["time"].is_sorted()
        else:
            assert result["time"].is_monotonic_increasing


class TestGetTradeMetrics:
    @pytest.mark.parametrize(
        "metric",
        get_args(TradeMetric),
    )
    def test_invalid_api_key_raises_401(self, metric):
        with pytest.raises(APIError) as exc_info:
            get_metrics(api_key="invalid-key", metric=metric, **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    @pytest.mark.parametrize(
        "metric",
        get_args(TradeMetric),
    )
    def test_returns_dataframe(self, metric):
        result = get_metrics(api_key=API_KEY, metric=metric, **COMMON_PARAMS)
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        assert "time" in result.columns
        if HAS_POLARS:
            assert result["time"].is_sorted()
        else:
            assert result["time"].is_monotonic_increasing


class TestGetL1Metrics:
    @pytest.mark.parametrize("metric", get_args(L1Metric))
    def test_invalid_api_key_raises_401(self, metric):
        with pytest.raises(APIError) as exc_info:
            get_metrics(api_key="invalid-key", metric=metric, **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    @pytest.mark.parametrize("metric", get_args(L1Metric))
    def test_returns_dataframe(self, metric):
        result = get_metrics(api_key=API_KEY, metric=metric, **COMMON_PARAMS)
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        assert "time" in result.columns
        if HAS_POLARS:
            assert result["time"].is_sorted()
        else:
            assert result["time"].is_monotonic_increasing


class TestGetL2Metrics:
    @pytest.mark.parametrize("metric", get_args(L2Metric))
    def test_invalid_api_key_raises_401(self, metric):
        with pytest.raises(APIError) as exc_info:
            get_metrics(api_key="invalid-key", metric=metric, **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    @pytest.mark.parametrize(
        "metric",
        get_args(L2Metric),
    )
    def test_returns_dataframe(self, metric):
        result = get_metrics(api_key=API_KEY, metric=metric, **COMMON_PARAMS)
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        if HAS_POLARS:
            assert result["time"].is_sorted()
        else:
            assert result["time"].is_monotonic_increasing


class TestGetDerivativeMetrics:
    @pytest.mark.parametrize("metric", get_args(DerivativeMetric))
    def test_invalid_api_key_raises_401(self, metric):
        with pytest.raises(APIError) as exc_info:
            get_derivative_metrics(
                api_key="invalid-key", metric=metric, **COMMON_PARAMS
            )
        assert exc_info.value.status_code == 401

    @requires_api_key
    @pytest.mark.parametrize("metric", get_args(DerivativeMetric))
    def test_returns_dataframe(self, metric):
        result = get_derivative_metrics(api_key=API_KEY, metric=metric, **COMMON_PARAMS)
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        assert "time" in result.columns
        if HAS_POLARS:
            assert result["time"].is_sorted()
        else:
            assert result["time"].is_monotonic_increasing


class TestGetSymbols:
    def test_invalid_api_key_raises_401(self):
        with pytest.raises(APIError) as exc_info:
            get_symbols(api_key="invalid-key", exchange="binance-futures")
        assert exc_info.value.status_code == 401

    def test_invalid_exchange_raises_error(self):
        with pytest.raises(APIError) as exc_info:
            get_symbols(api_key="test-key", exchange="invalid-exchange")  # type: ignore
        assert exc_info.value.status_code in [400, 401]

    @requires_api_key
    def test_returns_symbols_for_exchange(self):
        result = get_symbols(api_key=API_KEY, exchange="binance-futures")
        assert isinstance(result, list)
        assert len(result) > 0
        assert "perpetual-BTC-USDT:USDT" in result
