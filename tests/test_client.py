import os
from datetime import date
from typing import get_args

import polars as pl
import pytest

from aperiodic_data_client import (
    APIError,
    get_derivative_metrics,
    get_l1_metrics,
    get_l2_metrics,
    get_ohlcv,
    get_symbols,
    get_trade_metrics,
)
from aperiodic_data_client.types import (
    DerivativeMetric,
    Exchange,
    Interval,
    L1Metric,
    L2Metric,
    TimestampType,
    TradeMetric,
)

API_KEY = os.environ.get("APERIODIC_API_KEY")
requires_api_key = pytest.mark.skipif(
    API_KEY is None, reason="APERIODIC_API_KEY environment variable not set"
)

# Shared defaults for all data fetch calls
COMMON_PARAMS = dict(
    timestamp="exchange",
    interval="1d",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 2, 1),
    show_progress=False,
)


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
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        for col in ["open", "high", "low", "close", "volume"]:
            assert col in result.columns
        assert result["time"].min().date() >= COMMON_PARAMS["start_date"]
        assert result["time"].is_sorted()


class TestGetTradeMetrics:
    @pytest.mark.parametrize(
        "metric",
        get_args(TradeMetric),
    )
    def test_invalid_api_key_raises_401(self, metric):
        with pytest.raises(APIError) as exc_info:
            get_trade_metrics(api_key="invalid-key", metric=metric, **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    @pytest.mark.parametrize(
        "metric",
        get_args(TradeMetric),
    )
    def test_returns_dataframe(self, metric):
        result = get_trade_metrics(api_key=API_KEY, metric=metric, **COMMON_PARAMS)
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert "time" in result.columns
        assert result["time"].is_sorted()


class TestGetL1Metrics:
    @pytest.mark.parametrize("metric", get_args(L1Metric))
    def test_invalid_api_key_raises_401(self, metric):
        with pytest.raises(APIError) as exc_info:
            get_l1_metrics(api_key="invalid-key", metric=metric, **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    @pytest.mark.parametrize("metric", get_args(L1Metric))
    def test_returns_dataframe(self, metric):
        result = get_l1_metrics(api_key=API_KEY, metric=metric, **COMMON_PARAMS)
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert "time" in result.columns
        assert result["time"].is_sorted()


class TestGetL2Metrics:
    def test_invalid_api_key_raises_401(self):
        with pytest.raises(APIError) as exc_info:
            get_l2_metrics(api_key="invalid-key", **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    @pytest.mark.parametrize(
        "metric",
        get_args(L2Metric),
    )
    def test_returns_dataframe(self, metric):
        result = get_l2_metrics(api_key=API_KEY, metric=metric, **COMMON_PARAMS)
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert result["time"].is_sorted()


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
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert "time" in result.columns
        assert result["time"].is_sorted()


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


class TestTypes:
    def test_timestamp_literal(self):
        assert len(get_args(TimestampType)) == 2

    def test_interval_literal(self):
        assert len(get_args(Interval)) == 7

    def test_exchange_literal(self):
        assert len(get_args(Exchange)) == 3

    def test_trade_metric_literal(self):
        assert len(get_args(TradeMetric)) == 6

    def test_l1_metric_literal(self):
        assert len(get_args(L1Metric)) == 3

    def test_l2_metric_literal(self):
        assert len(get_args(L2Metric)) == 2

    def test_derivative_metric_literal(self):
        assert len(get_args(DerivativeMetric)) == 3
