"""Integration tests for the Unravel Data Client."""

import os
from datetime import date

import polars as pl
import pytest

from unravel_data_client import (
    APIError,
    get_l1_metrics,
    get_l2_metrics,
    get_ohlcv,
    get_symbols,
    get_trade_metrics,
)

API_KEY = os.environ.get("UNRAVEL_API_KEY")
requires_api_key = pytest.mark.skipif(
    API_KEY is None, reason="UNRAVEL_API_KEY environment variable not set"
)

# Shared defaults for all data fetch calls
COMMON_PARAMS = dict(
    timestamp="true",
    interval="1d",
    exchange="binance-futures",
    symbol="btcusdt",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 7),
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
                **{**COMMON_PARAMS, "start_date": date(2024, 3, 1), "end_date": date(2024, 1, 1)},
            )
        assert exc_info.value.status_code in [400, 401]

    @requires_api_key
    def test_returns_dataframe_with_ohlcv_columns(self):
        result = get_ohlcv(api_key=API_KEY, **COMMON_PARAMS)
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        for col in ["open", "high", "low", "close", "volume"]:
            assert col in result.columns

    @requires_api_key
    def test_result_sorted_by_timestamp(self):
        result = get_ohlcv(api_key=API_KEY, **COMMON_PARAMS)
        assert result["timestamp"].is_sorted()

    @requires_api_key
    def test_date_range_is_respected(self):
        result = get_ohlcv(api_key=API_KEY, **COMMON_PARAMS)
        assert result["datetime"].min().date() >= COMMON_PARAMS["start_date"]
        assert result["datetime"].max().date() <= COMMON_PARAMS["end_date"]


class TestGetTradeMetrics:
    @pytest.mark.parametrize(
        "metric",
        ["vtwap", "flow", "trade_size", "impact", "range", "updownticks"],
    )
    def test_invalid_api_key_raises_401(self, metric):
        with pytest.raises(APIError) as exc_info:
            get_trade_metrics(api_key="invalid-key", metric=metric, **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    @pytest.mark.parametrize(
        "metric",
        ["vtwap", "flow", "trade_size", "impact", "range", "updownticks"],
    )
    def test_returns_dataframe(self, metric):
        result = get_trade_metrics(api_key=API_KEY, metric=metric, **COMMON_PARAMS)
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0

    @requires_api_key
    def test_vtwap_columns(self):
        result = get_trade_metrics(api_key=API_KEY, metric="vtwap", **COMMON_PARAMS)
        assert "timestamp" in result.columns

    @requires_api_key
    def test_flow_columns(self):
        result = get_trade_metrics(api_key=API_KEY, metric="flow", **COMMON_PARAMS)
        assert "timestamp" in result.columns

    @requires_api_key
    def test_result_sorted_by_timestamp(self):
        result = get_trade_metrics(api_key=API_KEY, metric="flow", **COMMON_PARAMS)
        assert result["timestamp"].is_sorted()


class TestGetL1Metrics:
    @pytest.mark.parametrize("metric", ["l1_price", "l1_imbalance", "l1_liquidity"])
    def test_invalid_api_key_raises_401(self, metric):
        with pytest.raises(APIError) as exc_info:
            get_l1_metrics(api_key="invalid-key", metric=metric, **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    @pytest.mark.parametrize("metric", ["l1_price", "l1_imbalance", "l1_liquidity"])
    def test_returns_dataframe(self, metric):
        result = get_l1_metrics(api_key=API_KEY, metric=metric, **COMMON_PARAMS)
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0

    @requires_api_key
    def test_l1_price_columns(self):
        result = get_l1_metrics(api_key=API_KEY, metric="l1_price", **COMMON_PARAMS)
        assert "timestamp" in result.columns

    @requires_api_key
    def test_result_sorted_by_timestamp(self):
        result = get_l1_metrics(api_key=API_KEY, metric="l1_price", **COMMON_PARAMS)
        assert result["timestamp"].is_sorted()


class TestGetL2Metrics:
    def test_invalid_api_key_raises_401(self):
        with pytest.raises(APIError) as exc_info:
            get_l2_metrics(api_key="invalid-key", **COMMON_PARAMS)
        assert exc_info.value.status_code == 401

    @requires_api_key
    def test_returns_dataframe(self):
        result = get_l2_metrics(api_key=API_KEY, **COMMON_PARAMS)
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0

    @requires_api_key
    def test_result_sorted_by_timestamp(self):
        result = get_l2_metrics(api_key=API_KEY, **COMMON_PARAMS)
        assert result["timestamp"].is_sorted()


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
    @pytest.mark.parametrize("bucket", ["ohlcv", "flow", "l1_price", "l2_imbalance"])
    def test_returns_symbols_for_bucket(self, bucket):
        result = get_symbols(api_key=API_KEY, exchange="binance-futures", bucket=bucket)
        assert isinstance(result, list)
        assert len(result) > 0
        assert "btcusdt" in result

    @requires_api_key
    def test_symbols_are_lowercase(self):
        result = get_symbols(api_key=API_KEY, exchange="binance-futures")
        assert all(s == s.lower() for s in result)


class TestTypes:
    def test_timestamp_literal(self):
        from unravel_data_client.types import TimestampType
        valid_values: list[TimestampType] = ["exchange", "true"]
        assert len(valid_values) == 2

    def test_interval_literal(self):
        from unravel_data_client.types import Interval
        valid_values: list[Interval] = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
        assert len(valid_values) == 7

    def test_exchange_literal(self):
        from unravel_data_client.types import Exchange
        valid_values: list[Exchange] = ["binance-futures", "binance", "okx-perps"]
        assert len(valid_values) == 3

    def test_trade_metric_literal(self):
        from unravel_data_client.types import TradeMetric
        valid_values: list[TradeMetric] = ["vtwap", "flow", "trade_size", "impact", "range", "updownticks"]
        assert len(valid_values) == 6

    def test_l1_metric_literal(self):
        from unravel_data_client.types import L1Metric
        valid_values: list[L1Metric] = ["l1_price", "l1_imbalance", "l1_liquidity"]
        assert len(valid_values) == 3
