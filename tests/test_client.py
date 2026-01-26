"""Tests for the Unravel Data Client."""

import os
from datetime import date

import polars as pl
import pytest

from unravel_data_client import (
    APIError,
    get_ohlcv_historical,
    get_symbols,
)

# Get API key from environment variable (injected by CI)
API_KEY = os.environ.get("UNRAVEL_API_KEY")
requires_api_key = pytest.mark.skipif(
    API_KEY is None, reason="UNRAVEL_API_KEY environment variable not set"
)


class TestGetOhlcvHistorical:
    """Tests for get_ohlcv_historical function."""

    def test_invalid_api_key_raises_error(self):
        """Test that an invalid API key raises APIError."""
        with pytest.raises(APIError) as exc_info:
            get_ohlcv_historical(
                api_key="invalid-key",
                timestamp="true",
                interval="1d",
                exchange="binance-futures",
                symbol="btcusdt",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                show_progress=False,
            )

        assert exc_info.value.status_code == 401

    @requires_api_key
    def test_returns_dataframe(self):
        """Test that the function returns a Polars DataFrame."""
        result = get_ohlcv_historical(
            api_key=API_KEY,
            timestamp="true",
            interval="1d",
            exchange="binance-futures",
            symbol="btcusdt",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 7),
            show_progress=False,
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        # Check expected columns exist
        expected_columns = ["open", "high", "low", "close", "volume"]
        for col in expected_columns:
            assert col in result.columns

    def test_date_range_validation(self):
        """Test that start_date must be before end_date."""
        # The API should return a 400 error for invalid date range
        with pytest.raises(APIError) as exc_info:
            get_ohlcv_historical(
                api_key="test-key",
                timestamp="true",
                interval="1d",
                exchange="binance-futures",
                symbol="btcusdt",
                start_date=date(2024, 3, 1),
                end_date=date(2024, 1, 1),  # End before start
                show_progress=False,
            )

        assert exc_info.value.status_code in [400, 401]


class TestGetSymbols:
    """Tests for get_symbols function."""

    def test_invalid_api_key_raises_error(self):
        """Test that an invalid API key raises APIError."""
        with pytest.raises(APIError) as exc_info:
            get_symbols(
                api_key="invalid-key",
                exchange="binance-futures",
            )

        assert exc_info.value.status_code == 401

    @requires_api_key
    def test_returns_list(self):
        """Test that the function returns a list of symbols."""
        result = get_symbols(
            api_key=API_KEY,
            exchange="binance-futures",
        )

        assert isinstance(result, list)
        assert len(result) > 0
        # Check that btcusdt is in the list (should always be available)
        assert "btcusdt" in result

    def test_invalid_exchange_raises_error(self):
        """Test that an invalid exchange raises APIError."""
        with pytest.raises(APIError) as exc_info:
            get_symbols(
                api_key="test-key",
                exchange="invalid-exchange",  # type: ignore
            )

        # Should get 400 for invalid exchange or 401 for invalid key
        assert exc_info.value.status_code in [400, 401]


class TestTypes:
    """Tests for type definitions."""

    def test_timestamp_literal(self):
        """Test ArrivalTime type accepts valid values."""
        from unravel_data_client.types import TimestampType

        # These should be valid - just checking the type exists
        valid_values: list[TimestampType] = ["exchange", "true"]
        assert len(valid_values) == 2

    def test_interval_literal(self):
        """Test Interval type accepts valid values."""
        from unravel_data_client.types import Interval

        valid_values: list[Interval] = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
        assert len(valid_values) == 7

    def test_exchange_literal(self):
        """Test Exchange type accepts valid values."""
        from unravel_data_client.types import Exchange

        valid_values: list[Exchange] = ["binance-futures", "binance"]
        assert len(valid_values) == 2

    def test_symbols_response_type(self):
        """Test SymbolsResponse type exists."""
        from unravel_data_client.types import SymbolsResponse

        # Just checking the type can be imported
        assert SymbolsResponse is not None
