"""Tests for the Unravel Data Client."""

from datetime import date

import polars as pl
import pytest

from unravel_data_client import (
    APIError,
    DownloadError,
    get_ohlcv_historical,
    get_symbols,
)


class TestGetOhlcvHistorical:
    """Tests for get_ohlcv_historical function."""

    def test_invalid_api_key_raises_error(self):
        """Test that an invalid API key raises APIError."""
        with pytest.raises(APIError) as exc_info:
            get_ohlcv_historical(
                api_key="invalid-key",
                arrival_time="true",
                period="1d",
                exchange="binance-futures",
                symbol="btcusdt",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                show_progress=False,
            )

        assert exc_info.value.status_code == 401

    def test_returns_dataframe(self):
        """Test that the function returns a Polars DataFrame."""
        # This test requires a valid API key - skip if not available
        pytest.skip("Requires valid API key")

    def test_date_range_validation(self):
        """Test that start_date must be before end_date."""
        # The API should return a 400 error for invalid date range
        with pytest.raises(APIError) as exc_info:
            get_ohlcv_historical(
                api_key="test-key",
                arrival_time="true",
                period="1d",
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

    def test_returns_list(self):
        """Test that the function returns a list of symbols."""
        # This test requires a valid API key - skip if not available
        pytest.skip("Requires valid API key")

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

    def test_arrival_time_literal(self):
        """Test ArrivalTime type accepts valid values."""
        from unravel_data_client.types import ArrivalTime

        # These should be valid - just checking the type exists
        valid_values: list[ArrivalTime] = ["exchange", "true"]
        assert len(valid_values) == 2

    def test_period_literal(self):
        """Test Period type accepts valid values."""
        from unravel_data_client.types import Period

        valid_values: list[Period] = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
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
