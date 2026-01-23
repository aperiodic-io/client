"""
Unravel Data Client - Client library for Unravel aggregate market data API.

This package provides easy access to Unravel's pre-computed market data aggregates
including OHLCV candlestick data for crypto exchanges.

Example:
    >>> from datetime import date
    >>> from unravel_data_client import get_ohlcv_historical
    >>>
    >>> df = get_ohlcv_historical(
    ...     api_key="your-api-key",
    ...     arrival_time="true",
    ...     period="1h",
    ...     exchange="binance-futures",
    ...     symbol="btcusdt",
    ...     start_date=date(2024, 1, 1),
    ...     end_date=date(2024, 3, 31),
    ... )
"""

__version__ = "0.1.0"

from .client import (
    APIError,
    DownloadError,
    UnravelDataError,
    get_ohlcv_historical,
    get_ohlcv_historical_async,
    get_ohlcv_historical_multi,
    get_symbols,
    get_symbols_async,
)
from .types import ArrivalTime, Exchange, Period

__all__ = [
    # Functions
    "get_ohlcv_historical",
    "get_ohlcv_historical_async",
    "get_ohlcv_historical_multi",
    "get_symbols",
    "get_symbols_async",
    # Exceptions
    "APIError",
    "DownloadError",
    "UnravelDataError",
    # Types
    "ArrivalTime",
    "Exchange",
    "Period",
]
