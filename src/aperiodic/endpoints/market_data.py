from __future__ import annotations

from datetime import date

from .._compat import DataFrame, run_async
from ..config import DEFAULT_BASE_URL, MAX_CONCURRENT_DOWNLOADS
from ..types import Exchange, Interval, TimestampType
from .utils import _get_files_from_bucket_async


async def get_ohlcv_async(
    api_key: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
) -> DataFrame:
    """
    Fetch historical OHLCV (candlestick) data.

    Args:
        api_key: Your Aperiodic API key
        timestamp: Timestamp source - 'exchange' for exchange-reported time,
                   'true' for actual arrival time at Aperiodic servers
        interval: Aggregation interval ('1m', '5m', '15m', '30m', '1h', '4h', '1d')
        exchange: Source exchange ('binance-futures')
        symbol: Trading pair symbol in Atlas unified symbology
                (https://github.com/aperiodic-io/atlas), e.g. 'perpetual-BTC-USDT:USDT'
        start_date: Start date for the data range
        end_date: End date for the data range (inclusive)
        show_progress: Whether to show download progress bar (default: True)
        max_concurrent: Maximum concurrent downloads (default: 10)

    Returns:
        DataFrame with open, high, low, close, volume columns

    Raises:
        APIError: If the API returns an error response
        DownloadError: If a file download fails after all retries
    """
    return await _get_files_from_bucket_async(
        api_key=api_key,
        bucket="ohlcv",
        timestamp=timestamp,
        interval=interval,
        exchange=exchange,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        base_url=base_url,
        show_progress=show_progress,
        max_concurrent=max_concurrent,
    )


def get_ohlcv(
    api_key: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
) -> DataFrame:
    return run_async(
        get_ohlcv_async(
            api_key=api_key,
            timestamp=timestamp,
            interval=interval,
            exchange=exchange,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            base_url=base_url,
            show_progress=show_progress,
            max_concurrent=max_concurrent,
        )
    )


async def get_vwap_async(
    api_key: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
) -> DataFrame:
    """
    Fetch historical VWAP (volume-weighted average price) data.

    Args:
        api_key: Your Aperiodic API key
        timestamp: Timestamp source - 'exchange' for exchange-reported time,
                   'true' for actual arrival time at Aperiodic servers
        interval: Aggregation interval ('1m', '5m', '15m', '30m', '1h', '4h', '1d')
        exchange: Source exchange ('binance-futures')
        symbol: Trading pair symbol in Atlas unified symbology
                (https://github.com/aperiodic-io/atlas), e.g. 'perpetual-BTC-USDT:USDT'
        start_date: Start date for the data range
        end_date: End date for the data range (inclusive)
        show_progress: Whether to show download progress bar (default: True)
        max_concurrent: Maximum concurrent downloads (default: 10)

    Returns:
        DataFrame with VWAP columns

    Raises:
        APIError: If the API returns an error response
        DownloadError: If a file download fails after all retries
    """
    return await _get_files_from_bucket_async(
        api_key=api_key,
        bucket="vtwap",
        timestamp=timestamp,
        interval=interval,
        exchange=exchange,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        base_url=base_url,
        show_progress=show_progress,
        max_concurrent=max_concurrent,
    )


def get_vwap(
    api_key: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
) -> DataFrame:
    return run_async(
        get_vwap_async(
            api_key=api_key,
            timestamp=timestamp,
            interval=interval,
            exchange=exchange,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            base_url=base_url,
            show_progress=show_progress,
            max_concurrent=max_concurrent,
        )
    )


async def get_twap_async(
    api_key: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
) -> DataFrame:
    """
    Fetch historical TWAP (time-weighted average price) data.

    Args:
        api_key: Your Aperiodic API key
        timestamp: Timestamp source - 'exchange' for exchange-reported time,
                   'true' for actual arrival time at Aperiodic servers
        interval: Aggregation interval ('1m', '5m', '15m', '30m', '1h', '4h', '1d')
        exchange: Source exchange ('binance-futures')
        symbol: Trading pair symbol in Atlas unified symbology
                (https://github.com/aperiodic-io/atlas), e.g. 'perpetual-BTC-USDT:USDT'
        start_date: Start date for the data range
        end_date: End date for the data range (inclusive)
        show_progress: Whether to show download progress bar (default: True)
        max_concurrent: Maximum concurrent downloads (default: 10)

    Returns:
        DataFrame with TWAP columns

    Raises:
        APIError: If the API returns an error response
        DownloadError: If a file download fails after all retries
    """
    return await _get_files_from_bucket_async(
        api_key=api_key,
        bucket="vtwap",
        timestamp=timestamp,
        interval=interval,
        exchange=exchange,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        base_url=base_url,
        show_progress=show_progress,
        max_concurrent=max_concurrent,
    )


def get_twap(
    api_key: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
) -> DataFrame:
    return run_async(
        get_twap_async(
            api_key=api_key,
            timestamp=timestamp,
            interval=interval,
            exchange=exchange,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            base_url=base_url,
            show_progress=show_progress,
            max_concurrent=max_concurrent,
        )
    )
