from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import TYPE_CHECKING

import httpx
import polars as pl
from tqdm.auto import tqdm

from ..client import (
    download_parquet_with_retry,
    get_http_client,
    handle_api_error,
    run_async,
)
from ..config import DEFAULT_BASE_URL, MAX_CONCURRENT_DOWNLOADS
from ..types import AggregateDataResponse, TimestampType, Exchange, Interval

if TYPE_CHECKING:
    from collections.abc import Sequence

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
) -> pl.DataFrame:
    """
    Fetch historical OHLCV data.

    Downloads metric data for a specific symbol and date range.
    Files are downloaded in parallel with per-file retry logic for optimal
    performance and reliability.

    Args:
        api_key: Your Unravel API key
        timestamp: Timestamp source - 'exchange' for exchange-reported time,
                     'true' for actual arrival time at Unravel servers
        interval: Aggregation interval ('1m', '5m', '15m', '30m', '1h', '4h', '1d')
        exchange: Source exchange ('binance-futures', 'binance')
        symbol: Trading pair symbol (e.g., 'btcusdt', 'ethusdt')
        start_date: Start date for the data range
        end_date: End date for the data range (inclusive)
        show_progress: Whether to show download progress bar (default: True)
        max_concurrent: Maximum concurrent downloads (default: 10)

    Returns:
        pl.DataFrame: DataFrame with OHLCV data containing columns:
            - timestamp: Unix timestamp in milliseconds
            - datetime: Parsed datetime (added by client)
            - open: Opening price
            - high: Highest price
            - low: Lowest price
            - close: Closing price
            - volume: Trading volume

    Raises:
        APIError: If the API returns an error response
        DownloadError: If a file download fails after all retries

    """
    return await _get_files_from_bucket_async(
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
) -> pl.DataFrame:
    return run_async(
        get_metric_async(
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


