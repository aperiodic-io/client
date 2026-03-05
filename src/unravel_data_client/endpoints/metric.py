from __future__ import annotations

from datetime import date

import polars as pl

from ..client import run_async
from ..config import DEFAULT_BASE_URL, MAX_CONCURRENT_DOWNLOADS
from ..types import Exchange, Interval, TimestampType, TradeMetric
from .utils import _get_files_from_bucket_async


async def get_trade_metrics_async(
    api_key: str,
    metric: TradeMetric,
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
    Fetch historical trade metrics data.

    Available metrics:
        - 'vtwap': Volume-weighted and time-weighted average prices
        - 'flow': Taker buy/sell volume, count, ratios, size-segmented order flow
        - 'trade_size': Size-segmented order volume/count and distribution statistics
        - 'impact': Market impact metrics (Amihud, Kyle lambda, directional impact)
        - 'range': Price high/low range and distribution statistics
        - 'updownticks': Uptick and downtick count, volume, ratios and percentages

    Args:
        api_key: Your Unravel API key
        metric: Which trade metric to fetch
        timestamp: Timestamp source - 'exchange' or 'true'
        interval: Aggregation interval ('1m', '5m', '15m', '30m', '1h', '4h', '1d')
        exchange: Source exchange ('binance-futures', 'binance', 'okx-perps')
        symbol: Trading pair symbol (e.g., 'btcusdt', 'ethusdt')
        start_date: Start date for the data range
        end_date: End date for the data range (inclusive)
        show_progress: Whether to show download progress bar (default: True)
        max_concurrent: Maximum concurrent downloads (default: 10)

    Returns:
        pl.DataFrame with columns specific to the requested metric

    Raises:
        APIError: If the API returns an error response
        DownloadError: If a file download fails after all retries
    """
    return await _get_files_from_bucket_async(
        api_key=api_key,
        bucket=metric,
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


def get_trade_metrics(
    api_key: str,
    metric: TradeMetric,
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
        get_trade_metrics_async(
            api_key=api_key,
            metric=metric,
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
