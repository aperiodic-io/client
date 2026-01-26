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


async def _fetch_presigned_urls(
    client: httpx.AsyncClient,
    api_key: str,
    bucket: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str,
) -> AggregateDataResponse:
    """Fetch pre-signed URLs for all files in the date range."""
    url = f"{base_url}/data/{bucket}"
    params = {
        "timestamp": timestamp,
        "interval": interval,
        "exchange": exchange,
        "symbol": symbol,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    headers = {"X-API-KEY": api_key}

    response = await client.get(url, params=params, headers=headers)
    await handle_api_error(response)

    return response.json()


async def get_ohlcv_historical_async(
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
    Async implementation of get_ohlcv_historical.

    Fetches pre-signed URLs from the API, then downloads all parquet files
    in parallel with per-file retry logic and concatenates them into a single DataFrame.
    """
    async with get_http_client() as client:
        # Step 1: Get pre-signed URLs for all months
        response = await _fetch_presigned_urls(
            client=client,
            api_key=api_key,
            bucket="ohlcv",
            timestamp=timestamp,
            interval=interval,
            exchange=exchange,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            base_url=base_url,
        )

        files = response["files"]
        if not files:
            return pl.DataFrame()

        # Step 2: Download all files in parallel with per-file retry
        semaphore = asyncio.Semaphore(max_concurrent)
        tasks = [
            download_parquet_with_retry(
                client=client,
                url=file_info["url"],
                year=file_info["year"],
                month=file_info["month"],
                semaphore=semaphore,
            )
            for file_info in files
        ]

        # Use tqdm for progress if requested
        if show_progress:
            results = []
            for coro in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc=f"Downloading {symbol} OHLCV",
                unit="file",
            ):
                result = await coro
                results.append(result)
        else:
            results = await asyncio.gather(*tasks)

        # Step 3: Sort by year/month and concatenate
        results_sorted = sorted(results, key=lambda x: (x[0], x[1]))
        dataframes = [df for _, _, df in results_sorted]

        if not dataframes:
            return pl.DataFrame()

        combined = pl.concat(dataframes)

        # Filter to exact date range if timestamp column exists
        if "timestamp" in combined.columns:
            # Add datetime column from timestamp (assuming milliseconds)
            combined = combined.with_columns(
                pl.from_epoch("timestamp", time_unit="ms").alias("datetime")
            )

            # Filter to exact date range
            start_dt = datetime.combine(start_date, datetime.min.time())
            end_dt = datetime.combine(end_date, datetime.max.time())
            combined = combined.filter(
                (pl.col("datetime") >= start_dt) & (pl.col("datetime") <= end_dt)
            )

            # Sort by timestamp
            combined = combined.sort("timestamp")

        return combined


def get_ohlcv_historical(
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
    Fetch historical OHLCV (Open, High, Low, Close, Volume) data.

    Downloads candlestick data for a specific symbol and date range.
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

    Example:
        >>> from datetime import date
        >>> from unravel_data_client import get_ohlcv_historical
        >>>
        >>> df = get_ohlcv_historical(
        ...     api_key="your-api-key",
        ...     timestamp="true",
        ...     interval="1h",
        ...     exchange="binance-futures",
        ...     symbol="btcusdt",
        ...     start_date=date(2024, 1, 1),
        ...     end_date=date(2024, 3, 31),
        ... )
        >>> print(df.head())
    """
    return run_async(
        get_ohlcv_historical_async(
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



def get_ohlcv_historical_multi(
    api_key: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: Exchange,
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent_symbols: int = 3,
    max_concurrent_downloads: int = MAX_CONCURRENT_DOWNLOADS,
) -> dict[str, pl.DataFrame]:
    """
    Fetch historical OHLCV data for multiple symbols concurrently.

    Args:
        api_key: Your Unravel API key
        timestamp: Timestamp source
        interval: Aggregation interval
        exchange: Source exchange
        symbols: List of trading pair symbols
        start_date: Start date for the data range
        end_date: End date for the data range (inclusive)
        base_url: API base URL
        show_progress: Whether to show download progress bar
        max_concurrent_symbols: Maximum symbols to fetch concurrently (default: 3)
        max_concurrent_downloads: Maximum concurrent downloads per symbol (default: 10)

    Returns:
        dict[str, pl.DataFrame]: Dictionary mapping symbol to its DataFrame

    Example:
        >>> df_dict = get_ohlcv_historical_multi(
        ...     api_key="your-api-key",
        ...     timestamp="true",
        ...     interval="1h",
        ...     exchange="binance-futures",
        ...     symbols=["btcusdt", "ethusdt", "solusdt"],
        ...     start_date=date(2024, 1, 1),
        ...     end_date=date(2024, 1, 31),
        ... )
        >>> btc_df = df_dict["btcusdt"]
    """

    async def fetch_all():
        semaphore = asyncio.Semaphore(max_concurrent_symbols)

        async def fetch_with_semaphore(symbol: str) -> tuple[str, pl.DataFrame]:
            async with semaphore:
                df = await get_ohlcv_historical_async(
                    api_key=api_key,
                    timestamp=timestamp,
                    interval=interval,
                    exchange=exchange,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    base_url=base_url,
                    show_progress=show_progress,
                    max_concurrent=max_concurrent_downloads,
                )
                return symbol, df

        tasks = [fetch_with_semaphore(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return dict(results)

    return run_async(fetch_all())
