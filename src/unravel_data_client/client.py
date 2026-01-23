"""
Unravel Data Client - Client library for Unravel aggregate market data API.

This module provides async functions to fetch OHLCV and other aggregate data
from the Unravel API with parallel downloads for optimal performance.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from io import BytesIO
from typing import TYPE_CHECKING, TypeVar

import httpx
import polars as pl
from tqdm.auto import tqdm

from .config import DEFAULT_BASE_URL, MAX_CONCURRENT_DOWNLOADS
from .types import AggregateDataResponse, ArrivalTime, Exchange, Period

if TYPE_CHECKING:
    from collections.abc import Coroutine, Sequence

T = TypeVar("T")


def _run_async(coro: Coroutine[None, None, T]) -> T:
    """Run an async coroutine, handling both regular Python and Jupyter environments."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop, use asyncio.run()
        return asyncio.run(coro)

    # Running in an existing event loop (e.g., Jupyter)
    import nest_asyncio

    nest_asyncio.apply()
    return loop.run_until_complete(coro)


class UnravelDataError(Exception):
    """Base exception for Unravel Data Client errors."""


class APIError(UnravelDataError):
    """Exception raised when the API returns an error."""

    def __init__(
        self, message: str, status_code: int, details: list[str] | None = None
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or []
        super().__init__(f"{status_code}: {message}")


class DownloadError(UnravelDataError):
    """Exception raised when a file download fails."""

    def __init__(self, year: int, month: int, original_error: Exception):
        self.year = year
        self.month = month
        self.original_error = original_error
        super().__init__(
            f"Failed to download data for {year}-{month:02d}: {original_error}"
        )


async def _fetch_presigned_urls(
    client: httpx.AsyncClient,
    api_key: str,
    bucket: str,
    arrival_time: ArrivalTime,
    period: Period,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str,
) -> AggregateDataResponse:
    """Fetch pre-signed URLs for all files in the date range."""
    url = f"{base_url}/data/{bucket}"
    params = {
        "arrival_time": arrival_time,
        "period": period,
        "exchange": exchange,
        "symbol": symbol,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    headers = {"X-API-KEY": api_key}

    response = await client.get(url, params=params, headers=headers)

    if response.status_code != 200:
        try:
            error_data = response.json()
            raise APIError(
                message=error_data.get("error", "Unknown error"),
                status_code=response.status_code,
                details=error_data.get("details"),
            )
        except (ValueError, KeyError):
            raise APIError(
                message=response.text or "Unknown error",
                status_code=response.status_code,
            )

    return response.json()


async def _download_parquet(
    client: httpx.AsyncClient,
    url: str,
    year: int,
    month: int,
    semaphore: asyncio.Semaphore,
) -> tuple[int, int, pl.DataFrame]:
    """Download a single parquet file and return as DataFrame."""
    async with semaphore:
        try:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()

            # Read parquet from bytes using Polars
            buffer = BytesIO(response.content)
            df = pl.read_parquet(buffer)

            return year, month, df
        except Exception as e:
            raise DownloadError(year, month, e) from e


async def _get_ohlcv_historical_async(
    api_key: str,
    arrival_time: ArrivalTime,
    period: Period,
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
    in parallel and concatenates them into a single DataFrame.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        # Step 1: Get pre-signed URLs for all months
        response = await _fetch_presigned_urls(
            client=client,
            api_key=api_key,
            bucket="ohlcv",
            arrival_time=arrival_time,
            period=period,
            exchange=exchange,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            base_url=base_url,
        )

        files = response["files"]
        if not files:
            return pl.DataFrame()

        # Step 2: Download all files in parallel
        semaphore = asyncio.Semaphore(max_concurrent)
        tasks = [
            _download_parquet(
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
    arrival_time: ArrivalTime,
    period: Period,
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
    Files are downloaded in parallel for optimal performance.

    Args:
        api_key: Your Unravel API key
        arrival_time: Timestamp source - 'exchange' for exchange-reported time,
                     'true' for actual arrival time at Unravel servers
        period: Aggregation period ('1m', '5m', '15m', '30m', '1h', '4h', '1d')
        exchange: Source exchange ('binance-futures', 'binance')
        symbol: Trading pair symbol (e.g., 'btcusdt', 'ethusdt')
        start_date: Start date for the data range
        end_date: End date for the data range (inclusive)
        base_url: API base URL (default: https://unravel.finance/api/v1)
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
        DownloadError: If a file download fails

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
        >>> print(df.head())
    """
    return _run_async(
        _get_ohlcv_historical_async(
            api_key=api_key,
            arrival_time=arrival_time,
            period=period,
            exchange=exchange,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            base_url=base_url,
            show_progress=show_progress,
            max_concurrent=max_concurrent,
        )
    )


async def get_ohlcv_historical_async(
    api_key: str,
    arrival_time: ArrivalTime,
    period: Period,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
) -> pl.DataFrame:
    """
    Async version of get_ohlcv_historical.

    Use this when you're already in an async context or want to fetch
    data for multiple symbols concurrently.

    See get_ohlcv_historical for full documentation.
    """
    return await _get_ohlcv_historical_async(
        api_key=api_key,
        arrival_time=arrival_time,
        period=period,
        exchange=exchange,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        base_url=base_url,
        show_progress=show_progress,
        max_concurrent=max_concurrent,
    )


def get_ohlcv_historical_multi(
    api_key: str,
    arrival_time: ArrivalTime,
    period: Period,
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
        arrival_time: Timestamp source
        period: Aggregation period
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
        ...     arrival_time="true",
        ...     period="1h",
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
                df = await _get_ohlcv_historical_async(
                    api_key=api_key,
                    arrival_time=arrival_time,
                    period=period,
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

    return _run_async(fetch_all())
