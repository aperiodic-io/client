from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import TYPE_CHECKING

import httpx
from tqdm.auto import tqdm

from .._compat import (
    concat,
    empty_dataframe,
    filter_datetime_range,
    from_epoch_ms,
    has_column,
    sort_by,
)
from ..client import (
    download_parquet_with_retry,
    get_http_client,
    handle_api_error,
)
from ..config import (
    DEFAULT_BASE_URL,
    MAX_CONCURRENT_DOWNLOADS,
    TIMESTAMP_COL,
    get_headers,
)
from ..types import AggregateDataResponse, Interval, TimestampType

if TYPE_CHECKING:
    pass


async def _fetch_presigned_urls(
    client: httpx.AsyncClient,
    api_key: str,
    bucket: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: str,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str,
) -> AggregateDataResponse:
    """Fetch pre-signed URLs for all files in the date range."""
    url = f"{base_url}/data/{bucket}"
    params = {
        TIMESTAMP_COL: timestamp,
        "interval": interval,
        "exchange": exchange,
        "symbol": symbol,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    headers = get_headers(api_key)

    response = await client.get(url, params=params, headers=headers)
    await handle_api_error(response)

    return response.json()


async def _get_files_from_bucket_async(
    api_key: str,
    bucket: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: str,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
) -> object:
    """
    Async implementation for fetching data from any bucket.

    Fetches pre-signed URLs from the API, then downloads all parquet files
    in parallel with per-file retry logic and concatenates them into a single DataFrame.
    """
    async with get_http_client() as client:
        # Step 1: Get pre-signed URLs for all months
        response = await _fetch_presigned_urls(
            client=client,
            api_key=api_key,
            bucket=bucket,
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
            return empty_dataframe()

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

        if show_progress:
            results = []
            for coro in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc=f"Downloading {symbol} {bucket}",
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
            return empty_dataframe()

        combined = concat(dataframes)

        # Filter to exact date range if timestamp column exists
        if has_column(combined, TIMESTAMP_COL):
            combined = from_epoch_ms(combined, TIMESTAMP_COL)

            start_dt = datetime.combine(start_date, datetime.min.time())
            end_dt = datetime.combine(end_date, datetime.max.time())
            combined = filter_datetime_range(combined, start_dt, end_dt)

            combined = sort_by(combined, TIMESTAMP_COL)

        return combined
