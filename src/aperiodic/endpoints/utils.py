from __future__ import annotations

import asyncio
from datetime import date, datetime
from io import BytesIO
from typing import TYPE_CHECKING

from tqdm.auto import tqdm

from .._compat import get_backend_module
from ..client import AperiodicDataError, download_parquet_bytes, fetch_json
from ..config import (
    DEFAULT_BASE_URL,
    DEMO_API_KEY,
    MAX_CONCURRENT_DOWNLOADS,
    TIMESTAMP_COL,
    get_headers,
)
from ..types import (
    AggregateDataResponse,
    FileInfo,
    Interval,
    OutputFormat,
    TimestampType,
)

if TYPE_CHECKING:
    pass


def _resolve_api_key(api_key: str | None, preview: bool) -> str:
    """Resolve the effective API key for a data request.

    Preview data is served against a shared demo key, so ``api_key`` may be
    omitted when ``preview=True``. A key is always required otherwise.
    """
    if api_key:
        return api_key
    if preview:
        return DEMO_API_KEY
    raise AperiodicDataError(
        "api_key is required. Pass your Aperiodic API key, or set preview=True "
        "to query the free preview datasets with the shared demo key."
    )


async def _fetch_presigned_urls(
    api_key: str,
    bucket: str,
    timestamp: TimestampType,
    interval: Interval,
    exchange: str,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str,
    preview: bool = False,
) -> AggregateDataResponse:
    """Fetch pre-signed URLs for all files in the date range."""
    if preview:
        url = f"{base_url}/data/preview/{bucket}"
    else:
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

    return await fetch_json(url, params=params, headers=headers)


async def _download_file(
    file_info: FileInfo,
    headers: dict[str, str],
    semaphore: asyncio.Semaphore,
) -> tuple[tuple[int, int, int], bytes]:
    """Download one parquet file, tagged with its chronological sort key.

    Files are downloaded concurrently and complete out of order, so each one
    carries the key it must be concatenated by. Monthly files have no ``day``
    and sort ahead of the same month's daily files — which is also their
    chronological order, since a month is only served as a monthly file for the
    days before the daily cutover.
    """
    year, month, raw = await download_parquet_bytes(
        file_info["url"],
        headers,
        year=file_info["year"],
        month=file_info["month"],
        semaphore=semaphore,
    )
    return (year, month, file_info.get("day", 0)), raw


async def _get_files_from_bucket_async(
    api_key: str | None,
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
    output: OutputFormat = "polars",
    preview: bool = False,
) -> object:
    """
    Async implementation for fetching data from any bucket.

    Fetches pre-signed URLs from the API, then downloads all parquet files
    in parallel with per-file retry logic and concatenates them into a single DataFrame.
    """
    api_key = _resolve_api_key(api_key, preview)
    backend = get_backend_module(output)

    # Step 1: Get pre-signed URLs for every file covering the range
    response = await _fetch_presigned_urls(
        api_key=api_key,
        bucket=bucket,
        timestamp=timestamp,
        interval=interval,
        exchange=exchange,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        base_url=base_url,
        preview=preview,
    )

    files = response["files"]
    if not files:
        return backend.empty_dataframe()

    # Step 2: Download all files in parallel with per-file retry
    headers = get_headers(api_key)
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [_download_file(file_info, headers, semaphore) for file_info in files]

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

    # Step 3: Sort chronologically, read parquet, and concatenate
    results_sorted = sorted(results, key=lambda result: result[0])
    dataframes = [backend.read_parquet(BytesIO(raw)) for _, raw in results_sorted]

    if not dataframes:
        return backend.empty_dataframe()

    combined = backend.concat(dataframes)

    # Filter to exact date range if timestamp column exists
    if backend.has_column(combined, TIMESTAMP_COL):
        combined = backend.from_epoch_ms(combined, TIMESTAMP_COL)

        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date, datetime.max.time())
        combined = backend.filter_datetime_range(combined, start_dt, end_dt)

        combined = backend.sort_by(combined, TIMESTAMP_COL)

    return combined
