from __future__ import annotations

import asyncio
import random
from io import BytesIO
from typing import TYPE_CHECKING, TypeVar

import httpx
import polars as pl

from .config import DEFAULT_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF_BASE

if TYPE_CHECKING:
    from collections.abc import Coroutine

T = TypeVar("T")


def run_async(coro: Coroutine[None, None, T]) -> T:
    """
    Run an async coroutine, handling both regular Python and Jupyter environments.

    This function detects whether there's already a running event loop (e.g., in Jupyter)
    and uses nest_asyncio to allow nested event loops if needed.

    Args:
        coro: The coroutine to execute

    Returns:
        The result of the coroutine
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop, use asyncio.run()
        return asyncio.run(coro)

    # Running in an existing event loop (e.g., Jupyter)
    import nest_asyncio

    nest_asyncio.apply()
    return loop.run_until_complete(coro)


class AperiodicDataError(Exception):
    """Base exception for Aperiodic Data Client errors."""


class APIError(AperiodicDataError):
    """Exception raised when the API returns an error."""

    def __init__(
        self, message: str, status_code: int, details: list[str] | None = None
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or []
        super().__init__(f"{status_code}: {message}")


class DownloadError(AperiodicDataError):
    """Exception raised when a file download fails after all retries."""

    def __init__(self, year: int, month: int, original_error: Exception):
        self.year = year
        self.month = month
        self.original_error = original_error
        super().__init__(
            f"Failed to download data for {year}-{month:02d}: {original_error}"
        )


def get_http_client(timeout: float = DEFAULT_TIMEOUT) -> httpx.AsyncClient:
    """Create a configured HTTP client."""
    return httpx.AsyncClient(timeout=httpx.Timeout(timeout))


async def download_parquet_with_retry(
    client: httpx.AsyncClient,
    url: str,
    year: int,
    month: int,
    semaphore: asyncio.Semaphore,
    max_retries: int = MAX_RETRIES,
    backoff_base: float = RETRY_BACKOFF_BASE,
) -> tuple[int, int, pl.DataFrame]:
    """
    Download a single parquet file with retry logic and return as DataFrame.

    Each download is retried independently with exponential backoff.

    Args:
        client: The HTTP client to use
        url: The pre-signed URL to download from
        year: The year of the data file (for error reporting)
        month: The month of the data file (for error reporting)
        semaphore: Semaphore to limit concurrent downloads
        max_retries: Maximum number of retry attempts
        backoff_base: Base for exponential backoff calculation

    Returns:
        Tuple of (year, month, DataFrame)

    Raises:
        DownloadError: If download fails after all retries
    """
    async with semaphore:
        last_exception: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                response = await client.get(url, follow_redirects=True)
                response.raise_for_status()

                buffer = BytesIO(response.content)
                df = pl.read_parquet(buffer)

                return year, month, df

            except Exception as e:
                last_exception = e

                if attempt < max_retries:
                    delay = backoff_base * (2**attempt) + random.uniform(0, 1)
                    await asyncio.sleep(delay)

        raise DownloadError(year, month, last_exception or Exception("Unknown error"))


async def handle_api_error(response: httpx.Response) -> None:
    if response.status_code == 401:
        raise APIError(
            message="Unauthorized",
            status_code=response.status_code,
        )
    if response.status_code == 403:
        raise APIError(
            message="Forbidden",
            status_code=response.status_code,
        )
    if response.status_code == 404:
        raise APIError(
            message="Not Found",
            status_code=response.status_code,
        )
    if response.status_code == 429:
        raise APIError(
            message="Too Many Requests",
            status_code=response.status_code,
        )
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
            ) from None
