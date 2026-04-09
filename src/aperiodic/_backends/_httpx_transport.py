"""HTTP transport using httpx (default for CPython environments)."""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING, Any, TypeVar

import httpx

from ..config import DEFAULT_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF_BASE

if TYPE_CHECKING:
    from collections.abc import Coroutine

T = TypeVar("T")


def run_async(coro: Coroutine[None, None, T]) -> T:
    """Run an async coroutine, handling both regular Python and Jupyter environments.

    Detects whether there's already a running event loop (e.g., in Jupyter or
    pytest-asyncio) and runs the coroutine in a thread-pool worker with its own
    event loop, avoiding any interaction with the outer loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    # Running inside an existing event loop — spin up a thread with its own loop.
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


class APIError(Exception):
    """Exception raised when the API returns an error."""

    def __init__(
        self, message: str, status_code: int, details: list[str] | None = None
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or []
        super().__init__(f"{status_code}: {message}")


class DownloadError(Exception):
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


async def fetch_json(
    url: str,
    params: dict[str, str],
    headers: dict[str, str],
) -> Any:
    """Make a GET request and return parsed JSON."""
    async with get_http_client() as client:
        response = await client.get(url, params=params, headers=headers)
        await _handle_api_error(response)
        return response.json()


async def download_parquet_bytes(
    url: str,
    headers: dict[str, str],
    *,
    year: int,
    month: int,
    semaphore: asyncio.Semaphore,
    max_retries: int = MAX_RETRIES,
    backoff_base: float = RETRY_BACKOFF_BASE,
) -> tuple[int, int, bytes]:
    """Download a parquet file with retry logic.

    Returns:
        Tuple of (year, month, raw_bytes)
    """
    async with semaphore:
        last_exception: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                async with get_http_client() as client:
                    response = await client.get(url, follow_redirects=True)
                    response.raise_for_status()
                    return year, month, response.content

            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    delay = backoff_base * (2**attempt) + random.uniform(0, 1)
                    await asyncio.sleep(delay)

        raise DownloadError(year, month, last_exception or Exception("Unknown error"))


async def _handle_api_error(response: httpx.Response) -> None:
    if response.status_code == 401:
        raise APIError(message="Authorization Required", status_code=response.status_code)
    if response.status_code == 403:
        raise APIError(message="Forbidden", status_code=response.status_code)
    if response.status_code == 404:
        raise APIError(message="Not Found", status_code=response.status_code)
    if response.status_code == 429:
        raise APIError(message="Too Many Requests", status_code=response.status_code)
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
