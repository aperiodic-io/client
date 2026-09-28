"""HTTP transport using Pyodide's pyfetch (for WASM/marimo environments).

In Pyodide, httpx is not available. pyfetch is the built-in async HTTP client.
Parquet files are downloaded directly from R2 presigned URLs — CORS headers
on the R2 buckets allow cross-origin GET requests from the browser.
"""

from __future__ import annotations

import asyncio
import json
import sys
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import quote

from ..config import MAX_RETRIES, RETRY_BACKOFF_BASE, RETRYABLE_STATUS_CODES
from ._retry import retry_delay

if TYPE_CHECKING:
    from collections.abc import Coroutine

T = TypeVar("T")


def _has_pyodide_ffi() -> bool:
    """Detect ``pyodide.ffi`` without importing it or crashing on test mocks.

    ``importlib.util.find_spec("pyodide.ffi")`` is the project-wide
    convention for "is this optional dep installed?" (see ``_compat.py``:
    ``HAS_POLARS`` / ``HAS_PYARROW``). But our tests use
    ``mock.patch.dict("sys.modules", {"pyodide.ffi": MagicMock()})`` to
    mock Pyodide APIs, and ``find_spec`` trips over the MagicMock's lack
    of a real ``__spec__`` with ``ValueError: pyodide.ffi.__spec__ is not
    set``. So: probe ``sys.modules`` first (covers both real Pyodide
    imports and the test-mock case), and only fall through to
    ``find_spec`` when the module isn't already in ``sys.modules``.
    """
    if "pyodide.ffi" in sys.modules:
        return True
    return find_spec("pyodide.ffi") is not None


def run_async(coro: Coroutine[None, None, T]) -> T:
    """Run an async coroutine synchronously in a Pyodide/WASM environment.

    Pyodide's event loop is already running when our code executes (the
    browser owns the event loop), so we can't call ``loop.run_until_complete``
    to block on a coroutine — it returns a ``Task`` without actually running
    it, and the caller then ends up holding a pending future instead of the
    awaited result.

    We rely on ``pyodide.ffi.run_sync`` (Pyodide ≥ 0.26) which uses
    JavaScript Promise Integration / stack switching to block the current
    Python frame until the coroutine is done — the same mechanism marimo
    itself uses to offer a synchronous UI. This is enabled in marimo's WASM
    runtime, so users can keep calling ``get_ohlcv(...)`` etc. without
    needing top-level ``await``.

    If ``pyodide.ffi`` isn't available (older Pyodide, or non-Pyodide
    runtime), raise a clear error pointing users to the ``_async``
    counterparts rather than silently returning a ``Task``.
    """
    if not _has_pyodide_ffi():
        raise RuntimeError(
            "Synchronous Aperiodic client calls require Pyodide ≥ 0.26 with "
            "JavaScript Promise Integration (stack switching). Use the "
            "_async variants (e.g. `await get_ohlcv_async(...)`) in this "
            "runtime."
        )

    from pyodide.ffi import run_sync  # type: ignore[import-not-found]

    return run_sync(coro)


class APIError(Exception):
    """Exception raised when the API returns an error.

    ``code`` is the machine-readable reason some responses carry, e.g.
    ``"raw_not_in_plan"`` or ``"range_too_long"`` on raw data requests.
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        details: list[str] | None = None,
        code: str | None = None,
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or []
        self.code = code
        super().__init__(f"{status_code}: {message}")


class DownloadError(Exception):
    """Exception raised when a file download fails after all retries.

    ``day`` is set for daily files. ``status_code`` is set when the storage
    answered with an HTTP error; a 403 there means the presigned URL expired or
    was refused, and is not retried.
    """

    def __init__(
        self,
        year: int,
        month: int,
        original_error: Exception,
        *,
        day: int | None = None,
        status_code: int | None = None,
    ):
        self.year = year
        self.month = month
        self.day = day
        self.status_code = status_code
        self.original_error = original_error
        period = f"{year}-{month:02d}" + (f"-{day:02d}" if day else "")
        super().__init__(f"Failed to download data for {period}: {original_error}")


def _to_js_headers(headers: dict[str, str]) -> Any:
    """Convert Python dict to JS-compatible headers for pyfetch."""
    import js  # type: ignore[import-not-found]
    from pyodide.ffi import to_js  # type: ignore[import-not-found]

    return to_js(headers, dict_converter=js.Object.fromEntries)


async def fetch_json(
    url: str,
    params: dict[str, str],
    headers: dict[str, str],
    *,
    max_retries: int = MAX_RETRIES,
    backoff_base: float = RETRY_BACKOFF_BASE,
) -> Any:
    """Make a GET request and return parsed JSON.

    Transient failures — network errors, rate limits, upstream 5xx — are
    retried with exponential backoff. Every other response is handed to the
    caller as data or an ``APIError`` on the first attempt.
    """
    full_url = _build_url(url, params)

    for attempt in range(max_retries):
        try:
            resp = await _pyfetch_get(full_url, headers)
        except Exception:  # a failed fetch surfaces as an untyped JsException
            await asyncio.sleep(retry_delay(attempt, backoff_base))
            continue

        if resp.status not in RETRYABLE_STATUS_CODES:
            return await _parse_json_response(resp)

        await asyncio.sleep(retry_delay(attempt, backoff_base, _retry_after(resp)))

    resp = await _pyfetch_get(full_url, headers)
    return await _parse_json_response(resp)


async def _pyfetch_get(url: str, headers: dict[str, str]) -> Any:
    from pyodide.http import pyfetch  # type: ignore[import-not-found]

    return await pyfetch(url, headers=_to_js_headers(headers))


async def _parse_json_response(resp: Any) -> Any:
    if resp.status != 200:
        await _handle_pyfetch_error(resp)

    return json.loads(await resp.string())


def _retry_after(resp: Any) -> str | None:
    """Read the ``Retry-After`` header off a pyfetch response, if it has one.

    Pyodide exposes ``FetchResponse.headers`` as a plain dict with lower-cased
    keys; anything else (older Pyodide, test doubles) is treated as absent.
    """
    headers = getattr(resp, "headers", None)
    if not isinstance(headers, dict):
        return None

    return headers.get("retry-after")


async def download_parquet_bytes(
    url: str,
    headers: dict[str, str],
    *,
    year: int,
    month: int,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
    backoff_base: float = 1.0,
    day: int | None = None,
    client: Any = None,
) -> tuple[int, int, bytes]:
    """Download a parquet file directly from a presigned R2 URL.

    Presigned URLs carry auth in their query parameters (X-Amz-*), so no
    additional headers are required. CORS is configured on the R2 buckets to
    allow GET requests from the browser. ``client`` exists for parity with the
    httpx transport and is ignored: the browser pools connections itself. A 403
    is raised at once with ``status_code=403`` so the caller can request a
    fresh URL.

    Returns:
        Tuple of (year, month, raw_bytes)
    """
    from pyodide.http import pyfetch  # type: ignore[import-not-found]

    async with semaphore:
        last_exception: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                resp = await pyfetch(url, method="GET")
                if resp.status == 403:
                    raise DownloadError(
                        year,
                        month,
                        RuntimeError("403 Forbidden (URL expired or refused)"),
                        day=day,
                        status_code=403,
                    )
                if resp.status != 200:
                    text = await resp.string()
                    raise RuntimeError(f"Download failed ({resp.status}): {text}")
                raw = await resp.bytes()
                return year, month, raw

            except DownloadError:
                raise
            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    delay = backoff_base * (2**attempt)
                    await asyncio.sleep(delay)

        raise DownloadError(
            year, month, last_exception or Exception("Unknown error"), day=day
        )


def _build_url(base: str, params: dict[str, str]) -> str:
    """Append query parameters to a URL."""
    if not params:
        return base
    qs = "&".join(f"{quote(k)}={quote(str(v))}" for k, v in params.items())
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{qs}"


async def _handle_pyfetch_error(resp: Any) -> None:
    """Handle non-200 pyfetch responses by raising APIError."""
    if resp.status == 401:
        raise APIError(message="Authorization Required", status_code=resp.status)

    text = await resp.string()
    try:
        error_data = json.loads(text)
        msg = error_data.get("error", text)
        details = error_data.get("details")
        code = error_data.get("code")
    except (ValueError, KeyError, AttributeError):
        msg = text
        details = None
        code = None

    raise APIError(
        message=msg,
        status_code=resp.status,
        details=details,
        code=code,
    )
