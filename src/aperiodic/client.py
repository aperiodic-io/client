"""Client utilities — transport backend selection and shared exceptions.

Selects httpx (CPython) or pyfetch (Pyodide/WASM) automatically.
"""

from __future__ import annotations

import sys


class AperiodicDataError(Exception):
    """Base exception for Aperiodic Data Client errors."""


# --- HTTP transport backend detection ---
# In Pyodide/WASM (emscripten), httpx may be importable but cannot make
# real network requests (no sockets). Always use pyfetch there.

_IS_WASM = sys.platform == "emscripten"

if not _IS_WASM:
    try:
        import httpx  # noqa: F401

        HAS_HTTPX = True
    except ImportError:
        HAS_HTTPX = False
else:
    HAS_HTTPX = False

if HAS_HTTPX:
    from ._backends._httpx_transport import (
        APIError,
        DownloadError,
        download_parquet_bytes,
        fetch_json,
        run_async,
    )
else:
    from ._backends._pyfetch_transport import (
        APIError,
        DownloadError,
        download_parquet_bytes,
        fetch_json,
        run_async,
    )

__all__ = [
    "HAS_HTTPX",
    "APIError",
    "AperiodicDataError",
    "DownloadError",
    "download_parquet_bytes",
    "fetch_json",
    "run_async",
]
