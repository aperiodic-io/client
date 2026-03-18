"""Async HTTP transport using Pyodide's pyfetch for WASM environments."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote


async def fetch_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
) -> Any:
    """Make a GET request and return parsed JSON."""
    from pyodide.http import pyfetch  # type: ignore[import-not-found]

    full_url = _build_url(url, params)
    js_headers = _to_js_headers(headers or {})
    resp = await pyfetch(full_url, headers=js_headers)

    if resp.status != 200:
        text = await resp.string()
        try:
            error_data = json.loads(text)
            msg = error_data.get("error", text)
        except (ValueError, KeyError):
            msg = text
        raise RuntimeError(
            f"API request failed ({resp.status}): {msg}. "
            "If this is a CORS error, ensure you're using the proxy endpoint."
        )

    return json.loads(await resp.string())


async def fetch_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
) -> bytes:
    """Make a GET request and return raw bytes."""
    from pyodide.http import pyfetch  # type: ignore[import-not-found]

    js_headers = _to_js_headers(headers or {})
    resp = await pyfetch(url, headers=js_headers)

    if resp.status != 200:
        text = await resp.string()
        raise RuntimeError(
            f"Download failed ({resp.status}): {text}. "
            "If this is a CORS error, ensure you're using the proxy endpoint."
        )

    return await resp.bytes()


def _build_url(base: str, params: dict[str, str] | None) -> str:
    """Append query parameters to a URL."""
    if not params:
        return base
    qs = "&".join(f"{quote(k)}={quote(str(v))}" for k, v in params.items())
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{qs}"


def _to_js_headers(headers: dict[str, str]) -> Any:
    """Convert Python dict to JS-compatible headers for pyfetch."""
    import js  # type: ignore[import-not-found]
    from pyodide.ffi import to_js  # type: ignore[import-not-found]

    return to_js(headers, dict_converter=js.Object.fromEntries)
