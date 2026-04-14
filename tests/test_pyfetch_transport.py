"""Tests for the pyfetch transport backend (mocking Pyodide APIs)."""

from __future__ import annotations

import json
from unittest import mock

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


class FakeResponse:
    """Minimal mock for pyfetch response."""

    def __init__(self, status: int, body: bytes | str):
        self.status = status
        self._body = body

    async def string(self) -> str:
        if isinstance(self._body, bytes):
            return self._body.decode()
        return self._body

    async def bytes(self) -> bytes:
        if isinstance(self._body, str):
            return self._body.encode()
        return self._body


def _patch_pyodide(pyfetch_side_effect):
    """Patch pyodide modules for testing outside Pyodide."""
    fake_pyfetch = mock.AsyncMock(side_effect=pyfetch_side_effect)
    fake_to_js = mock.MagicMock(side_effect=lambda d, **kw: d)

    modules = {
        "pyodide": mock.MagicMock(),
        "pyodide.http": mock.MagicMock(pyfetch=fake_pyfetch),
        "pyodide.ffi": mock.MagicMock(to_js=fake_to_js),
        "js": mock.MagicMock(),
    }
    return mock.patch.dict("sys.modules", modules), fake_pyfetch


class TestFetchJson:
    @pytest.mark.asyncio
    async def test_returns_parsed_json(self):
        body = json.dumps({"files": [{"url": "https://example.com/f.parquet"}]})
        patcher, _ = _patch_pyodide(lambda *a, **kw: FakeResponse(200, body))
        with patcher:
            from aperiodic._backends._pyfetch_transport import fetch_json

            result = await fetch_json(
                "https://api.example.com/data/ohlcv",
                params={"exchange": "binance-futures"},
                headers={"X-API-KEY": "test"},
            )
        assert result == {"files": [{"url": "https://example.com/f.parquet"}]}

    @pytest.mark.asyncio
    async def test_raises_api_error_on_401(self):
        patcher, _ = _patch_pyodide(
            lambda *a, **kw: FakeResponse(401, '{"error": "Unauthorized"}')
        )
        with patcher:
            from aperiodic._backends._pyfetch_transport import APIError, fetch_json

            with pytest.raises(APIError) as exc_info:
                await fetch_json(
                    "https://api.example.com/data/ohlcv",
                    params={},
                    headers={"X-API-KEY": "bad"},
                )
            assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_builds_url_with_params(self):
        body = json.dumps({"ok": True})
        patcher, fake_pyfetch = _patch_pyodide(
            lambda *a, **kw: FakeResponse(200, body)
        )
        with patcher:
            from aperiodic._backends._pyfetch_transport import fetch_json

            await fetch_json(
                "https://api.example.com/data",
                params={"exchange": "binance", "interval": "1d"},
                headers={},
            )

        call_url = fake_pyfetch.call_args[0][0]
        assert "exchange=binance" in call_url
        assert "interval=1d" in call_url


class TestDownloadParquetBytes:
    @pytest.mark.asyncio
    async def test_returns_bytes_directly(self):
        import asyncio

        raw_bytes = b"parquet-data"
        presigned_url = "https://ohlcv.aperiodic.io/file.parquet?X-Amz-Signature=abc"
        patcher, fake_pyfetch = _patch_pyodide(
            lambda *a, **kw: FakeResponse(200, raw_bytes)
        )
        with patcher:
            from aperiodic._backends._pyfetch_transport import (
                download_parquet_bytes,
            )

            year, month, data = await download_parquet_bytes(
                presigned_url,
                {},
                year=2024,
                month=1,
                semaphore=asyncio.Semaphore(10),
            )

        assert year == 2024
        assert month == 1
        assert data == raw_bytes
        # Should fetch the presigned URL directly — no proxy hop
        call_url = fake_pyfetch.call_args[0][0]
        assert call_url == presigned_url

    @pytest.mark.asyncio
    async def test_raises_download_error_after_retries(self):
        import asyncio

        patcher, _ = _patch_pyodide(
            lambda *a, **kw: FakeResponse(500, "Server Error")
        )
        with patcher:
            from aperiodic._backends._pyfetch_transport import (
                DownloadError,
                download_parquet_bytes,
            )

            with pytest.raises(DownloadError):
                await download_parquet_bytes(
                    "https://ohlcv.aperiodic.io/file.parquet?X-Amz-Signature=abc",
                    {},
                    year=2024,
                    month=1,
                    semaphore=asyncio.Semaphore(10),
                    max_retries=0,
                )
