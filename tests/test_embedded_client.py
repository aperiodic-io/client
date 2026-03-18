"""Tests for the embedded Client (mocking pyfetch since it requires Pyodide)."""

from __future__ import annotations

import json
from io import BytesIO
from unittest import mock

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def _make_parquet_bytes(data: dict) -> bytes:
    """Create parquet bytes from a dict of columns."""
    table = pa.table(data)
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


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


def _patch_pyfetch(side_effect):
    """Patch pyodide.http.pyfetch and pyodide.ffi.to_js for testing outside Pyodide."""
    fake_pyfetch = mock.AsyncMock(side_effect=side_effect)
    fake_to_js = mock.MagicMock(side_effect=lambda d, **kw: d)

    modules = {
        "pyodide": mock.MagicMock(),
        "pyodide.http": mock.MagicMock(pyfetch=fake_pyfetch),
        "pyodide.ffi": mock.MagicMock(to_js=fake_to_js),
        "js": mock.MagicMock(),
    }
    return mock.patch.dict("sys.modules", modules), fake_pyfetch


COMMON_KWARGS = {
    "exchange": "binance-futures",
    "symbol": "perpetual-BTC-USDT:USDT",
    "interval": "1d",
    "start_date": "2024-01-01",
    "end_date": "2024-03-01",
}


class TestClientGetData:
    @pytest.mark.asyncio
    async def test_returns_dataframe_from_parquet_files(self):
        parquet_bytes = _make_parquet_bytes(
            {"timestamp": [1704067200000], "close": [42000.0]}
        )

        api_response = json.dumps(
            {"files": [{"url": "https://r2.example.com/file.parquet", "year": 2024, "month": 1}]}
        )

        call_count = 0

        async def fake_pyfetch(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return FakeResponse(200, api_response)
            return FakeResponse(200, parquet_bytes)

        patcher, _ = _patch_pyfetch(fake_pyfetch)
        with patcher:
            from aperiodic.embedded import Client

            client = Client(api_key="test-key")
            df = await client.get_data("ohlcv", **COMMON_KWARGS)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "close" in df.columns

    @pytest.mark.asyncio
    async def test_returns_dataframe_from_direct_data(self):
        api_response = json.dumps(
            {"data": [{"timestamp": 1704067200000, "close": 42000.0}]}
        )

        patcher, _ = _patch_pyfetch(
            lambda *a, **kw: FakeResponse(200, api_response)
        )
        with patcher:
            from aperiodic.embedded import Client

            client = Client(api_key="test-key")
            df = await client.get_data("ohlcv", **COMMON_KWARGS)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    @pytest.mark.asyncio
    async def test_raises_on_api_error(self):
        api_response = json.dumps({"error": "Unauthorized"})

        patcher, _ = _patch_pyfetch(
            lambda *a, **kw: FakeResponse(200, api_response)
        )
        with patcher:
            from aperiodic.embedded import Client

            client = Client(api_key="bad-key")
            with pytest.raises(RuntimeError, match="API error"):
                await client.get_data("ohlcv", **COMMON_KWARGS)

    @pytest.mark.asyncio
    async def test_raises_on_http_error(self):
        patcher, _ = _patch_pyfetch(
            lambda *a, **kw: FakeResponse(401, "Unauthorized")
        )
        with patcher:
            from aperiodic.embedded import Client

            client = Client(api_key="bad-key")
            with pytest.raises(RuntimeError, match="401"):
                await client.get_data("ohlcv", **COMMON_KWARGS)

    @pytest.mark.asyncio
    async def test_returns_empty_dataframe_when_no_files(self):
        api_response = json.dumps({"files": []})

        patcher, _ = _patch_pyfetch(
            lambda *a, **kw: FakeResponse(200, api_response)
        )
        with patcher:
            from aperiodic.embedded import Client

            client = Client(api_key="test-key")
            df = await client.get_data("ohlcv", **COMMON_KWARGS)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_concatenates_multiple_parquet_files(self):
        parquet1 = _make_parquet_bytes({"val": [1, 2]})
        parquet2 = _make_parquet_bytes({"val": [3, 4]})

        api_response = json.dumps(
            {
                "files": [
                    {"url": "https://r2.example.com/a.parquet", "year": 2024, "month": 1},
                    {"url": "https://r2.example.com/b.parquet", "year": 2024, "month": 2},
                ]
            }
        )

        responses = iter([
            FakeResponse(200, api_response),
            FakeResponse(200, parquet1),
            FakeResponse(200, parquet2),
        ])

        patcher, _ = _patch_pyfetch(lambda *a, **kw: next(responses))
        with patcher:
            from aperiodic.embedded import Client

            client = Client(api_key="test-key")
            df = await client.get_data("ohlcv", **COMMON_KWARGS)

        assert len(df) == 4
        assert df["val"].tolist() == [1, 2, 3, 4]


class TestClientConvenienceMethods:
    @pytest.mark.asyncio
    async def test_get_ohlcv_calls_get_data(self):
        api_response = json.dumps({"files": []})

        patcher, fake_pyfetch = _patch_pyfetch(
            lambda *a, **kw: FakeResponse(200, api_response)
        )
        with patcher:
            from aperiodic.embedded import Client

            client = Client(api_key="test-key")
            await client.get_ohlcv(**COMMON_KWARGS)

        call_url = fake_pyfetch.call_args[0][0]
        assert "/data/ohlcv" in call_url

    @pytest.mark.asyncio
    async def test_get_metrics_uses_metric_as_bucket(self):
        api_response = json.dumps({"files": []})

        patcher, fake_pyfetch = _patch_pyfetch(
            lambda *a, **kw: FakeResponse(200, api_response)
        )
        with patcher:
            from aperiodic.embedded import Client

            client = Client(api_key="test-key")
            await client.get_metrics("flow", **COMMON_KWARGS)

        call_url = fake_pyfetch.call_args[0][0]
        assert "/data/flow" in call_url

    @pytest.mark.asyncio
    async def test_get_symbols(self):
        api_response = json.dumps(
            {"symbols": ["perpetual-BTC-USDT:USDT", "perpetual-ETH-USDT:USDT"]}
        )

        patcher, _ = _patch_pyfetch(
            lambda *a, **kw: FakeResponse(200, api_response)
        )
        with patcher:
            from aperiodic.embedded import Client

            client = Client(api_key="test-key")
            result = await client.get_symbols("binance-futures")

        assert result == ["perpetual-BTC-USDT:USDT", "perpetual-ETH-USDT:USDT"]


class TestClientConfig:
    def test_default_base_url(self):
        # Can't import Client directly (pyodide not available at top level)
        # but we can import _client which only uses pyodide lazily
        from aperiodic.embedded._client import Client

        client = Client(api_key="test")
        assert client.base_url == "https://aperiodic.io"
        assert client._api_url == "https://aperiodic.io/api/v1"

    def test_custom_base_url(self):
        from aperiodic.embedded._client import Client

        client = Client(api_key="test", base_url="https://custom.example.com/")
        assert client.base_url == "https://custom.example.com"
        assert client._api_url == "https://custom.example.com/api/v1"

    def test_headers_contain_api_key(self):
        from aperiodic.embedded._client import Client

        client = Client(api_key="my-secret-key")
        assert client._headers() == {"X-API-KEY": "my-secret-key"}
