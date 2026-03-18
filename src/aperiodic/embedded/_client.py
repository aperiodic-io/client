"""Embedded async client for Pyodide/WASM environments.

Uses pyfetch for HTTP and pyarrow+pandas for data handling.
All methods are async since pyfetch is async-only.
"""

from __future__ import annotations

from io import BytesIO
from typing import Any
from urllib.parse import quote

import pandas as pd
import pyarrow.parquet as pq

from ._transport import fetch_bytes, fetch_json

DEFAULT_BASE_URL = "https://aperiodic.io"
API_PREFIX = "/api/v1"


class Client:
    """Async client for the Aperiodic API, designed for Pyodide/WASM environments.

    Uses pyfetch (Pyodide's built-in HTTP) and returns pandas DataFrames.

    Example::

        from aperiodic.embedded import Client

        client = Client(api_key="your-key")
        df = await client.get_ohlcv(
            exchange="binance-futures",
            symbol="perpetual-BTC-USDT:USDT",
            interval="1d",
            start_date="2024-01-01",
            end_date="2024-03-01",
        )
    """

    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self._api_url = f"{self.base_url}{API_PREFIX}"

    def _headers(self) -> dict[str, str]:
        return {"X-API-KEY": self.api_key}

    async def get_data(
        self,
        bucket: str,
        *,
        exchange: str,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str,
        timestamp: str = "exchange",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Fetch data from any bucket, download parquets, and return a DataFrame.

        Args:
            bucket: Data bucket (e.g. 'ohlcv', 'vwap', 'flow', 'basis').
            exchange: Exchange name (e.g. 'binance-futures').
            symbol: Trading pair in Atlas symbology (e.g. 'perpetual-BTC-USDT:USDT').
            interval: Aggregation interval ('1m', '5m', '15m', '30m', '1h', '4h', '1d').
            start_date: Start date as ISO string (e.g. '2024-01-01').
            end_date: End date as ISO string (e.g. '2024-03-01').
            timestamp: Timestamp source - 'exchange' or 'true'.
            **kwargs: Additional query parameters.

        Returns:
            pandas DataFrame with the requested data.
        """
        params = {
            "timestamp": timestamp,
            "interval": interval,
            "exchange": exchange,
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            **kwargs,
        }

        url = f"{self._api_url}/data/{bucket}"
        response = await fetch_json(url, headers=self._headers(), params=params)

        if "error" in response:
            raise RuntimeError(f"API error: {response['error']}")

        if "data" in response:
            return pd.DataFrame(response["data"])

        files = response.get("files", [])
        if not files:
            return pd.DataFrame()

        dataframes = []
        for file_info in files:
            presigned_url = file_info["url"]
            df = await self._download_parquet(presigned_url)
            dataframes.append(df)

        if not dataframes:
            return pd.DataFrame()

        return pd.concat(dataframes, ignore_index=True)

    async def _download_parquet(self, presigned_url: str) -> pd.DataFrame:
        """Download a parquet file via the CORS proxy and return as DataFrame."""
        proxy_url = f"{self._api_url}/data/proxy?url={quote(presigned_url)}"
        raw = await fetch_bytes(proxy_url, headers=self._headers())
        table = pq.read_table(BytesIO(raw))
        return table.to_pandas()

    async def get_ohlcv(self, **kwargs: Any) -> pd.DataFrame:
        """Fetch OHLCV candlestick data."""
        return await self.get_data("ohlcv", **kwargs)

    async def get_vwap(self, **kwargs: Any) -> pd.DataFrame:
        """Fetch VWAP data."""
        return await self.get_data("vwap", **kwargs)

    async def get_twap(self, **kwargs: Any) -> pd.DataFrame:
        """Fetch TWAP data."""
        return await self.get_data("twap", **kwargs)

    async def get_metrics(self, metric: str, **kwargs: Any) -> pd.DataFrame:
        """Fetch trade/L1/L2 metrics data.

        Args:
            metric: Metric name (e.g. 'flow', 'trade_size', 'l1_price').
            **kwargs: Passed to get_data.
        """
        return await self.get_data(metric, **kwargs)

    async def get_derivative_metrics(
        self, metric: str, **kwargs: Any
    ) -> pd.DataFrame:
        """Fetch derivative metrics data.

        Args:
            metric: Metric name (e.g. 'basis', 'funding', 'open_interest').
            **kwargs: Passed to get_data.
        """
        return await self.get_data(metric, **kwargs)

    async def get_symbols(self, exchange: str) -> list[str]:
        """Get available symbols for an exchange.

        Args:
            exchange: Exchange name (e.g. 'binance-futures').

        Returns:
            List of symbol strings.
        """
        url = f"{self._api_url}/metadata/symbols"
        response = await fetch_json(
            url, headers=self._headers(), params={"exchange": exchange}
        )
        return response["symbols"]
