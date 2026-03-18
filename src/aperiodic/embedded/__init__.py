"""Embedded client for Pyodide/WASM environments (e.g. marimo).

Install with: pip install aperiodic[embedded]

Usage::

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

from ._client import Client

__all__ = ["Client"]
