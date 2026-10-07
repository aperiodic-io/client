# Aperiodic Python Client

Python client library for [Aperiodic.io](https://aperiodic.io) — institutional-grade market microstructure, liquidity and order flow metrics with full exchange universe coverage. Turn flow dynamics into alpha in hours, not months. No tick infrastructure to build or maintain.

Access pre-computed derivative and microstructure metrics with parallel downloads for optimal performance.

## Installation

```bash
pip install aperiodic
```

Install from source:

```bash
git clone https://github.com/aperiodic-io/aperiodic-client.git
cd aperiodic-client
pip install -e .
```

## Authentication

All endpoints require your [Aperiodic.io](https://aperiodic.io) API key passed as `api_key="..."`.

The one exception is [preview data](#preview-no-subscription-required): with `preview=True` the `api_key` is optional — omit it and the shared public demo key is used automatically.

## Symbology

Symbols are expected in **[Atlas unified symbology](https://github.com/aperiodic-io/atlas)** — a standardised, exchange-agnostic naming scheme.

- Atlas repo: <https://github.com/aperiodic-io/atlas>
- Example symbol: `perpetual-BTC-USDT:USDT`

## Quick Start

```python
from datetime import date
from aperiodic import get_metrics

df = get_metrics(
    api_key="your-api-key",
    metric="flow",
    timestamp="true",
    interval="1h",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT", # See https://github.com/aperiodic-io/atlas
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 31),
)

print(df.head())
print(df.columns)
```

## Available Functions

| Dataset | Sync | Async | `metric` values |
|---------|------|-------|-----------------|
| Order, L1, L2 metrics | `get_metrics` | `get_metrics_async` | see below |
| OHLCV candles | `get_ohlcv` | `get_ohlcv_async` | — |
| VWAP | `get_vwap` | `get_vwap_async` | — |
| TWAP | `get_twap` | `get_twap_async` | — |
| Derivative metrics | `get_derivative_metrics` | `get_derivative_metrics_async` | see below |
| Exchange symbols | `get_symbols` | `get_symbols_async` | — |
| Raw data into a DataFrame | `get_raw` | `get_raw_async` | `dataset` (`RawDataset`) |
| Raw data to disk | `download_raw` | `download_raw_async` | `dataset` (`RawDataset`) |
| Raw coverage | `get_raw_coverage` | `get_raw_coverage_async` | — |
| Live rows over WebSocket | `stream` | — | `dataset`, `exchange`, `interval` |

### `get_metrics` — Trade & order book metrics

**Trade metrics** (`TradeMetric`): `"vtwap"`, `"flow"`, `"trade_size"`, `"impact"`, `"range"`, `"updownticks"`, `"run_structure"`, `"returns"`, `"slippage"`

**L1 order book** (`L1Metric`): `"l1_price"`, `"l1_imbalance"`, `"l1_liquidity"`

**L2 order book** (`L2Metric`): `"l2_imbalance"`, `"l2_liquidity"`

### `get_derivative_metrics` — Derivative metrics

`"basis"`, `"funding"`, `"open_interest"`, `"derivative_price"`

## Core Parameters

All data endpoints share this shape:

- `api_key`: Your [Aperiodic.io](https://aperiodic.io) API key. Optional when `preview=True` — the shared public demo key is used automatically.
- `timestamp`: `"exchange"` or `"true"`.
- `interval`: `"1m"` | `"5m"` | `"15m"` | `"30m"` | `"1h"` | `"4h"` | `"1d"`.
- `exchange`: `"binance-futures"` | `"okx-perps"` | `"hyperliquid-perps"`.
- `symbol`: [Atlas](https://github.com/aperiodic-io/atlas)-formatted symbol string (e.g. `"perpetual-BTC-USDT:USDT"`).
- `start_date` / `end_date`: Inclusive date boundaries.
- `preview`: `bool = False`. When `True`, routes to the free preview endpoint — no subscription required, but the request must match an exact whitelisted parameter combination (exchange, symbol, interval, timestamp, date range).
- `show_progress`: show `tqdm` progress bar (default: `True`).
- `max_concurrent`: max parallel file downloads (default: `10`).

## Examples

### Trade metrics

```python
from datetime import date
from aperiodic import get_metrics

flow_df = get_metrics(
    api_key="your-api-key",
    metric="flow",
    timestamp="exchange",
    interval="5m",
    exchange="binance-futures",
    symbol="perpetual-ETH-USDT:USDT", # See https://github.com/aperiodic-io/atlas
    start_date=date(2024, 2, 1),
    end_date=date(2024, 2, 29),
)
```

### L1 / L2 order book metrics

```python
from datetime import date
from aperiodic import get_metrics

l1_df = get_metrics(
    api_key="your-api-key",
    metric="l1_imbalance",
    timestamp="true",
    interval="1m",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT", # See https://github.com/aperiodic-io/atlas
    start_date=date(2024, 3, 1),
    end_date=date(2024, 3, 7),
)

l2_df = get_metrics(
    api_key="your-api-key",
    metric="l2_liquidity",
    timestamp="true",
    interval="1m",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT", # See https://github.com/aperiodic-io/atlas
    start_date=date(2024, 3, 1),
    end_date=date(2024, 3, 7),
)
```

### Derivative metrics

```python
from datetime import date
from aperiodic import get_derivative_metrics

funding_df = get_derivative_metrics(
    api_key="your-api-key",
    metric="funding",
    timestamp="exchange",
    interval="1h",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT", # See https://github.com/aperiodic-io/atlas
    start_date=date(2024, 1, 1),
    end_date=date(2024, 3, 31),
)
```

### Symbol discovery

```python
from aperiodic import get_symbols

symbols = get_symbols(api_key="your-api-key", exchange="binance-futures") # Returns Atlas symbols: https://github.com/aperiodic-io/atlas
perpetuals = [s for s in symbols if s.startswith("perpetual-")]
print(f"Found {len(perpetuals)} perpetual symbols")
```

### Async usage

```python
import asyncio
from datetime import date
from aperiodic import get_metrics_async, get_symbols_async

async def main() -> None:
    symbols = await get_symbols_async(
        api_key="your-api-key",
        exchange="binance-futures",
    )
    for symbol in symbols:
        df = await get_metrics_async(
            api_key="your-api-key",
            metric="l1_liquidity",
            timestamp="true",
            interval="1h",
            exchange="binance-futures",
            symbol=symbol, # See https://github.com/aperiodic-io/atlas
            start_date=date(2024, 1, 1),
            end_date=date(2026, 1, 1),
        )

asyncio.run(main())
```

### Preview (no subscription required)

Anyone can access a curated slice of data via `preview=True` — no subscription and no API key required. Omit `api_key` and the client uses the shared public demo key automatically. The request must match the exact parameters (exchange, symbol, interval, timestamp, date range) for one of the whitelisted entries.

**Available preview datasets:** [aperiodic.io/catalog#preview](https://aperiodic.io/catalog#preview)

```python
from datetime import date
from aperiodic import get_ohlcv

# Use the exact parameters listed at https://aperiodic.io/catalog#preview
df = get_ohlcv(
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT",
    interval="5m",
    timestamp="exchange",
    start_date=date(2025, 5, 1),
    end_date=date(2025, 5, 31),
    preview=True,
)

print(df.head())
```

## Raw data (Prime + Raw plan)

Raw trades, top-of-book quotes and derivative ticks for Binance, OKX and
Hyperliquid perpetuals, the data the metrics are built from. Same API key and
symbols as the metrics. History is one Parquet file per calendar month; from
2026-08-01 there is one file per day.

**Datasets** (`RawDataset`): `"trades"`, `"quotes"`, `"mark_price"`,
`"index_price"`, `"funding_rate"`, `"open_interest"`, on every venue.

Every file starts with `exchange_timestamp` (the venue's time) and
`local_timestamp` (when the event reached our capture machine). Timestamps are
timezone-aware UTC.

Hyperliquid's derivative feed carries no exchange time, so in its
`mark_price`, `index_price`, `funding_rate` and `open_interest` files
`exchange_timestamp` is modelled from the capture time, and an
`exchange_timestamp_kind` column (`"modelled"`) follows it.

<!-- The raw examples use "py" fences, not "python": tests/test_readme.py runs
python blocks containing api_key= against production, where raw data isn't live
yet. Switch them to python once it is. -->

```py
from datetime import date
import aperiodic as ap

# Into one DataFrame, trimmed to the range on exchange_timestamp
trades = ap.get_raw(
    api_key="your-api-key",
    dataset="trades",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT",
    start_date=date(2025, 6, 1),
    end_date=date(2025, 6, 3),
)

# Large ranges: stream the files to disk instead (skips files you already have)
paths = ap.download_raw(
    api_key="your-api-key",
    dataset="quotes",
    exchange="okx-perps",
    symbol="perpetual-BTC-USDT:USDT",
    start_date=date(2024, 1, 1),
    end_date=date(2025, 12, 31),
    output_dir="raw",
)
```

`download_raw` writes the bucket's own layout,
`raw/{dataset}/exchange=…/symbol=…/year=YYYY/month=MM[/day=DD]/data.parquet`
(`:` becomes `%3A` on Windows). Monthly and daily files sit at different
depths, so read a folder with a glob and filter on `exchange_timestamp`:

```py
import polars as pl

lazy = pl.scan_parquet("raw/quotes/**/data.parquet")
```

- Ranges over 366 days are split into several requests for you.
- Download URLs are valid for one hour; one that has expired is re-requested
  automatically.
- A plan without raw data raises `APIError` with `status_code=403` and
  `code="raw_not_in_plan"`.
- `get_raw(..., preview=True)` returns the free June 2025 file of each venue's
  BTC perpetual without a key. `get_raw_coverage()` lists every symbol's first
  and last day, no key needed.
- `download_raw` needs CPython (httpx and a filesystem); in Pyodide use
  `get_raw`.

## Live streaming

Rows as they are published, over WebSocket, on plans with live data. Needs the
`stream` extra:

```bash
pip install "aperiodic[stream]"
```

```python
from aperiodic import stream

for message in stream(
    api_key="your-api-key",
    dataset="ohlcv",
    exchange="binance-futures",
    interval="1m",
    symbols=["perpetual-BTC-USDT:USDT"],  # omit for every symbol on your plan
):
    print(message.channel, message.snapshot, message.data["close"])
```

Each `StreamMessage` has `channel` (`"ohlcv.binance-futures.1m"`), `data` (the
row as published; `time` is microseconds since the epoch) and `snapshot`.
Right after subscribing, Pro plans and above get the latest row per symbol,
flagged `snapshot=True`; pass `snapshot=False` to skip those.

- **More channels:** `channels=["open_interest.okx-perps.1m", {"dataset": "ohlcv",
  "exchange": "okx-perps", "interval": "1m", "symbols": [...]}]`, alone or
  next to `dataset`/`exchange`/`interval`, up to 200 in all.
- **Rejections:** if every channel is refused, `StreamSubscriptionError` is
  raised and `.rejected` says why (`not_entitled`, `not_live`,
  `unknown_channel`, `limit_exceeded`, `invalid_message`). If only some are,
  a `StreamWarning` is emitted and the rest stream; the granted and rejected
  channels are on the stream's `.subscription`.
- **Refused connections** raise `APIError`: `401` for a bad key, `403` when
  the plan has no live data, `429` when all of the plan's connections are in
  use. These are never retried.
- **Reconnects:** a dropped connection or a server restart is re-opened with
  exponential backoff and the same subscription (`reconnect=False` raises
  `StreamClosedError` instead). A lapsed plan or rotated key (close code
  4001) always raises `StreamClosedError`.
- **At-most-once:** rows published while disconnected are not replayed. If a
  gap matters, fill it from the REST endpoints (`get_ohlcv`, ...).
- Leaving the loop (`break`, Ctrl-C) closes the connection. To close it from
  elsewhere, keep the stream and call `.close()`, or use it as a context
  manager.
- CPython only: a browser WebSocket (Pyodide, marimo) cannot send the API key
  header.

## Performance Notes

- Downloads are split into monthly parquet files server-side.
- Files are fetched concurrently and concatenated locally.
- Final output is sorted and filtered to your exact requested date range.
- Tune `max_concurrent` based on your network and compute resources.
- Transient failures — rate limits, upstream 5xx, dropped connections — are
  retried with exponential backoff before an `APIError` is raised.

## Requirements

- Python 3.11+
- `httpx`
- `polars`
- `tqdm`
- `nest-asyncio`
- `websockets` (only for live streaming, via the `stream` extra)

## License

MIT
