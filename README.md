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

### `get_metrics` — Trade & order book metrics

**Trade metrics** (`TradeMetric`): `"vtwap"`, `"flow"`, `"trade_size"`, `"impact"`, `"range"`, `"updownticks"`, `"run_structure"`, `"returns"`, `"slippage"`

**L1 order book** (`L1Metric`): `"l1_price"`, `"l1_imbalance"`, `"l1_liquidity"`

**L2 order book** (`L2Metric`): `"l2_imbalance"`, `"l2_liquidity"`

### `get_derivative_metrics` — Derivative metrics

`"basis"`, `"funding"`, `"open_interest"`, `"derivative_price"`

## Core Parameters

All data endpoints share this shape:

- `api_key`: Your [Aperiodic.io](https://aperiodic.io) API key.
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

Any authenticated user — even without a paid subscription — can access a curated slice of data via `preview=True`. The request must match the exact parameters (exchange, symbol, interval, timestamp, date range) for one of the whitelisted entries.

**Available preview datasets:** [aperiodic.io/catalog#preview](https://aperiodic.io/catalog#preview)

```python
from datetime import date
from aperiodic import get_ohlcv

# Copy the exact parameters from https://aperiodic.io/catalog#preview
df = get_ohlcv(
    api_key="your-api-key",  # sign up free at aperiodic.io
    exchange="...",
    symbol="...",
    interval="...",
    timestamp="...",
    start_date=date(...),
    end_date=date(...),
    preview=True,
)

print(df.head())
```

## Performance Notes

- Downloads are split into monthly parquet files server-side.
- Files are fetched concurrently and concatenated locally.
- Final output is sorted and filtered to your exact requested date range.
- Tune `max_concurrent` based on your network and compute resources.

## Requirements

- Python 3.11+
- `httpx`
- `polars`
- `tqdm`
- `nest-asyncio`

## License

MIT
