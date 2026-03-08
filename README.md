# Aperiodic Python Client

A fast, typed Python SDK for downloading Aperiodic aggregate market datasets as `polars` DataFrames.

This client is built for research and production workflows that need reliable historical data pulls across configurable date ranges, with concurrent downloads and retry handling built in.

## Why use this client?

- **One-line historical pulls** for OHLCV, trades, order-book, and derivatives datasets.
- **Typed interfaces** for exchanges, intervals, timestamps, and metric names.
- **Parallel parquet downloads** with per-file retries and exponential backoff.
- **Sync + async APIs** for notebooks, scripts, and backend services.
- **Polars-native output** for high-performance analytical pipelines.

## Installation

```bash
pip install aperiodic
```

Install from source:

```bash
git clone https://github.com/aperiodic-io/client.git
cd client
pip install -e .
```

## Authentication

All endpoints require your API key passed as `api_key="..."`.

```python
API_KEY = "your-api-key"
```

## Symbology

Symbols are expected in **Atlas unified symbology**.

- Atlas repo: <https://github.com/aperiodic-io/atlas>
- Example symbol: `perpetual-BTC-USDT:USDT`

## Quick start

```python
from datetime import date
from aperiodic import get_ohlcv

# 1h BTC perpetual OHLCV from Binance futures
df = get_ohlcv(
    api_key="your-api-key",
    timestamp="true",
    interval="1h",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 31),
)

print(df.head())
print(df.columns)
```

## Available datasets and functions

| Dataset family | Sync | Async | Metrics |
|---|---|---|---|
| OHLCV candles | `get_ohlcv` | `get_ohlcv_async` | N/A |
| Trade metrics | `get_trade_metrics` | `get_trade_metrics_async` | `vtwap`, `flow`, `trade_size`, `impact`, `range`, `updownticks`, `run_structure`, `returns`, `slippage` |
| L1 book metrics | `get_l1_metrics` | `get_l1_metrics_async` | `l1_price`, `l1_imbalance`, `l1_liquidity` |
| L2 book metrics | `get_l2_metrics` | `get_l2_metrics_async` | `l2_imbalance`, `l2_liquidity` |
| Derivatives metrics | `get_derivative_metrics` | `get_derivative_metrics_async` | `basis`, `funding`, `open_interest`, `derivative_price` |
| Exchange symbols | `get_symbols` | `get_symbols_async` | N/A |

## Core parameters

Most data endpoints share this shape:

- `api_key`: Your Aperiodic API key.
- `timestamp`: `"exchange"` or `"true"`.
- `interval`: `"1m" | "5m" | "15m" | "30m" | "1h" | "4h" | "1d"`.
- `exchange`: `"binance-futures" | "binance" | "okx-perps"`.
- `symbol`: Atlas-formatted symbol string.
- `start_date` / `end_date`: Inclusive date boundaries.
- `show_progress`: show `tqdm` progress bar (default: `True`).
- `max_concurrent`: max parallel file downloads (default: `10`).
- `base_url`: API base URL override if needed.

## Examples

### Trade metrics example

```python
from datetime import date
from aperiodic import get_trade_metrics

flow_df = get_trade_metrics(
    api_key="your-api-key",
    metric="flow",
    timestamp="exchange",
    interval="5m",
    exchange="binance-futures",
    symbol="perpetual-ETH-USDT:USDT",
    start_date=date(2024, 2, 1),
    end_date=date(2024, 2, 29),
)
```

### L1 / L2 metrics example

```python
from datetime import date
from aperiodic import get_l1_metrics, get_l2_metrics

l1_df = get_l1_metrics(
    api_key="your-api-key",
    metric="l1_imbalance",
    timestamp="true",
    interval="1m",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT",
    start_date=date(2024, 3, 1),
    end_date=date(2024, 3, 7),
)

l2_df = get_l2_metrics(
    api_key="your-api-key",
    metric="l2_liquidity",
    timestamp="true",
    interval="1m",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT",
    start_date=date(2024, 3, 1),
    end_date=date(2024, 3, 7),
)
```

### Derivatives metrics example

```python
from datetime import date
from aperiodic import get_derivative_metrics

funding_df = get_derivative_metrics(
    api_key="your-api-key",
    metric="funding",
    timestamp="exchange",
    interval="1h",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 3, 31),
)
```

### Async usage

```python
import asyncio
from datetime import date
from aperiodic import get_ohlcv_async

async def main() -> None:
    df = await get_ohlcv_async(
        api_key="your-api-key",
        timestamp="true",
        interval="1h",
        exchange="binance-futures",
        symbol="perpetual-BTC-USDT:USDT",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )
    print(df.shape)

asyncio.run(main())
```

### Symbol discovery

```python
from aperiodic import get_symbols

symbols = get_symbols(api_key="your-api-key", exchange="binance-futures")
print(f"symbols: {len(symbols)}")
print(symbols[:10])
```

## Error handling

```python
from aperiodic import APIError, DownloadError, get_ohlcv

try:
    df = get_ohlcv(...)
except APIError as exc:
    print(f"API error {exc.status_code}: {exc.message}")
    if exc.details:
        print(exc.details)
except DownloadError as exc:
    print(f"Download failed for {exc.year}-{exc.month:02d}: {exc.original_error}")
```

## Performance notes

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

## Go CLI downloader

This repo also includes a Go CLI that downloads the raw monthly parquet files returned by the same API used by the Python client.

```bash
cd go-cli
go run . \
  -api-key "$API_KEY" \
  -bucket ohlcv \
  -timestamp true \
  -interval 1h \
  -exchange binance-futures \
  -symbol 'perpetual-BTC-USDT:USDT' \
  -start-date 2024-01-01 \
  -end-date 2024-01-31 \
  -out ./downloads
```

It fetches `/data/{bucket}` pre-signed URLs, then downloads each file concurrently with retries.
