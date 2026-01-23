# Unravel Data Client

Python client library for the Unravel aggregate market data API. Access pre-computed OHLCV candlestick data and other market aggregates with parallel downloads for optimal performance.

## Installation

```bash
pip install unravel-data-client
```

Or install from source:

```bash
git clone https://github.com/unravel-finance/unravel-data-client.git
cd unravel-data-client
pip install -e .
```

## Quick Start

```python
from datetime import date
from unravel_data_client import get_ohlcv_historical

# Fetch hourly OHLCV data for BTC
df = get_ohlcv_historical(
    api_key="your-api-key",
    arrival_time="true",
    period="1h",
    exchange="binance-futures",
    symbol="btcusdt",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 3, 31),
)

print(df.head())
```

## Features

- **Parallel Downloads**: Files are downloaded concurrently for fast data retrieval
- **Date Range Support**: Fetch data spanning multiple months in a single call
- **Progress Bar**: Visual progress indication with tqdm
- **Type Hints**: Full type annotations for IDE support
- **Async Support**: Both sync and async APIs available

## API Reference

### `get_ohlcv_historical`

Fetch historical OHLCV (Open, High, Low, Close, Volume) data.

```python
def get_ohlcv_historical(
    api_key: str,
    arrival_time: Literal["exchange", "true"],
    period: Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d"],
    exchange: Literal["binance-futures", "binance"],
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = "https://unravel.finance/api/v1",
    show_progress: bool = True,
    max_concurrent: int = 10,
) -> pl.DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `api_key` | `str` | Your Unravel API key |
| `arrival_time` | `"exchange"` \| `"true"` | Timestamp source - `"exchange"` for exchange-reported time, `"true"` for actual arrival time |
| `period` | `str` | Aggregation period (`"1m"`, `"5m"`, `"15m"`, `"30m"`, `"1h"`, `"4h"`, `"1d"`) |
| `exchange` | `str` | Source exchange (`"binance-futures"`, `"binance"`) |
| `symbol` | `str` | Trading pair symbol (e.g., `"btcusdt"`, `"ethusdt"`) |
| `start_date` | `date` | Start date for the data range |
| `end_date` | `date` | End date for the data range (inclusive) |
| `base_url` | `str` | API base URL (optional) |
| `show_progress` | `bool` | Show download progress bar (default: `True`) |
| `max_concurrent` | `int` | Maximum concurrent downloads (default: `10`) |

**Returns:**

`pl.DataFrame` with columns:
- `timestamp`: Unix timestamp in milliseconds
- `datetime`: Parsed datetime (added by client)
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price
- `close`: Closing price
- `volume`: Trading volume

### `get_ohlcv_historical_async`

Async version of `get_ohlcv_historical`. Use this when you're already in an async context.

```python
import asyncio
from unravel_data_client import get_ohlcv_historical_async

async def main():
    df = await get_ohlcv_historical_async(
        api_key="your-api-key",
        arrival_time="true",
        period="1h",
        exchange="binance-futures",
        symbol="btcusdt",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 31),
    )
    return df

df = asyncio.run(main())
```

### `get_ohlcv_historical_multi`

Fetch data for multiple symbols concurrently.

```python
from unravel_data_client import get_ohlcv_historical_multi

df_dict = get_ohlcv_historical_multi(
    api_key="your-api-key",
    arrival_time="true",
    period="1h",
    exchange="binance-futures",
    symbols=["btcusdt", "ethusdt", "solusdt"],
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 31),
)

btc_df = df_dict["btcusdt"]
eth_df = df_dict["ethusdt"]
```

## Error Handling

```python
from unravel_data_client import get_ohlcv_historical, APIError, DownloadError

try:
    df = get_ohlcv_historical(...)
except APIError as e:
    print(f"API error {e.status_code}: {e.message}")
    if e.details:
        print(f"Details: {e.details}")
except DownloadError as e:
    print(f"Failed to download {e.year}-{e.month:02d}: {e.original_error}")
```

## Data Storage

Data is stored using Hive partitioning in Parquet format:

```
{arrival_time}/{period}/exchange={exchange}/symbol={symbol}/year={year}/month={month}.parquet
```

The client handles all the complexity of fetching multiple monthly files and combining them into a single DataFrame.

## Requirements

- Python 3.11+
- httpx
- polars
- tqdm

## License

MIT License - see LICENSE file for details.
