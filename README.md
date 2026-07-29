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

## Live Streams (Kafka)

Metrics are also published as live Kafka streams. Authentication uses the same API key as the REST endpoints above, and your subscription tier decides which topics you can read.

```bash
pip install aperiodic[streaming]
```

### Credentials

Three values, read from the environment:

| Variable | Value |
|----------|-------|
| `APERIODIC_KAFKA_BOOTSTRAP` | Bootstrap endpoint in `host:port` form — we provide this |
| `APERIODIC_KAFKA_ACCOUNT_ID` | Your account id, used as the SASL username |
| `APERIODIC_KAFKA_API_KEY` | Your Aperiodic data API key, used as the SASL password |

The API key is the same one the REST API takes — there is no separate streaming password. The client ships no defaults for any of these; a missing variable raises `StreamConfigError` naming the variable.

### Quick Start

```python
from aperiodic.streaming import KafkaStreamClient

with KafkaStreamClient.from_env() as stream:
    print(stream.list_topics())

    for message in stream.consume("ohlcv.binance-futures.m1", group_suffix="research"):
        print(message)
```

Credentials can also be passed directly, if you keep them somewhere other than the environment:

```python
from aperiodic.streaming import KafkaStreamClient

stream = KafkaStreamClient(
    bootstrap_servers="broker.example.net:9093",
    account_id="your-account-id",
    api_key="your-api-key",
    timeout=10.0,
)
```

### Topics

Topics are named `<dataset>.<exchange>.<interval>`, for example `ohlcv.binance-futures.m1`. Parse one with `TopicName.parse(...)`.

`list_topics()` returns only the datasets your tier covers. Reading anything else raises `StreamAuthorizationError` with code `TOPIC_AUTHORIZATION_FAILED`; `check_topic_access(topic)` tests a single topic without consuming from it.

### Consumer Groups

**Consumer groups must be named `<account-id>-<suffix>`.** The broker rejects every other name with `GROUP_AUTHORIZATION_FAILED`. Pass `group_suffix=` and the client applies the prefix for you:

```python
stream.build_group_id("research")   # "<your-account-id>-research"
```

### Reading

`consume(topic, group_suffix=..., timeout=..., max_messages=...)` returns a list of message payloads. It returns early rather than blocking: an empty list means the topic was quiet for `timeout` seconds, which is not an error. Being refused the topic, or failing to reach the cluster, raises.

Every call takes a bounded timeout, defaulting to the client's `timeout` (10s).

### Connection Details

`SASL_SSL` with `SCRAM-SHA-256`. The brokers present a publicly trusted certificate that validates against the system CA store, so there is no CA bundle to configure — and certificate verification should never be disabled.

### Errors

All streaming exceptions inherit `StreamingError`, which inherits `AperiodicDataError`. Each carries a `code` attribute holding the underlying Kafka error name.

| Exception | Raised when |
|-----------|-------------|
| `StreamConfigError` | A required environment variable is missing, or a setting is invalid |
| `StreamGroupIdError` | A consumer group id does not carry your account-id prefix |
| `StreamConnectionError` | The cluster could not be reached — DNS, TLS or timeout |
| `StreamAuthError` | The account id / API key pair was rejected |
| `StreamAuthorizationError` | Authenticated, but not entitled to that topic or group |

### Logging

The client routes librdkafka's output through a redacting log filter and scrubs its own errors, so the endpoint, your account id and your API key do not appear in exception messages, tracebacks, `repr()` or log records. This makes the client's output safe to paste into a shared terminal or a CI log — but the credentials themselves still belong in a secret store, never in source.

Client logs are emitted under the `aperiodic.streaming` logger.

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
- `confluent-kafka` — only for [live streams](#live-streams-kafka), via `pip install aperiodic[streaming]`

## License

MIT
