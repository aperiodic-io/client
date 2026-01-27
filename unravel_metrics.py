# %%
"""
Standalone example using obstore to list and download parquet files from the Unravel data bucket.

Path structure:
bucket/metric_type/timestamp_type/interval/exchange=exchange/symbol=symbol/year=year/month=month.parquet

Requirements: obstore, polars
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import obstore as obs
import polars as pl
from obstore.store import S3Store

S3_BASE_URL = "https://afb64de9fa5f1b82f808e37f7ddd4004.r2.cloudflarestorage.com/"
ACCESS_KEY_ID = "12cf2d015ef7c4884fba678a321cd438"
SECRET_ACCESS_KEY = "6e552c35655df1742b9e4939cd292c0dc2d72aab31a2048c7c5ac3e8337edd77"


MetricType = Literal["ohlcv", "taker_metrics", "trade_size_metrics", "vwap_twap"]
TimestampType = Literal["true", "exchange"]
Interval = Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
Exchange = Literal["binance-futures"]
Symbol = str
Year = int
Month = int


@dataclass
class MetricMetadata:
    metric: MetricType
    bucket: str
    folder: str | None


metric_registry = [
    MetricMetadata(metric="ohlcv", bucket="ohlcv", folder=None),
    MetricMetadata(metric="taker_metrics", bucket="trade-metrics", folder="taker"),
    MetricMetadata(
        metric="trade_size_metrics", bucket="trade-metrics", folder="trade_size"
    ),
    MetricMetadata(metric="vwap_twap", bucket="trade-metrics", folder="vtwap"),
]


def create_store(bucket: str) -> S3Store:
    return S3Store.from_url(
        S3_BASE_URL + bucket,
        config={"access_key_id": ACCESS_KEY_ID, "secret_access_key": SECRET_ACCESS_KEY},
    )


async def _list_files(
    metric: MetricType,
    timestamp_type: TimestampType,
    interval: Interval,
    exchange: Exchange,
    symbol: Symbol,
) -> list[str]:
    """
    List parquet files in the bucket matching the given parameters.

    Args:
        metric: The metric type (default: "ohlcv")
        timestamp_type: "true" for actual arrival time, "exchange" for exchange-reported time
        interval: Aggregation interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
        exchange: Source exchange (binance-futures, binance)
        symbol: Optional symbol filter (e.g., "btcusdt"). If None, lists all symbols.

    Returns:
        List of file paths matching the criteria
    """
    metric_metadata = next(m for m in metric_registry if m.metric == metric)
    store = create_store(bucket=metric_metadata.bucket)

    prefix = (
        f"{metric_metadata.folder}/{timestamp_type}/{interval}/exchange={exchange}/symbol={symbol}/"
        if metric_metadata.folder
        else f"{timestamp_type}/{interval}/exchange={exchange}/symbol={symbol}/"
    )
    # List all objects with the prefix
    files = []
    list_stream = obs.list(store, prefix=prefix)

    async for batch in list_stream:
        for obj in batch:
            if obj["path"].endswith(".parquet"):
                files.append(obj["path"])

    return sorted(files)


async def list_symbols(
    metric: MetricType,
    timestamp_type: Literal["true", "exchange"] = "true",
    interval: str = "1h",
    exchange: str = "binance-futures",
) -> list[str]:
    """
    List all available symbols for the given parameters.

    Args:
        metric: The metric bucket name (default: "ohlcv")
        timestamp_type: "true" for actual arrival time, "exchange" for exchange-reported time
        interval: Aggregation interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
        exchange: Source exchange (binance-futures, binance)

    Returns:
        List of available symbol names
    """
    metric_metadata = next(m for m in metric_registry if m.metric == metric)

    store = create_store(bucket=metric_metadata.bucket)
    prefix = (
        f"{metric_metadata.folder}/{timestamp_type}/{interval}/exchange={exchange}/"
        if metric_metadata.folder
        else f"{timestamp_type}/{interval}/exchange={exchange}/"
    )

    list_stream = obs.list(store, prefix=prefix)

    symbols = set()
    async for batch in list_stream:
        for obj in batch:
            path = obj["path"]
            # Extract symbol from path like: .../symbol=btcusdt/year=.../...
            if "symbol=" in path:
                symbol_part = path.split("symbol=")[1].split("/")[0]
                symbols.add(symbol_part)

    return sorted(symbols)


async def download_symbol(
    symbol: str,
    metric: MetricType,
    timestamp_type: TimestampType,
    interval: Interval,
    exchange: Exchange,
    output_dir: str | Path,
) -> list[Path]:
    """
    Download all parquet files for a specific symbol.

    Args:
        symbol: Trading pair symbol (e.g., "btcusdt")
        output_dir: Directory to save downloaded files
        metric: The metric bucket name (default: "ohlcv")
        timestamp_type: "true" for actual arrival time, "exchange" for exchange-reported time
        interval: Aggregation interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
        exchange: Source exchange (binance-futures, binance)

    Returns:
        List of paths to downloaded files
    """
    metric_metadata = next(m for m in metric_registry if m.metric == metric)
    store = create_store(bucket=metric_metadata.bucket)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    files = await _list_files(
        metric=metric,
        timestamp_type=timestamp_type,
        interval=interval,
        exchange=exchange,
        symbol=symbol,
    )

    async def download_file(file_path: str) -> Path:
        local_path = output_path / file_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Downloading {file_path} -> {local_path}")
        result = await obs.get_async(store, file_path)
        content = await result.bytes_async()

        with open(local_path, "wb") as f:
            f.write(content)

        return local_path

    # Download all files concurrently
    downloaded = await asyncio.gather(*[download_file(f) for f in files])

    return list(downloaded)


async def load_symbol_data(
    symbol: str,
    metric: MetricType,
    timestamp_type: TimestampType,
    interval: Interval,
    exchange: Exchange,
    output_dir: str | Path,
) -> pl.DataFrame:
    downloaded = await download_symbol(
        symbol=symbol,
        output_dir=output_dir,
        metric=metric,
        timestamp_type=timestamp_type,
        interval=interval,
        exchange=exchange,
    )

    return pl.read_parquet(downloaded)


# Example usage
if __name__ == "__main__":

    async def main():
        # # Example 1: List all symbols
        symbols = await list_symbols(
            metric="ohlcv",
            timestamp_type="true",
            interval="1h",
            exchange="binance-futures",
        )
        print(f"Found {len(symbols)} symbols")
        if symbols:
            print(f"First 10: {symbols[:10]}")

        # Example 2: Download data for a specific symbol
        print("\n=== Downloading data for btcusdt ===")
        downloaded = await download_symbol(
            metric="ohlcv",
            symbol="btcusdt",
            timestamp_type="true",
            interval="1h",
            exchange="binance-futures",
            output_dir="./downloads",
        )

        # Example 3: Load data directly into DataFrame (downloads data again)
        print("\n=== Loading btcusdt data into DataFrame ===")
        df = await load_symbol_data(
            metric="ohlcv",
            output_dir="./downloads",
            symbol="btcusdt",
            timestamp_type="true",
            interval="1h",
            exchange="binance-futures",
        )
        print(f"Loaded {len(df)} rows")
        print(df.head())

    asyncio.run(main())

# %%
