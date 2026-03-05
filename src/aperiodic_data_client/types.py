from typing import Literal, TypedDict

TimestampType = Literal["exchange", "true"]
Interval = Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d"]

# Spot/futures exchanges
Exchange = Literal["binance-futures", "binance", "okx-perps"]

# Derivatives exchanges
DerivativesExchange = Literal[
    "binance-delivery",
    "binance-futures",
    "bitfinex-derivatives",
    "bitget-futures",
    "bitmex",
    "bybit",
    "crypto-com",
    "deribit",
    "ftx",
    "gate-io-futures",
    "hyperliquid",
    "okex-swap",
]

# Trade metrics dataset identifiers (map to the trade-metrics R2 bucket)
TradeMetric = Literal["vtwap", "flow", "trade_size", "impact", "range", "updownticks"]

# L1 metrics dataset identifiers (map to the l1-metrics R2 bucket)
L1Metric = Literal["l1_price", "l1_imbalance", "l1_liquidity"]


class FileInfo(TypedDict):
    year: int
    month: int
    url: str


class AggregateDataResponse(TypedDict):
    files: list[FileInfo]


class APIError(TypedDict, total=False):
    error: str
    details: list[str]


class SymbolsResponse(TypedDict):
    symbols: list[str]
    exchange: str
    bucket: str
