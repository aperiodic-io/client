from typing import Literal, NotRequired, TypedDict

OutputFormat = Literal["polars", "pandas"]

TimestampType = Literal["exchange", "true"]
# 15s/30s require a Tier 3 subscription; the API returns 403 otherwise.
Interval = Literal["15s", "30s", "1m", "5m", "15m", "30m", "1h", "4h", "1d"]

# Spot/futures exchanges
Exchange = Literal["binance-futures", "okx-perps", "hyperliquid-perps"]

# Derivatives exchanges
DerivativesExchange = Literal["binance-futures", "hyperliquid-perps"]

TradeMetric = Literal[
    "vtwap",
    "flow",
    "trade_size",
    "impact",
    "range",
    "updownticks",
    "run_structure",
    "returns",
    "slippage",
]

L1Metric = Literal["l1_price", "l1_imbalance", "l1_liquidity"]
L2Metric = Literal["l2_imbalance", "l2_liquidity"]

DerivativeMetric = Literal["basis", "funding", "open_interest", "derivative_price"]


class FileInfo(TypedDict):
    year: int
    month: int
    url: str
    # Present only for daily files. Older data is served as one file per month,
    # which carries no `day`.
    day: NotRequired[int]


class AggregateDataResponse(TypedDict):
    files: list[FileInfo]


class APIError(TypedDict, total=False):
    error: str
    details: list[str]


class SymbolsResponse(TypedDict):
    symbols: list[str]
    exchange: str
    bucket: str
