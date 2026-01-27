from typing import Literal, TypedDict

TimestampType = Literal["exchange", "true"]
Interval = Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
Exchange = Literal["binance-futures"]
MetricType = Literal["taker", "trade_size", "vtwap"]

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
