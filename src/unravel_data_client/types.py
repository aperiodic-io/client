"""Type definitions for the Unravel Data Client."""

from typing import Literal, TypedDict

# Literal types matching the API
ArrivalTime = Literal["exchange", "true"]
Period = Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
Exchange = Literal["binance-futures", "binance"]


class FileInfo(TypedDict):
    """Information about a single data file."""

    year: int
    month: int
    url: str


class AggregateDataResponse(TypedDict):
    """Response from the aggregate data API."""

    files: list[FileInfo]


class APIError(TypedDict, total=False):
    """Error response from the API."""

    error: str
    details: list[str]
