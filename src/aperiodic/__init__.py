__version__ = "0.1.0"

from .client import AperiodicDataError, APIError, DownloadError
from .endpoints.derivative import get_derivative_metrics, get_derivative_metrics_async
from .endpoints.market_data import (
    get_ohlcv,
    get_ohlcv_async,
    get_twap,
    get_twap_async,
    get_vwap,
    get_vwap_async,
)
from .endpoints.metrics import get_metrics, get_metrics_async
from .endpoints.symbols import get_symbols, get_symbols_async
from .types import (
    DerivativeMetric,
    Exchange,
    Interval,
    L1Metric,
    L2Metric,
    TimestampType,
    TradeMetric,
)

__all__ = [
    "APIError",
    "AperiodicDataError",
    "DerivativeMetric",
    "DownloadError",
    "Exchange",
    "Interval",
    "L1Metric",
    "L2Metric",
    "TimestampType",
    "TradeMetric",
    "get_derivative_metrics",
    "get_derivative_metrics_async",
    "get_metrics",
    "get_metrics_async",
    "get_ohlcv",
    "get_ohlcv_async",
    "get_symbols",
    "get_symbols_async",
    "get_twap",
    "get_twap_async",
    "get_vwap",
    "get_vwap_async",
]
