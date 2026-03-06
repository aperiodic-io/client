__version__ = "0.1.0"

from .client import AperiodicDataError, APIError, DownloadError
from .endpoints.derivative import get_derivative_metrics, get_derivative_metrics_async
from .endpoints.l1 import get_l1_metrics, get_l1_metrics_async
from .endpoints.l2 import get_l2_metrics, get_l2_metrics_async
from .endpoints.ohlcv import get_ohlcv, get_ohlcv_async
from .endpoints.symbols import get_symbols, get_symbols_async
from .endpoints.trades import get_trade_metrics, get_trade_metrics_async
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
    "get_l1_metrics",
    "get_l1_metrics_async",
    "get_l2_metrics",
    "get_l2_metrics_async",
    "get_ohlcv",
    "get_ohlcv_async",
    "get_symbols",
    "get_symbols_async",
    "get_trade_metrics",
    "get_trade_metrics_async",
]
