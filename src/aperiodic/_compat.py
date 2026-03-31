"""
Compatibility layer for DataFrame backends.

When polars is available (pip install aperiodic[polars]), all data is returned
as polars DataFrames (original behavior).

When polars is not available (pip install aperiodic[pandas]), pyarrow is used
to read parquet files and data is returned as pandas DataFrames - suitable for
environments like marimo where Rust-based packages (polars) cannot be installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

# --- DataFrame backend detection ---

try:
    import polars  # noqa: F401

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


if HAS_POLARS:
    from ._backends._polars import (
        DataFrame,
        concat,
        empty_dataframe,
        filter_datetime_range,
        from_epoch_ms,
        has_column,
        read_parquet,
        sort_by,
    )
elif HAS_PYARROW:
    from ._backends._pandas import (
        DataFrame,
        concat,
        empty_dataframe,
        filter_datetime_range,
        from_epoch_ms,
        has_column,
        read_parquet,
        sort_by,
    )
else:
    from datetime import datetime
    from io import BytesIO

    DataFrame = Any  # type: ignore[assignment,misc]

    def read_parquet(buffer: BytesIO) -> Any:  # type: ignore[misc]
        raise ImportError(
            "Either polars or pyarrow is required to read parquet files. "
            "Install with: pip install aperiodic[polars] or pip install aperiodic[pandas]"
        )

    def concat(dataframes: list[Any]) -> Any:  # type: ignore[misc]
        raise ImportError(
            "Either polars or pyarrow is required. "
            "Install with: pip install aperiodic[polars] or pip install aperiodic[pandas]"
        )

    def empty_dataframe() -> Any:  # type: ignore[misc]
        raise ImportError(
            "Either polars or pyarrow is required. "
            "Install with: pip install aperiodic[polars] or pip install aperiodic[pandas]"
        )

    def from_epoch_ms(df: Any, column: str) -> Any:  # type: ignore[misc]
        raise ImportError(
            "Either polars or pyarrow is required. "
            "Install with: pip install aperiodic[polars] or pip install aperiodic[pandas]"
        )

    def filter_datetime_range(  # type: ignore[misc]
        df: Any,
        start_date: datetime,
        end_date: datetime,
    ) -> Any:
        raise ImportError(
            "Either polars or pyarrow is required. "
            "Install with: pip install aperiodic[polars] or pip install aperiodic[pandas]"
        )

    def sort_by(df: Any, column: str) -> Any:  # type: ignore[misc]
        raise ImportError(
            "Either polars or pyarrow is required. "
            "Install with: pip install aperiodic[polars] or pip install aperiodic[pandas]"
        )

    def has_column(df: Any, column: str) -> bool:
        return column in df.columns


def get_backend_module(output: str) -> Any:
    """Return the backend module (_polars or _pandas) for the given output format."""
    if output == "polars":
        if not HAS_POLARS:
            raise ImportError(
                "polars is not installed. Install with: pip install aperiodic[polars]"
            )
        from ._backends import _polars
        return _polars
    # "pandas"
    if not HAS_PYARROW:
        raise ImportError(
            "pandas/pyarrow is not installed. Install with: pip install aperiodic[pandas]"
        )
    from ._backends import _pandas
    return _pandas


__all__ = [
    "HAS_POLARS",
    "HAS_PYARROW",
    "DataFrame",
    "concat",
    "empty_dataframe",
    "filter_datetime_range",
    "from_epoch_ms",
    "get_backend_module",
    "has_column",
    "read_parquet",
    "sort_by",
]
