"""
Compatibility layer for DataFrame backends.

When polars is available (pip install aperiodic[polars]), all data is returned
as polars DataFrames (original behavior).

When polars is not available (pip install aperiodic[pandas]), pyarrow is used
to read parquet files and data is returned as pandas DataFrames - suitable for
environments like marimo where Rust-based packages (polars) cannot be installed.
"""

from __future__ import annotations

from importlib import import_module
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


# --- DataFrame backend detection ---
# Use find_spec so we do not import heavy optional dependencies while importing
# this module. This avoids hard failures in environments where a package is
# present but cannot be imported (e.g. incompatible wheel/runtime).
HAS_POLARS = find_spec("polars") is not None
HAS_PYARROW = find_spec("pyarrow") is not None


def _import_polars_backend() -> Any:
    """Import and return the polars backend module."""
    return import_module("aperiodic._backends._polars")


def _import_pandas_backend() -> Any:
    """Import and return the pandas backend module."""
    return import_module("aperiodic._backends._pandas")


def _get_default_backend() -> Any:
    """Return the default backend module for top-level compatibility exports."""
    if HAS_POLARS:
        try:
            return _import_polars_backend()
        except Exception:
            # Fall back to pandas/pyarrow when polars is present but unusable.
            # This is common in constrained runtimes where Rust wheels cannot load.
            pass

    if HAS_PYARROW:
        return _import_pandas_backend()

    return None


_backend = _get_default_backend()

if _backend is not None:
    DataFrame = _backend.DataFrame
    concat = _backend.concat
    empty_dataframe = _backend.empty_dataframe
    filter_datetime_range = _backend.filter_datetime_range
    from_epoch_ms = _backend.from_epoch_ms
    has_column = _backend.has_column
    read_parquet = _backend.read_parquet
    sort_by = _backend.sort_by
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
        try:
            return _import_polars_backend()
        except ImportError as exc:
            raise ImportError(
                "polars is not installed. Install with: pip install aperiodic[polars]"
            ) from exc

    # "pandas"
    try:
        return _import_pandas_backend()
    except ImportError as exc:
        raise ImportError(
            "pandas/pyarrow is not installed. Install with: pip install aperiodic[pandas]"
        ) from exc


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
