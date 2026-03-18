"""
Compatibility layer for DataFrame backends.

When polars is available (pip install aperiodic[polars]), all data is returned
as polars DataFrames (original behavior).

When polars is not available (pip install aperiodic[embedded]), pyarrow is used
to read parquet files and data is returned as pandas DataFrames - suitable for
environments like marimo where Rust-based packages (polars) cannot be installed.
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Any

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    import pyarrow.parquet as pq

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


# Re-export a generic DataFrame type for annotations
if HAS_POLARS:
    DataFrame = pl.DataFrame
else:
    try:
        import pandas as pd

        DataFrame = pd.DataFrame
    except ImportError:
        DataFrame = Any  # type: ignore[assignment,misc]


def read_parquet(buffer: BytesIO) -> Any:
    """Read a parquet file from a bytes buffer into a DataFrame."""
    if HAS_POLARS:
        return pl.read_parquet(buffer)
    if HAS_PYARROW:

        table = pq.read_table(buffer)
        return table.to_pandas()
    raise ImportError(
        "Either polars or pyarrow is required to read parquet files. "
        "Install with: pip install aperiodic[polars] or pip install aperiodic[embedded]"
    )


def concat(dataframes: list[Any]) -> Any:
    """Concatenate a list of DataFrames."""
    if HAS_POLARS:
        return pl.concat(dataframes)

    import pandas as pd

    return pd.concat(dataframes, ignore_index=True)


def empty_dataframe() -> Any:
    """Return an empty DataFrame."""
    if HAS_POLARS:
        return pl.DataFrame()

    import pandas as pd

    return pd.DataFrame()


def from_epoch_ms(df: Any, column: str) -> Any:
    """Convert an epoch-milliseconds column to datetime and add as 'datetime' column."""
    if HAS_POLARS:
        return df.with_columns(
            pl.from_epoch(column, time_unit="ms").alias("datetime")
        )

    import pandas as pd

    df = df.copy()
    df["datetime"] = pd.to_datetime(df[column], unit="ms")
    return df


def filter_datetime_range(
    df: Any,
    start_date: datetime,
    end_date: datetime,
) -> Any:
    """Filter DataFrame to rows where 'datetime' is between start_date and end_date."""
    if HAS_POLARS:
        return df.filter(
            (pl.col("datetime") >= start_date) & (pl.col("datetime") <= end_date)
        )

    mask = (df["datetime"] >= start_date) & (df["datetime"] <= end_date)
    return df.loc[mask].reset_index(drop=True)


def sort_by(df: Any, column: str) -> Any:
    """Sort DataFrame by a column."""
    if HAS_POLARS:
        return df.sort(column)

    return df.sort_values(column).reset_index(drop=True)


def has_column(df: Any, column: str) -> bool:
    """Check if DataFrame has a column."""
    return column in df.columns
