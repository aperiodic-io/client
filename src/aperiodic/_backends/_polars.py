"""Polars DataFrame backend.

All DataFrame operations using only polars.
Install with: pip install aperiodic[polars]
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO

import polars as pl

DataFrame = pl.DataFrame


def read_parquet(buffer: BytesIO) -> pl.DataFrame:
    """Read a parquet file from a bytes buffer into a polars DataFrame."""
    return pl.read_parquet(buffer)


def concat(dataframes: list[pl.DataFrame]) -> pl.DataFrame:
    """Concatenate a list of polars DataFrames."""
    return pl.concat(dataframes)


def empty_dataframe() -> pl.DataFrame:
    """Return an empty polars DataFrame."""
    return pl.DataFrame()


def from_epoch_ms(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """Convert an epoch-milliseconds column to datetime and add as 'datetime' column."""
    return df.with_columns(pl.from_epoch(column, time_unit="ms").alias("datetime"))


def filter_datetime_range(
    df: pl.DataFrame,
    start_date: datetime,
    end_date: datetime,
) -> pl.DataFrame:
    """Filter DataFrame to rows where 'datetime' is between start_date and end_date."""
    return df.filter(
        (pl.col("datetime") >= start_date) & (pl.col("datetime") <= end_date)
    )


def sort_by(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """Sort DataFrame by a column."""
    return df.sort(column)


def has_column(df: pl.DataFrame, column: str) -> bool:
    """Check if DataFrame has a column."""
    return column in df.columns
