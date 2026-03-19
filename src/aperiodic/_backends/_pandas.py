"""Pandas DataFrame backend.

All DataFrame operations using only pandas and pyarrow.
Install with: pip install aperiodic[pandas]
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO

import pandas as pd
import pyarrow.parquet as pq

DataFrame = pd.DataFrame


def read_parquet(buffer: BytesIO) -> pd.DataFrame:
    """Read a parquet file from a bytes buffer into a pandas DataFrame."""
    table = pq.read_table(buffer)
    return table.to_pandas()


def concat(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate a list of pandas DataFrames."""
    return pd.concat(dataframes, ignore_index=True)


def empty_dataframe() -> pd.DataFrame:
    """Return an empty pandas DataFrame."""
    return pd.DataFrame()


def from_epoch_ms(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Convert an epoch-milliseconds column to datetime and add as 'datetime' column."""
    df = df.copy()
    df["datetime"] = pd.to_datetime(df[column], unit="ms")
    return df


def filter_datetime_range(
    df: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    """Filter DataFrame to rows where 'datetime' is between start_date and end_date."""
    mask = (df["datetime"] >= start_date) & (df["datetime"] <= end_date)
    return df.loc[mask].reset_index(drop=True)


def sort_by(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Sort DataFrame by a column."""
    return df.sort_values(column).reset_index(drop=True)


def has_column(df: pd.DataFrame, column: str) -> bool:
    """Check if DataFrame has a column."""
    return column in df.columns
