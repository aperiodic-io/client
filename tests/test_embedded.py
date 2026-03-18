"""Tests for the pandas DataFrame backend.

These tests verify that all backend functions work correctly when using
aperiodic[pandas] (pyarrow + pandas), simulating environments where polars
cannot be installed (e.g. marimo).
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from aperiodic._backends._pandas import (  # noqa: E402
    concat,
    empty_dataframe,
    filter_datetime_range,
    from_epoch_ms,
    has_column,
    read_parquet,
    sort_by,
)


@pytest.fixture
def sample_parquet_buffer():
    """Create a parquet buffer with sample data for testing."""
    table = pa.table(
        {
            "timestamp": [1704067200000, 1704153600000, 1704240000000],  # 2024-01-01, 02, 03 in ms
            "open": [42000.0, 42500.0, 43000.0],
            "high": [42800.0, 43200.0, 43500.0],
            "low": [41500.0, 42000.0, 42800.0],
            "close": [42500.0, 43000.0, 43200.0],
            "volume": [1000.0, 1500.0, 1200.0],
        }
    )
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf


class TestReadParquet:
    def test_returns_pandas_dataframe(self, sample_parquet_buffer):
        result = read_parquet(sample_parquet_buffer)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ["timestamp", "open", "high", "low", "close", "volume"]

    def test_values_are_correct(self, sample_parquet_buffer):
        result = read_parquet(sample_parquet_buffer)
        assert result["open"].tolist() == [42000.0, 42500.0, 43000.0]
        assert result["volume"].tolist() == [1000.0, 1500.0, 1200.0]


class TestConcat:
    def test_concatenates_pandas_dataframes(self):
        df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df2 = pd.DataFrame({"a": [5, 6], "b": [7, 8]})
        result = concat([df1, df2])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        assert result["a"].tolist() == [1, 2, 5, 6]

    def test_resets_index(self):
        df1 = pd.DataFrame({"a": [1]}, index=[5])
        df2 = pd.DataFrame({"a": [2]}, index=[10])
        result = concat([df1, df2])
        assert result.index.tolist() == [0, 1]


class TestEmptyDataframe:
    def test_returns_empty_pandas_dataframe(self):
        result = empty_dataframe()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestFromEpochMs:
    def test_adds_datetime_column(self):
        df = pd.DataFrame({"timestamp": [1704067200000]})  # 2024-01-01 00:00:00 UTC
        result = from_epoch_ms(df, "timestamp")
        assert "datetime" in result.columns
        assert result["datetime"].iloc[0] == pd.Timestamp("2024-01-01")

    def test_does_not_mutate_original(self):
        df = pd.DataFrame({"timestamp": [1704067200000]})
        from_epoch_ms(df, "timestamp")
        assert "datetime" not in df.columns


class TestFilterDatetimeRange:
    def test_filters_within_range(self):
        df = pd.DataFrame(
            {
                "timestamp": [1, 2, 3],
                "datetime": pd.to_datetime(
                    ["2024-01-01", "2024-01-15", "2024-02-01"]
                ),
            }
        )
        result = filter_datetime_range(
            df,
            start_date=datetime(2024, 1, 10),  # noqa: DTZ001
            end_date=datetime(2024, 1, 20),  # noqa: DTZ001
        )
        assert len(result) == 1
        assert result["timestamp"].tolist() == [2]

    def test_resets_index_after_filter(self):
        df = pd.DataFrame(
            {
                "datetime": pd.to_datetime(
                    ["2024-01-01", "2024-01-15", "2024-02-01"]
                ),
            }
        )
        result = filter_datetime_range(
            df,
            start_date=datetime(2024, 1, 10),  # noqa: DTZ001
            end_date=datetime(2024, 2, 15),  # noqa: DTZ001
        )
        assert result.index.tolist() == [0, 1]


class TestSortBy:
    def test_sorts_ascending(self):
        df = pd.DataFrame({"val": [3, 1, 2]})
        result = sort_by(df, "val")
        assert result["val"].tolist() == [1, 2, 3]

    def test_resets_index_after_sort(self):
        df = pd.DataFrame({"val": [3, 1, 2]})
        result = sort_by(df, "val")
        assert result.index.tolist() == [0, 1, 2]


class TestHasColumn:
    def test_true_when_exists(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        assert has_column(df, "a") is True

    def test_false_when_missing(self):
        df = pd.DataFrame({"a": [1]})
        assert has_column(df, "z") is False
