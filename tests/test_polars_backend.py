"""Tests for the polars DataFrame backend.

These tests verify that all backend functions work correctly when polars is
already installed in the environment and aperiodic is installed without the
[polars] extra (i.e. plain ``pip install aperiodic``).

When polars is present, _compat.py auto-detects it and routes all DataFrame
operations through the polars backend — no [polars] suffix required.
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO

import pytest

pl = pytest.importorskip("polars")

from aperiodic._backends._polars import (  # noqa: E402
    concat,
    empty_dataframe,
    filter_datetime_range,
    from_epoch_ms,
    has_column,
    read_parquet,
    sort_by,
)
from aperiodic._compat import HAS_POLARS  # noqa: E402


def test_compat_detects_polars():
    """Plain aperiodic install uses the polars backend when polars is present."""
    assert HAS_POLARS is True


@pytest.fixture
def sample_parquet_buffer():
    """Create a parquet buffer with sample data for testing."""
    table = pl.DataFrame(
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
    table.write_parquet(buf)
    buf.seek(0)
    return buf


class TestReadParquet:
    def test_returns_polars_dataframe(self, sample_parquet_buffer):
        result = read_parquet(sample_parquet_buffer)
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3
        assert result.columns == ["timestamp", "open", "high", "low", "close", "volume"]

    def test_values_are_correct(self, sample_parquet_buffer):
        result = read_parquet(sample_parquet_buffer)
        assert result["open"].to_list() == [42000.0, 42500.0, 43000.0]
        assert result["volume"].to_list() == [1000.0, 1500.0, 1200.0]


class TestConcat:
    def test_concatenates_polars_dataframes(self):
        df1 = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        df2 = pl.DataFrame({"a": [5, 6], "b": [7, 8]})
        result = concat([df1, df2])
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 4
        assert result["a"].to_list() == [1, 2, 5, 6]


class TestEmptyDataframe:
    def test_returns_empty_polars_dataframe(self):
        result = empty_dataframe()
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0


class TestFromEpochMs:
    def test_adds_datetime_column(self):
        df = pl.DataFrame({"timestamp": [1704067200000]})  # 2024-01-01 00:00:00 UTC
        result = from_epoch_ms(df, "timestamp")
        assert "datetime" in result.columns
        assert result["datetime"][0] == datetime(2024, 1, 1)  # noqa: DTZ001

    def test_does_not_mutate_original(self):
        df = pl.DataFrame({"timestamp": [1704067200000]})
        from_epoch_ms(df, "timestamp")
        assert "datetime" not in df.columns


class TestFilterDatetimeRange:
    def test_filters_within_range(self):
        df = pl.DataFrame(
            {
                "timestamp": [1, 2, 3],
                "datetime": [
                    datetime(2024, 1, 1),  # noqa: DTZ001
                    datetime(2024, 1, 15),  # noqa: DTZ001
                    datetime(2024, 2, 1),  # noqa: DTZ001
                ],
            }
        )
        result = filter_datetime_range(
            df,
            start_date=datetime(2024, 1, 10),  # noqa: DTZ001
            end_date=datetime(2024, 1, 20),  # noqa: DTZ001
        )
        assert len(result) == 1
        assert result["timestamp"].to_list() == [2]


class TestSortBy:
    def test_sorts_ascending(self):
        df = pl.DataFrame({"val": [3, 1, 2]})
        result = sort_by(df, "val")
        assert result["val"].to_list() == [1, 2, 3]

    def test_result_is_sorted(self):
        df = pl.DataFrame({"val": [3, 1, 2]})
        result = sort_by(df, "val")
        assert result["val"].is_sorted()


class TestHasColumn:
    def test_true_when_exists(self):
        df = pl.DataFrame({"a": [1], "b": [2]})
        assert has_column(df, "a") is True

    def test_false_when_missing(self):
        df = pl.DataFrame({"a": [1]})
        assert has_column(df, "z") is False
