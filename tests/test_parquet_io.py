"""Tests for scripts.utils.parquet_io."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import pandas as pd
import pyarrow.parquet as pq
import pytest

if TYPE_CHECKING:
    from pathlib import Path

from scripts.utils.parquet_io import (
    BARS_COLUMNS,
    BARS_SCHEMA,
    append_bars,
    read_bars,
    validate_bars_df,
    write_bars,
)


def _make_bars_df(
    codes: list[str] | None = None,
    dates: list[datetime.date] | None = None,
) -> pd.DataFrame:
    """Helper to create a small valid bars DataFrame."""
    if codes is None:
        codes = ["600000", "000001"]
    if dates is None:
        dates = [datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)]

    rows = []
    for d in dates:
        for c in codes:
            rows.append({
                "date": d,
                "code": c,
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "volume": 100000,
                "amount": 1050000.0,
                "turnover": 1.5,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# validate_bars_df
# ---------------------------------------------------------------------------


class TestValidateBarsDf:
    def test_valid_df_returns_sorted(self):
        df = _make_bars_df()
        result = validate_bars_df(df)
        assert list(result.columns) == BARS_COLUMNS
        # Should be sorted by (date, code)
        assert result["date"].is_monotonic_increasing or len(result) <= 1

    def test_deduplicates_by_date_code(self):
        df = _make_bars_df(codes=["600000"], dates=[datetime.date(2025, 1, 2)])
        # Duplicate the row with a different close price
        df2 = df.copy()
        df2["close"] = 99.0
        combined = pd.concat([df, df2], ignore_index=True)
        result = validate_bars_df(combined)
        assert len(result) == 1
        # Should keep the last (new) value
        assert result.iloc[0]["close"] == 99.0

    def test_missing_column_raises(self):
        df = _make_bars_df().drop(columns=["volume"])
        with pytest.raises(ValueError, match="volume"):
            validate_bars_df(df)

    def test_coerces_dtypes(self):
        df = _make_bars_df()
        # Pass volume as float instead of int
        df["volume"] = df["volume"].astype(float)
        result = validate_bars_df(df)
        assert result["volume"].dtype == "int64"

    def test_date_column_is_date_type(self):
        df = _make_bars_df()
        result = validate_bars_df(df)
        # date column should contain date objects (not datetime/Timestamp)
        assert hasattr(result["date"].iloc[0], "year")


# ---------------------------------------------------------------------------
# write_bars / read_bars
# ---------------------------------------------------------------------------


class TestWriteReadBars:
    def test_roundtrip(self, tmp_path: Path):
        path = tmp_path / "test.parquet"
        df = _make_bars_df()
        write_bars(path, df)

        result = read_bars(path)
        assert len(result) == len(df)
        assert list(result.columns) == BARS_COLUMNS
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            validate_bars_df(df).reset_index(drop=True),
        )

    def test_write_creates_parent_dirs(self, tmp_path: Path):
        path = tmp_path / "sub" / "dir" / "test.parquet"
        df = _make_bars_df()
        write_bars(path, df)
        assert path.exists()

    def test_write_deduplicates(self, tmp_path: Path):
        path = tmp_path / "test.parquet"
        df = _make_bars_df(codes=["600000"], dates=[datetime.date(2025, 1, 2)])
        df = pd.concat([df, df], ignore_index=True)
        write_bars(path, df)
        result = read_bars(path)
        assert len(result) == 1

    def test_read_nonexistent_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            read_bars(tmp_path / "nonexistent.parquet")

    def test_read_validates_schema(self, tmp_path: Path):
        # Write a parquet with wrong schema
        path = tmp_path / "bad.parquet"
        bad_df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
        bad_df.to_parquet(path)
        with pytest.raises(ValueError, match="column"):
            read_bars(path)

    def test_written_file_uses_snappy(self, tmp_path: Path):
        path = tmp_path / "test.parquet"
        write_bars(path, _make_bars_df())
        meta = pq.read_metadata(path)
        # Check that at least one row group uses snappy
        rg = meta.row_group(0)
        col = rg.column(0)
        assert col.compression == "SNAPPY"

    def test_written_file_has_correct_arrow_schema(self, tmp_path: Path):
        path = tmp_path / "test.parquet"
        write_bars(path, _make_bars_df())
        schema = pq.read_schema(path)
        for field in BARS_SCHEMA:
            assert field.name in schema.names
            assert schema.field(field.name).type == field.type


# ---------------------------------------------------------------------------
# append_bars
# ---------------------------------------------------------------------------


class TestAppendBars:
    def test_append_to_nonexistent_creates_file(self, tmp_path: Path):
        path = tmp_path / "new.parquet"
        df = _make_bars_df()
        append_bars(path, df)
        assert path.exists()
        result = read_bars(path)
        assert len(result) == len(df)

    def test_append_adds_new_rows(self, tmp_path: Path):
        path = tmp_path / "test.parquet"
        df1 = _make_bars_df(dates=[datetime.date(2025, 1, 2)])
        write_bars(path, df1)

        df2 = _make_bars_df(dates=[datetime.date(2025, 1, 3)])
        append_bars(path, df2)

        result = read_bars(path)
        assert len(result) == len(df1) + len(df2)

    def test_append_deduplicates_overlap(self, tmp_path: Path):
        path = tmp_path / "test.parquet"
        df1 = _make_bars_df(
            codes=["600000"],
            dates=[datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)],
        )
        write_bars(path, df1)

        # Append overlapping date with updated close
        df2 = _make_bars_df(codes=["600000"], dates=[datetime.date(2025, 1, 3)])
        df2["close"] = 99.0
        append_bars(path, df2)

        result = read_bars(path)
        assert len(result) == 2  # 2 unique (date, code) pairs
        row_jan3 = result[result["date"] == datetime.date(2025, 1, 3)]
        assert row_jan3.iloc[0]["close"] == 99.0

    def test_append_preserves_existing_data(self, tmp_path: Path):
        path = tmp_path / "test.parquet"
        df1 = _make_bars_df(codes=["600000"], dates=[datetime.date(2025, 1, 2)])
        write_bars(path, df1)

        df2 = _make_bars_df(codes=["000001"], dates=[datetime.date(2025, 1, 3)])
        append_bars(path, df2)

        result = read_bars(path)
        assert set(result["code"]) == {"600000", "000001"}
