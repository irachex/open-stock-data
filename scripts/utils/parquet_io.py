"""Parquet I/O utilities for daily bar data.

Provides read/write/append operations on yearly parquet files with a fixed
schema for OHLCV bar data.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Canonical schema for daily bar parquet files.
BARS_SCHEMA = pa.schema([
    pa.field("date", pa.date32()),
    pa.field("code", pa.string()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("amount", pa.float64()),
    pa.field("turnover", pa.float64()),
])

BARS_COLUMNS: list[str] = [field.name for field in BARS_SCHEMA]

# Arrow type mapping for coercion
_ARROW_TYPE_MAP: dict[str, str] = {
    "date": "object",      # will hold datetime.date objects
    "code": "object",      # string
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "int64",
    "amount": "float64",
    "turnover": "float64",
}


def read_bars(path: str | Path) -> pd.DataFrame:
    """Read a parquet file and return a DataFrame with the bars schema.

    Args:
        path: Path to the parquet file.

    Returns:
        DataFrame with columns matching BARS_COLUMNS, sorted by (date, code).

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file schema does not match BARS_SCHEMA.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    table = pq.read_table(path)
    schema_names = set(table.schema.names)
    missing = set(BARS_COLUMNS) - schema_names
    if missing:
        raise ValueError(f"Parquet file is missing required column(s): {', '.join(sorted(missing))}")

    # Select only our columns and cast to expected schema
    table = table.select(BARS_COLUMNS)
    table = table.cast(BARS_SCHEMA)
    df = table.to_pandas()

    # Ensure date column contains python date objects
    if hasattr(df["date"].dtype, "tz") or pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = df["date"].dt.date

    df = df.sort_values(["date", "code"]).reset_index(drop=True)
    return df


def write_bars(path: str | Path, df: pd.DataFrame) -> None:
    """Write a DataFrame to a parquet file using the bars schema.

    The DataFrame is validated, deduplicated by (date, code), sorted,
    and written with snappy compression.

    Args:
        path: Destination parquet file path. Parent dirs are created if needed.
        df: DataFrame with columns matching BARS_COLUMNS.

    Raises:
        ValueError: If required columns are missing.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = validate_bars_df(df)
    table = pa.Table.from_pandas(df, schema=BARS_SCHEMA, preserve_index=False)
    pq.write_table(table, path, compression="snappy")


def append_bars(path: str | Path, new_df: pd.DataFrame) -> None:
    """Append new bar rows to an existing parquet file.

    If the file does not exist, it is created. Duplicate (date, code) rows
    are resolved by keeping the new data.

    Args:
        path: Path to the parquet file.
        new_df: New rows to append, with columns matching BARS_COLUMNS.

    Raises:
        ValueError: If required columns are missing in new_df.
    """
    path = Path(path)
    if path.exists():
        existing = read_bars(path)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    write_bars(path, combined)


def validate_bars_df(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a DataFrame to match the bars schema.

    - Checks all required columns are present.
    - Coerces dtypes to match BARS_SCHEMA.
    - Drops duplicates by (date, code), keeping last.
    - Sorts by (date, code).

    Args:
        df: Input DataFrame.

    Returns:
        Cleaned DataFrame.

    Raises:
        ValueError: If required columns are missing.
    """
    missing = set(BARS_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required column(s): {', '.join(sorted(missing))}")

    df = df[BARS_COLUMNS].copy()

    # Coerce date column
    if pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = df["date"].dt.date
    elif not all(isinstance(v, __import__("datetime").date) for v in df["date"].head(1)):
        df["date"] = pd.to_datetime(df["date"]).dt.date

    # Coerce numeric columns
    for col in ["open", "high", "low", "close", "amount", "turnover"]:
        df[col] = df[col].astype("float64")
    df["volume"] = df["volume"].astype("int64")
    df["code"] = df["code"].astype(str)

    # Deduplicate and sort
    df = df.drop_duplicates(subset=["date", "code"], keep="last")
    df = df.sort_values(["date", "code"]).reset_index(drop=True)
    return df
