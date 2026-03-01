"""Tests for symbol CSV schema consistency.

Verifies that all symbol CSV files have identical headers and structure.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

SYMBOLS_DIR = Path(__file__).parent.parent / "symbols"

# Expected header for all symbol CSV files
EXPECTED_COLUMNS = ["code", "region", "name", "exchange", "type"]

# Minimum expected number of symbols per exchange (sanity check)
MIN_SYMBOLS = {
    "SSE": 1000,      # Several thousand SSE stocks
    "SZSE": 1500,     # Several thousand SZSE stocks
    "BSE": 100,       # Several hundred BSE stocks
    "HKEX": 100,      # Several hundred HKEX listings
    "NASDAQ": 3000,   # Several thousand NASDAQ
    "NYSE": 2000,     # Several thousand NYSE
    "AMEX": 100,      # Several hundred AMEX
}


class TestSymbolsSchemaConsistency:
    """Verify all symbol CSV files have consistent schema."""

    def test_all_csv_files_exist(self):
        """All 7 exchange CSV files should exist."""
        expected_files = {"SSE", "SZSE", "BSE", "HKEX", "NASDAQ", "NYSE", "AMEX"}
        existing_files = {f.stem for f in SYMBOLS_DIR.glob("*.csv")}
        assert expected_files.issubset(existing_files), (
            f"Missing CSV files. Expected: {expected_files}, Found: {existing_files}"
        )

    def test_all_csv_headers_are_identical(self):
        """All symbol CSV files should have identical headers."""
        headers = {}
        for csv_file in SYMBOLS_DIR.glob("*.csv"):
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False, nrows=0)
            headers[csv_file.stem] = list(df.columns)

        # All headers should match the expected columns
        for exchange, columns in headers.items():
            assert columns == EXPECTED_COLUMNS, (
                f"Invalid header for {exchange}.csv. "
                f"Expected: {EXPECTED_COLUMNS}, Got: {columns}"
            )

    def test_all_csv_headers_match_expected(self):
        """Verify headers match the expected standard schema."""
        for csv_file in SYMBOLS_DIR.glob("*.csv"):
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False, nrows=0)
            assert list(df.columns) == EXPECTED_COLUMNS, (
                f"{csv_file.name} has unexpected header: {list(df.columns)}"
            )

    def test_no_listing_date_column(self):
        """Verify no CSV has the deprecated listing_date column."""
        for csv_file in SYMBOLS_DIR.glob("*.csv"):
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False, nrows=0)
            assert "listing_date" not in df.columns, (
                f"{csv_file.name} should not have listing_date column"
            )

    def test_required_fields_not_empty(self):
        """Verify required fields (code, name, region) are not empty."""
        for csv_file in SYMBOLS_DIR.glob("*.csv"):
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
            exchange = csv_file.stem

            # Check minimum row count
            if exchange in MIN_SYMBOLS:
                assert len(df) >= MIN_SYMBOLS[exchange], (
                    f"{exchange}.csv has only {len(df)} symbols, "
                    f"expected at least {MIN_SYMBOLS[exchange]}"
                )

            # Check required fields are not empty or NaN
            for col in ["code", "name", "region", "exchange"]:
                assert not df[col].isna().any(), (
                    f"{exchange}.csv has NaN values in {col} column"
                )
                assert not (df[col] == "").any(), (
                    f"{exchange}.csv has empty strings in {col} column"
                )

    def test_region_values_are_valid(self):
        """Verify region column contains only valid region codes."""
        valid_regions = {"SH", "SZ", "BJ", "HK", "US"}

        for csv_file in SYMBOLS_DIR.glob("*.csv"):
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
            exchange = csv_file.stem

            invalid_regions = set(df["region"].unique()) - valid_regions
            assert not invalid_regions, (
                f"{exchange}.csv has invalid region values: {invalid_regions}"
            )

    def test_type_values_are_valid(self):
        """Verify type column contains only valid type codes."""
        valid_types = {"stock", "reit", "fund"}

        for csv_file in SYMBOLS_DIR.glob("*.csv"):
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
            exchange = csv_file.stem

            invalid_types = set(df["type"].unique()) - valid_types
            assert not invalid_types, (
                f"{exchange}.csv has invalid type values: {invalid_types}"
            )

    def test_code_format_is_valid(self):
        """Verify code column contains valid numeric codes."""
        us_exchanges = {"NASDAQ", "NYSE", "AMEX"}

        for csv_file in SYMBOLS_DIR.glob("*.csv"):
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
            exchange = csv_file.stem

            # US codes are typically alphanumeric
            if exchange in us_exchanges:
                assert df["code"].str.len().ge(1).all(), (
                    f"{exchange}.csv has invalid codes"
                )
            else:
                # CN/HK codes should be numeric strings
                assert df["code"].str.match(r"^\d+$", na=False).all(), (
                    f"{exchange}.csv has non-numeric codes"
                )

    def test_exchange_column_matches_filename(self):
        """Verify exchange column matches the file's exchange."""
        exchange_mapping = {
            "SSE": "SSE",
            "SZSE": "SZSE",
            "BSE": "BSE",
            "HKEX": "HKEX",
            "NASDAQ": "NASDAQ",
            "NYSE": "NYSE",
            "AMEX": "AMEX",
        }

        for csv_file in SYMBOLS_DIR.glob("*.csv"):
            df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
            exchange = csv_file.stem
            expected_exchange = exchange_mapping[exchange]

            # All rows should have the correct exchange in exchange column
            assert (df["exchange"] == expected_exchange).all(), (
                f"{exchange}.csv has mismatched exchange values"
            )

    def test_csv_is_consistent_across_multiple_reads(self):
        """Verify CSV structure is stable across multiple reads."""
        for csv_file in SYMBOLS_DIR.glob("*.csv"):
            df1 = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
            df2 = pd.read_csv(csv_file, dtype=str, keep_default_na=False)

            pd.testing.assert_frame_equal(df1, df2)
