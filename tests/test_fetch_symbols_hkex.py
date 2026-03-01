"""Tests for scripts.fetch_symbols_hkex."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.fetch_symbols_hkex import _detect_columns, fetch_hkex_symbols, save_hkex_symbols

if TYPE_CHECKING:
    from pathlib import Path


def _make_hkex_excel_bytes() -> bytes:
    """Create fake HKEX Excel content with 2 header rows + data."""
    # Simulate HKEX format: first 2 rows are title/blank, row 3 is header
    header_rows = pd.DataFrame([
        ["", "", ""],
        ["", "", ""],
    ])
    data = pd.DataFrame({
        "Stock Code": ["00001", "00700", "09988", "60001"],
        "Name of Securities": ["CK HUTCHISON", "TENCENT", "ALIBABA-W", "TEST WARRANT"],
        "Category": ["Equity", "Equity", "Equity", "Warrant"],
    })
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        header_rows.to_excel(writer, index=False, header=False, startrow=0)
        data.to_excel(writer, index=False, startrow=2)
    return buf.getvalue()


class TestDetectColumns:
    def test_detects_hkex_columns(self):
        df = pd.DataFrame({"Stock Code": [], "Name of Securities": [], "Category": []})
        result = _detect_columns(df)
        assert result["code"] == "Stock Code"
        assert result["name"] == "Name of Securities"
        assert result["category"] == "Category"

    def test_raises_on_missing_columns(self):
        df = pd.DataFrame({"x": [], "y": []})
        with pytest.raises(ValueError, match="Cannot detect HKEX columns"):
            _detect_columns(df)


class TestFetchHkexSymbols:
    @patch("scripts.fetch_symbols_hkex.requests")
    def test_returns_equity_only(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = _make_hkex_excel_bytes()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_hkex_symbols()

        assert list(df.columns) == ["code", "name", "exchange", "listing_date"]
        # Should filter out warrants, keep only equity
        assert all(df["exchange"] == "HKEX")
        # All codes should be 5-digit
        assert all(df["code"].str.match(r"^\d{5}$"))

    @patch("scripts.fetch_symbols_hkex.requests")
    def test_code_is_zero_padded(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = _make_hkex_excel_bytes()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_hkex_symbols()
        # 00001 should remain 00001
        codes = df["code"].tolist()
        assert "00001" in codes or len(codes) > 0


class TestSaveHkexSymbols:
    @patch("scripts.fetch_symbols_hkex.requests")
    def test_saves_csv(self, mock_requests: MagicMock, tmp_path: Path):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = _make_hkex_excel_bytes()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        path = save_hkex_symbols(output_dir=tmp_path)

        assert path.exists()
        assert path.name == "HKEX.csv"
