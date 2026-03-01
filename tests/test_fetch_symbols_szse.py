"""Tests for scripts.fetch_symbols_szse."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.fetch_symbols_szse import _detect_columns, fetch_szse_symbols, save_szse_symbols

if TYPE_CHECKING:
    from pathlib import Path


def _make_szse_excel_bytes() -> bytes:
    """Create fake SZSE Excel content."""
    df = pd.DataFrame({
        "板块": ["主板", "创业板", "主板"],
        "A股代码": ["000001", "300750", "002714"],
        "A股简称": ["平安银行", "宁德时代", "牧原股份"],
        "A股上市日期": ["1991-04-03", "2018-06-11", "2014-01-28"],
    })
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


class TestDetectColumns:
    def test_detects_szse_columns(self):
        df = pd.DataFrame({"A股代码": [], "A股简称": [], "A股上市日期": []})
        result = _detect_columns(df)
        assert result["code"] == "A股代码"
        assert result["name"] == "A股简称"
        assert result["listing_date"] == "A股上市日期"

    def test_raises_on_missing_columns(self):
        df = pd.DataFrame({"x": [], "y": []})
        with pytest.raises(ValueError, match="Cannot detect SZSE columns"):
            _detect_columns(df)


class TestFetchSzseSymbols:
    @patch("scripts.fetch_symbols_szse.requests")
    def test_returns_correct_dataframe(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = _make_szse_excel_bytes()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_szse_symbols()

        assert list(df.columns) == ["code", "name", "exchange", "listing_date"]
        assert len(df) == 3
        assert df.iloc[0]["code"] == "000001"
        assert df.iloc[0]["exchange"] == "SZSE"
        assert df.iloc[1]["name"] == "宁德时代"


class TestSaveSzseSymbols:
    @patch("scripts.fetch_symbols_szse.requests")
    def test_saves_csv(self, mock_requests: MagicMock, tmp_path: Path):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = _make_szse_excel_bytes()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        path = save_szse_symbols(output_dir=tmp_path)

        assert path.exists()
        assert path.name == "SZSE.csv"
        df = pd.read_csv(path, dtype=str)
        assert len(df) == 3
