"""Tests for scripts.fetch_symbols_szse."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pandas as pd

from scripts.fetch_symbols_szse import fetch_szse_symbols, save_szse_symbols

if TYPE_CHECKING:
    from pathlib import Path


def _make_akshare_df() -> pd.DataFrame:
    """Create fake AKShare DataFrame."""
    return pd.DataFrame(
        {
            "板块": ["主板", "创业板", "主板"],
            "A股代码": ["000001", "300750", "002714"],
            "A股简称": ["平安银行", "宁德时代", "牧原股份"],
            "A股上市日期": ["1991-04-03", "2018-06-11", "2014-01-28"],
            "A股总股本": ["1", "2", "3"],
            "A股流通股本": ["1", "2", "3"],
            "所属行业": ["J", "C", "A"],
        }
    )


class TestFetchSzseSymbols:
    @patch("scripts.fetch_symbols_szse.ak.stock_info_sz_name_code")
    def test_returns_correct_dataframe(self, mock_func: MagicMock):
        mock_func.return_value = _make_akshare_df()

        df = fetch_szse_symbols()

        assert list(df.columns) == ["code", "region", "name", "exchange", "type"]
        assert len(df) == 3
        assert df.iloc[0]["code"] == "000001"
        assert df.iloc[0]["region"] == "SZ"
        assert df.iloc[0]["exchange"] == "SZSE"
        assert df.iloc[0]["type"] == "stock"
        assert df.iloc[1]["name"] == "宁德时代"

    @patch("scripts.fetch_symbols_szse.ak.stock_info_sz_name_code")
    def test_removes_internal_spaces(self, mock_func: MagicMock):
        """Names with internal spaces (e.g. '万  科A') should have spaces removed."""
        mock_func.return_value = pd.DataFrame(
            {
                "板块": ["主板", "主板"],
                "A股代码": ["000001", "000002"],
                "A股简称": ["万  科A", "中  兴通  讯"],
                "A股上市日期": ["1991-04-03", "1997-04-29"],
                "A股总股本": ["1", "2"],
                "A股流通股本": ["1", "2"],
                "所属行业": ["K", "I"],
            }
        )

        df = fetch_szse_symbols()
        assert df.iloc[0]["name"] == "万科A"
        assert df.iloc[1]["name"] == "中兴通讯"

    @patch("scripts.fetch_symbols_szse.ak.stock_info_sz_name_code")
    def test_filters_invalid_codes(self, mock_func: MagicMock):
        """Only 6-digit numeric codes should be kept."""
        mock_func.return_value = pd.DataFrame(
            {
                "板块": ["主板", "主板"],
                "A股代码": ["000001", "invalid"],
                "A股简称": ["平安银行", "Test"],
                "A股上市日期": ["1991-04-03", "2020-01-01"],
                "A股总股本": ["1", "2"],
                "A股流通股本": ["1", "2"],
                "所属行业": ["J", "C"],
            }
        )

        df = fetch_szse_symbols()
        assert len(df) == 1
        assert df.iloc[0]["code"] == "000001"


class TestSaveSzseSymbols:
    @patch("scripts.fetch_symbols_szse.ak.stock_info_sz_name_code")
    def test_saves_csv(self, mock_func: MagicMock, tmp_path: Path):
        mock_func.return_value = _make_akshare_df()

        path = save_szse_symbols(output_dir=tmp_path)

        assert path.exists()
        assert path.name == "SZSE.csv"
        df = pd.read_csv(path, dtype=str)
        assert len(df) == 3
