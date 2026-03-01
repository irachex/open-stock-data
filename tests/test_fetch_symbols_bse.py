"""Tests for scripts.fetch_symbols_bse."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path

from scripts.fetch_symbols_bse import fetch_bse_symbols, save_bse_symbols


def _make_bse_df(items: list[dict] | None = None) -> pd.DataFrame:
    """Create a fake ak.stock_info_bj_name_code() response."""
    if items is None:
        items = [
            {"证券代码": "830799", "证券简称": "艾融软件", "上市日期": "2021-11-15",
             "总股本": 100000000, "流通股本": 60000000, "所属行业": "软件", "地区": "上海市", "报告日期": "2026-02-27"},
            {"证券代码": "920000", "证券简称": "安徽凤凰", "上市日期": "2020-12-23",
             "总股本": 91680000, "流通股本": 57593925, "所属行业": "汽车", "地区": "安徽省", "报告日期": "2026-02-27"},
        ]
    return pd.DataFrame(items)


class TestFetchBseSymbols:
    @patch("scripts.fetch_symbols_bse.ak")
    def test_returns_correct_dataframe(self, mock_ak):
        mock_ak.stock_info_bj_name_code.return_value = _make_bse_df()

        df = fetch_bse_symbols()

        assert list(df.columns) == ["code", "name", "region", "exchange", "type", "listing_date"]
        assert len(df) == 2
        assert df.iloc[0]["code"] == "830799"
        assert df.iloc[0]["name"] == "艾融软件"
        assert df.iloc[0]["region"] == "BJ"
        assert df.iloc[0]["exchange"] == "BSE"
        assert df.iloc[0]["type"] == "stock"
        assert df.iloc[0]["listing_date"] == "2021-11-15"
        assert df.iloc[1]["name"] == "安徽凤凰"

    @patch("scripts.fetch_symbols_bse.ak")
    def test_empty_response(self, mock_ak):
        mock_ak.stock_info_bj_name_code.return_value = pd.DataFrame(
            columns=["证券代码", "证券简称", "上市日期", "总股本", "流通股本", "所属行业", "地区", "报告日期"]
        )

        df = fetch_bse_symbols()

        assert len(df) == 0
        assert list(df.columns) == ["code", "name", "region", "exchange", "type", "listing_date"]

    @patch("scripts.fetch_symbols_bse.ak")
    def test_filters_invalid_codes(self, mock_ak):
        mock_ak.stock_info_bj_name_code.return_value = pd.DataFrame({
            "证券代码": ["830799", "INVALID", "N/A"],
            "证券简称": ["艾融软件", "无效A", "无效B"],
            "上市日期": ["2021-11-15", "2020-01-01", "2020-01-01"],
            "总股本": [1, 1, 1], "流通股本": [1, 1, 1],
            "所属行业": ["软件", "软件", "软件"], "地区": ["上海市", "上海市", "上海市"],
            "报告日期": ["2026-02-27", "2026-02-27", "2026-02-27"],
        })

        df = fetch_bse_symbols()
        assert len(df) == 1
        assert df.iloc[0]["code"] == "830799"


class TestSaveBseSymbols:
    @patch("scripts.fetch_symbols_bse.ak")
    def test_saves_csv(self, mock_ak, tmp_path: Path):
        mock_ak.stock_info_bj_name_code.return_value = _make_bse_df()

        path = save_bse_symbols(output_dir=tmp_path)

        assert path.exists()
        assert path.name == "BSE.csv"
        df = pd.read_csv(path, dtype=str)
        assert len(df) == 2
        assert list(df.columns) == ["code", "name", "region", "exchange", "type", "listing_date"]

