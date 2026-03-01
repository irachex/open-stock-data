"""Tests for scripts.fetch_bars_cn."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.fetch_bars_cn import (
    _baostock_prefix,
    _normalize_bars_df,
    fetch_cn_bars,
    fetch_cn_bars_akshare,
    fetch_cn_bars_baostock,
    load_cn_symbols,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestBaostockPrefix:
    def test_sse_main_board(self):
        assert _baostock_prefix("600000") == "sh.600000"

    def test_sse_star_market(self):
        assert _baostock_prefix("688001") == "sh.688001"

    def test_szse_main_board(self):
        assert _baostock_prefix("000001") == "sz.000001"

    def test_szse_chinext(self):
        assert _baostock_prefix("300750") == "sz.300750"

    def test_szse_sme(self):
        assert _baostock_prefix("002714") == "sz.002714"

    def test_sse_b_shares(self):
        assert _baostock_prefix("900901") == "sh.900901"


class TestNormalizeBarsDF:
    def test_converts_types_and_sorts(self):
        df = pd.DataFrame({
            "date": ["2025-01-03", "2025-01-02"],
            "code": ["600000", "600000"],
            "open": ["10.0", "9.5"],
            "high": ["11.0", "10.0"],
            "low": ["9.5", "9.0"],
            "close": ["10.5", "9.8"],
            "volume": ["100000", "90000"],
            "amount": ["1050000", "900000"],
            "turnover": ["1.5", "1.2"],
        })
        result = _normalize_bars_df(df)

        assert result.iloc[0]["date"] == datetime.date(2025, 1, 2)  # sorted
        assert result["open"].dtype == float
        assert result["volume"].dtype == "int64"

    def test_deduplicates(self):
        df = pd.DataFrame({
            "date": ["2025-01-02", "2025-01-02"],
            "code": ["600000", "600000"],
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.5, 10.5],
            "close": [10.5, 11.5],
            "volume": [100000, 110000],
            "amount": [1050000.0, 1100000.0],
            "turnover": [1.5, 1.6],
        })
        result = _normalize_bars_df(df)
        assert len(result) == 1
        assert result.iloc[0]["close"] == 11.5  # keeps last

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = _normalize_bars_df(df)
        assert len(result) == 0


class TestFetchCnBarsBaostock:
    @patch("scripts.fetch_bars_cn.bs")
    def test_fetches_and_normalizes(self, mock_bs: MagicMock):
        # Mock login
        login_result = MagicMock()
        login_result.error_code = "0"
        mock_bs.login.return_value = login_result

        # Mock query result
        rs = MagicMock()
        rs.error_code = "0"
        rs.error_msg = ""
        # Simulate 2 rows
        rows = [
            ["2025-01-02", "sh.600000", "10.0", "11.0", "9.5", "10.5", "100000", "1050000", "1.5"],
            ["2025-01-03", "sh.600000", "10.5", "11.5", "10.0", "11.0", "110000", "1100000", "1.6"],
        ]
        call_count = {"n": 0}

        def next_side_effect():
            if call_count["n"] < len(rows):
                call_count["n"] += 1
                return True
            return False

        rs.next = next_side_effect
        rs.get_row_data = lambda: rows[call_count["n"] - 1]
        mock_bs.query_history_k_data_plus.return_value = rs

        result = fetch_cn_bars_baostock(
            ["600000"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 3),
        )

        assert len(result) == 2
        assert result.iloc[0]["code"] == "600000"  # 6-digit, not sh. prefix
        assert result.iloc[0]["date"] == datetime.date(2025, 1, 2)
        mock_bs.login.assert_called_once()
        mock_bs.logout.assert_called_once()

    @patch("scripts.fetch_bars_cn.bs")
    def test_handles_error_code(self, mock_bs: MagicMock):
        login_result = MagicMock()
        login_result.error_code = "0"
        mock_bs.login.return_value = login_result

        rs = MagicMock()
        rs.error_code = "-1"
        rs.error_msg = "some error"
        mock_bs.query_history_k_data_plus.return_value = rs

        result = fetch_cn_bars_baostock(
            ["600000"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 3),
        )

        assert len(result) == 0

    @patch("scripts.fetch_bars_cn.bs")
    def test_login_failure_raises(self, mock_bs: MagicMock):
        login_result = MagicMock()
        login_result.error_code = "-1"
        login_result.error_msg = "login failed"
        mock_bs.login.return_value = login_result

        with pytest.raises(RuntimeError, match="login failed"):
            fetch_cn_bars_baostock(
                ["600000"],
                datetime.date(2025, 1, 2),
                datetime.date(2025, 1, 3),
            )


class TestFetchCnBarsAkshare:
    @patch("scripts.fetch_bars_cn.ak")
    def test_fetches_and_normalizes(self, mock_ak: MagicMock):
        raw = pd.DataFrame({
            "日期": ["2025-01-02", "2025-01-03"],
            "开盘": [10.0, 10.5],
            "最高": [11.0, 11.5],
            "最低": [9.5, 10.0],
            "收盘": [10.5, 11.0],
            "成交量": [100000, 110000],
            "成交额": [1050000.0, 1100000.0],
            "换手率": [1.5, 1.6],
        })
        mock_ak.stock_zh_a_hist.return_value = raw

        result = fetch_cn_bars_akshare(
            ["830799"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 3),
        )

        assert len(result) == 2
        assert result.iloc[0]["code"] == "830799"

    @patch("scripts.fetch_bars_cn.ak")
    def test_handles_exception(self, mock_ak: MagicMock):
        mock_ak.stock_zh_a_hist.side_effect = Exception("network error")

        result = fetch_cn_bars_akshare(
            ["830799"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 3),
        )

        assert len(result) == 0

    @patch("scripts.fetch_bars_cn.ak")
    def test_handles_empty_response(self, mock_ak: MagicMock):
        mock_ak.stock_zh_a_hist.return_value = pd.DataFrame()

        result = fetch_cn_bars_akshare(
            ["830799"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 3),
        )

        assert len(result) == 0


class TestFetchCnBars:
    @patch("scripts.fetch_bars_cn.fetch_cn_bars_akshare")
    @patch("scripts.fetch_bars_cn.fetch_cn_bars_baostock")
    def test_combines_baostock_and_akshare(self, mock_baostock: MagicMock, mock_akshare: MagicMock):
        baostock_df = pd.DataFrame({
            "date": [datetime.date(2025, 1, 2)],
            "code": ["600000"],
            "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
            "volume": [100000], "amount": [1050000.0], "turnover": [1.5],
        })
        akshare_df = pd.DataFrame({
            "date": [datetime.date(2025, 1, 2)],
            "code": ["830799"],
            "open": [5.0], "high": [5.5], "low": [4.5], "close": [5.2],
            "volume": [50000], "amount": [260000.0], "turnover": [2.0],
        })
        mock_baostock.return_value = baostock_df
        mock_akshare.return_value = akshare_df

        result = fetch_cn_bars(
            sse_szse_symbols=["600000"],
            bse_symbols=["830799"],
            start_date=datetime.date(2025, 1, 2),
            end_date=datetime.date(2025, 1, 2),
        )

        assert len(result) == 2
        assert set(result["code"]) == {"600000", "830799"}

    @patch("scripts.fetch_bars_cn.fetch_cn_bars_akshare")
    @patch("scripts.fetch_bars_cn.fetch_cn_bars_baostock")
    def test_empty_symbols(self, mock_baostock: MagicMock, mock_akshare: MagicMock):
        result = fetch_cn_bars([], [], datetime.date(2025, 1, 2), datetime.date(2025, 1, 2))
        assert len(result) == 0
        mock_baostock.assert_not_called()
        mock_akshare.assert_not_called()


class TestLoadCnSymbols:
    def test_loads_from_csv_files(self, tmp_path: Path):
        sse_df = pd.DataFrame({"code": ["600000", "688001"], "name": ["A", "B"], "exchange": "SSE", "listing_date": ""})
        szse_df = pd.DataFrame({"code": ["000001", "300750"], "name": ["C", "D"], "exchange": "SZSE", "listing_date": ""})
        bse_df = pd.DataFrame({"code": ["830799"], "name": ["E"], "exchange": "BSE", "listing_date": ""})

        sse_df.to_csv(tmp_path / "SSE.csv", index=False)
        szse_df.to_csv(tmp_path / "SZSE.csv", index=False)
        bse_df.to_csv(tmp_path / "BSE.csv", index=False)

        sse_szse, bse = load_cn_symbols(tmp_path)

        assert len(sse_szse) == 4
        assert len(bse) == 1
        assert "600000" in sse_szse
        assert "830799" in bse

    def test_missing_files_returns_empty(self, tmp_path: Path):
        sse_szse, bse = load_cn_symbols(tmp_path)
        assert sse_szse == []
        assert bse == []
