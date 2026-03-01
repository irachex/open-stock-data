"""Tests for scripts.fetch_bars_us."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from unittest.mock import patch

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path

from scripts.fetch_bars_us import (
    _normalize_bars_df,
    fetch_us_bars,
    load_us_symbols,
)


class TestNormalizeBarsDF:
    """Tests for _normalize_bars_df."""

    def test_converts_types_and_sorts(self) -> None:
        df = pd.DataFrame({
            "date": ["2025-01-03", "2025-01-02"],
            "code": ["AAPL", "AAPL"],
            "open": ["180.0", "175.0"],
            "high": ["185.0", "180.0"],
            "low": ["178.0", "173.0"],
            "close": ["183.0", "178.0"],
            "volume": ["50000000", "45000000"],
            "amount": ["9000000000", "8100000000"],
            "turnover": ["0.3", "0.28"],
        })
        result = _normalize_bars_df(df)
        assert result.iloc[0]["date"] == datetime.date(2025, 1, 2)
        assert result.iloc[1]["date"] == datetime.date(2025, 1, 3)
        assert result["volume"].dtype == "int64"

    def test_deduplicates(self) -> None:
        df = pd.DataFrame({
            "date": ["2025-01-02", "2025-01-02"],
            "code": ["AAPL", "AAPL"],
            "open": [180.0, 181.0],
            "high": [185.0, 186.0],
            "low": [178.0, 179.0],
            "close": [183.0, 184.0],
            "volume": [50000000, 51000000],
            "amount": [9000000000, 9300000000],
            "turnover": [0.3, 0.31],
        })
        result = _normalize_bars_df(df)
        assert len(result) == 1
        assert result.iloc[0]["close"] == 184.0

    def test_empty_dataframe(self) -> None:
        result = _normalize_bars_df(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == [
            "date", "code", "open", "high", "low", "close",
            "volume", "amount", "turnover",
        ]


class TestFetchUsBars:
    """Tests for fetch_us_bars."""

    @patch("scripts.fetch_bars_us.ak")
    def test_fetches_and_normalizes(self, mock_ak: object) -> None:
        mock_ak.stock_us_hist.return_value = pd.DataFrame({
            "日期": ["2025-01-02"],
            "开盘": [180.0],
            "最高": [185.0],
            "最低": [178.0],
            "收盘": [183.0],
            "成交量": [50000000],
            "成交额": [9000000000.0],
            "换手率": [0.3],
        })

        result = fetch_us_bars(
            ["AAPL"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert len(result) == 1
        assert result.iloc[0]["code"] == "AAPL"
        assert result.iloc[0]["close"] == 183.0

        mock_ak.stock_us_hist.assert_called_once_with(
            symbol="AAPL",
            period="daily",
            start_date="20250102",
            end_date="20250102",
            adjust="",
        )

    @patch("scripts.fetch_bars_us.ak")
    def test_handles_exception(self, mock_ak: object) -> None:
        mock_ak.stock_us_hist.side_effect = Exception("API error")
        result = fetch_us_bars(
            ["AAPL"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert result.empty

    @patch("scripts.fetch_bars_us.ak")
    def test_handles_empty_response(self, mock_ak: object) -> None:
        mock_ak.stock_us_hist.return_value = pd.DataFrame()
        result = fetch_us_bars(
            ["AAPL"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert result.empty

    def test_empty_symbols(self) -> None:
        result = fetch_us_bars(
            [],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert result.empty

    @patch("scripts.fetch_bars_us.ak")
    def test_multiple_symbols(self, mock_ak: object) -> None:
        def side_effect(symbol, **kwargs):
            return pd.DataFrame({
                "日期": ["2025-01-02"],
                "开盘": [180.0],
                "最高": [185.0],
                "最低": [178.0],
                "收盘": [183.0],
                "成交量": [50000000],
                "成交额": [9000000000.0],
                "换手率": [0.3],
            })

        mock_ak.stock_us_hist.side_effect = side_effect
        result = fetch_us_bars(
            ["AAPL", "MSFT"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert len(result) == 2
        assert set(result["code"]) == {"AAPL", "MSFT"}


class TestLoadUsSymbols:
    """Tests for load_us_symbols."""

    def test_loads_from_csv_files(self, tmp_path: Path) -> None:
        for name, codes in [("NASDAQ.csv", ["AAPL", "MSFT"]), ("NYSE.csv", ["JPM"]), ("AMEX.csv", ["SPY"])]:
            path = tmp_path / name
            path.write_text("code,name,exchange,listing_date\n" + "\n".join(f"{c},Test,EX," for c in codes) + "\n")
        result = load_us_symbols(tmp_path)
        assert result == ["AAPL", "MSFT", "JPM", "SPY"]

    def test_missing_files_returns_empty(self, tmp_path: Path) -> None:
        result = load_us_symbols(tmp_path)
        assert result == []

    def test_partial_files(self, tmp_path: Path) -> None:
        (tmp_path / "NASDAQ.csv").write_text("code,name,exchange,listing_date\nAAPL,Apple,NASDAQ,\n")
        result = load_us_symbols(tmp_path)
        assert result == ["AAPL"]
