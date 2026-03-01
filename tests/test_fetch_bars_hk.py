"""Tests for scripts.fetch_bars_hk."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from unittest.mock import patch

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path

from scripts.fetch_bars_hk import (
    _normalize_bars_df,
    fetch_hk_bars,
    load_hk_symbols,
)


class TestNormalizeBarsDF:
    """Tests for _normalize_bars_df."""

    def test_converts_types_and_sorts(self) -> None:
        df = pd.DataFrame({
            "date": ["2025-01-03", "2025-01-02"],
            "code": ["00700", "00700"],
            "open": ["350.0", "340.0"],
            "high": ["355.0", "345.0"],
            "low": ["348.0", "338.0"],
            "close": ["353.0", "343.0"],
            "volume": ["1000000", "1200000"],
            "amount": ["350000000", "410000000"],
            "turnover": ["0.5", "0.6"],
        })
        result = _normalize_bars_df(df)
        assert result.iloc[0]["date"] == datetime.date(2025, 1, 2)
        assert result.iloc[1]["date"] == datetime.date(2025, 1, 3)
        assert result["volume"].dtype == "int64"

    def test_deduplicates(self) -> None:
        df = pd.DataFrame({
            "date": ["2025-01-02", "2025-01-02"],
            "code": ["00700", "00700"],
            "open": [350.0, 351.0],
            "high": [355.0, 356.0],
            "low": [348.0, 349.0],
            "close": [353.0, 354.0],
            "volume": [1000000, 1100000],
            "amount": [350000000, 380000000],
            "turnover": [0.5, 0.55],
        })
        result = _normalize_bars_df(df)
        assert len(result) == 1
        assert result.iloc[0]["close"] == 354.0  # keeps last

    def test_empty_dataframe(self) -> None:
        result = _normalize_bars_df(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == [
            "date", "code", "open", "high", "low", "close",
            "volume", "amount", "turnover",
        ]


class TestFetchHkBars:
    """Tests for fetch_hk_bars."""

    @patch("scripts.fetch_bars_hk.ak")
    def test_fetches_and_normalizes(self, mock_ak: object) -> None:
        mock_ak.stock_hk_hist.return_value = pd.DataFrame({
            "日期": ["2025-01-02"],
            "开盘": [350.0],
            "最高": [355.0],
            "最低": [348.0],
            "收盘": [353.0],
            "成交量": [1000000],
            "成交额": [350000000.0],
            "换手率": [0.5],
        })

        result = fetch_hk_bars(
            ["00700"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert len(result) == 1
        assert result.iloc[0]["code"] == "00700"
        assert result.iloc[0]["close"] == 353.0

        mock_ak.stock_hk_hist.assert_called_once_with(
            symbol="00700",
            period="daily",
            start_date="20250102",
            end_date="20250102",
            adjust="",
        )

    @patch("scripts.fetch_bars_hk.ak")
    def test_handles_exception(self, mock_ak: object) -> None:
        mock_ak.stock_hk_hist.side_effect = Exception("API error")
        result = fetch_hk_bars(
            ["00700"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert result.empty

    @patch("scripts.fetch_bars_hk.ak")
    def test_handles_empty_response(self, mock_ak: object) -> None:
        mock_ak.stock_hk_hist.return_value = pd.DataFrame()
        result = fetch_hk_bars(
            ["00700"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert result.empty

    def test_empty_symbols(self) -> None:
        result = fetch_hk_bars(
            [],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert result.empty

    @patch("scripts.fetch_bars_hk.ak")
    def test_multiple_symbols(self, mock_ak: object) -> None:
        def side_effect(symbol, **kwargs):
            return pd.DataFrame({
                "日期": ["2025-01-02"],
                "开盘": [350.0],
                "最高": [355.0],
                "最低": [348.0],
                "收盘": [353.0],
                "成交量": [1000000],
                "成交额": [350000000.0],
                "换手率": [0.5],
            })

        mock_ak.stock_hk_hist.side_effect = side_effect
        result = fetch_hk_bars(
            ["00700", "09988"],
            datetime.date(2025, 1, 2),
            datetime.date(2025, 1, 2),
        )
        assert len(result) == 2
        assert set(result["code"]) == {"00700", "09988"}


class TestLoadHkSymbols:
    """Tests for load_hk_symbols."""

    def test_loads_from_csv(self, tmp_path: Path) -> None:
        csv = tmp_path / "HKEX.csv"
        csv.write_text("code,name,exchange,listing_date\n00700,Tencent,HKEX,2004-06-16\n09988,Alibaba,HKEX,2019-11-26\n")
        result = load_hk_symbols(tmp_path)
        assert result == ["00700", "09988"]

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        result = load_hk_symbols(tmp_path)
        assert result == []
