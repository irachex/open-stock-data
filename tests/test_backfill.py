"""Tests for scripts.backfill."""

from __future__ import annotations

import argparse
import datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path

from scripts.backfill import (
    MARKET_CONFIG,
    build_parser,
    fetch_bars_for_market,
    run_backfill,
)


class TestBuildParser:
    """Tests for build_parser."""

    def test_required_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--market", "cn", "--start-year", "2020"])
        assert args.market == "cn"
        assert args.start_year == 2020
        assert args.end_year is None

    def test_all_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "--market", "us",
            "--start-year", "2022",
            "--end-year", "2024",
            "--repo", "user/repo",
            "--token", "ghp_xxx",
            "--symbols-dir", "/tmp/syms",
            "--local-only",
            "--output-dir", "/tmp/out",
        ])
        assert args.market == "us"
        assert args.start_year == 2022
        assert args.end_year == 2024
        assert args.repo == "user/repo"
        assert args.token == "ghp_xxx"
        assert args.symbols_dir == "/tmp/syms"
        assert args.local_only is True
        assert args.output_dir == "/tmp/out"

    def test_market_choices(self) -> None:
        parser = build_parser()
        for market in ("cn", "hk", "us"):
            args = parser.parse_args(["--market", market, "--start-year", "2020"])
            assert args.market == market


class TestFetchBarsForMarket:
    """Tests for fetch_bars_for_market."""

    @patch("scripts.backfill.fetch_cn_bars")
    @patch("scripts.backfill.load_cn_symbols")
    def test_cn_market(self, mock_load: MagicMock, mock_fetch: MagicMock) -> None:
        mock_load.return_value = (["600000"], ["830001"])
        mock_fetch.return_value = pd.DataFrame({"date": ["2025-01-02"]})
        result = fetch_bars_for_market("cn", datetime.date(2025, 1, 2), datetime.date(2025, 1, 2), "symbols")
        mock_load.assert_called_once_with("symbols")
        mock_fetch.assert_called_once_with(["600000"], ["830001"], datetime.date(2025, 1, 2), datetime.date(2025, 1, 2))
        assert len(result) == 1

    @patch("scripts.backfill.fetch_hk_bars")
    @patch("scripts.backfill.load_hk_symbols")
    def test_hk_market(self, mock_load: MagicMock, mock_fetch: MagicMock) -> None:
        mock_load.return_value = ["00700"]
        mock_fetch.return_value = pd.DataFrame({"date": ["2025-01-02"]})
        result = fetch_bars_for_market("hk", datetime.date(2025, 1, 2), datetime.date(2025, 1, 2), "symbols")
        mock_load.assert_called_once_with("symbols")
        mock_fetch.assert_called_once()
        assert len(result) == 1

    @patch("scripts.backfill.fetch_us_bars")
    @patch("scripts.backfill.load_us_symbols")
    def test_us_market(self, mock_load: MagicMock, mock_fetch: MagicMock) -> None:
        mock_load.return_value = ["AAPL"]
        mock_fetch.return_value = pd.DataFrame({"date": ["2025-01-02"]})
        result = fetch_bars_for_market("us", datetime.date(2025, 1, 2), datetime.date(2025, 1, 2), "symbols")
        mock_load.assert_called_once_with("symbols")
        mock_fetch.assert_called_once()
        assert len(result) == 1

    def test_unknown_market(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Unknown market"):
            fetch_bars_for_market("jp", datetime.date(2025, 1, 2), datetime.date(2025, 1, 2), "symbols")


class TestRunBackfill:
    """Tests for run_backfill."""

    @patch("scripts.backfill.upload_asset")
    @patch("scripts.backfill.download_asset")
    @patch("scripts.backfill.get_or_create_release")
    @patch("scripts.backfill.fetch_bars_for_market")
    def test_local_only(
        self,
        mock_fetch: MagicMock,
        mock_release: MagicMock,
        mock_download: MagicMock,
        mock_upload: MagicMock,
        tmp_path: Path,
    ) -> None:
        bars = pd.DataFrame({
            "date": [datetime.date(2024, 1, 2)],
            "code": ["600000"],
            "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
            "volume": [1000000], "amount": [10500000.0], "turnover": [0.5],
        })
        mock_fetch.return_value = bars

        args = argparse.Namespace(
            market="cn",
            start_year=2024,
            end_year=2024,
            repo=None,
            token=None,
            symbols_dir="symbols",
            local_only=True,
            output_dir=str(tmp_path),
        )
        run_backfill(args)

        mock_fetch.assert_called_once()
        mock_release.assert_not_called()
        mock_upload.assert_not_called()
        assert (tmp_path / "cn_bars.parquet").exists()

    @patch("scripts.backfill.upload_asset")
    @patch("scripts.backfill.download_asset")
    @patch("scripts.backfill.get_or_create_release")
    @patch("scripts.backfill.fetch_bars_for_market")
    def test_upload_to_github(
        self,
        mock_fetch: MagicMock,
        mock_release: MagicMock,
        mock_download: MagicMock,
        mock_upload: MagicMock,
        tmp_path: Path,
    ) -> None:
        bars = pd.DataFrame({
            "date": [datetime.date(2024, 1, 2)],
            "code": ["00700"],
            "open": [350.0], "high": [355.0], "low": [348.0], "close": [353.0],
            "volume": [1000000], "amount": [350000000.0], "turnover": [0.5],
        })
        mock_fetch.return_value = bars
        mock_download.return_value = False

        args = argparse.Namespace(
            market="hk",
            start_year=2024,
            end_year=2024,
            repo="user/repo",
            token="ghp_xxx",
            symbols_dir="symbols",
            local_only=False,
            output_dir=str(tmp_path),
        )
        run_backfill(args)

        mock_release.assert_called_once_with("user/repo", "data-hk-bars", "ghp_xxx", name="HK Daily Bars")
        mock_upload.assert_called_once()

    @patch("scripts.backfill.fetch_bars_for_market")
    def test_empty_bars_skips(
        self,
        mock_fetch: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_fetch.return_value = pd.DataFrame()

        args = argparse.Namespace(
            market="us",
            start_year=2024,
            end_year=2024,
            repo=None,
            token=None,
            symbols_dir="symbols",
            local_only=True,
            output_dir=str(tmp_path),
        )
        run_backfill(args)
        assert not (tmp_path / "us_bars.parquet").exists()


class TestMarketConfig:
    """Tests for MARKET_CONFIG."""

    def test_all_markets_present(self) -> None:
        assert set(MARKET_CONFIG.keys()) == {"cn", "hk", "us"}

    def test_config_has_required_keys(self) -> None:
        for config in MARKET_CONFIG.values():
            assert "tag" in config
            assert "asset" in config
            assert "release_name" in config
