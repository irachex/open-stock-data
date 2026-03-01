"""Tests for scripts.fetch_symbols_nasdaq."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from scripts.fetch_symbols_nasdaq import fetch_us_symbols, save_us_symbols

if TYPE_CHECKING:
    from pathlib import Path


def _make_nasdaq_response(symbols: list[dict] | None = None) -> dict:
    """Create a fake NASDAQ screener API response."""
    if symbols is None:
        symbols = [
            {"symbol": "AAPL", "name": "Apple Inc.", "lastsale": "$150.00", "marketCap": "2500000000000"},
            {"symbol": "MSFT", "name": "Microsoft Corp", "lastsale": "$300.00", "marketCap": "2300000000000"},
            {"symbol": "GOOGL", "name": "Alphabet Inc.", "lastsale": "$140.00", "marketCap": "1800000000000"},
        ]
    return {"data": {"rows": symbols}}


class TestFetchUsSymbols:
    @patch("scripts.fetch_symbols_nasdaq.requests")
    def test_returns_correct_dataframe(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _make_nasdaq_response()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_us_symbols("NASDAQ")

        assert list(df.columns) == ["code", "region", "name", "exchange", "type"]
        assert len(df) == 3
        assert df.iloc[0]["code"] == "AAPL"
        assert df.iloc[0]["region"] == "US"
        assert df.iloc[0]["exchange"] == "NASDAQ"
        assert df.iloc[0]["type"] == "stock"

    @patch("scripts.fetch_symbols_nasdaq.requests")
    def test_works_for_nyse(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _make_nasdaq_response([
            {"symbol": "JPM", "name": "JPMorgan Chase", "lastsale": "$180.00", "marketCap": "500000000000"},
        ])
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_us_symbols("NYSE")

        assert df.iloc[0]["exchange"] == "NYSE"

    def test_invalid_exchange_raises(self):
        with pytest.raises(ValueError, match="Unknown exchange"):
            fetch_us_symbols("TOKYO")

    @patch("scripts.fetch_symbols_nasdaq.requests")
    def test_case_insensitive_exchange(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _make_nasdaq_response()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_us_symbols("nasdaq")
        assert df.iloc[0]["exchange"] == "NASDAQ"

    @patch("scripts.fetch_symbols_nasdaq.requests")
    def test_empty_response(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"data": {"rows": []}}
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_us_symbols("NASDAQ")
        assert len(df) == 0
        assert list(df.columns) == ["code", "region", "name", "exchange", "type"]

    @patch("scripts.fetch_symbols_nasdaq.requests")
    def test_deduplicates_symbols(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _make_nasdaq_response([
            {"symbol": "AAPL", "name": "Apple", "lastsale": "$150"},
            {"symbol": "AAPL", "name": "Apple", "lastsale": "$150"},
        ])
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_us_symbols("NASDAQ")
        assert len(df) == 1


class TestSaveUsSymbols:
    @patch("scripts.fetch_symbols_nasdaq.requests")
    def test_saves_three_csvs(self, mock_requests: MagicMock, tmp_path: Path):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _make_nasdaq_response()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        paths = save_us_symbols(output_dir=tmp_path)

        assert len(paths) == 3
        assert (tmp_path / "NASDAQ.csv").exists()
        assert (tmp_path / "NYSE.csv").exists()
        assert (tmp_path / "AMEX.csv").exists()
