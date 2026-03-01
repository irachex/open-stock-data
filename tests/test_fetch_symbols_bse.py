"""Tests for scripts.fetch_symbols_bse."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from scripts.fetch_symbols_bse import fetch_bse_symbols, save_bse_symbols

if TYPE_CHECKING:
    from pathlib import Path


def _make_bse_page(items: list[dict], page: int = 0, total_pages: int = 1) -> list[dict]:
    """Create a BSE API response page."""
    return [{
        "content": items,
        "page": page,
        "totalPages": total_pages,
    }]


class TestFetchBseSymbols:
    @patch("scripts.fetch_symbols_bse.requests")
    def test_returns_correct_dataframe(self, mock_requests: MagicMock):
        items = [
            {"xxzqdm": "830799", "xxzqjc": "艾融软件", "fxxsrq": "2021-11-15"},
            {"xxzqdm": "920000", "xxzqjc": "安徽凤凰", "fxxsrq": "2024-06-01"},
        ]
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _make_bse_page(items)
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_bse_symbols()

        assert list(df.columns) == ["code", "name", "region", "exchange", "type", "listing_date"]
        assert len(df) == 2
        assert df.iloc[0]["code"] == "830799"
        assert df.iloc[0]["region"] == "BJ"
        assert df.iloc[0]["exchange"] == "BSE"
        assert df.iloc[0]["type"] == "stock"
        assert df.iloc[1]["name"] == "安徽凤凰"

    @patch("scripts.fetch_symbols_bse.requests")
    def test_paginates_through_all_pages(self, mock_requests: MagicMock):
        page1_items = [{"xxzqdm": "830001", "xxzqjc": "A公司", "fxxsrq": "2021-01-01"}]
        page2_items = [{"xxzqdm": "830002", "xxzqjc": "B公司", "fxxsrq": "2021-02-01"}]

        resp1 = MagicMock()
        resp1.status_code = 200
        resp1.json.return_value = _make_bse_page(page1_items, page=0, total_pages=2)
        resp1.raise_for_status = MagicMock()

        resp2 = MagicMock()
        resp2.status_code = 200
        resp2.json.return_value = _make_bse_page(page2_items, page=1, total_pages=2)
        resp2.raise_for_status = MagicMock()

        mock_requests.get.side_effect = [resp1, resp2]

        df = fetch_bse_symbols()

        assert len(df) == 2
        assert mock_requests.get.call_count == 2

    @patch("scripts.fetch_symbols_bse.requests")
    def test_empty_response(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _make_bse_page([])
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_bse_symbols()

        assert len(df) == 0
        assert list(df.columns) == ["code", "name", "region", "exchange", "type", "listing_date"]


class TestSaveBseSymbols:
    @patch("scripts.fetch_symbols_bse.requests")
    def test_saves_csv(self, mock_requests: MagicMock, tmp_path: Path):
        items = [{"xxzqdm": "830799", "xxzqjc": "艾融软件", "fxxsrq": "2021-11-15"}]
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _make_bse_page(items)
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        path = save_bse_symbols(output_dir=tmp_path)

        assert path.exists()
        assert path.name == "BSE.csv"
