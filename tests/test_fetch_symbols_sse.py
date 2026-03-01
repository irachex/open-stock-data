"""Tests for scripts.fetch_symbols_sse."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.fetch_symbols_sse import _detect_columns, fetch_sse_symbols, save_sse_symbols

if TYPE_CHECKING:
    from pathlib import Path


def _make_sse_excel_bytes() -> bytes:
    """Create fake SSE Excel content matching the expected format."""
    df = pd.DataFrame({
        "公司代码": ["600000", "600001", "688001"],
        "公司简称": ["浦发银行", "邯郸钢铁", "华兴源创"],
        "上市日期": ["1999-11-10", "1997-06-16", "2019-07-22"],
    })
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


class TestDetectColumns:
    def test_detects_chinese_columns(self):
        df = pd.DataFrame({"公司代码": [], "公司简称": [], "上市日期": []})
        result = _detect_columns(df)
        assert result["code"] == "公司代码"
        assert result["name"] == "公司简称"
        assert result["listing_date"] == "上市日期"

    def test_detects_alternative_columns(self):
        df = pd.DataFrame({"股票代码": [], "股票简称": [], "上市日期": []})
        result = _detect_columns(df)
        assert result["code"] == "股票代码"
        assert result["name"] == "股票简称"

    def test_raises_on_missing_columns(self):
        df = pd.DataFrame({"x": [], "y": []})
        with pytest.raises(ValueError, match="Cannot detect SSE columns"):
            _detect_columns(df)


class TestFetchSseSymbols:
    @patch("scripts.fetch_symbols_sse.requests")
    def test_returns_correct_dataframe(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = _make_sse_excel_bytes()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_sse_symbols()

        assert list(df.columns) == ["code", "name", "exchange", "listing_date"]
        assert len(df) == 3
        assert df.iloc[0]["code"] == "600000"
        assert df.iloc[0]["name"] == "浦发银行"
        assert df.iloc[0]["exchange"] == "SSE"
        assert df.iloc[0]["listing_date"] == "1999-11-10"

    @patch("scripts.fetch_symbols_sse.requests")
    def test_code_is_zero_padded(self, mock_requests: MagicMock):
        """Codes shorter than 6 digits should be zero-padded."""
        excel_df = pd.DataFrame({
            "公司代码": ["1", "600000"],
            "公司简称": ["测试A", "测试B"],
            "上市日期": ["2020-01-01", "2020-01-01"],
        })
        buf = io.BytesIO()
        excel_df.to_excel(buf, index=False, engine="openpyxl")

        resp = MagicMock()
        resp.status_code = 200
        resp.content = buf.getvalue()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        df = fetch_sse_symbols()
        assert df.iloc[0]["code"] == "000001"

    @patch("scripts.fetch_symbols_sse.requests")
    def test_sends_referer_header(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = _make_sse_excel_bytes()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        fetch_sse_symbols()

        call_kwargs = mock_requests.get.call_args
        headers = call_kwargs[1].get("headers") or call_kwargs.kwargs.get("headers", {})
        assert "sse.com.cn" in headers.get("Referer", "")


class TestSaveSseSymbols:
    @patch("scripts.fetch_symbols_sse.requests")
    def test_saves_csv(self, mock_requests: MagicMock, tmp_path: Path):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = _make_sse_excel_bytes()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        path = save_sse_symbols(output_dir=tmp_path)

        assert path.exists()
        assert path.name == "SSE.csv"
        df = pd.read_csv(path, dtype=str)
        assert len(df) == 3
        assert list(df.columns) == ["code", "name", "exchange", "listing_date"]
