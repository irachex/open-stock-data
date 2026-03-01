"""Fetch stock symbols from Shanghai Stock Exchange (SSE).

Data source: SSE official query API returning Excel format.
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import requests

# SSE query endpoint for listed stocks
SSE_URL = (
    "http://query.sse.com.cn/sseQuery/commonExcelDd.do"
    "?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L"
    "&type=inParams"
    "&CSRC_CODE="
    "&STOCK_CODE="
    "&REG_PROVINCE="
    "&STOCK_TYPE=1"
    "&COMPANY_STATUS=2,4,5,7,8"
)

SSE_HEADERS = {
    "Referer": "http://www.sse.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


def fetch_sse_symbols() -> pd.DataFrame:
    """Fetch all SSE-listed A-share stock symbols.

    Returns:
        DataFrame with columns: code, name, exchange, listing_date
        - code: 6-digit stock code (e.g. "600000")
        - name: Company short name (Chinese)
        - exchange: Always "SSE"
        - listing_date: Listing date as string "YYYY-MM-DD"

    Raises:
        requests.HTTPError: If the SSE API request fails.
        ValueError: If the response cannot be parsed.
    """
    resp = requests.get(SSE_URL, headers=SSE_HEADERS, timeout=30)
    resp.raise_for_status()

    raw = pd.read_excel(io.BytesIO(resp.content), dtype=str)

    # SSE Excel columns vary; find the right ones by matching known patterns
    # Typical columns: 公司代码, 公司简称, 上市日期
    col_map = _detect_columns(raw)

    df = pd.DataFrame({
        "code": raw[col_map["code"]].astype(str).str.strip().str.zfill(6),
        "name": raw[col_map["name"]].astype(str).str.strip(),
        "exchange": "SSE",
        "listing_date": pd.to_datetime(raw[col_map["listing_date"]], errors="coerce").dt.strftime("%Y-%m-%d"),
    })

    df = df.dropna(subset=["code"]).reset_index(drop=True)
    return df


def _detect_columns(raw: pd.DataFrame) -> dict[str, str]:
    """Detect column name mapping from the raw SSE Excel data."""
    columns = list(raw.columns)
    col_map: dict[str, str] = {}

    for col in columns:
        col_lower = str(col).strip()
        if "代码" in col_lower or "STOCK_CODE" in col_lower.upper():
            col_map["code"] = col
        elif "简称" in col_lower or "STOCK_NAME" in col_lower.upper():
            col_map["name"] = col
        elif "上市日期" in col_lower or "LIST_DATE" in col_lower.upper():
            col_map["listing_date"] = col

    required = {"code", "name", "listing_date"}
    missing = required - set(col_map)
    if missing:
        raise ValueError(f"Cannot detect SSE columns. Missing: {missing}. Available: {columns}")

    return col_map


def save_sse_symbols(output_dir: str | Path = "symbols") -> Path:
    """Fetch and save SSE symbols to CSV.

    Args:
        output_dir: Directory to save SSE.csv.

    Returns:
        Path to the saved CSV file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = fetch_sse_symbols()
    path = output_dir / "SSE.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
