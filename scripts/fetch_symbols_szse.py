"""Fetch stock symbols from Shenzhen Stock Exchange (SZSE).

Data source: SZSE official API returning Excel format.
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import requests

# SZSE API endpoint for A-share list (TABKEY=tab1 = A-shares)
SZSE_URL = (
    "http://www.szse.cn/api/report/ShowReport"
    "?SHOWTYPE=xlsx"
    "&CATALOGID=1110"
    "&TABKEY=tab1"
    "&random=0.5"
)

SZSE_HEADERS = {
    "Referer": "http://www.szse.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


def fetch_szse_symbols() -> pd.DataFrame:
    """Fetch all SZSE-listed A-share stock symbols.

    Returns:
        DataFrame with columns: code, region, name, exchange, type

    Raises:
        requests.HTTPError: If the SZSE API request fails.
        ValueError: If the response cannot be parsed.
    """
    resp = requests.get(SZSE_URL, headers=SZSE_HEADERS, timeout=30)
    resp.raise_for_status()

    raw = pd.read_excel(io.BytesIO(resp.content), dtype=str)
    col_map = _detect_columns(raw)

    df = pd.DataFrame({
        "code": raw[col_map["code"]].astype(str).str.strip().str.zfill(6),
        "region": "SZ",
        "name": raw[col_map["name"]].astype(str).str.strip().str.replace(r"\s+", "", regex=True),
        "exchange": "SZSE",
        "type": "stock",
    })

    df = df[df["code"].str.match(r"^\d{6}$", na=False)].reset_index(drop=True)
    return df


def _detect_columns(raw: pd.DataFrame) -> dict[str, str]:
    """Detect column name mapping from the raw SZSE Excel data."""
    columns = list(raw.columns)
    col_map: dict[str, str] = {}

    for col in columns:
        col_str = str(col).strip()
        # Skip English-name columns explicitly
        if "英文" in col_str:
            continue
        if "代码" in col_str:
            col_map.setdefault("code", col)
        elif "A股简称" in col_str:
            col_map["name"] = col  # prefer A股简称 over generic 简称/名称
        elif ("简称" in col_str or "名称" in col_str) and "name" not in col_map:
            col_map["name"] = col
        elif "上市日期" in col_str:
            col_map.setdefault("listing_date", col)

    required = {"code", "name"}
    missing = required - set(col_map)
    if missing:
        raise ValueError(f"Cannot detect SZSE columns. Missing: {missing}. Available: {columns}")

    return col_map


def save_szse_symbols(output_dir: str | Path = "symbols") -> Path:
    """Fetch and save SZSE symbols to CSV."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = fetch_szse_symbols()
    path = output_dir / "SZSE.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
