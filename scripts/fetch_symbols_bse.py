"""Fetch stock symbols from Beijing Stock Exchange (BSE).

Data source: AKShare stock_info_bj_name_code (wraps 东方财富 data).
The old bse.cn JSON API returned 404 / 403 as of 2026.
"""

from __future__ import annotations

import logging
from pathlib import Path

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)


def fetch_bse_symbols() -> pd.DataFrame:
    """Fetch all BSE-listed stock symbols via AKShare.

    Returns:
        DataFrame with columns: code, name, region, exchange, type, listing_date
        - code: 6-digit stock code (e.g. "830799")
        - name: Company short name (Chinese)
        - region: Always "BJ"
        - exchange: Always "BSE"
        - type: Always "stock"
        - listing_date: Listing date as string "YYYY-MM-DD"

    Raises:
        Exception: If the AKShare call fails.
    """
    raw = ak.stock_info_bj_name_code()

    df = pd.DataFrame({
        "code": raw["证券代码"].astype(str).str.strip().str.zfill(6),
        "name": raw["证券简称"].astype(str).str.strip(),
        "region": "BJ",
        "exchange": "BSE",
        "type": "stock",
        "listing_date": pd.to_datetime(raw["上市日期"], errors="coerce").dt.strftime("%Y-%m-%d"),
    })

    df = df[df["code"].str.match(r"^\d{6}$", na=False)].reset_index(drop=True)
    return df


def save_bse_symbols(output_dir: str | Path = "symbols") -> Path:
    """Fetch and save BSE symbols to CSV."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = fetch_bse_symbols()
    path = output_dir / "BSE.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
