"""Fetch stock symbols from Shenzhen Stock Exchange (SZSE).

Data source: AKShare stock_info_sz_name_code (wraps 东方财富 data).
The official SZSE API (szse.cn) is blocked from GitHub Actions runners.
"""

from __future__ import annotations

import logging
from pathlib import Path

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)


def fetch_szse_symbols() -> pd.DataFrame:
    """Fetch all SZSE-listed A-share stock symbols via AKShare.

    Returns:
        DataFrame with columns: code, region, name, exchange, type
        - code: 6-digit stock code (e.g. "000001")
        - region: Always "SZ"
        - name: Company short name (Chinese)
        - exchange: Always "SZSE"
        - type: Always "stock"

    Raises:
        Exception: If the AKShare call fails.
    """
    raw = ak.stock_info_sz_name_code()

    df = pd.DataFrame(
        {
            "code": raw["A股代码"].astype(str).str.strip().str.zfill(6),
            "region": "SZ",
            "name": raw["A股简称"].astype(str).str.strip().str.replace(r"\s+", "", regex=True),
            "exchange": "SZSE",
            "type": "stock",
        }
    )

    df = df[df["code"].str.match(r"^\d{6}$", na=False)].reset_index(drop=True)
    return df


def save_szse_symbols(output_dir: str | Path = "symbols") -> Path:
    """Fetch and save SZSE symbols to CSV."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = fetch_szse_symbols()
    path = output_dir / "SZSE.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
