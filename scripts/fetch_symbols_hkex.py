"""Fetch stock symbols from Hong Kong Exchange (HKEX).

Data source: HKEX official ListOfSecurities Excel file.
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import requests

HKEX_URL = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"

HKEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


def fetch_hkex_symbols() -> pd.DataFrame:
    """Fetch all HKEX-listed equity stock symbols.

    Downloads the official HKEX securities list, filters for equity
    categories, and returns a normalized DataFrame.

    Returns:
        DataFrame with columns: code, name, exchange, listing_date

    Raises:
        requests.HTTPError: If the download fails.
        ValueError: If the file cannot be parsed.
    """
    resp = requests.get(HKEX_URL, headers=HKEX_HEADERS, timeout=60)
    resp.raise_for_status()

    raw = pd.read_excel(io.BytesIO(resp.content), dtype=str, header=2)
    col_map = _detect_columns(raw)

    df = pd.DataFrame({
        "code": raw[col_map["code"]].astype(str).str.strip().str.zfill(5),
        "name": raw[col_map["name"]].astype(str).str.strip(),
        "exchange": "HKEX",
        "listing_date": "",  # HKEX list doesn't always include listing date
    })

    # Filter out non-equity entries if category column exists
    if "category" in col_map:
        category = raw[col_map["category"]].astype(str).str.strip().str.upper()
        equity_mask = category.str.contains("EQUITY|ORD|SHARES", na=False)
        df = df[equity_mask]

    # Drop rows with invalid codes
    df = df[df["code"].str.match(r"^\d{5}$", na=False)]
    df = df.drop_duplicates(subset=["code"]).reset_index(drop=True)
    return df


def _detect_columns(raw: pd.DataFrame) -> dict[str, str]:
    """Detect column name mapping from the HKEX Excel data."""
    columns = list(raw.columns)
    col_map: dict[str, str] = {}

    for col in columns:
        col_upper = str(col).strip().upper()
        if "STOCK CODE" in col_upper or "CODE" in col_upper:
            col_map.setdefault("code", col)
        elif "NAME OF SECURITIES" in col_upper or "NAME" in col_upper:
            col_map.setdefault("name", col)
        elif "CATEGORY" in col_upper:
            col_map["category"] = col

    required = {"code", "name"}
    missing = required - set(col_map)
    if missing:
        raise ValueError(f"Cannot detect HKEX columns. Missing: {missing}. Available: {columns}")

    return col_map


def save_hkex_symbols(output_dir: str | Path = "symbols") -> Path:
    """Fetch and save HKEX symbols to CSV."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = fetch_hkex_symbols()
    path = output_dir / "HKEX.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
