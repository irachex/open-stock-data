"""Fetch stock symbols from US exchanges (NASDAQ, NYSE, AMEX).

Data source: NASDAQ Screener API (public JSON endpoint).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

NASDAQ_API_URL = "https://api.nasdaq.com/api/screener/stocks"

NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

EXCHANGES = ["NASDAQ", "NYSE", "AMEX"]


def fetch_us_symbols(exchange: str) -> pd.DataFrame:
    """Fetch stock symbols for a single US exchange.

    Args:
        exchange: One of "NASDAQ", "NYSE", "AMEX".

    Returns:
        DataFrame with columns: code, name, exchange, listing_date

    Raises:
        ValueError: If exchange is not valid.
        requests.HTTPError: If the API request fails.
    """
    exchange = exchange.upper()
    if exchange not in EXCHANGES:
        raise ValueError(f"Unknown exchange '{exchange}'. Valid: {EXCHANGES}")

    params = {
        "tableonly": "true",
        "limit": 10000,
        "offset": 0,
        "exchange": exchange.lower(),
        "download": "true",
    }
    resp = requests.get(NASDAQ_API_URL, params=params, headers=NASDAQ_HEADERS, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    rows = data.get("data", {}).get("rows", [])
    if not rows:
        return pd.DataFrame(columns=["code", "name", "region", "exchange", "type"])

    df = pd.DataFrame(rows)

    # Map NASDAQ API fields to our schema
    result = pd.DataFrame({
        "code": df.get("symbol", pd.Series(dtype=str)).astype(str).str.strip(),
        "name": df.get("name", pd.Series(dtype=str)).astype(str).str.strip(),
        "region": "US",
        "exchange": exchange,
        "type": "stock",
    })

    result = result[result["code"].str.len() > 0].drop_duplicates(subset=["code"]).reset_index(drop=True)
    return result


def save_us_symbols(output_dir: str | Path = "symbols") -> list[Path]:
    """Fetch and save symbols for all US exchanges to CSV.

    Creates NASDAQ.csv, NYSE.csv, and AMEX.csv.

    Returns:
        List of paths to saved CSV files.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for exchange in EXCHANGES:
        df = fetch_us_symbols(exchange)
        path = output_dir / f"{exchange}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Saved %d symbols to %s", len(df), path)
        paths.append(path)
    return paths
