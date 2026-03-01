"""Fetch stock symbols from Beijing Stock Exchange (BSE).

Data source: BSE official JSON API.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BSE_URL = "https://www.bse.cn/nqxxController/nqxxCnAssign.do"

BSE_HEADERS = {
    "Referer": "https://www.bse.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


def fetch_bse_symbols() -> pd.DataFrame:
    """Fetch all BSE-listed stock symbols.

    Paginates through the BSE API to collect all listed companies.

    Returns:
        DataFrame with columns: code, name, exchange, listing_date

    Raises:
        requests.HTTPError: If the BSE API request fails.
        ValueError: If the response cannot be parsed.
    """
    all_rows: list[dict[str, str]] = []
    page = 0

    while True:
        params = {
            "page": page,
            "typejb": "T",
            "xxfcbj[]": "2",
            "sortfield": "xxzqdm",
            "sorttype": "asc",
        }
        resp = requests.get(BSE_URL, params=params, headers=BSE_HEADERS, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        # BSE API wraps data in a list with one element
        if isinstance(data, list) and len(data) > 0:
            data = data[0]

        content = data.get("content", [])
        if not content:
            break

        for item in content:
            all_rows.append({
                "code": str(item.get("xxzqdm", "")).strip(),
                "name": str(item.get("xxzqjc", "")).strip(),
                "region": "BJ",
                "exchange": "BSE",
                "type": "stock",
                "listing_date": str(item.get("fxxsrq", "")).strip()[:10],
            })

        total_pages = int(data.get("totalPages", 1))
        page += 1
        if page >= total_pages:
            break

    df = pd.DataFrame(all_rows)
    if df.empty:
        df = pd.DataFrame(columns=["code", "name", "region", "exchange", "type", "listing_date"])

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
