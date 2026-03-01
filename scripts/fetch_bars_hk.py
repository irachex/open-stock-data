"""Fetch daily bar data for HK stocks.

Data source: AKShare stock_hk_hist (wraps 东方财富 API).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import akshare as ak
import pandas as pd

if TYPE_CHECKING:
    import datetime

logger = logging.getLogger(__name__)


def fetch_hk_bars(
    symbols: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """Fetch daily bars for HK stocks using AKShare.

    Args:
        symbols: List of 5-digit HKEX stock codes (e.g. ["00700", "09988"]).
        start_date: Start date (inclusive).
        end_date: End date (inclusive).

    Returns:
        DataFrame with columns: date, code, open, high, low, close, volume, amount, turnover
    """
    if not symbols:
        return _empty_bars_df()

    all_dfs: list[pd.DataFrame] = []

    for i, code in enumerate(symbols):
        try:
            raw = ak.stock_hk_hist(
                symbol=code,
                period="daily",
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
                adjust="",
            )
        except Exception:
            logger.warning("Failed to fetch HK bars for %s", code, exc_info=True)
            continue

        if raw is None or raw.empty:
            continue

        df = pd.DataFrame({
            "date": raw.iloc[:, 0],
            "code": code,
            "open": raw.iloc[:, 1],
            "high": raw.iloc[:, 2],
            "low": raw.iloc[:, 3],
            "close": raw.iloc[:, 4],
            "volume": raw.iloc[:, 5],
            "amount": raw.iloc[:, 6],
            "turnover": raw.iloc[:, 7] if raw.shape[1] > 7 else 0.0,
        })
        all_dfs.append(df)

        if (i + 1) % 50 == 0:
            logger.info("HK bars progress: %d / %d symbols", i + 1, len(symbols))

    if not all_dfs:
        return _empty_bars_df()

    return _normalize_bars_df(pd.concat(all_dfs, ignore_index=True))


def _empty_bars_df() -> pd.DataFrame:
    """Return an empty DataFrame with the bars schema."""
    return pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "volume", "amount", "turnover"])


def _normalize_bars_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a bars DataFrame: convert types, sort, deduplicate."""
    if df.empty:
        return _empty_bars_df()

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["code"] = df["code"].astype(str)
    for col in ["open", "high", "low", "close", "amount", "turnover"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

    df = df.drop_duplicates(subset=["date", "code"], keep="last")
    df = df.sort_values(["date", "code"]).reset_index(drop=True)
    return df


def load_hk_symbols(symbols_dir: str | Path = "symbols") -> list[str]:
    """Load HK stock symbols from CSV file.

    Returns:
        List of 5-digit HKEX stock codes.
    """
    path = Path(symbols_dir) / "HKEX.csv"
    if not path.exists():
        return []
    df = pd.read_csv(path, dtype=str)
    return df["code"].dropna().str.strip().tolist()
