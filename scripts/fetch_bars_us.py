"""Fetch daily bar data for US stocks.

Data source: AKShare stock_us_hist (wraps 东方财富 API).
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


def fetch_us_bars(
    symbols: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """Fetch daily bars for US stocks using AKShare.

    Args:
        symbols: List of US ticker symbols (e.g. ["AAPL", "MSFT"]).
        start_date: Start date (inclusive).
        end_date: End date (inclusive).

    Returns:
        DataFrame with columns: date, code, open, high, low, close, volume, amount, turnover
    """
    if not symbols:
        return _empty_bars_df()

    all_dfs: list[pd.DataFrame] = []

    for i, symbol in enumerate(symbols):
        try:
            raw = ak.stock_us_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
                adjust="",
            )
        except Exception:
            logger.warning("Failed to fetch US bars for %s", symbol, exc_info=True)
            continue

        if raw is None or raw.empty:
            continue

        df = pd.DataFrame({
            "date": raw.iloc[:, 0],
            "code": symbol,
            "open": raw.iloc[:, 1],
            "high": raw.iloc[:, 2],
            "low": raw.iloc[:, 3],
            "close": raw.iloc[:, 4],
            "volume": raw.iloc[:, 5],
            "amount": raw.iloc[:, 6] if raw.shape[1] > 6 else 0.0,
            "turnover": raw.iloc[:, 7] if raw.shape[1] > 7 else 0.0,
        })
        all_dfs.append(df)

        if (i + 1) % 50 == 0:
            logger.info("US bars progress: %d / %d symbols", i + 1, len(symbols))

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


def load_us_symbols(symbols_dir: str | Path = "symbols") -> list[str]:
    """Load US stock symbols from CSV files.

    Returns:
        List of US ticker symbols from NASDAQ, NYSE, and AMEX.
    """
    symbols_dir = Path(symbols_dir)
    all_symbols: list[str] = []

    for filename in ("NASDAQ.csv", "NYSE.csv", "AMEX.csv"):
        path = symbols_dir / filename
        if path.exists():
            df = pd.read_csv(path, dtype=str)
            codes = df["code"].dropna().str.strip().tolist()
            all_symbols.extend(codes)

    return all_symbols
