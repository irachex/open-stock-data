"""Fetch daily bar data for CN A-share stocks.

Data sources:
- BaoStock: SSE (sh.) and SZSE (sz.) stocks — faster, more stable
- AKShare: BSE stocks — covers Beijing Stock Exchange
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import akshare as ak
import baostock as bs
import pandas as pd

if TYPE_CHECKING:
    import datetime

logger = logging.getLogger(__name__)


def fetch_cn_bars_baostock(
    symbols: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """Fetch daily bars for SSE/SZSE stocks using BaoStock.

    Args:
        symbols: List of 6-digit stock codes (e.g. ["600000", "000001"]).
        start_date: Start date (inclusive).
        end_date: End date (inclusive).

    Returns:
        DataFrame with columns: date, code, open, high, low, close, volume, amount, turnover
    """
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock login failed: {lg.error_msg}")

    try:
        return _fetch_bars_baostock_impl(symbols, start_date, end_date)
    finally:
        bs.logout()


def _baostock_prefix(code: str) -> str:
    """Determine BaoStock prefix (sh. or sz.) from stock code."""
    code = code.strip()
    if code.startswith(("6", "9")):
        return f"sh.{code}"
    return f"sz.{code}"


def _fetch_bars_baostock_impl(
    symbols: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """Internal implementation for BaoStock fetching."""
    fields = "date,code,open,high,low,close,volume,amount,turn"
    all_rows: list[dict] = []
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    for i, symbol in enumerate(symbols):
        bs_code = _baostock_prefix(symbol)
        rs = bs.query_history_k_data_plus(
            bs_code,
            fields,
            start_date=start_str,
            end_date=end_str,
            frequency="d",
            adjustflag="3",  # unadjusted
        )

        if rs.error_code != "0":
            logger.warning("BaoStock error for %s: %s", bs_code, rs.error_msg)
            continue

        while rs.next():
            row = rs.get_row_data()
            all_rows.append({
                "date": row[0],
                "code": symbol,  # Use original 6-digit code, not bs prefix
                "open": row[2],
                "high": row[3],
                "low": row[4],
                "close": row[5],
                "volume": row[6],
                "amount": row[7],
                "turnover": row[8],
            })

        if (i + 1) % 100 == 0:
            logger.info("BaoStock progress: %d / %d symbols", i + 1, len(symbols))

    if not all_rows:
        return _empty_bars_df()

    df = pd.DataFrame(all_rows)
    return _normalize_bars_df(df)


def fetch_cn_bars_akshare(
    symbols: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """Fetch daily bars for BSE stocks using AKShare.

    Args:
        symbols: List of stock codes (e.g. ["830799", "920000"]).
        start_date: Start date (inclusive).
        end_date: End date (inclusive).

    Returns:
        DataFrame with columns: date, code, open, high, low, close, volume, amount, turnover
    """
    all_dfs: list[pd.DataFrame] = []
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    for i, symbol in enumerate(symbols):
        try:
            raw = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_str,
                end_date=end_str,
                adjust="",
            )
        except Exception:
            logger.warning("AKShare error for %s", symbol, exc_info=True)
            continue

        if raw is None or raw.empty:
            continue

        df = pd.DataFrame({
            "date": pd.to_datetime(raw.iloc[:, 0]).dt.strftime("%Y-%m-%d"),
            "code": symbol,
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
            logger.info("AKShare progress: %d / %d symbols", i + 1, len(symbols))

    if not all_dfs:
        return _empty_bars_df()

    return _normalize_bars_df(pd.concat(all_dfs, ignore_index=True))


def fetch_cn_bars(
    sse_szse_symbols: list[str],
    bse_symbols: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """Fetch daily bars for all CN stocks.

    Uses BaoStock for SSE/SZSE and AKShare for BSE.

    Args:
        sse_szse_symbols: SSE + SZSE stock codes.
        bse_symbols: BSE stock codes.
        start_date: Start date (inclusive).
        end_date: End date (inclusive).

    Returns:
        Combined DataFrame with bars schema.
    """
    dfs: list[pd.DataFrame] = []

    if sse_szse_symbols:
        logger.info("Fetching %d SSE/SZSE stocks via BaoStock...", len(sse_szse_symbols))
        dfs.append(fetch_cn_bars_baostock(sse_szse_symbols, start_date, end_date))

    if bse_symbols:
        logger.info("Fetching %d BSE stocks via AKShare...", len(bse_symbols))
        dfs.append(fetch_cn_bars_akshare(bse_symbols, start_date, end_date))

    if not dfs:
        return _empty_bars_df()

    return _normalize_bars_df(pd.concat(dfs, ignore_index=True))


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


def load_cn_symbols(symbols_dir: str | Path = "symbols") -> tuple[list[str], list[str]]:
    """Load CN stock symbols from CSV files.

    Returns:
        Tuple of (sse_szse_symbols, bse_symbols).
    """
    symbols_dir = Path(symbols_dir)
    sse_szse: list[str] = []
    bse: list[str] = []

    for filename, target in [("SSE.csv", sse_szse), ("SZSE.csv", sse_szse), ("BSE.csv", bse)]:
        path = symbols_dir / filename
        if path.exists():
            df = pd.read_csv(path, dtype=str)
            codes = df["code"].dropna().str.strip().tolist()
            target.extend(codes)

    return sse_szse, bse
