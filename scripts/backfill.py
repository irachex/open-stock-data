"""Backfill historical daily bar data.

CLI tool for initial data backfill. Fetches historical bars in yearly
chunks and uploads to GitHub Releases as parquet files.

Usage:
    uv run python -m scripts.backfill --market cn --start-year 2020 --end-year 2024
    uv run python -m scripts.backfill --market hk --start-year 2020
    uv run python -m scripts.backfill --market us --start-year 2023 --end-year 2024
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from scripts.fetch_bars_cn import fetch_cn_bars, load_cn_symbols
from scripts.fetch_bars_hk import fetch_hk_bars, load_hk_symbols
from scripts.fetch_bars_us import fetch_us_bars, load_us_symbols

if TYPE_CHECKING:
    import pandas as pd
from scripts.utils.parquet_io import append_bars, write_bars
from scripts.utils.release_upload import (
    download_asset,
    get_or_create_release,
    upload_asset,
)

logger = logging.getLogger(__name__)

MARKET_CONFIG = {
    "cn": {
        "tag": "data-cn-bars",
        "asset": "cn_bars.parquet",
        "release_name": "CN Daily Bars",
    },
    "hk": {
        "tag": "data-hk-bars",
        "asset": "hk_bars.parquet",
        "release_name": "HK Daily Bars",
    },
    "us": {
        "tag": "data-us-bars",
        "asset": "us_bars.parquet",
        "release_name": "US Daily Bars",
    },
}


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Backfill historical daily bar data.",
    )
    parser.add_argument(
        "--market",
        required=True,
        choices=["cn", "hk", "us"],
        help="Market to backfill: cn, hk, or us.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        required=True,
        help="Start year (inclusive).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="End year (inclusive). Defaults to current year.",
    )
    parser.add_argument(
        "--repo",
        default=None,
        help="GitHub repo (owner/name). Defaults to GITHUB_REPOSITORY env var.",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="GitHub token. Defaults to GITHUB_TOKEN env var.",
    )
    parser.add_argument(
        "--symbols-dir",
        default="symbols",
        help="Directory containing symbol CSV files.",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Save parquet locally without uploading to GitHub Releases.",
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Local output directory for parquet files.",
    )
    return parser


def fetch_bars_for_market(
    market: str,
    start_date: datetime.date,
    end_date: datetime.date,
    symbols_dir: str,
) -> pd.DataFrame:
    """Fetch bars for a given market and date range.

    Args:
        market: One of 'cn', 'hk', 'us'.
        start_date: Start date (inclusive).
        end_date: End date (inclusive).
        symbols_dir: Path to the symbols directory.

    Returns:
        DataFrame with bars data.
    """
    if market == "cn":
        sse_szse, bse = load_cn_symbols(symbols_dir)
        return fetch_cn_bars(sse_szse, bse, start_date, end_date)
    if market == "hk":
        symbols = load_hk_symbols(symbols_dir)
        return fetch_hk_bars(symbols, start_date, end_date)
    if market == "us":
        symbols = load_us_symbols(symbols_dir)
        return fetch_us_bars(symbols, start_date, end_date)
    msg = f"Unknown market: {market}"
    raise ValueError(msg)


def run_backfill(args: argparse.Namespace) -> None:
    """Execute the backfill process."""
    market = args.market
    start_year = args.start_year
    end_year = args.end_year or datetime.date.today().year
    repo = args.repo or os.environ.get("GITHUB_REPOSITORY", "")
    token = args.token or os.environ.get("GITHUB_TOKEN", "")
    symbols_dir = args.symbols_dir
    local_only = args.local_only
    output_dir = args.output_dir

    config = MARKET_CONFIG[market]
    local_path = Path(output_dir) / config["asset"]

    # Download existing data if uploading to GitHub
    if not local_only and repo and token:
        get_or_create_release(repo, config["tag"], token, name=config["release_name"])
        downloaded = download_asset(repo, config["tag"], config["asset"], str(local_path), token)
        if downloaded:
            logger.info("Downloaded existing %s", config["asset"])
    else:
        local_path.parent.mkdir(parents=True, exist_ok=True)

    for year in range(start_year, end_year + 1):
        start_date = datetime.date(year, 1, 1)
        end_date = datetime.date(year, 12, 31)

        # Don't go beyond today
        today = datetime.date.today()
        if end_date > today:
            end_date = today

        if start_date > today:
            logger.info("Skipping year %d (in the future)", year)
            continue

        logger.info("Fetching %s bars for %d (%s to %s)", market.upper(), year, start_date, end_date)

        bars = fetch_bars_for_market(market, start_date, end_date, symbols_dir)

        if bars.empty:
            logger.info("No bars for %d, skipping", year)
            continue

        logger.info("Got %d bar records for %d", len(bars), year)

        if local_path.exists():
            append_bars(local_path, bars)
        else:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            write_bars(local_path, bars)

        logger.info("Saved to %s", local_path)

    # Upload to GitHub Releases
    if not local_only and repo and token:
        if local_path.exists():
            upload_asset(repo, config["tag"], str(local_path), token)
            logger.info("Uploaded %s to release %s", config["asset"], config["tag"])
        else:
            logger.warning("No data file to upload")
    elif local_only:
        if local_path.exists():
            logger.info("Local-only mode: data saved to %s", local_path)
        else:
            logger.warning("No data was fetched")


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args()
    run_backfill(args)


if __name__ == "__main__":
    main()
