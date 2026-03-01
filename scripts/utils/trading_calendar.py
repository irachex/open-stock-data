"""Trading calendar utilities.

Determines whether a given date is a trading day for CN, HK, or US markets,
using the `exchange_calendars` library for accurate holiday handling.
"""

from __future__ import annotations

import datetime
from functools import lru_cache
from typing import Literal

import exchange_calendars as ecals
import pandas as pd

MarketCode = Literal["CN", "HK", "US"]

# Mapping from our market codes to exchange_calendars exchange keys.
# See: https://github.com/gerrymanoim/exchange_calendars#calendars
MARKET_TO_EXCHANGE: dict[MarketCode, str] = {
    "CN": "XSHG",  # Shanghai Stock Exchange (covers SSE + SZSE + BSE trading days)
    "HK": "XHKG",  # Hong Kong Exchange
    "US": "XNYS",  # New York Stock Exchange (covers NYSE + NASDAQ + AMEX)
}


@lru_cache(maxsize=8)
def _get_calendar(exchange_key: str) -> ecals.ExchangeCalendar:
    """Return a cached ExchangeCalendar instance."""
    return ecals.get_calendar(exchange_key)


def _resolve_market(market: str) -> str:
    """Resolve market code to exchange_calendars key, raising on invalid input."""
    try:
        return MARKET_TO_EXCHANGE[market]  # type: ignore[index]
    except KeyError:
        valid = ", ".join(MARKET_TO_EXCHANGE)
        raise ValueError(f"Unknown market '{market}'. Valid markets: {valid}") from None


def is_trading_day(market: MarketCode, date: datetime.date | None = None) -> bool:
    """Check if the given date is a trading day for the specified market.

    Args:
        market: One of "CN", "HK", "US".
        date: The date to check. Defaults to today (UTC).

    Returns:
        True if the date is a trading day, False otherwise.

    Raises:
        ValueError: If the market code is not recognized.
    """
    exchange_key = _resolve_market(market)
    if date is None:
        date = datetime.datetime.now(datetime.UTC).date()
    cal = _get_calendar(exchange_key)
    ts = pd.Timestamp(date)
    return cal.is_session(ts)


def last_trading_day(market: MarketCode, before: datetime.date | None = None) -> datetime.date:
    """Return the most recent trading day on or before the given date.

    Args:
        market: One of "CN", "HK", "US".
        before: The reference date. Defaults to today (UTC).

    Returns:
        The most recent trading day as a date object.

    Raises:
        ValueError: If the market code is not recognized.
    """
    exchange_key = _resolve_market(market)
    if before is None:
        before = datetime.datetime.now(datetime.UTC).date()
    cal = _get_calendar(exchange_key)
    ts = pd.Timestamp(before)
    if cal.is_session(ts):
        return before
    prev = cal.previous_close(ts).date()
    return prev
