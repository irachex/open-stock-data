"""Tests for scripts.utils.trading_calendar."""

from __future__ import annotations

import datetime

import pytest

from scripts.utils.trading_calendar import is_trading_day, last_trading_day

# ---------------------------------------------------------------------------
# is_trading_day
# ---------------------------------------------------------------------------


class TestIsTradingDay:
    """Tests for is_trading_day()."""

    # --- CN market ---

    def test_cn_normal_weekday_is_trading_day(self):
        """A regular Monday-Friday should be a trading day in CN."""
        # 2025-12-01 is a Monday
        assert is_trading_day("CN", datetime.date(2025, 12, 1)) is True

    def test_cn_weekend_is_not_trading_day(self):
        """Saturdays and Sundays are never trading days."""
        # 2025-11-29 is a Saturday
        assert is_trading_day("CN", datetime.date(2025, 11, 29)) is False
        # 2025-11-30 is a Sunday
        assert is_trading_day("CN", datetime.date(2025, 11, 30)) is False

    def test_cn_national_holiday(self):
        """Chinese National Day (Oct 1) is not a trading day."""
        assert is_trading_day("CN", datetime.date(2025, 10, 1)) is False

    def test_cn_spring_festival(self):
        """Spring Festival period is not a trading day."""
        # 2025 Spring Festival: Jan 28 - Feb 4 (approx)
        assert is_trading_day("CN", datetime.date(2025, 1, 29)) is False

    # --- HK market ---

    def test_hk_normal_weekday(self):
        assert is_trading_day("HK", datetime.date(2025, 12, 1)) is True

    def test_hk_weekend(self):
        assert is_trading_day("HK", datetime.date(2025, 11, 29)) is False

    def test_hk_christmas(self):
        """Christmas Day is not a trading day in HK."""
        assert is_trading_day("HK", datetime.date(2025, 12, 25)) is False

    # --- US market ---

    def test_us_normal_weekday(self):
        assert is_trading_day("US", datetime.date(2025, 12, 1)) is True

    def test_us_weekend(self):
        assert is_trading_day("US", datetime.date(2025, 11, 29)) is False

    def test_us_independence_day(self):
        """July 4 is not a trading day in the US."""
        assert is_trading_day("US", datetime.date(2025, 7, 4)) is False

    def test_us_thanksgiving(self):
        """Thanksgiving (4th Thursday of November) is not a trading day."""
        # 2025 Thanksgiving: Nov 27
        assert is_trading_day("US", datetime.date(2025, 11, 27)) is False

    # --- Edge cases ---

    def test_defaults_to_today(self):
        """When no date is given, should not raise (uses today)."""
        # Just ensure it returns a bool without error
        result = is_trading_day("CN")
        assert isinstance(result, bool)

    def test_invalid_market_raises(self):
        with pytest.raises(ValueError, match="Unknown market"):
            is_trading_day("JP", datetime.date(2025, 12, 1))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# last_trading_day
# ---------------------------------------------------------------------------


class TestLastTradingDay:
    """Tests for last_trading_day()."""

    def test_trading_day_returns_same_date(self):
        """If the given date is a trading day, return it as-is."""
        # 2025-12-01 Monday, normal trading day for CN
        assert last_trading_day("CN", datetime.date(2025, 12, 1)) == datetime.date(2025, 12, 1)

    def test_weekend_returns_friday(self):
        """Saturday should roll back to the previous Friday."""
        # 2025-11-29 Saturday → 2025-11-28 Friday
        assert last_trading_day("CN", datetime.date(2025, 11, 29)) == datetime.date(2025, 11, 28)

    def test_sunday_returns_friday(self):
        """Sunday should also roll back to Friday."""
        assert last_trading_day("US", datetime.date(2025, 11, 30)) == datetime.date(2025, 11, 28)

    def test_after_long_holiday(self):
        """After a multi-day holiday, should find the trading day before it."""
        # 2025-10-01 to 2025-10-07 is CN National Day holiday
        result = last_trading_day("CN", datetime.date(2025, 10, 3))
        assert result < datetime.date(2025, 10, 1)
        # It should be a weekday
        assert result.weekday() < 5

    def test_defaults_to_today(self):
        """When no date is given, should return a date without error."""
        result = last_trading_day("US")
        assert isinstance(result, datetime.date)

    def test_invalid_market_raises(self):
        with pytest.raises(ValueError, match="Unknown market"):
            last_trading_day("JP", datetime.date(2025, 12, 1))  # type: ignore[arg-type]
