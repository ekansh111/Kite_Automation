"""Tests for exchange-aware holiday checking in Holidays.py."""
import sys
import os
import importlib
from datetime import date

import pytest

# Other test files stub sys.modules["Holidays"] at import time.  Force-reload
# the real module so these tests exercise the actual implementation.
sys.path.insert(0, os.path.dirname(__file__))
_saved = sys.modules.pop("Holidays", None)
import Holidays as _real_holidays
importlib.reload(_real_holidays)          # pick up the real file even if cached
if _saved is not None:
    sys.modules["Holidays"] = _saved      # restore the stub for other tests
else:
    sys.modules.pop("Holidays", None)

CheckForDateHoliday = _real_holidays.CheckForDateHoliday
MCX_FULL_HOLIDAYS = _real_holidays.MCX_FULL_HOLIDAYS
COMMODITY_EXCHANGES = _real_holidays.COMMODITY_EXCHANGES


# ─── CheckForDateHoliday — backwards compatibility (no exchange) ─────

class TestCheckForDateHolidayBackwardsCompat:
    """When exchange=None (the default), all NSE/BSE holidays return True."""

    def test_ambedkar_jayanti_2026_is_holiday(self):
        assert CheckForDateHoliday("2026-04-14") is True

    def test_republic_day_2026_is_holiday(self):
        assert CheckForDateHoliday("2026-01-26") is True

    def test_regular_day_is_not_holiday(self):
        assert CheckForDateHoliday("2026-04-15") is False

    def test_date_object_works(self):
        assert CheckForDateHoliday(date(2026, 4, 14)) is True

    def test_good_friday_2026_is_holiday(self):
        assert CheckForDateHoliday("2026-04-03") is True

    def test_christmas_2026_is_holiday(self):
        assert CheckForDateHoliday("2026-12-25") is True


# ─── CheckForDateHoliday — MCX exchange ──────────────────────────────

class TestCheckForDateHolidayMCX:
    """MCX should only be blocked on MCX full-closure days."""

    def test_ambedkar_jayanti_mcx_is_open(self):
        """April 14 — MCX evening session is open."""
        assert CheckForDateHoliday("2026-04-14", exchange="MCX") is False

    def test_maharashtra_day_mcx_is_open(self):
        """May 1 — MCX evening session is open."""
        assert CheckForDateHoliday("2026-05-01", exchange="MCX") is False

    def test_holi_mcx_is_open(self):
        """Holi — MCX evening session is open."""
        assert CheckForDateHoliday("2026-03-04", exchange="MCX") is False

    def test_republic_day_mcx_is_closed(self):
        """Republic Day — MCX full closure."""
        assert CheckForDateHoliday("2026-01-26", exchange="MCX") is True

    def test_good_friday_mcx_is_closed(self):
        """Good Friday — MCX full closure."""
        assert CheckForDateHoliday("2026-04-03", exchange="MCX") is True

    def test_gandhi_jayanti_mcx_is_closed(self):
        """Gandhi Jayanti — MCX full closure."""
        assert CheckForDateHoliday("2026-10-02", exchange="MCX") is True

    def test_christmas_mcx_is_closed(self):
        """Christmas — MCX full closure."""
        assert CheckForDateHoliday("2026-12-25", exchange="MCX") is True

    def test_regular_day_mcx_is_open(self):
        assert CheckForDateHoliday("2026-04-15", exchange="MCX") is False

    def test_date_object_works_with_mcx(self):
        assert CheckForDateHoliday(date(2026, 4, 14), exchange="MCX") is False


# ─── CheckForDateHoliday — NCDEX exchange ────────────────────────────

class TestCheckForDateHolidayNCDEX:
    """NCDEX follows the same calendar as MCX."""

    def test_ambedkar_jayanti_ncdex_is_open(self):
        assert CheckForDateHoliday("2026-04-14", exchange="NCDEX") is False

    def test_republic_day_ncdex_is_closed(self):
        assert CheckForDateHoliday("2026-01-26", exchange="NCDEX") is True


# ─── CheckForDateHoliday — equity exchanges ──────────────────────────

class TestCheckForDateHolidayEquity:
    """Equity exchanges (NFO, BFO, NSE, BSE) use the full NSE/BSE list."""

    def test_ambedkar_jayanti_nfo_is_closed(self):
        assert CheckForDateHoliday("2026-04-14", exchange="NFO") is True

    def test_ambedkar_jayanti_bfo_is_closed(self):
        assert CheckForDateHoliday("2026-04-14", exchange="BFO") is True

    def test_republic_day_nfo_is_closed(self):
        assert CheckForDateHoliday("2026-01-26", exchange="NFO") is True


# ─── 2025 holidays ───────────────────────────────────────────────────

class TestCheckForDateHoliday2025:
    """Verify 2025 MCX full closures are correct."""

    def test_good_friday_2025_mcx_closed(self):
        assert CheckForDateHoliday("2025-04-18", exchange="MCX") is True

    def test_independence_day_2025_mcx_closed(self):
        assert CheckForDateHoliday("2025-08-15", exchange="MCX") is True

    def test_ambedkar_jayanti_2025_mcx_open(self):
        assert CheckForDateHoliday("2025-04-14", exchange="MCX") is False

    def test_maharashtra_day_2025_mcx_open(self):
        assert CheckForDateHoliday("2025-05-01", exchange="MCX") is False

    def test_diwali_2025_mcx_closed(self):
        assert CheckForDateHoliday("2025-10-21", exchange="MCX") is True


# ─── MCX_FULL_HOLIDAYS constant ──────────────────────────────────────

class TestMCXFullHolidays:
    """Verify the MCX_FULL_HOLIDAYS set is well-formed."""

    def test_mcx_full_holidays_is_a_set(self):
        assert isinstance(MCX_FULL_HOLIDAYS, set)

    def test_mcx_full_holidays_all_valid_dates(self):
        for d in MCX_FULL_HOLIDAYS:
            # Should parse without error
            date.fromisoformat(d)

    def test_commodity_exchanges_contains_mcx(self):
        assert "MCX" in COMMODITY_EXCHANGES

    def test_commodity_exchanges_contains_ncdex(self):
        assert "NCDEX" in COMMODITY_EXCHANGES
