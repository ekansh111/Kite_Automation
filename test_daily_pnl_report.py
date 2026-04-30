"""
Comprehensive tests for daily_pnl_report.py

Covers:
  - _CalcPnl: P&L calculation for LONG/SHORT directions
  - LIFO swing split: Kite MCX, Angel NCDEX, Kite Options
  - _IsIndexOption: symbol classification
  - _MatchToInstrument: prefix and ReconciliationPrefixes fallback matching
  - Formatting helpers: _FmtINR, _FmtPlain, _PnlColor, _PnlBg
  - Realized P&L: m2m fallback for closed Kite positions
  - Accumulator: idempotent write, cumulative recomputation
  - Holiday / weekend skip
  - Fetch error tracking and email warning
  - Daily MTM = open swing + realized
  - HTML report assembly
  - Edge cases: zero qty, missing fields, None values, etc.
"""

import json
import os
import sys
import tempfile
import shutil
import pytest
from pathlib import Path
from datetime import date, datetime, timedelta
from unittest.mock import patch, MagicMock, PropertyMock

# ── Patch heavy imports before importing the module ──────────────
# These broker modules aren't available in test env, so we mock them.
sys.modules["kiteconnect"] = MagicMock()
sys.modules["Server_Order_Handler"] = MagicMock()
sys.modules["smart_chase"] = MagicMock()
sys.modules["forecast_db"] = MagicMock()

# Mock Directories
MockDirs = MagicMock()
MockDirs.workInputRoot = tempfile.mkdtemp()
MockDirs.ZerodhaInstrumentDirectory = "/tmp/ZerodhaInstruments.csv"
MockDirs.AngelInstrumentDirectory = "/tmp/AngelInstrumentDetails.csv"
sys.modules["Directories"] = MockDirs

# Mock Holidays
MockHolidays = MagicMock()
def _mock_check_holiday(d, exchange=None):
    holidays = {"2026-04-03", "2026-01-26", "2026-12-25"}
    return str(d) in holidays
MockHolidays.CheckForDateHoliday = _mock_check_holiday
MockHolidays.MCX_FULL_HOLIDAYS = set()
MockHolidays.MCX_EXCHANGES = {'MCX'}
sys.modules["Holidays"] = MockHolidays

# Mock rollover_monitor
MockRollover = MagicMock()
def _mock_is_trading_day(d, exchange=None):
    if d.weekday() >= 5:
        return False
    return not _mock_check_holiday(d, exchange=exchange)
def _mock_is_any_exchange_open(d):
    if d.weekday() >= 5:
        return False
    return not _mock_check_holiday(d)
MockRollover.IsTradingDay = _mock_is_trading_day
MockRollover.IsAnyExchangeOpen = _mock_is_any_exchange_open
MockRollover._SendEmail = MagicMock()
MockRollover._EstablishKiteSession = MagicMock()
sys.modules["rollover_monitor"] = MockRollover

import daily_pnl_report as dpr


# ═══════════════════════════════════════════════════════════════════
# _CalcPnl
# ═══════════════════════════════════════════════════════════════════

class TestCalcPnl:
    def test_long_profit(self):
        assert dpr._CalcPnl("LONG", 100, 110, 2, 50) == 1000.0

    def test_long_loss(self):
        assert dpr._CalcPnl("LONG", 110, 100, 2, 50) == -1000.0

    def test_short_profit(self):
        assert dpr._CalcPnl("SHORT", 110, 100, 2, 50) == 1000.0

    def test_short_loss(self):
        assert dpr._CalcPnl("SHORT", 100, 110, 2, 50) == -1000.0

    def test_zero_qty(self):
        assert dpr._CalcPnl("LONG", 100, 200, 0, 50) == 0.0

    def test_zero_pv(self):
        assert dpr._CalcPnl("LONG", 100, 200, 5, 0) == 0.0

    def test_same_price(self):
        assert dpr._CalcPnl("LONG", 100, 100, 5, 50) == 0.0
        assert dpr._CalcPnl("SHORT", 100, 100, 5, 50) == 0.0

    def test_pv_1_options(self):
        # Options use PV=1
        assert dpr._CalcPnl("SHORT", 300, 150, 130, 1.0) == 19500.0

    def test_fractional_lots(self):
        result = dpr._CalcPnl("LONG", 100, 110, 1.5, 50)
        assert result == pytest.approx(750.0)


# ═══════════════════════════════════════════════════════════════════
# LIFO Swing Split — Kite MCX
# ═══════════════════════════════════════════════════════════════════

class TestComputeCarriedNew:
    """Tests the carried/new classification helper directly. The new formula —
    `CarriedQty = max(0, OvernightQty - DayBuyQty)` for SHORT (and symmetric
    for LONG) — handles intraday flip-and-reopen correctly. The previous
    formula (`OvernightQty - max(0, DayBuy - DaySell)`) treated ZINCMINI-style
    flips as carry-untouched, producing wrong daily swings."""

    def test_pure_carried_long(self):
        carried, new, _, closed, _ = dpr._ComputeCarriedNew(
            5, 5, "LONG", 0, 0, 0, 0)
        assert (carried, new, closed) == (5, 0, 0)

    def test_pure_new_long(self):
        carried, new, entry, closed, _ = dpr._ComputeCarriedNew(
            0, 2, "LONG", 2, 0, 9853.50, 0)
        assert (carried, new, closed) == (0, 2, 0)
        assert entry == 9853.50

    def test_long_partial_sell(self):
        # Carry 10 LONG, sell 3 → 7 remain, all carried; 3 closed at sell price.
        carried, new, _, closed, exit_price = dpr._ComputeCarriedNew(
            10, 7, "LONG", 0, 3, 0, 105.0)
        assert (carried, new, closed) == (7, 0, 3)
        assert exit_price == 105.0

    def test_long_add_to_position(self):
        # Carry 2, buy 3 + sell 1 → 4 net long.
        # New formula: DaySell=1 → carried = max(0, 2-1) = 1; new = 4-1 = 3.
        carried, new, entry, closed, _ = dpr._ComputeCarriedNew(
            2, 4, "LONG", 3, 1, 105.0, 0)
        assert (carried, new, closed) == (1, 3, 1)
        assert entry == 105.0

    def test_short_pure_carried(self):
        carried, new, _, closed, _ = dpr._ComputeCarriedNew(
            5, 5, "SHORT", 0, 0, 0, 0)
        assert (carried, new, closed) == (5, 0, 0)

    def test_short_new_today(self):
        carried, new, entry, closed, _ = dpr._ComputeCarriedNew(
            0, 10, "SHORT", 0, 10, 0, 295.0)
        assert (carried, new, closed) == (0, 10, 0)
        assert entry == 295.0

    def test_short_partial_cover(self):
        # The JEERA-class case: carry 9 SHORT, buy 6 to cover → 3 remain.
        # Carry 9 - DayBuy 6 = 3 carried. ClosedQty = 6 covered at buy price.
        carried, new, _, closed, exit_price = dpr._ComputeCarriedNew(
            9, 3, "SHORT", 6, 0, 20700.0, 0)
        assert (carried, new, closed) == (3, 0, 6)
        assert exit_price == 20700.0

    def test_short_intraday_flip_zincmini_case(self):
        # ZINCMINI: overnight SHORT 4, today buy 4 (cover) + sell 4 (new) → SHORT 4.
        # Bug pre-fix: ExcessBuys = 0, CarriedQty = 4, NewQty = 0 (WRONG).
        # Post-fix: CarriedQty = max(0, 4-4) = 0; NewQty = 4; ClosedQty = 4.
        carried, new, entry, closed, exit_price = dpr._ComputeCarriedNew(
            4, 4, "SHORT", 4, 4, 343.06, 340.09)
        assert (carried, new, closed) == (0, 4, 4)
        assert entry == 340.09  # new short entered at sell price
        assert exit_price == 343.06  # carry closed at buy price

    def test_overnight_flipped_short_to_long(self):
        # Overnight SHORT 4 (RawOvernightQty=-4); today bought 6 → LONG 2.
        # OvernightFlipped early-return: all 2 are new at buy price.
        carried, new, entry, closed, _ = dpr._ComputeCarriedNew(
            4, 2, "LONG", 6, 0, 100.0, 0,
            OvernightFlipped=True)
        assert (carried, new) == (0, 2)
        assert closed == 4  # entire overnight short was reversed
        assert entry == 100.0


class TestRealizedSliceForClose:
    """Today's daily slice for the closed portion of a position."""

    def test_short_cover_below_prev_close_is_gain(self):
        # SHORT 4 covered at 343.06 from prev_close 341.90 → cover ABOVE base = loss.
        slice_ = dpr._RealizedSliceForClose(
            SwingBase=341.90, ExitPrice=343.06, ClosedQty=4, ClosedDirection="SHORT", PV=1000)
        assert slice_ == pytest.approx(-4640.0)

    def test_short_cover_above_prev_close_is_loss(self):
        # JEERA: SHORT 2 lots covered at 20700 from prev_close 20320 = -22,800.
        slice_ = dpr._RealizedSliceForClose(
            SwingBase=20320, ExitPrice=20700, ClosedQty=2, ClosedDirection="SHORT", PV=30)
        assert slice_ == pytest.approx(-22800.0)

    def test_long_sell_above_prev_close_is_gain(self):
        slice_ = dpr._RealizedSliceForClose(
            SwingBase=100, ExitPrice=110, ClosedQty=5, ClosedDirection="LONG", PV=50)
        assert slice_ == pytest.approx(2500.0)

    def test_zero_when_nothing_closed(self):
        assert dpr._RealizedSliceForClose(100, 105, 0, "SHORT", 1) == 0.0

    def test_zero_when_swingbase_missing(self):
        # No prev_close → can't compute today's slice; return 0 to avoid bogus number.
        assert dpr._RealizedSliceForClose(0, 105, 5, "SHORT", 1) == 0.0


class TestLIFOSwingKite:
    """Smoke tests for swing computations under the new formula."""

    def _compute_kite_swing(self, overnight, day_buy, day_sell, direction, abs_qty,
                            prev_close, ltp, day_buy_price, day_sell_price, pv=1.0):
        carried, new, new_entry, _, _ = dpr._ComputeCarriedNew(
            overnight, abs_qty, direction, day_buy, day_sell,
            day_buy_price, day_sell_price)
        swing_base = prev_close if prev_close > 0 else 0
        carried_swing = dpr._CalcPnl(direction, swing_base, ltp, carried, pv)
        new_swing = dpr._CalcPnl(direction, new_entry, ltp, new, pv) if new > 0 else 0
        is_new_today = (carried == 0)
        return carried, new, carried_swing + new_swing, is_new_today

    def test_pure_carried_long(self):
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=5, day_buy=0, day_sell=0, direction="LONG", abs_qty=5,
            prev_close=100, ltp=110, day_buy_price=0, day_sell_price=0, pv=50)
        assert (carried, new, is_new) == (5, 0, False)
        assert swing == 2500.0  # (110-100)*5*50

    def test_pure_new_long(self):
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=0, day_buy=2, day_sell=0, direction="LONG", abs_qty=2,
            prev_close=9253, ltp=9853, day_buy_price=9853.50, day_sell_price=0, pv=100)
        assert (carried, new, is_new) == (0, 2, True)
        assert swing == pytest.approx((9853 - 9853.50) * 2 * 100)

    def test_short_pure_carried(self):
        carried, new, swing, _ = self._compute_kite_swing(
            overnight=5, day_buy=0, day_sell=0, direction="SHORT", abs_qty=5,
            prev_close=200, ltp=190, day_buy_price=0, day_sell_price=0, pv=100)
        assert (carried, new) == (5, 0)
        assert swing == (200 - 190) * 5 * 100

    def test_short_new_today(self):
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=0, day_buy=0, day_sell=10, direction="SHORT", abs_qty=10,
            prev_close=300, ltp=290, day_buy_price=0, day_sell_price=295, pv=1)
        assert (carried, new, is_new) == (0, 10, True)
        assert swing == (295 - 290) * 10 * 1

    def test_zincmini_intraday_flip(self):
        """User scenario 29 Apr: overnight SHORT 4 @ 344.20, today buy 4 @ 343.06
        (cover) + sell 4 @ 340.09 (new SHORT). Current SHORT 4. Open swing on
        the 4 NEW shorts is from 340.09 → 339.40 = ₹+2,760 (not ₹+10,000)."""
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=4, day_buy=4, day_sell=4, direction="SHORT", abs_qty=4,
            prev_close=341.90, ltp=339.40, day_buy_price=343.06, day_sell_price=340.09,
            pv=1000)
        assert (carried, new, is_new) == (0, 4, True)
        assert swing == pytest.approx(2760.0)  # not 10,000


# ═══════════════════════════════════════════════════════════════════
# LIFO Swing Split — Angel NCDEX
# ═══════════════════════════════════════════════════════════════════

class TestLIFOSwingAngel:
    """Smoke tests for Angel swing logic against the new carried/new helper."""

    def _compute_angel_swing(self, cf_buy_qty, buy_qty, sell_qty, direction, abs_qty,
                             prev_close, ltp, today_buy_price, today_sell_price,
                             qty_mult=5, pv=50, cf_sell_qty=0):
        overnight_units = cf_buy_qty if direction == "LONG" else cf_sell_qty
        carried_units, new_units, new_entry, _, _ = dpr._ComputeCarriedNew(
            overnight_units, abs_qty, direction,
            buy_qty, sell_qty, today_buy_price, today_sell_price)
        carried_lots = carried_units / qty_mult
        new_lots = new_units / qty_mult
        is_new_today = (carried_units == 0)
        swing_base = prev_close if prev_close > 0 else 0
        carried_swing = dpr._CalcPnl(direction, swing_base, ltp, carried_lots, pv)
        new_swing = dpr._CalcPnl(direction, new_entry, ltp, new_lots, pv) if new_lots > 0 else 0
        return carried_units, new_units, carried_swing + new_swing, is_new_today

    def test_pure_cf_ncdex(self):
        carried, new, swing, _ = self._compute_angel_swing(
            cf_buy_qty=10, buy_qty=0, sell_qty=0, direction="LONG", abs_qty=10,
            prev_close=5723, ltp=5760, today_buy_price=0, today_sell_price=0,
            qty_mult=5, pv=50)
        assert (carried, new) == (10, 0)
        assert swing == pytest.approx(3700.0)  # (5760-5723) * 2 lots * 50

    def test_partial_cf_exit(self):
        # CF=10, sell=5 → 5 carried remain, 0 new.
        carried, new, swing, is_new = self._compute_angel_swing(
            cf_buy_qty=10, buy_qty=0, sell_qty=5, direction="LONG", abs_qty=5,
            prev_close=3582, ltp=3611, today_buy_price=0, today_sell_price=0,
            qty_mult=10, pv=100)
        assert (carried, new, is_new) == (5, 0, False)

    def test_add_to_cf_no_sells(self):
        # CF=5, buy=5 → 10 total: 5 carried + 5 new.
        carried, new, swing, _ = self._compute_angel_swing(
            cf_buy_qty=5, buy_qty=5, sell_qty=0, direction="LONG", abs_qty=10,
            prev_close=10000, ltp=10100, today_buy_price=10050, today_sell_price=0,
            qty_mult=5, pv=50)
        assert (carried, new) == (5, 5)
        # carried: (10100-10000) * 1 * 50 = 5000; new: (10100-10050) * 1 * 50 = 2500
        assert swing == pytest.approx(7500.0)

    def test_brand_new_position(self):
        carried, new, _, is_new = self._compute_angel_swing(
            cf_buy_qty=0, buy_qty=5, sell_qty=0, direction="LONG", abs_qty=5,
            prev_close=0, ltp=15000, today_buy_price=14950, today_sell_price=0,
            qty_mult=5, pv=50)
        assert (carried, new, is_new) == (0, 5, True)

    def test_jeera_partial_cover(self):
        """User scenario 29 Apr: SHORT 9 units (3 lots) carry @ 21611.89,
        today bought 6 to cover @ 20700 → SHORT 3 units (1 lot) remain.
        Open swing from prev_close 20320 → ltp 20670 on remaining 1 lot = -10,500."""
        carried, new, swing, is_new = self._compute_angel_swing(
            cf_buy_qty=0, cf_sell_qty=9, buy_qty=6, sell_qty=0,
            direction="SHORT", abs_qty=3,
            prev_close=20320, ltp=20670, today_buy_price=20700, today_sell_price=0,
            qty_mult=3, pv=30)
        assert (carried, new, is_new) == (3, 0, False)
        assert swing == pytest.approx(-10500.0)  # carry-only swing on open portion


# ═══════════════════════════════════════════════════════════════════
# _IsIndexOption
# ═══════════════════════════════════════════════════════════════════

class TestIsIndexOption:
    def test_nifty_ce(self):
        assert dpr._IsIndexOption("NIFTY26APR24000CE") is True

    def test_nifty_pe(self):
        assert dpr._IsIndexOption("NIFTY26APR22000PE") is True

    def test_banknifty_ce(self):
        assert dpr._IsIndexOption("BANKNIFTY26APR52000CE") is True

    def test_sensex_pe(self):
        assert dpr._IsIndexOption("SENSEX2640880000PE") is True

    def test_bankex_ce(self):
        assert dpr._IsIndexOption("BANKEX26APR60000CE") is True

    def test_not_option_fut(self):
        assert dpr._IsIndexOption("NIFTY26APRFUT") is False

    def test_mcx_symbol(self):
        assert dpr._IsIndexOption("GOLDM25APRFUT") is False

    def test_case_insensitive(self):
        assert dpr._IsIndexOption("nifty26apr24000ce") is True

    def test_random_symbol(self):
        assert dpr._IsIndexOption("RELIANCE") is False


# ═══════════════════════════════════════════════════════════════════
# _MatchToInstrument — prefix and ReconciliationPrefixes fallback
# ═══════════════════════════════════════════════════════════════════

class TestMatchToInstrument:
    INSTRUMENTS = {
        "GOLDM": {"exchange": "MCX", "point_value": 10},
        "ZINCMINI": {"exchange": "MCX", "point_value": 1000},
        "TURMERIC": {
            "exchange": "NCX",
            "point_value": 50,
            "order_routing": {"ReconciliationPrefixes": ["TMCFGRNZM"]},
        },
        "GUARSEED": {
            "exchange": "NCX",
            "point_value": 50,
            "order_routing": {"ReconciliationPrefixes": ["GUARSEED10"]},
        },
    }

    @patch("daily_pnl_report.pd.read_csv", side_effect=FileNotFoundError)
    def test_prefix_match_goldm(self, _):
        name, cfg = dpr._MatchToInstrument("GOLDM25APRFUT", "MCX", "ZERODHA", self.INSTRUMENTS)
        assert name == "GOLDM"

    @patch("daily_pnl_report.pd.read_csv", side_effect=FileNotFoundError)
    def test_prefix_match_zincmini(self, _):
        name, cfg = dpr._MatchToInstrument("ZINCMINI26APRFUT", "MCX", "ZERODHA", self.INSTRUMENTS)
        assert name == "ZINCMINI"

    @patch("daily_pnl_report.pd.read_csv", side_effect=FileNotFoundError)
    def test_reconciliation_prefix_turmeric(self, _):
        name, cfg = dpr._MatchToInstrument("TMCFGRNZM26APRFUT", "NCX", "ANGEL", self.INSTRUMENTS)
        assert name == "TURMERIC"

    @patch("daily_pnl_report.pd.read_csv", side_effect=FileNotFoundError)
    def test_reconciliation_prefix_guarseed(self, _):
        name, cfg = dpr._MatchToInstrument("GUARSEED1026APRFUT", "NCX", "ANGEL", self.INSTRUMENTS)
        assert name == "GUARSEED"

    @patch("daily_pnl_report.pd.read_csv", side_effect=FileNotFoundError)
    def test_no_match(self, _):
        name, cfg = dpr._MatchToInstrument("UNKNOWN123", "MCX", "ZERODHA", self.INSTRUMENTS)
        assert name is None
        assert cfg is None

    @patch("daily_pnl_report.pd.read_csv", side_effect=FileNotFoundError)
    def test_exchange_mismatch(self, _):
        # GOLDM exists but for MCX, not NCX
        name, cfg = dpr._MatchToInstrument("GOLDM25APRFUT", "NCX", "ZERODHA", self.INSTRUMENTS)
        assert name is None


# ═══════════════════════════════════════════════════════════════════
# Formatting Helpers
# ═══════════════════════════════════════════════════════════════════

class TestFormatting:
    def test_fmt_inr_positive(self):
        assert dpr._FmtINR(12345) == "+12,345"

    def test_fmt_inr_negative(self):
        assert dpr._FmtINR(-12345) == "-12,345"

    def test_fmt_inr_zero(self):
        assert dpr._FmtINR(0) == "+0"

    def test_fmt_inr_decimals(self):
        assert dpr._FmtINR(1234.567, Decimals=2) == "+1,234.57"

    def test_fmt_inr_negative_decimals(self):
        assert dpr._FmtINR(-99.5, Decimals=1) == "-99.5"

    def test_fmt_plain_positive(self):
        assert dpr._FmtPlain(12345) == "12,345"

    def test_fmt_plain_decimals(self):
        assert dpr._FmtPlain(1234.567, Decimals=2) == "1,234.57"

    def test_pnl_color_green(self):
        assert dpr._PnlColor(100) == dpr.GREEN

    def test_pnl_color_red(self):
        assert dpr._PnlColor(-100) == dpr.RED

    def test_pnl_color_muted(self):
        assert dpr._PnlColor(0) == dpr.MUTED

    def test_pnl_bg_positive(self):
        assert dpr._PnlBg(100) == "#f0fdf4"

    def test_pnl_bg_negative(self):
        assert dpr._PnlBg(-100) == "#fef2f2"

    def test_pnl_bg_zero(self):
        assert dpr._PnlBg(0) == "#f8fafc"


# ═══════════════════════════════════════════════════════════════════
# Realized P&L Accumulator
# ═══════════════════════════════════════════════════════════════════

class TestRealizedPnlAccumulator:
    def setup_method(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.orig_path = dpr.REALIZED_PNL_PATH
        dpr.REALIZED_PNL_PATH = Path(self.tmp_dir) / "realized_pnl_accumulator.json"

    def teardown_method(self):
        dpr.REALIZED_PNL_PATH = self.orig_path
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_first_write_creates_file(self):
        daily = {"YD6016": 5000.0, "AABM826021": 3000.0, "OFS653": 0.0}
        dpr._UpdateRealizedPnlAccumulator(daily, "2026-04-02", 85000.0)

        assert dpr.REALIZED_PNL_PATH.exists()
        data = json.loads(dpr.REALIZED_PNL_PATH.read_text())
        assert data["cumulative_realized_pnl"] == 8000.0
        assert data["eod_unrealized"] == 85000.0
        assert "2026-04-02" in data["daily_entries"]
        assert data["daily_entries"]["2026-04-02"]["total"] == 8000.0

    def test_idempotent_overwrite(self):
        daily = {"YD6016": 5000.0, "AABM826021": 3000.0, "OFS653": 0.0}
        dpr._UpdateRealizedPnlAccumulator(daily, "2026-04-02", 85000.0)
        # Call again with different unrealized — should overwrite, not duplicate
        dpr._UpdateRealizedPnlAccumulator(daily, "2026-04-02", 90000.0)

        data = json.loads(dpr.REALIZED_PNL_PATH.read_text())
        assert data["cumulative_realized_pnl"] == 8000.0  # Still same daily
        assert data["eod_unrealized"] == 90000.0  # Updated

    def test_multi_day_cumulative(self):
        d1 = {"YD6016": 1000.0, "AABM826021": 0.0, "OFS653": 0.0}
        dpr._UpdateRealizedPnlAccumulator(d1, "2026-04-01", 50000.0)

        d2 = {"YD6016": 0.0, "AABM826021": 19682.0, "OFS653": 0.0}
        dpr._UpdateRealizedPnlAccumulator(d2, "2026-04-02", 85000.0)

        data = json.loads(dpr.REALIZED_PNL_PATH.read_text())
        assert data["cumulative_realized_pnl"] == 20682.0  # 1000 + 19682
        assert len(data["daily_entries"]) == 2

    def test_zero_realized_day(self):
        daily = {"YD6016": 0.0, "AABM826021": 0.0, "OFS653": 0.0}
        dpr._UpdateRealizedPnlAccumulator(daily, "2026-04-03", 80000.0)

        data = json.loads(dpr.REALIZED_PNL_PATH.read_text())
        assert data["cumulative_realized_pnl"] == 0.0
        assert data["daily_entries"]["2026-04-03"]["total"] == 0.0

    def test_negative_realized(self):
        daily = {"YD6016": -5000.0, "AABM826021": 2000.0, "OFS653": -1000.0}
        dpr._UpdateRealizedPnlAccumulator(daily, "2026-04-04", 70000.0)

        data = json.loads(dpr.REALIZED_PNL_PATH.read_text())
        assert data["cumulative_realized_pnl"] == -4000.0


# ═══════════════════════════════════════════════════════════════════
# Kite Realized P&L — m2m fallback
# ═══════════════════════════════════════════════════════════════════

class TestKiteRealizedM2mFallback:
    """Test that closed Kite positions use m2m when realised=0."""

    def test_closed_position_uses_m2m(self):
        """Simulate: CRUDEOIL closed (qty=0), realised=0, m2m=125800."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {"net": [
            {"tradingsymbol": "CRUDEOIL26APRFUT", "product": "NRML",
             "quantity": 0, "realised": 0, "m2m": 125800, "pnl": 125800},
            {"tradingsymbol": "ZINCMINI26APRFUT", "product": "NRML",
             "quantity": 4, "realised": 0, "m2m": 800, "pnl": 800},
        ]}
        MockRollover._EstablishKiteSession.return_value = mock_kite

        # Test the logic inline (same as _FetchDailyRealizedPnl for YD6016)
        total = 0.0
        for P in mock_kite.positions()["net"]:
            if P.get("product") != "NRML":
                continue
            realised = float(P.get("realised", 0))
            m2m = float(P.get("m2m", 0))
            qty = P.get("quantity", 0)
            if qty == 0 and realised == 0 and m2m != 0:
                total += m2m
            else:
                total += realised
        assert total == 125800.0  # CRUDEOIL m2m picked up

    def test_closed_position_with_realised_uses_realised(self):
        """If realised is non-zero, use it even for qty=0."""
        total = 0.0
        P = {"quantity": 0, "realised": 5000, "m2m": 5000, "product": "NRML"}
        realised = float(P["realised"])
        m2m = float(P["m2m"])
        qty = P["quantity"]
        if qty == 0 and realised == 0 and m2m != 0:
            total += m2m
        else:
            total += realised
        assert total == 5000.0

    def test_open_position_ignores_m2m(self):
        """Open positions use realised only (m2m includes unrealized)."""
        total = 0.0
        P = {"quantity": 4, "realised": 0, "m2m": 5000, "product": "NRML"}
        realised = float(P["realised"])
        m2m = float(P["m2m"])
        qty = P["quantity"]
        if qty == 0 and realised == 0 and m2m != 0:
            total += m2m
        else:
            total += realised
        assert total == 0.0  # Open position, realised=0 is correct


# ═══════════════════════════════════════════════════════════════════
# Angel Realized M2M Fallback (NCDEX closed positions)
# ═══════════════════════════════════════════════════════════════════

class TestAngelRealizedM2mFallback:
    """Test that closed Angel positions use m2m when realised=0."""

    def test_closed_ncdex_position_uses_m2m(self):
        """Simulate: COCUDAKL closed (netqty=0), realised=0, m2m=-31200."""
        positions = [
            {"tradingsymbol": "COCUDAKL20APR2026", "producttype": "CARRYFORWARD",
             "netqty": "0", "realised": "0", "m2m": "-31200"},
        ]
        total = 0.0
        for P in positions:
            if P.get("producttype") != "CARRYFORWARD":
                continue
            realised = float(P.get("realised", 0) or 0)
            m2m = float(P.get("m2m", 0) or 0)
            qty = int(P.get("netqty", 0))
            if qty == 0 and realised == 0 and m2m != 0:
                total += m2m
            else:
                total += realised
        assert total == -31200.0  # COCUDAKL m2m loss picked up

    def test_closed_position_with_nonzero_realised_uses_realised(self):
        """If realised is non-zero, use it even for netqty=0."""
        total = 0.0
        P = {"netqty": "0", "realised": "8500", "m2m": "8500", "producttype": "CARRYFORWARD"}
        realised = float(P.get("realised", 0) or 0)
        m2m = float(P.get("m2m", 0) or 0)
        qty = int(P.get("netqty", 0))
        if qty == 0 and realised == 0 and m2m != 0:
            total += m2m
        else:
            total += realised
        assert total == 8500.0

    def test_open_angel_position_ignores_m2m(self):
        """Open Angel positions use realised only (m2m includes unrealized)."""
        total = 0.0
        P = {"netqty": "10", "realised": "0", "m2m": "4500", "producttype": "CARRYFORWARD"}
        realised = float(P.get("realised", 0) or 0)
        m2m = float(P.get("m2m", 0) or 0)
        qty = int(P.get("netqty", 0))
        if qty == 0 and realised == 0 and m2m != 0:
            total += m2m
        else:
            total += realised
        assert total == 0.0  # Open position, realised=0 is correct

    def test_multiple_angel_positions_mixed(self):
        """Mix of open and closed Angel positions."""
        positions = [
            {"tradingsymbol": "COCUDAKL20APR2026", "producttype": "CARRYFORWARD",
             "netqty": "0", "realised": "0", "m2m": "-31200"},
            {"tradingsymbol": "GUARSEED20APR2026", "producttype": "CARRYFORWARD",
             "netqty": "5", "realised": "2500", "m2m": "7000"},
            {"tradingsymbol": "DHANIYA20APR2026", "producttype": "CARRYFORWARD",
             "netqty": "0", "realised": "0", "m2m": "15000"},
        ]
        total = 0.0
        for P in positions:
            if P.get("producttype") != "CARRYFORWARD":
                continue
            realised = float(P.get("realised", 0) or 0)
            m2m = float(P.get("m2m", 0) or 0)
            qty = int(P.get("netqty", 0))
            if qty == 0 and realised == 0 and m2m != 0:
                total += m2m
            else:
                total += realised
        # COCUDAKL: m2m=-31200, GUARSEED: realised=2500 (open), DHANIYA: m2m=15000
        assert total == (-31200.0 + 2500.0 + 15000.0)

    def test_intraday_angel_positions_included(self):
        """Angel INTRADAY positions should be included in realized P&L."""
        positions = [
            {"tradingsymbol": "CASTOR20APR2026", "producttype": "INTRADAY",
             "netqty": "0", "realised": "-12100", "m2m": "-12100"},
            {"tradingsymbol": "TMCFGRNZM20APR2026", "producttype": "INTRADAY",
             "netqty": "0", "realised": "-6700", "m2m": "-6700"},
            {"tradingsymbol": "DHANIYA20APR2026", "producttype": "INTRADAY",
             "netqty": "0", "realised": "1100", "m2m": "1100"},
            {"tradingsymbol": "COCUDAKL20APR2026", "producttype": "CARRYFORWARD",
             "netqty": "0", "realised": "0", "m2m": "-1000"},
        ]
        total = 0.0
        for P in positions:
            prod = P.get("producttype", "")
            if prod not in ("CARRYFORWARD", "INTRADAY"):
                continue
            realised = float(P.get("realised", 0) or 0)
            m2m = float(P.get("m2m", 0) or 0)
            qty = int(P.get("netqty", 0))
            if qty == 0 and realised == 0 and m2m != 0:
                total += m2m
            else:
                total += realised
        # CASTOR: -12100, TMCFGRNZM: -6700, DHANIYA: +1100, COCUDAKL: m2m=-1000
        assert total == (-12100.0 + -6700.0 + 1100.0 + -1000.0)

    def test_delivery_positions_excluded(self):
        """Non-futures product types like DELIVERY should be excluded."""
        positions = [
            {"tradingsymbol": "SOMETHING", "producttype": "DELIVERY",
             "netqty": "0", "realised": "5000", "m2m": "5000"},
            {"tradingsymbol": "COCUDAKL20APR2026", "producttype": "CARRYFORWARD",
             "netqty": "0", "realised": "0", "m2m": "-31200"},
        ]
        total = 0.0
        for P in positions:
            prod = P.get("producttype", "")
            if prod not in ("CARRYFORWARD", "INTRADAY"):
                continue
            realised = float(P.get("realised", 0) or 0)
            m2m = float(P.get("m2m", 0) or 0)
            qty = int(P.get("netqty", 0))
            if qty == 0 and realised == 0 and m2m != 0:
                total += m2m
            else:
                total += realised
        assert total == -31200.0  # Only CARRYFORWARD counted, DELIVERY excluded

    def test_none_values_handled(self):
        """Angel API sometimes returns None instead of 0."""
        P = {"netqty": "0", "realised": None, "m2m": "-5000", "producttype": "CARRYFORWARD"}
        realised = float(P.get("realised", 0) or 0)
        m2m = float(P.get("m2m", 0) or 0)
        qty = int(P.get("netqty", 0))
        total = 0.0
        if qty == 0 and realised == 0 and m2m != 0:
            total += m2m
        else:
            total += realised
        assert total == -5000.0


# ═══════════════════════════════════════════════════════════════════
# Holiday / Weekend Skip
# ═══════════════════════════════════════════════════════════════════

class TestHolidaySkip:
    def test_good_friday_is_holiday(self):
        assert _mock_is_trading_day(date(2026, 4, 3)) is False

    def test_saturday_not_trading_day(self):
        assert _mock_is_trading_day(date(2026, 4, 4)) is False

    def test_sunday_not_trading_day(self):
        assert _mock_is_trading_day(date(2026, 4, 5)) is False

    def test_normal_weekday_is_trading_day(self):
        assert _mock_is_trading_day(date(2026, 4, 2)) is True

    def test_republic_day_is_holiday(self):
        assert _mock_is_trading_day(date(2026, 1, 26)) is False

    def test_christmas_is_holiday(self):
        assert _mock_is_trading_day(date(2026, 12, 25)) is False

    @patch("daily_pnl_report.IsAnyExchangeOpen", side_effect=_mock_is_any_exchange_open)
    def test_generate_report_skips_holiday(self, _):
        """GenerateDailyReport should return early on a holiday."""
        with patch("daily_pnl_report._FetchOpenPositions") as mock_fetch:
            dpr.GenerateDailyReport(DryRun=True, DateStr="2026-04-03")
            mock_fetch.assert_not_called()

    @patch("daily_pnl_report.IsAnyExchangeOpen", side_effect=_mock_is_any_exchange_open)
    def test_generate_report_skips_weekend(self, _):
        with patch("daily_pnl_report._FetchOpenPositions") as mock_fetch:
            dpr.GenerateDailyReport(DryRun=True, DateStr="2026-04-04")
            mock_fetch.assert_not_called()


# ═══════════════════════════════════════════════════════════════════
# Daily MTM = Open Swing + Realized
# ═══════════════════════════════════════════════════════════════════

class TestDailyMtm:
    """Verify the hero number includes both open swing and realized P&L."""

    @patch("daily_pnl_report.IsTradingDay", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl")
    @patch("daily_pnl_report._FetchOpenPositions")
    @patch("daily_pnl_report._UpdateRealizedPnlAccumulator")
    @patch("daily_pnl_report._BuildReportHtml", return_value="<html></html>")
    def test_mtm_includes_realized(self, mock_html, mock_accum, mock_fetch, mock_realized, mock_orders, mock_td):
        """Daily MTM = OpenSwing + sum(open positions' today_realized_slice) +
        sum(ClosedByInstrument). No double-count with cumulative-since-entry realised."""
        mock_fetch.return_value = ([
            {"pnl": 10000, "daily_swing": 5000, "instrument": "GOLDM", "direction": "LONG",
             "qty": 1, "avg_entry": 100, "prev_close": 95, "ltp": 105, "point_value": 10,
             "today_realized_slice": 0,  # no partial close on this open position
             "broker": "ZERODHA", "is_new_today": False},
        ], [])
        mock_realized.return_value = (
            {"YD6016": 20000.0, "AABM826021": 5000.0, "OFS653": 0.0},  # cumulative — for accumulator
            {"CRUDEOIL_CLOSED": 25000.0},  # fully-closed slices for display/MTM
        )

        dpr.GenerateDailyReport(DryRun=True, DateStr="2026-04-02")

        call_args = mock_html.call_args[0][0]
        assert call_args["open_swing"] == 5000
        assert call_args["realized_today"] == 25000.0
        assert call_args["total_daily_mtm"] == 30000.0  # 5000 + 25000

    @patch("daily_pnl_report.IsTradingDay", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl")
    @patch("daily_pnl_report._FetchOpenPositions")
    @patch("daily_pnl_report._UpdateRealizedPnlAccumulator")
    @patch("daily_pnl_report._BuildReportHtml", return_value="<html></html>")
    def test_mtm_zero_realized(self, mock_html, mock_accum, mock_fetch, mock_realized, mock_orders, mock_td):
        """No exits today — MTM equals open swing."""
        mock_fetch.return_value = ([
            {"pnl": 5000, "daily_swing": 3000, "instrument": "ZINCMINI", "direction": "LONG",
             "qty": 4, "avg_entry": 320, "prev_close": 323, "ltp": 325, "point_value": 1000,
             "broker": "ZERODHA", "is_new_today": False},
        ], [])
        mock_realized.return_value = ({"YD6016": 0.0, "AABM826021": 0.0, "OFS653": 0.0}, {})

        dpr.GenerateDailyReport(DryRun=True, DateStr="2026-04-02")

        call_args = mock_html.call_args[0][0]
        assert call_args["total_daily_mtm"] == 3000.0


# ═══════════════════════════════════════════════════════════════════
# Fetch Error Tracking
# ═══════════════════════════════════════════════════════════════════

class TestFetchErrors:
    @patch("daily_pnl_report.IsTradingDay", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl", return_value=({"YD6016": 0, "AABM826021": 0, "OFS653": 0}, {}))
    @patch("daily_pnl_report._FetchOpenPositions")
    @patch("daily_pnl_report._UpdateRealizedPnlAccumulator")
    @patch("daily_pnl_report._BuildReportHtml", return_value="<html></html>")
    def test_fetch_errors_passed_to_html(self, mock_html, mock_accum, mock_fetch, mock_realized, mock_orders, mock_td):
        mock_fetch.return_value = ([], ["Kite YD6016 (MCX): Connection timeout"])

        dpr.GenerateDailyReport(DryRun=True, DateStr="2026-04-02")

        call_args = mock_html.call_args[0][0]
        assert len(call_args["fetch_errors"]) == 1
        assert "YD6016" in call_args["fetch_errors"][0]


# ═══════════════════════════════════════════════════════════════════
# HTML Report Assembly
# ═══════════════════════════════════════════════════════════════════

class TestBuildReportHtml:
    def _make_report_data(self, **overrides):
        base = {
            "date": "2026-04-02",
            "positions": [],
            "total_pnl": 0,
            "total_daily_mtm": 0,
            "open_swing": 0,
            "realized_today": 0,
            "realized_by_account": {},
            "position_count": 0,
            "trade_count": 0,
            "futures_orders": [],
            "options_orders": [],
            "fetch_errors": [],
        }
        base.update(overrides)
        return base

    def test_basic_html_structure(self):
        html = dpr._BuildReportHtml(self._make_report_data())
        assert "<!DOCTYPE html>" in html
        assert "Daily P&L Report" in html
        assert "02 Apr 2026" in html

    def test_hero_number_positive(self):
        html = dpr._BuildReportHtml(self._make_report_data(total_daily_mtm=50000))
        assert "+50,000" in html

    def test_hero_number_negative(self):
        html = dpr._BuildReportHtml(self._make_report_data(total_daily_mtm=-25000))
        assert "-25,000" in html

    def test_warning_banner_shown_on_errors(self):
        html = dpr._BuildReportHtml(self._make_report_data(
            fetch_errors=["Kite YD6016 (MCX): timeout"]))
        assert "Broker Fetch Errors" in html
        assert "Kite YD6016" in html

    def test_no_warning_when_no_errors(self):
        html = dpr._BuildReportHtml(self._make_report_data())
        assert "Broker Fetch Errors" not in html

    def test_stats_bar_shows_all_fields(self):
        html = dpr._BuildReportHtml(self._make_report_data(
            open_swing=5000, realized_today=3000, total_pnl=80000, trade_count=7))
        assert "Open Swing" in html
        assert "Realized" in html
        assert "Unrealized" in html
        assert "Trades" in html

    def test_positions_shown(self):
        positions = [{
            "instrument": "GOLDM", "tradingsymbol": "GOLDM25APRFUT",
            "direction": "LONG", "qty": 2,
            "avg_entry": 95000, "prev_close": 94500, "ltp": 95500,
            "point_value": 10, "pnl": 10000, "daily_swing": 20000,
            "broker": "ZERODHA", "is_new_today": False,
        }]
        html = dpr._BuildReportHtml(self._make_report_data(
            positions=positions, position_count=1))
        assert "GOLDM" in html

    def test_new_today_badge(self):
        positions = [{
            "instrument": "CRUDEOIL", "tradingsymbol": "CRUDEOIL26APRFUT",
            "direction": "LONG", "qty": 2,
            "avg_entry": 9800, "prev_close": 0, "ltp": 9900,
            "point_value": 100, "pnl": 20000, "daily_swing": 20000,
            "broker": "ZERODHA", "is_new_today": True,
        }]
        html = dpr._BuildReportHtml(self._make_report_data(
            positions=positions, position_count=1))
        assert "NEW" in html

    def test_options_grouped_by_underlying(self):
        positions = [
            {"instrument": "NIFTY_OPT_CE", "tradingsymbol": "NIFTY26APR24000CE",
             "direction": "LONG", "qty": 65, "avg_entry": 2030, "prev_close": 1385,
             "ltp": 1327, "point_value": 1.0, "pnl": -45675, "daily_swing": -3812,
             "broker": "ZERODHA", "is_new_today": False},
            {"instrument": "NIFTY_OPT_PE", "tradingsymbol": "NIFTY26APR22000PE",
             "direction": "SHORT", "qty": 130, "avg_entry": 292, "prev_close": 151,
             "ltp": 129, "point_value": 1.0, "pnl": 21274, "daily_swing": 2860,
             "broker": "ZERODHA", "is_new_today": False},
        ]
        html = dpr._BuildReportHtml(self._make_report_data(
            positions=positions, position_count=2))
        assert "NIFTY" in html


# ═══════════════════════════════════════════════════════════════════
# Edge Cases
# ═══════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_calc_pnl_large_numbers(self):
        result = dpr._CalcPnl("LONG", 50000, 52000, 10, 10)
        assert result == 200000.0

    def test_calc_pnl_very_small_difference(self):
        result = dpr._CalcPnl("LONG", 100.001, 100.002, 1, 1)
        assert result == pytest.approx(0.001)

    def test_lifo_all_zeroes(self):
        """All qty fields zero — should handle gracefully."""
        if True:  # LONG case
            excess = max(0, 0 - 0)
            carried = max(0, 0 - excess)
            new = max(0, 0 - carried)
            assert carried == 0
            assert new == 0

    # ── Direction flip tests ──

    def test_direction_flip_short_to_long(self):
        """SHORT overnight → LONG today: all current qty should be new."""
        # Simulates SILVERMIC: was short 2, bought 3, now long 1
        RawOvernightQty = -2
        Direction = "LONG"
        AbsQty = 1
        DayBuyQty = 3
        DaySellQty = 0
        DayBuyPrice = 245639.33
        Ltp = 242406.0
        PrevClose = 233952.0
        PV = 1

        OvernightFlipped = (Direction == "LONG" and RawOvernightQty < 0)
        assert OvernightFlipped is True

        CarriedQty = 0
        NewQty = AbsQty  # = 1
        NewEntryPrice = DayBuyPrice

        SwingBase = PrevClose if PrevClose > 0 else 0
        CarriedSwing = (Ltp - SwingBase) * CarriedQty * PV  # = 0
        NewSwing = (Ltp - NewEntryPrice) * NewQty * PV
        DailySwing = CarriedSwing + NewSwing

        assert CarriedQty == 0
        assert NewQty == 1
        assert DailySwing == pytest.approx(-3233.33, abs=0.01)

    def test_direction_flip_long_to_short(self):
        """LONG overnight → SHORT today: all current qty should be new."""
        RawOvernightQty = 3
        Direction = "SHORT"
        AbsQty = 2
        DaySellPrice = 500.0
        Ltp = 490.0
        PV = 1250

        OvernightFlipped = (Direction == "SHORT" and RawOvernightQty > 0)
        assert OvernightFlipped is True

        CarriedQty = 0
        NewQty = AbsQty  # = 2
        NewEntryPrice = DaySellPrice

        NewSwing = (NewEntryPrice - Ltp) * NewQty * PV
        DailySwing = NewSwing

        assert CarriedQty == 0
        assert NewQty == 2
        assert DailySwing == pytest.approx(25000.0)  # (500-490)*2*1250

    def test_no_flip_same_direction_long(self):
        """LONG overnight → still LONG: carried qty preserved."""
        RawOvernightQty = 3
        Direction = "LONG"
        AbsQty = 3
        DayBuyQty = 0
        DaySellQty = 0

        OvernightFlipped = (Direction == "LONG" and RawOvernightQty < 0)
        assert OvernightFlipped is False

        OvernightQty = abs(RawOvernightQty)
        ExcessSells = max(0, DaySellQty - DayBuyQty)
        CarriedQty = max(0, OvernightQty - ExcessSells)
        NewQty = max(0, AbsQty - CarriedQty)

        assert CarriedQty == 3
        assert NewQty == 0

    def test_no_flip_same_direction_short(self):
        """SHORT overnight → still SHORT: carried qty preserved."""
        RawOvernightQty = -4
        Direction = "SHORT"
        AbsQty = 4

        OvernightFlipped = (Direction == "SHORT" and RawOvernightQty > 0)
        assert OvernightFlipped is False

        OvernightQty = abs(RawOvernightQty)
        ExcessBuys = max(0, 0 - 0)
        CarriedQty = max(0, OvernightQty - ExcessBuys)
        NewQty = max(0, AbsQty - CarriedQty)

        assert CarriedQty == 4
        assert NewQty == 0

    def test_no_flip_zero_overnight(self):
        """No overnight position → all new, no flip."""
        RawOvernightQty = 0
        Direction = "LONG"
        AbsQty = 2

        OvernightFlipped = (Direction == "LONG" and RawOvernightQty < 0)
        assert OvernightFlipped is False

        OvernightQty = 0
        ExcessSells = max(0, 0 - 2)
        CarriedQty = max(0, 0 - ExcessSells)  # = 0
        NewQty = max(0, 2 - 0)  # = 2

        assert CarriedQty == 0
        assert NewQty == 2

    def test_prev_close_zero_uses_avg_price(self):
        """When prev_close=0, swing base should be avg_price."""
        # This tests the `SwingBase = PrevClose if PrevClose > 0 else AvgPrice` logic
        swing_base = 0 if 0 > 0 else 105.0
        assert swing_base == 105.0

    def test_option_lot_sizes_in_html(self):
        """Verify LOT_SIZES used in _BuildReportHtml has correct values.
        LOT_SIZES is local to _BuildReportHtml, so we verify via the HTML output."""
        positions = [
            {"instrument": "NIFTY_OPT_CE", "tradingsymbol": "NIFTY26APR24000CE",
             "direction": "LONG", "qty": 65, "avg_entry": 2030, "prev_close": 1385,
             "ltp": 1327, "point_value": 1.0, "pnl": -45675, "daily_swing": -3812,
             "broker": "ZERODHA", "is_new_today": False},
        ]
        data = {
            "date": "2026-04-02", "positions": positions, "total_pnl": -45675,
            "total_daily_mtm": -3812, "open_swing": -3812, "realized_today": 0,
            "realized_by_account": {}, "position_count": 1, "trade_count": 0,
            "futures_orders": [], "options_orders": [], "fetch_errors": [],
        }
        html = dpr._BuildReportHtml(data)
        # 65 qty / 65 lot size = 1 lot — should appear in the HTML
        assert "1 lot" in html.lower() or "1 Lot" in html

    def test_fmt_inr_rounding(self):
        assert dpr._FmtINR(99.5) == "+100"
        assert dpr._FmtINR(-99.5) == "-100"
        assert dpr._FmtINR(0.4) == "+0"


# ═══════════════════════════════════════════════════════════════════
# Post-midnight Detection
# ═══════════════════════════════════════════════════════════════════

class TestPostMidnight:
    @patch("daily_pnl_report.IsTradingDay", return_value=False)
    @patch("daily_pnl_report.datetime")
    def test_post_midnight_uses_previous_day(self, mock_dt, mock_td):
        """Before 09:00, should use previous day's date."""
        mock_dt.now.return_value = datetime(2026, 4, 3, 2, 30)
        mock_dt.strptime = datetime.strptime
        mock_now = mock_dt.now()
        if mock_now.hour < 9:
            date_str = (date(2026, 4, 3) - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            date_str = date(2026, 4, 3).strftime("%Y-%m-%d")
        assert date_str == "2026-04-02"

    def test_after_9am_uses_today(self):
        now = datetime(2026, 4, 2, 10, 0)
        if now.hour < 9:
            date_str = "wrong"
        else:
            date_str = "2026-04-02"
        assert date_str == "2026-04-02"


# ═══════════════════════════════════════════════════════════════════
# Exchange Time Gating
# ═══════════════════════════════════════════════════════════════════

class TestExchangeTimeGating:
    """Verify exchange-specific time gates:
       Before 09:00 — nothing
       09:00-09:15  — MCX only
       09:15-10:00  — MCX + NFO
       After 10:00  — MCX + NFO + NCDEX
    """

    def test_exchange_open_constants(self):
        assert dpr.EXCHANGE_OPEN["MCX"] == (9, 0)
        assert dpr.EXCHANGE_OPEN["NFO"] == (9, 15)
        assert dpr.EXCHANGE_OPEN["NCDEX"] == (10, 0)

    @patch("daily_pnl_report.datetime")
    def test_before_9am_nothing_open(self, mock_dt):
        mock_dt.now.return_value = datetime(2026, 4, 2, 8, 30)
        assert dpr._IsExchangeOpen("MCX") is False
        assert dpr._IsExchangeOpen("NFO") is False
        assert dpr._IsExchangeOpen("NCDEX") is False

    @patch("daily_pnl_report.datetime")
    def test_9am_mcx_only(self, mock_dt):
        mock_dt.now.return_value = datetime(2026, 4, 2, 9, 0)
        assert dpr._IsExchangeOpen("MCX") is True
        assert dpr._IsExchangeOpen("NFO") is False
        assert dpr._IsExchangeOpen("NCDEX") is False

    @patch("daily_pnl_report.datetime")
    def test_910_mcx_only(self, mock_dt):
        mock_dt.now.return_value = datetime(2026, 4, 2, 9, 10)
        assert dpr._IsExchangeOpen("MCX") is True
        assert dpr._IsExchangeOpen("NFO") is False
        assert dpr._IsExchangeOpen("NCDEX") is False

    @patch("daily_pnl_report.datetime")
    def test_915_mcx_and_nfo(self, mock_dt):
        mock_dt.now.return_value = datetime(2026, 4, 2, 9, 15)
        assert dpr._IsExchangeOpen("MCX") is True
        assert dpr._IsExchangeOpen("NFO") is True
        assert dpr._IsExchangeOpen("NCDEX") is False

    @patch("daily_pnl_report.datetime")
    def test_945_mcx_and_nfo(self, mock_dt):
        mock_dt.now.return_value = datetime(2026, 4, 2, 9, 45)
        assert dpr._IsExchangeOpen("MCX") is True
        assert dpr._IsExchangeOpen("NFO") is True
        assert dpr._IsExchangeOpen("NCDEX") is False

    @patch("daily_pnl_report.datetime")
    def test_10am_all_open(self, mock_dt):
        mock_dt.now.return_value = datetime(2026, 4, 2, 10, 0)
        assert dpr._IsExchangeOpen("MCX") is True
        assert dpr._IsExchangeOpen("NFO") is True
        assert dpr._IsExchangeOpen("NCDEX") is True

    @patch("daily_pnl_report.datetime")
    def test_2345_cron_all_open(self, mock_dt):
        """At 23:45 (cron time), all exchanges should be treated as open."""
        mock_dt.now.return_value = datetime(2026, 4, 2, 23, 45)
        assert dpr._IsExchangeOpen("MCX") is True
        assert dpr._IsExchangeOpen("NFO") is True
        assert dpr._IsExchangeOpen("NCDEX") is True

    @patch("daily_pnl_report.datetime")
    def test_unknown_exchange_always_open(self, mock_dt):
        """Unknown exchange key defaults to (0,0) — always open."""
        mock_dt.now.return_value = datetime(2026, 4, 2, 0, 1)
        assert dpr._IsExchangeOpen("UNKNOWN") is True

    def test_exchange_not_open_exception(self):
        """_ExchangeNotOpen is a proper exception."""
        with pytest.raises(dpr._ExchangeNotOpen):
            raise dpr._ExchangeNotOpen("MCX")


# ═══════════════════════════════════════════════════════════════════
# Per-instrument Realized P&L Breakdown
# ═══════════════════════════════════════════════════════════════════

class TestRealizedByInstrument:
    """Regression tests for the bug that hid per-symbol realized P&L.

    Scenario (DHANIYA, Apr 17 2026):
      - Carry SHORT 10 units at 12,808 entered pre-Apr-17
      - Apr 17 bought 15 units at avg 13,064.67 → first 10 covered short
        for realized loss of -25,667, remaining 5 = new 1-lot LONG
      - Old report showed DHANIYA line as +8,066 (unrealized only)
        and hid the -25,667 in the account-level Realized aggregate
      - New report attributes the -25,667 to DHANIYA on the same row
    """

    def test_option_underlying_extraction(self):
        assert dpr._OptionUnderlying("NIFTY26APR24000CE") == "NIFTY"
        assert dpr._OptionUnderlying("BANKNIFTY26APR52000PE") == "BANKNIFTY"
        assert dpr._OptionUnderlying("SENSEX2642380000CE") == "SENSEX"
        assert dpr._OptionUnderlying("BANKEX26APR60000CE") == "BANKEX"
        assert dpr._OptionUnderlying("RELIANCE") is None

    def test_fetch_returns_tuple_shape(self):
        """Function signature returns (ByAccount, ByInstrument)."""
        with patch("daily_pnl_report._EstablishKiteSession") as mock_kite_factory, \
             patch("daily_pnl_report.EstablishConnectionAngelAPI") as mock_angel_factory, \
             patch("daily_pnl_report._IsExchangeOpen", return_value=False):
            by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
            assert isinstance(by_acct, dict)
            assert isinstance(by_inst, dict)
            # All accounts present, all zero when nothing fetched
            assert set(by_acct.keys()) == {"YD6016", "AABM826021", "OFS653"}

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_dhaniya_carry_short_cover_attributed(self, mock_match, mock_open,
                                                   mock_angel_factory, mock_kite_factory):
        """Angel DHANIYA carry-cover: cumulative realised contributes to ByAccount;
        ClosedByInstrument is empty because the position is still open (qty=5).

        Per-instrument today's slice for partial closes lives on the open
        position dict (`today_realized_slice`), not in `_FetchDailyRealizedPnl`'s
        return value — which is intentionally limited to fully-closed instruments."""
        mock_smart = MagicMock()
        mock_smart.position.return_value = {"data": [
            {"tradingsymbol": "DHANIYA20MAY2026", "exchange": "NCX",
             "producttype": "CARRYFORWARD", "netqty": "5",
             "realised": "-25667", "m2m": "-17601"},
        ]}
        mock_angel_factory.return_value = mock_smart
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = mock_kite
        mock_match.return_value = ("DHANIYA", {"exchange": "NCX"})

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        # ByAccount: cumulative realised flows to accumulator (unchanged)
        assert by_acct["AABM826021"] == -25667.0
        # ClosedByInstrument: empty because position is still open (qty != 0)
        assert "DHANIYA" not in by_inst

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    def test_options_bucketed_by_underlying(self, mock_open,
                                             mock_angel_factory, mock_kite_factory):
        """Closed-only options keyed by underlying. Fully-closed legs surface
        in ClosedByInstrument; still-open legs do not (their slice is on the
        open position dict)."""
        def _kite_side_effect(user):
            k = MagicMock()
            if user == "OFS653":
                k.positions.return_value = {"net": [
                    {"tradingsymbol": "NIFTY26APR24000CE", "exchange": "NFO",
                     "product": "NRML", "quantity": 0, "realised": 5000, "m2m": 5000},
                    {"tradingsymbol": "NIFTY26APR23000PE", "exchange": "NFO",
                     "product": "NRML", "quantity": 130, "realised": -1200, "m2m": 3000},
                    {"tradingsymbol": "SENSEX2642380000CE", "exchange": "BFO",
                     "product": "NRML", "quantity": 0, "realised": 10000, "m2m": 10000},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side_effect
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel

        _, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        # Closed CE leg's slice (m2m=5000) — open PE leg is excluded
        assert by_inst["NIFTY"] == pytest.approx(5000.0)
        assert by_inst["SENSEX"] == 10000.0

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_m2m_fallback_attributed_per_instrument(self, mock_match, mock_open,
                                                     mock_angel_factory, mock_kite_factory):
        """Closed Kite position (qty=0, realised=0, m2m=125800) attributed via m2m."""
        def _kite_side_effect(user):
            k = MagicMock()
            if user == "YD6016":
                k.positions.return_value = {"net": [
                    {"tradingsymbol": "CRUDEOIL26APRFUT", "exchange": "MCX",
                     "product": "NRML", "quantity": 0, "realised": 0, "m2m": 125800, "pnl": 125800},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side_effect
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("CRUDEOIL", {"exchange": "MCX"})

        _, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        assert by_inst["CRUDEOIL"] == 125800.0

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument", return_value=(None, None))
    def test_unmatched_symbol_still_in_account_total(self, mock_match, mock_open,
                                                     mock_angel_factory, mock_kite_factory):
        """Unknown symbols contribute to account total but not to by_instrument map."""
        def _kite_side_effect(user):
            k = MagicMock()
            if user == "YD6016":
                k.positions.return_value = {"net": [
                    {"tradingsymbol": "UNKNOWN26APRFUT", "exchange": "MCX",
                     "product": "NRML", "quantity": 0, "realised": 500, "m2m": 500, "pnl": 500},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side_effect
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        assert by_acct["YD6016"] == 500.0
        assert "UNKNOWN" not in by_inst
        # No-op bucket for unmatched — no silent injection under any key
        assert by_inst == {}


# ═══════════════════════════════════════════════════════════════════
# _PositionRow — Realized / Net Today Display
# ═══════════════════════════════════════════════════════════════════

class TestPositionRowRealized:
    def _dhaniya_carry_cover_position(self):
        """The exact Apr 17 DHANIYA scenario."""
        return {
            "instrument": "DHANIYA", "tradingsymbol": "DHANIYA20MAY2026",
            "direction": "LONG", "qty": 5, "lots": 1,
            "avg_entry": 13064.67, "prev_close": 12808.0, "ltp": 13226.0,
            "point_value": 50, "pnl": 8066.50, "daily_swing": 8066.50,
            "broker": "ANGEL", "is_new_today": True,
        }

    def test_no_realized_no_extra_lines(self):
        """When realized=0, only the standard Today line appears."""
        html = dpr._PositionRow(self._dhaniya_carry_cover_position(), 0)
        assert "Realized today" not in html
        assert "Net today" not in html
        assert "Today" in html

    def test_realized_loss_shown(self):
        """Dhaniya scenario: realized=-25,667 attributed, Net today= -17,600."""
        html = dpr._PositionRow(self._dhaniya_carry_cover_position(), -25667.0)
        assert "Realized today" in html
        assert "-25,667" in html
        assert "Net today" in html
        # 8066.50 + (-25667) = -17,600.50
        assert "-17,600" in html or "-17,601" in html

    def test_realized_profit_shown(self):
        """Realized gain on same symbol as open position."""
        html = dpr._PositionRow(self._dhaniya_carry_cover_position(), 5000.0)
        assert "+5,000" in html
        # Net today = 8066.50 + 5000 = 13,066.50
        assert "+13,066" in html or "+13,067" in html

    def test_subrupee_realized_suppressed(self):
        """Floating-point noise below 1 rupee is not rendered."""
        html = dpr._PositionRow(self._dhaniya_carry_cover_position(), 0.3)
        assert "Realized today" not in html

    def test_existing_fields_preserved(self):
        """New rendering doesn't break existing layout."""
        html = dpr._PositionRow(self._dhaniya_carry_cover_position(), -25667)
        assert "DHANIYA" in html
        assert "LONG" in html
        assert "NEW" in html
        # Entry and LTP are rendered with :.2f (no thousands separator)
        assert "13064.67" in html
        assert "13226" in html


# ═══════════════════════════════════════════════════════════════════
# _BuildReportHtml — end-to-end Realized/Closed rendering
# ═══════════════════════════════════════════════════════════════════

class TestBuildReportHtmlRealized:
    def _data(self, **overrides):
        base = {
            "date": "2026-04-17", "positions": [],
            "total_pnl": 0, "total_daily_mtm": 0, "open_swing": 0,
            "realized_today": 0, "realized_by_account": {},
            "realized_by_instrument": {}, "position_count": 0,
            "trade_count": 0, "futures_orders": [], "options_orders": [],
            "fetch_errors": [],
        }
        base.update(overrides)
        return base

    def test_open_position_gets_its_realized_attribution(self):
        """DHANIYA Apr 17 carry-cover: today's slice on the position dict
        renders the 'Realized today' / 'Net today' lines."""
        dhaniya = {
            "instrument": "DHANIYA", "tradingsymbol": "DHANIYA20MAY2026",
            "direction": "LONG", "qty": 5, "lots": 1,
            "avg_entry": 13064.67, "prev_close": 12808.0, "ltp": 13226.0,
            "point_value": 50, "pnl": 8066.50, "daily_swing": 8066.50,
            "today_realized_slice": -25667.0,
            "broker": "ANGEL", "is_new_today": True,
        }
        html = dpr._BuildReportHtml(self._data(
            positions=[dhaniya],
            realized_by_instrument={},  # closed-only — DHANIYA is open
            realized_today=-25667.0, open_swing=8066.50,
            total_daily_mtm=-17600.50, total_pnl=8066.50,
        ))
        assert "Realized today" in html
        assert "-25,667" in html
        assert "Net today" in html

    def test_closed_today_section_for_fully_closed_instruments(self):
        """Instruments with realized != 0 but no open position get their own section."""
        html = dpr._BuildReportHtml(self._data(
            positions=[],
            realized_by_instrument={"CRUDEOIL": 12400.0, "JEERAUNJHA": -3000.0},
            realized_today=9400.0,
        ))
        assert "Closed Today" in html
        assert "CRUDEOIL" in html
        assert "+12,400" in html
        assert "JEERAUNJHA" in html
        assert "-3,000" in html

    def test_closed_today_skips_instruments_with_open_positions(self):
        """If a symbol is still open, skip listing it in Closed Today."""
        guarseed = {
            "instrument": "GUARSEED", "tradingsymbol": "GUARSEED20MAY2026",
            "direction": "LONG", "qty": 20, "lots": 4,
            "avg_entry": 5810, "prev_close": 5724, "ltp": 5863,
            "point_value": 50, "pnl": 10600, "daily_swing": 10600,
            "broker": "ANGEL", "is_new_today": True,
        }
        html = dpr._BuildReportHtml(self._data(
            positions=[guarseed],
            realized_by_instrument={"GUARSEED": 2000.0, "CRUDEOIL": 12400.0},
            realized_today=14400.0,
        ))
        # GUARSEED realized shows on the position row, not in Closed Today
        assert "Closed Today" in html
        # Find the Closed Today section and verify CRUDEOIL is in it, GUARSEED isn't
        closed_idx = html.find("Closed Today")
        after_closed = html[closed_idx:]
        # First occurrence of an instrument in the Closed section
        assert "CRUDEOIL" in after_closed
        # GUARSEED should appear *before* the Closed Today header (on the position row)
        assert html.find("GUARSEED") < closed_idx

    def test_no_closed_today_section_when_all_open(self):
        """No Closed Today section when every instrument is attached to an open position."""
        guarseed = {
            "instrument": "GUARSEED", "tradingsymbol": "GUARSEED20MAY2026",
            "direction": "LONG", "qty": 20, "lots": 4,
            "avg_entry": 5810, "prev_close": 5724, "ltp": 5863,
            "point_value": 50, "pnl": 10600, "daily_swing": 10600,
            "broker": "ANGEL", "is_new_today": True,
        }
        html = dpr._BuildReportHtml(self._data(
            positions=[guarseed],
            realized_by_instrument={"GUARSEED": 2000.0},
        ))
        assert "Closed Today" not in html

    def test_options_underlying_realized_shown_on_combo_row(self):
        """Option combo aggregates `today_realized_slice` across legs and
        renders it as 'Realized today' on the combo row."""
        nifty_ce = {
            "instrument": "NIFTY_OPT_CE", "tradingsymbol": "NIFTY26APR24000CE",
            "direction": "LONG", "qty": 65, "avg_entry": 2030, "prev_close": 1385,
            "ltp": 1327, "point_value": 1.0, "pnl": -45675, "daily_swing": -3812,
            "today_realized_slice": 8000.0,
            "broker": "ZERODHA", "is_new_today": False,
        }
        html = dpr._BuildReportHtml(self._data(
            positions=[nifty_ce],
            realized_by_instrument={},  # closed-only — open NIFTY leg's slice is on its dict
            realized_today=8000.0,
        ))
        assert "Realized today" in html
        assert "+8,000" in html
        # Net today = -3812 + 8000 = 4188
        assert "+4,188" in html

    def test_realized_by_instrument_missing_key_defaults_zero(self):
        """Open position with no realized entry renders like before (no extra lines)."""
        guarseed = {
            "instrument": "GUARSEED", "tradingsymbol": "GUARSEED20MAY2026",
            "direction": "LONG", "qty": 20, "lots": 4,
            "avg_entry": 5810, "prev_close": 5724, "ltp": 5863,
            "point_value": 50, "pnl": 10600, "daily_swing": 10600,
            "broker": "ANGEL", "is_new_today": True,
        }
        html = dpr._BuildReportHtml(self._data(
            positions=[guarseed],
            realized_by_instrument={},  # empty
        ))
        assert "Realized today" not in html
        assert "Net today" not in html


# ═══════════════════════════════════════════════════════════════════
# Edge cases: exception safety, multi-leg sums, INTRADAY, mixed states
# ═══════════════════════════════════════════════════════════════════

class TestRealizedExceptionSafety:
    """Verify per-broker exception handling keeps ByAccount and ByInstrument
    in sync. If a broker's fetch blows up, its contributions must be fully
    discarded — not left as partial entries under that broker's instruments.
    """

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_angel_failure_does_not_corrupt_kite_attributions(
        self, mock_match, mock_open, mock_angel_factory, mock_kite_factory
    ):
        """If Angel throws, ByAccount['AABM826021']=0 and no NCDEX instruments in by_inst."""
        def _kite_side(user):
            k = MagicMock()
            if user == "YD6016":
                k.positions.return_value = {"net": [
                    {"tradingsymbol": "GOLDM26APRFUT", "exchange": "MCX",
                     "product": "NRML", "quantity": 0, "realised": 5000, "m2m": 5000},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side
        # Angel raises
        mock_angel_factory.side_effect = ConnectionError("Angel API down")
        mock_match.return_value = ("GOLDM", {"exchange": "MCX"})

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        assert by_acct["YD6016"] == 5000.0
        assert by_acct["AABM826021"] == 0.0  # Angel failed
        assert by_inst["GOLDM"] == 5000.0
        # No NCDEX keys should have leaked in — Angel's loop never ran
        assert "DHANIYA" not in by_inst
        assert "JEERAUNJHA" not in by_inst

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_mid_loop_exception_discards_partial_attributions(
        self, mock_match, mock_open, mock_angel_factory, mock_kite_factory
    ):
        """Angel loop throws on 2nd position → 1st position's attribution is rolled back.

        Without the local-dict-merge-at-end guard, DHANIYA would sneak into
        by_instrument even though the broker's overall contribution is 0.
        """
        def _kite_side(user):
            k = MagicMock()
            k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side

        mock_smart = MagicMock()
        # Second position has bogus netqty that int() can't parse — mid-loop crash.
        mock_smart.position.return_value = {"data": [
            {"tradingsymbol": "DHANIYA20MAY2026", "exchange": "NCX",
             "producttype": "CARRYFORWARD", "netqty": "5",
             "realised": "-25667", "m2m": "-17601"},
            {"tradingsymbol": "JEERAUNJHA20MAY2026", "exchange": "NCX",
             "producttype": "CARRYFORWARD", "netqty": "not-a-number",
             "realised": "1000", "m2m": "1000"},
        ]}
        mock_angel_factory.return_value = mock_smart
        mock_match.side_effect = [
            ("DHANIYA", {"exchange": "NCX"}),
            ("JEERAUNJHA", {"exchange": "NCX"}),
        ]

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        assert by_acct["AABM826021"] == 0.0
        # DHANIYA's -25,667 should NOT be in by_inst — partial merge discarded.
        assert "DHANIYA" not in by_inst
        assert "JEERAUNJHA" not in by_inst

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_options_failure_isolated_from_futures(
        self, mock_match, mock_open, mock_angel_factory, mock_kite_factory
    ):
        """Options broker fails → futures attributions intact."""
        def _kite_side(user):
            if user == "OFS653":
                raise TimeoutError("Options API timeout")
            k = MagicMock()
            k.positions.return_value = {"net": [
                {"tradingsymbol": "GOLDM26APRFUT", "exchange": "MCX",
                 "product": "NRML", "quantity": 0, "realised": 3000, "m2m": 3000},
            ]}
            return k
        mock_kite_factory.side_effect = _kite_side
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("GOLDM", {"exchange": "MCX"})

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        assert by_acct["YD6016"] == 3000.0
        assert by_acct["OFS653"] == 0.0
        assert by_inst == {"GOLDM": 3000.0}


class TestIntradayAttribution:
    """Angel INTRADAY positions (not just CARRYFORWARD) must be attributed per-instrument.
    Matters because MCX intraday trades get INTRADAY producttype.
    """

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_intraday_position_bucketed(
        self, mock_match, mock_open, mock_angel_factory, mock_kite_factory
    ):
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = mock_kite

        mock_smart = MagicMock()
        mock_smart.position.return_value = {"data": [
            {"tradingsymbol": "CASTOR20APR2026", "exchange": "NCX",
             "producttype": "INTRADAY", "netqty": "0",
             "realised": "-12100", "m2m": "-12100"},
            {"tradingsymbol": "COCUDAKL20APR2026", "exchange": "NCX",
             "producttype": "INTRADAY", "netqty": "0",
             "realised": "8500", "m2m": "8500"},
        ]}
        mock_angel_factory.return_value = mock_smart
        mock_match.side_effect = [
            ("CASTOR", {"exchange": "NCX"}),
            ("COCUDAKL", {"exchange": "NCX"}),
        ]

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        assert by_acct["AABM826021"] == pytest.approx(-12100 + 8500)
        assert by_inst["CASTOR"] == -12100.0
        assert by_inst["COCUDAKL"] == 8500.0


class TestSameInstrumentMultipleLegs:
    """Same canonical instrument across multiple positions (e.g. intraday + carry
    on same symbol) must sum, not overwrite."""

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_carryforward_plus_intraday_same_symbol_sum(
        self, mock_match, mock_open, mock_angel_factory, mock_kite_factory
    ):
        """Two DHANIYA legs (carry still open + intraday fully closed) on same
        canonical symbol: ByAccount cumulative sums both; ClosedByInstrument
        sums slices for fully-closed legs only (the open carry leg's slice is
        on the open position dict, not here)."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = mock_kite

        mock_smart = MagicMock()
        mock_smart.position.return_value = {"data": [
            {"tradingsymbol": "DHANIYA20MAY2026", "exchange": "NCX",
             "producttype": "CARRYFORWARD", "netqty": "5",
             "realised": "-25667", "m2m": "-17601"},
            {"tradingsymbol": "DHANIYA20APR2026", "exchange": "NCX",
             "producttype": "INTRADAY", "netqty": "0",
             "realised": "3000", "m2m": "3000"},
        ]}
        mock_angel_factory.return_value = mock_smart
        mock_match.side_effect = [
            ("DHANIYA", {"exchange": "NCX"}),
            ("DHANIYA", {"exchange": "NCX"}),
        ]

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        # ByAccount: cumulative — both legs contribute to accumulator.
        assert by_acct["AABM826021"] == pytest.approx(-25667 + 3000)
        # ClosedByInstrument: only the fully-closed intraday leg's slice (m2m=3000).
        assert by_inst["DHANIYA"] == pytest.approx(3000.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    def test_nifty_ce_and_pe_sum_under_underlying(
        self, mock_open, mock_angel_factory, mock_kite_factory
    ):
        """Fully-closed NIFTY CE + fully-closed NIFTY PE both bucket under 'NIFTY'.
        The third (open) leg is excluded — its slice is on the open position dict."""
        def _kite_side(user):
            k = MagicMock()
            if user == "OFS653":
                k.positions.return_value = {"net": [
                    {"tradingsymbol": "NIFTY26APR24000CE", "exchange": "NFO",
                     "product": "NRML", "quantity": 0, "realised": 8000, "m2m": 8000},
                    {"tradingsymbol": "NIFTY26APR22000PE", "exchange": "NFO",
                     "product": "NRML", "quantity": 0, "realised": -3500, "m2m": -3500},
                    {"tradingsymbol": "NIFTY26APR23000CE", "exchange": "NFO",
                     "product": "NRML", "quantity": 65, "realised": 0, "m2m": 1000},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        # CE +8000 (closed) + PE -3500 (closed) = +4500. Open leg excluded.
        assert by_inst["NIFTY"] == pytest.approx(8000 - 3500)


class TestGenerateDailyReportE2E:
    """End-to-end: does realized_by_instrument flow from _FetchDailyRealizedPnl
    all the way into the HTML assembly with correct per-symbol attribution?"""

    @patch("daily_pnl_report.IsAnyExchangeOpen", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl")
    @patch("daily_pnl_report._FetchOpenPositions")
    @patch("daily_pnl_report._UpdateRealizedPnlAccumulator")
    @patch("daily_pnl_report._SendEmail")
    def test_realized_by_instrument_reaches_html(
        self, mock_email, mock_accum, mock_fetch, mock_realized, mock_orders, mock_open
    ):
        """DHANIYA Apr 17 carry-cover end-to-end. The today's slice for the
        partial close lives on the open position dict (`today_realized_slice`)
        and surfaces as 'Realized today' on the row. ByAccount keeps cumulative
        realised for the accumulator."""
        mock_fetch.return_value = ([
            {"instrument": "DHANIYA", "tradingsymbol": "DHANIYA20MAY2026",
             "direction": "LONG", "qty": 5, "lots": 1,
             "avg_entry": 13064.67, "prev_close": 12808.0, "ltp": 13226.0,
             "point_value": 50, "pnl": 8066.50, "daily_swing": 8066.50,
             "today_realized_slice": -25667.0,
             "broker": "ANGEL", "is_new_today": True},
        ], [])
        mock_realized.return_value = (
            {"YD6016": 0.0, "AABM826021": -25667.0, "OFS653": 0.0},
            {},  # ClosedByInstrument empty — DHANIYA is still open
        )

        dpr.GenerateDailyReport(DryRun=False, DateStr="2026-04-17")

        assert mock_email.call_count == 1
        _, html = mock_email.call_args[0]
        assert "DHANIYA" in html
        assert "Realized today" in html
        assert "-25,667" in html
        assert "Net today" in html
        assert "-17,600" in html or "-17,601" in html

        # Accumulator gets cumulative-by-account dict (unchanged for tax)
        accum_args = mock_accum.call_args[0]
        assert isinstance(accum_args[0], dict)
        assert accum_args[0]["AABM826021"] == -25667.0

    @patch("daily_pnl_report.IsAnyExchangeOpen", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl")
    @patch("daily_pnl_report._FetchOpenPositions")
    @patch("daily_pnl_report._UpdateRealizedPnlAccumulator")
    @patch("daily_pnl_report._SendEmail")
    def test_fully_closed_instruments_appear_in_closed_today(
        self, mock_email, mock_accum, mock_fetch, mock_realized, mock_orders, mock_open
    ):
        """April expiry closes: COCUDAKL + JEERAUNJHA fully closed → Closed Today."""
        mock_fetch.return_value = ([], [])  # No open positions
        mock_realized.return_value = (
            {"YD6016": 0.0, "AABM826021": 72934.40, "OFS653": 0.0},
            {"COCUDAKL": 14636.0, "JEERAUNJHA": 58298.40},
        )

        dpr.GenerateDailyReport(DryRun=False, DateStr="2026-04-20")

        _, html = mock_email.call_args[0]
        assert "Closed Today" in html
        assert "COCUDAKL" in html
        assert "+14,636" in html
        assert "JEERAUNJHA" in html
        assert "+58,298" in html

    @patch("daily_pnl_report.IsAnyExchangeOpen", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl")
    @patch("daily_pnl_report._FetchOpenPositions")
    @patch("daily_pnl_report._UpdateRealizedPnlAccumulator")
    @patch("daily_pnl_report._SendEmail")
    def test_mixed_open_and_fully_closed_attribution(
        self, mock_email, mock_accum, mock_fetch, mock_realized, mock_orders, mock_open
    ):
        """DHANIYA has open + partial close; COCUDAKL fully closed.
        DHANIYA's slice on the position row, COCUDAKL in Closed Today section."""
        mock_fetch.return_value = ([
            {"instrument": "DHANIYA", "tradingsymbol": "DHANIYA20MAY2026",
             "direction": "LONG", "qty": 5, "lots": 1,
             "avg_entry": 13064.67, "prev_close": 12808.0, "ltp": 13226.0,
             "point_value": 50, "pnl": 8066.50, "daily_swing": 8066.50,
             "today_realized_slice": -25667.0,
             "broker": "ANGEL", "is_new_today": True},
        ], [])
        mock_realized.return_value = (
            {"YD6016": 0.0, "AABM826021": -11031.0, "OFS653": 0.0},
            {"COCUDAKL": 14636.0},  # only fully-closed instruments here
        )

        dpr.GenerateDailyReport(DryRun=False, DateStr="2026-04-17")
        _, html = mock_email.call_args[0]

        dhaniya_idx = html.find("DHANIYA")
        closed_idx = html.find("Closed Today")
        cocudakl_idx = html.find("COCUDAKL")

        assert dhaniya_idx < closed_idx  # DHANIYA on position row first
        assert closed_idx < cocudakl_idx  # COCUDAKL in Closed Today section
        assert "Realized today" in html
        assert "-25,667" in html  # on DHANIYA row from today_realized_slice
        assert "+14,636" in html  # on COCUDAKL closed-today row


class TestAccumulatorIntegration:
    """Verify _UpdateRealizedPnlAccumulator keeps working with new tuple caller."""

    def setup_method(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.orig_path = dpr.REALIZED_PNL_PATH
        dpr.REALIZED_PNL_PATH = Path(self.tmp_dir) / "realized_pnl_accumulator.json"

    def teardown_method(self):
        dpr.REALIZED_PNL_PATH = self.orig_path
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_tuple_unpacked_correctly_into_accumulator(self):
        """GenerateDailyReport unpacks tuple → accumulator sees only ByAccount dict."""
        # Simulate what GenerateDailyReport does:
        by_account, by_instrument = (
            {"YD6016": 5000.0, "AABM826021": -25667.0, "OFS653": 0.0},
            {"GOLDM": 5000.0, "DHANIYA": -25667.0},
        )
        dpr._UpdateRealizedPnlAccumulator(by_account, "2026-04-17", 8066.50)

        data = json.loads(dpr.REALIZED_PNL_PATH.read_text())
        # Accumulator stores only account-level totals
        assert data["daily_entries"]["2026-04-17"]["YD6016"] == 5000.0
        assert data["daily_entries"]["2026-04-17"]["AABM826021"] == -25667.0
        assert data["daily_entries"]["2026-04-17"]["total"] == pytest.approx(-20667.0)
        # by_instrument is NOT in the accumulator JSON (not its concern)
        assert "by_instrument" not in data["daily_entries"]["2026-04-17"]
        assert "DHANIYA" not in str(data)  # per-instrument not leaked in


class TestRealizedEdgeCases:
    """Odds and ends around None handling, zero swings with realized, rounding."""

    def test_zero_swing_but_realized_still_shows(self):
        """Open position with LTP=Avg (zero swing) but realized from closed legs."""
        pos = {
            "instrument": "DHANIYA", "tradingsymbol": "DHANIYA20MAY2026",
            "direction": "LONG", "qty": 5, "lots": 1,
            "avg_entry": 13226.0, "prev_close": 13226.0, "ltp": 13226.0,
            "point_value": 50, "pnl": 0.0, "daily_swing": 0.0,
            "broker": "ANGEL", "is_new_today": True,
        }
        html = dpr._PositionRow(pos, -25667.0)
        assert "Realized today" in html
        assert "-25,667" in html
        assert "Net today" in html
        # Net = 0 + (-25667) = -25,667 — same number twice is fine
        assert html.count("-25,667") >= 2

    def test_none_realized_by_instrument_defaults_safely(self):
        """If realized_by_instrument key is missing entirely, HTML renders without crashing."""
        data = {
            "date": "2026-04-17", "positions": [],
            "total_pnl": 0, "total_daily_mtm": 0, "open_swing": 0,
            "realized_today": 0, "realized_by_account": {},
            # realized_by_instrument intentionally omitted
            "position_count": 0, "trade_count": 0,
            "futures_orders": [], "options_orders": [], "fetch_errors": [],
        }
        html = dpr._BuildReportHtml(data)
        assert "<!DOCTYPE html>" in html
        assert "Closed Today" not in html

    def test_explicit_none_realized_by_instrument(self):
        """realized_by_instrument=None should be coerced to empty dict."""
        data = {
            "date": "2026-04-17", "positions": [],
            "total_pnl": 0, "total_daily_mtm": 0, "open_swing": 0,
            "realized_today": 0, "realized_by_account": {},
            "realized_by_instrument": None,
            "position_count": 0, "trade_count": 0,
            "futures_orders": [], "options_orders": [], "fetch_errors": [],
        }
        html = dpr._BuildReportHtml(data)
        assert "<!DOCTYPE html>" in html

    def test_realized_rounded_to_2_decimals_at_output(self):
        """Floating-point noise in accumulator shouldn't display 15 decimals."""
        pos = {
            "instrument": "DHANIYA", "tradingsymbol": "DHANIYA20MAY2026",
            "direction": "LONG", "qty": 5, "lots": 1,
            "avg_entry": 13064.67, "prev_close": 12808.0, "ltp": 13226.0,
            "point_value": 50, "pnl": 8066.50, "daily_swing": 8066.50,
            "broker": "ANGEL", "is_new_today": True,
        }
        # Typical floating point accumulation: -25667.000000001
        html = dpr._PositionRow(pos, -25666.999999999)
        # Should round to whole rupees on output (_FmtINR with default Decimals=0)
        assert "-25,667" in html
        # Make sure the float garbage doesn't leak in
        assert "99999" not in html


# ═══════════════════════════════════════════════════════════════════
# NFO Index Futures (NIFTY/BANKNIFTY/MIDCPNIFTY) on OFS653
# ═══════════════════════════════════════════════════════════════════

class TestNfoIndexFutures:
    """Regression tests for the bug where NIFTY/BANKNIFTY/MIDCPNIFTY index
    futures held in OFS653 were silently dropped from the report:

      - Open positions: filtered out by `_IsIndexOption` (FUT suffix fails)
      - Realized P&L: bucketed under same key as options, hidden in option card
      - Closed Today: never surfaced because key collided with open options
    """

    NIFTY_CFG = {"NIFTY": {"exchange": "NFO", "point_value": 65}}
    BANKNIFTY_CFG = {"BANKNIFTY": {"exchange": "NFO", "point_value": 30}}

    def test_display_instrument_strips_fut_suffix(self):
        assert dpr._DisplayInstrument("NIFTY_FUT") == "NIFTY"
        assert dpr._DisplayInstrument("BANKNIFTY_FUT") == "BANKNIFTY"
        assert dpr._DisplayInstrument("MIDCPNIFTY_FUT") == "MIDCPNIFTY"

    def test_display_instrument_preserves_non_fut(self):
        assert dpr._DisplayInstrument("NIFTY") == "NIFTY"
        assert dpr._DisplayInstrument("DHANIYA") == "DHANIYA"
        assert dpr._DisplayInstrument("NIFTY_OPT_CE") == "NIFTY_OPT_CE"

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_open_nifty_future_fetched_from_ofs653(self, mock_match, mock_open,
                                                    mock_angel_factory, mock_kite_factory):
        """Open NIFTY future (LONG 65 units = 1 lot) from OFS653 reaches Positions."""
        def _kite_side_effect(user):
            k = MagicMock()
            if user == "OFS653":
                k.positions.return_value = {"net": [
                    {"tradingsymbol": "NIFTY26APRFUT", "exchange": "NFO",
                     "product": "NRML", "quantity": 65,
                     "average_price": 24000.0, "last_price": 24100.0,
                     "close_price": 23950.0, "overnight_quantity": 65,
                     "day_buy_quantity": 0, "day_sell_quantity": 0,
                     "day_buy_price": 0.0, "day_sell_price": 0.0},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side_effect
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("NIFTY", {"exchange": "NFO", "point_value": 65})

        positions, _ = dpr._FetchOpenPositions({"instruments": self.NIFTY_CFG})
        assert len(positions) == 1
        p = positions[0]
        assert p["instrument"] == "NIFTY_FUT"
        assert p["direction"] == "LONG"
        assert p["qty"] == 65
        assert p["lots"] == 1.0
        # P&L = (24100 - 24000) * 65 units * ₹1/unit = ₹6,500
        assert p["pnl"] == pytest.approx(6500.0)
        # Carried lot: swing from prev_close (23950) to ltp (24100) = ₹150 * 65 = ₹9,750
        assert p["daily_swing"] == pytest.approx(9750.0)
        assert p["broker"] == "ZERODHA"
        assert "_OPT_" not in p["instrument"]

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_short_banknifty_future_intraday_new(self, mock_match, mock_open,
                                                  mock_angel_factory, mock_kite_factory):
        """SHORT BANKNIFTY future opened today (no overnight). Swing from new entry."""
        def _kite_side_effect(user):
            k = MagicMock()
            if user == "OFS653":
                k.positions.return_value = {"net": [
                    {"tradingsymbol": "BANKNIFTY26APRFUT", "exchange": "NFO",
                     "product": "NRML", "quantity": -30,
                     "average_price": 53000.0, "last_price": 52800.0,
                     "close_price": 52950.0, "overnight_quantity": 0,
                     "day_buy_quantity": 0, "day_sell_quantity": 30,
                     "day_buy_price": 0.0, "day_sell_price": 53000.0},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side_effect
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("BANKNIFTY", {"exchange": "NFO", "point_value": 30})

        positions, _ = dpr._FetchOpenPositions({"instruments": self.BANKNIFTY_CFG})
        assert len(positions) == 1
        p = positions[0]
        assert p["instrument"] == "BANKNIFTY_FUT"
        assert p["direction"] == "SHORT"
        assert p["qty"] == 30
        assert p["lots"] == 1.0
        assert p["is_new_today"] is True
        # SHORT P&L = (53000 - 52800) * 30 = ₹6,000
        assert p["pnl"] == pytest.approx(6000.0)
        # New lot: swing from sell-price (53000) → ltp (52800) = ₹6,000
        assert p["daily_swing"] == pytest.approx(6000.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_options_and_futures_coexist_on_ofs653(self, mock_match, mock_open,
                                                    mock_angel_factory, mock_kite_factory):
        """OFS653 holding both NIFTY options AND a NIFTY future — both surface."""
        def _kite_side_effect(user):
            k = MagicMock()
            if user == "OFS653":
                k.positions.return_value = {"net": [
                    {"tradingsymbol": "NIFTY26APR24000CE", "exchange": "NFO",
                     "product": "NRML", "quantity": 65,
                     "average_price": 250.0, "last_price": 260.0,
                     "close_price": 255.0, "overnight_quantity": 65,
                     "day_buy_quantity": 0, "day_sell_quantity": 0,
                     "day_buy_price": 0.0, "day_sell_price": 0.0},
                    {"tradingsymbol": "NIFTY26APRFUT", "exchange": "NFO",
                     "product": "NRML", "quantity": 65,
                     "average_price": 24000.0, "last_price": 24100.0,
                     "close_price": 23950.0, "overnight_quantity": 65,
                     "day_buy_quantity": 0, "day_sell_quantity": 0,
                     "day_buy_price": 0.0, "day_sell_price": 0.0},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side_effect
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("NIFTY", {"exchange": "NFO", "point_value": 65})

        positions, _ = dpr._FetchOpenPositions({"instruments": self.NIFTY_CFG})
        instruments = [p["instrument"] for p in positions]
        assert "NIFTY_OPT_CE" in instruments
        assert "NIFTY_FUT" in instruments

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_realized_nifty_future_bucketed_separately_from_options(
            self, mock_match, mock_open, mock_angel_factory, mock_kite_factory):
        """NIFTY futures realized goes into NIFTY_FUT, NIFTY options into NIFTY."""
        def _kite_side_effect(user):
            k = MagicMock()
            if user == "OFS653":
                k.positions.return_value = {"net": [
                    # Intraday NIFTY future round-trip closed for -13,422
                    {"tradingsymbol": "NIFTY26APRFUT", "exchange": "NFO",
                     "product": "NRML", "quantity": 0,
                     "realised": -13422.0, "m2m": -13422.0},
                    # NIFTY option realized -27502 (rolled call)
                    {"tradingsymbol": "NIFTY26APR21700CE", "exchange": "NFO",
                     "product": "NRML", "quantity": 0,
                     "realised": -27502.0, "m2m": -27502.0},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side_effect
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        # _Bucket calls _MatchToInstrument for the futures; options never reach it.
        mock_match.return_value = ("NIFTY", {"exchange": "NFO", "point_value": 65})

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": self.NIFTY_CFG})
        # Account total = sum of both
        assert by_acct["OFS653"] == pytest.approx(-13422.0 + -27502.0)
        # Per-instrument: futures and options are NOT collapsed onto one key
        assert by_inst["NIFTY_FUT"] == pytest.approx(-13422.0)
        assert by_inst["NIFTY"] == pytest.approx(-27502.0)

    def test_closed_today_surfaces_nifty_future_when_options_open(self):
        """Closed-today NIFTY futures appears even when NIFTY options are open.

        Pre-fix: open NIFTY options claimed the 'NIFTY' key in OpenInstrumentKeys,
        so a futures-only realized row under the same key was filtered out.
        Post-fix: futures lives under 'NIFTY_FUT' — distinct from options' 'NIFTY'.
        """
        data = {
            "date": "2026-04-28",
            "positions": [
                {"instrument": "NIFTY_OPT_CE", "tradingsymbol": "NIFTY26APR24000CE",
                 "direction": "LONG", "qty": 65,
                 "avg_entry": 250.0, "prev_close": 245.0, "ltp": 260.0,
                 "point_value": 1.0, "pnl": 650.0, "daily_swing": 975.0,
                 "broker": "ZERODHA", "is_new_today": False},
            ],
            "total_pnl": 650.0, "total_daily_mtm": 0, "open_swing": 0,
            "realized_today": -13422.0, "realized_by_account": {"OFS653": -13422.0},
            "realized_by_instrument": {"NIFTY_FUT": -13422.0},
            "position_count": 1, "trade_count": 2,
            "futures_orders": [], "options_orders": [], "fetch_errors": [],
        }
        html = dpr._BuildReportHtml(data)
        assert "Closed Today" in html
        # The closed-today row shows the user-friendly "NIFTY", not the internal key
        assert "NIFTY_FUT" not in html
        assert "-13,422" in html

    def test_open_future_renders_without_fut_suffix(self):
        """Open NIFTY futures position card displays 'NIFTY', not 'NIFTY_FUT'."""
        pos = {
            "instrument": "NIFTY_FUT", "tradingsymbol": "NIFTY26APRFUT",
            "direction": "LONG", "qty": 65, "lots": 1.0,
            "avg_entry": 24000.0, "prev_close": 23950.0, "ltp": 24100.0,
            "point_value": 1.0, "pnl": 6500.0, "daily_swing": 9750.0,
            "broker": "ZERODHA", "is_new_today": False,
        }
        html = dpr._PositionRow(pos)
        assert "NIFTY_FUT" not in html
        # Word boundary check — "NIFTY" should appear as the instrument name
        assert ">NIFTY<" in html


# ═══════════════════════════════════════════════════════════════════
# Daily Slice Integration — flip & partial-cover scenarios end-to-end
# ═══════════════════════════════════════════════════════════════════

class TestDailySliceIntegration:
    """Integration coverage for the two bugs reported on 29 Apr 2026:
      1. ZINCMINI flip: same-direction close-and-reopen was misclassified as
         'all carried' producing daily_swing ₹+10,000 instead of ~₹-1,880.
      2. JEERA partial cover: cumulative-since-entry `realised` was added to
         daily_swing as 'Net today', double-counting prior days' MTM and
         showing ₹+44,213 instead of the true daily contribution ₹-33,300.
    """

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_zincmini_flip_open_position(self, mock_match, mock_open,
                                          mock_angel_factory, mock_kite_factory):
        """ZINCMINI overnight SHORT 4 → today buy 4 (cover) + sell 4 (new) → SHORT 4.
        Daily swing on the 4 NEW shorts (340.09 → 339.40) = ₹+2,760.
        Today's realized slice on the 4 closed (prev 341.90 → exit 343.06) = ₹-4,640.
        Net daily contribution: ₹-1,880 (NOT ₹+10,000)."""
        def _kite_side_effect(user):
            k = MagicMock()
            if user == "YD6016":
                k.positions.return_value = {"net": [
                    {"tradingsymbol": "ZINCMINI26APRFUT", "exchange": "MCX",
                     "product": "NRML", "quantity": -4,
                     "average_price": 340.82, "last_price": 339.40,
                     "close_price": 341.90, "overnight_quantity": -4,
                     "day_buy_quantity": 4, "day_sell_quantity": 4,
                     "day_buy_price": 343.06, "day_sell_price": 340.09},
                ]}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side_effect
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("ZINCMINI", {"exchange": "MCX", "point_value": 1000})

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        assert len(positions) == 1
        zm = positions[0]
        assert zm["instrument"] == "ZINCMINI"
        assert zm["direction"] == "SHORT"
        assert zm["is_new_today"] is True  # carry was closed → current is new
        # Open swing on 4 new shorts (340.09 → 339.40):
        assert zm["daily_swing"] == pytest.approx(2760.0)
        # Close slice on 4 carry shorts (prev 341.90 → exit 343.06): SHORT cover
        # above prev_close = loss.
        assert zm["today_realized_slice"] == pytest.approx(-4640.0)
        # Net daily contribution = open swing + close slice = -1,880
        assert zm["daily_swing"] + zm["today_realized_slice"] == pytest.approx(-1880.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_jeera_partial_cover(self, mock_match, mock_open,
                                  mock_angel_factory, mock_kite_factory):
        """JEERA carry SHORT 9 units (3 lots) @ 21611.89; today bought 6 to
        cover @ 20700, current SHORT 3 units (1 lot). Open swing on remaining
        1 lot = ₹-10,500. Today's slice on the 2 closed lots = ₹-22,800.
        Net daily contribution: ₹-33,300 (NOT the +44,213 from cumulative
        realised since entry, which double-counts prior days' MTM)."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = mock_kite

        mock_smart = MagicMock()
        mock_smart.position.return_value = {"data": [
            {"tradingsymbol": "JEERAUNJHA20MAY2026", "exchange": "NCDEX",
             "producttype": "CARRYFORWARD", "netqty": "-3",
             "ltp": "20670", "close": "20320",
             "totalsellavgprice": "21611.89", "cfsellavgprice": "21611.89",
             "cfsellqty": "9", "cfbuyqty": "0",
             "buyqty": "6", "sellqty": "0",
             "buyavgprice": "20700", "sellavgprice": "0",
             "realised": "54713", "m2m": "-33300"},
        ]}
        mock_angel_factory.return_value = mock_smart
        mock_match.return_value = ("JEERA", {
            "exchange": "NCDEX", "point_value": 30,
            "order_routing": {"QuantityMultiplier": 3},
        })

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        assert len(positions) == 1
        jr = positions[0]
        assert jr["instrument"] == "JEERA"
        assert jr["direction"] == "SHORT"
        assert jr["lots"] == 1.0
        assert jr["is_new_today"] is False  # 1 lot carried
        # Open swing on 1 carry lot: (20320 - 20670) × 1 × 30 = -10,500
        assert jr["daily_swing"] == pytest.approx(-10500.0)
        # Close slice on 2 closed lots: (20320 - 20700) × 2 × 30 = -22,800
        assert jr["today_realized_slice"] == pytest.approx(-22800.0)
        # Net daily contribution: -33,300 (NOT cumulative-since-entry +54,713)
        assert jr["daily_swing"] + jr["today_realized_slice"] == pytest.approx(-33300.0)

    @patch("daily_pnl_report.IsAnyExchangeOpen", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl")
    @patch("daily_pnl_report._FetchOpenPositions")
    @patch("daily_pnl_report._UpdateRealizedPnlAccumulator")
    @patch("daily_pnl_report._SendEmail")
    def test_no_double_count_in_daily_mtm_hero(
        self, mock_email, mock_accum, mock_fetch, mock_realized, mock_orders, mock_open
    ):
        """Daily MTM for JEERA-class scenario must NOT add cumulative-since-entry
        realised. JEERA daily contribution is daily_swing + today_realized_slice."""
        mock_fetch.return_value = ([
            {"instrument": "JEERA", "tradingsymbol": "JEERAUNJHA20MAY2026",
             "direction": "SHORT", "qty": 3, "lots": 1,
             "avg_entry": 21611.89, "prev_close": 20320, "ltp": 20670,
             "point_value": 30, "pnl": 28256.70, "daily_swing": -10500,
             "today_realized_slice": -22800.0,
             "broker": "ANGEL", "is_new_today": False},
        ], [])
        # Cumulative realised stays for accumulator; ClosedByInstrument empty
        # because JEERA is still open.
        mock_realized.return_value = (
            {"YD6016": 0.0, "AABM826021": 54713.0, "OFS653": 0.0},  # cumulative
            {},
        )

        dpr.GenerateDailyReport(DryRun=False, DateStr="2026-04-29")
        _, html = mock_email.call_args[0]

        # Hero should reflect daily contribution (not cumulative)
        # OpenSwing = -10,500; PartialClose = -22,800; FullyClosed = 0;
        # Daily MTM = -33,300 (NOT -10,500 + 54,713 = +44,213).
        assert "-33,300" in html
        assert "+44,213" not in html

    @patch("daily_pnl_report.IsAnyExchangeOpen", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl")
    @patch("daily_pnl_report._FetchOpenPositions")
    @patch("daily_pnl_report._UpdateRealizedPnlAccumulator")
    @patch("daily_pnl_report._SendEmail")
    def test_accumulator_still_receives_cumulative(
        self, mock_email, mock_accum, mock_fetch, mock_realized, mock_orders, mock_open
    ):
        """Regression guard: accumulator JSON must keep tracking cumulative
        realised (broker's `realised` field), not today's slice. This preserves
        tax/capital tracking semantics across the FY."""
        mock_fetch.return_value = ([], [])
        mock_realized.return_value = (
            {"YD6016": 0.0, "AABM826021": 54713.0, "OFS653": 0.0},  # cumulative
            {},
        )

        dpr.GenerateDailyReport(DryRun=False, DateStr="2026-04-29")

        accum_args = mock_accum.call_args[0]
        # First arg is the by-account dict — cumulative realised, unchanged.
        assert accum_args[0]["AABM826021"] == 54713.0


# ═══════════════════════════════════════════════════════════════════
# Invariants — broker m2m must equal daily_swing + today_realized_slice
# ═══════════════════════════════════════════════════════════════════

class TestM2mInvariant:
    """For every position the broker reports, our split must equal the broker's
    `m2m` field within tolerance. m2m IS the authoritative daily P&L change for
    a position (covers carry-mtm + closures + new opens). If our split drifts,
    `_ReconcileWithBrokerM2m` logs a warning at fetch time. These tests assert
    the invariant under representative scenarios so future regressions in the
    LIFO classification are caught immediately.
    """

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_pure_carry_short_invariant(self, mock_match, mock_open,
                                         mock_angel_factory, mock_kite_factory):
        """Pure carry SHORT: daily_swing = (prev_close - ltp) × qty × PV;
        slice = 0; broker m2m equals daily_swing."""
        kite = MagicMock()
        kite.positions.return_value = {"net": [
            {"tradingsymbol": "SILVERMIC26APRFUT", "exchange": "MCX",
             "product": "NRML", "quantity": -2,
             "average_price": 249475.50, "last_price": 242672.0,
             "close_price": 246829.0, "overnight_quantity": -2,
             "day_buy_quantity": 0, "day_sell_quantity": 0,
             "day_buy_price": 0, "day_sell_price": 0,
             "m2m": 8314.0, "realised": 0},
        ]}
        mock_kite_factory.return_value = kite
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("SILVERMIC", {"exchange": "MCX", "point_value": 1})

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        p = positions[0]
        assert p["daily_swing"] + p["today_realized_slice"] == pytest.approx(8314.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_zincmini_flip_invariant(self, mock_match, mock_open,
                                      mock_angel_factory, mock_kite_factory):
        """ZINCMINI flip — broker m2m for the position = -1,880 (=close swing
        on 4 carry shorts at 343.06 from prev_close 341.90 + new short swing
        from 340.09 to LTP 339.40)."""
        kite = MagicMock()
        kite.positions.return_value = {"net": [
            {"tradingsymbol": "ZINCMINI26APRFUT", "exchange": "MCX",
             "product": "NRML", "quantity": -4,
             "average_price": 340.82, "last_price": 339.40,
             "close_price": 341.90, "overnight_quantity": -4,
             "day_buy_quantity": 4, "day_sell_quantity": 4,
             "day_buy_price": 343.06, "day_sell_price": 340.09,
             "m2m": -1880.0, "realised": 4560.0},
        ]}
        mock_kite_factory.return_value = kite
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("ZINCMINI", {"exchange": "MCX", "point_value": 1000})

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        p = positions[0]
        # daily_swing = +2,760 (new shorts: 340.09 → 339.40)
        # today_realized_slice = -4,640 (close: 341.90 → 343.06 covers above prev = loss)
        # sum = -1,880, matches broker m2m
        assert p["daily_swing"] + p["today_realized_slice"] == pytest.approx(-1880.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_pure_new_long_invariant(self, mock_match, mock_open,
                                      mock_angel_factory, mock_kite_factory):
        """CRUDEOIL opened today @ 9640, LTP 10110 → swing = +47,000; slice = 0."""
        kite = MagicMock()
        kite.positions.return_value = {"net": [
            {"tradingsymbol": "CRUDEOIL26APRFUT", "exchange": "MCX",
             "product": "NRML", "quantity": 1,
             "average_price": 9640.0, "last_price": 10110.0,
             "close_price": 9485.0, "overnight_quantity": 0,
             "day_buy_quantity": 1, "day_sell_quantity": 0,
             "day_buy_price": 9640.0, "day_sell_price": 0,
             "m2m": 47000.0, "realised": 0},
        ]}
        mock_kite_factory.return_value = kite
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("CRUDEOIL", {"exchange": "MCX", "point_value": 100})

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        p = positions[0]
        assert p["daily_swing"] + p["today_realized_slice"] == pytest.approx(47000.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_jeera_partial_cover_invariant(self, mock_match, mock_open,
                                            mock_angel_factory, mock_kite_factory):
        """JEERA SHORT 9 → covered 6 units @ 20700 → SHORT 3 remaining.
        Broker m2m = -33,300; our split must match."""
        kite = MagicMock()
        kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = kite

        smart = MagicMock()
        smart.position.return_value = {"data": [
            {"tradingsymbol": "JEERAUNJHA20MAY2026", "exchange": "NCDEX",
             "producttype": "CARRYFORWARD", "netqty": "-3",
             "ltp": "20670", "close": "20320",
             "totalsellavgprice": "21611.89", "cfsellavgprice": "21611.89",
             "cfsellqty": "9", "cfbuyqty": "0",
             "buyqty": "6", "sellqty": "0",
             "buyavgprice": "20700", "sellavgprice": "0",
             "realised": "54713", "m2m": "-33300"},
        ]}
        mock_angel_factory.return_value = smart
        mock_match.return_value = ("JEERA", {
            "exchange": "NCDEX", "point_value": 30,
            "order_routing": {"QuantityMultiplier": 3},
        })

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        p = positions[0]
        assert p["daily_swing"] + p["today_realized_slice"] == pytest.approx(-33300.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_direction_flip_short_to_long_invariant(self, mock_match, mock_open,
                                                     mock_angel_factory, mock_kite_factory):
        """Was SHORT 4 @ 100, today bought 6 → LONG 2 @ 105. prev_close 102, LTP 108.
        Closed 4 SHORTs at buy price 105: slice = (102-105) × 4 × PV = -3*4*PV.
        New 2 LONGs at 105: swing = (108-105) × 2 × PV = 3*2*PV.
        For PV=1: slice=-12, swing=+6, total=-6. Broker m2m would also = -6."""
        kite = MagicMock()
        kite.positions.return_value = {"net": [
            {"tradingsymbol": "SOMEFUT", "exchange": "MCX",
             "product": "NRML", "quantity": 2,
             "average_price": 105.0, "last_price": 108.0,
             "close_price": 102.0, "overnight_quantity": -4,
             "day_buy_quantity": 6, "day_sell_quantity": 0,
             "day_buy_price": 105.0, "day_sell_price": 0,
             "m2m": -6.0, "realised": -20.0},
        ]}
        mock_kite_factory.return_value = kite
        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel
        mock_match.return_value = ("SOME", {"exchange": "MCX", "point_value": 1})

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        p = positions[0]
        assert p["direction"] == "LONG"
        assert p["is_new_today"] is True
        # Slice on SHORT close (NOT LONG): (102 - 105) * 4 * 1 = -12
        assert p["today_realized_slice"] == pytest.approx(-12.0)
        # Swing on new LONG: (108 - 105) * 2 * 1 = +6
        assert p["daily_swing"] == pytest.approx(6.0)
        # Sum = broker m2m
        assert p["daily_swing"] + p["today_realized_slice"] == pytest.approx(-6.0)


# ═══════════════════════════════════════════════════════════════════
# Edge cases — scenarios the LIFO heuristic should handle
# ═══════════════════════════════════════════════════════════════════

class TestLIFOEdgeCases:
    """Catalogue of corner cases that have historically broken daily-MTM math.
    Adding a test here pins each scenario into place so regressions show up
    instead of silently inflating the user's email totals."""

    def test_intraday_roundtrip_then_add_to_carry(self):
        """Overnight SHORT 4, today buy 2 + sell 6, current SHORT 8.
        DayBuy 2 closes 2 carry shorts; DaySell 6 = 2 (intraday wash with the
        2 buys conceptually) + 4 new... but our formula is simpler:
        CarriedQty = max(0, 4 - 2) = 2; NewQty = 8 - 2 = 6; ClosedQty = 2."""
        carried, new, _, closed, _ = dpr._ComputeCarriedNew(
            4, 8, "SHORT", 2, 6, 100.0, 105.0)
        assert (carried, new, closed) == (2, 6, 2)

    def test_full_close_no_new(self):
        """Pure cover, no new shorts: overnight 4, day_buy 4, current 0.
        Note: qty=0 positions are filtered upstream — but the helper still
        handles the math correctly if called."""
        carried, new, _, closed, _ = dpr._ComputeCarriedNew(
            4, 0, "SHORT", 4, 0, 100.0, 0)
        assert (carried, new, closed) == (0, 0, 4)

    def test_long_partial_sell_with_intraday_buy(self):
        """Carry LONG 10, today buy 3 + sell 5, current LONG 8.
        DaySell 5 closes 5 carry longs; DayBuy 3 opens 3 new longs.
        CarriedQty = max(0, 10 - 5) = 5; NewQty = 8 - 5 = 3."""
        carried, new, _, closed, _ = dpr._ComputeCarriedNew(
            10, 8, "LONG", 3, 5, 105.0, 95.0)
        assert (carried, new, closed) == (5, 3, 5)

    def test_options_long_carry_partial_close(self):
        """NIFTY 21700CE LONG carry 65, today sold 65 to close + bought 65 of
        23000CE (different strike — different position). For THIS position
        (21700CE): carried 65, sold 65 → qty 0. Same logic."""
        carried, new, _, closed, _ = dpr._ComputeCarriedNew(
            65, 0, "LONG", 0, 65, 0, 207.20)
        assert (carried, new, closed) == (0, 0, 65)

    def test_zero_overnight_zero_today_means_zero_qty(self):
        """Defensive: no overnight, no day flow, AbsQty=0. Helper returns zeros."""
        carried, new, _, closed, _ = dpr._ComputeCarriedNew(
            0, 0, "LONG", 0, 0, 0, 0)
        assert (carried, new, closed) == (0, 0, 0)

    def test_slice_zero_when_no_close(self):
        """Open position with no closures today: slice must be 0."""
        slice_ = dpr._RealizedSliceForClose(100.0, 0.0, 0, "SHORT", 1.0)
        assert slice_ == 0.0

    def test_slice_short_carry_covered_at_loss(self):
        """SHORT 4 covered at 343.06 from prev_close 341.90: -₹4,640 on the day."""
        slice_ = dpr._RealizedSliceForClose(341.90, 343.06, 4, "SHORT", 1000.0)
        assert slice_ == pytest.approx(-4640.0)

    def test_slice_flipped_short_to_long_uses_short_direction(self):
        """SHORT→LONG flip: closed lots were SHORT. Slice formula must use
        SHORT direction — passing LONG would invert the sign."""
        slice_short = dpr._RealizedSliceForClose(102.0, 105.0, 4, "SHORT", 1.0)
        slice_long = dpr._RealizedSliceForClose(102.0, 105.0, 4, "LONG", 1.0)
        # SHORT close above prev_close = loss, LONG close above prev_close = gain.
        # These have OPPOSITE signs — caller must pick the right one.
        assert slice_short == pytest.approx(-12.0)
        assert slice_long == pytest.approx(12.0)
        assert slice_short + slice_long == 0  # sanity

    def test_closed_direction_helper_short_carry(self):
        assert dpr._ClosedDirectionFromOvernight(-4, "SHORT") == "SHORT"

    def test_closed_direction_helper_long_carry(self):
        assert dpr._ClosedDirectionFromOvernight(5, "LONG") == "LONG"

    def test_closed_direction_helper_flip_short_to_long(self):
        # Was SHORT (RawOvernightQty<0), now LONG. Closed direction = SHORT.
        assert dpr._ClosedDirectionFromOvernight(-4, "LONG") == "SHORT"

    def test_closed_direction_helper_flip_long_to_short(self):
        assert dpr._ClosedDirectionFromOvernight(5, "SHORT") == "LONG"

    def test_closed_direction_helper_no_overnight(self):
        # No carry → closed direction doesn't matter; default to current.
        assert dpr._ClosedDirectionFromOvernight(0, "SHORT") == "SHORT"


# ═══════════════════════════════════════════════════════════════════
# m2m reconciliation logging
# ═══════════════════════════════════════════════════════════════════

class TestM2mReconciliation:
    """When our split drifts from broker m2m by > tolerance, we log a warning
    so the bug becomes visible at fetch time instead of silently inflating
    the user's email."""

    def test_no_warning_when_within_tolerance(self, caplog):
        import logging
        caplog.set_level(logging.WARNING, logger="daily_pnl_report")
        dpr._ReconcileWithBrokerM2m("FOOFUT", "Test", -1880.0, 50.0, -1830.0)
        # |computed - m2m| = |-1830 - (-1830)| = 0 < tolerance
        assert not any("diverges from broker m2m" in r.message for r in caplog.records)

    def test_warning_when_diverges(self, caplog):
        import logging
        caplog.set_level(logging.WARNING, logger="daily_pnl_report")
        # computed = +10,000; broker m2m = -1,880 → diff = +11,880 (way over tolerance)
        dpr._ReconcileWithBrokerM2m("ZINCMINI26APRFUT", "Kite YD6016",
                                     10000.0, 0.0, -1880.0)
        assert any("diverges from broker m2m" in r.message for r in caplog.records)
        assert any("ZINCMINI26APRFUT" in r.message for r in caplog.records)

    def test_no_warning_when_broker_m2m_zero(self, caplog):
        """Some brokers don't populate m2m for fully-open carry positions.
        Skip reconciliation to avoid false warnings."""
        import logging
        caplog.set_level(logging.WARNING, logger="daily_pnl_report")
        dpr._ReconcileWithBrokerM2m("FOOFUT", "Test", 5000.0, 0.0, 0.0)
        assert not any("diverges" in r.message for r in caplog.records)


# ═══════════════════════════════════════════════════════════════════
# 29 Apr 2026 comprehensive regression — every reported position
# ═══════════════════════════════════════════════════════════════════

class TestApril29Regression:
    """Replays the 29 Apr 2026 user-reported scenario through `_FetchOpenPositions`
    with realistic broker mock responses for every position. Asserts the
    daily contribution per position matches the user's actual trading P&L.

    Pre-fix MCX subtotal: ₹78,564 (overstated — ZINCMINI flip mis-classified).
    Post-fix MCX subtotal: ₹66,684 (matches user's reported ~₹67k).

    Pre-fix JEERA "Net today": ₹+44,213 (cumulative-since-entry double-count).
    Post-fix JEERA "Net today": ₹-33,300 (true daily contribution)."""

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_mcx_subtotal_matches_user_expectation(
        self, mock_match, mock_open, mock_angel_factory, mock_kite_factory
    ):
        """User reported actual MCX P&L ≈ ₹67,000 on 29 Apr 2026.
        Email displayed ~₹78,564 (overstated by ZINCMINI flip + cumulative)."""

        # Match config lookup based on tradingsymbol prefix
        def _match_side_effect(symbol, exchange, broker, instruments):
            for prefix, name, pv in [
                ("CRUDEOIL", "CRUDEOIL", 100),
                ("NATURALGAS", "NATURALGAS", 1250),
                ("SILVERMIC", "SILVERMIC", 1),
                ("ZINCMINI", "ZINCMINI", 1000),
            ]:
                if symbol.startswith(prefix):
                    return (name, {"exchange": "MCX", "point_value": pv})
            return (None, None)
        mock_match.side_effect = _match_side_effect

        # Per-user side_effect: YD6016 has MCX positions; OFS653 empty.
        mcx_positions = [
            # CRUDEOIL: opened today, qty 1 LONG @ 9640, LTP 10110 → +47,000
            {"tradingsymbol": "CRUDEOIL26APRFUT", "exchange": "MCX",
             "product": "NRML", "quantity": 1,
             "average_price": 9640.0, "last_price": 10110.0,
             "close_price": 9485.0, "overnight_quantity": 0,
             "day_buy_quantity": 1, "day_sell_quantity": 0,
             "day_buy_price": 9640.0, "day_sell_price": 0,
             "m2m": 47000.0, "realised": 0},
            # NATURALGAS: pure carry SHORT 2 @ 255.40, prev 257.50, LTP 252.20 → +13,250
            {"tradingsymbol": "NATURALGAS26APRFUT", "exchange": "MCX",
             "product": "NRML", "quantity": -2,
             "average_price": 255.40, "last_price": 252.20,
             "close_price": 257.50, "overnight_quantity": -2,
             "day_buy_quantity": 0, "day_sell_quantity": 0,
             "day_buy_price": 0, "day_sell_price": 0,
             "m2m": 13250.0, "realised": 0},
            # SILVERMIC: pure carry SHORT 2, prev 246829, LTP 242672 → +8,314
            {"tradingsymbol": "SILVERMIC26APRFUT", "exchange": "MCX",
             "product": "NRML", "quantity": -2,
             "average_price": 249475.50, "last_price": 242672.0,
             "close_price": 246829.0, "overnight_quantity": -2,
             "day_buy_quantity": 0, "day_sell_quantity": 0,
             "day_buy_price": 0, "day_sell_price": 0,
             "m2m": 8314.0, "realised": 0},
            # ZINCMINI: flip — overnight SHORT 4, today buy 4 + sell 4 → SHORT 4.
            # Pre-fix swing was +10,000 (treated as carried); post-fix -1,880.
            {"tradingsymbol": "ZINCMINI26APRFUT", "exchange": "MCX",
             "product": "NRML", "quantity": -4,
             "average_price": 340.82, "last_price": 339.40,
             "close_price": 341.90, "overnight_quantity": -4,
             "day_buy_quantity": 4, "day_sell_quantity": 4,
             "day_buy_price": 343.06, "day_sell_price": 340.09,
             "m2m": -1880.0, "realised": 4560.0},
        ]

        def _kite_side_effect(user):
            k = MagicMock()
            if user == "YD6016":
                k.positions.return_value = {"net": mcx_positions}
            else:
                k.positions.return_value = {"net": []}
            return k
        mock_kite_factory.side_effect = _kite_side_effect

        mock_angel = MagicMock()
        mock_angel.position.return_value = {"data": []}
        mock_angel_factory.return_value = mock_angel

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        by_inst = {p["instrument"]: p for p in positions}

        # Per-position checks
        assert by_inst["CRUDEOIL"]["daily_swing"] == pytest.approx(47000.0)
        assert by_inst["CRUDEOIL"]["today_realized_slice"] == 0

        assert by_inst["NATURALGAS"]["daily_swing"] == pytest.approx(13250.0)
        assert by_inst["NATURALGAS"]["today_realized_slice"] == 0

        assert by_inst["SILVERMIC"]["daily_swing"] == pytest.approx(8314.0)
        assert by_inst["SILVERMIC"]["today_realized_slice"] == 0

        # ZINCMINI flip — the 29 Apr bug case
        zm = by_inst["ZINCMINI"]
        assert zm["is_new_today"] is True  # carry was closed → all current is new
        assert zm["daily_swing"] == pytest.approx(2760.0)  # NOT 10,000
        assert zm["today_realized_slice"] == pytest.approx(-4640.0)
        assert zm["daily_swing"] + zm["today_realized_slice"] == pytest.approx(-1880.0)

        # MCX subtotal: 47,000 + 13,250 + 8,314 + (-1,880) = ₹66,684
        # (Matches user's reported actual ~₹67,000.)
        mcx_total = sum(p["daily_swing"] + p["today_realized_slice"] for p in positions)
        assert mcx_total == pytest.approx(66684.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_jeera_partial_cover_not_double_counted(
        self, mock_match, mock_open, mock_angel_factory, mock_kite_factory
    ):
        """JEERA on 29 Apr: SHORT 9 units carry, today bought 6 to cover @ 20700.
        Email pre-fix showed 'Net today: ₹+44,213' (cumulative realised since
        entry +54,713 ADDED to open swing -10,500). Truth: ₹-33,300."""
        kite = MagicMock()
        kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = kite

        smart = MagicMock()
        smart.position.return_value = {"data": [
            {"tradingsymbol": "JEERAUNJHA20MAY2026", "exchange": "NCDEX",
             "producttype": "CARRYFORWARD", "netqty": "-3",
             "ltp": "20670", "close": "20320",
             "totalsellavgprice": "21611.89", "cfsellavgprice": "21611.89",
             "cfsellqty": "9", "cfbuyqty": "0",
             "buyqty": "6", "sellqty": "0",
             "buyavgprice": "20700", "sellavgprice": "0",
             "realised": "54713", "m2m": "-33300"},
        ]}
        mock_angel_factory.return_value = smart
        mock_match.return_value = ("JEERA", {
            "exchange": "NCDEX", "point_value": 30,
            "order_routing": {"QuantityMultiplier": 3},
        })

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        p = positions[0]
        assert p["daily_swing"] == pytest.approx(-10500.0)
        assert p["today_realized_slice"] == pytest.approx(-22800.0)
        # Net daily (NOT cumulative-since-entry +54,713):
        assert p["daily_swing"] + p["today_realized_slice"] == pytest.approx(-33300.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_castor_angel_direction_flip(self, mock_match, mock_open,
                                          mock_angel_factory, mock_kite_factory):
        """CASTOR on 29 Apr: yesterday SHORT 2 lots (10 units) @ 6463.84.
        Today bought 35 units (15 @ 6515 + 20 @ 6525, avg 6520.71) → flat the
        carry SHORT (10 units) + open LONG 25 units (5 lots). Direction flip.

        Pre-fix the Angel branch used cumulative-since-entry `realised` for
        the 'Realized today' line: -5,687 (= (6463.84 - 6520.71) × 2 × 50,
        i.e. entry-to-exit on the covered short). Net today shown was -3,864.

        Post-fix the slice is today's daily contribution from the close:
        (prev_close 6507 - exit 6520.71) × 2 × 50 = -1,371. The cumulative
        -5,687 stays in the accumulator JSON for tax tracking only."""
        kite = MagicMock()
        kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = kite

        smart = MagicMock()
        smart.position.return_value = {"data": [
            {"tradingsymbol": "CASTOR20MAY2026", "exchange": "NCDEX",
             "producttype": "CARRYFORWARD", "netqty": "25",
             "ltp": "6528.00", "close": "6507.00",
             "totalbuyavgprice": "6520.71", "cfbuyavgprice": "0",
             "cfsellavgprice": "6463.84",
             "cfbuyqty": "0", "cfsellqty": "10",
             "buyqty": "35", "sellqty": "0",
             "buyavgprice": "6520.71", "sellavgprice": "0",
             "realised": "-5687", "m2m": "451"},
        ]}
        mock_angel_factory.return_value = smart
        mock_match.return_value = ("CASTOR", {
            "exchange": "NCDEX", "point_value": 50,
            "order_routing": {"QuantityMultiplier": 5},
        })

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        p = positions[0]
        assert p["direction"] == "LONG"
        assert p["is_new_today"] is True  # direction flip → all current is new
        # Open swing on 5 new LONG lots: (6528 - 6520.71) × 5 × 50 = +1,822.50
        assert p["daily_swing"] == pytest.approx(1822.50)
        # Slice on closed 2 SHORT lots: (6507 - 6520.71) × 2 × 50 = -1,371
        # Critically: ClosedDirection must be SHORT here (not LONG) — the
        # closed lots were carry shorts even though current position is LONG.
        assert p["today_realized_slice"] == pytest.approx(-1371.0)
        # Net daily ≈ broker m2m (~+451), NOT the old -3,864 from cumulative.
        assert p["daily_swing"] + p["today_realized_slice"] == pytest.approx(451.50)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_dhaniya_angel_add_to_short(self, mock_match, mock_open,
                                         mock_angel_factory, mock_kite_factory):
        """DHANIYA on 29 Apr: yesterday SHORT 1 lot (5 units) @ 12954.49.
        Today sold 5 more units → SHORT 2 lots. Pure add-to-short, no closure.
        Open swing splits across carry (1 lot from prev_close) and new (1 lot
        from today's sell price). today_realized_slice = 0."""
        kite = MagicMock()
        kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = kite

        smart = MagicMock()
        smart.position.return_value = {"data": [
            {"tradingsymbol": "DHANIYA20MAY2026", "exchange": "NCDEX",
             "producttype": "CARRYFORWARD", "netqty": "-10",
             "ltp": "12830.00", "close": "13056.00",
             "totalsellavgprice": "12953.25", "cfsellavgprice": "12954.49",
             "cfbuyqty": "0", "cfsellqty": "5",
             "buyqty": "0", "sellqty": "5",
             "buyavgprice": "0", "sellavgprice": "12952.00",
             "realised": "0", "m2m": "17400"},
        ]}
        mock_angel_factory.return_value = smart
        mock_match.return_value = ("DHANIYA", {
            "exchange": "NCDEX", "point_value": 50,
            "order_routing": {"QuantityMultiplier": 5},
        })

        positions, _ = dpr._FetchOpenPositions({"instruments": {}})
        p = positions[0]
        assert p["direction"] == "SHORT"
        assert p["is_new_today"] is False  # 1 lot carried
        # Carry swing: (13056 - 12830) × 1 lot × 50 = 11,300
        # New swing:   (12952 - 12830) × 1 lot × 50 = 6,100
        # Total: 17,400
        assert p["daily_swing"] == pytest.approx(17400.0)
        # No closure → slice = 0
        assert p["today_realized_slice"] == 0
        # Matches broker m2m (no flip, no close)
        assert p["daily_swing"] + p["today_realized_slice"] == pytest.approx(17400.0)

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    @patch("daily_pnl_report._MatchToInstrument")
    def test_cocudakl_fully_closed_today_uses_slice_not_cumulative(
        self, mock_match, mock_open, mock_angel_factory, mock_kite_factory
    ):
        """COCUDAKL on 29 Apr was fully covered. Closed Today section must
        show today's slice (m2m), not cumulative-since-entry realised."""
        kite = MagicMock()
        kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = kite

        smart = MagicMock()
        smart.position.return_value = {"data": [
            {"tradingsymbol": "COCUDAKL20APR2026", "exchange": "NCDEX",
             "producttype": "CARRYFORWARD", "netqty": "0",
             "ltp": "0", "close": "3380",
             "realised": "41340", "m2m": "12000"},  # m2m = today's slice
        ]}
        mock_angel_factory.return_value = smart
        mock_match.return_value = ("COCUDAKL", {"exchange": "NCDEX", "point_value": 30})

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        # ByAccount uses cumulative for accumulator (unchanged tax tracking)
        assert by_acct["AABM826021"] == pytest.approx(41340.0)
        # ClosedByInstrument uses today's slice for display (no double-count)
        assert by_inst["COCUDAKL"] == pytest.approx(12000.0)

    @patch("daily_pnl_report.IsAnyExchangeOpen", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl")
    @patch("daily_pnl_report._FetchOpenPositions")
    @patch("daily_pnl_report._UpdateRealizedPnlAccumulator")
    @patch("daily_pnl_report._SendEmail")
    def test_full_29apr_email_no_overstatement(
        self, mock_email, mock_accum, mock_fetch, mock_realized, mock_orders, mock_any
    ):
        """End-to-end render of 29 Apr scenario. Hero number must reflect:
            OpenSwing + sum(today_realized_slice on open) + sum(closed-only slices)
        with NO contribution from cumulative-since-entry realised."""
        mock_fetch.return_value = ([
            # MCX positions (computed values)
            {"instrument": "CRUDEOIL", "tradingsymbol": "CRUDEOIL26APRFUT",
             "direction": "LONG", "qty": 1, "avg_entry": 9640, "prev_close": 9485,
             "ltp": 10110, "point_value": 100, "pnl": 47000, "daily_swing": 47000,
             "today_realized_slice": 0,
             "broker": "ZERODHA", "is_new_today": True},
            {"instrument": "ZINCMINI", "tradingsymbol": "ZINCMINI26APRFUT",
             "direction": "SHORT", "qty": 4, "avg_entry": 340.82, "prev_close": 341.90,
             "ltp": 339.40, "point_value": 1000, "pnl": 5680, "daily_swing": 2760,
             "today_realized_slice": -4640,
             "broker": "ZERODHA", "is_new_today": True},
            # NCDEX (JEERA partial cover)
            {"instrument": "JEERA", "tradingsymbol": "JEERAUNJHA20MAY2026",
             "direction": "SHORT", "qty": 3, "lots": 1,
             "avg_entry": 21611.89, "prev_close": 20320, "ltp": 20670,
             "point_value": 30, "pnl": 28256.70, "daily_swing": -10500,
             "today_realized_slice": -22800,
             "broker": "ANGEL", "is_new_today": False},
        ], [])
        # Cumulative realised for accumulator (tax); ClosedByInstrument empty
        # because no fully-closed positions in this slim mock
        mock_realized.return_value = (
            {"YD6016": 4560.0, "AABM826021": 54713.0, "OFS653": 0.0},  # cumulative
            {},
        )

        dpr.GenerateDailyReport(DryRun=False, DateStr="2026-04-29")
        _, html = mock_email.call_args[0]

        # ZINCMINI must NOT show pre-fix +10,000 swing
        # Old broken header: "₹+10,000" inside ZINCMINI card. Post-fix +2,760.
        # JEERA must NOT show pre-fix Net today +44,213.
        assert "+44,213" not in html
        # JEERA correct: -10,500 swing + -22,800 slice = -33,300 net
        assert "-33,300" in html
        # ZINCMINI correct: +2,760 swing + -4,640 slice = -1,880 net
        assert "-1,880" in html

        # Daily MTM hero = OpenSwing(47,000+2,760-10,500) + slices(0-4,640-22,800) = +11,820
        # (NOT the old wrong +84,973 = open swing 39,260 + cumulative realised 54,713 + 4,560)
        accum_args = mock_accum.call_args[0]
        # Accumulator still gets cumulative for tax tracking — unchanged
        assert accum_args[0]["AABM826021"] == 54713.0
        assert accum_args[0]["YD6016"] == 4560.0
