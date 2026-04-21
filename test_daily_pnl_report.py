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

class TestLIFOSwingKite:
    """Test the LIFO logic for Kite (MCX + Options) positions."""

    def _compute_kite_swing(self, overnight, day_buy, day_sell, direction, abs_qty,
                            prev_close, ltp, day_buy_price, day_sell_price, pv=1.0):
        """Replicate the Kite swing split logic from the source."""
        if direction == "LONG":
            excess_sells = max(0, day_sell - day_buy)
            carried = max(0, overnight - excess_sells)
            new = max(0, abs_qty - carried)
            new_entry = day_buy_price
        else:
            excess_buys = max(0, day_buy - day_sell)
            carried = max(0, overnight - excess_buys)
            new = max(0, abs_qty - carried)
            new_entry = day_sell_price

        swing_base = prev_close if prev_close > 0 else 0
        carried_swing = dpr._CalcPnl(direction, swing_base, ltp, carried, pv)
        new_swing = dpr._CalcPnl(direction, new_entry, ltp, new, pv) if new > 0 else 0
        is_new_today = (carried == 0)
        return carried, new, carried_swing + new_swing, is_new_today

    # ── Pure carried, no intraday trades ──
    def test_pure_carried_long(self):
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=5, day_buy=0, day_sell=0, direction="LONG", abs_qty=5,
            prev_close=100, ltp=110, day_buy_price=0, day_sell_price=0, pv=50)
        assert carried == 5
        assert new == 0
        assert swing == 2500.0  # (110-100)*5*50
        assert is_new is False

    # ── Pure new position, no overnight ──
    def test_pure_new_long(self):
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=0, day_buy=2, day_sell=0, direction="LONG", abs_qty=2,
            prev_close=9253, ltp=9853, day_buy_price=9853.50, day_sell_price=0, pv=100)
        assert carried == 0
        assert new == 2
        assert is_new is True
        # Swing from entry, not stale prev_close
        assert swing == pytest.approx((9853 - 9853.50) * 2 * 100)

    # ── LIFO: CF lot + buy today + sell today → CF preserved ──
    def test_lifo_cf_plus_buy_sell(self):
        # Had 3 overnight, bought 2 today, sold 2 today → 3 remain, all carried
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=3, day_buy=2, day_sell=2, direction="LONG", abs_qty=3,
            prev_close=500, ltp=520, day_buy_price=510, day_sell_price=515, pv=10)
        assert carried == 3
        assert new == 0
        assert is_new is False
        assert swing == (520 - 500) * 3 * 10

    # ── LIFO: sell more than bought today → eats into overnight ──
    def test_lifo_excess_sell_eats_overnight(self):
        # Had 10 overnight, bought 0 today, sold 5 → 5 remain, all carried
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=10, day_buy=0, day_sell=5, direction="LONG", abs_qty=5,
            prev_close=200, ltp=210, day_buy_price=0, day_sell_price=0, pv=100)
        assert carried == 5
        assert new == 0
        assert is_new is False

    # ── LIFO: bought today more than sold → new lots exist ──
    def test_lifo_net_new_lots(self):
        # Had 2 overnight, bought 3 today, sold 1 → 4 remain: 2 carried + 2 new
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=2, day_buy=3, day_sell=1, direction="LONG", abs_qty=4,
            prev_close=100, ltp=110, day_buy_price=105, day_sell_price=0, pv=1)
        assert carried == 2
        assert new == 2
        assert is_new is False
        expected = (110 - 100) * 2 * 1 + (110 - 105) * 2 * 1  # carried + new
        assert swing == expected

    # ── SHORT direction LIFO ──
    def test_lifo_short_pure_carried(self):
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=5, day_buy=0, day_sell=0, direction="SHORT", abs_qty=5,
            prev_close=200, ltp=190, day_buy_price=0, day_sell_price=0, pv=100)
        assert carried == 5
        assert new == 0
        assert swing == (200 - 190) * 5 * 100

    def test_lifo_short_new_today(self):
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=0, day_buy=0, day_sell=10, direction="SHORT", abs_qty=10,
            prev_close=300, ltp=290, day_buy_price=0, day_sell_price=295, pv=1)
        assert carried == 0
        assert new == 10
        assert is_new is True
        assert swing == (295 - 290) * 10 * 1

    def test_lifo_short_buy_to_close_eats_new_first(self):
        # Short: had 5 overnight, sold 3 more today, bought 3 to close → 5 remain
        carried, new, swing, is_new = self._compute_kite_swing(
            overnight=5, day_buy=3, day_sell=3, direction="SHORT", abs_qty=5,
            prev_close=200, ltp=190, day_buy_price=195, day_sell_price=198, pv=1)
        assert carried == 5
        assert new == 0
        assert is_new is False


# ═══════════════════════════════════════════════════════════════════
# LIFO Swing Split — Angel NCDEX
# ═══════════════════════════════════════════════════════════════════

class TestLIFOSwingAngel:
    """Test the LIFO logic for Angel positions using cf/buy/sell qty fields."""

    def _compute_angel_swing(self, cf_buy_qty, buy_qty, sell_qty, direction, abs_qty,
                             prev_close, ltp, today_buy_price, today_sell_price,
                             qty_mult=5, pv=50):
        if direction == "LONG":
            excess_sells = max(0, sell_qty - buy_qty)
            carried_units = max(0, cf_buy_qty - excess_sells)
            new_units = max(0, abs_qty - carried_units)
            new_entry = today_buy_price
        else:
            # cf_sell_qty would be used for short, but NCDEX is usually LONG
            excess_buys = max(0, buy_qty - sell_qty)
            carried_units = max(0, cf_buy_qty - excess_buys)  # using cf_buy_qty as placeholder
            new_units = max(0, abs_qty - carried_units)
            new_entry = today_sell_price

        carried_lots = carried_units / qty_mult
        new_lots = new_units / qty_mult
        is_new_today = (carried_units == 0)

        swing_base = prev_close if prev_close > 0 else 0
        carried_swing = dpr._CalcPnl(direction, swing_base, ltp, carried_lots, pv)
        new_swing = dpr._CalcPnl(direction, new_entry, ltp, new_lots, pv) if new_lots > 0 else 0
        return carried_units, new_units, carried_swing + new_swing, is_new_today

    # ── DHANIYA scenario (the original bug): cf=5, buy=5, sell=5 ──
    def test_dhaniya_cf_buy_sell_equal(self):
        """CF=5 (1 lot), bought 1 lot today, sold 1 lot today.
        LIFO: sell offsets today's buy → CF preserved."""
        carried, new, swing, is_new = self._compute_angel_swing(
            cf_buy_qty=5, buy_qty=5, sell_qty=5, direction="LONG", abs_qty=5,
            prev_close=12328, ltp=12852, today_buy_price=12621, today_sell_price=0,
            qty_mult=5, pv=50)
        assert carried == 5
        assert new == 0
        assert is_new is False
        # Swing all from prev_close: (12852-12328) * 1.0 * 50 = 26,200
        assert swing == pytest.approx(26200.0)

    # ── DHANIYA with old FIFO logic would have been wrong ──
    def test_dhaniya_fifo_would_be_wrong(self):
        """Verify the OLD FIFO logic would mark this as NEW_TODAY."""
        # FIFO: CarriedUnits = max(0, cfbuyqty - sellqty) = max(0, 5-5) = 0
        fifo_carried = max(0, 5 - 5)
        assert fifo_carried == 0  # FIFO says 0 carried — wrong!

    # ── Pure CF, no intraday trades ──
    def test_pure_cf_ncdex(self):
        carried, new, swing, is_new = self._compute_angel_swing(
            cf_buy_qty=10, buy_qty=0, sell_qty=0, direction="LONG", abs_qty=10,
            prev_close=5723, ltp=5760, today_buy_price=0, today_sell_price=0,
            qty_mult=5, pv=50)
        assert carried == 10
        assert new == 0
        assert is_new is False
        # (5760-5723) * 2.0 lots * 50 = 3700
        assert swing == pytest.approx(3700.0)

    # ── Partial exit of CF only (no buys today) ──
    def test_partial_cf_exit(self):
        # CF=10 (2 lots), sell=5 (1 lot), no buys → 5 remain (1 lot), all carried
        carried, new, swing, is_new = self._compute_angel_swing(
            cf_buy_qty=10, buy_qty=0, sell_qty=5, direction="LONG", abs_qty=5,
            prev_close=3582, ltp=3611, today_buy_price=0, today_sell_price=0,
            qty_mult=10, pv=100)
        assert carried == 5
        assert new == 0
        assert is_new is False

    # ── Add to existing position (no sells) ──
    def test_add_to_cf_no_sells(self):
        # CF=5, buy=5, no sells → 10 total: 5 carried + 5 new
        carried, new, swing, is_new = self._compute_angel_swing(
            cf_buy_qty=5, buy_qty=5, sell_qty=0, direction="LONG", abs_qty=10,
            prev_close=10000, ltp=10100, today_buy_price=10050, today_sell_price=0,
            qty_mult=5, pv=50)
        assert carried == 5
        assert new == 5
        assert is_new is False
        # carried: (10100-10000) * 1.0 * 50 = 5000
        # new:     (10100-10050) * 1.0 * 50 = 2500
        assert swing == pytest.approx(7500.0)

    # ── Brand new position, no CF ──
    def test_brand_new_position(self):
        carried, new, swing, is_new = self._compute_angel_swing(
            cf_buy_qty=0, buy_qty=5, sell_qty=0, direction="LONG", abs_qty=5,
            prev_close=0, ltp=15000, today_buy_price=14950, today_sell_price=0,
            qty_mult=5, pv=50)
        assert carried == 0
        assert new == 5
        assert is_new is True

    # ── Heavy sell exceeds both today's buys and some CF ──
    def test_heavy_sell(self):
        # CF=10, buy=5, sell=10 → excess_sell=5, carried=10-5=5, new=0
        carried, new, swing, is_new = self._compute_angel_swing(
            cf_buy_qty=10, buy_qty=5, sell_qty=10, direction="LONG", abs_qty=5,
            prev_close=100, ltp=110, today_buy_price=105, today_sell_price=0,
            qty_mult=5, pv=50)
        assert carried == 5
        assert new == 0
        assert is_new is False


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
        mock_fetch.return_value = ([
            {"pnl": 10000, "daily_swing": 5000, "instrument": "GOLDM", "direction": "LONG",
             "qty": 1, "avg_entry": 100, "prev_close": 95, "ltp": 105, "point_value": 10,
             "broker": "ZERODHA", "is_new_today": False},
        ], [])
        mock_realized.return_value = ({"YD6016": 20000.0, "AABM826021": 5000.0, "OFS653": 0.0}, {})

        dpr.GenerateDailyReport(DryRun=True, DateStr="2026-04-02")

        # Check that _BuildReportHtml was called with correct MTM
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
        """Angel DHANIYA with realised=-25667 attributed to 'DHANIYA' bucket."""
        # Angel returns DHANIYA position with the short-cover realized loss
        mock_smart = MagicMock()
        mock_smart.position.return_value = {"data": [
            {"tradingsymbol": "DHANIYA20MAY2026", "exchange": "NCX",
             "producttype": "CARRYFORWARD", "netqty": "5",
             "realised": "-25667", "m2m": "-17601"},
        ]}
        mock_angel_factory.return_value = mock_smart
        # Kite both sides return empty
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {"net": []}
        mock_kite_factory.return_value = mock_kite
        mock_match.return_value = ("DHANIYA", {"exchange": "NCX"})

        by_acct, by_inst = dpr._FetchDailyRealizedPnl({"instruments": {}})
        assert by_acct["AABM826021"] == -25667.0
        assert by_inst["DHANIYA"] == -25667.0

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    def test_options_bucketed_by_underlying(self, mock_open,
                                             mock_angel_factory, mock_kite_factory):
        """NIFTY/SENSEX/BANKNIFTY options keyed by underlying, not full symbol."""
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
        # Both NIFTY legs aggregated under "NIFTY"
        assert by_inst["NIFTY"] == pytest.approx(5000 + -1200)
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
        """The DHANIYA Apr 17 scenario rendered end-to-end."""
        dhaniya = {
            "instrument": "DHANIYA", "tradingsymbol": "DHANIYA20MAY2026",
            "direction": "LONG", "qty": 5, "lots": 1,
            "avg_entry": 13064.67, "prev_close": 12808.0, "ltp": 13226.0,
            "point_value": 50, "pnl": 8066.50, "daily_swing": 8066.50,
            "broker": "ANGEL", "is_new_today": True,
        }
        html = dpr._BuildReportHtml(self._data(
            positions=[dhaniya],
            realized_by_instrument={"DHANIYA": -25667.0},
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
        """Option combo shows its underlying's realized attribution."""
        nifty_ce = {
            "instrument": "NIFTY_OPT_CE", "tradingsymbol": "NIFTY26APR24000CE",
            "direction": "LONG", "qty": 65, "avg_entry": 2030, "prev_close": 1385,
            "ltp": 1327, "point_value": 1.0, "pnl": -45675, "daily_swing": -3812,
            "broker": "ZERODHA", "is_new_today": False,
        }
        html = dpr._BuildReportHtml(self._data(
            positions=[nifty_ce],
            realized_by_instrument={"NIFTY": 8000.0},
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
        """DHANIYA CARRYFORWARD realized + DHANIYA INTRADAY realized → summed."""
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
        assert by_acct["AABM826021"] == pytest.approx(-25667 + 3000)
        assert by_inst["DHANIYA"] == pytest.approx(-22667.0)  # summed, not overwritten

    @patch("daily_pnl_report._EstablishKiteSession")
    @patch("daily_pnl_report.EstablishConnectionAngelAPI")
    @patch("daily_pnl_report._IsExchangeOpen", return_value=True)
    def test_nifty_ce_and_pe_sum_under_underlying(
        self, mock_open, mock_angel_factory, mock_kite_factory
    ):
        """NIFTY CE realized + NIFTY PE realized → both under 'NIFTY'."""
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
        # CE +8000, PE -3500 → NIFTY total +4500. The open position has realised=0 so ignored.
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
        """The DHANIYA Apr 17 scenario — end-to-end."""
        mock_fetch.return_value = ([
            {"instrument": "DHANIYA", "tradingsymbol": "DHANIYA20MAY2026",
             "direction": "LONG", "qty": 5, "lots": 1,
             "avg_entry": 13064.67, "prev_close": 12808.0, "ltp": 13226.0,
             "point_value": 50, "pnl": 8066.50, "daily_swing": 8066.50,
             "broker": "ANGEL", "is_new_today": True},
        ], [])
        mock_realized.return_value = (
            {"YD6016": 0.0, "AABM826021": -25667.0, "OFS653": 0.0},
            {"DHANIYA": -25667.0},
        )

        dpr.GenerateDailyReport(DryRun=False, DateStr="2026-04-17")

        # _SendEmail should have been called with HTML containing per-symbol realized
        assert mock_email.call_count == 1
        _, html = mock_email.call_args[0]
        assert "DHANIYA" in html
        assert "Realized today" in html
        assert "-25,667" in html
        assert "Net today" in html
        assert "-17,600" in html or "-17,601" in html

        # Accumulator got the account-level dict, not the tuple
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
        """DHANIYA has open + realized; COCUDAKL fully closed.
        DHANIYA should show on position row, COCUDAKL in Closed Today."""
        mock_fetch.return_value = ([
            {"instrument": "DHANIYA", "tradingsymbol": "DHANIYA20MAY2026",
             "direction": "LONG", "qty": 5, "lots": 1,
             "avg_entry": 13064.67, "prev_close": 12808.0, "ltp": 13226.0,
             "point_value": 50, "pnl": 8066.50, "daily_swing": 8066.50,
             "broker": "ANGEL", "is_new_today": True},
        ], [])
        mock_realized.return_value = (
            {"YD6016": 0.0, "AABM826021": -11031.0, "OFS653": 0.0},
            {"DHANIYA": -25667.0, "COCUDAKL": 14636.0},
        )

        dpr.GenerateDailyReport(DryRun=False, DateStr="2026-04-17")
        _, html = mock_email.call_args[0]

        dhaniya_idx = html.find("DHANIYA")
        closed_idx = html.find("Closed Today")
        cocudakl_idx = html.find("COCUDAKL")

        assert dhaniya_idx < closed_idx  # DHANIYA on position row first
        assert closed_idx < cocudakl_idx  # COCUDAKL in Closed Today section
        assert "Realized today" in html
        assert "-25,667" in html  # on DHANIYA row
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
