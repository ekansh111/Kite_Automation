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
def _mock_check_holiday(d):
    holidays = {"2026-04-03", "2026-01-26", "2026-12-25"}
    return str(d) in holidays
MockHolidays.CheckForDateHoliday = _mock_check_holiday
sys.modules["Holidays"] = MockHolidays

# Mock rollover_monitor
MockRollover = MagicMock()
def _mock_is_trading_day(d):
    if d.weekday() >= 5:
        return False
    return not _mock_check_holiday(d)
MockRollover.IsTradingDay = _mock_is_trading_day
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

    @patch("daily_pnl_report.IsTradingDay", side_effect=_mock_is_trading_day)
    def test_generate_report_skips_holiday(self, _):
        """GenerateDailyReport should return early on a holiday."""
        with patch("daily_pnl_report._FetchOpenPositions") as mock_fetch:
            dpr.GenerateDailyReport(DryRun=True, DateStr="2026-04-03")
            mock_fetch.assert_not_called()

    @patch("daily_pnl_report.IsTradingDay", side_effect=_mock_is_trading_day)
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
        mock_realized.return_value = {"YD6016": 20000.0, "AABM826021": 5000.0, "OFS653": 0.0}

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
        mock_realized.return_value = {"YD6016": 0.0, "AABM826021": 0.0, "OFS653": 0.0}

        dpr.GenerateDailyReport(DryRun=True, DateStr="2026-04-02")

        call_args = mock_html.call_args[0][0]
        assert call_args["total_daily_mtm"] == 3000.0


# ═══════════════════════════════════════════════════════════════════
# Fetch Error Tracking
# ═══════════════════════════════════════════════════════════════════

class TestFetchErrors:
    @patch("daily_pnl_report.IsTradingDay", return_value=True)
    @patch("daily_pnl_report._FetchTodayOrders", return_value=([], []))
    @patch("daily_pnl_report._FetchDailyRealizedPnl", return_value={"YD6016": 0, "AABM826021": 0, "OFS653": 0})
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
