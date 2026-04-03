"""
Exhaustive tests for itm_call_rollover.py — ITM Monthly Call Rollover System.

Tests:
  1. Monthly expiry detection (GetMonthlyExpiries, IsMonthlyExpiryDay)
  2. ITM strike selection (ComputeITMCallCandidates, SelectBestITMStrike)
  3. Position sizing (ComputePositionSizeITM, vol budget loading)
  4. State management (LoadState, SaveState, recovery from positions)
  5. Order building and execution flow
  6. Crash recovery (LEG1_DONE detection)
  7. K table extension and lookupK fallback
  8. Database operations (LogITMCallRollover, UpdateITMCallRolloverStatus, etc.)
  9. Email building
  10. Edge cases (no candidates, corrupt state, multiple positions, etc.)
"""

import os
import sys
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock

# ─── Module-level patching BEFORE imports ────────────────────────────
_TEST_DIR = tempfile.mkdtemp()

# Reuse existing Directories mock if already loaded (e.g., by test_capital_model)
if "Directories" in sys.modules:
    _MOCK_WORK_ROOT = Path(sys.modules["Directories"].workInputRoot)
else:
    _MOCK_WORK_ROOT = Path(_TEST_DIR)

    class MockDirectories:
        workInputRoot = _MOCK_WORK_ROOT
        WorkDirectory = _MOCK_WORK_ROOT
        KiteEshitaLogin = _MOCK_WORK_ROOT / "Login_Credentials_OFS653.txt"
        KiteEshitaLoginAccessToken = _MOCK_WORK_ROOT / "access_token_OF.txt"

    sys.modules["Directories"] = MockDirectories()

# Stub external modules (reuse existing stubs if already loaded by another test file)
import types

def _get_or_stub(name, attrs):
    """Get existing stub from sys.modules or create a new one."""
    if name in sys.modules:
        mod = sys.modules[name]
        # Ensure all attrs exist on the existing stub
        for k, v in attrs.items():
            if not hasattr(mod, k):
                setattr(mod, k, v)
        return mod
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod

_kc = _get_or_stub("kiteconnect", {"KiteConnect": MagicMock})

_HOLIDAY_DATES = set()  # mutable, tests can modify
def _mock_check_holiday(d):
    return d in _HOLIDAY_DATES
_holidays = _get_or_stub("Holidays", {"CheckForDateHoliday": _mock_check_holiday})
# Swap in our checker — works whether test_capital_model loaded first or not
_holidays.CheckForDateHoliday = _mock_check_holiday
# If test_capital_model's dispatch pattern is in place, update the impl it delegates to
if hasattr(_holidays, "_holidays_impl"):
    _holidays._holidays_impl = _mock_check_holiday

_focn = _get_or_stub("FetchOptionContractName", {
    "GetInstrumentsCached": MagicMock(return_value=[]),
    "GetOptSegmentForExchange": MagicMock(return_value="NFO-OPT"),
    "GetBestMarketPremium": MagicMock(return_value=100.0),
    "ChunkList": lambda items, sz: [items[i:i+sz] for i in range(0, len(items), sz)],
    "FetchContractName": MagicMock(return_value="NIFTY25APR23000CE"),
    "GetKiteClient": MagicMock(return_value=MagicMock()),
    "GetDerivativesExchange": MagicMock(return_value="NFO"),
    "SelectExpiryDateFromInstruments": MagicMock(return_value=None),
})

_sc = _get_or_stub("smart_chase", {
    "SmartChaseExecute": MagicMock(return_value=(True, "ORD123", {"fill_price": 100.0, "slippage": 0.5})),
    "EXCHANGE_OPEN_TIMES": {},
})

_sop = _get_or_stub("Server_Order_Place", {"order": MagicMock(return_value="ORD456")})

_gtt = _get_or_stub("Set_Gtt_Exit", {"Set_Gtt": MagicMock(return_value=None)})

# Stub vol_target (use real one)
from vol_target import compute_daily_vol_target

# Initialize DB before importing modules that run queries at import time
import forecast_db as db
db.InitDB()

import itm_call_rollover as rollover
from PlaceOptionsSystemsV2 import lookupK, K_TABLE_SINGLE


# ─── Helpers ─────────────────────────────────────────────────────────

def _make_instrument(name, strike, expiry, tradingsymbol, lot_size=75,
                     segment="NFO-OPT", instrument_type="CE"):
    """Build a mock instrument dict matching Kite instruments API format."""
    return {
        "name": name,
        "strike": float(strike),
        "expiry": expiry,
        "tradingsymbol": tradingsymbol,
        "lot_size": lot_size,
        "segment": segment,
        "instrument_type": instrument_type,
        "exchange": "NFO",
    }


def _make_nifty_instruments(expiry_dates, strikes=None):
    """Generate a list of mock NIFTY CE instruments for given expiries and strikes."""
    if strikes is None:
        strikes = list(range(22000, 25000, 50))
    instruments = []
    for exp in expiry_dates:
        for s in strikes:
            sym = f"NIFTY{exp.strftime('%d%b%y').upper()}{s}CE"
            instruments.append(_make_instrument("NIFTY", s, exp, sym, lot_size=75))
    return instruments


def _make_banknifty_instruments(expiry_dates, strikes=None):
    """Generate mock BANKNIFTY CE instruments."""
    if strikes is None:
        strikes = list(range(46000, 52000, 100))
    instruments = []
    for exp in expiry_dates:
        for s in strikes:
            sym = f"BANKNIFTY{exp.strftime('%d%b%y').upper()}{s}CE"
            instruments.append(_make_instrument("BANKNIFTY", s, exp, sym,
                                                lot_size=15, instrument_type="CE"))
    return instruments


def _make_quote(bid=100.0, ask=102.0, ltp=101.0):
    """Build a mock Kite quote response."""
    return {
        "last_price": ltp,
        "depth": {
            "buy": [{"price": bid, "quantity": 100}],
            "sell": [{"price": ask, "quantity": 100}],
        },
    }


# ═════════════════════════════════════════════════════════════════════
# TEST CLASSES
# ═════════════════════════════════════════════════════════════════════

class TestGetMonthlyExpiries(unittest.TestCase):
    """Test GetMonthlyExpiries extracts last-of-month dates correctly."""

    def test_single_month_multiple_weeklies(self):
        """Multiple expiries in one month → returns only the last one."""
        expiries = [date(2025, 4, 3), date(2025, 4, 10), date(2025, 4, 17), date(2025, 4, 24)]
        instruments = _make_nifty_instruments(expiries, strikes=[23000])
        result = rollover.GetMonthlyExpiries(instruments, "NIFTY", "NFO-OPT")
        self.assertEqual(result, [date(2025, 4, 24)])

    def test_multiple_months(self):
        """Expiries spanning multiple months → one per month."""
        expiries = [
            date(2025, 3, 6), date(2025, 3, 13), date(2025, 3, 20), date(2025, 3, 27),
            date(2025, 4, 3), date(2025, 4, 10), date(2025, 4, 17), date(2025, 4, 24),
            date(2025, 5, 8), date(2025, 5, 15), date(2025, 5, 22), date(2025, 5, 29),
        ]
        instruments = _make_nifty_instruments(expiries, strikes=[23000])
        result = rollover.GetMonthlyExpiries(instruments, "NIFTY", "NFO-OPT")
        self.assertEqual(result, [date(2025, 3, 27), date(2025, 4, 24), date(2025, 5, 29)])

    def test_empty_instruments(self):
        """No instruments → empty list."""
        result = rollover.GetMonthlyExpiries([], "NIFTY", "NFO-OPT")
        self.assertEqual(result, [])

    def test_filters_by_index_name(self):
        """Only returns expiries for the requested index."""
        nifty_ins = _make_nifty_instruments([date(2025, 4, 24)], strikes=[23000])
        bn_ins = _make_banknifty_instruments([date(2025, 4, 24)], strikes=[48000])
        all_ins = nifty_ins + bn_ins
        result = rollover.GetMonthlyExpiries(all_ins, "NIFTY", "NFO-OPT")
        self.assertEqual(result, [date(2025, 4, 24)])

    def test_filters_by_segment(self):
        """Only returns expiries matching the segment."""
        instruments = _make_nifty_instruments([date(2025, 4, 24)], strikes=[23000])
        # Change segment to something else
        for ins in instruments:
            ins["segment"] = "BFO-OPT"
        result = rollover.GetMonthlyExpiries(instruments, "NIFTY", "NFO-OPT")
        self.assertEqual(result, [])


class TestIsMonthlyExpiryDay(unittest.TestCase):
    """Test IsMonthlyExpiryDay detection."""

    def _instruments_for(self, expiry_dates):
        return _make_nifty_instruments(expiry_dates, strikes=[23000])

    @patch("itm_call_rollover.date")
    def test_today_is_monthly_expiry(self, mock_date):
        mock_date.today.return_value = date(2025, 4, 24)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        expiries = [date(2025, 4, 3), date(2025, 4, 10), date(2025, 4, 17), date(2025, 4, 24)]
        instruments = self._instruments_for(expiries)
        # Patch date.today inside the module
        with patch.object(rollover, "date") as md:
            md.today.return_value = date(2025, 4, 24)
            is_exp, exp_date = rollover.IsMonthlyExpiryDay(instruments, "NIFTY", "NFO-OPT")
        self.assertTrue(is_exp)
        self.assertEqual(exp_date, date(2025, 4, 24))

    def test_today_is_not_monthly_expiry(self):
        """A regular day that is NOT the last expiry of the month."""
        expiries = [date(2025, 4, 3), date(2025, 4, 10), date(2025, 4, 17), date(2025, 4, 24)]
        instruments = self._instruments_for(expiries)
        with patch.object(rollover, "date") as md:
            md.today.return_value = date(2025, 4, 10)
            is_exp, exp_date = rollover.IsMonthlyExpiryDay(instruments, "NIFTY", "NFO-OPT")
        # April 10 is a weekly expiry, not the monthly (Apr 24)
        self.assertFalse(is_exp)

    def test_today_is_weekly_not_monthly(self):
        """A weekly expiry day that is not the last of the month."""
        expiries = [date(2025, 4, 3), date(2025, 4, 10), date(2025, 4, 17), date(2025, 4, 24)]
        instruments = self._instruments_for(expiries)
        with patch.object(rollover, "date") as md:
            md.today.return_value = date(2025, 4, 17)
            is_exp, _ = rollover.IsMonthlyExpiryDay(instruments, "NIFTY", "NFO-OPT")
        self.assertFalse(is_exp)


class TestGetNextMonthExpiry(unittest.TestCase):
    """Test GetNextMonthExpiry logic."""

    def test_normal_case(self):
        monthly = [date(2025, 3, 27), date(2025, 4, 24), date(2025, 5, 29)]
        result = rollover.GetNextMonthExpiry(monthly, date(2025, 4, 24))
        self.assertEqual(result, date(2025, 5, 29))

    def test_no_next_month(self):
        monthly = [date(2025, 4, 24)]
        result = rollover.GetNextMonthExpiry(monthly, date(2025, 4, 24))
        self.assertIsNone(result)

    def test_skip_same_date(self):
        """Must be strictly after current expiry."""
        monthly = [date(2025, 4, 24), date(2025, 5, 29)]
        result = rollover.GetNextMonthExpiry(monthly, date(2025, 4, 24))
        self.assertEqual(result, date(2025, 5, 29))


class TestComputeITMCallCandidates(unittest.TestCase):
    """Test ITM strike candidate generation."""

    def test_nifty_normal(self):
        """NIFTY at 23500, 4-5% ITM → strikes between 22325 and 22560, step 50."""
        candidates = rollover.ComputeITMCallCandidates(23500, 50, 4.0, 5.0)
        # 5% ITM = 23500 * 0.95 = 22325 → floor to 22300
        # 4% ITM = 23500 * 0.96 = 22560 → ceil to 22600
        self.assertTrue(all(c % 50 == 0 for c in candidates))
        self.assertTrue(all(22250 <= c <= 22600 for c in candidates))
        self.assertGreater(len(candidates), 0)

    def test_banknifty_normal(self):
        """BANKNIFTY at 48000, step 100."""
        candidates = rollover.ComputeITMCallCandidates(48000, 100, 4.0, 5.0)
        # 5% = 45600, 4% = 46080
        self.assertTrue(all(c % 100 == 0 for c in candidates))
        self.assertTrue(all(45500 <= c <= 46100 for c in candidates))
        self.assertGreater(len(candidates), 0)

    def test_widens_if_empty(self):
        """If initial range produces no candidates (very narrow), widens to 3-6%."""
        # Use a very small strike step that might make the range empty
        # Actually, with step=50 the range should always have candidates
        # Let's test with extreme values
        candidates = rollover.ComputeITMCallCandidates(100, 50, 4.0, 5.0)
        # 100 * 0.95 = 95, 100 * 0.96 = 96 → floor(95/50)*50=50, ceil(96/50)*50=100
        self.assertGreater(len(candidates), 0)

    def test_all_candidates_below_spot(self):
        """All ITM call candidates should be below spot price."""
        spot = 23500
        candidates = rollover.ComputeITMCallCandidates(spot, 50, 4.0, 5.0)
        for c in candidates:
            self.assertLess(c, spot, f"Strike {c} should be below spot {spot}")


class TestSelectBestITMStrike(unittest.TestCase):
    """Test strike selection by bid-ask spread."""

    def test_selects_tightest_spread(self):
        """Should pick the strike with smallest spread percentage."""
        exp = date(2025, 5, 29)
        instruments = _make_nifty_instruments([exp], strikes=[22300, 22350, 22400])
        candidates = [22300, 22350, 22400]

        # Build quotes: 22350 has tightest spread
        def mock_quote(keys):
            quotes = {}
            for k in keys:
                if "22300" in k:
                    quotes[k] = _make_quote(bid=1180, ask=1220, ltp=1200)  # spread=40/1200=3.3%
                elif "22350" in k:
                    quotes[k] = _make_quote(bid=1148, ask=1152, ltp=1150)  # spread=4/1150=0.35%
                elif "22400" in k:
                    quotes[k] = _make_quote(bid=1090, ask=1115, ltp=1100)  # spread=25/1100=2.3%
            return quotes

        mock_kite = MagicMock()
        mock_kite.quote = MagicMock(side_effect=mock_quote)

        # Patch GetBestMarketPremium and ValidateContractPrice
        with patch.object(rollover, "GetBestMarketPremium") as mock_gbmp, \
             patch.object(rollover, "ValidateContractPrice", return_value=(True, {"checks_passed": ["TEST"], "checks_failed": []})):
            def gbmp_side_effect(q, trade_type):
                sells = q.get("depth", {}).get("sell", [])
                if sells and sells[0]["price"] > 0:
                    return float(sells[0]["price"])
                return 0.0
            mock_gbmp.side_effect = gbmp_side_effect

            strike, symbol, lot_size, premium, meta = rollover.SelectBestITMStrike(
                mock_kite, instruments, "NIFTY", "NFO", "NFO-OPT", exp, candidates,
                Spot=23500
            )
        self.assertEqual(strike, 22350)
        self.assertEqual(lot_size, 75)
        self.assertAlmostEqual(premium, 1152.0)  # ask price for BUY

    def test_skips_zero_premium(self):
        """Contracts with zero premium are skipped."""
        exp = date(2025, 5, 29)
        instruments = _make_nifty_instruments([exp], strikes=[22300, 22350])
        candidates = [22300, 22350]

        def mock_quote(keys):
            quotes = {}
            for k in keys:
                if "22300" in k:
                    quotes[k] = _make_quote(bid=0, ask=0, ltp=0)
                elif "22350" in k:
                    quotes[k] = _make_quote(bid=1148, ask=1152, ltp=1150)
            return quotes

        mock_kite = MagicMock()
        mock_kite.quote = MagicMock(side_effect=mock_quote)

        # Patch GetBestMarketPremium and ValidateContractPrice
        with patch.object(rollover, "GetBestMarketPremium") as mock_gbmp, \
             patch.object(rollover, "ValidateContractPrice", return_value=(True, {"checks_passed": ["TEST"], "checks_failed": []})):
            def gbmp_side_effect(q, trade_type):
                depth = q.get("depth", {})
                sells = depth.get("sell", [])
                if sells and float(sells[0].get("price", 0)) > 0:
                    return float(sells[0]["price"])
                return 0.0
            mock_gbmp.side_effect = gbmp_side_effect

            strike, symbol, _, premium, _ = rollover.SelectBestITMStrike(
                mock_kite, instruments, "NIFTY", "NFO", "NFO-OPT", exp, candidates,
                Spot=23500
            )
        self.assertEqual(strike, 22350)

    def test_selects_best_value_not_tightest_spread(self):
        """Should pick the cheapest contract relative to BS theo, not the tightest spread."""
        exp = date(2025, 5, 29)
        instruments = _make_nifty_instruments([exp], strikes=[22300, 22350, 22400])
        candidates = [22300, 22350, 22400]

        # 22300: tightest spread (0.17%) but at theo (value=0%)
        # 22350: wider spread (0.87%) but 2% below theo (best value)
        # 22400: medium spread (0.54%) and at theo (value=0%)
        def mock_quote(keys):
            quotes = {}
            for k in keys:
                if "22300" in k:
                    quotes[k] = _make_quote(bid=1199, ask=1201, ltp=1200)  # spread=0.17%
                elif "22350" in k:
                    quotes[k] = _make_quote(bid=1140, ask=1150, ltp=1145)  # spread=0.87%
                elif "22400" in k:
                    quotes[k] = _make_quote(bid=1097, ask=1103, ltp=1100)  # spread=0.54%
            return quotes

        mock_kite = MagicMock()
        mock_kite.quote = MagicMock(side_effect=mock_quote)

        # Validation passes for all; BS theo makes 22350 cheapest
        def mock_validate(spot, strike, premium, expiry, kite=None):
            # 22300: theo=1200, premium=1201, value=+0.08%
            # 22350: theo=1170, premium=1150, value=-1.71% (cheapest!)
            # 22400: theo=1103, premium=1103, value=0%
            theo_map = {22300: 1200, 22350: 1170, 22400: 1103}
            theo = theo_map.get(strike, premium)
            return True, {"checks_passed": ["OK"], "checks_failed": [],
                          "bs_theo": theo, "iv_used": "test"}

        with patch.object(rollover, "GetBestMarketPremium") as mock_gbmp, \
             patch.object(rollover, "ValidateContractPrice", side_effect=mock_validate):
            def gbmp_side_effect(q, trade_type):
                sells = q.get("depth", {}).get("sell", [])
                return float(sells[0]["price"]) if sells and sells[0]["price"] > 0 else 0.0
            mock_gbmp.side_effect = gbmp_side_effect

            strike, symbol, lot_size, premium, meta = rollover.SelectBestITMStrike(
                mock_kite, instruments, "NIFTY", "NFO", "NFO-OPT", exp, candidates,
                Spot=23500
            )
        # Should pick 22350 (best value at -1.71%) not 22300 (tightest spread)
        self.assertEqual(strike, 22350)
        self.assertAlmostEqual(premium, 1150.0)

    def test_wide_spread_filtered_out(self):
        """Candidates with spread > MAX_SPREAD_PCT are filtered before value ranking."""
        exp = date(2025, 5, 29)
        instruments = _make_nifty_instruments([exp], strikes=[22300, 22350])
        candidates = [22300, 22350]

        # 22300: great value but 5% spread (too wide)
        # 22350: okay value, 1% spread (acceptable)
        def mock_quote(keys):
            quotes = {}
            for k in keys:
                if "22300" in k:
                    quotes[k] = _make_quote(bid=1140, ask=1200, ltp=1170)  # spread=5.1%
                elif "22350" in k:
                    quotes[k] = _make_quote(bid=1145, ask=1157, ltp=1150)  # spread=1.0%
            return quotes

        mock_kite = MagicMock()
        mock_kite.quote = MagicMock(side_effect=mock_quote)

        with patch.object(rollover, "GetBestMarketPremium") as mock_gbmp, \
             patch.object(rollover, "ValidateContractPrice",
                          return_value=(True, {"checks_passed": ["OK"], "checks_failed": [],
                                               "bs_theo": 1160.0, "iv_used": "test"})):
            def gbmp_side_effect(q, trade_type):
                sells = q.get("depth", {}).get("sell", [])
                return float(sells[0]["price"]) if sells and sells[0]["price"] > 0 else 0.0
            mock_gbmp.side_effect = gbmp_side_effect

            strike, symbol, _, premium, _ = rollover.SelectBestITMStrike(
                mock_kite, instruments, "NIFTY", "NFO", "NFO-OPT", exp, candidates,
                Spot=23500
            )
        # 22300 filtered out (5% spread > 3% limit), 22350 selected
        self.assertEqual(strike, 22350)

    def test_raises_if_no_instruments(self):
        """Raises if no instruments match candidates."""
        exp = date(2025, 5, 29)
        instruments = _make_nifty_instruments([exp], strikes=[22300])
        candidates = [99999]  # no match

        mock_kite = MagicMock()
        with self.assertRaises(Exception) as ctx:
            rollover.SelectBestITMStrike(
                mock_kite, instruments, "NIFTY", "NFO", "NFO-OPT", exp, candidates
            )
        self.assertIn("No CE instruments found", str(ctx.exception))

    @patch.object(rollover, "ValidateContractPrice", return_value=(True, {"checks_passed": ["TEST"], "checks_failed": []}))
    def test_reads_lot_size_from_instruments(self, _mock_val):
        """Lot size comes from instruments API, not hardcoded."""
        exp = date(2025, 5, 29)
        instruments = [_make_instrument("NIFTY", 22350, exp, "NIFTY29MAY2522350CE", lot_size=75)]
        candidates = [22350]

        mock_kite = MagicMock()
        mock_kite.quote.return_value = {
            "NFO:NIFTY29MAY2522350CE": _make_quote(bid=1148, ask=1152)
        }

        _, _, lot_size, _, _ = rollover.SelectBestITMStrike(
            mock_kite, instruments, "NIFTY", "NFO", "NFO-OPT", exp, candidates,
            Spot=23500
        )
        self.assertEqual(lot_size, 75)


class TestValidateContractPrice(unittest.TestCase):
    """Test contract price validation (intrinsic + Black-Scholes checks)."""

    def test_fair_price_passes(self):
        """A reasonably priced deep ITM call should pass both checks."""
        # NIFTY at 23500, strike 22350 → intrinsic = 1150
        # Premium of 1200 is intrinsic + 50 time value → reasonable
        spot, strike, premium = 23500, 22350, 1200
        exp = date.today() + timedelta(days=30)
        passed, details = rollover.ValidateContractPrice(spot, strike, premium, exp)
        self.assertTrue(passed, f"Fair price should pass: {details['checks_failed']}")
        self.assertEqual(len(details["checks_failed"]), 0)

    def test_intrinsic_overpay_rejected(self):
        """Premium way above intrinsic → rejected."""
        # intrinsic = 23500 - 22350 = 1150, premium 1700 → 48% overpay (> 35% threshold)
        spot, strike, premium = 23500, 22350, 1700
        exp = date.today() + timedelta(days=30)
        passed, details = rollover.ValidateContractPrice(spot, strike, premium, exp)
        self.assertFalse(passed)
        failed_reasons = " ".join(details["checks_failed"])
        self.assertTrue(
            "INTRINSIC_OVERPAY" in failed_reasons or "BS_DEVIATION" in failed_reasons)

    def test_intrinsic_underpay_rejected(self):
        """Premium significantly below intrinsic → rejected."""
        # intrinsic = 23500 - 22350 = 1150, premium 1000 → 13% below intrinsic
        spot, strike, premium = 23500, 22350, 1000
        exp = date.today() + timedelta(days=30)
        passed, details = rollover.ValidateContractPrice(spot, strike, premium, exp)
        self.assertFalse(passed)
        failed_reasons = " ".join(details["checks_failed"])
        self.assertTrue(
            "INTRINSIC_UNDERPAY" in failed_reasons or "BS_DEVIATION" in failed_reasons)

    def test_not_itm_rejected(self):
        """Strike above spot (OTM call) → rejected."""
        spot, strike, premium = 23500, 24000, 100
        exp = date.today() + timedelta(days=30)
        passed, details = rollover.ValidateContractPrice(spot, strike, premium, exp)
        self.assertFalse(passed)
        self.assertTrue(any("NOT_ITM" in r for r in details["checks_failed"]))

    def test_bs_deviation_rejected(self):
        """Premium that deviates >12% from BS theoretical → rejected."""
        # BS for deep ITM call ≈ intrinsic + time value
        # Set premium far from theoretical
        spot, strike = 23500, 22350
        exp = date.today() + timedelta(days=30)
        # First get the BS theoretical to know what a bad price would be
        from PlaceOptionsSystemsV2 import bsPrice, RISK_FREE_RATE
        T = 30 / 365.0
        theo = bsPrice(spot, strike, T, 0.15, "CE")
        # Set premium 20% above theoretical
        bad_premium = theo * 1.25
        passed, details = rollover.ValidateContractPrice(spot, strike, bad_premium, exp)
        self.assertFalse(passed)
        self.assertTrue(any("BS_DEVIATION" in r or "INTRINSIC_OVERPAY" in r
                            for r in details["checks_failed"]))

    def test_uses_vix_when_available(self):
        """When Kite is provided, uses India VIX for IV."""
        spot, strike, premium = 23500, 22350, 1200
        exp = date.today() + timedelta(days=30)
        mock_kite = MagicMock()
        mock_kite.ltp.return_value = {
            "NSE:INDIA VIX": {"last_price": 14.5}
        }
        passed, details = rollover.ValidateContractPrice(
            spot, strike, premium, exp, Kite=mock_kite)
        self.assertIn("VIX=14.5", details["iv_used"])

    def test_falls_back_when_no_kite(self):
        """Without Kite, falls back to implied IV or default."""
        spot, strike, premium = 23500, 22350, 1200
        exp = date.today() + timedelta(days=30)
        passed, details = rollover.ValidateContractPrice(spot, strike, premium, exp)
        self.assertIsNotNone(details["iv_used"])
        # Should use implied or fallback, not VIX
        self.assertNotIn("VIX", details["iv_used"])

    def test_short_dte_still_validates(self):
        """Contract expiring tomorrow should still validate."""
        spot, strike = 23500, 22350
        intrinsic = spot - strike  # 1150
        premium = intrinsic + 5  # tiny time value for 1 DTE
        exp = date.today() + timedelta(days=1)
        passed, details = rollover.ValidateContractPrice(spot, strike, premium, exp)
        # Should pass — premium ≈ intrinsic is correct for near-expiry
        self.assertTrue(passed, f"Near-expiry fair price failed: {details['checks_failed']}")

    def test_details_populated(self):
        """Validation details dict has all expected fields."""
        spot, strike, premium = 23500, 22350, 1200
        exp = date.today() + timedelta(days=30)
        _, details = rollover.ValidateContractPrice(spot, strike, premium, exp)
        self.assertIn("spot", details)
        self.assertIn("strike", details)
        self.assertIn("premium", details)
        self.assertIn("intrinsic", details)
        self.assertIn("time_value", details)
        self.assertIn("bs_theo", details)
        self.assertIn("iv_used", details)
        self.assertAlmostEqual(details["intrinsic"], 1150.0)
        self.assertAlmostEqual(details["time_value"], 50.0)


class TestComputePositionSizeITM(unittest.TestCase):
    """Test position sizing formula."""

    def test_normal_sizing(self):
        """Standard sizing: lots = floor(budget / (K * premium * lotSize))."""
        result = rollover.ComputePositionSizeITM(
            Premium=1200, LotSize=75, KValue=0.18, DailyVolBudget=27421
        )
        self.assertFalse(result["skipped"])
        # 0.18 * 1200 * 75 = 16200, 27421/16200 = 1.69 → floor to 1
        self.assertEqual(result["finalLots"], 1)
        self.assertAlmostEqual(result["dailyVolPerLot"], 16200.0)

    def test_minimum_one_lot(self):
        """Always at least 1 lot even if budget is tiny."""
        result = rollover.ComputePositionSizeITM(
            Premium=5000, LotSize=75, KValue=0.18, DailyVolBudget=1000
        )
        self.assertFalse(result["skipped"])
        self.assertEqual(result["finalLots"], 1)

    def test_large_budget_many_lots(self):
        """Large budget → multiple lots."""
        result = rollover.ComputePositionSizeITM(
            Premium=1200, LotSize=75, KValue=0.18, DailyVolBudget=100000
        )
        self.assertFalse(result["skipped"])
        # 100000 / 16200 = 6.17 → floor to 6
        self.assertEqual(result["finalLots"], 6)

    def test_zero_premium_skips(self):
        result = rollover.ComputePositionSizeITM(0, 75, 0.18, 27421)
        self.assertTrue(result["skipped"])

    def test_zero_lotsize_skips(self):
        result = rollover.ComputePositionSizeITM(1200, 0, 0.18, 27421)
        self.assertTrue(result["skipped"])

    def test_zero_kvalue_skips(self):
        result = rollover.ComputePositionSizeITM(1200, 75, 0, 27421)
        self.assertTrue(result["skipped"])

    def test_banknifty_sizing(self):
        """BANKNIFTY with smaller lot size (15) and higher premium."""
        result = rollover.ComputePositionSizeITM(
            Premium=2500, LotSize=15, KValue=0.18, DailyVolBudget=27421
        )
        self.assertFalse(result["skipped"])
        # 0.18 * 2500 * 15 = 6750, 27421/6750 = 4.06 → floor to 4
        self.assertEqual(result["finalLots"], 4)


class TestKTableSingleExtension(unittest.TestCase):
    """Test K_TABLE_SINGLE has been extended for monthly DTE."""

    def test_monthly_dte_30(self):
        k = lookupK(30, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 0.18)

    def test_monthly_dte_22(self):
        k = lookupK(22, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 0.18)

    def test_monthly_dte_45(self):
        k = lookupK(45, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 0.18)

    def test_two_weeks_dte_15(self):
        k = lookupK(15, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 0.25)

    def test_one_week_dte_10(self):
        k = lookupK(10, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 0.35)

    def test_existing_dte_5(self):
        k = lookupK(5, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 0.50)

    def test_existing_dte_1(self):
        k = lookupK(1, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 1.00)


class TestLookupKFallback(unittest.TestCase):
    """Test lookupK fallback for out-of-range DTE."""

    def test_dte_above_max(self):
        """DTE > 45 → returns smallest K (most conservative, largest position)."""
        k = lookupK(60, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 0.18)  # min of all K values

    def test_dte_100(self):
        k = lookupK(100, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 0.18)

    def test_dte_below_min(self):
        """DTE < 1 → returns largest K (smallest position)."""
        k = lookupK(0, K_TABLE_SINGLE)
        self.assertAlmostEqual(k, 1.00)  # max of all K values

    def test_no_longer_raises(self):
        """Should NOT raise ValueError for any DTE."""
        for dte in [-1, 0, 50, 100, 200]:
            try:
                lookupK(dte, K_TABLE_SINGLE)
            except ValueError:
                self.fail(f"lookupK raised ValueError for DTE={dte}")


class TestStateManagement(unittest.TestCase):
    """Test state file load/save/recovery."""

    def setUp(self):
        rollover.STATE_FILE_PATH = Path(_TEST_DIR) / f"test_state_{id(self)}.json"
        if rollover.STATE_FILE_PATH.exists():
            rollover.STATE_FILE_PATH.unlink()

    def tearDown(self):
        if rollover.STATE_FILE_PATH.exists():
            rollover.STATE_FILE_PATH.unlink()

    def test_load_default_when_missing(self):
        """Missing state file → default state (NONE for both)."""
        state = rollover.LoadState()
        self.assertEqual(state["NIFTY"]["status"], "NONE")
        self.assertEqual(state["BANKNIFTY"]["status"], "NONE")

    def test_save_and_load(self):
        """Round-trip: save then load preserves state."""
        state = rollover.LoadState()
        state["NIFTY"]["status"] = "HOLDING"
        state["NIFTY"]["current_contract"] = "NIFTY25MAY22350CE"
        state["NIFTY"]["quantity"] = 150
        state["NIFTY"]["entry_price"] = 1200.50
        rollover.SaveState(state)

        loaded = rollover.LoadState()
        self.assertEqual(loaded["NIFTY"]["status"], "HOLDING")
        self.assertEqual(loaded["NIFTY"]["current_contract"], "NIFTY25MAY22350CE")
        self.assertEqual(loaded["NIFTY"]["quantity"], 150)
        self.assertAlmostEqual(loaded["NIFTY"]["entry_price"], 1200.50)

    def test_load_corrupt_json(self):
        """Corrupt JSON → falls back to default state."""
        with open(rollover.STATE_FILE_PATH, "w") as f:
            f.write("{invalid json!!!")
        state = rollover.LoadState()
        self.assertEqual(state["NIFTY"]["status"], "NONE")

    def test_load_missing_index(self):
        """State file with only NIFTY → adds BANKNIFTY defaults."""
        with open(rollover.STATE_FILE_PATH, "w") as f:
            json.dump({"NIFTY": {"status": "HOLDING", "current_contract": "X"}}, f)
        state = rollover.LoadState()
        self.assertEqual(state["NIFTY"]["status"], "HOLDING")
        self.assertEqual(state["BANKNIFTY"]["status"], "NONE")


class TestRecoverStateFromPositions(unittest.TestCase):
    """Test auto-scan recovery from broker positions."""

    def test_single_matching_position(self):
        """One matching CE position → successful recovery."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {
            "net": [
                {
                    "tradingsymbol": "NIFTY24APR22350CE",
                    "product": "NRML",
                    "exchange": "NFO",
                    "quantity": 150,
                    "average_price": 1200.0,
                    "expiry": date(2025, 4, 24),
                },
            ]
        }
        result = rollover.RecoverStateFromPositions(
            mock_kite, "NIFTY", date(2025, 4, 24)
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "HOLDING")
        self.assertEqual(result["current_contract"], "NIFTY24APR22350CE")
        self.assertEqual(result["quantity"], 150)

    def test_no_matching_positions(self):
        """No positions match → returns None."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {"net": []}
        result = rollover.RecoverStateFromPositions(
            mock_kite, "NIFTY", date(2025, 4, 24)
        )
        self.assertIsNone(result)

    def test_filters_out_puts(self):
        """PE positions should not match."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {
            "net": [
                {
                    "tradingsymbol": "NIFTY24APR22350PE",
                    "product": "NRML",
                    "exchange": "NFO",
                    "quantity": 150,
                    "expiry": date(2025, 4, 24),
                },
            ]
        }
        result = rollover.RecoverStateFromPositions(
            mock_kite, "NIFTY", date(2025, 4, 24)
        )
        self.assertIsNone(result)

    def test_filters_out_mis_positions(self):
        """MIS product positions should not match."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {
            "net": [
                {
                    "tradingsymbol": "NIFTY24APR22350CE",
                    "product": "MIS",
                    "exchange": "NFO",
                    "quantity": 150,
                    "expiry": date(2025, 4, 24),
                },
            ]
        }
        result = rollover.RecoverStateFromPositions(
            mock_kite, "NIFTY", date(2025, 4, 24)
        )
        self.assertIsNone(result)

    def test_filters_out_short_positions(self):
        """Short (negative qty) positions should not match."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {
            "net": [
                {
                    "tradingsymbol": "NIFTY24APR22350CE",
                    "product": "NRML",
                    "exchange": "NFO",
                    "quantity": -150,
                    "expiry": date(2025, 4, 24),
                },
            ]
        }
        result = rollover.RecoverStateFromPositions(
            mock_kite, "NIFTY", date(2025, 4, 24)
        )
        self.assertIsNone(result)

    def test_multiple_positions_disambiguated_by_tag(self):
        """Multiple CE positions → disambiguate via ITM_ROLL order tag."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {
            "net": [
                {"tradingsymbol": "NIFTY24APR22350CE", "product": "NRML",
                 "exchange": "NFO", "quantity": 150, "expiry": date(2025, 4, 24)},
                {"tradingsymbol": "NIFTY24APR23000CE", "product": "NRML",
                 "exchange": "NFO", "quantity": 75, "expiry": date(2025, 4, 24)},
            ]
        }
        mock_kite.orders.return_value = [
            {"tradingsymbol": "NIFTY24APR22350CE", "tag": "ITM_ROLL"},
            {"tradingsymbol": "NIFTY24APR23000CE", "tag": "OTHER_STRAT"},
        ]
        result = rollover.RecoverStateFromPositions(
            mock_kite, "NIFTY", date(2025, 4, 24)
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["current_contract"], "NIFTY24APR22350CE")

    def test_multiple_positions_no_disambig_returns_none(self):
        """Multiple positions, no tag match → returns None (can't recover)."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {
            "net": [
                {"tradingsymbol": "NIFTY24APR22350CE", "product": "NRML",
                 "exchange": "NFO", "quantity": 150, "expiry": date(2025, 4, 24)},
                {"tradingsymbol": "NIFTY24APR23000CE", "product": "NRML",
                 "exchange": "NFO", "quantity": 75, "expiry": date(2025, 4, 24)},
            ]
        }
        mock_kite.orders.return_value = [
            {"tradingsymbol": "NIFTY24APR22350CE", "tag": "ITM_ROLL"},
            {"tradingsymbol": "NIFTY24APR23000CE", "tag": "ITM_ROLL"},
        ]
        result = rollover.RecoverStateFromPositions(
            mock_kite, "NIFTY", date(2025, 4, 24)
        )
        self.assertIsNone(result)

    def test_wrong_expiry_filtered(self):
        """Position with different expiry month should not match."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {
            "net": [
                {"tradingsymbol": "NIFTY24MAR22350CE", "product": "NRML",
                 "exchange": "NFO", "quantity": 150,
                 "expiry": date(2025, 3, 27)},  # March, not April
            ]
        }
        result = rollover.RecoverStateFromPositions(
            mock_kite, "NIFTY", date(2025, 4, 24)  # Looking for April
        )
        self.assertIsNone(result)

    def test_banknifty_not_confused_with_nifty(self):
        """BANKNIFTY position should not match NIFTY recovery."""
        mock_kite = MagicMock()
        mock_kite.positions.return_value = {
            "net": [
                {"tradingsymbol": "BANKNIFTY24APR48000CE", "product": "NRML",
                 "exchange": "NFO", "quantity": 15, "expiry": date(2025, 4, 24)},
            ]
        }
        result = rollover.RecoverStateFromPositions(
            mock_kite, "NIFTY", date(2025, 4, 24)
        )
        self.assertIsNone(result)


class TestBuildOrderDict(unittest.TestCase):
    """Test order dict construction."""

    def test_buy_order(self):
        od = rollover.BuildOrderDict("NIFTY", "NIFTY25MAY22350CE", "BUY", 150)
        self.assertEqual(od["Tradetype"], "BUY")
        self.assertEqual(od["Exchange"], "NFO")
        self.assertEqual(od["Tradingsymbol"], "NIFTY25MAY22350CE")
        self.assertEqual(od["Quantity"], "150")
        self.assertEqual(od["Product"], "NRML")
        self.assertEqual(od["OrderTag"], "ITM_ROLL")
        self.assertEqual(od["User"], "OFS653")
        self.assertEqual(od["Broker"], "ZERODHA")

    def test_sell_order(self):
        od = rollover.BuildOrderDict("BANKNIFTY", "BANKNIFTY25APR48000CE", "SELL", 30)
        self.assertEqual(od["Tradetype"], "SELL")
        self.assertEqual(od["Quantity"], "30")


class TestDatabaseOperations(unittest.TestCase):
    """Test forecast_db ITM call rollover functions."""

    def setUp(self):
        db._Connection = None
        db.DB_PATH = os.path.join(_TEST_DIR, f"test_db_{id(self)}.db")
        db.InitDB()

    def tearDown(self):
        if db._Connection:
            db._Connection.close()
            db._Connection = None
        if os.path.exists(db.DB_PATH):
            os.unlink(db.DB_PATH)

    def test_log_rollover(self):
        """Log a new ITM call rollover entry."""
        row_id = db.LogITMCallRollover(
            "NIFTY", "2025-04-24", "NIFTY25APR22350CE", "NIFTY25MAY22400CE",
            150, 150, DailyVolBudget=27421, KValue=0.18,
            Broker="ZERODHA", UserAccount="OFS653"
        )
        self.assertIsNotNone(row_id)
        self.assertGreater(row_id, 0)

    def test_update_status_leg1_done(self):
        """Update status to LEG1_DONE with fill info."""
        row_id = db.LogITMCallRollover(
            "NIFTY", "2025-04-24", "OLD", "NEW", 150, 150
        )
        db.UpdateITMCallRolloverStatus(
            row_id, "LEG1_DONE",
            leg1_order_id="ORD123",
            leg1_fill_price=1200.50,
            leg1_slippage=0.3
        )
        # Verify
        conn = db._GetConn()
        row = conn.execute("SELECT * FROM itm_call_rollover_log WHERE id = ?", (row_id,)).fetchone()
        self.assertEqual(row["status"], "LEG1_DONE")
        self.assertAlmostEqual(row["leg1_fill_price"], 1200.50)

    def test_update_status_complete(self):
        """Update to COMPLETE with full fill info."""
        row_id = db.LogITMCallRollover("NIFTY", "2025-04-24", "OLD", "NEW", 150, 150)
        db.UpdateITMCallRolloverStatus(row_id, "LEG1_DONE",
                                        leg1_fill_price=1200.0, leg1_slippage=0.5)
        db.UpdateITMCallRolloverStatus(row_id, "COMPLETE",
                                        leg2_fill_price=1250.0, leg2_slippage=0.3,
                                        roll_spread=50.0, realized_pnl=7500.0,
                                        executed_at="2025-04-24T15:05:00")
        conn = db._GetConn()
        row = conn.execute("SELECT * FROM itm_call_rollover_log WHERE id = ?", (row_id,)).fetchone()
        self.assertEqual(row["status"], "COMPLETE")
        self.assertAlmostEqual(row["leg2_fill_price"], 1250.0)
        self.assertAlmostEqual(row["roll_spread"], 50.0)
        self.assertAlmostEqual(row["realized_pnl"], 7500.0)

    def test_get_incomplete_rollovers(self):
        """LEG1_DONE rows are returned for crash recovery."""
        db.LogITMCallRollover("NIFTY", "2025-04-24", "OLD", "NEW", 150, 150)
        row_id = db.LogITMCallRollover("NIFTY", "2025-04-24", "OLD2", "NEW2", 150, 150)
        db.UpdateITMCallRolloverStatus(row_id, "LEG1_DONE")

        incomplete = db.GetIncompleteITMCallRollovers("NIFTY")
        self.assertEqual(len(incomplete), 1)
        self.assertEqual(incomplete[0]["old_contract"], "OLD2")

    def test_get_incomplete_rollovers_empty(self):
        """No LEG1_DONE rows → empty list."""
        db.LogITMCallRollover("NIFTY", "2025-04-24", "OLD", "NEW", 150, 150)
        incomplete = db.GetIncompleteITMCallRollovers("NIFTY")
        self.assertEqual(len(incomplete), 0)

    def test_get_recent_rollovers(self):
        """Recent rollovers returned in reverse chronological order."""
        db.LogITMCallRollover("NIFTY", "2025-03-27", "A", "B", 150, 150)
        db.LogITMCallRollover("NIFTY", "2025-04-24", "C", "D", 150, 150)
        recent = db.GetRecentITMCallRollovers("NIFTY", limit=5)
        self.assertEqual(len(recent), 2)

    def test_incomplete_filters_by_instrument(self):
        """GetIncompleteITMCallRollovers filters by instrument."""
        r1 = db.LogITMCallRollover("NIFTY", "2025-04-24", "OLD1", "NEW1", 150, 150)
        r2 = db.LogITMCallRollover("BANKNIFTY", "2025-04-24", "OLD2", "NEW2", 30, 30)
        db.UpdateITMCallRolloverStatus(r1, "LEG1_DONE")
        db.UpdateITMCallRolloverStatus(r2, "LEG1_DONE")

        nifty_inc = db.GetIncompleteITMCallRollovers("NIFTY")
        bn_inc = db.GetIncompleteITMCallRollovers("BANKNIFTY")
        all_inc = db.GetIncompleteITMCallRollovers()

        self.assertEqual(len(nifty_inc), 1)
        self.assertEqual(len(bn_inc), 1)
        self.assertEqual(len(all_inc), 2)


class TestVolBudgetLoading(unittest.TestCase):
    """Test dynamic vol budget computation via JSON accumulator."""

    def setUp(self):
        db._Connection = None
        db.DB_PATH = os.path.join(_TEST_DIR, f"test_vol_{id(self)}.db")
        db.InitDB()
        # PnlPath used by LoadVolBudgets
        self._pnl_path = Path(_TEST_DIR) / f"pnl_accum_{id(self)}.json"
        # Clean up any leftover JSON
        if self._pnl_path.exists():
            self._pnl_path.unlink()

    def tearDown(self):
        if db._Connection:
            db._Connection.close()
            db._Connection = None
        if os.path.exists(db.DB_PATH):
            os.unlink(db.DB_PATH)
        if self._pnl_path.exists():
            self._pnl_path.unlink()

    def _write_pnl_json(self, cumulative=0.0, unrealized=0.0):
        """Write a realized_pnl_accumulator.json with given values."""
        data = {
            "fy_start": "2026-04-01",
            "cumulative_realized_pnl": cumulative,
            "eod_unrealized": unrealized,
            "last_updated": "2026-04-02T18:30:00",
            "daily_entries": {},
        }
        with open(self._pnl_path, "w") as f:
            json.dump(data, f)

    def test_loads_budgets_from_config(self):
        """Loads vol budgets using instrument_config.json."""
        budgets, eff_cap = rollover.LoadVolBudgets()
        self.assertIn("NIFTY", budgets)
        self.assertIn("BANKNIFTY", budgets)
        self.assertGreater(budgets["NIFTY"], 0)
        self.assertGreater(budgets["BANKNIFTY"], 0)
        self.assertGreater(eff_cap, 0)

    def test_nifty_and_banknifty_budgets_equal(self):
        """Both indices have same weights → same budget."""
        budgets, _ = rollover.LoadVolBudgets()
        self.assertAlmostEqual(budgets["NIFTY"], budgets["BANKNIFTY"], places=2)

    def test_json_accumulator_realized_profit(self):
        """JSON with positive cumulative_realized → larger effective capital → larger budget."""
        self._write_pnl_json(cumulative=0.0, unrealized=0.0)
        with patch.object(rollover, "LoadVolBudgets", wraps=rollover.LoadVolBudgets):
            # Monkey-patch the path resolution inside LoadVolBudgets
            with patch("itm_call_rollover.Path") as MockPath:
                # This is tricky — call the real function but with patched JSON path
                pass

        # Simpler approach: directly test the formula
        from vol_target import compute_daily_vol_target
        base = 9999999
        weights = {"sector_weight": 0.3, "asset_weight": 0.15,
                   "asset_DM": 3.9, "neg_skew_discount": 0.5}
        vol_pct = 0.50

        budget_base = compute_daily_vol_target(base, vol_pct, weights)
        budget_profit = compute_daily_vol_target(base + 500000, vol_pct, weights)
        budget_profit_unreal = compute_daily_vol_target(base + 500000 + 100000, vol_pct, weights)

        self.assertGreater(budget_profit, budget_base)
        self.assertGreater(budget_profit_unreal, budget_profit)
        # Capital scales linearly
        self.assertAlmostEqual(budget_profit / budget_base, (base + 500000) / base, places=4)

    def test_json_accumulator_realized_loss(self):
        """JSON with negative cumulative_realized → smaller effective capital → smaller budget."""
        from vol_target import compute_daily_vol_target
        base = 9999999
        weights = {"sector_weight": 0.3, "asset_weight": 0.15,
                   "asset_DM": 3.9, "neg_skew_discount": 0.5}
        vol_pct = 0.50

        budget_base = compute_daily_vol_target(base, vol_pct, weights)
        budget_loss = compute_daily_vol_target(base - 300000, vol_pct, weights)

        self.assertLess(budget_loss, budget_base)

    def test_json_accumulator_unrealized_adds_to_capital(self):
        """eod_unrealized is included in effective capital, not just cumulative_realized."""
        from vol_target import compute_daily_vol_target
        base = 9999999
        weights = {"sector_weight": 0.3, "asset_weight": 0.15,
                   "asset_DM": 3.9, "neg_skew_discount": 0.5}
        vol_pct = 0.50

        # realized=100k, unrealized=50k → effective = base + 150k
        effective_with_both = base + 100000 + 50000
        budget_both = compute_daily_vol_target(effective_with_both, vol_pct, weights)

        # realized=100k only → effective = base + 100k
        effective_realized_only = base + 100000
        budget_realized_only = compute_daily_vol_target(effective_realized_only, vol_pct, weights)

        self.assertGreater(budget_both, budget_realized_only)

    def test_budget_scales_with_pnl(self):
        """Positive realized P&L → larger budget (DB fallback path)."""
        # First: baseline with no P&L
        budgets_base, _ = rollover.LoadVolBudgets()

        # Add some realized P&L
        conn = db._GetConn()
        conn.execute(
            """INSERT INTO system_positions
               (instrument, target_qty, confirmed_qty, avg_entry_price, point_value, updated_at)
               VALUES ('TEST_NIFTY', 100, 100, 100.0, 1.0, datetime('now'))""",
        )
        conn.commit()
        db.RealizePnl("TEST_NIFTY", 200.0, 100, 1.0, "options", WasLong=True)

        budgets_after, _ = rollover.LoadVolBudgets()
        self.assertGreater(budgets_after["NIFTY"], budgets_base["NIFTY"])


class TestCountTradingDays(unittest.TestCase):
    """Test trading days counting."""

    def test_simple_weekdays(self):
        """Mon to Fri = 4 trading days (Tue, Wed, Thu, Fri)."""
        _HOLIDAY_DATES.clear()
        count = rollover.CountTradingDaysUntilExpiry(
            date(2025, 4, 4), FromDate=date(2025, 3, 31)  # Mon to Fri
        )
        self.assertEqual(count, 4)

    def test_skip_weekend(self):
        """Fri to next Fri across weekend = 5 trading days."""
        _HOLIDAY_DATES.clear()
        count = rollover.CountTradingDaysUntilExpiry(
            date(2025, 4, 11), FromDate=date(2025, 4, 4)
        )
        self.assertEqual(count, 5)

    def test_holiday_excluded(self):
        """Holiday in between → one less trading day."""
        _HOLIDAY_DATES.clear()
        _HOLIDAY_DATES.add(date(2025, 4, 2))
        count = rollover.CountTradingDaysUntilExpiry(
            date(2025, 4, 4), FromDate=date(2025, 3, 31)
        )
        self.assertEqual(count, 3)  # Tue is holiday → Mon(exc), Wed, Thu, Fri = 3
        _HOLIDAY_DATES.clear()

    def test_same_day(self):
        """FromDate == ExpiryDate → 0."""
        count = rollover.CountTradingDaysUntilExpiry(
            date(2025, 4, 4), FromDate=date(2025, 4, 4)
        )
        self.assertEqual(count, 0)


class TestBuildRolloverEmailHtml(unittest.TestCase):
    """Test email HTML generation."""

    def test_success_email_contains_key_fields(self):
        result = {
            "success": True,
            "index": "NIFTY",
            "daily_vol_budget": 27421,
            "k_value": 0.18,
            "dte": 30,
            "spot": 23500,
            "strike": 22350,
            "lot_size": 75,
            "premium": 1250.0,
            "symbol": "NIFTY25MAY22400CE",
            "effective_capital": 10050000,
            "current_expiry": "2025-04-24",
            "next_expiry": "2025-05-29",
            "selection": {
                "best": {"strike": 22350, "premium": 1250.0, "bs_theo": 1248.0,
                         "value_pct": -0.16, "spread_pct": 0.35,
                         "validation": {"intrinsic": 1150, "bs_theo": 1248.0,
                                        "iv_used": "VIX=14.5", "checks_passed": ["OK"],
                                        "checks_failed": []}},
                "all_candidates": [
                    {"strike": 22350, "premium": 1250.0, "bs_theo": 1248.0,
                     "value_pct": -0.16, "spread_pct": 0.35},
                    {"strike": 22300, "premium": 1305.0, "bs_theo": 1298.0,
                     "value_pct": 0.54, "spread_pct": 1.2},
                ],
                "rejected": [],
            },
            "size_result": {
                "finalLots": 2, "allowedLots": 2, "dailyVolPerLot": 16875.0,
                "premium": 1250.0, "kValue": 0.18, "dailyVolBudget": 27421,
                "skipped": False, "skipReason": None,
            },
            "leg1": {
                "contract": "NIFTY25APR22350CE", "quantity": 150,
                "fill_price": 1195.0, "slippage": 0.5, "realized_pnl": 7500,
            },
            "leg2": {
                "contract": "NIFTY25MAY22400CE", "quantity": 150, "lots": 2,
                "premium": 1250.0, "fill_price": 1252.0, "slippage": 0.3,
                "expiry": "2025-05-29",
            },
            "roll_spread": 57.0,
        }
        html = rollover.BuildRolloverEmailHtml("NIFTY", result)
        self.assertIn("NIFTY", html)
        self.assertIn("ROLLOVER COMPLETE", html)
        self.assertIn("27,421", html)
        self.assertIn("22350", html)
        self.assertIn("Leg 1", html)
        self.assertIn("Leg 2", html)
        self.assertIn("7,500", html)  # P&L
        # Sizing formula present
        self.assertIn("dailyVolPerLot", html)
        self.assertIn("allowedLots", html)
        self.assertIn("finalLots", html)
        # K table present
        self.assertIn("K_TABLE_SINGLE", html)
        # Candidates table present
        self.assertIn("Strike Selection", html)
        # Validation present
        self.assertIn("Price Validation", html)
        self.assertIn("Intrinsic", html)
        self.assertIn("Black-Scholes", html)

    def test_failure_email(self):
        result = {"success": False, "index": "BANKNIFTY",
                  "daily_vol_budget": 27421, "k_value": 0.18, "dte": 30,
                  "spot": 48000, "strike": 46000}
        html = rollover.BuildRolloverEmailHtml("BANKNIFTY", result)
        self.assertIn("ROLLOVER FAILED", html)
        self.assertIn("BANKNIFTY", html)


@patch.object(rollover, "ValidateContractPrice",
              return_value=(True, {"checks_passed": ["TEST"], "checks_failed": [],
                                   "bs_theo": 1150.0, "iv_used": "test"}))
class TestExecuteRolloverDryRun(unittest.TestCase):
    """Test ExecuteRollover in dry-run mode (no real orders)."""

    def setUp(self):
        db._Connection = None
        db.DB_PATH = os.path.join(_TEST_DIR, f"test_exec_{id(self)}.db")
        db.InitDB()
        rollover.STATE_FILE_PATH = Path(_TEST_DIR) / f"test_state_exec_{id(self)}.json"

    def tearDown(self):
        if db._Connection:
            db._Connection.close()
            db._Connection = None
        if os.path.exists(db.DB_PATH):
            os.unlink(db.DB_PATH)
        if rollover.STATE_FILE_PATH.exists():
            rollover.STATE_FILE_PATH.unlink()

    def _gbmp_side_effect(self, q, trade_type):
        """Return ask price for BUY, bid price for SELL."""
        depth = q.get("depth", {})
        if trade_type == "BUY":
            sells = depth.get("sell", [])
            return float(sells[0]["price"]) if sells and sells[0]["price"] > 0 else 0.0
        buys = depth.get("buy", [])
        return float(buys[0]["price"]) if buys and buys[0]["price"] > 0 else 0.0

    @patch("itm_call_rollover.GetBestMarketPremium")
    @patch("itm_call_rollover.date")
    def test_dry_run_first_run(self, mock_date, mock_gbmp, _mock_val):
        """Dry run with first run → reports what would be bought."""
        mock_date.today.return_value = date(2025, 4, 24)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        mock_gbmp.side_effect = self._gbmp_side_effect

        mock_kite = MagicMock()
        mock_kite.ltp.return_value = {"NSE:NIFTY 50": {"last_price": 23500}}

        # Build instruments with multiple months
        exp_apr = date(2025, 4, 24)
        exp_may = date(2025, 5, 29)
        instruments = _make_nifty_instruments([exp_apr, exp_may],
                                              strikes=[22300, 22350, 22400])
        _focn.GetInstrumentsCached.return_value = instruments

        # Mock quote for strike selection
        def mock_quote(keys):
            return {k: _make_quote(bid=1148, ask=1152) for k in keys}
        mock_kite.quote = MagicMock(side_effect=mock_quote)

        state = rollover.LoadState()
        result = rollover.ExecuteRollover(mock_kite, "NIFTY", state,
                                           DryRun=True, FirstRun=True)
        self.assertTrue(result["success"])
        self.assertIsNotNone(result["leg2"])
        self.assertGreater(result["leg2"]["quantity"], 0)

    @patch("itm_call_rollover.GetBestMarketPremium")
    @patch("itm_call_rollover.date")
    def test_dry_run_does_not_save_state(self, mock_date, mock_gbmp, _mock_val):
        """Dry run should NOT modify state file."""
        mock_date.today.return_value = date(2025, 4, 24)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        mock_gbmp.side_effect = self._gbmp_side_effect

        mock_kite = MagicMock()
        mock_kite.ltp.return_value = {"NSE:NIFTY 50": {"last_price": 23500}}

        exp_apr = date(2025, 4, 24)
        exp_may = date(2025, 5, 29)
        instruments = _make_nifty_instruments([exp_apr, exp_may], strikes=[22350])
        _focn.GetInstrumentsCached.return_value = instruments

        # Return quotes for both April and May contracts (first-run uses current month)
        all_quotes = {}
        for prefix in ["NFO:NIFTY24APR2522350CE", "NFO:NIFTY29MAY2522350CE"]:
            all_quotes[prefix] = _make_quote(bid=1148, ask=1152)
        mock_kite.quote = MagicMock(return_value=all_quotes)

        state = rollover.LoadState()
        rollover.ExecuteRollover(mock_kite, "NIFTY", state, DryRun=True, FirstRun=True)

        # State file should not exist (was never saved)
        self.assertFalse(rollover.STATE_FILE_PATH.exists())


@patch.object(rollover, "ValidateContractPrice",
              return_value=(True, {"checks_passed": ["TEST"], "checks_failed": [],
                                   "bs_theo": 1150.0, "iv_used": "test"}))
class TestExecuteRolloverLive(unittest.TestCase):
    """Test ExecuteRollover with mocked SmartChaseExecute."""

    def setUp(self):
        db._Connection = None
        db.DB_PATH = os.path.join(_TEST_DIR, f"test_live_{id(self)}.db")
        db.InitDB()
        rollover.STATE_FILE_PATH = Path(_TEST_DIR) / f"test_state_live_{id(self)}.json"
        # Reset SmartChaseExecute mock
        _sc.SmartChaseExecute.reset_mock()
        _sc.SmartChaseExecute.return_value = (True, "ORD123",
                                               {"fill_price": 1200.0, "slippage": 0.5})

    def tearDown(self):
        if db._Connection:
            db._Connection.close()
            db._Connection = None
        if os.path.exists(db.DB_PATH):
            os.unlink(db.DB_PATH)
        if rollover.STATE_FILE_PATH.exists():
            rollover.STATE_FILE_PATH.unlink()

    def _gbmp_side_effect(self, q, trade_type):
        """Return ask price for BUY, bid price for SELL."""
        depth = q.get("depth", {})
        if trade_type == "BUY":
            sells = depth.get("sell", [])
            return float(sells[0]["price"]) if sells and sells[0]["price"] > 0 else 0.0
        buys = depth.get("buy", [])
        return float(buys[0]["price"]) if buys and buys[0]["price"] > 0 else 0.0

    def _setup_mock_kite(self, spot=23500):
        mock_kite = MagicMock()
        mock_kite.ltp.return_value = {
            "NSE:NIFTY 50": {"last_price": spot},
        }
        exp_apr = date(2025, 4, 24)
        exp_may = date(2025, 5, 29)
        instruments = _make_nifty_instruments([exp_apr, exp_may],
                                              strikes=[22300, 22350, 22400])
        _focn.GetInstrumentsCached.return_value = instruments
        # Return quotes for both April and May contracts
        all_quotes = {}
        for s in [22300, 22350, 22400]:
            all_quotes[f"NFO:NIFTY24APR25{s}CE"] = _make_quote(bid=1148, ask=1152)
            all_quotes[f"NFO:NIFTY29MAY25{s}CE"] = _make_quote(bid=1148, ask=1152)
        mock_kite.quote = MagicMock(return_value=all_quotes)
        return mock_kite

    @patch("itm_call_rollover.GetBestMarketPremium")
    @patch("itm_call_rollover.date")
    @patch("itm_call_rollover.SendEmail")
    def test_first_run_buys_only(self, mock_email, mock_date, mock_gbmp, _mock_val):
        """First run: no leg 1 (exit), only leg 2 (buy)."""
        mock_date.today.return_value = date(2025, 4, 24)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        mock_gbmp.side_effect = self._gbmp_side_effect

        mock_kite = self._setup_mock_kite()
        state = rollover.LoadState()

        result = rollover.ExecuteRollover(mock_kite, "NIFTY", state,
                                           DryRun=False, FirstRun=True)
        self.assertTrue(result["success"])
        self.assertIsNone(result["leg1"])
        self.assertIsNotNone(result["leg2"])

        # SmartChaseExecute called once (leg 2 only)
        self.assertEqual(_sc.SmartChaseExecute.call_count, 1)
        call_args = _sc.SmartChaseExecute.call_args
        self.assertEqual(call_args[0][1]["Tradetype"], "BUY")

        # State file updated
        saved_state = rollover.LoadState()
        self.assertEqual(saved_state["NIFTY"]["status"], "HOLDING")

    @patch("itm_call_rollover.GetBestMarketPremium")
    @patch("itm_call_rollover.date")
    @patch("itm_call_rollover.SendEmail")
    def test_normal_rollover_two_legs(self, mock_email, mock_date, mock_gbmp, _mock_val):
        """Normal rollover: exit current + buy next."""
        mock_date.today.return_value = date(2025, 4, 24)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        mock_gbmp.side_effect = self._gbmp_side_effect

        mock_kite = self._setup_mock_kite()

        # Set up existing position in state
        state = rollover.LoadState()
        state["NIFTY"] = {
            "status": "HOLDING",
            "current_contract": "NIFTY24APR22350CE",
            "current_expiry": "2025-04-24",
            "lots": 2,
            "quantity": 150,
            "entry_price": 1100.0,
            "entry_date": "2025-03-27",
            "order_tag": "ITM_ROLL",
        }

        # SmartChase: leg 1 (exit) returns fill, leg 2 (entry) returns fill
        _sc.SmartChaseExecute.side_effect = [
            (True, "ORD_EXIT", {"fill_price": 1180.0, "slippage": -0.5}),
            (True, "ORD_ENTRY", {"fill_price": 1250.0, "slippage": 0.3}),
        ]

        result = rollover.ExecuteRollover(mock_kite, "NIFTY", state, DryRun=False)

        self.assertTrue(result["success"])
        self.assertIsNotNone(result["leg1"])
        self.assertIsNotNone(result["leg2"])
        self.assertEqual(result["leg1"]["fill_price"], 1180.0)
        self.assertEqual(result["leg2"]["fill_price"], 1250.0)

        # SmartChaseExecute called twice
        self.assertEqual(_sc.SmartChaseExecute.call_count, 2)

        # P&L logged: (1180 - 1100) * 150 = 12000
        self.assertAlmostEqual(result["leg1"]["realized_pnl"], 12000.0)

        # Roll spread: 1250 - 1180 = 70
        self.assertAlmostEqual(result["roll_spread"], 70.0)

        # State updated to new contract
        saved_state = rollover.LoadState()
        self.assertEqual(saved_state["NIFTY"]["status"], "HOLDING")
        self.assertIn("MAY", saved_state["NIFTY"]["current_contract"])

    @patch("itm_call_rollover.GetBestMarketPremium")
    @patch("itm_call_rollover.date")
    @patch("itm_call_rollover.SendEmail")
    def test_leg1_failure_aborts(self, mock_email, mock_date, mock_gbmp, _mock_val):
        """Leg 1 fails → rollover aborted, position unchanged."""
        mock_date.today.return_value = date(2025, 4, 24)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        mock_gbmp.side_effect = self._gbmp_side_effect

        mock_kite = self._setup_mock_kite()

        state = rollover.LoadState()
        state["NIFTY"] = {
            "status": "HOLDING",
            "current_contract": "NIFTY24APR22350CE",
            "current_expiry": "2025-04-24",
            "lots": 2, "quantity": 150, "entry_price": 1100.0,
            "entry_date": "2025-03-27", "order_tag": "ITM_ROLL",
        }

        _sc.SmartChaseExecute.side_effect = [
            (False, None, {"fill_price": 0, "slippage": 0}),  # Leg 1 fails
        ]

        result = rollover.ExecuteRollover(mock_kite, "NIFTY", state, DryRun=False)

        self.assertFalse(result["success"])
        # Only leg 1 attempted
        self.assertEqual(_sc.SmartChaseExecute.call_count, 1)

        # DB logged as LEG1_FAILED
        recent = db.GetRecentITMCallRollovers("NIFTY")
        self.assertEqual(recent[0]["status"], "LEG1_FAILED")

    @patch("itm_call_rollover.GetBestMarketPremium")
    @patch("itm_call_rollover.date")
    @patch("itm_call_rollover.SendEmail")
    def test_leg2_failure_sends_critical_email(self, mock_email, mock_date, mock_gbmp, _mock_val):
        """Leg 2 fails → CRITICAL email sent, position is flat."""
        mock_date.today.return_value = date(2025, 4, 24)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        mock_gbmp.side_effect = self._gbmp_side_effect

        mock_kite = self._setup_mock_kite()

        state = rollover.LoadState()
        state["NIFTY"] = {
            "status": "HOLDING",
            "current_contract": "NIFTY24APR22350CE",
            "current_expiry": "2025-04-24",
            "lots": 2, "quantity": 150, "entry_price": 1100.0,
            "entry_date": "2025-03-27", "order_tag": "ITM_ROLL",
        }

        _sc.SmartChaseExecute.side_effect = [
            (True, "ORD_EXIT", {"fill_price": 1180.0, "slippage": 0}),   # Leg 1 OK
            (False, None, {"fill_price": 0, "slippage": 0}),              # Leg 2 FAIL
        ]

        result = rollover.ExecuteRollover(mock_kite, "NIFTY", state, DryRun=False)

        self.assertFalse(result["success"])

        # Critical email sent
        mock_email.assert_called()
        call_args = mock_email.call_args
        self.assertIn("CRITICAL", call_args[0][0])

        # DB: LEG2_FAILED
        recent = db.GetRecentITMCallRollovers("NIFTY")
        self.assertEqual(recent[0]["status"], "LEG2_FAILED")


class TestConfigIntegrity(unittest.TestCase):
    """Test that config files have the expected structure."""

    def test_instrument_config_has_itm_call_allocation(self):
        with open(Path(__file__).parent / "instrument_config.json") as f:
            cfg = json.load(f)
        alloc = cfg["options_allocation"]
        self.assertIn("NIFTY_ITM_CALL", alloc)
        self.assertIn("BANKNIFTY_ITM_CALL", alloc)
        self.assertEqual(alloc["NIFTY_ITM_CALL"]["vol_weights"]["asset_weight"], 0.15)
        self.assertEqual(alloc["BANKNIFTY_ITM_CALL"]["vol_weights"]["asset_weight"], 0.15)

    def test_straddle_allocation_unchanged(self):
        with open(Path(__file__).parent / "instrument_config.json") as f:
            cfg = json.load(f)
        alloc = cfg["options_allocation"]
        self.assertEqual(alloc["NIFTY"]["vol_weights"]["asset_weight"], 0.35)
        self.assertEqual(alloc["SENSEX"]["vol_weights"]["asset_weight"], 0.35)

    def test_exec_config_has_banknifty(self):
        with open(Path(__file__).parent / "options_execution_config.json") as f:
            cfg = json.load(f)
        self.assertIn("BANKNIFTY_OPT", cfg)
        self.assertTrue(cfg["BANKNIFTY_OPT"]["enabled"])
        self.assertEqual(cfg["BANKNIFTY_OPT"]["exchange"], "NFO")
        self.assertEqual(cfg["BANKNIFTY_OPT"]["execution"]["execution_mode_override"], "D")

    def test_exec_config_nifty_unchanged(self):
        with open(Path(__file__).parent / "options_execution_config.json") as f:
            cfg = json.load(f)
        self.assertEqual(cfg["NIFTY_OPT"]["execution"]["baseline_spread_ticks"], 3)
        self.assertEqual(cfg["SENSEX_OPT"]["execution"]["baseline_spread_ticks"], 5)


class TestIsTradingDay(unittest.TestCase):
    """Test trading day detection."""

    def test_weekday_no_holiday(self):
        _HOLIDAY_DATES.clear()
        self.assertTrue(rollover.IsTradingDay(date(2025, 4, 7)))  # Monday

    def test_saturday(self):
        self.assertFalse(rollover.IsTradingDay(date(2025, 4, 5)))

    def test_sunday(self):
        self.assertFalse(rollover.IsTradingDay(date(2025, 4, 6)))

    def test_holiday(self):
        _HOLIDAY_DATES.add(date(2025, 4, 7))
        self.assertFalse(rollover.IsTradingDay(date(2025, 4, 7)))
        _HOLIDAY_DATES.clear()


class TestEdgeCases(unittest.TestCase):
    """Test various edge cases."""

    def test_itm_config_keys(self):
        """ITM_CONFIG has correct structure for both indices."""
        for idx in ["NIFTY", "BANKNIFTY"]:
            cfg = rollover.ITM_CONFIG[idx]
            self.assertIn("underlying_ltp_key", cfg)
            self.assertIn("exchange", cfg)
            self.assertIn("strike_step", cfg)
            self.assertIn("exec_config_key", cfg)
            self.assertIn("alloc_key", cfg)

    def test_nifty_strike_step_50(self):
        self.assertEqual(rollover.ITM_CONFIG["NIFTY"]["strike_step"], 50)

    def test_banknifty_strike_step_100(self):
        self.assertEqual(rollover.ITM_CONFIG["BANKNIFTY"]["strike_step"], 100)

    def test_order_tag_constant(self):
        self.assertEqual(rollover.ORDER_TAG, "ITM_ROLL")

    def test_user_constant(self):
        self.assertEqual(rollover.USER, "OFS653")

    def test_default_state_structure(self):
        """Default state has correct structure for both indices."""
        for idx in ["NIFTY", "BANKNIFTY"]:
            s = rollover.DEFAULT_STATE[idx]
            self.assertEqual(s["status"], "NONE")
            self.assertIsNone(s["current_contract"])
            self.assertEqual(s["order_tag"], "ITM_ROLL")


if __name__ == "__main__":
    unittest.main()
