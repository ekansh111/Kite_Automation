"""
Tests for nifty_put_rollover.py — NIFTY Monthly Put Buying System.

Mirrors test_itm_call_rollover.py structure with put-specific divergences:
  - Premium-at-risk sizing instead of vol-budget × K
  - Skip rule when 1 lot exceeds monthly budget
  - PE strike filtering (1% ITM, round 100-step)
  - PUT_ROLL order tag for state recovery
  - nifty_put_rollover_log DB table
"""

import os
import sys
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

# ─── Module-level patching BEFORE imports ────────────────────────────
_TEST_DIR = tempfile.mkdtemp()

# Reuse existing Directories mock if already loaded by another test file
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

import types

def _get_or_stub(name, attrs):
    """Get existing stub from sys.modules or create a new one."""
    if name in sys.modules:
        mod = sys.modules[name]
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

# Reuse existing Holidays mock if loaded; otherwise create a no-op stub.
# We DO NOT overwrite an existing CheckForDateHoliday to avoid breaking other
# test files that share the same set (e.g. test_itm_call_rollover.py).
def _default_check_holiday(d, exchange=None):
    return False
_holidays = _get_or_stub("Holidays", {"CheckForDateHoliday": _default_check_holiday})

_focn = _get_or_stub("FetchOptionContractName", {
    "GetInstrumentsCached": MagicMock(return_value=[]),
    "GetOptSegmentForExchange": MagicMock(return_value="NFO-OPT"),
    "GetBestMarketPremium": MagicMock(return_value=500.0),
    "ChunkList": lambda items, sz: [items[i:i+sz] for i in range(0, len(items), sz)],
    "FetchContractName": MagicMock(return_value="NIFTY26MAY24400PE"),
    "GetKiteClient": MagicMock(return_value=MagicMock()),
    "GetDerivativesExchange": MagicMock(return_value="NFO"),
    "SelectExpiryDateFromInstruments": MagicMock(return_value=None),
})

_sc = _get_or_stub("smart_chase", {
    "SmartChaseExecute": MagicMock(return_value=(True, "ORD123",
                                                 {"fill_price": 500.0, "slippage": 0.5})),
    "EXCHANGE_OPEN_TIMES": {},
})

_sop = _get_or_stub("Server_Order_Place", {"order": MagicMock(return_value="ORD456")})
_gtt = _get_or_stub("Set_Gtt_Exit", {"Set_Gtt": MagicMock(return_value=None)})

from vol_target import compute_daily_vol_target

import forecast_db as db
db.InitDB()

import nifty_put_rollover as rollover


# ─── Helpers ─────────────────────────────────────────────────────────

def _make_pe_instrument(name, strike, expiry, tradingsymbol, lot_size=65,
                        segment="NFO-OPT"):
    """Build a mock PE instrument dict."""
    return {
        "name": name,
        "strike": float(strike),
        "expiry": expiry,
        "tradingsymbol": tradingsymbol,
        "lot_size": lot_size,
        "segment": segment,
        "instrument_type": "PE",
        "exchange": "NFO",
    }


def _make_nifty_pe_instruments(expiry_dates, strikes=None):
    """Generate mock NIFTY PE instruments for given expiries and strikes."""
    if strikes is None:
        strikes = list(range(23000, 26000, 100))
    instruments = []
    for exp in expiry_dates:
        for s in strikes:
            sym = f"NIFTY{exp.strftime('%d%b%y').upper()}{s}PE"
            instruments.append(_make_pe_instrument("NIFTY", s, exp, sym, lot_size=65))
    return instruments


def _make_quote(bid=500.0, ask=502.0, ltp=501.0, oi=100000):
    """Mock Kite quote response."""
    return {
        "last_price": ltp,
        "oi": oi,
        "depth": {
            "buy": [{"price": bid, "quantity": 100}],
            "sell": [{"price": ask, "quantity": 100}],
        },
    }


# ═════════════════════════════════════════════════════════════════════
# TEST CLASSES
# ═════════════════════════════════════════════════════════════════════


class TestComputePutCandidates(unittest.TestCase):
    """ComputePutCandidates generates 1% ITM PE strikes at round 100-step."""

    def test_basic_range(self):
        """Spot 24000, step 100 → strikes between 24100 and 24360."""
        cands = rollover.ComputePutCandidates(Spot=24000, StrikeStep=100,
                                              ITMPctMin=0.5, ITMPctMax=1.5)
        self.assertTrue(all(c >= 24100 and c <= 24400 for c in cands))
        # All strikes are ABOVE spot (ITM puts)
        self.assertTrue(all(c > 24000 for c in cands))

    def test_round_100_step_only(self):
        """All candidates must be multiples of 100."""
        cands = rollover.ComputePutCandidates(Spot=24327, StrikeStep=100,
                                              ITMPctMin=0.5, ITMPctMax=1.5)
        for c in cands:
            self.assertEqual(c % 100, 0, f"strike {c} not multiple of 100")

    def test_widens_when_empty(self):
        """If initial range produces nothing, widens to 0.25-2%."""
        # Tight range that may or may not produce — function widens automatically
        cands = rollover.ComputePutCandidates(Spot=24025, StrikeStep=100,
                                              ITMPctMin=0.5, ITMPctMax=0.6)
        self.assertGreater(len(cands), 0)

    def test_strikes_above_spot(self):
        """Put ITM = strike ABOVE spot. Verify direction."""
        cands = rollover.ComputePutCandidates(Spot=24300, StrikeStep=100)
        for c in cands:
            self.assertGreater(c, 24300)


class TestComputePositionSizePut(unittest.TestCase):
    """Premium-at-risk sizing with skip rule."""

    def test_normal_sizing_one_lot_fits(self):
        """1 lot cost < budget → returns 1 lot."""
        r = rollover.ComputePositionSizePut(Premium=560, LotSize=65, MonthlyBudget=48750)
        self.assertFalse(r["skipped"])
        self.assertEqual(r["finalLots"], 1)
        self.assertEqual(r["costPerLot"], 560 * 65)
        self.assertGreater(r["budgetUsedPct"], 0)

    def test_two_lots_fit_in_budget(self):
        """If budget supports 2 lots cleanly, returns 2."""
        # Cost per lot = 200 × 65 = 13,000; budget 48,750 → floor(48750/13000) = 3
        r = rollover.ComputePositionSizePut(Premium=200, LotSize=65, MonthlyBudget=48750)
        self.assertEqual(r["finalLots"], 3)
        self.assertFalse(r["skipped"])

    def test_floor_not_round(self):
        """Always floor — never overshoot the budget."""
        # Cost = 25,000 per lot; budget 48,750 → floor(1.95) = 1, NOT round(1.95) = 2
        r = rollover.ComputePositionSizePut(Premium=25000/65, LotSize=65, MonthlyBudget=48750)
        self.assertEqual(r["finalLots"], 1)
        self.assertLessEqual(r["costPerLot"] * r["finalLots"], 48750)

    def test_skip_when_cost_exceeds_tolerance(self):
        """1 lot > budget × 1.30 → skipped with reason."""
        # cost = 900 × 65 = 58,500; budget = 30,000; tolerance = 30,000 × 1.30 = 39,000
        # 58,500 > 39,000 → skip
        r = rollover.ComputePositionSizePut(Premium=900, LotSize=65, MonthlyBudget=30000)
        self.assertTrue(r["skipped"])
        self.assertEqual(r["finalLots"], 0)
        self.assertIn("cost_per_lot", r["skipReason"].lower())
        self.assertIn("max_tolerated", r["skipReason"].lower())

    def test_buy_when_cost_within_30pct_tolerance(self):
        """Cost slightly over budget but within 30% tolerance → still buy 1 lot."""
        # cost = 600 × 65 = 39,000; budget = 30,000; tolerance limit = 39,000
        # 39,000 ≤ 39,000 → buy 1 lot
        r = rollover.ComputePositionSizePut(Premium=600, LotSize=65, MonthlyBudget=30000)
        self.assertFalse(r["skipped"])
        self.assertEqual(r["finalLots"], 1)
        # Budget used > 100% (since cost > budget) but within tolerance
        self.assertGreater(r["budgetUsedPct"], 100)
        self.assertLessEqual(r["budgetUsedPct"], 130)

    def test_buy_when_cost_just_over_budget(self):
        """Cost 5% over budget → buy 1 lot."""
        r = rollover.ComputePositionSizePut(Premium=485, LotSize=65, MonthlyBudget=30000)
        # cost = 31,525, 5.1% over budget
        self.assertFalse(r["skipped"])
        self.assertEqual(r["finalLots"], 1)

    def test_invalid_inputs(self):
        """Invalid premium/lot returns skip."""
        r = rollover.ComputePositionSizePut(Premium=0, LotSize=65, MonthlyBudget=48750)
        self.assertTrue(r["skipped"])
        r = rollover.ComputePositionSizePut(Premium=500, LotSize=0, MonthlyBudget=48750)
        self.assertTrue(r["skipped"])

    def test_zero_budget(self):
        """Budget zero → skipped."""
        r = rollover.ComputePositionSizePut(Premium=500, LotSize=65, MonthlyBudget=0)
        self.assertTrue(r["skipped"])
        self.assertIn("budget", r["skipReason"].lower())

    def test_budget_used_pct_correct(self):
        """budget_used_pct should reflect cost × lots / budget."""
        r = rollover.ComputePositionSizePut(Premium=300, LotSize=65, MonthlyBudget=48750)
        # cost/lot = 19,500; budget 48,750 → 2 lots; used = 39,000/48,750 = 80%
        self.assertEqual(r["finalLots"], 2)
        self.assertAlmostEqual(r["budgetUsedPct"], 80.0, places=1)


class TestVolBudgetLoading(unittest.TestCase):
    """LoadMonthlyBudget reads NIFTY_PUT_BUY allocation correctly."""

    def test_loads_allocation(self):
        """Should resolve NIFTY_PUT_BUY key from instrument_config.json."""
        budget, eff_cap, daily, annual = rollover.LoadMonthlyBudget()
        self.assertGreater(budget, 0)
        self.assertGreater(eff_cap, 0)
        # monthly = annual / 12
        self.assertAlmostEqual(budget * 12, annual, places=0)
        # annual = daily * 16
        self.assertAlmostEqual(daily * 16, annual, places=0)


class TestStateManagement(unittest.TestCase):
    """LoadState / SaveState round-trip."""

    def setUp(self):
        # Use a temp state file
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        self.tmp.close()
        self._original_path = rollover.STATE_FILE_PATH
        rollover.STATE_FILE_PATH = Path(self.tmp.name)
        # Start clean
        os.unlink(self.tmp.name)

    def tearDown(self):
        rollover.STATE_FILE_PATH = self._original_path
        if os.path.exists(self.tmp.name):
            os.unlink(self.tmp.name)

    def test_load_default_when_missing(self):
        """Missing state file returns DEFAULT_STATE."""
        self.assertFalse(rollover.STATE_FILE_PATH.exists())
        state = rollover.LoadState()
        self.assertEqual(state["NIFTY"]["status"], "NONE")
        self.assertEqual(state["NIFTY"]["order_tag"], "PUT_ROLL")

    def test_save_and_load_roundtrip(self):
        """Save then load returns same state."""
        state = {
            "NIFTY": {
                "status": "HOLDING",
                "current_contract": "NIFTY26MAY24400PE",
                "current_expiry": "2026-05-26",
                "lots": 1, "quantity": 65,
                "entry_price": 565.5,
                "entry_date": "2026-04-30",
                "order_tag": "PUT_ROLL",
            }
        }
        rollover.SaveState(state)
        loaded = rollover.LoadState()
        self.assertEqual(loaded["NIFTY"]["status"], "HOLDING")
        self.assertEqual(loaded["NIFTY"]["current_contract"], "NIFTY26MAY24400PE")
        self.assertEqual(loaded["NIFTY"]["lots"], 1)

    def test_corrupt_file_returns_default(self):
        """Corrupt JSON returns default."""
        with open(rollover.STATE_FILE_PATH, "w") as f:
            f.write("{not valid json")
        state = rollover.LoadState()
        self.assertEqual(state["NIFTY"]["status"], "NONE")


class TestRecoverStateFromPositions(unittest.TestCase):
    """RecoverStateFromPositions: filter PE, NRML, NFO, expiry."""

    def test_no_matches_returns_none(self):
        """No matching positions → None."""
        kite = MagicMock()
        kite.positions.return_value = {"net": []}
        result = rollover.RecoverStateFromPositions(kite, "NIFTY", date(2026, 5, 26))
        self.assertIsNone(result)

    def test_single_pe_match_recovers(self):
        """One PE position matching all filters → recovers."""
        kite = MagicMock()
        kite.positions.return_value = {"net": [
            {
                "tradingsymbol": "NIFTY26MAY24400PE",
                "product": "NRML",
                "exchange": "NFO",
                "quantity": 65,
                "expiry": date(2026, 5, 26),
                "average_price": 565.5,
            }
        ]}
        result = rollover.RecoverStateFromPositions(kite, "NIFTY", date(2026, 5, 26))
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "HOLDING")
        self.assertEqual(result["current_contract"], "NIFTY26MAY24400PE")
        self.assertEqual(result["quantity"], 65)
        self.assertEqual(result["entry_price"], 565.5)

    def test_filters_out_ce_positions(self):
        """CE positions are not matched (puts only)."""
        kite = MagicMock()
        kite.positions.return_value = {"net": [
            {
                "tradingsymbol": "NIFTY26MAY24400CE",   # CE not PE
                "product": "NRML",
                "exchange": "NFO",
                "quantity": 65,
                "expiry": date(2026, 5, 26),
                "average_price": 100.0,
            }
        ]}
        result = rollover.RecoverStateFromPositions(kite, "NIFTY", date(2026, 5, 26))
        self.assertIsNone(result)

    def test_filters_out_wrong_expiry(self):
        """Position with different expiry is not matched."""
        kite = MagicMock()
        kite.positions.return_value = {"net": [
            {
                "tradingsymbol": "NIFTY26JUN24400PE",
                "product": "NRML",
                "exchange": "NFO",
                "quantity": 65,
                "expiry": date(2026, 6, 30),
                "average_price": 565.5,
            }
        ]}
        result = rollover.RecoverStateFromPositions(kite, "NIFTY", date(2026, 5, 26))
        self.assertIsNone(result)

    def test_disambiguates_multiple_matches_via_tag(self):
        """Multiple PE matches → disambiguate by PUT_ROLL order tag."""
        kite = MagicMock()
        kite.positions.return_value = {"net": [
            {"tradingsymbol": "NIFTY26MAY24400PE", "product": "NRML",
             "exchange": "NFO", "quantity": 65, "expiry": date(2026, 5, 26),
             "average_price": 565.5},
            {"tradingsymbol": "NIFTY26MAY24500PE", "product": "NRML",
             "exchange": "NFO", "quantity": 65, "expiry": date(2026, 5, 26),
             "average_price": 600.0},
        ]}
        kite.orders.return_value = [
            {"tag": "PUT_ROLL", "tradingsymbol": "NIFTY26MAY24400PE"},
            # Other tag for the second
            {"tag": "OTHER", "tradingsymbol": "NIFTY26MAY24500PE"},
        ]
        result = rollover.RecoverStateFromPositions(kite, "NIFTY", date(2026, 5, 26))
        self.assertIsNotNone(result)
        self.assertEqual(result["current_contract"], "NIFTY26MAY24400PE")


class TestBuildOrderDict(unittest.TestCase):
    """BuildOrderDict produces SmartChaseExecute-compatible dict."""

    def test_buy_order_structure(self):
        d = rollover.BuildOrderDict("NIFTY", "NIFTY26MAY24400PE", "BUY", 65)
        self.assertEqual(d["Tradetype"], "BUY")
        self.assertEqual(d["Tradingsymbol"], "NIFTY26MAY24400PE")
        self.assertEqual(d["Quantity"], "65")
        self.assertEqual(d["Product"], "NRML")
        self.assertEqual(d["OrderTag"], "PUT_ROLL")
        self.assertEqual(d["Broker"], "ZERODHA")

    def test_sell_order_structure(self):
        d = rollover.BuildOrderDict("NIFTY", "NIFTY26MAY24400PE", "SELL", 130)
        self.assertEqual(d["Tradetype"], "SELL")
        self.assertEqual(d["Quantity"], "130")


class TestDatabaseOperations(unittest.TestCase):
    """nifty_put_rollover_log CRUD operations."""

    def setUp(self):
        # Defensive: ensure the table exists (handles cases where another test
        # file initialized the DB at a path before nifty_put_rollover_log was
        # added, or where module-level cached connections need a re-init).
        db.InitDB()
        # Each test uses a fresh row
        self.row_id = db.LogNiftyPutRollover(
            "NIFTY", "2026-05-26", "NIFTY26APR24300PE", "NIFTY26MAY24400PE",
            65, 65, MonthlyBudget=48750.0, CostPerLot=37170.0, Premium=571.85,
            Broker="ZERODHA", UserAccount="OFS653"
        )

    def tearDown(self):
        # Clean up — remove the test row
        import sqlite3
        conn = sqlite3.connect(db.DB_PATH)
        conn.execute("DELETE FROM nifty_put_rollover_log WHERE id = ?", (self.row_id,))
        conn.commit()
        conn.close()

    def test_log_creates_pending_row(self):
        """LogNiftyPutRollover inserts a PENDING row."""
        rows = db.GetRecentNiftyPutRollovers(limit=20)
        match = next((r for r in rows if r["id"] == self.row_id), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["status"], "PENDING")
        self.assertEqual(match["instrument"], "NIFTY")
        self.assertEqual(match["monthly_budget"], 48750.0)
        self.assertEqual(match["cost_per_lot"], 37170.0)

    def test_update_to_leg1_done(self):
        """UpdateNiftyPutRolloverStatus to LEG1_DONE."""
        db.UpdateNiftyPutRolloverStatus(self.row_id, "LEG1_DONE",
                                         leg1_order_id="ORD1", leg1_fill_price=572.0)
        incomplete = db.GetIncompleteNiftyPutRollovers("NIFTY")
        match = next((r for r in incomplete if r["id"] == self.row_id), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["status"], "LEG1_DONE")
        self.assertEqual(match["leg1_fill_price"], 572.0)

    def test_complete_removes_from_incomplete(self):
        """LEG1_DONE → COMPLETE moves row out of GetIncomplete."""
        db.UpdateNiftyPutRolloverStatus(self.row_id, "LEG1_DONE")
        db.UpdateNiftyPutRolloverStatus(self.row_id, "COMPLETE",
                                         leg2_order_id="ORD2", roll_spread=1.0)
        incomplete = db.GetIncompleteNiftyPutRollovers("NIFTY")
        ids = [r["id"] for r in incomplete]
        self.assertNotIn(self.row_id, ids)

    def test_skip_reason_stored(self):
        """Skip reason can be persisted."""
        # Create a separate skipped row
        skip_id = db.LogNiftyPutRollover(
            "NIFTY", "2026-05-26", "", "NIFTY26MAY24400PE",
            0, 0, MonthlyBudget=5000.0, CostPerLot=37170.0, Premium=571.85,
            Broker="ZERODHA", UserAccount="OFS653",
            SkipReason="cost too high"
        )
        rows = db.GetRecentNiftyPutRollovers(limit=20)
        match = next((r for r in rows if r["id"] == skip_id), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["skip_reason"], "cost too high")

        # Cleanup
        import sqlite3
        conn = sqlite3.connect(db.DB_PATH)
        conn.execute("DELETE FROM nifty_put_rollover_log WHERE id = ?", (skip_id,))
        conn.commit()
        conn.close()


class TestExecuteRolloverDryRun(unittest.TestCase):
    """End-to-end dry-run flow."""

    def setUp(self):
        # Clean state
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        self.tmp.close()
        os.unlink(self.tmp.name)
        self._original_path = rollover.STATE_FILE_PATH
        rollover.STATE_FILE_PATH = Path(self.tmp.name)

    def tearDown(self):
        rollover.STATE_FILE_PATH = self._original_path
        if os.path.exists(self.tmp.name):
            os.unlink(self.tmp.name)

    def test_dry_run_first_run_completes(self):
        """Cold start dry-run produces a SUCCESS result with leg2 details."""
        kite = MagicMock()
        kite.ltp.return_value = {
            "NSE:NIFTY 50": {"last_price": 24300.0},
            "NSE:INDIA VIX": {"last_price": 18.5},
        }
        # Build PE instruments around target strike
        target_expiry = date(2026, 5, 26)
        instruments = _make_nifty_pe_instruments([target_expiry, date(2026, 6, 30)],
                                                  strikes=[24300, 24400, 24500, 24600])

        # Map strikes to realistic premium so GetBestMarketPremium returns the right value
        prem_map = {24400: 572.0, 24500: 621.0}

        def MockBestPremium(quote, side):
            ltp = quote.get("last_price", 0)
            return ltp if ltp else 0

        # Premiums chosen to be consistent with BS at VIX 18.5%, T~26d.
        # BS theo for 24400PE ≈ ₹430-450 (intrinsic 100, ~330 time value)
        # BS theo for 24500PE ≈ ₹490-520 (intrinsic 200, ~290 time value)
        kite.quote.return_value = {
            f"NFO:NIFTY{target_expiry.strftime('%d%b%y').upper()}24400PE":
                _make_quote(bid=438.0, ask=441.0, ltp=440.0, oi=80000),
            f"NFO:NIFTY{target_expiry.strftime('%d%b%y').upper()}24500PE":
                _make_quote(bid=505.0, ask=508.0, ltp=506.5, oi=600000),
        }

        with patch.object(rollover, "GetBestMarketPremium", side_effect=MockBestPremium):
            with patch.object(rollover, "GetInstrumentsCached", return_value=instruments):
                with patch.object(rollover, "GetMonthlyExpiries",
                                  return_value=[target_expiry, date(2026, 6, 30)]):
                    with patch.object(rollover, "GetCurrentMonthExpiry",
                                      return_value=target_expiry):
                        with patch.object(rollover, "GetNextMonthExpiry",
                                          return_value=date(2026, 6, 30)):
                            state = rollover.LoadState()
                            result = rollover.ExecuteRollover(
                                kite, "NIFTY", state, DryRun=True, FirstRun=True
                            )

        self.assertTrue(result.get("success"), f"Expected success, got: {result}")
        self.assertFalse(result.get("skipped", False))
        self.assertIsNotNone(result.get("leg2"))
        self.assertEqual(result["leg2"]["lots"], 1)


class TestExecuteRolloverSkipsHighIV(unittest.TestCase):
    """Skip rule fires when premium exceeds monthly budget."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        self.tmp.close()
        os.unlink(self.tmp.name)
        self._original_path = rollover.STATE_FILE_PATH
        rollover.STATE_FILE_PATH = Path(self.tmp.name)

    def tearDown(self):
        rollover.STATE_FILE_PATH = self._original_path
        if os.path.exists(self.tmp.name):
            os.unlink(self.tmp.name)

    def test_skip_rule_triggers_when_cost_exceeds_budget(self):
        """When 1 lot cost > monthly budget → returns skipped=True."""
        # Test the skip path directly via ComputePositionSizePut + ExecuteRollover
        # by patching LoadMonthlyBudget to return a tiny budget. The actual
        # premium fetching pipeline is exercised in test_dry_run_first_run_completes.
        kite = MagicMock()
        kite.ltp.return_value = {
            "NSE:NIFTY 50": {"last_price": 24300.0},
            "NSE:INDIA VIX": {"last_price": 18.5},
        }
        target_expiry = date(2026, 5, 26)
        instruments = _make_nifty_pe_instruments([target_expiry, date(2026, 6, 30)],
                                                  strikes=[24400, 24500])
        # Normal premiums — sized lot cost ≈ 32k
        kite.quote.return_value = {
            f"NFO:NIFTY{target_expiry.strftime('%d%b%y').upper()}24400PE":
                _make_quote(bid=499.0, ask=501.0, ltp=500.0, oi=80000),
            f"NFO:NIFTY{target_expiry.strftime('%d%b%y').upper()}24500PE":
                _make_quote(bid=549.0, ask=551.0, ltp=550.0, oi=600000),
        }

        # Tiny budget (5,000) — 1 lot at ₹500 × 65 = ₹32,500 ≫ tolerance (5,000 × 1.3 = 6,500)
        def TinyBudget():
            return 5000.0, 10000000.0, 7500.0, 120000.0

        def MockBestPremium(quote, side):
            ltp = quote.get("last_price", 0)
            return ltp if ltp else 0

        with patch.object(rollover, "GetBestMarketPremium", side_effect=MockBestPremium):
            with patch.object(rollover, "GetInstrumentsCached", return_value=instruments):
                with patch.object(rollover, "GetMonthlyExpiries",
                                  return_value=[target_expiry, date(2026, 6, 30)]):
                    with patch.object(rollover, "GetCurrentMonthExpiry",
                                      return_value=target_expiry):
                        with patch.object(rollover, "GetNextMonthExpiry",
                                          return_value=date(2026, 6, 30)):
                            with patch.object(rollover, "LoadMonthlyBudget",
                                              side_effect=TinyBudget):
                                state = rollover.LoadState()
                                result = rollover.ExecuteRollover(
                                    kite, "NIFTY", state, DryRun=True, FirstRun=True
                                )

        self.assertTrue(result.get("skipped"), f"Expected skipped, got: {result}")
        self.assertFalse(result.get("success"))
        self.assertIn("cost_per_lot", result.get("skip_reason", "").lower())


class TestEdgeCases(unittest.TestCase):
    """Misc edge cases — IsTradingDay / CountTradingDaysUntilExpiry coverage
    is in test_itm_call_rollover.py since the helpers are duplicated."""

    def test_count_trading_days_basic(self):
        """5 weekdays in a row → 5 trading days."""
        # Mon 2026-05-04 → Fri 2026-05-08 (one full week, no holiday)
        days = rollover.CountTradingDaysUntilExpiry(date(2026, 5, 8),
                                                    FromDate=date(2026, 5, 3))
        # FromDate exclusive, ExpiryDate inclusive: 4, 5, 6, 7, 8 = 5 days
        self.assertEqual(days, 5)

    def test_is_trading_day_weekend(self):
        """Saturdays and Sundays are not trading days."""
        # 2026-05-02 is a Saturday
        self.assertFalse(rollover.IsTradingDay(date(2026, 5, 2)))
        # 2026-05-03 is a Sunday
        self.assertFalse(rollover.IsTradingDay(date(2026, 5, 3)))

    def test_is_trading_day_weekday(self):
        """Weekdays without holiday are trading days."""
        # 2026-05-04 is a Monday
        self.assertTrue(rollover.IsTradingDay(date(2026, 5, 4)))


if __name__ == "__main__":
    unittest.main()
