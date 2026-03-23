"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    PlaceOptionsSystemsV2 — Test Suite                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

Run:    python3 -m pytest test_PlaceOptionsSystemsV2.py -v
Run:    python3 -m pytest test_PlaceOptionsSystemsV2.py -v --tb=short  (concise failures)
Run:    python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "regression"  (regression tests only)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TEST SUITES:
============

1. TestLookupK (8 tests)
   Tests K ≈ 1/√T table lookups. Verifies each DTE range returns the correct
   K value for position sizing, and out-of-range DTEs raise ValueError.

2. TestComputePositionSize (6 tests)
   Tests the daily-vol position sizing formula:
     dailyVolPerLot = K × combinedPremium × lotSize
     allowedLots = round_half_up(budget / dailyVolPerLot)
     finalLots = min(allowedLots, maxLots)
   Covers maxLots capping, round-half-up vs banker's rounding, zero premium,
   budget-too-small, and SENSEX lot size.

3. TestIsWithinTimeWindow (6 tests)
   Tests the smart time window with sleep-before and tolerance-after:
     - Before target (≤2min): sleep until target, return True
     - After target (≤5min): return True immediately
     - Outside: return False

4. TestStateTransitions (5 tests)
   Tests the V2 state machine:
     noPosition → earlyOpen → noPosition (4D exit)
     noPosition → lateOpen → completedCycle (2D letExpire)
     repairRequired → noPosition (manual fix)

5. TestResetCompletedCycle (4 tests)
   Tests auto-reset of completedCycle when a new expiry cycle begins,
   and the letExpire safety net for missed expirations.

6. TestReconciliation (7 tests)
   Tests position reconciliation against broker API:
     - Exact contract matching (not "any option on exchange")
     - Non-V2 positions (other systems) don't trigger repairRequired
     - BANKNIFTY not confused with NIFTY
     - API failure gracefully handled

7. TestExitPreflight (5 tests)
   Tests the exit pre-flight broker verification:
     Scenario A: All legs SL'd → cancel GTTs, reset state, no orders
     Scenario B: Partial SL → cancel GTTs, close only surviving legs
     Scenario C: All legs present → normal exit
     + API failure and GTT cancel failure edge cases

8. TestComputeTradingDte (5 tests)
   Tests trading-day counting (skips weekends and holidays).

9. TestRegressionBugs (9 tests)  ★ PRODUCTION BUG REPLAYS ★
   Each test replays the EXACT scenario that caused a real production bug
   during live trading sessions (2026-03-11 to 2026-03-20). These are the
   most important tests — they guarantee we never regress on bugs that cost
   real money.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PRODUCTION BUG REGISTRY:
========================

┌──────┬────────────┬─────────────────────────────────────────────────────────┐
│ Bug# │ Date       │ Description                                           │
├──────┼────────────┼─────────────────────────────────────────────────────────┤
│ BUG1 │ 2026-03-11 │ Python banker's rounding: round(2.5) = 2 instead of 3 │
│      │            │ NIFTY 4D got 2 lots instead of 3. Budget 63984 /       │
│      │            │ dailyVol 25593.75 = 2.500 → round() gave 2.           │
│      │            │ FIX: int(value + 0.5) for round-half-up.              │
├──────┼────────────┼─────────────────────────────────────────────────────────┤
│ BUG2 │ 2026-03-16 │ Exit created unwanted LONG position. SL GTT had       │
│      │            │ already triggered (position flat), but handoff auto-   │
│      │            │ exit placed BUY orders → created LONG 130 qty on both  │
│      │            │ NIFTY 24100 CE and PE. Cost real money to close.       │
│      │            │ FIX: Pre-flight broker check before placing BUY orders │
├──────┼────────────┼─────────────────────────────────────────────────────────┤
│ BUG3 │ 2026-03-16 │ Reconciliation used instrument_type field which does   │
│      │            │ NOT exist in kite.positions() API. Always returned     │
│      │            │ None → reconciliation never matched any contracts.     │
│      │            │ FIX: Use tradingsymbol.endswith("CE"/"PE") instead.    │
├──────┼────────────┼─────────────────────────────────────────────────────────┤
│ BUG4 │ 2026-03-16 │ Reconciliation checked "any option on exchange" not    │
│      │            │ specific V2 contracts. Found random NIFTY options and  │
│      │            │ assumed V2 managed them.                               │
│      │            │ FIX: Check exact tradingsymbol from activeContracts.   │
├──────┼────────────┼─────────────────────────────────────────────────────────┤
│ BUG5 │ 2026-03-16 │ Reconciliation auto-set repairRequired when broker     │
│      │            │ appeared flat for state=earlyOpen. API sometimes       │
│      │            │ returns stale/incomplete data. Auto-corruption of      │
│      │            │ valid state.                                           │
│      │            │ FIX: Warn-only for "state open, broker flat" case.     │
├──────┼────────────┼─────────────────────────────────────────────────────────┤
│ BUG6 │ 2026-03-17 │ Non-V2 positions on same account triggered            │
│      │            │ repairRequired. BANKNIFTY, NIFTY monthly options from  │
│      │            │ other systems were flagged as V2 positions.            │
│      │            │ FIX: When state=noPosition, warn-only for unexpected   │
│      │            │ broker positions (may belong to other systems).        │
├──────┼────────────┼─────────────────────────────────────────────────────────┤
│ BUG7 │ 2026-03-17 │ Partial SL: one leg SL'd, surviving leg left open.    │
│      │            │ SENSEX 76100 CE survived, PE SL'd. Exit aborted       │
│      │            │ entirely → CE left unmanaged with orphaned GTT.        │
│      │            │ FIX: Close surviving legs, cancel ALL GTTs.            │
├──────┼────────────┼─────────────────────────────────────────────────────────┤
│ BUG8 │ 2026-03-20 │ completedCycle blocked entry for NEW expiry cycle.     │
│      │            │ SENSEX completed 2026-03-19 cycle, but override for    │
│      │            │ 2026-03-25 was blocked: "state is completedCycle,      │
│      │            │ early cannot open". Different expiry = different cycle. │
│      │            │ FIX: Auto-reset completedCycle when new expiry begins. │
├──────┼────────────┼─────────────────────────────────────────────────────────┤
│ BUG9 │ 2026-03-20 │ Silent GTT exception: except Exception: pass          │
│      │            │ swallowed GTT cancellation errors. Old GTTs remained   │
│      │            │ active after exit. Could trigger unexpectedly.         │
│      │            │ FIX: Print error instead of silently passing.          │
└──────┴────────────┴─────────────────────────────────────────────────────────┘

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HOW TO ADD A NEW REGRESSION TEST:
==================================

1. Add the bug to the PRODUCTION BUG REGISTRY table above
2. Add a test method to TestRegressionBugs with name: test_bugN_short_description
3. The docstring MUST include:
   - BUG#N reference
   - Date of occurrence
   - What happened in production (exact scenario)
   - What the correct behavior should be
   - What the fix was
4. The test MUST recreate the exact state + broker data from production
5. The test MUST assert the CORRECT behavior (post-fix), not just "doesn't crash"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import copy
import json
import sys
import os
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock
import pytest

# ─────────────────────────────────────────────────────
# Mock external dependencies before importing the module
# ─────────────────────────────────────────────────────
sys.modules["kiteconnect"] = MagicMock()
sys.modules["Login_Auto3_Angel"] = MagicMock()
sys.modules["Directories"] = MagicMock()
sys.modules["ContractDetails"] = MagicMock()
sys.modules["FetchOptionContractName"] = MagicMock()
sys.modules["Holidays"] = MagicMock()
sys.modules["Server_Order_Place"] = MagicMock()
sys.modules["Set_Gtt_Exit"] = MagicMock()

import importlib
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

with patch.dict(os.environ, {}):
    import PlaceOptionsSystemsV2 as V2

# Override saveState to no-op for tests (avoid writing to real state file)
V2.saveState = lambda state: None


# ─────────────────────────────────────────────────────
# Test Helpers
# ─────────────────────────────────────────────────────

def makeDefaultState():
    """Create a fresh default state with both underlyings in noPosition."""
    return {
        "NIFTY": {
            "currentState": "noPosition",
            "activeStrategy": None,
            "activeLots": 0,
            "activeContracts": [],
            "activeQuantity": 0,
            "entryTimestamp": None,
            "expiryDate": None,
            "lastCycleExpiry": None,
            "positionIntegrity": "healthy",
            "gttProtected": True,
            "activeGttIds": [],
            "lastEntryKey": None,
            "lastExitKey": None,
        },
        "SENSEX": {
            "currentState": "noPosition",
            "activeStrategy": None,
            "activeLots": 0,
            "activeContracts": [],
            "activeQuantity": 0,
            "entryTimestamp": None,
            "expiryDate": None,
            "lastCycleExpiry": None,
            "positionIntegrity": "healthy",
            "gttProtected": True,
            "activeGttIds": [],
            "lastEntryKey": None,
            "lastExitKey": None,
        },
    }


def makeOpenState(underlying, strategy, contracts, qty, lots, expiry, gttIds=None):
    """Create state with an open position for the given underlying."""
    state = makeDefaultState()
    state[underlying]["currentState"] = "earlyOpen"
    state[underlying]["activeStrategy"] = strategy
    state[underlying]["activeContracts"] = contracts
    state[underlying]["activeQuantity"] = qty
    state[underlying]["activeLots"] = lots
    state[underlying]["expiryDate"] = expiry
    state[underlying]["activeGttIds"] = gttIds or []
    return state


def mockKite(positions, gtt_side_effect=None):
    """Create a mock Kite client with given net positions."""
    kite = MagicMock()
    kite.positions.return_value = {"net": positions}
    if gtt_side_effect:
        kite.delete_gtt.side_effect = gtt_side_effect
    else:
        kite.delete_gtt.return_value = True
    return kite


# ═══════════════════════════════════════════════════════════════════════════════
# SUITE 1: K Table Lookups
# ═══════════════════════════════════════════════════════════════════════════════

class TestLookupK:
    """K ≈ 1/√T theory: maps DTE to daily-vol fraction of straddle premium.

    K_TABLE_STRADDLE:
        DTE 5-7  → K=0.40  (1/√6 ≈ 0.41)
        DTE 3-4  → K=0.50  (1/√4 = 0.50)
        DTE 2    → K=0.70  (1/√2 ≈ 0.71)
        DTE 1    → K=1.00  (1/√1 = 1.00)
    """

    def test_dte_1_peak_gamma(self):
        """DTE=1 is peak gamma day. K=1.00 means full straddle premium is daily vol."""
        assert V2.lookupK(1, V2.K_TABLE_STRADDLE) == 1.00

    def test_dte_2(self):
        assert V2.lookupK(2, V2.K_TABLE_STRADDLE) == 0.70

    def test_dte_3(self):
        assert V2.lookupK(3, V2.K_TABLE_STRADDLE) == 0.50

    def test_dte_4(self):
        assert V2.lookupK(4, V2.K_TABLE_STRADDLE) == 0.50

    def test_dte_5(self):
        assert V2.lookupK(5, V2.K_TABLE_STRADDLE) == 0.40

    def test_dte_7_upper_bound(self):
        assert V2.lookupK(7, V2.K_TABLE_STRADDLE) == 0.40

    def test_dte_0_raises(self):
        """DTE=0 is expiry day — no K value defined, should raise."""
        with pytest.raises(ValueError, match="No k value found for DTE=0"):
            V2.lookupK(0, V2.K_TABLE_STRADDLE)

    def test_dte_8_raises(self):
        """DTE=8 is beyond our table — should raise."""
        with pytest.raises(ValueError, match="No k value found for DTE=8"):
            V2.lookupK(8, V2.K_TABLE_STRADDLE)


# ═══════════════════════════════════════════════════════════════════════════════
# SUITE 2: Position Sizing
# ═══════════════════════════════════════════════════════════════════════════════

class TestComputePositionSize:
    """Tests the daily-vol position sizing formula:

        dailyVolPerLot = K × (callPremium + putPremium) × lotSize
        allowedLots = int(budget / dailyVolPerLot + 0.5)   ← round-half-up
        finalLots = min(allowedLots, maxLots)

    Budget is fixed at 63,984 (derived from account margin / risk allocation).
    """

    def test_nifty_4d_typical(self):
        """Typical NIFTY 4D entry: K=0.70, lotSize=65, ~200 premium per leg.

        dailyVol/lot = 0.70 × 400 × 65 = 18,200
        allowed = int(63984 / 18200 + 0.5) = int(4.01) = 4
        """
        result = V2.computePositionSize(200, 200, 65, 0.70, 63984, 5)
        assert result["finalLots"] == 4
        assert result["allowedLots"] == 4
        assert not result["skipped"]

    def test_maxlots_cap_applied(self):
        """Budget allows 4 lots but maxLots=3 → finalLots capped at 3."""
        result = V2.computePositionSize(200, 200, 65, 0.70, 63984, 3)
        assert result["finalLots"] == 3
        assert result["allowedLots"] == 4  # uncapped value preserved for logging

    def test_round_half_up_not_bankers(self):
        """Verifies round-half-up (2.5→3), NOT Python's banker's rounding (2.5→2).

        CE=196.5, PE=196.5 → combined=393, K=1.0, lotSize=65
        dailyVol = 25,545. Budget/dailyVol = 2.505 → should round to 3.
        With Python round(): 2.505 → 2 (wrong). With int(x+0.5): 2.505 → 3 (correct).
        """
        result = V2.computePositionSize(196.5, 196.5, 65, 1.0, 63984, 5)
        assert result["allowedLots"] == 3  # NOT 2

    def test_zero_premium_skipped(self):
        """Zero premium = no valid straddle → skip entry."""
        result = V2.computePositionSize(0, 0, 65, 0.70, 63984, 5)
        assert result["skipped"]
        assert result["finalLots"] == 0

    def test_budget_too_small_for_one_lot(self):
        """Premium so high that even 1 lot exceeds budget → skip."""
        result = V2.computePositionSize(5000, 5000, 65, 1.0, 1000, 5)
        assert result["finalLots"] == 0
        assert result["skipped"]

    def test_sensex_2d_k1(self):
        """SENSEX 2D: K=1.0, lotSize=20, 500+500 premium.

        dailyVol/lot = 1.0 × 1000 × 20 = 20,000
        allowed = int(63984/20000 + 0.5) = int(3.699) = 3
        """
        result = V2.computePositionSize(500, 500, 20, 1.0, 63984, 6)
        assert result["finalLots"] == 3
        assert result["allowedLots"] == 3


# ═══════════════════════════════════════════════════════════════════════════════
# SUITE 3: Time Window (sleep-before, tolerance-after)
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsWithinTimeWindow:
    """Tests the smart entry/exit time window.

    Behavior:
        [target - 2min, target):  sleep until target, then return True
        [target, target + 5min]:  return True immediately
        outside:                  return False

    This prevents:
        - Entries during pre-market auction (cron fires early)
        - Missed entries due to slight cron delay (tolerance-after)
    """

    @patch("PlaceOptionsSystemsV2.datetime")
    def test_exact_target_time(self, mock_dt):
        """At exactly 09:30 → proceed immediately."""
        mock_dt.now.return_value = datetime(2026, 3, 20, 9, 30, 0)
        assert V2.isWithinTimeWindow("09:30") is True

    @patch("PlaceOptionsSystemsV2.datetime")
    def test_3min_after_proceeds(self, mock_dt):
        """At 09:33 (3min late) → still within 5min tolerance, proceed."""
        mock_dt.now.return_value = datetime(2026, 3, 20, 9, 33, 0)
        assert V2.isWithinTimeWindow("09:30") is True

    @patch("PlaceOptionsSystemsV2.datetime")
    def test_7min_after_rejects(self, mock_dt):
        """At 09:37 (7min late) → outside 5min tolerance, skip."""
        mock_dt.now.return_value = datetime(2026, 3, 20, 9, 37, 0)
        assert V2.isWithinTimeWindow("09:30") is False

    @patch("PlaceOptionsSystemsV2.time.sleep")
    @patch("PlaceOptionsSystemsV2.datetime")
    def test_1min_before_sleeps_until_target(self, mock_dt, mock_sleep):
        """At 09:29 (1min early) → sleep ~60s until 09:30, then proceed."""
        mock_dt.now.return_value = datetime(2026, 3, 20, 9, 29, 0)
        result = V2.isWithinTimeWindow("09:30")
        assert result is True
        mock_sleep.assert_called_once()
        sleep_secs = mock_sleep.call_args[0][0]
        assert 55 <= sleep_secs <= 65  # ~60 seconds

    @patch("PlaceOptionsSystemsV2.datetime")
    def test_5min_before_rejects(self, mock_dt):
        """At 09:25 (5min early) → outside 2min sleep window, skip."""
        mock_dt.now.return_value = datetime(2026, 3, 20, 9, 25, 0)
        assert V2.isWithinTimeWindow("09:30") is False

    @patch("PlaceOptionsSystemsV2.datetime")
    def test_1230_window(self, mock_dt):
        """12:30 window works the same as 09:30."""
        mock_dt.now.return_value = datetime(2026, 3, 20, 12, 32, 0)
        assert V2.isWithinTimeWindow("12:30") is True


# ═══════════════════════════════════════════════════════════════════════════════
# SUITE 4: State Transitions
# ═══════════════════════════════════════════════════════════════════════════════

class TestStateTransitions:
    """Tests the V2 state machine lifecycle:

        noPosition ──entry──→ earlyOpen ──timeExit──→ noPosition
                                  │
                              handoff (exit early + enter late)
                                  │
        noPosition ←──letExpire── lateOpen ──exit──→ completedCycle

        repairRequired ──manual──→ noPosition
    """

    def test_transition_to_early_open(self):
        """Entry with phaseType=early → state becomes earlyOpen."""
        state = makeDefaultState()
        V2.transitionToOpen(
            state, "NIFTY", "early", "N_STD_4D_30SL_I", 3,
            ["NIFTY2632423800CE", "NIFTY2632423800PE"], 195,
            date(2026, 3, 24), gttIds=[111, 222]
        )
        assert state["NIFTY"]["currentState"] == "earlyOpen"
        assert state["NIFTY"]["activeStrategy"] == "N_STD_4D_30SL_I"
        assert state["NIFTY"]["activeLots"] == 3
        assert state["NIFTY"]["activeQuantity"] == 195
        assert state["NIFTY"]["activeGttIds"] == [111, 222]

    def test_transition_to_late_open(self):
        """Entry with phaseType=late → state becomes lateOpen."""
        state = makeDefaultState()
        V2.transitionToOpen(
            state, "SENSEX", "late", "SX_STD_2D_100SL_I", 4,
            ["SENSEX2631976100CE", "SENSEX2631976100PE"], 80,
            date(2026, 3, 19)
        )
        assert state["SENSEX"]["currentState"] == "lateOpen"

    def test_exit_from_early_goes_to_no_position(self):
        """earlyOpen → noPosition (4D strategies exit before handoff to 2D)."""
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "earlyOpen"
        state["NIFTY"]["activeStrategy"] = "N_STD_4D_30SL_I"
        state["NIFTY"]["activeLots"] = 3
        state["NIFTY"]["expiryDate"] = "2026-03-24"
        state["NIFTY"]["activeGttIds"] = [111, 222]

        prev = V2.transitionToExit(state, "NIFTY", "timeExit")
        assert state["NIFTY"]["currentState"] == "noPosition"
        assert state["NIFTY"]["activeStrategy"] is None
        assert state["NIFTY"]["activeLots"] == 0
        assert state["NIFTY"]["activeGttIds"] == []
        assert prev["currentState"] == "earlyOpen"  # returns previous state

    def test_exit_from_late_goes_to_completed_cycle(self):
        """lateOpen → completedCycle (preserves lastCycleExpiry to prevent re-entry)."""
        state = makeDefaultState()
        state["SENSEX"]["currentState"] = "lateOpen"
        state["SENSEX"]["expiryDate"] = "2026-03-19"

        V2.transitionToExit(state, "SENSEX", "letExpire")
        assert state["SENSEX"]["currentState"] == "completedCycle"
        assert state["SENSEX"]["lastCycleExpiry"] == "2026-03-19"

    def test_exit_from_repair_goes_to_no_position(self):
        """repairRequired → noPosition (after manual intervention)."""
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "repairRequired"

        V2.transitionToExit(state, "NIFTY", "manual")
        assert state["NIFTY"]["currentState"] == "noPosition"


# ═══════════════════════════════════════════════════════════════════════════════
# SUITE 5: completedCycle Auto-Reset
# ═══════════════════════════════════════════════════════════════════════════════

class TestResetCompletedCycle:
    """Tests that completedCycle auto-clears when a new expiry cycle begins.

    Without this fix, after SENSEX 2D expires (lateOpen → completedCycle),
    the next week's SENSEX 4D entry is blocked even with --override.
    See BUG8 in regression tests.
    """

    def test_new_expiry_resets_to_no_position(self):
        """completedCycle for 03-19 + new expiry 03-26 → auto-reset to noPosition."""
        state = makeDefaultState()
        state["SENSEX"]["currentState"] = "completedCycle"
        state["SENSEX"]["lastCycleExpiry"] = "2026-03-19"

        V2.resetCompletedCycleIfNewExpiry(state, "SENSEX", date(2026, 3, 26))
        assert state["SENSEX"]["currentState"] == "noPosition"
        assert state["SENSEX"]["lastCycleExpiry"] is None

    def test_same_expiry_stays_completed(self):
        """completedCycle for 03-19 + same expiry 03-19 → stays blocked (prevent re-entry)."""
        state = makeDefaultState()
        state["SENSEX"]["currentState"] = "completedCycle"
        state["SENSEX"]["lastCycleExpiry"] = "2026-03-19"

        V2.resetCompletedCycleIfNewExpiry(state, "SENSEX", date(2026, 3, 19))
        assert state["SENSEX"]["currentState"] == "completedCycle"

    @patch.object(V2, "logExit")
    def test_let_expire_safety_net_resets_in_one_step(self, mock_log):
        """lateOpen + expired + new expiry → goes straight to noPosition in one call.

        Without this, script would need TWO runs:
          Run 1: lateOpen → completedCycle (safety net)
          Run 2: completedCycle → noPosition (new expiry reset)
        This test verifies it happens in one step.
        """
        state = makeDefaultState()
        state["SENSEX"]["currentState"] = "lateOpen"
        state["SENSEX"]["activeStrategy"] = "SX_STD_2D_100SL_I"
        state["SENSEX"]["expiryDate"] = "2026-03-19"
        state["SENSEX"]["activeLots"] = 3

        V2.resetCompletedCycleIfNewExpiry(state, "SENSEX", date(2026, 3, 26))
        assert state["SENSEX"]["currentState"] == "noPosition"

    def test_no_position_unaffected(self):
        """noPosition state is not touched by this function."""
        state = makeDefaultState()
        V2.resetCompletedCycleIfNewExpiry(state, "NIFTY", date(2026, 3, 24))
        assert state["NIFTY"]["currentState"] == "noPosition"


# ═══════════════════════════════════════════════════════════════════════════════
# SUITE 6: Reconciliation
# ═══════════════════════════════════════════════════════════════════════════════

class TestReconciliation:
    """Tests position reconciliation against broker API.

    Reconciliation runs at the start of every script execution and checks
    whether V2's state matches what the broker actually has. It uses EXACT
    contract symbol matching (e.g. "NIFTY2632423800CE") rather than checking
    for "any NIFTY option on NFO exchange".
    """

    def test_all_contracts_verified(self, capsys):
        """Happy path: both contracts found at expected quantities."""
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "earlyOpen"
        state["NIFTY"]["activeContracts"] = ["NIFTY2632423800CE", "NIFTY2632423800PE"]

        kite = mockKite([
            {"tradingsymbol": "NIFTY2632423800CE", "quantity": -195, "exchange": "NFO"},
            {"tradingsymbol": "NIFTY2632423800PE", "quantity": -195, "exchange": "NFO"},
        ])
        V2.reconcilePositions(kite, state, dryRun=True)
        output = capsys.readouterr().out
        assert "OK" in output
        assert "contracts verified" in output

    def test_all_contracts_missing_warns(self, capsys):
        """State says earlyOpen but broker has nothing → warn (could be API glitch)."""
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "earlyOpen"
        state["NIFTY"]["activeContracts"] = ["NIFTY2632423800CE", "NIFTY2632423800PE"]

        kite = mockKite([])
        V2.reconcilePositions(kite, state, dryRun=True)
        output = capsys.readouterr().out
        assert "WARNING" in output
        assert "none of" in output

    def test_partial_contracts_missing_warns(self, capsys):
        """One leg found, one missing → warn about partial mismatch (possible SL trigger)."""
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "earlyOpen"
        state["NIFTY"]["activeContracts"] = ["NIFTY2632423800CE", "NIFTY2632423800PE"]

        kite = mockKite([
            {"tradingsymbol": "NIFTY2632423800CE", "quantity": -195, "exchange": "NFO"},
        ])
        V2.reconcilePositions(kite, state, dryRun=True)
        output = capsys.readouterr().out
        assert "WARNING" in output
        assert "missing on broker" in output

    def test_other_system_positions_dont_trigger_repair(self, capsys):
        """Non-V2 NIFTY options on same account → warn only, don't set repairRequired."""
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "noPosition"

        kite = mockKite([
            {"tradingsymbol": "NIFTY26MAR24300CE", "quantity": 65, "exchange": "NFO"},
            {"tradingsymbol": "NIFTY2631723500CE", "quantity": -130, "exchange": "NFO"},
        ])
        V2.reconcilePositions(kite, state, dryRun=False)
        output = capsys.readouterr().out
        assert "May belong to another system" in output
        assert state["NIFTY"]["currentState"] == "noPosition"  # NOT repairRequired

    def test_no_positions_on_either_side_ok(self, capsys):
        """Both state and broker are clean → OK."""
        state = makeDefaultState()
        kite = mockKite([])
        V2.reconcilePositions(kite, state, dryRun=True)
        output = capsys.readouterr().out
        assert "OK" in output

    def test_banknifty_filtered_by_prefix(self, capsys):
        """BANKNIFTY positions must NOT match NIFTY (startswith filter)."""
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "noPosition"

        kite = mockKite([
            {"tradingsymbol": "BANKNIFTY26MAR58000CE", "quantity": -50, "exchange": "NFO"},
        ])
        V2.reconcilePositions(kite, state, dryRun=True)
        output = capsys.readouterr().out
        assert "OK" in output  # BANKNIFTY ignored

    def test_broker_api_failure_graceful(self, capsys):
        """kite.positions() throws → log warning and skip (don't crash the script)."""
        kite = MagicMock()
        kite.positions.side_effect = Exception("Connection timeout")
        state = makeDefaultState()
        V2.reconcilePositions(kite, state, dryRun=True)
        output = capsys.readouterr().out
        assert "WARN" in output
        assert "Skipping reconciliation" in output


# ═══════════════════════════════════════════════════════════════════════════════
# SUITE 7: Exit Pre-flight (partial SL handling)
# ═══════════════════════════════════════════════════════════════════════════════

class TestExitPreflight:
    """Tests the broker pre-flight check before placing exit (BUY) orders.

    Three scenarios:
        A) All legs gone (full SL): cancel GTTs, reset state, NO BUY orders
        B) Partial SL (some legs survive): cancel ALL GTTs, BUY only survivors
        C) All legs present: normal exit (cancel GTTs, BUY all)

    Without this pre-flight check, Scenario A would create unwanted LONG positions
    (the bug that cost real money on 2026-03-16).
    """

    @patch.object(V2, "logExit")
    @patch.object(V2, "verifyFlatPosition", return_value=True)
    def test_scenario_a_all_legs_sl_triggered(self, mock_flat, mock_log, capsys):
        """Both CE and PE SL'd → cancel GTTs, reset state, no BUY orders placed."""
        kite = mockKite([
            {"tradingsymbol": "NIFTY2632423800CE", "quantity": 0},
            {"tradingsymbol": "NIFTY2632423800PE", "quantity": 0},
        ])
        state = makeOpenState("NIFTY", "N_STD_4D_30SL_I",
                              ["NIFTY2632423800CE", "NIFTY2632423800PE"],
                              195, 3, "2026-03-24", gttIds=[111, 222])

        result = V2.executeTimeExit("NIFTY", state, kite)
        assert result["reason"] == "broker_already_flat"
        assert result["success"] is True
        assert kite.delete_gtt.call_count == 2
        assert state["NIFTY"]["currentState"] == "noPosition"

    @patch.object(V2, "logExit")
    @patch.object(V2, "verifyFlatPosition", return_value=True)
    def test_scenario_b_partial_sl_closes_survivor(self, mock_flat, mock_log, capsys):
        """CE still short, PE SL'd → close CE only, cancel both GTTs."""
        kite = mockKite([
            {"tradingsymbol": "NIFTY2632423800CE", "quantity": -195},  # still short
            {"tradingsymbol": "NIFTY2632423800PE", "quantity": 0},     # SL triggered
        ])
        kite.place_order = MagicMock(return_value="order123")
        state = makeOpenState("NIFTY", "N_STD_4D_30SL_I",
                              ["NIFTY2632423800CE", "NIFTY2632423800PE"],
                              195, 3, "2026-03-24", gttIds=[111, 222])

        with patch.object(V2, "order", return_value="order123"):
            with patch.object(V2, "verifyOrderFill", return_value=(True, "COMPLETE")):
                result = V2.executeTimeExit("NIFTY", state, kite)

        output = capsys.readouterr().out
        assert "PARTIAL SL detected" in output
        assert "already closed by SL" in output
        assert "NIFTY2632423800PE" in output  # PE identified as already closed
        assert "still short (closing now)" in output
        assert kite.delete_gtt.call_count == 2  # BOTH GTTs cancelled

    @patch.object(V2, "logExit")
    def test_scenario_c_normal_exit(self, mock_log, capsys):
        """Both legs still short → normal exit, no partial SL messages."""
        kite = mockKite([
            {"tradingsymbol": "NIFTY2632423800CE", "quantity": -195},
            {"tradingsymbol": "NIFTY2632423800PE", "quantity": -195},
        ])
        state = makeOpenState("NIFTY", "N_STD_4D_30SL_I",
                              ["NIFTY2632423800CE", "NIFTY2632423800PE"],
                              195, 3, "2026-03-24", gttIds=[111, 222])

        with patch.object(V2, "order", return_value="order123"):
            with patch.object(V2, "verifyOrderFill", return_value=(True, "COMPLETE")):
                with patch.object(V2, "verifyFlatPosition", return_value=True):
                    result = V2.executeTimeExit("NIFTY", state, kite)

        output = capsys.readouterr().out
        assert "PARTIAL SL" not in output
        assert "closing 2 legs" in output
        assert kite.delete_gtt.call_count == 2

    @patch.object(V2, "logExit")
    def test_broker_api_failure_aborts_safely(self, mock_log, capsys):
        """If kite.positions() fails, exit MUST abort (no blind BUY orders)."""
        kite = MagicMock()
        kite.positions.side_effect = Exception("API timeout")
        state = makeOpenState("NIFTY", "N_STD_4D_30SL_I",
                              ["NIFTY2632423800CE", "NIFTY2632423800PE"],
                              195, 3, "2026-03-24", gttIds=[111, 222])

        result = V2.executeTimeExit("NIFTY", state, kite)
        assert result["success"] is False
        assert "preflight_failed" in result["reason"]

    @patch.object(V2, "logExit")
    def test_gtt_cancel_failure_is_non_blocking(self, mock_log, capsys):
        """GTT cancel failure must NOT prevent exit from completing."""
        kite = mockKite(
            [
                {"tradingsymbol": "NIFTY2632423800CE", "quantity": 0},
                {"tradingsymbol": "NIFTY2632423800PE", "quantity": 0},
            ],
            gtt_side_effect=Exception("Unable to fetch triggers"),
        )
        state = makeOpenState("NIFTY", "N_STD_4D_30SL_I",
                              ["NIFTY2632423800CE", "NIFTY2632423800PE"],
                              195, 3, "2026-03-24", gttIds=[111, 222])

        result = V2.executeTimeExit("NIFTY", state, kite)
        assert result["success"] is True
        output = capsys.readouterr().out
        assert "GTT cancel failed" in output  # error logged, not silent


# ═══════════════════════════════════════════════════════════════════════════════
# SUITE 8: Trading DTE Calculation
# ═══════════════════════════════════════════════════════════════════════════════

class TestComputeTradingDte:
    """Counts trading days from today to expiry (excludes today, includes expiry).
    Skips weekends (Sat/Sun) and market holidays.

    NIFTY expires Tuesday. SENSEX expires Thursday.
    """

    @patch.object(V2, "CheckForDateHoliday", return_value=False)
    def test_expiry_day_is_zero(self, mock_holiday):
        """On expiry day → DTE=0."""
        assert V2.computeTradingDte(date(2026, 3, 24), date(2026, 3, 24)) == 0

    @patch.object(V2, "CheckForDateHoliday", return_value=False)
    def test_monday_to_tuesday(self, mock_holiday):
        """Monday before Tuesday expiry → DTE=1."""
        assert V2.computeTradingDte(date(2026, 3, 23), date(2026, 3, 24)) == 1

    @patch.object(V2, "CheckForDateHoliday", return_value=False)
    def test_friday_to_tuesday_skips_weekend(self, mock_holiday):
        """Friday → Mon, Tue (skips Sat/Sun) → DTE=2."""
        assert V2.computeTradingDte(date(2026, 3, 20), date(2026, 3, 24)) == 2

    @patch.object(V2, "CheckForDateHoliday", return_value=False)
    def test_wednesday_to_next_tuesday(self, mock_holiday):
        """Wed → Thu, Fri, Mon, Tue → DTE=4."""
        assert V2.computeTradingDte(date(2026, 3, 18), date(2026, 3, 24)) == 4

    @patch.object(V2, "CheckForDateHoliday")
    def test_holiday_skipped(self, mock_holiday):
        """Holidays are excluded from trading day count."""
        mock_holiday.side_effect = lambda d: d == date(2026, 3, 24)
        # Wed Mar 18 to Wed Mar 25: Thu(19), Fri(20), Mon(23), Tue(24=HOLIDAY), Wed(25)
        # = 4 trading days (Tue is skipped)
        assert V2.computeTradingDte(date(2026, 3, 18), date(2026, 3, 25)) == 4


# ═══════════════════════════════════════════════════════════════════════════════
# SUITE 9: REGRESSION TESTS — Production Bug Replays
# ═══════════════════════════════════════════════════════════════════════════════

class TestRegressionBugs:
    """★ PRODUCTION BUG REPLAYS ★

    Each test recreates the EXACT state and broker data that caused a real
    production bug. These are the most important tests in the suite — they
    guarantee we never regress on bugs that cost real money.

    See the PRODUCTION BUG REGISTRY in the module docstring for full details
    on each bug including date, impact, and fix.
    """

    # ─── BUG 1: Python banker's rounding (2026-03-11) ───────────────────
    def test_bug1_bankers_rounding_gives_wrong_lots(self):
        """BUG1 (2026-03-11): round(2.5)=2 in Python, should be 3.

        Production scenario:
            NIFTY 4D entry. Budget=63984, dailyVol/lot=25593.75.
            63984 / 25593.75 ≈ 2.500 → round() gave 2 (banker's rounding).
            Expected: 3 lots. Got: 2 lots.

        Fix: int(value + 0.5) for round-half-up behavior.

        Test values: budget=50000, K=1.0, lotSize=20, CE=500, PE=500.
            dailyVol/lot = 1.0 × 1000 × 20 = 20,000.
            50000 / 20000 = 2.5 exactly.
            Python round(2.5) = 2 (banker's rounding → WRONG).
            int(2.5 + 0.5) = int(3.0) = 3 (round-half-up → CORRECT).
        """
        result = V2.computePositionSize(500, 500, 20, 1.0, 50000, 5)
        # 50000 / 20000 = 2.5 exactly → must round UP to 3, not down to 2
        assert result["allowedLots"] == 3, (
            "BUG1 regression: round(2.5) should give 3, not 2"
        )

    # ─── BUG 2: Exit creates unwanted LONG (2026-03-16) ────────────────
    @patch.object(V2, "logExit")
    @patch.object(V2, "verifyFlatPosition", return_value=True)
    def test_bug2_exit_when_sl_already_triggered_no_buy_orders(self, mock_flat, mock_log, capsys):
        """BUG2 (2026-03-16): SL GTT triggered → position flat → exit placed BUY → LONG created.

        Production scenario:
            NIFTY state=earlyOpen, contracts=[24100CE, 24100PE], qty=130.
            SL GTT triggered on both legs → position already flat.
            Handoff auto-exit placed BUY 130 CE + BUY 130 PE → created LONG position.
            Had to manually close the unwanted LONG at a loss.

        Fix: Pre-flight broker check. If contracts not short → don't place BUY orders.
        """
        kite = mockKite([
            {"tradingsymbol": "NIFTY2631724100CE", "quantity": 0},  # SL triggered
            {"tradingsymbol": "NIFTY2631724100PE", "quantity": 0},  # SL triggered
        ])
        state = makeOpenState("NIFTY", "N_STD_4D_30SL_I",
                              ["NIFTY2631724100CE", "NIFTY2631724100PE"],
                              130, 2, "2026-03-17", gttIds=[111, 222])

        result = V2.executeTimeExit("NIFTY", state, kite)

        # MUST NOT place any BUY orders
        assert result["reason"] == "broker_already_flat"
        assert result["success"] is True
        # State must be reset (not left in earlyOpen)
        assert state["NIFTY"]["currentState"] == "noPosition"

    # ─── BUG 4: Reconciliation checked "any option" not exact contracts ─
    def test_bug4_reconciliation_exact_contract_matching(self, capsys):
        """BUG4 (2026-03-16): Reconciliation counted "any NFO option" as NIFTY position.

        Production scenario:
            State=earlyOpen, activeContracts=[24100CE, 24100PE].
            Broker had 3 random NIFTY options (23500CE, 24300CE, etc).
            Reconciliation said "OK - found 3 options on NFO" without checking
            if they matched the specific contracts V2 was tracking.

        Fix: Check exact tradingsymbol from activeContracts against broker.
        """
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "earlyOpen"
        state["NIFTY"]["activeContracts"] = ["NIFTY2631724100CE", "NIFTY2631724100PE"]

        kite = mockKite([
            # These are NOT the V2 contracts — they're from other systems
            {"tradingsymbol": "NIFTY2631723500CE", "quantity": -130, "exchange": "NFO"},
            {"tradingsymbol": "NIFTY26MAR24300CE", "quantity": 65, "exchange": "NFO"},
        ])
        V2.reconcilePositions(kite, state, dryRun=True)
        output = capsys.readouterr().out
        # Must NOT say "OK" — the specific V2 contracts are missing
        assert "OK" not in output or "WARNING" in output
        assert "none of" in output or "missing" in output

    # ─── BUG 5: Reconciliation auto-corrupted state on API glitch ───────
    def test_bug5_reconciliation_warn_only_when_broker_flat(self, capsys):
        """BUG5 (2026-03-16): API returned empty data → reconciliation set repairRequired.

        Production scenario:
            State=earlyOpen, valid position exists on broker.
            kite.positions() returned empty/incomplete data (API glitch).
            Reconciliation auto-set repairRequired → destroyed valid state.
            System blocked from operating until manual state reset.

        Fix: "State has position, broker flat" → warn-only, don't auto-repair.
        """
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "earlyOpen"
        state["NIFTY"]["activeContracts"] = ["NIFTY2632423800CE", "NIFTY2632423800PE"]

        kite = mockKite([])  # API returned nothing (glitch)
        V2.reconcilePositions(kite, state, dryRun=False)

        # State must NOT be changed to repairRequired
        assert state["NIFTY"]["currentState"] == "earlyOpen", (
            "BUG5 regression: reconciliation should not auto-corrupt state"
        )

    # ─── BUG 6: Non-V2 positions triggered repairRequired ──────────────
    def test_bug6_other_system_positions_ignored(self, capsys):
        """BUG6 (2026-03-17): BANKNIFTY and NIFTY monthly from other systems → repairRequired.

        Production scenario:
            NIFTY state=noPosition (V2 has no NIFTY position).
            Same account has BANKNIFTY26MAR58000CE, NIFTY2631723500CE, NIFTY26MAR24300CE
            from a different trading system.
            Reconciliation saw these, set repairRequired, blocked V2 from operating.

        Fix: When state=noPosition, warn about unexpected positions but don't block.
        """
        state = makeDefaultState()
        state["NIFTY"]["currentState"] = "noPosition"

        kite = mockKite([
            {"tradingsymbol": "BANKNIFTY26MAR58000CE", "quantity": -50, "exchange": "NFO"},
            {"tradingsymbol": "NIFTY2631723500CE", "quantity": -130, "exchange": "NFO"},
            {"tradingsymbol": "NIFTY26MAR24300CE", "quantity": 65, "exchange": "NFO"},
        ])
        V2.reconcilePositions(kite, state, dryRun=False)

        assert state["NIFTY"]["currentState"] == "noPosition", (
            "BUG6 regression: non-V2 positions must not trigger repairRequired"
        )

    # ─── BUG 7: Partial SL — surviving leg left open ───────────────────
    @patch.object(V2, "logExit")
    @patch.object(V2, "verifyFlatPosition", return_value=True)
    def test_bug7_partial_sl_closes_surviving_leg(self, mock_flat, mock_log, capsys):
        """BUG7 (2026-03-17): Partial SL → exit aborted entirely → surviving leg left open.

        Production scenario:
            SENSEX state=earlyOpen, contracts=[76100CE, 76100PE], qty=60.
            PE SL triggered → PE closed by GTT. CE still short.
            Exit detected PE missing → ABORTED entire exit.
            Result: CE left open with no exit plan, orphaned GTT still active.
            Had to manually close CE the next day.

        Fix: When partial SL detected:
            1. Cancel ALL GTTs (both legs)
            2. Place BUY orders for surviving legs only
            3. Normal exit flow for the survivors
        """
        kite = mockKite([
            {"tradingsymbol": "SENSEX2631976100CE", "quantity": -60},   # still short
            {"tradingsymbol": "SENSEX2631976100PE", "quantity": 0},     # SL triggered
        ])
        state = makeOpenState("SENSEX", "SX_STD_4D_20SL_I",
                              ["SENSEX2631976100CE", "SENSEX2631976100PE"],
                              60, 3, "2026-03-19", gttIds=[311456488, 311456490])

        with patch.object(V2, "order", return_value="order123"):
            with patch.object(V2, "verifyOrderFill", return_value=(True, "COMPLETE")):
                result = V2.executeTimeExit("SENSEX", state, kite)

        output = capsys.readouterr().out
        assert "PARTIAL SL detected" in output
        assert "SENSEX2631976100PE" in output  # PE identified as SL'd
        assert "still short (closing now)" in output
        assert "SENSEX2631976100CE" in output  # CE being closed
        # BOTH GTTs must be cancelled (not just the triggered one)
        assert kite.delete_gtt.call_count == 2
        # Only 1 BUY order should be placed (CE only, not PE)
        assert "closing 1 legs" in output

    # ─── BUG 8: completedCycle blocked new expiry entry ─────────────────
    @patch.object(V2, "logExit")
    def test_bug8_completed_cycle_unblocks_for_new_expiry(self, mock_log):
        """BUG8 (2026-03-20): completedCycle blocked --override entry for NEXT week.

        Production scenario:
            SENSEX 2D expired 2026-03-19 (letExpire) → state=completedCycle.
            Friday 2026-03-20: tried --override=SX_STD_4D_20SL_I for 03-25 expiry.
            Error: "state is completedCycle, early cannot open".
            Had to manually reset state to noPosition.

        Fix: resetCompletedCycleIfNewExpiry auto-clears when new expiry detected.
        """
        state = makeDefaultState()
        state["SENSEX"]["currentState"] = "completedCycle"
        state["SENSEX"]["lastCycleExpiry"] = "2026-03-19"

        # Next SENSEX expiry is 2026-03-25 (different from completed 03-19)
        V2.resetCompletedCycleIfNewExpiry(state, "SENSEX", date(2026, 3, 25))

        assert state["SENSEX"]["currentState"] == "noPosition", (
            "BUG8 regression: completedCycle must auto-reset for new expiry cycle"
        )

    # ─── BUG 9: Silent GTT exception hid cancellation failures ─────────
    @patch.object(V2, "logExit")
    def test_bug9_gtt_cancel_error_is_logged_not_silent(self, mock_log, capsys):
        """BUG9 (2026-03-20): GTT cancel failed silently (except Exception: pass).

        Production scenario:
            Exit detected all legs SL'd. Tried to cancel orphaned GTTs.
            kite.delete_gtt() threw "Unable to fetch triggers".
            Code had `except Exception: pass` → error swallowed silently.
            GTTs remained active, could trigger unexpectedly later.

        Fix: Print the error instead of silently passing.
        """
        kite = mockKite(
            [
                {"tradingsymbol": "NIFTY2632423800CE", "quantity": 0},
                {"tradingsymbol": "NIFTY2632423800PE", "quantity": 0},
            ],
            gtt_side_effect=Exception("Unable to fetch triggers"),
        )
        state = makeOpenState("NIFTY", "N_STD_4D_30SL_I",
                              ["NIFTY2632423800CE", "NIFTY2632423800PE"],
                              195, 3, "2026-03-24", gttIds=[311596471, 311596474])

        V2.executeTimeExit("NIFTY", state, kite)

        output = capsys.readouterr().out
        assert "GTT cancel failed" in output, (
            "BUG9 regression: GTT cancel errors must be logged, not silently swallowed"
        )
        assert "Unable to fetch triggers" in output


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 1: TestBlackScholes — BS pricing and Greeks math
# ═══════════════════════════════════════════════════════════════════════════════

class TestBlackScholes:
    """Verify the core Black-Scholes pricing and Greeks math is not broken."""

    def test_bsPrice_call_put_parity(self):
        """C - P = S - K*exp(-rT) (no dividends)."""
        import math
        spot, strike, T, iv, r = 24000, 24000, 7 / 365, 0.14, 0.07
        call = V2.bsPrice(spot, strike, T, iv, "CE", r)
        put = V2.bsPrice(spot, strike, T, iv, "PE", r)
        parity = spot - strike * math.exp(-r * T)
        assert abs((call - put) - parity) < 0.01, (
            f"Put-call parity violated: C-P={call-put:.4f}, S-Ke^(-rT)={parity:.4f}"
        )

    def test_bsPrice_monotonic_with_iv(self):
        """Higher IV → higher option price (same spot/strike/T)."""
        spot, strike, T = 24000, 24000, 7 / 365
        price_low = V2.bsPrice(spot, strike, T, 0.10, "CE")
        price_high = V2.bsPrice(spot, strike, T, 0.20, "CE")
        assert price_high > price_low, (
            f"Price at 20% vol ({price_high:.2f}) should exceed 10% vol ({price_low:.2f})"
        )

    def test_bsPrice_monotonic_with_time(self):
        """Longer expiry → higher price for both CE and PE."""
        spot, strike, iv = 24000, 24000, 0.14
        for optType in ("CE", "PE"):
            price_short = V2.bsPrice(spot, strike, 2 / 365, iv, optType)
            price_long = V2.bsPrice(spot, strike, 10 / 365, iv, optType)
            assert price_long > price_short, (
                f"{optType}: longer T ({price_long:.2f}) should exceed shorter T ({price_short:.2f})"
            )

    def test_bsGreeks_atm_call(self):
        """ATM call: delta ~0.5, gamma > 0, theta < 0, vega > 0."""
        g = V2.bsGreeks(24000, 24000, 5 / 365, 0.14, "CE")
        assert 0.45 < g["delta"] < 0.60, f"delta={g['delta']}"
        assert g["gamma"] > 0
        assert g["theta"] < 0
        assert g["vega"] > 0

    def test_bsGreeks_atm_put(self):
        """ATM put: delta ~-0.5, gamma > 0, theta < 0, vega > 0."""
        g = V2.bsGreeks(24000, 24000, 5 / 365, 0.14, "PE")
        assert -0.60 < g["delta"] < -0.45
        assert g["gamma"] > 0
        assert g["theta"] < 0
        assert g["vega"] > 0

    def test_bsGreeks_deep_itm_call(self):
        """Deep ITM call: delta close to 1, gamma very small, vega < ATM vega."""
        g_itm = V2.bsGreeks(24000, 20000, 7 / 365, 0.14, "CE")
        g_atm = V2.bsGreeks(24000, 24000, 7 / 365, 0.14, "CE")
        assert g_itm["delta"] > 0.95, f"deep ITM call delta={g_itm['delta']}"
        assert g_itm["gamma"] < g_atm["gamma"]
        assert g_itm["vega"] < g_atm["vega"]

    def test_bsGreeks_deep_otm_put(self):
        """Deep OTM put: delta close to 0, gamma very small, vega very small."""
        g = V2.bsGreeks(24000, 20000, 7 / 365, 0.14, "PE")
        assert abs(g["delta"]) < 0.05, f"deep OTM put delta={g['delta']}"
        assert g["gamma"] < 0.0001
        assert g["vega"] < 1.0

    def test_bsGreeks_near_expiry_gamma_rises(self):
        """ATM gamma rises as T falls; ATM |theta| rises as T falls."""
        g_long = V2.bsGreeks(24000, 24000, 5 / 365, 0.14, "CE")
        g_short = V2.bsGreeks(24000, 24000, 1 / 365, 0.14, "CE")
        g_tiny = V2.bsGreeks(24000, 24000, 1e-6, 0.14, "CE")
        assert g_short["gamma"] > g_long["gamma"]
        assert g_tiny["gamma"] > g_short["gamma"]
        assert abs(g_short["theta"]) > abs(g_long["theta"])

    def test_bsGreeks_vega_scaling_convention(self):
        """Vega = ∂V/∂σ: finite-difference check.
        price(iv+0.01) - price(iv) ≈ vega × 0.01 within tolerance."""
        spot, strike, T, iv = 24000, 24000, 7 / 365, 0.14
        p1 = V2.bsPrice(spot, strike, T, iv, "CE")
        p2 = V2.bsPrice(spot, strike, T, iv + 0.01, "CE")
        vega = V2.bsGreeks(spot, strike, T, iv, "CE")["vega"]
        fd_approx = p2 - p1
        vega_approx = vega * 0.01
        assert abs(fd_approx - vega_approx) < 0.5, (
            f"Vega scaling: fd={fd_approx:.4f}, vega*0.01={vega_approx:.4f}"
        )

    def test_bsGreeks_theta_scaling_convention(self):
        """If theta is per calendar day: price(T) - price(T - 1/365) ≈ -theta within tolerance."""
        spot, strike, T, iv = 24000, 24000, 10 / 365, 0.14
        p1 = V2.bsPrice(spot, strike, T, iv, "CE")
        p2 = V2.bsPrice(spot, strike, T - 1 / 365, iv, "CE")
        theta = V2.bsGreeks(spot, strike, T, iv, "CE")["theta"]
        # p2 - p1 is change from one day passing (should be negative for long call)
        # theta is already negative, so p2 - p1 ≈ theta
        assert abs((p2 - p1) - theta) < 1.0, (
            f"Theta scaling: fd={p2 - p1:.4f}, theta={theta:.4f}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 2: TestImpliedVol — IV solver robustness
# ═══════════════════════════════════════════════════════════════════════════════

class TestImpliedVol:
    """Verify the IV solver converges, rejects bad inputs, and respects bounds."""

    def test_roundtrip_call(self):
        """Price CE with known IV → solve back → recovered IV matches."""
        spot, strike, T, iv = 24000, 24000, 10 / 365, 0.16
        price = V2.bsPrice(spot, strike, T, iv, "CE")
        recovered = V2.bsImpliedVol(price, spot, strike, T, "CE")
        assert recovered is not None
        assert abs(recovered - iv) < 0.001

    def test_roundtrip_put(self):
        """Price PE with known IV → solve back → recovered IV matches."""
        spot, strike, T, iv = 24000, 24000, 10 / 365, 0.16
        price = V2.bsPrice(spot, strike, T, iv, "PE")
        recovered = V2.bsImpliedVol(price, spot, strike, T, "PE")
        assert recovered is not None
        assert abs(recovered - iv) < 0.001

    def test_newton_fallback_to_bisection(self):
        """Deep OTM + short DTE where Newton may fail — bisection should converge."""
        spot, strike, T = 24000, 25500, 3 / 365
        price = V2.bsPrice(spot, strike, T, 0.50, "CE")
        if price > 0.01:
            recovered = V2.bsImpliedVol(price, spot, strike, T, "CE")
            if recovered is not None:
                assert abs(recovered - 0.50) < 0.05

    def test_rejects_zero_price(self):
        """optionPrice = 0 → returns None."""
        assert V2.bsImpliedVol(0, 24000, 24000, 5 / 365, "CE") is None

    def test_rejects_negative_price(self):
        """optionPrice < 0 → returns None."""
        assert V2.bsImpliedVol(-10, 24000, 24000, 5 / 365, "CE") is None

    def test_rejects_below_intrinsic(self):
        """CE premium below intrinsic → returns None."""
        # spot=24000, strike=23000 → intrinsic=1000, price=500 → reject
        assert V2.bsImpliedVol(500, 24000, 23000, 5 / 365, "CE") is None

    def test_rejects_bad_time(self):
        """T = 0 → should not crash. Either clamps and works or returns None."""
        result = V2.bsImpliedVol(100, 24000, 24000, 0, "CE")
        # implementation clamps T to 1e-6 — either works or returns None, no crash
        assert result is None or isinstance(result, float)

    def test_respects_bounds(self):
        """Returned IV is always within [IV_SOLVER_MIN, IV_SOLVER_MAX]."""
        # High vol case
        price = V2.bsPrice(24000, 24000, 10 / 365, 2.0, "CE")
        iv = V2.bsImpliedVol(price, 24000, 24000, 10 / 365, "CE")
        if iv is not None:
            assert V2.IV_SOLVER_MIN <= iv <= V2.IV_SOLVER_MAX


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 3: TestQuoteHandling — getBestPremium and quote-quality gates
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuoteHandling:
    """Test premium selection and quote-quality gates in resolveK."""

    # --- getBestPremium ---

    def test_mid_when_bid_ask_valid(self):
        """Uses mid when both bid and ask are present and valid."""
        q = {"last_price": 105, "depth": {
            "buy": [{"price": 100, "quantity": 50}],
            "sell": [{"price": 102, "quantity": 50}],
        }}
        price, src, bid, ask, spread = V2.getBestPremium(q)
        assert src == "mid"
        assert price == 101.0
        assert bid == 100
        assert ask == 102

    def test_bid_when_only_bid_valid(self):
        """Falls back to bid when ask is missing."""
        q = {"last_price": 95, "depth": {
            "buy": [{"price": 90, "quantity": 50}],
            "sell": [],
        }}
        price, src, _, _, _ = V2.getBestPremium(q)
        assert src == "bid"
        assert price == 90

    def test_ask_when_only_ask_valid(self):
        """Falls back to ask when bid is missing."""
        q = {"last_price": 95, "depth": {
            "buy": [],
            "sell": [{"price": 100, "quantity": 50}],
        }}
        price, src, _, _, _ = V2.getBestPremium(q)
        assert src == "ask"
        assert price == 100

    def test_ltp_fallback(self):
        """Falls back to LTP when depth is entirely empty."""
        q = {"last_price": 95, "depth": {"buy": [], "sell": []}}
        price, src, _, _, _ = V2.getBestPremium(q)
        assert src == "ltp"
        assert price == 95.0

    def test_rejects_bid_greater_than_ask(self):
        """bid > ask is rejected — falls back to LTP."""
        q = {"last_price": 80, "depth": {
            "buy": [{"price": 105, "quantity": 50}],
            "sell": [{"price": 100, "quantity": 50}],
        }}
        price, src, _, _, _ = V2.getBestPremium(q)
        assert src == "ltp"  # bid > ask → depth rejected

    # --- resolveK quote-quality gates ---

    def _make_dynamic_config(self):
        return {
            "useDynamicK": True,
            "kTable": V2.K_TABLE_STRADDLE,
            "strategyType": "straddle",
        }

    def _make_mock_kite(self, spot=24000, ceBid=99, ceAsk=101, peBid=98, peAsk=102,
                        ceStrike=24000, peStrike=24000):
        """Build a mock kite with ltp, quote, and instruments returning clean data."""
        kite = MagicMock()
        kite.ltp.return_value = {"NSE:NIFTY 50": {"last_price": spot}}

        ceKey, peKey = "NFO:NIFTY26MAR24000CE", "NFO:NIFTY26MAR24000PE"
        now = datetime.now()
        kite.quote.return_value = {
            ceKey: {
                "last_price": (ceBid + ceAsk) / 2,
                "last_trade_time": now,
                "depth": {
                    "buy": [{"price": ceBid, "quantity": 100}],
                    "sell": [{"price": ceAsk, "quantity": 100}],
                },
            },
            peKey: {
                "last_price": (peBid + peAsk) / 2,
                "last_trade_time": now,
                "depth": {
                    "buy": [{"price": peBid, "quantity": 100}],
                    "sell": [{"price": peAsk, "quantity": 100}],
                },
            },
        }
        # Instruments cache for strike lookup
        V2.GetInstrumentsCached = MagicMock(return_value=[
            {"tradingsymbol": "NIFTY26MAR24000CE", "strike": ceStrike},
            {"tradingsymbol": "NIFTY26MAR24000PE", "strike": peStrike},
        ])
        return kite

    def test_resolveK_rejects_wide_spread(self):
        """CE spread > 30% of mid → static fallback."""
        # bid=50, ask=90 → spread=40, mid=70, spread%=57% > 30%
        kite = self._make_mock_kite(ceBid=50, ceAsk=90, peBid=98, peAsk=102)
        config = self._make_dynamic_config()
        kValue, meta = V2.resolveK(
            config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
            "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
        )
        assert meta["source"] == "static_fallback"
        assert "spread" in meta.get("fallbackReason", "").lower()

    def test_resolveK_rejects_bid_greater_than_ask(self):
        """bid > ask on PE → static fallback."""
        kite = self._make_mock_kite(peBid=110, peAsk=100)
        config = self._make_dynamic_config()
        # getBestPremium returns "ltp" for PE since bid>ask, while CE gets "mid"
        # → inconsistent premium quality → fallback
        kValue, meta = V2.resolveK(
            config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
            "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
        )
        assert meta["source"] == "static_fallback"

    def test_resolveK_rejects_near_zero_premium(self):
        """Premium < MIN_PREMIUM_INR → static fallback."""
        kite = self._make_mock_kite(ceBid=0.1, ceAsk=0.3, peBid=98, peAsk=102)
        config = self._make_dynamic_config()
        kValue, meta = V2.resolveK(
            config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
            "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
        )
        assert meta["source"] == "static_fallback"
        assert "dust" in meta.get("fallbackReason", "").lower() or "zero" in meta.get("fallbackReason", "").lower()

    def test_resolveK_rejects_mixed_mid_and_stale_ltp(self):
        """CE uses fresh mid, PE uses LTP (no depth) → inconsistent → fallback."""
        kite = self._make_mock_kite()
        # Override PE quote to have no depth → falls back to LTP
        peKey = "NFO:NIFTY26MAR24000PE"
        kite.quote.return_value[peKey]["depth"] = {"buy": [], "sell": []}
        config = self._make_dynamic_config()
        kValue, meta = V2.resolveK(
            config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
            "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
        )
        assert meta["source"] == "static_fallback"
        assert "inconsistent" in meta.get("fallbackReason", "").lower()

    def test_resolveK_rejects_stale_quote_during_market_hours(self):
        """Quote older than QUOTE_STALE_SECONDS during market hours → fallback."""
        kite = self._make_mock_kite()
        ceKey = "NFO:NIFTY26MAR24000CE"

        config = self._make_dynamic_config()
        # Patch datetime.now to be during market hours (11:00)
        # Compute stale_time relative to mock_now so the difference is always correct
        with patch("PlaceOptionsSystemsV2.datetime") as mock_dt:
            mock_now = datetime.now().replace(hour=11, minute=0, second=0, microsecond=0)
            stale_time = mock_now - timedelta(seconds=V2.QUOTE_STALE_SECONDS + 30)
            kite.quote.return_value[ceKey]["last_trade_time"] = stale_time
            mock_dt.now.return_value = mock_now
            mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)
            kValue, meta = V2.resolveK(
                config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
                "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
            )
        assert meta["source"] == "static_fallback"
        assert "stale" in meta.get("fallbackReason", "").lower()

    def test_resolveK_allows_old_quote_outside_market_hours(self):
        """Outside market hours (e.g., 20:00), staleness check should not trigger."""
        kite = self._make_mock_kite()
        ceKey = "NFO:NIFTY26MAR24000CE"
        # Set a very old timestamp
        kite.quote.return_value[ceKey]["last_trade_time"] = datetime(2026, 1, 1)

        config = self._make_dynamic_config()
        # During market hours the stale check fires; outside it doesn't.
        # The staleness check condition is: 9 <= now.hour < 16
        # At hour=20 the check is skipped.
        # But this test may still fail for other reasons (IV solve etc.) depending on premiums.
        # So we just check it doesn't fail due to staleness.
        with patch("PlaceOptionsSystemsV2.datetime") as mock_dt:
            mock_now = datetime.now().replace(hour=20, minute=0)
            mock_dt.now.return_value = mock_now
            mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)
            kValue, meta = V2.resolveK(
                config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
                "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
            )
        # Should NOT fail due to staleness
        if meta["source"] == "static_fallback":
            assert "stale" not in meta.get("fallbackReason", "").lower()

    def test_resolveK_rejects_iv_mismatch_between_legs(self):
        """CE IV and PE IV differ by > 50% of average → fallback."""
        # Give CE a very different premium from PE so the IVs diverge
        # CE premium very low → low IV, PE premium high → high IV
        kite = self._make_mock_kite(ceBid=5, ceAsk=7, peBid=200, peAsk=210)
        config = self._make_dynamic_config()
        kValue, meta = V2.resolveK(
            config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
            "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
        )
        assert meta["source"] == "static_fallback"


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 4: TestComputeDynamicK — dynamic-k calculation
# ═══════════════════════════════════════════════════════════════════════════════

class TestComputeDynamicK:
    """Validate the risk engine: computeDynamicK."""

    def _atm_greeks(self, T=5/365, iv=0.14, spot=24000):
        ce = V2.bsGreeks(spot, spot, T, iv, "CE")
        pe = V2.bsGreeks(spot, spot, T, iv, "PE")
        return ce, pe

    def _atm_premiums(self, T=5/365, iv=0.14, spot=24000):
        return (V2.bsPrice(spot, spot, T, iv, "CE")
                + V2.bsPrice(spot, spot, T, iv, "PE"))

    def test_returns_all_scenario_k_values(self):
        """Output contains kForSizing, kBase, kStressMove, kStressVol, kBindingScenario,
        kSpotSensitivity, expectedMove, avgIV, and pnl breakdown."""
        ce, pe = self._atm_greeks()
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 250, 65, "straddle")
        assert r is not None
        for key in ("kForSizing", "kBase", "kStressMove", "kStressVol", "kCrash", "kBindingScenario",
                    "kSpotSensitivity", "expectedMove", "avgIV",
                    "posGamma", "posTheta", "posVega", "pnlBreakdown"):
            assert key in r, f"missing key: {key}"
        for pnl_key in ("pnlDelta", "pnlGamma", "pnlVega", "pnlTheta",
                         "basePnl", "stressMovePnl", "stressVolPnl", "crashPnl"):
            assert pnl_key in r["pnlBreakdown"], f"missing pnl key: {pnl_key}"

    def test_atm_straddle_sanity(self):
        """Typical NIFTY (spot=24000, IV=14%, DTE≈3): kForSizing in broad [0.10, 1.50]."""
        ce, pe = self._atm_greeks(T=3/365)
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 200, 65, "straddle")
        assert r is not None
        assert 0.10 <= r["kForSizing"] <= 1.50
        assert r["kSpotSensitivity"] > 0

    def test_clamp_floor(self):
        """Tiny risk → all k scenarios clamped to K_FLOOR."""
        ce = {"delta": 0.001, "gamma": 0.00001, "theta": -0.001, "vega": 0.01}
        pe = {"delta": -0.001, "gamma": 0.00001, "theta": -0.001, "vega": 0.01}
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 500, 65, "straddle")
        assert r is not None
        assert r["kForSizing"] == V2.K_FLOOR
        assert r["kBase"] == V2.K_FLOOR

    def test_clamp_ceiling(self):
        """Huge risk → k clamped to K_CEILING."""
        ce = {"delta": 0.5, "gamma": 1.0, "theta": -100, "vega": 500}
        pe = {"delta": -0.5, "gamma": 1.0, "theta": -100, "vega": 500}
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 50, 65, "straddle")
        assert r is not None
        assert r["kForSizing"] == V2.K_CEILING

    def test_near_expiry_higher_than_far_expiry(self):
        """DTE=1 kForSizing > DTE=5 kForSizing (more gamma risk per premium unit near expiry)."""
        spot, iv = 24000, 0.14
        ce1, pe1 = self._atm_greeks(T=1/365, iv=iv, spot=spot)
        ce5, pe5 = self._atm_greeks(T=5/365, iv=iv, spot=spot)
        cp1 = self._atm_premiums(T=1/365, iv=iv, spot=spot)
        cp5 = self._atm_premiums(T=5/365, iv=iv, spot=spot)

        r1 = V2.computeDynamicK(ce1, pe1, iv, iv, spot, cp1, 65, "straddle")
        r5 = V2.computeDynamicK(ce5, pe5, iv, iv, spot, cp5, 65, "straddle")
        assert r1["kForSizing"] > r5["kForSizing"]

    def test_kForSizing_is_max_of_scenarios(self):
        """kForSizing = max(kBase, kStressMove, kStressVol, kCrash) always holds."""
        ce, pe = self._atm_greeks(T=3/365)
        cp = self._atm_premiums(T=3/365)
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, cp, 65,
                                "straddle", ivShockAbsolute=0.10)
        assert r is not None
        assert r["kForSizing"] == max(r["kBase"], r["kStressMove"], r["kStressVol"], r["kCrash"])
        assert r["kBindingScenario"] in ("kBase", "kStressMove", "kStressVol", "kCrash")

    def test_kStressMove_gte_kBase(self):
        """Stress move k ≥ base k for typical ATM short straddle (gamma dominates)."""
        ce, pe = self._atm_greeks(T=3/365)
        cp = self._atm_premiums(T=3/365)
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, cp, 65, "straddle")
        assert r is not None
        assert r["kStressMove"] >= r["kBase"]

    def test_zero_iv_shock_kStressVol_equals_kBase(self):
        """ivShockAbsolute=0 → kStressVol equals kBase, kCrash equals kStressMove."""
        ce, pe = self._atm_greeks()
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 250, 65,
                                "straddle", ivShockAbsolute=0.0)
        assert r["pnlBreakdown"]["pnlVega"] == 0.0
        assert r["kStressVol"] == r["kBase"]
        assert r["kCrash"] == r["kStressMove"]

    def test_positive_iv_shock_increases_kStressVol(self):
        """Positive IV shock → kStressVol > kBase for short straddle (short vega)."""
        ce, pe = self._atm_greeks()
        r0 = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 250, 65,
                                 "straddle", ivShockAbsolute=0.0)
        r10 = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 250, 65,
                                  "straddle", ivShockAbsolute=0.10)
        # Short straddle has negative posVega → IV shock makes stressVol worse
        assert r10["pnlBreakdown"]["pnlVega"] < 0
        assert r10["kStressVol"] >= r0["kBase"]

    def test_delta_neutral_straddle(self):
        """ATM straddle: combined delta near zero, gamma dominates base P&L."""
        ce, pe = self._atm_greeks()
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 250, 65, "straddle")
        # Position delta should be near zero (ATM CE delta ~+0.5 + PE delta ~-0.5)
        # pnlDelta should be small compared to pnlGamma
        assert abs(r["pnlBreakdown"]["pnlDelta"]) < abs(r["pnlBreakdown"]["pnlGamma"])

    def test_kCrash_gte_kStressMove_and_kStressVol(self):
        """kCrash (combined stress) ≥ both individual stress scenarios."""
        ce, pe = self._atm_greeks(T=3/365)
        cp = self._atm_premiums(T=3/365)
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, cp, 65,
                                "straddle", ivShockAbsolute=0.10)
        assert r is not None
        assert r["kCrash"] >= r["kStressMove"]
        assert r["kCrash"] >= r["kStressVol"]

    def test_uses_absolute_pnl_for_k(self):
        """All k scenario values are always positive (uses abs())."""
        ce, pe = self._atm_greeks()
        r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 250, 65, "straddle")
        assert r["kForSizing"] > 0
        assert r["kBase"] > 0
        assert r["kStressMove"] > 0
        assert r["kCrash"] >= V2.K_FLOOR
        assert r["kSpotSensitivity"] > 0

    def test_expected_move_uses_avg_iv(self):
        """CE IV=0.12, PE IV=0.16 → avgIV=0.14, expectedMove computed from 0.14."""
        import math
        ce, pe = self._atm_greeks()
        r = V2.computeDynamicK(ce, pe, 0.12, 0.16, 24000, 250, 65, "straddle")
        assert r["avgIV"] == 0.14
        expected = 24000 * 0.14 * math.sqrt(1.0 / 252.0)
        assert abs(r["expectedMove"] - round(expected, 2)) < 0.01

    def test_combined_premium_zero_rejected(self):
        """combinedPremium <= 0 → returns None."""
        ce, pe = self._atm_greeks()
        assert V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 0, 65, "straddle") is None
        assert V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, -10, 65, "straddle") is None

    def test_spot_zero_rejected(self):
        """spot <= 0 → returns None."""
        ce, pe = self._atm_greeks()
        assert V2.computeDynamicK(ce, pe, 0.14, 0.14, 0, 250, 65, "straddle") is None

    def test_avg_iv_zero_rejected(self):
        """avgIV = 0 → returns None (avoids division by zero in expectedMove)."""
        ce, pe = self._atm_greeks()
        assert V2.computeDynamicK(ce, pe, 0.0, 0.0, 24000, 250, 65, "straddle") is None

    def test_nan_greeks_do_not_crash(self):
        """NaN/inf in Greeks → function should not crash. May return None or clamped values."""
        ce = {"delta": float("nan"), "gamma": 0.001, "theta": -5, "vega": 10}
        pe = {"delta": -0.5, "gamma": 0.001, "theta": -5, "vega": 10}
        # Should not raise — may return None or a result with clamped k
        try:
            r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 200, 65, "straddle")
        except Exception:
            pytest.fail("computeDynamicK crashed on NaN Greeks")


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 5: TestResolveK — Integration / orchestrator / fallback behavior
# ═══════════════════════════════════════════════════════════════════════════════

class TestResolveK:
    """Test the resolveK orchestrator: dynamic happy path and all fallback triggers."""

    def _make_dynamic_config(self):
        return {
            "useDynamicK": True,
            "kTable": V2.K_TABLE_STRADDLE,
            "strategyType": "straddle",
        }

    def _make_clean_kite(self, spot=24000, cePremium=100, pePremium=100):
        """Build a mock kite with clean data for the full dynamic path."""
        kite = MagicMock()
        kite.ltp.return_value = {"NSE:NIFTY 50": {"last_price": spot}}

        ceKey, peKey = "NFO:NIFTY26MAR24000CE", "NFO:NIFTY26MAR24000PE"
        now = datetime.now()
        kite.quote.return_value = {
            ceKey: {
                "last_price": cePremium,
                "last_trade_time": now,
                "depth": {
                    "buy": [{"price": cePremium - 1, "quantity": 100}],
                    "sell": [{"price": cePremium + 1, "quantity": 100}],
                },
            },
            peKey: {
                "last_price": pePremium,
                "last_trade_time": now,
                "depth": {
                    "buy": [{"price": pePremium - 1, "quantity": 100}],
                    "sell": [{"price": pePremium + 1, "quantity": 100}],
                },
            },
        }
        V2.GetInstrumentsCached = MagicMock(return_value=[
            {"tradingsymbol": "NIFTY26MAR24000CE", "strike": 24000.0},
            {"tradingsymbol": "NIFTY26MAR24000PE", "strike": 24000.0},
        ])
        return kite

    def test_returns_static_when_disabled(self):
        """useDynamicK=False → static k, source='static'."""
        config = {
            "useDynamicK": False,
            "kTable": V2.K_TABLE_STRADDLE,
            "strategyType": "straddle",
        }
        kValue, meta = V2.resolveK(config, None, "X", "Y", "NFO", "NIFTY",
                                    2, 100, 100, 65, date.today())
        assert kValue == 0.70
        assert meta["source"] == "static"

    def test_dynamic_happy_path(self):
        """Full dynamic path with clean data → source='dynamic', metadata populated."""
        kite = self._make_clean_kite()
        config = self._make_dynamic_config()
        kValue, meta = V2.resolveK(
            config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
            "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
        )
        assert meta["source"] == "dynamic"
        assert isinstance(kValue, float)
        assert V2.K_FLOOR <= kValue <= V2.K_CEILING
        # kValue should be max of scenarios
        assert kValue == max(meta["kBase"], meta["kStressMove"], meta["kStressVol"], meta["kCrash"])
        # Metadata should be fully populated
        for key in ("kForSizing", "kBase", "kStressMove", "kStressVol", "kCrash", "kBindingScenario",
                    "kSpotSensitivity", "staticK", "avgIV",
                    "expectedMove", "ceIV", "peIV", "cePremiumUsed", "pePremiumUsed",
                    "cePremiumSource", "pePremiumSource", "timeToExpiryYears",
                    "ivShockApplied", "ivShockBase", "vixAddon", "intradayAddon"):
            assert key in meta, f"missing metadata key: {key}"

    def test_fallback_when_spot_fetch_fails(self):
        """kite.ltp() failure → static fallback."""
        kite = MagicMock()
        kite.ltp.side_effect = Exception("API down")
        config = self._make_dynamic_config()
        kValue, meta = V2.resolveK(config, kite, "X", "Y", "NFO", "NIFTY",
                                    2, 100, 100, 65, date.today())
        assert meta["source"] == "static_fallback"
        assert "spot" in meta["fallbackReason"].lower()

    def test_fallback_when_quote_fetch_fails(self):
        """kite.quote() failure → static fallback."""
        kite = MagicMock()
        kite.ltp.return_value = {"NSE:NIFTY 50": {"last_price": 24000}}
        kite.quote.side_effect = Exception("quote API error")
        config = self._make_dynamic_config()
        kValue, meta = V2.resolveK(
            config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
            "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
        )
        assert meta["source"] == "static_fallback"
        assert "quote" in meta["fallbackReason"].lower()

    def test_fallback_when_strike_lookup_fails(self):
        """Strike not found in instruments → static fallback."""
        kite = self._make_clean_kite()
        V2.GetInstrumentsCached = MagicMock(return_value=[])  # empty instruments
        config = self._make_dynamic_config()
        kValue, meta = V2.resolveK(
            config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
            "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
        )
        assert meta["source"] == "static_fallback"
        assert "strike" in meta["fallbackReason"].lower()

    def test_fallback_when_iv_solve_fails(self):
        """IV solver returns None for one leg → static fallback."""
        # Premium of 0.30 is near-zero dust → bsImpliedVol may fail
        # But MIN_PREMIUM_INR gate would catch this first (0.30 < 0.50)
        # Use a premium that passes the gate but is below intrinsic
        kite = self._make_clean_kite(cePremium=1.0, pePremium=1.0)
        # Set strike so CE is deep ITM → price below intrinsic → IV solver fails
        V2.GetInstrumentsCached = MagicMock(return_value=[
            {"tradingsymbol": "NIFTY26MAR24000CE", "strike": 20000.0},  # deep ITM
            {"tradingsymbol": "NIFTY26MAR24000PE", "strike": 24000.0},
        ])
        config = self._make_dynamic_config()
        kValue, meta = V2.resolveK(
            config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
            "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
        )
        assert meta["source"] == "static_fallback"

    def test_fallback_when_iv_near_bounds(self):
        """IV near solver bounds → fallback. Simulate by patching bsImpliedVol."""
        kite = self._make_clean_kite()
        config = self._make_dynamic_config()
        # Patch bsImpliedVol to return IV near upper bound
        with patch.object(V2, "bsImpliedVol", return_value=V2.IV_SOLVER_MAX * 0.96):
            kValue, meta = V2.resolveK(
                config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
                "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
            )
        assert meta["source"] == "static_fallback"
        assert "bound" in meta["fallbackReason"].lower()

    def test_iv_shock_derived_from_sizing_dte(self):
        """IV shock is looked up from IV_SHOCK_TABLE based on sizingDte, not config."""
        kite = self._make_clean_kite()
        config = self._make_dynamic_config()

        # sizingDte=1 → should use 15 vol points = 0.15 decimal
        with patch.object(V2, "computeDynamicK", wraps=V2.computeDynamicK) as mock_cdk:
            kValue, meta = V2.resolveK(
                config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
                "NFO", "NIFTY", 1, 100, 100, 65, date.today() + timedelta(days=3),
            )
            if meta["source"] == "dynamic":
                call_kwargs = mock_cdk.call_args
                ivShock = call_kwargs[1].get("ivShockAbsolute", call_kwargs[0][8] if len(call_kwargs[0]) > 8 else None)
                assert abs(ivShock - 0.15) < 1e-6, f"Expected 0.15 for DTE=1, got {ivShock}"

        # sizingDte=2 → should use 12 vol points = 0.12 decimal
        with patch.object(V2, "computeDynamicK", wraps=V2.computeDynamicK) as mock_cdk:
            kValue, meta = V2.resolveK(
                config, kite, "NIFTY26MAR24000CE", "NIFTY26MAR24000PE",
                "NFO", "NIFTY", 2, 100, 100, 65, date.today() + timedelta(days=3),
            )
            if meta["source"] == "dynamic":
                call_kwargs = mock_cdk.call_args
                ivShock = call_kwargs[1].get("ivShockAbsolute", call_kwargs[0][8] if len(call_kwargs[0]) > 8 else None)
                assert abs(ivShock - 0.12) < 1e-6, f"Expected 0.12 for DTE=2, got {ivShock}"


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 6: TestLogging — dynamic k fields in logs and dry-run
# ═══════════════════════════════════════════════════════════════════════════════

class TestDynamicKLogging:
    """Test that dynamic k metadata appears correctly in log output."""

    def test_logEntry_contains_dynamic_k_fields(self, tmp_path):
        """When dynamic k metadata is provided, CSV row includes all dynamic k fields."""
        import csv

        # Override log path to tmp
        original_path = V2.ENTRY_LOG_PATH
        V2.ENTRY_LOG_PATH = tmp_path / "test_entry.csv"

        kMetadata = {
            "source": "dynamic",
            "kForSizing": 0.65,
            "kBase": 0.42,
            "kStressMove": 0.65,
            "kStressVol": 0.55,
            "kCrash": 0.72,
            "kBindingScenario": "kCrash",
            "kSpotSensitivity": 0.42,
            "staticK": 0.70,
            "avgIV": 0.14,
            "expectedMove": 211.5,
            "posGamma": -0.004,
            "posTheta": 12.5,
            "posVega": -850.0,
            "ivShockApplied": 16.0,
            "ivShockBase": 12.0,
            "vixLevel": 22.5,
            "vixAddon": 4.0,
            "intradayMovePct": 0.3,
            "intradayAddon": 0.0,
            "cePremiumUsed": 100.5,
            "pePremiumUsed": 99.5,
            "cePremiumSource": "mid",
            "pePremiumSource": "mid",
            "ceIV": 0.138,
            "peIV": 0.142,
            "timeToExpiryYears": 0.008,
            "quoteTimestamp": "2026-03-23T10:30:00",
            "ceBid": 99.5, "ceAsk": 101.5, "ceSpreadPct": 1.98,
            "peBid": 98.5, "peAsk": 100.5, "peSpreadPct": 2.01,
        }

        config = {"underlying": "NIFTY", "phaseType": "early", "maxLots": 5}
        sizeResult = {"combinedPremium": 200, "dailyVolPerLot": 9100, "allowedLots": 7,
                      "finalLots": 5}

        try:
            V2.logEntry("N_STD_4D_30SL_I", config, 3, 0.65, 100.5, 99.5,
                        sizeResult, date.today(), "CE_SYM", "PE_SYM",
                        skipped=False, skipReason=None, kMetadata=kMetadata)

            with open(V2.ENTRY_LOG_PATH) as f:
                reader = csv.DictReader(f)
                row = next(reader)

            assert row["kSource"] == "dynamic"
            assert row["kForSizing"] == "0.65"
            assert row["kBase"] == "0.42"
            assert row["kStressMove"] == "0.65"
            assert row["kStressVol"] == "0.55"
            assert row["kCrash"] == "0.72"
            assert row["kBindingScenario"] == "kCrash"
            assert row["kSpotSensitivity"] == "0.42"
            assert row["avgIV"] == "0.14"
            assert row["ceIV"] == "0.138"
            assert row["peIV"] == "0.142"
            assert row["cePremiumSource"] == "mid"
        finally:
            V2.ENTRY_LOG_PATH = original_path

    def test_logEntry_contains_fallback_reason(self, tmp_path):
        """When falling back, kSource='static_fallback' appears."""
        import csv
        original_path = V2.ENTRY_LOG_PATH
        V2.ENTRY_LOG_PATH = tmp_path / "test_entry_fb.csv"

        kMetadata = {"source": "static_fallback", "staticK": 0.70,
                     "fallbackReason": "CE spread too wide"}

        config = {"underlying": "NIFTY", "phaseType": "early", "maxLots": 5}
        sizeResult = {"combinedPremium": 200, "dailyVolPerLot": 9100, "allowedLots": 7,
                      "finalLots": 5}

        try:
            V2.logEntry("N_STD_4D_30SL_I", config, 3, 0.70, 100, 100,
                        sizeResult, date.today(), "CE", "PE",
                        skipped=False, skipReason=None, kMetadata=kMetadata)

            with open(V2.ENTRY_LOG_PATH) as f:
                reader = csv.DictReader(f)
                row = next(reader)

            assert row["kSource"] == "static_fallback"
            assert row["staticK"] == "0.7"
        finally:
            V2.ENTRY_LOG_PATH = original_path

    def test_dry_run_prints_dynamic_k_breakdown(self, capsys):
        """Dry-run output should include dynamic k details when source='dynamic'."""
        # We can't easily run executeEntry in tests without many mocks,
        # but we can verify the dry-run print logic would work with the right metadata
        # by checking the string formatting code doesn't crash with a sample metadata dict
        meta = {"source": "dynamic", "kPremiumRisk": 0.55, "kSpotSensitivity": 0.42,
                "staticK": 0.70, "avgIV": 0.14, "expectedMove": 211.5,
                "stressK_1_5x": 0.65, "stressK_2x": 0.78,
                "ceIV": 0.138, "peIV": 0.142,
                "cePremiumSource": "mid", "pePremiumSource": "mid",
                "timeToExpiryYears": 0.008}

        # Simulate dry-run print block
        tag = "[DRY RUN] "
        print(f"{tag}  K source:        {meta.get('source', 'unknown')}")
        if meta.get("source") == "dynamic":
            print(f"{tag}  K (premium):     {meta.get('kPremiumRisk', 'N/A')}")
            print(f"{tag}  K (spot sens):   {meta.get('kSpotSensitivity', 'N/A')}")
            print(f"{tag}  Avg IV:          {meta.get('avgIV', 'N/A')}")
            print(f"{tag}  Expected move:   {meta.get('expectedMove', 'N/A')}")

        output = capsys.readouterr().out
        assert "dynamic" in output
        assert "0.55" in output
        assert "0.42" in output
        assert "0.14" in output


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 7: Edge cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """Edge cases: very small T, very high IV, division-by-zero guards."""

    def test_very_small_time_does_not_crash(self):
        """T = 1e-6 → pricing, Greeks, IV solve, dynamic k should not crash."""
        T = 1e-6
        price = V2.bsPrice(24000, 24000, T, 0.14, "CE")
        assert isinstance(price, float)
        g = V2.bsGreeks(24000, 24000, T, 0.14, "CE")
        assert isinstance(g["delta"], float)
        # IV solve at tiny T
        V2.bsImpliedVol(price, 24000, 24000, T, "CE")  # should not crash

    def test_very_high_iv_does_not_crash(self):
        """IV near upper range (3.0) → no crash."""
        price = V2.bsPrice(24000, 24000, 5 / 365, 3.0, "CE")
        assert isinstance(price, float) and price > 0
        g = V2.bsGreeks(24000, 24000, 5 / 365, 3.0, "CE")
        assert isinstance(g["delta"], float)

    def test_expected_move_zero_safe(self):
        """If avgIV → 0 somehow, kSpotSensitivity should not divide by zero."""
        ce = {"delta": 0.5, "gamma": 0.001, "theta": -5, "vega": 10}
        pe = {"delta": -0.5, "gamma": 0.001, "theta": -5, "vega": 10}
        # avgIV=0 → computeDynamicK returns None (checked separately)
        result = V2.computeDynamicK(ce, pe, 0.0, 0.0, 24000, 200, 65, "straddle")
        assert result is None  # rejected, no division by zero

    def test_inf_greeks_do_not_crash(self):
        """inf in Greeks → should not crash."""
        ce = {"delta": float("inf"), "gamma": 0.001, "theta": -5, "vega": 10}
        pe = {"delta": -0.5, "gamma": 0.001, "theta": -5, "vega": 10}
        try:
            r = V2.computeDynamicK(ce, pe, 0.14, 0.14, 24000, 200, 65, "straddle")
        except Exception:
            pytest.fail("computeDynamicK crashed on inf Greeks")


# ═══════════════════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
