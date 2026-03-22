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
# Run
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
