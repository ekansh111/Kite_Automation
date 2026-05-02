"""Tests for the ITM call dynamic K + pooled allocation framework.

Covers:
  - computeDynamicK with strategyType='long_single' (kBase, kStressMove, kVegaCrush)
  - lookupRegimeAddon mapping
  - ComputePositionSizeITM with premium cap
  - AllocateLotsBalanced (balance-preserving + 80% round-up)
"""
import math
import unittest

from PlaceOptionsSystemsV2 import (
    computeDynamicK, lookupRegimeAddon, REGIME_ADDON_TABLE,
    K_FLOOR, K_CEILING,
)
from itm_call_rollover import (
    ComputePositionSizeITM, AllocateLotsBalanced, POOL_ROUNDUP_THRESHOLD,
)


# ─── computeDynamicK long_single ──────────────────────────────────

class TestComputeDynamicKLongSingle(unittest.TestCase):
    """Verify long_single produces three scenarios with negative-direction stresses."""

    def setUp(self):
        # Today's NIFTY-like inputs
        self.greeks = {
            "delta": 0.792, "gamma": 0.000180,
            "theta": -7.19, "vega": 2791,
        }
        self.dummy = {"delta": 0, "gamma": 0, "theta": 0, "vega": 0}
        self.spot = 23997.55
        self.iv = 0.164
        self.premium = 1405.15
        self.shock = 0.08  # 8 vp

    def _run(self, **overrides):
        kwargs = dict(
            ceGreeks=self.greeks, peGreeks=self.dummy,
            ceIV=self.iv, peIV=self.iv,
            spot=self.spot, combinedPremium=self.premium,
            lotSize=65, strategyType="long_single",
            ivShockAbsolute=self.shock,
        )
        kwargs.update(overrides)
        return computeDynamicK(**kwargs)

    def test_returns_three_scenarios_for_long_single(self):
        r = self._run()
        self.assertIsNotNone(r["kBase"])
        self.assertIsNotNone(r["kStressMove"])
        self.assertIsNotNone(r["kVegaCrush"])
        self.assertIsNone(r["kStressVol"])
        self.assertIsNone(r["kCrash"])

    def test_kVegaCrush_binds_at_8vp_shock(self):
        r = self._run()
        self.assertEqual(r["kBindingScenario"], "kVegaCrush")

    def test_kBase_value_matches_hand_calc(self):
        r = self._run()
        # kBase ≈ 0.140 from hand calculation
        self.assertAlmostEqual(r["kBase"], 0.140, places=2)

    def test_kStressMove_value_matches_hand_calc(self):
        r = self._run()
        # kStressMove ≈ 0.206
        self.assertAlmostEqual(r["kStressMove"], 0.206, places=2)

    def test_kVegaCrush_value_matches_hand_calc(self):
        r = self._run()
        # kVegaCrush at 8vp ≈ 0.300
        self.assertAlmostEqual(r["kVegaCrush"], 0.300, places=2)

    def test_K_for_sizing_clamped_to_floor(self):
        # If everything is tiny, K_for_sizing should clamp to K_FLOOR
        result = computeDynamicK(
            ceGreeks={"delta": 0.001, "gamma": 0, "theta": 0, "vega": 1},
            peGreeks=self.dummy,
            ceIV=0.001, peIV=0.001,
            spot=100, combinedPremium=1000,
            lotSize=1, strategyType="long_single",
            ivShockAbsolute=0.001,
        )
        self.assertGreaterEqual(result["kForSizing"], K_FLOOR)

    def test_short_straddle_unchanged(self):
        # Existing short straddle path still produces the four scenarios
        ce = {"delta": 0.5, "gamma": 0.0001, "theta": -5, "vega": 1500}
        pe = {"delta": -0.5, "gamma": 0.0001, "theta": -5, "vega": 1500}
        r = computeDynamicK(
            ceGreeks=ce, peGreeks=pe,
            ceIV=0.14, peIV=0.14,
            spot=24000, combinedPremium=300,
            lotSize=65, strategyType="straddle",
            ivShockAbsolute=0.06,
        )
        self.assertIsNotNone(r["kBase"])
        self.assertIsNotNone(r["kStressMove"])
        self.assertIsNotNone(r["kStressVol"])
        self.assertIsNotNone(r["kCrash"])
        self.assertIsNone(r["kVegaCrush"])


# ─── Regime addon ──────────────────────────────────────────────────

class TestRegimeAddon(unittest.TestCase):
    def test_below_baseline_negative_addon(self):
        self.assertLess(lookupRegimeAddon(0.6), 0)

    def test_near_baseline_zero_addon(self):
        self.assertEqual(lookupRegimeAddon(1.0), 0)
        self.assertEqual(lookupRegimeAddon(1.05), 0)

    def test_mild_expansion_positive_addon(self):
        self.assertGreater(lookupRegimeAddon(1.2), 0)

    def test_extreme_expansion_caps(self):
        self.assertGreater(lookupRegimeAddon(2.5), 0)
        self.assertEqual(lookupRegimeAddon(2.5), REGIME_ADDON_TABLE[-1][2] / 100)

    def test_addon_returns_decimal(self):
        # +2 vp should be 0.02 decimal
        self.assertAlmostEqual(lookupRegimeAddon(1.2), 0.02)


# ─── ComputePositionSizeITM with premium cap ──────────────────────

class TestComputePositionSizeITM(unittest.TestCase):
    def test_no_cap_uses_vol_target_only(self):
        r = ComputePositionSizeITM(
            Premium=1405, LotSize=65, KValue=0.30, DailyVolBudget=45703,
            MaxPremiumOutlay=None,
        )
        # dvpl = 0.30 * 1405 * 65 = 27,398; floor(45703/27398) = 1
        self.assertEqual(r["finalLots"], 1)
        self.assertEqual(r["bindingConstraint"], "vol-target")
        self.assertIsNone(r["lotsCap"])

    def test_cap_binds_when_lots_vol_exceeds_cap(self):
        # Make K small so vol-target wants lots of lots, but cap restricts
        r = ComputePositionSizeITM(
            Premium=1405, LotSize=65, KValue=0.05, DailyVolBudget=500000,
            MaxPremiumOutlay=200000,
        )
        # lots_vol = 500000 / (0.05*1405*65) = 109; cap = 200000/91325 = 2
        self.assertEqual(r["lotsCap"], 2)
        self.assertEqual(r["finalLots"], 2)
        self.assertEqual(r["bindingConstraint"], "premium-cap")

    def test_minimum_one_lot_enforced(self):
        # Tiny budget — vol-target would say 0, but we ensure min 1
        r = ComputePositionSizeITM(
            Premium=10000, LotSize=100, KValue=0.50, DailyVolBudget=100,
        )
        self.assertEqual(r["finalLots"], 1)


# ─── AllocateLotsBalanced ─────────────────────────────────────────

class TestAllocateLotsBalanced(unittest.TestCase):
    def _make_inputs(self, n_dvpl, b_dvpl, n_cost, b_cost, n_floor, b_floor,
                      max_outlay=300000, daily_budget=45703):
        return {
            "NIFTY": {"dvpl": n_dvpl, "cost_per_lot": n_cost, "max_outlay": max_outlay,
                      "daily_budget_per_idx": daily_budget, "floor_lots": n_floor},
            "BANKNIFTY": {"dvpl": b_dvpl, "cost_per_lot": b_cost, "max_outlay": max_outlay,
                          "daily_budget_per_idx": daily_budget, "floor_lots": b_floor},
        }

    def test_today_scenario_2N_1B(self):
        # Today's setup: floor 1+1, leftover allows 1 more NIFTY
        inputs = self._make_inputs(
            n_dvpl=27379, b_dvpl=35100, n_cost=91335, b_cost=105517,
            n_floor=1, b_floor=1,
        )
        r = AllocateLotsBalanced(inputs)
        self.assertEqual(r["allocations"], {"NIFTY": 2, "BANKNIFTY": 1})

    def test_balance_rule_prefers_index_with_fewer_lots(self):
        # NIFTY already has 2 (from per-index floor), BANK has 1
        # Pool leftover should go to BANK first (balance), not NIFTY
        inputs = self._make_inputs(
            n_dvpl=18000, b_dvpl=25000, n_cost=120000, b_cost=110000,
            n_floor=2, b_floor=1,
        )
        r = AllocateLotsBalanced(inputs)
        # After floor: 2N + 1B, used = 2*18000 + 25000 = 61000, left 30406
        # BANK has fewer (1) → try BANK first → fits → 2N + 2B
        self.assertEqual(r["allocations"]["BANKNIFTY"], 2)

    def test_NIFTY_tiebreaker_when_equal_lots(self):
        # Both at 1 lot floor, leftover allows either → NIFTY wins
        inputs = self._make_inputs(
            n_dvpl=20000, b_dvpl=20000, n_cost=100000, b_cost=100000,
            n_floor=1, b_floor=1,
        )
        r = AllocateLotsBalanced(inputs)
        # Pool = 91406, used = 40000, left = 51406
        # Tied → NIFTY wins → +1 NIFTY → 2+1, left = 31406
        # Now BANK fewer → +1 BANK → 2+2, left = 11406
        # Tied again at 2+2 → NIFTY → +1, left = -8594 (over budget)... hmm
        # Actually 11406 < 20000 → strict 100% fails; 11406/20000 = 57% < 80% → no
        # So final 2+2
        self.assertEqual(r["allocations"], {"NIFTY": 2, "BANKNIFTY": 2})

    def test_premium_cap_blocks_extra_lot(self):
        # NIFTY would want extra but cap prevents
        inputs = self._make_inputs(
            n_dvpl=20000, b_dvpl=20000, n_cost=200000, b_cost=200000,
            n_floor=1, b_floor=1,
            max_outlay=250000,  # 1 lot uses 200k; 2 lots = 400k > cap
        )
        r = AllocateLotsBalanced(inputs)
        # Tied → NIFTY → can leftover fit? 51406 ≥ 0.8*20000=16000 ✓
        # But adding 2nd NIFTY → outlay 400k > cap 250k → blocked
        # Try BANK → same situation → blocked
        # Stop with 1+1
        self.assertEqual(r["allocations"], {"NIFTY": 1, "BANKNIFTY": 1})

    def test_80_pct_round_up_kicks_in(self):
        # Construct case where strict 100% fails but 80% rule succeeds
        # Pool = 91406, after floor 1+1 use 32000+32500 = 64500, left 26906
        # Try add NIFTY: need 32000, have 26906 → 84% ≥ 80% → ADD
        inputs = self._make_inputs(
            n_dvpl=32000, b_dvpl=32500, n_cost=91000, b_cost=110000,
            n_floor=1, b_floor=1,
        )
        r = AllocateLotsBalanced(inputs)
        # 80% rule should add NIFTY (over-budget by ~5k)
        self.assertEqual(r["allocations"]["NIFTY"], 2)
        # Pool over-used (over_budget > 0)
        self.assertGreater(r["over_budget"], 0)

    def test_no_extras_when_leftover_too_small(self):
        # Per-index floor uses most of budget, no room for extras
        inputs = self._make_inputs(
            n_dvpl=44000, b_dvpl=44000, n_cost=100000, b_cost=100000,
            n_floor=1, b_floor=1,
        )
        # Used 88000, left 3406 → way below threshold
        r = AllocateLotsBalanced(inputs)
        self.assertEqual(r["allocations"], {"NIFTY": 1, "BANKNIFTY": 1})


if __name__ == "__main__":
    unittest.main()
