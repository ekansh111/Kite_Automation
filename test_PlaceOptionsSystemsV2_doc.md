# PlaceOptionsSystemsV2 — Unit Test Document

## Overview

**Test file:** `test_PlaceOptionsSystemsV2.py`
**Total tests:** 114
**Framework:** pytest
**Run command:** `python3 -m pytest test_PlaceOptionsSystemsV2.py -v`

The test suite is organized into **16 test classes** covering the V2 options trading system: core position sizing, state machine, reconciliation, exit logic, and the new dynamic k framework (Black-Scholes, IV solver, quote handling, dynamic k computation, and integration).

---

## Test Classes Summary

| # | Class | Tests | Category |
|---|-------|-------|----------|
| 1 | TestLookupK | 8 | Core — Static K |
| 2 | TestComputePositionSize | 6 | Core — Position Sizing |
| 3 | TestIsWithinTimeWindow | 6 | Core — Time Windows |
| 4 | TestStateTransitions | 5 | Core — State Machine |
| 5 | TestResetCompletedCycle | 4 | Core — Cycle Management |
| 6 | TestReconciliation | 7 | Core — Broker Reconciliation |
| 7 | TestExitPreflight | 5 | Core — Exit Logic |
| 8 | TestComputeTradingDte | 5 | Core — DTE Computation |
| 9 | TestRegressionBugs | 9 | Production Bug Replays |
| 10 | TestBlackScholes | 11 | Dynamic K — BS Pricing & Greeks |
| 11 | TestImpliedVol | 8 | Dynamic K — IV Solver |
| 12 | TestQuoteHandling | 12 | Dynamic K — Quote Quality |
| 13 | TestComputeDynamicK | 15 | Dynamic K — K Computation |
| 14 | TestResolveK | 8 | Dynamic K — Orchestration |
| 15 | TestDynamicKLogging | 3 | Dynamic K — Logging |
| 16 | TestEdgeCases | 4 | Dynamic K — Edge Cases |
| | **Total** | **114** | |

---

## Group 1: Core V2 System (54 tests)

### TestLookupK (8 tests)

Tests the static K ≈ 1/√T table lookup used for position sizing.

| Test | What it verifies |
|------|-----------------|
| `test_dte_1_peak_gamma` | DTE=1 returns K=1.00 (highest gamma risk) |
| `test_dte_2` | DTE=2 returns K=0.70 |
| `test_dte_3` | DTE=3 returns K=0.58 |
| `test_dte_4` | DTE=4 returns K=0.50 |
| `test_dte_5` | DTE=5 returns K=0.45 |
| `test_dte_7_upper_bound` | DTE=7 returns K=0.38 |
| `test_dte_0_raises` | DTE=0 raises ValueError |
| `test_dte_8_raises` | DTE=8 (out of range) raises ValueError |

### TestComputePositionSize (6 tests)

Tests the daily-vol position sizing formula: `lots = round_half_up(budget / (K × premium × lotSize))`.

| Test | What it verifies |
|------|-----------------|
| `test_nifty_4d_typical` | Typical NIFTY 4-day: budget/vol gives correct lots |
| `test_maxlots_cap_applied` | Max lots cap is enforced |
| `test_round_half_up_not_bankers` | Uses round-half-up (not Python's banker's rounding) |
| `test_zero_premium_skipped` | Zero premium returns 0 lots (no division by zero) |
| `test_budget_too_small_for_one_lot` | Budget below 1 lot returns 0 |
| `test_sensex_2d_k1` | SENSEX lot size (10) with K=1.0 |

### TestIsWithinTimeWindow (6 tests)

Tests the smart time window: sleep-before (≤2min) and tolerance-after (≤5min).

| Test | What it verifies |
|------|-----------------|
| `test_exact_target_time` | Exactly at target time → True |
| `test_3min_after_proceeds` | 3 minutes after target → True |
| `test_7min_after_rejects` | 7 minutes after target → False |
| `test_1min_before_sleeps_until_target` | 1 min before → sleeps, then True |
| `test_5min_before_rejects` | 5 min before → False |
| `test_1230_window` | 12:30 window works correctly |

### TestStateTransitions (5 tests)

Tests the V2 state machine transitions.

| Test | What it verifies |
|------|-----------------|
| `test_transition_to_early_open` | noPosition → earlyOpen |
| `test_transition_to_late_open` | noPosition → lateOpen |
| `test_exit_from_early_goes_to_no_position` | earlyOpen → noPosition (4D exit) |
| `test_exit_from_late_goes_to_completed_cycle` | lateOpen → completedCycle |
| `test_exit_from_repair_goes_to_no_position` | repairRequired → noPosition |

### TestResetCompletedCycle (4 tests)

| Test | What it verifies |
|------|-----------------|
| `test_new_expiry_resets_to_no_position` | New expiry cycle resets state |
| `test_same_expiry_stays_completed` | Same expiry stays completedCycle |
| `test_let_expire_safety_net_resets_in_one_step` | letExpire safety net works |
| `test_no_position_unaffected` | noPosition state unaffected by reset |

### TestReconciliation (7 tests)

Tests position reconciliation against broker API.

| Test | What it verifies |
|------|-----------------|
| `test_all_contracts_verified` | All V2 contracts found at broker → OK |
| `test_all_contracts_missing_warns` | All missing → warn only (no auto-repair) |
| `test_partial_contracts_missing_warns` | Partial missing → warn |
| `test_other_system_positions_dont_trigger_repair` | Non-V2 positions ignored |
| `test_no_positions_on_either_side_ok` | Both sides empty → OK |
| `test_banknifty_filtered_by_prefix` | BANKNIFTY not confused with NIFTY |
| `test_broker_api_failure_graceful` | API failure handled gracefully |

### TestExitPreflight (5 tests)

Tests exit pre-flight broker verification.

| Test | What it verifies |
|------|-----------------|
| `test_scenario_a_all_legs_sl_triggered` | All legs SL'd → cancel GTTs, no orders |
| `test_scenario_b_partial_sl_closes_survivor` | Partial SL → close only surviving legs |
| `test_scenario_c_normal_exit` | All legs present → normal exit |
| `test_broker_api_failure_aborts_safely` | API failure aborts exit safely |
| `test_gtt_cancel_failure_is_non_blocking` | GTT cancel failure logged but non-blocking |

### TestComputeTradingDte (5 tests)

| Test | What it verifies |
|------|-----------------|
| `test_expiry_day_is_zero` | Expiry day = DTE 0 |
| `test_monday_to_tuesday` | Mon→Tue = 1 trading day |
| `test_friday_to_tuesday_skips_weekend` | Fri→Tue = 1 (skips Sat/Sun) |
| `test_wednesday_to_next_tuesday` | Wed→Tue = 4 trading days |
| `test_holiday_skipped` | Holidays excluded from count |

### TestRegressionBugs (9 tests) — Production Bug Replays

Each test replays the exact scenario that caused a real production bug. These are the highest-priority tests.

| Test | Bug | Date | What happened |
|------|-----|------|--------------|
| `test_bug1_bankers_rounding` | BUG1 | 2026-03-11 | `round(2.5)=2` gave wrong lots |
| `test_bug2_exit_when_sl_already_triggered` | BUG2 | 2026-03-16 | Exit created unwanted LONG position |
| `test_bug4_reconciliation_exact_contract_matching` | BUG4 | 2026-03-16 | Checked "any option" not specific contracts |
| `test_bug5_reconciliation_warn_only_when_broker_flat` | BUG5 | 2026-03-16 | Auto-set repairRequired on stale API data |
| `test_bug6_other_system_positions_ignored` | BUG6 | 2026-03-17 | Non-V2 positions triggered repairRequired |
| `test_bug7_partial_sl_closes_surviving_leg` | BUG7 | 2026-03-17 | Only surviving leg closed after partial SL |
| `test_bug8_completed_cycle_unblocks_for_new_expiry` | BUG8 | 2026-03-18 | completedCycle blocked new expiry entry |
| `test_bug9_gtt_cancel_error_is_logged_not_silent` | BUG9 | 2026-03-20 | GTT cancel errors were silently swallowed |

---

## Group 2: Dynamic K Framework (60 tests)

### TestBlackScholes (11 tests)

Tests Black-Scholes European option pricing and Greeks computation.

| Test | What it verifies |
|------|-----------------|
| `test_bsPrice_call_put_parity` | C − P = S − K·e^(−rT) (put-call parity, no dividends) |
| `test_bsPrice_monotonic_with_iv` | Higher IV → higher option price |
| `test_bsPrice_monotonic_with_time` | Longer time → higher option price |
| `test_bsGreeks_atm_call` | ATM call: δ≈0.5, γ>0, θ<0, ν>0 |
| `test_bsGreeks_atm_put` | ATM put: δ≈−0.5 |
| `test_bsGreeks_deep_itm_call` | Deep ITM call: δ→1.0 |
| `test_bsGreeks_deep_otm_put` | Deep OTM put: δ→0.0 |
| `test_bsGreeks_near_expiry_gamma_rises` | Near expiry: ATM gamma spikes |
| `test_bsGreeks_vega_scaling_convention` | Vega = raw ∂V/∂σ (finite-difference verified) |
| `test_bsGreeks_theta_scaling_convention` | Theta reported per calendar day (finite-difference verified) |

**Key conventions verified:**
- Theta = premium change per 1 calendar day
- Vega = raw ∂V/∂σ (multiply by Δσ in absolute decimal to get P&L)
- No dividend yield (q=0), appropriate for NIFTY/SENSEX index options

### TestImpliedVol (8 tests)

Tests the Newton-Raphson + bisection IV solver.

| Test | What it verifies |
|------|-----------------|
| `test_roundtrip_call` | Price CE with known IV → solve back → IV matches within 0.001 |
| `test_roundtrip_put` | Price PE with known IV → solve back → IV matches within 0.001 |
| `test_newton_fallback_to_bisection` | Newton diverges → bisection converges correctly |
| `test_rejects_zero_price` | Premium=0 → returns None |
| `test_rejects_negative_price` | Premium<0 → returns None |
| `test_rejects_below_intrinsic` | Premium < intrinsic value → returns None |
| `test_rejects_bad_time` | T=0 or T<0 → returns None |
| `test_respects_bounds` | IV solution within [IV_SOLVER_MIN, IV_SOLVER_MAX] |

### TestQuoteHandling (12 tests)

Tests `getBestPremium()` premium sourcing and `resolveK()` quote-quality gates.

**getBestPremium tests (5):**

| Test | What it verifies |
|------|-----------------|
| `test_mid_when_bid_ask_valid` | Both bid and ask present → returns mid-price |
| `test_bid_when_only_bid_valid` | Only bid available → returns bid |
| `test_ask_when_only_ask_valid` | Only ask available → returns ask |
| `test_ltp_fallback` | No depth → returns LTP |
| `test_rejects_bid_greater_than_ask` | Crossed book (bid>ask) → falls back to LTP |

**resolveK quote-quality gate tests (7):**

| Test | Gate tested | What it verifies |
|------|------------|-----------------|
| `test_resolveK_rejects_wide_spread` | Spread gate | Bid-ask spread > 30% → static fallback |
| `test_resolveK_rejects_bid_greater_than_ask` | Crossed book | bid > ask → static fallback |
| `test_resolveK_rejects_near_zero_premium` | Dust gate | Premium < MIN_PREMIUM_INR → static fallback |
| `test_resolveK_rejects_mixed_mid_and_stale_ltp` | Quality consistency | CE=mid, PE=ltp → static fallback |
| `test_resolveK_rejects_stale_quote_during_market_hours` | Staleness gate | Quote > QUOTE_STALE_SECONDS old during market hours → fallback |
| `test_resolveK_allows_old_quote_outside_market_hours` | Staleness bypass | Outside market hours → staleness check skipped |
| `test_resolveK_rejects_iv_mismatch_between_legs` | IV consistency | CE IV and PE IV differ > 50% of average → fallback |

### TestComputeDynamicK (15 tests)

Tests the pure `computeDynamicK()` function that derives k from Greeks and IV.

| Test | What it verifies |
|------|-----------------|
| `test_returns_both_k_values` | Returns both kPremiumRisk and kSpotSensitivity |
| `test_atm_straddle_sanity` | Typical ATM (spot=24000, IV=14%, DTE=3): k ∈ [0.10, 1.20] |
| `test_clamp_floor` | Extreme inputs produce k ≥ K_FLOOR (0.20) |
| `test_clamp_ceiling` | Extreme inputs produce k ≤ K_CEILING (1.50) |
| `test_near_expiry_higher_than_far_expiry` | DTE=1 k > DTE=5 k (gamma effect) |
| `test_stress_pnl_magnitude_grows` | Stress P&L at 1.5x/2x ≥ base P&L |
| `test_zero_iv_shock_zero_vega_contribution` | ivShockPercent=0 → vega contributes 0 to P&L |
| `test_positive_iv_shock_hurts_short_straddle` | IV shock increases P&L magnitude (bad for sellers) |
| `test_delta_neutral_straddle` | ATM straddle: net delta ≈ 0 |
| `test_uses_absolute_pnl_for_k` | k uses |pnlPerUnit|, always positive |
| `test_expected_move_uses_avg_iv` | Expected move = spot × avgIV × √(1/252) |
| `test_combined_premium_zero_rejected` | combinedPremium=0 → returns None |
| `test_spot_zero_rejected` | spot=0 → returns None |
| `test_avg_iv_zero_rejected` | avgIV=0 → returns None |
| `test_nan_greeks_do_not_crash` | NaN/inf Greeks → graceful handling |

**Key formula verified:**
```
pnlPerUnit = δ·ΔS + ½·Γ·(ΔS)² + ν·Δσ + θ·Δt
kPremiumRisk = |pnlPerUnit| / combinedPremium    (used for sizing)
kSpotSensitivity = |pnlPerUnit| / expectedMove    (logged for insight)
```

### TestResolveK (8 tests)

Tests the `resolveK()` orchestrator — the integration point that fetches market data, computes Greeks/IV, and falls back to static k on failure.

| Test | What it verifies |
|------|-----------------|
| `test_returns_static_when_disabled` | useDynamicK=False → static k, source="static" |
| `test_dynamic_happy_path` | Full dynamic k flow: spot→quote→strike→IV→Greeks→k |
| `test_fallback_when_spot_fetch_fails` | kite.ltp() throws → static fallback |
| `test_fallback_when_quote_fetch_fails` | kite.quote() throws → static fallback |
| `test_fallback_when_strike_lookup_fails` | Instruments cache miss → static fallback |
| `test_fallback_when_iv_solve_fails` | IV solver returns None → static fallback |
| `test_fallback_when_iv_near_bounds` | IV near solver bounds → static fallback |
| `test_passes_correct_iv_shock_to_compute` | ivShockPercent config → passed to computeDynamicK |

### TestDynamicKLogging (3 tests)

Tests that dynamic k metadata flows correctly into entry logs and dry-run output.

| Test | What it verifies |
|------|-----------------|
| `test_logEntry_contains_dynamic_k_fields` | CSV row includes all 18 new k-metadata fields |
| `test_logEntry_contains_fallback_reason` | Fallback reason written to CSV |
| `test_dry_run_prints_dynamic_k_breakdown` | --dry-run prints k details (both variants, Greeks, IV) |

**New CSV fields (18):** kSource, kPremiumRisk, kSpotSensitivity, staticK, avgIV, expectedMove, posGamma, posTheta, posVega, stressK_1_5x, stressK_2x, cePremiumUsed, pePremiumUsed, cePremiumSource, pePremiumSource, ceIV, peIV, timeToExpiryYears, quoteTimestamp, ceBid, ceAsk, ceSpreadPct, peBid, peAsk, peSpreadPct

### TestEdgeCases (4 tests)

Tests extreme inputs that could cause numerical issues.

| Test | What it verifies |
|------|-----------------|
| `test_very_small_time_does_not_crash` | T=1e-7: no crash, Greeks may be extreme but finite or handled |
| `test_very_high_iv_does_not_crash` | IV=4.99 (near solver max): price/Greeks compute without error |
| `test_expected_move_zero_safe` | Expected move = 0 does not cause division by zero |
| `test_inf_greeks_do_not_crash` | Infinite Greek values handled gracefully |

---

## Constants Tested

| Constant | Value | Tested in |
|----------|-------|-----------|
| K_FLOOR | 0.20 | TestComputeDynamicK::test_clamp_floor |
| K_CEILING | 1.50 | TestComputeDynamicK::test_clamp_ceiling |
| RISK_FREE_RATE | 0.07 | TestBlackScholes (all BS tests) |
| IV_SOLVER_MIN | 0.01 | TestImpliedVol::test_respects_bounds |
| IV_SOLVER_MAX | 5.0 | TestImpliedVol::test_respects_bounds, TestResolveK::test_fallback_when_iv_near_bounds |
| QUOTE_STALE_SECONDS | 60 | TestQuoteHandling::test_resolveK_rejects_stale_quote_during_market_hours |
| BID_ASK_SPREAD_GATE | 0.30 | TestQuoteHandling::test_resolveK_rejects_wide_spread |
| MIN_PREMIUM_INR | 0.50 | TestQuoteHandling::test_resolveK_rejects_near_zero_premium |
| IV_SPREAD_GATE | 0.50 | TestQuoteHandling::test_resolveK_rejects_iv_mismatch_between_legs |

---

## Running Tests

```bash
# All tests
python3 -m pytest test_PlaceOptionsSystemsV2.py -v

# Concise failure output
python3 -m pytest test_PlaceOptionsSystemsV2.py -v --tb=short

# By group
python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "BlackScholes"
python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "ImpliedVol"
python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "QuoteHandling"
python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "ComputeDynamicK"
python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "ResolveK"
python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "Logging"
python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "EdgeCases"

# Regression tests only
python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "regression"

# Dynamic K tests only
python3 -m pytest test_PlaceOptionsSystemsV2.py -v -k "BlackScholes or ImpliedVol or QuoteHandling or ComputeDynamicK or ResolveK or Logging or EdgeCases"
```

---

## Test Design Principles

1. **No network calls.** All Kite API interactions are mocked with `unittest.mock.MagicMock`.
2. **Math verified by finite differences.** Vega and theta scaling conventions are cross-checked against small perturbation ΔV/Δx, not just sign/direction.
3. **Production bug replay.** 9 tests replay exact production failures with real parameters — these must never regress.
4. **Quality gates are tested independently.** Each quote-quality gate (spread, staleness, dust premium, IV mismatch, crossed book) has its own test with only that gate triggering.
5. **Fallback chain is tested end-to-end.** resolveK tests verify that every failure mode (spot, quote, strike, IV, IV bounds) falls back to static k with the correct reason.
6. **Edge cases test for crashes, not correctness.** Extreme inputs (T→0, IV→max, inf Greeks) must not crash — the output may be clamped or None, but never an unhandled exception.
