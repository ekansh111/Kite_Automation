# PlaceOptionsSystemsV2 — Test & Design Document

## Overview

**Test file:** `test_PlaceOptionsSystemsV2.py`
**Total tests:** 116
**Framework:** pytest
**Run command:** `python3 -m pytest test_PlaceOptionsSystemsV2.py -v`

The test suite covers the V2 options trading system: core position sizing, state machine, reconciliation, exit logic, and the 4-scenario dynamic k framework (Black-Scholes, IV solver, quote handling, scenario-based k computation, and integration).

---

## Part A: Dynamic K Framework — Design & Theory

### A.1 What is K?

K is a multiplier that controls position sizing. The formula is:

```
dailyVolPerLot = k × combinedPremium × lotSize
allowedLots   = dailyVolBudget / dailyVolPerLot
```

Higher k = higher estimated daily risk per lot = fewer lots allowed = more conservative sizing.

K answers: **"how many multiples of the premium I collected could I lose in one bad day?"**

### A.2 Static K vs Dynamic K

**Static K** uses a hardcoded lookup table based on DTE:

| DTE | Static K | Theory |
|-----|----------|--------|
| 5-7 | 0.40 | 1/sqrt(6) approx 0.41 |
| 3-4 | 0.50 | 1/sqrt(4) = 0.50 |
| 2 | 0.70 | 1/sqrt(2) approx 0.71 |
| 1 | 1.00 | 1/sqrt(1) = 1.00 |

Static K is the same regardless of market conditions. It doesn't know if VIX is 12 or 35, whether the market is crashing right now, or what the actual option premiums and greeks are.

**Dynamic K** computes k from live market data using Black-Scholes Greeks and scenario-based stress testing. It adapts to current IV, VIX regime, and intraday market conditions.

### A.3 The Four Scenarios

Instead of computing one blended P&L number, the framework runs four independent "what if" scenarios and sizes off the worst one.

#### Scenario 1: kBase — "Normal day"

```
P&L = delta * deltaS + 0.5 * gamma * deltaS^2 + theta * 1day
```

- deltaS = expected 1-day move = spot * IV * sqrt(1/252)
- No IV shock applied
- This is the calm day scenario

**Example** (NIFTY 23000, IV=21%, 2 DTE):
```
Expected move = 23000 * 0.21 * sqrt(1/252) = 304 pts
Gamma P&L    = 0.5 * (-0.001854) * 304^2 = -85.7
Theta gain   = +85.8
Net P&L      = +0.1   (theta almost exactly offsets gamma)
kBase        = |0.1| / 344 = 0.00
```

On a normal day, theta saves you. That's the whole thesis of selling options. But sizing off kBase alone would be reckless.

#### Scenario 2: kStressMove — "Fat-tail move, IV stays flat"

```
P&L = delta * (1.5 * deltaS) + 0.5 * gamma * (1.5 * deltaS)^2 + theta * 1day
```

- Spot moves 50% more than expected (a bad-but-not-rare day)
- No IV shock — vol stays flat
- Gamma P&L scales quadratically: 1.5x move = 2.25x gamma loss

**Example** (same setup):
```
Stress move  = 1.5 * 304 = 456 pts
Gamma P&L    = 0.5 * (-0.001854) * 456^2 = -192.8
Theta gain   = +85.8
Net P&L      = -107.0   (gamma overwhelms theta)
kStressMove  = 107 / 344 = 0.31
```

#### Scenario 3: kStressVol — "IV spikes, normal move" (policy-driven vol stress)

```
P&L = delta * deltaS + 0.5 * gamma * deltaS^2 + vega * ivShock + theta * 1day
```

- Normal 1-sigma spot move
- IV increases by the policy-driven shock amount
- This is the vol spike scenario

**Example** (same setup, ivShock = 18vp = 0.18):
```
Gamma P&L    = -85.7
Vega P&L     = (-1635) * 0.18 = -294.3   (short vega, IV spike hurts)
Theta gain   = +85.8
Net P&L      = -294.2   (vega loss dominates)
kStressVol   = 294.2 / 344 = 0.85
```

#### Scenario 4: kCrash — "Big move AND IV spike" (the true bad day)

```
P&L = delta * (1.5 * deltaS) + 0.5 * gamma * (1.5 * deltaS)^2 + vega * ivShock + theta * 1day
```

- Spot moves 1.5x expected AND IV spikes simultaneously
- This is the combined crash scenario — in real crashes, spot drops hard and IV surges at the same time (they're correlated)

**Example** (same setup):
```
Gamma P&L    = -192.8
Vega P&L     = -294.3
Theta gain   = +85.8
Net P&L      = -401.3   (gamma AND vega both hit you)
kCrash       = 401.3 / 344 = 1.17
```

#### The Binding Rule

```
kForSizing = max(kBase, kStressMove, kStressVol, kCrash)
           = max(0.00,  0.31,        0.85,       1.17)
           = 1.17  (kCrash binds)
```

You size for the **worst** scenario, not the average. If gamma risk says k=0.31 but the combined crash says k=1.17, you don't blend — you use 1.17.

#### Which scenario binds when

| Condition | Usually binding | Why |
|-----------|----------------|-----|
| 1 DTE | kCrash (gamma-dominated) | Gamma explodes near expiry, quadratic losses from big moves |
| 2-3 DTE | kCrash (vega-dominated) | Moderate gamma but meaningful vega, vol spike is the bigger threat |
| 4-7 DTE, calm | kCrash or kStressVol | Depends on VIX regime and shock size |
| High VIX + intraday move | kCrash | Large additive IV shock compounds with move |

#### Clamping: applied once to final value

Raw k values are computed unclamped for all 4 scenarios. The binding scenario is determined from raw values. Floor (K_FLOOR = 0.20) and ceiling (K_CEILING = 5.00) are applied only once to the final kForSizing.

- **K_FLOOR = 0.20**: prudence guard — never undersize below this, even if the model says "riskless"
- **K_CEILING = 5.00**: data-quality guard — catches garbage quotes or numerical glitches, not a risk limit

Both raw and clamped values are logged for transparency.

### A.4 How DTE Affects the Scenarios

DTE flows through **two separate channels**:

**Channel 1 — The Greeks themselves (automatic, no tuning)**

When `bsGreeks(S, K, T, r, sigma, optType)` is called, `T = DTE/252`. This single variable controls how gamma, vega, and theta scale:

| Greek | As DTE shrinks | Effect |
|-------|---------------|--------|
| Gamma | Explodes up | A given spot move creates much larger P&L |
| Vega | Collapses down | IV spikes hurt less (less time for vol to matter) |
| Theta | Explodes up | You collect more decay per day |

At 1 DTE, gamma is enormous — the 1.5x move scenario creates quadratic losses. At 4 DTE, gamma is moderate but vega is substantial — the vol spike scenario dominates. The `max()` binding rule picks the right risk driver automatically without any if/else logic.

**Channel 2 — The IV shock size (policy choice, our tables)**

The base shock increases at shorter DTE because IV swings are more violent near expiry:

| DTE | Base shock |
|-----|-----------|
| 3-7 | 10 vol points |
| 2 | 12 vol points |
| 1 | 15 vol points |

### A.5 IV Shock: Policy-Driven Vol Stress

The IV shock is a **risk policy** — a deliberate conservative stress assumption, not a forecast or a theorem. It answers: "how much could IV spike in a bad scenario?"

#### Additive formula with cap

```
ivShock = baseShockByDTE + vixAddon + intradayMoveAddon
```

Capped at `IV_SHOCK_CAP_VP = 30` vol points (0.30 decimal).

Each component is independent and additive:

#### Component 1: Base shock by DTE

| DTE range | Base shock (vol points) | Rationale |
|-----------|------------------------|-----------|
| 3 to 7 | 10vp | Further from expiry, IV is more stable |
| 2 | 12vp | Getting closer, IV can gap more |
| 1 | 15vp | Near expiry, IV swings wildly in hours |

Shorter-dated options see more violent IV swings. A 0-DTE option can see IV jump 20+ points in minutes during a sell-off; a 5-DTE option barely moves 5 points in the same event.

#### Component 2: VIX addon

Fetched live via `kite.ltp(["NSE:INDIA VIX"])`.

| VIX range | Regime | Addon (vol points) |
|-----------|--------|---------------------|
| 0-14 | Calm | +0vp |
| 14-18 | Normal | +2vp |
| 18-24 | Elevated | +4vp |
| 24-30 | Stressed | +6vp |
| 30+ | Panic | +8vp |

When VIX is already high, the next IV spike tends to be larger. VIX=12 bad day might add 5vp; VIX=28 bad day can add 15vp.

Fail-safe: if VIX fetch fails, returns (0.0, None) — no addon applied.

#### Component 3: Intraday move addon

Fetched live via `kite.ohlc()` — computes `|lastPrice - openPrice| / openPrice`.

| Intraday move | Addon (vol points) |
|--------------|---------------------|
| 0-0.5% | +0vp |
| 0.5-1.0% | +2vp |
| 1.0-1.5% | +4vp |
| 1.5%+ | +6vp |

This is calculated **every time resolveK runs** using today's live open and current price. At 9:30 AM it might be 0.2% (no addon). By 12:30 PM after a sell-off it could be 1.3% (+4vp). It responds to stress happening right now.

Fail-safe: if OHLC fetch fails, returns (0.0, None) — no addon applied.

#### Why the cap at 30vp

Without a cap, extreme combinations could produce unrealistic shocks:
```
VIX=35 (panic)     +8vp
1 DTE              +15vp
2% intraday move   +6vp
                   = 29vp  (just under cap)
```

With future additions (termAdd, eventAdd), totals could exceed 35-40vp. A 40vp shock on a 20% IV option means IV jumps from 20% to 60% — tripling. At that point the Taylor expansion approximation breaks down. The cap bounds the input to where the model is reliable.

#### Example: IV shock build-up

**NIFTY, 2 DTE, VIX=20, market already down 0.6%:**
```
ivShock = 12vp (base) + 4vp (VIX=20 elevated) + 2vp (0.6% move)
        = 18vp = 0.18 decimal
```

**NIFTY, 1 DTE, VIX=27, market down 1.4%:**
```
ivShock = 15vp (base) + 6vp (VIX=27 stressed) + 4vp (1.4% move)
        = 25vp = 0.25 decimal
```

### A.6 Why K Decreases at Higher VIX

This is counterintuitive but correct. K is a **ratio**: `|stress P&L| / premium collected`.

At high VIX:
- Premium is much larger (you collect more for selling options)
- Gamma is actually **lower** (gamma is inversely proportional to IV: `gamma = N'(d1) / (S * sigma * sqrt(T))`)
- The premium cushion grows faster than the stress losses

**Example at 1 DTE, NIFTY 23000:**

| | VIX=10 (IV=11%) | VIX=30 (IV=38%) |
|---|---|---|
| Premium collected | 128 | 440 |
| Stress move (1.5x) | 239 pts | 827 pts |
| Gamma per option | 0.0026 | 0.00074 |
| Crash P&L (absolute) | ~253 | ~563 |
| **k = P&L / premium** | **1.98** | **1.28** |

The absolute loss at VIX=30 is 2.2x larger, but the premium is 3.4x larger. The denominator grows faster.

**But** — k is only part of the sizing formula:

| | VIX=10 | VIX=30 |
|---|---|---|
| k | 1.98 | 1.28 |
| premium | 128 | 440 |
| dailyVol/lot (k * premium * 65) | 16,474 | 36,608 |
| **Lots allowed** (64k budget) | **3** | **1** |

Even though k is lower at VIX=30, you still get **fewer lots** because premium per lot is much higher. The absolute rupee risk is controlled by the `premium * lotSize` term, not by k alone.

### A.7 Reference: kForSizing Across VIX and DTE (NIFTY, spot=23000)

Standard conditions assumed: IV proportional to VIX, calm intraday for low VIX, stressed intraday for high VIX.

| DTE | VIX=10 | VIX=15 | VIX=20 | VIX=25 | VIX=30 | Static K |
|-----|--------|--------|--------|--------|--------|----------|
| 5 | 1.03 | 0.87 | 0.88 | 0.72 | 0.65 | 0.40 |
| 4 | 1.06 | 0.90 | 0.92 | 0.75 | 0.68 | 0.50 |
| 3 | 1.11 | 0.96 | 0.97 | 0.81 | 0.73 | 0.50 |
| 2 | 1.40 | 1.18 | 1.17 | 0.98 | 0.89 | 0.70 |
| 1 | 1.98 | 1.68 | 1.62 | 1.39 | 1.28 | 1.00 |
| 0 | 3.85 | 3.55 | 3.49 | 3.26 | 3.15 | 1.00 |

**Observations:**
1. kCrash binds in every cell — the combined crash scenario is always worst for short straddles
2. k increases as DTE shrinks — gamma explodes near expiry
3. k is higher at low VIX — thin premiums mean stress losses are a larger multiple of what you collected
4. Dynamic k is consistently more conservative than static k — the static table undersizes risk
5. 0 DTE is extremely dangerous — k = 3-4x, meaning you could lose 3-4x the premium collected

### A.8 The Taylor Expansion P&L Model

All scenarios use the same 1-day Taylor expansion for option P&L:

```
deltaV = delta * deltaS + 0.5 * gamma * deltaS^2 + vega * deltaSigma + theta * deltaT
```

Where:
- `delta * deltaS`: directional P&L (near zero for ATM straddle, delta is roughly neutral)
- `0.5 * gamma * deltaS^2`: convexity P&L (always negative for short gamma — this is what hurts)
- `vega * deltaSigma`: vol P&L (negative for short vega when IV rises)
- `theta * deltaT`: time decay (positive for short options — this is what you collect)

For a **short straddle**, the position-level signs are:
- posGamma = negative (short gamma — moves hurt you)
- posVega = negative (short vega — IV spikes hurt you)
- posTheta = positive (long theta — you collect time decay)
- posDelta = near zero (ATM straddle is approximately delta-neutral)

**Note on theta**: Theta is included in all four scenarios, including kStressMove and kCrash. This is intentional — even on a crash day, one day of theta still passes. It would be incorrect to exclude it.

### A.9 The resolveK Pipeline

`resolveK()` is the orchestrator that ties everything together:

```
1. Check useDynamicK flag → if False, return static k
2. Fetch spot price via kite.ltp()
3. Fetch full quotes via kite.quote() (bid, ask, LTP, depth, timestamp)
4. Extract best premiums (mid-price preferred, bid/ask/LTP fallback)
5. Quality gates:
   a. Spread gate: bid-ask spread < 30% of mid
   b. Dust gate: premium >= 0.50 INR
   c. Staleness gate: quote timestamp < 60 seconds old (market hours only)
   d. IV consistency gate: |ceIV - peIV| / avgIV < 50%
6. Look up strike price from instruments cache
7. Solve implied volatility via Newton-Raphson (bisection fallback)
8. Compute Black-Scholes Greeks
9. Build IV shock: base + vixAddon + intradayAddon (capped at 30vp)
10. Run computeDynamicK() → 4 scenarios → max → kForSizing
11. Clamp to [K_FLOOR, K_CEILING]
12. Return (kValue, metadata)
```

If any step fails, it falls back to static k with a logged reason. The system is designed to never crash — it degrades gracefully to the static table.

### A.10 Future Extensions (Not Yet Implemented)

The IV shock formula is designed to accommodate additional add-ons:

```
ivShock = baseShockByDTE + vixAddon + termAddon + intradayMoveAddon + eventAddon
```

- **termAddon**: IV term structure slope — if near-term IV is much higher than next-week IV, add extra shock (requires pricing two expiries)
- **eventAddon**: known event risk — RBI policy, budget, F&O expiry clustering (requires event calendar)

These would slot into the existing additive formula as zero-valued add-ons until implemented.

---

## Part B: Test Classes Summary

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
| 13 | TestComputeDynamicK | 17 | Dynamic K — 4-Scenario K Computation |
| 14 | TestResolveK | 8 | Dynamic K — Orchestration |
| 15 | TestDynamicKLogging | 3 | Dynamic K — Logging |
| 16 | TestEdgeCases | 3 | Dynamic K — Edge Cases |
| | **Total** | **116** | |

---

## Part C: Test Details

### Group 1: Core V2 System (54 tests)

#### TestLookupK (8 tests)

Tests the static K = 1/sqrt(T) table lookup used as fallback for position sizing.

| Test | What it verifies |
|------|-----------------|
| `test_dte_1_peak_gamma` | DTE=1 returns K=1.00 (highest gamma risk) |
| `test_dte_2` | DTE=2 returns K=0.70 |
| `test_dte_3` | DTE=3 returns K=0.50 |
| `test_dte_4` | DTE=4 returns K=0.50 |
| `test_dte_5` | DTE=5 returns K=0.40 |
| `test_dte_7_upper_bound` | DTE=7 returns K=0.40 |
| `test_dte_0_raises` | DTE=0 raises ValueError |
| `test_dte_8_raises` | DTE=8 (out of range) raises ValueError |

#### TestComputePositionSize (6 tests)

Tests the daily-vol position sizing formula: `lots = round_half_up(budget / (K * premium * lotSize))`.

| Test | What it verifies |
|------|-----------------|
| `test_nifty_4d_typical` | Typical NIFTY 4-day: budget/vol gives correct lots |
| `test_maxlots_cap_applied` | Max lots cap is enforced |
| `test_round_half_up_not_bankers` | Uses round-half-up (not Python's banker's rounding) |
| `test_zero_premium_skipped` | Zero premium returns 0 lots (no division by zero) |
| `test_budget_too_small_for_one_lot` | Budget below 1 lot returns 0 |
| `test_sensex_2d_k1` | SENSEX lot size (20) with K=1.0 |

#### TestIsWithinTimeWindow (6 tests)

Tests the smart time window: sleep-before (<=2min) and tolerance-after (<=5min).

| Test | What it verifies |
|------|-----------------|
| `test_exact_target_time` | Exactly at target time -> True |
| `test_3min_after_proceeds` | 3 minutes after target -> True |
| `test_7min_after_rejects` | 7 minutes after target -> False |
| `test_1min_before_sleeps_until_target` | 1 min before -> sleeps, then True |
| `test_5min_before_rejects` | 5 min before -> False |
| `test_1230_window` | 12:30 window works correctly |

#### TestStateTransitions (5 tests)

| Test | What it verifies |
|------|-----------------|
| `test_transition_to_early_open` | noPosition -> earlyOpen |
| `test_transition_to_late_open` | noPosition -> lateOpen |
| `test_exit_from_early_goes_to_no_position` | earlyOpen -> noPosition (4D exit) |
| `test_exit_from_late_goes_to_completed_cycle` | lateOpen -> completedCycle |
| `test_exit_from_repair_goes_to_no_position` | repairRequired -> noPosition |

#### TestResetCompletedCycle (4 tests)

| Test | What it verifies |
|------|-----------------|
| `test_new_expiry_resets_to_no_position` | New expiry cycle resets state |
| `test_same_expiry_stays_completed` | Same expiry stays completedCycle |
| `test_let_expire_safety_net_resets_in_one_step` | letExpire safety net works |
| `test_no_position_unaffected` | noPosition state unaffected by reset |

#### TestReconciliation (7 tests)

| Test | What it verifies |
|------|-----------------|
| `test_all_contracts_verified` | All V2 contracts found at broker -> OK |
| `test_all_contracts_missing_warns` | All missing -> warn only (no auto-repair) |
| `test_partial_contracts_missing_warns` | Partial missing -> warn |
| `test_other_system_positions_dont_trigger_repair` | Non-V2 positions ignored |
| `test_no_positions_on_either_side_ok` | Both sides empty -> OK |
| `test_banknifty_filtered_by_prefix` | BANKNIFTY not confused with NIFTY |
| `test_broker_api_failure_graceful` | API failure handled gracefully |

#### TestExitPreflight (5 tests)

| Test | What it verifies |
|------|-----------------|
| `test_scenario_a_all_legs_sl_triggered` | All legs SL'd -> cancel GTTs, no orders |
| `test_scenario_b_partial_sl_closes_survivor` | Partial SL -> close only surviving legs |
| `test_scenario_c_normal_exit` | All legs present -> normal exit |
| `test_broker_api_failure_aborts_safely` | API failure aborts exit safely |
| `test_gtt_cancel_failure_is_non_blocking` | GTT cancel failure logged but non-blocking |

#### TestComputeTradingDte (5 tests)

| Test | What it verifies |
|------|-----------------|
| `test_expiry_day_is_zero` | Expiry day = DTE 0 |
| `test_monday_to_tuesday` | Mon->Tue = 1 trading day |
| `test_friday_to_tuesday_skips_weekend` | Fri->Tue = 1 (skips Sat/Sun) |
| `test_wednesday_to_next_tuesday` | Wed->Tue = 4 trading days |
| `test_holiday_skipped` | Holidays excluded from count |

#### TestRegressionBugs (9 tests) — Production Bug Replays

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

### Group 2: Dynamic K Framework (62 tests)

#### TestBlackScholes (11 tests)

Tests Black-Scholes European option pricing and Greeks computation.

| Test | What it verifies |
|------|-----------------|
| `test_bsPrice_call_put_parity` | C - P = S - K*e^(-rT) (put-call parity, no dividends) |
| `test_bsPrice_monotonic_with_iv` | Higher IV -> higher option price |
| `test_bsPrice_monotonic_with_time` | Longer time -> higher option price |
| `test_bsGreeks_atm_call` | ATM call: delta approx 0.5, gamma>0, theta<0, vega>0 |
| `test_bsGreeks_atm_put` | ATM put: delta approx -0.5 |
| `test_bsGreeks_deep_itm_call` | Deep ITM call: delta -> 1.0 |
| `test_bsGreeks_deep_otm_put` | Deep OTM put: delta -> 0.0 |
| `test_bsGreeks_near_expiry_gamma_rises` | Near expiry: ATM gamma spikes |
| `test_bsGreeks_vega_scaling_convention` | Vega = raw dV/d(sigma) (finite-difference verified) |
| `test_bsGreeks_theta_scaling_convention` | Theta reported per calendar day (finite-difference verified) |

**Key conventions verified:**
- Theta = premium change per 1 calendar day
- Vega = raw dV/d(sigma) (multiply by delta-sigma in absolute decimal to get P&L)
- No dividend yield (q=0), appropriate for NIFTY/SENSEX index options

#### TestImpliedVol (8 tests)

Tests the Newton-Raphson + bisection IV solver.

| Test | What it verifies |
|------|-----------------|
| `test_roundtrip_call` | Price CE with known IV -> solve back -> IV matches within 0.001 |
| `test_roundtrip_put` | Price PE with known IV -> solve back -> IV matches within 0.001 |
| `test_newton_fallback_to_bisection` | Newton diverges -> bisection converges correctly |
| `test_rejects_zero_price` | Premium=0 -> returns None |
| `test_rejects_negative_price` | Premium<0 -> returns None |
| `test_rejects_below_intrinsic` | Premium < intrinsic value -> returns None |
| `test_rejects_bad_time` | T=0 or T<0 -> returns None |
| `test_respects_bounds` | IV solution within [IV_SOLVER_MIN, IV_SOLVER_MAX] |

#### TestQuoteHandling (12 tests)

Tests `getBestPremium()` premium sourcing and `resolveK()` quote-quality gates.

**getBestPremium tests (5):**

| Test | What it verifies |
|------|-----------------|
| `test_mid_when_bid_ask_valid` | Both bid and ask present -> returns mid-price |
| `test_bid_when_only_bid_valid` | Only bid available -> returns bid |
| `test_ask_when_only_ask_valid` | Only ask available -> returns ask |
| `test_ltp_fallback` | No depth -> returns LTP |
| `test_rejects_bid_greater_than_ask` | Crossed book (bid>ask) -> falls back to LTP |

**resolveK quote-quality gate tests (7):**

| Test | Gate tested | What it verifies |
|------|------------|-----------------|
| `test_resolveK_rejects_wide_spread` | Spread gate | Bid-ask spread > 30% -> static fallback |
| `test_resolveK_rejects_bid_greater_than_ask` | Crossed book | bid > ask -> static fallback |
| `test_resolveK_rejects_near_zero_premium` | Dust gate | Premium < MIN_PREMIUM_INR -> static fallback |
| `test_resolveK_rejects_mixed_mid_and_stale_ltp` | Quality consistency | CE=mid, PE=ltp -> static fallback |
| `test_resolveK_rejects_stale_quote_during_market_hours` | Staleness gate | Quote > QUOTE_STALE_SECONDS old -> fallback |
| `test_resolveK_allows_old_quote_outside_market_hours` | Staleness bypass | Outside market hours -> staleness check skipped |
| `test_resolveK_rejects_iv_mismatch_between_legs` | IV consistency | CE IV and PE IV differ > 50% of average -> fallback |

#### TestComputeDynamicK (17 tests)

Tests the `computeDynamicK()` function: 4-scenario sizing with Taylor expansion P&L.

| Test | What it verifies |
|------|-----------------|
| `test_returns_all_scenario_k_values` | Returns kBase, kStressMove, kStressVol, kCrash, kForSizing, kBindingScenario |
| `test_atm_straddle_sanity` | Typical ATM (spot=24000, IV=14%, DTE=3): kForSizing in sane range |
| `test_clamp_floor` | kForSizing >= K_FLOOR (0.20) after clamping |
| `test_clamp_ceiling` | kForSizing <= K_CEILING (5.00) after clamping |
| `test_near_expiry_higher_than_far_expiry` | DTE=1 kForSizing > DTE=5 kForSizing (gamma effect) |
| `test_kForSizing_is_max_of_scenarios` | kForSizing == max(kBase, kStressMove, kStressVol, kCrash) |
| `test_kStressMove_gte_kBase` | kStressMove >= kBase (bigger move = more loss) |
| `test_zero_iv_shock_kStressVol_equals_kBase` | ivShockAbsolute=0 -> kStressVol == kBase (no vol stress) |
| `test_positive_iv_shock_increases_kStressVol` | Positive IV shock -> kStressVol > kBase |
| `test_delta_neutral_straddle` | ATM straddle: net delta approx 0 |
| `test_kCrash_gte_kStressMove_and_kStressVol` | kCrash >= kStressMove AND kCrash >= kStressVol |
| `test_uses_absolute_pnl_for_k` | k uses |pnlPerUnit|, always positive |
| `test_expected_move_uses_avg_iv` | Expected move = spot * avgIV * sqrt(1/252) |
| `test_combined_premium_zero_rejected` | combinedPremium=0 -> returns None |
| `test_spot_zero_rejected` | spot=0 -> returns None |
| `test_avg_iv_zero_rejected` | avgIV=0 -> returns None |
| `test_nan_greeks_do_not_crash` | NaN/inf Greeks -> graceful handling |

**Key relationships tested:**
```
kBase       = |P&L(1x move, no shock)| / premium
kStressMove = |P&L(1.5x move, no shock)| / premium
kStressVol  = |P&L(1x move, ivShock)| / premium
kCrash      = |P&L(1.5x move, ivShock)| / premium
kForSizing  = max(kBase, kStressMove, kStressVol, kCrash)
```

#### TestResolveK (8 tests)

Tests the `resolveK()` orchestrator: market data -> Greeks/IV -> 4 scenarios -> fallback on failure.

| Test | What it verifies |
|------|-----------------|
| `test_returns_static_when_disabled` | useDynamicK=False -> static k, source="static" |
| `test_dynamic_happy_path` | Full flow: spot->quote->strike->IV->Greeks->4 scenarios->k |
| `test_fallback_when_spot_fetch_fails` | kite.ltp() throws -> static fallback |
| `test_fallback_when_quote_fetch_fails` | kite.quote() throws -> static fallback |
| `test_fallback_when_strike_lookup_fails` | Instruments cache miss -> static fallback |
| `test_fallback_when_iv_solve_fails` | IV solver returns None -> static fallback |
| `test_fallback_when_iv_near_bounds` | IV near solver bounds -> static fallback |
| `test_iv_shock_derived_from_sizing_dte` | IV shock looked up from IV_SHOCK_TABLE by sizingDte (not config) |

#### TestDynamicKLogging (3 tests)

Tests that dynamic k metadata flows correctly into entry logs and dry-run output.

| Test | What it verifies |
|------|-----------------|
| `test_logEntry_contains_dynamic_k_fields` | CSV row includes all scenario k fields and IV shock metadata |
| `test_logEntry_contains_fallback_reason` | Fallback reason written to CSV |
| `test_dry_run_prints_dynamic_k_breakdown` | --dry-run prints full scenario breakdown |

**Key CSV fields logged:** kSource, kForSizing, kBase, kStressMove, kStressVol, kCrash, kBindingScenario, kSpotSensitivity, staticK, avgIV, expectedMove, posGamma, posTheta, posVega, ceIV, peIV, timeToExpiryYears, ivShockApplied, ivShockBase, vixLevel, vixAddon, intradayMovePct, intradayAddon, ceBid, ceAsk, ceSpreadPct, peBid, peAsk, peSpreadPct

#### TestEdgeCases (3 tests)

| Test | What it verifies |
|------|-----------------|
| `test_very_small_time_does_not_crash` | T=1e-7: no crash, Greeks may be extreme but finite or handled |
| `test_very_high_iv_does_not_crash` | IV=4.99 (near solver max): price/Greeks compute without error |
| `test_inf_greeks_do_not_crash` | Infinite Greek values handled gracefully |

---

## Part D: Constants Reference

| Constant | Value | Purpose | Tested in |
|----------|-------|---------|-----------|
| K_FLOOR | 0.20 | Minimum k — prudence guard | TestComputeDynamicK::test_clamp_floor |
| K_CEILING | 5.00 | Maximum k — data-quality guard (not a risk limit) | TestComputeDynamicK::test_clamp_ceiling |
| STRESS_MOVE_MULTIPLIER | 1.5 | Spot move scaling for kStressMove and kCrash | TestComputeDynamicK::test_kStressMove_gte_kBase |
| IV_SHOCK_CAP_VP | 30 | Max IV shock in vol points — model validity bound | resolveK IV shock build-up |
| RISK_FREE_RATE | 0.07 | Risk-free rate for Black-Scholes | TestBlackScholes (all) |
| IV_SOLVER_MIN | 0.01 | Lower bound for IV solver | TestImpliedVol::test_respects_bounds |
| IV_SOLVER_MAX | 5.0 | Upper bound for IV solver | TestImpliedVol::test_respects_bounds |
| QUOTE_STALE_SECONDS | 60 | Max quote age during market hours | TestQuoteHandling::test_resolveK_rejects_stale_quote |
| BID_ASK_SPREAD_GATE | 0.30 | Max bid-ask spread (30% of mid) | TestQuoteHandling::test_resolveK_rejects_wide_spread |
| MIN_PREMIUM_INR | 0.50 | Reject dust premiums below this | TestQuoteHandling::test_resolveK_rejects_near_zero_premium |
| IV_SPREAD_GATE | 0.50 | Max |ceIV-peIV|/avgIV (50%) | TestQuoteHandling::test_resolveK_rejects_iv_mismatch |

---

## Part E: Running Tests

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

## Part F: Test Design Principles

1. **No network calls.** All Kite API interactions are mocked with `unittest.mock.MagicMock`.
2. **Math verified by finite differences.** Vega and theta scaling conventions are cross-checked against small perturbation dV/dx, not just sign/direction.
3. **Production bug replay.** 9 tests replay exact production failures with real parameters — these must never regress.
4. **Quality gates are tested independently.** Each quote-quality gate (spread, staleness, dust premium, IV mismatch, crossed book) has its own test with only that gate triggering.
5. **Fallback chain is tested end-to-end.** resolveK tests verify that every failure mode (spot, quote, strike, IV, IV bounds) falls back to static k with the correct reason.
6. **Edge cases test for crashes, not correctness.** Extreme inputs (T->0, IV->max, inf Greeks) must not crash — the output may be clamped or None, but never an unhandled exception.
7. **Scenario relationships are tested structurally.** kCrash >= kStressMove, kCrash >= kStressVol, kStressMove >= kBase, kForSizing == max of all four. These invariants hold regardless of inputs.
8. **Clamping tested separately from scenarios.** Raw scenario values are unclamped; floor/ceiling applied only to final kForSizing. Tests verify both raw ordering and clamped bounds.
