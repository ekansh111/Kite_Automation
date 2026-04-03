# ITM Monthly Call Rollover System

## Overview

Automated system that maintains perpetual long ITM (in-the-money) call positions on NIFTY and BANKNIFTY. On monthly expiry day at 3:00 PM, it exits the current month's ITM call and rolls into the next month's ITM call.

**Strategy:** Always long a 4-5% ITM monthly call. No stoploss. Always holding. Position sized by daily volatility target from dynamic capital.

**File:** `itm_call_rollover.py`

---

## Architecture

```
                    ┌──────────────────────────────────┐
                    │   realized_pnl_accumulator.json   │
                    │   (single source of truth)        │
                    └──────────────┬───────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                     │
    itm_call_rollover     PlaceOptionsSystemsV2   forecast_orchestrator
              │                    │                     │
              ▼                    ▼                     ▼
    effective = base_capital + cumulative_realized + eod_unrealized
```

All three consumers share the same capital formula and read from the same JSON file.

---

## Capital Tracking Model

### Source of Truth

`realized_pnl_accumulator.json` (located at `Directories.workInputRoot`):

```json
{
  "fy_start": "2026-04-01",
  "cumulative_realized_pnl": 150000.0,
  "eod_unrealized": -30000.0,
  "last_updated": "2026-04-02T18:30:00",
  "daily_entries": {}
}
```

Updated at end-of-day by `daily_pnl_report.py`.

### Formula

```
effective_capital = base_capital + cumulative_realized_pnl + eod_unrealized
```

- `base_capital`: From `instrument_config.json` → `account.base_capital` (currently 9,999,999)
- `cumulative_realized_pnl`: All booked P&L since FY start (April 1)
- `eod_unrealized`: Mark-to-market of open positions at close

### Null Safety

All three consumers use the `or 0.0` pattern:

```python
cumulative = float(pnl_data.get("cumulative_realized_pnl") or 0.0)
unrealized = float(pnl_data.get("eod_unrealized") or 0.0)
```

**Why not `dict.get(key, default)`?** When the JSON key exists with value `null`, `.get("key", 0.0)` returns `None` (not `0.0`). The `or 0.0` pattern handles this edge case.

### Fallback

If the JSON file is missing or corrupt:

```python
except (FileNotFoundError, json.JSONDecodeError):
    cumulative = db.GetCumulativeRealizedPnl()  # DB-only, no unrealized
    unrealized = 0.0
```

---

## Strike Selection

### Candidate Generation (`ComputeITMCallCandidates`)

Generates strikes 4-5% below spot:

```
NIFTY spot = 22,713 → candidates: 21,550 to 21,850 (step 50)
BANKNIFTY spot = 51,549 → candidates: 48,900 to 49,500 (step 100)
```

If no candidates found in 4-5% range, widens to 3-6%.

### Price Validation (`ValidateContractPrice`)

Every candidate goes through two checks before being considered:

**Check 1: Intrinsic Value Bounds**

```
intrinsic = max(spot - strike, 0)
upper_limit = intrinsic * 1.35   (35% above — max overpay)
lower_limit = intrinsic * 0.95   (5% below — suspicious underpay)

PASS if: lower_limit <= premium <= upper_limit
```

**Check 2: Black-Scholes Theoretical**

```
IV source priority:
  1. India VIX (live from broker)
  2. Implied from market premium (Newton-Raphson solver)
  3. Fallback: 15% annualised

BS_theo = bsPrice(spot, strike, T, IV, "CE")
deviation = |premium - BS_theo| / BS_theo * 100

PASS if: deviation <= 12%
```

### Ranking (`SelectBestITMStrike`)

Valid candidates ranked by **value score** (cheapest relative to BS theoretical):

```
value_pct = (market_premium - BS_theo) / BS_theo * 100
```

Most negative = best value (underpriced). Returns 5-tuple:

```python
(strike, tradingsymbol, lot_size, premium, SelectionMeta)
```

`SelectionMeta` contains:
- `best`: the winning candidate's full data
- `all_candidates`: all valid candidates with scores
- `rejected`: candidates that failed validation with reasons

### Spread Filter

Candidates with bid-ask spread > 3% of mid-price are skipped entirely (illiquid).

---

## Position Sizing

### Volatility Budget

Computed via `vol_target.compute_daily_vol_target()`:

```
daily_vol_budget = effective_capital * annual_vol_pct * product(weights) / 16
```

Where 16 = sqrt(256) annualization factor.

ITM call allocation weights (from `instrument_config.json → options_allocation`):
- `sector_weight`: 0.30
- `asset_weight`: 0.15
- `asset_DM`: 3.90
- `neg_skew_discount`: 0.50

### K Value Table (`K_TABLE_SINGLE`)

Maps days-to-expiry to a volatility scaling factor:

| DTE Range | K Value | Context |
|-----------|---------|---------|
| 22-45     | 0.18    | Monthly territory (deep ITM, low gamma) |
| 15-21     | 0.25    | 2-3 weeks out |
| 8-14      | 0.35    | 1-2 weeks out |
| 5-7       | 0.50    | Expiry week |
| 3-4       | 0.60    | Near expiry |
| 2         | 0.80    | Penultimate day |
| 1         | 1.00    | Expiry day |

Fallback: DTE > 45 → K=0.18 (smallest), DTE < 1 → K=1.00 (largest).

### Lot Calculation (`ComputePositionSizeITM`)

```
Step 1: dailyVolPerLot = K * premium * lot_size
Step 2: allowedLots = floor(budget / dailyVolPerLot)
Step 3: finalLots = max(1, allowedLots)
```

Uses `floor()` (not `round()`) because there's no stoploss — overshooting the vol budget compounds over ~22 trading days.

---

## Execution Flow

### Normal Rollover (Expiry Day, 3:00 PM)

```
1. EstablishKiteSession() → Kite client for OFS653
2. Load instruments, find monthly expiries
3. IsMonthlyExpiryDay() → verify today is monthly expiry
4. LoadVolBudgets() → read JSON accumulator, compute daily budgets
5. For each index (NIFTY, BANKNIFTY):
   a. ComputeITMCallCandidates() → strike list
   b. SelectBestITMStrike() → best strike with validation
   c. ComputePositionSizeITM() → lot count
   d. Leg 1: SELL current month call (SmartChaseExecute)
   e. Leg 2: BUY next month call (SmartChaseExecute)
   f. Update state, log to DB, send email
```

### First Run (`--first-run`)

Buy only (Leg 2), no exit. Used for cold start when no existing position.

### Dry Run (`--dry-run`)

Log all decisions but don't place orders or save state.

---

## State Management

**File:** `itm_call_state.json` (in WorkDirectory)

```json
{
  "NIFTY": {
    "status": "HOLDING",
    "current_contract": "NIFTY26APR21850CE",
    "current_expiry": "2026-04-28",
    "lots": 1,
    "quantity": 65,
    "entry_price": 1156.75,
    "entry_date": "2026-04-03",
    "order_tag": "ITM_ROLL"
  },
  "BANKNIFTY": { ... }
}
```

### State Recovery

If state file is lost, `RecoverStateFromPositions()` scans broker positions:
- Matches by index prefix + CE + NRML + NFO + current month expiry
- Disambiguates multiple matches via `ITM_ROLL` order tag

---

## Email Notifications

Styled to match the existing straddle V2 email format:

- Navy/blue color palette (`#003366`, `#2E75B6`)
- Sectioned layout: Header → Status Banner → Contract & Market Data → Position Sizing → K Value → Strike Selection → Price Validation → Leg 1/2 → Roll Summary
- `<meta charset="utf-8">` for rupee symbol (₹)
- `_fmtEmail()` helper for number formatting

### Critical Alerts

- **Leg 2 failure**: "CRITICAL: {Index} ITM Call LEG 2 FAILED — FLAT" (you're unhedged)
- **No valid candidates**: Rollover aborted with rejection details

---

## Configuration

### `instrument_config.json`

ITM call allocation under `options_allocation`:

```json
"NIFTY_ITM_CALL": {
  "vol_weights": {
    "sector_weight": 0.3,
    "asset_weight": 0.15,
    "asset_DM": 3.9,
    "neg_skew_discount": 0.5
  }
}
```

### `options_execution_config.json`

Smart chase parameters for NIFTY_OPT / BANKNIFTY_OPT.

### Constants in `itm_call_rollover.py`

| Constant | Value | Purpose |
|----------|-------|---------|
| `INTRINSIC_OVERPAY_MAX_PCT` | 35% | Max premium above intrinsic |
| `INTRINSIC_UNDERPAY_MIN_PCT` | 5% | Min premium below intrinsic |
| `BS_DEVIATION_MAX_PCT` | 12% | Max deviation from BS theoretical |
| `BS_FALLBACK_IV` | 15% | Fallback if no VIX/implied IV |
| `MAX_SPREAD_PCT` | 3% | Skip if bid-ask spread wider |

---

## Safety & Edge Cases

| Scenario | Behavior |
|----------|----------|
| Not expiry day | Script exits (unless `--force`) |
| No candidates pass validation | Abort roll, email alert |
| Leg 1 OK, Leg 2 fails | CRITICAL email — position is flat |
| All spreads > 3% | No valid candidates, abort |
| VIX unavailable | Implied IV from market price, then 15% fallback |
| JSON file missing/corrupt | DB fallback (`GetCumulativeRealizedPnl`) |
| JSON key is `null` | `or 0.0` handles it safely |
| State file corrupt | Reset to defaults |
| Multiple positions found | Disambiguate via order tag, or abort |

---

## Test Coverage

### `test_itm_call_rollover.py` — 102 tests

| Test Class | Count | Covers |
|------------|-------|--------|
| TestGetMonthlyExpiries | 5 | Expiry detection, filtering |
| TestIsMonthlyExpiryDay | 3 | Today vs monthly vs weekly |
| TestGetNextMonthExpiry | 3 | Next month lookup |
| TestComputeITMCallCandidates | 4 | Strike generation, widening |
| TestSelectBestITMStrike | 6 | Selection, spread filter, validation |
| TestValidateContractPrice | 9 | Intrinsic, BS, edge cases |
| TestComputePositionSizeITM | 7 | Sizing, minimums, zero inputs |
| TestKTableSingleExtension | 7 | K value for all DTE ranges |
| TestLookupKFallback | 4 | DTE outside table bounds |
| TestStateManagement | 4 | Save/load/corrupt/missing |
| TestRecoverStateFromPositions | 9 | Recovery, filtering, disambiguation |
| TestBuildOrderDict | 2 | Order construction |
| TestDatabaseOperations | 7 | Logging, querying, status updates |
| TestVolBudgetLoading | 6 | JSON accumulator, PnL effects |
| TestCountTradingDays | 4 | Weekends, holidays |
| TestBuildRolloverEmailHtml | 2 | Success/failure emails |
| TestExecuteRolloverDryRun | 2 | Dry run behavior |
| TestExecuteRolloverLive | 4 | Full rollover, leg failures |
| TestConfigIntegrity | 4 | Config file validation |
| TestIsTradingDay | 4 | Weekdays, weekends, holidays |
| TestEdgeCases | 6 | Constants, defaults, config keys |

### `test_capital_model.py` — 41 tests

| Test Class | Count | Covers |
|------------|-------|--------|
| TestITMCallCapitalFromJSON | 12 | ITM consumer reads JSON correctly |
| TestV2CapitalFromJSON | 9 | Straddle V2 consumer |
| TestOrchestratorCapitalFromJSON | 6 | Forecast orchestrator consumer |
| TestCrossConsumerConsistency | 6 | All 3 agree on capital |
| TestJSONEdgeCases | 8 | Null, empty, corrupt, string values |

### Running Tests

```bash
# All tests together (cross-file isolation verified)
pytest test_capital_model.py test_itm_call_rollover.py -v

# Individual
pytest test_itm_call_rollover.py -v
pytest test_capital_model.py -v
```

---

## Files Modified

| File | Changes |
|------|---------|
| `itm_call_rollover.py` | Email rewrite (V2 style), SelectionMeta 5-tuple, ValidateContractPrice, `or 0.0` null safety, DASH variable for Python 3.9 compat |
| `PlaceOptionsSystemsV2.py` | `or 0.0` null safety in `_load_vol_budgets()`, enhanced docstring |
| `forecast_orchestrator.py` | `or 0.0` null safety in capital computation |
| `test_itm_call_rollover.py` | 5-tuple unpacking, enriched email test, vol budget JSON tests, cross-file isolation |
| `test_capital_model.py` | **NEW** — 41 tests for shared capital model across all 3 consumers |

---

## CLI Usage

```bash
python itm_call_rollover.py                    # Normal run (3 PM on expiry)
python itm_call_rollover.py --dry-run          # Log decisions only
python itm_call_rollover.py --force            # Force rollover regardless of date
python itm_call_rollover.py --first-run        # Cold start: buy only
python itm_call_rollover.py --index=NIFTY      # Run for one index only
python itm_call_rollover.py --status           # Print current state
```

---

## Broker Accounts

- **OFS653** (Zerodha): Used for options trading (ITM calls + straddles)
- Execution via `SmartChaseExecute` for optimal limit-order fills
