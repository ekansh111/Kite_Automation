"""
itm_call_rollover.py — Automated ITM Monthly Call Rollover System.

Standalone process that manages perpetual long ITM call positions on NIFTY
and BANKNIFTY. On monthly expiry day at 3:00 PM, exits the current month's
ITM call and rolls into the next month's ITM call.

Strategy:
  - Always long a 4-5% ITM monthly call (strike 4-5% below spot)
  - No stoploss — always holding
  - Position sized by daily vol target from dynamic capital
  - Executes via SmartChaseExecute for optimal fills

Usage:
    python itm_call_rollover.py                          # Normal run (3:00 PM on expiry day)
    python itm_call_rollover.py --dry-run                # Log decisions, no orders
    python itm_call_rollover.py --force                  # Force rollover regardless of date
    python itm_call_rollover.py --first-run              # Cold start: buy only, no exit
    python itm_call_rollover.py --index=NIFTY            # Run for one index only
    python itm_call_rollover.py --status                 # Print current state
"""

import argparse
import json
import logging
import math
import smtplib
import sys
import time
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from kiteconnect import KiteConnect

from Directories import (
    WorkDirectory,
    KiteEshitaLogin, KiteEshitaLoginAccessToken,
)
from Holidays import CheckForDateHoliday
from FetchOptionContractName import (
    GetInstrumentsCached,
    GetOptSegmentForExchange,
    GetBestMarketPremium,
    ChunkList,
)
from smart_chase import SmartChaseExecute
from vol_target import compute_daily_vol_target
from PlaceOptionsSystemsV2 import (
    lookupK, K_TABLE_SINGLE, bsPrice, bsImpliedVol, RISK_FREE_RATE,
    resolveKLongSingle, getRegimeAddon, lookupIvShock, getVixAddon,
)
import forecast_db as db

Logger = logging.getLogger("itm_call_rollover")

# ─── Configuration ───────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent / "instrument_config.json"
EXEC_CONFIG_PATH = Path(__file__).parent / "options_execution_config.json"
STATE_FILE_PATH = Path(WorkDirectory) / "itm_call_state.json"

ORDER_TAG = "ITM_ROLL"
USER = "OFS653"

ITM_CONFIG = {
    "NIFTY": {
        "underlying_ltp_key": "NSE:NIFTY 50",
        "exchange": "NFO",
        "strike_step": 50,
        "itm_pct_min": 4.0,
        "itm_pct_max": 5.0,
        "exec_config_key": "NIFTY_OPT",
        "alloc_key": "NIFTY_ITM_CALL",
    },
    "BANKNIFTY": {
        "underlying_ltp_key": "NSE:NIFTY BANK",
        "exchange": "NFO",
        "strike_step": 100,
        "itm_pct_min": 4.0,
        "itm_pct_max": 5.0,
        "exec_config_key": "BANKNIFTY_OPT",
        "alloc_key": "BANKNIFTY_ITM_CALL",
    },
}

VIX_LTP_KEY = "NSE:INDIA VIX"

# Email (same config as PlaceOptionsSystemsV2)
EMAIL_NOTIFY_ENABLED = True
EMAIL_FROM = "ekansh.n111@gmail.com"
EMAIL_FROM_PASSWORD = "sgwl lnvt hewf wplo"
EMAIL_TO = "ekansh.n@gmail.com"
EMAIL_SMTP = "smtp.gmail.com"
EMAIL_PORT = 465


# ─── Broker Session ─────────────────────────────────────────────────

def EstablishKiteSession():
    """Create a KiteConnect session for OFS653."""
    with open(KiteEshitaLogin, "r") as f:
        Lines = f.readlines()
        ApiKey = Lines[2].strip("\n")
    Kite = KiteConnect(api_key=ApiKey)
    with open(KiteEshitaLoginAccessToken, "r") as f:
        AccessToken = f.read().strip()
    Kite.set_access_token(AccessToken)
    Logger.info("Kite session established for %s", USER)
    return Kite


# ─── Trading Day Utilities ──────────────────────────────────────────

def IsTradingDay(D):
    """Return True if D is a weekday and not a market holiday."""
    if D.weekday() >= 5:
        return False
    if CheckForDateHoliday(D):
        return False
    return True


# ─── Monthly Expiry Detection ───────────────────────────────────────

def GetMonthlyExpiries(Instruments, IndexName, OptSegment):
    """Get all monthly expiry dates for an index from the instruments list.

    Monthly expiry = the last expiry date within a given calendar month.
    Returns a sorted list of dates.
    """
    ExpirySet = set()
    for Ins in Instruments:
        if Ins.get("segment") != OptSegment:
            continue
        if Ins.get("name") != IndexName:
            continue
        Exp = Ins.get("expiry")
        if Exp is not None:
            ExpirySet.add(Exp)

    if not ExpirySet:
        return []

    # Group by (year, month) and take the max of each group = monthly expiry
    ByMonth = {}
    for E in sorted(ExpirySet):
        Key = (E.year, E.month)
        ByMonth[Key] = E  # last (max) expiry in that month

    return sorted(ByMonth.values())


def IsMonthlyExpiryDay(Instruments, IndexName, OptSegment):
    """Check if today is a monthly options expiry day for IndexName.

    Returns (isExpiry: bool, expiryDate: date or None).
    """
    Today = date.today()
    MonthlyExpiries = GetMonthlyExpiries(Instruments, IndexName, OptSegment)

    for Exp in MonthlyExpiries:
        if Exp == Today:
            return True, Exp

    return False, None


def GetCurrentMonthExpiry(MonthlyExpiries):
    """Get the monthly expiry for the current calendar month."""
    Today = date.today()
    for Exp in MonthlyExpiries:
        if Exp.month == Today.month and Exp.year == Today.year:
            return Exp
    return None


def GetNextMonthExpiry(MonthlyExpiries, CurrentExpiry):
    """Get the first monthly expiry strictly after CurrentExpiry."""
    for Exp in MonthlyExpiries:
        if Exp > CurrentExpiry:
            return Exp
    return None


# ─── Strike Selection ───────────────────────────────────────────────

def ComputeITMCallCandidates(Spot, StrikeStep, ITMPctMin=4.0, ITMPctMax=5.0):
    """Generate candidate strikes in the 4-5% ITM range for calls.

    ITM call = strike below spot. 4% ITM = strike at 96% of spot.
    Returns list of strike values (ints), sorted ascending.
    """
    LowStrike = Spot * (100 - ITMPctMax) / 100   # 5% ITM = lower bound
    HighStrike = Spot * (100 - ITMPctMin) / 100   # 4% ITM = upper bound

    MinStrike = math.floor(LowStrike / StrikeStep) * StrikeStep
    MaxStrike = math.ceil(HighStrike / StrikeStep) * StrikeStep

    Candidates = list(range(MinStrike, MaxStrike + StrikeStep, StrikeStep))

    if not Candidates:
        # Widen to 3-6% ITM if initial range produces nothing
        Logger.warning("No candidates in %.1f-%.1f%% ITM range (spot=%.0f), widening to 3-6%%",
                       ITMPctMin, ITMPctMax, Spot)
        LowStrike = Spot * 0.94
        HighStrike = Spot * 0.97
        MinStrike = math.floor(LowStrike / StrikeStep) * StrikeStep
        MaxStrike = math.ceil(HighStrike / StrikeStep) * StrikeStep
        Candidates = list(range(MinStrike, MaxStrike + StrikeStep, StrikeStep))

    return Candidates


def SelectBestITMStrike(Kite, Instruments, IndexName, Exchange, OptSegment,
                        ExpiryDate, Candidates, Spot=None):
    """From candidate strikes, pick the one with tightest bid-ask spread.

    Each candidate is validated against intrinsic value and Black-Scholes
    theoretical price before being considered. Candidates that fail validation
    are skipped with a warning.

    Args:
        Spot: underlying spot price (required for price validation).

    Returns (strike, tradingsymbol, lotSize, premium) or raises Exception.
    """
    # Filter instruments to matching CE contracts at the target expiry
    MatchingInstruments = {}
    for Ins in Instruments:
        if Ins.get("segment") != OptSegment:
            continue
        if Ins.get("name") != IndexName:
            continue
        if Ins.get("expiry") != ExpiryDate:
            continue
        if Ins.get("instrument_type") != "CE":
            continue
        Strike = int(float(Ins.get("strike", 0)))
        if Strike in Candidates:
            MatchingInstruments[Strike] = Ins

    if not MatchingInstruments:
        raise Exception(f"No CE instruments found for {IndexName} expiry={ExpiryDate} "
                        f"strikes={Candidates}")

    # Fetch quotes for all candidates
    QuoteKeys = []
    StrikeToKey = {}
    for Strike, Ins in MatchingInstruments.items():
        Key = f"{Exchange}:{Ins['tradingsymbol']}"
        QuoteKeys.append(Key)
        StrikeToKey[Key] = Strike

    # Collect all valid candidates with their scores
    ValidCandidates = []
    RejectedStrikes = []

    for Chunk in ChunkList(QuoteKeys, 150):
        Quotes = Kite.quote(Chunk)
        time.sleep(0.2)

        for Qk in Chunk:
            Q = Quotes.get(Qk)
            if not Q:
                continue

            Strike = StrikeToKey[Qk]
            Ins = MatchingInstruments[Strike]

            # Get bid-ask for BUY side (we are buying calls)
            Premium = GetBestMarketPremium(Q, "BUY")
            if Premium <= 0:
                continue

            # Compute spread
            Depth = Q.get("depth", {})
            Buys = Depth.get("buy", [])
            Sells = Depth.get("sell", [])
            Bid = float(Buys[0].get("price", 0)) if Buys else 0
            Ask = float(Sells[0].get("price", 0)) if Sells else 0

            if Bid > 0 and Ask > 0:
                Mid = (Bid + Ask) / 2
                SpreadPct = (Ask - Bid) / Mid * 100 if Mid > 0 else 999
            else:
                # No order book depth — use LTP if available (post-hours / thin book)
                Ltp = float(Q.get("last_price", 0))
                if Ltp > 0:
                    Mid = Ltp
                    SpreadPct = -1  # signal: no live depth, using LTP
                    Premium = Ltp   # override premium to LTP
                    Logger.warning("[%s] Strike %d: no order book depth, using LTP %.2f",
                                   IndexName, Strike, Ltp)
                else:
                    SpreadPct = 999

            # Filter: skip if spread is too wide (SpreadPct=-1 means LTP fallback, allow through)
            if SpreadPct > MAX_SPREAD_PCT:
                Logger.warning("[%s] Strike %d skipped: spread %.1f%% > %.1f%% limit",
                               IndexName, Strike, SpreadPct, MAX_SPREAD_PCT)
                continue

            # Validate contract price against intrinsic + BS theoretical
            if Spot is not None and Spot > 0:
                ValPassed, ValDetails = ValidateContractPrice(
                    Spot, Strike, Premium, ExpiryDate, Kite)
                if not ValPassed:
                    Logger.warning("[%s] Strike %d REJECTED: %s",
                                   IndexName, Strike,
                                   "; ".join(ValDetails["checks_failed"]))
                    RejectedStrikes.append((Strike, ValDetails))
                    continue
            else:
                ValDetails = {"bs_theo": None}

            # Value score: how cheap is market premium vs BS theoretical?
            # Lower (more negative) = better value for buyer
            BsTheo = ValDetails.get("bs_theo") if ValDetails else None
            if BsTheo and BsTheo > 0:
                ValuePct = (Premium - BsTheo) / BsTheo * 100  # negative = underpriced
            else:
                ValuePct = 0  # no BS data — neutral score

            ValidCandidates.append({
                "strike": Strike,
                "symbol": Ins["tradingsymbol"],
                "lot_size": int(Ins.get("lot_size", 1)),
                "premium": Premium,
                "spread_pct": SpreadPct,
                "value_pct": ValuePct,
                "bs_theo": BsTheo,
                "validation": ValDetails,
            })

    if RejectedStrikes:
        Logger.warning("[%s] %d candidate(s) rejected by price validation",
                       IndexName, len(RejectedStrikes))

    if not ValidCandidates:
        RejectSummary = "; ".join(
            f"{s}: {'; '.join(d['checks_failed'])}" for s, d in RejectedStrikes
        )
        raise Exception(f"No valid CE contracts for {IndexName} expiry={ExpiryDate}. "
                        f"Rejected: {RejectSummary}")

    # Rank by value: pick the cheapest relative to BS theoretical (most negative value_pct)
    ValidCandidates.sort(key=lambda c: c["value_pct"])
    Best = ValidCandidates[0]

    # Log all valid candidates for transparency
    for C in ValidCandidates:
        Marker = " <<<" if C is Best else ""
        Logger.info("[%s]   strike=%d premium=%.2f bs_theo=%s value=%.1f%% spread=%.1f%%%s",
                    IndexName, C["strike"], C["premium"],
                    f"{C['bs_theo']:.2f}" if C["bs_theo"] else "N/A",
                    C["value_pct"], C["spread_pct"], Marker)

    BsTheoVal = Best.get("bs_theo")
    BsTheoStr = f"{BsTheoVal:.2f}" if isinstance(BsTheoVal, (int, float)) else "N/A"
    Logger.info("[%s] Selected strike=%d symbol=%s premium=%.2f value=%.1f%% "
                "spread=%.1f%% lotSize=%d bs_theo=%s",
                IndexName, Best["strike"], Best["symbol"], Best["premium"],
                Best["value_pct"], Best["spread_pct"], Best["lot_size"], BsTheoStr)

    SelectionMeta = {
        "best": Best,
        "all_candidates": ValidCandidates,
        "rejected": RejectedStrikes,
    }
    return Best["strike"], Best["symbol"], Best["lot_size"], Best["premium"], SelectionMeta


# ─── Contract Price Validation ──────────────────────────────────────

# Thresholds for price validation
INTRINSIC_OVERPAY_MAX_PCT = 35.0    # reject if market premium > intrinsic * (1 + this/100)
INTRINSIC_UNDERPAY_MIN_PCT = 5.0    # reject if market premium < intrinsic * (1 - this/100)
BS_DEVIATION_MAX_PCT = 12.0         # reject if |market - BS_theo| > this % of BS_theo
BS_FALLBACK_IV = 0.15               # 15% annualised — fallback if VIX unavailable
MAX_SPREAD_PCT = 3.0                # skip candidates with spread wider than this %


def ValidateContractPrice(Spot, Strike, Premium, ExpiryDate, Kite=None):
    """Validate that a quoted option premium is reasonable.

    Two checks:
      1. Intrinsic value: premium must be between 95-115% of intrinsic
         (deep ITM calls have small time value relative to intrinsic)
      2. Black-Scholes: premium must be within 12% of BS theoretical price
         (uses India VIX for IV if available, else 15% fallback)

    Returns:
        (passed: bool, details: dict) — details includes intrinsic, bs_theo, iv, reasons
    """
    Today = date.today()
    DaysToExpiry = (ExpiryDate - Today).days
    T = max(DaysToExpiry / 365.0, 1e-6)

    Intrinsic = max(Spot - Strike, 0.0)
    TimeValue = Premium - Intrinsic

    Details = {
        "spot": Spot, "strike": Strike, "premium": Premium,
        "intrinsic": Intrinsic, "time_value": TimeValue,
        "days_to_expiry": DaysToExpiry, "T": T,
        "bs_theo": None, "iv_used": None, "implied_iv": None,
        "checks_passed": [], "checks_failed": [],
    }

    # ── Check 1: Intrinsic value bounds ─────────────────────────
    if Intrinsic > 0:
        OverpayLimit = Intrinsic * (1 + INTRINSIC_OVERPAY_MAX_PCT / 100)
        UnderpayLimit = Intrinsic * (1 - INTRINSIC_UNDERPAY_MIN_PCT / 100)

        if Premium > OverpayLimit:
            Details["checks_failed"].append(
                f"INTRINSIC_OVERPAY: premium {Premium:.2f} > {OverpayLimit:.2f} "
                f"({INTRINSIC_OVERPAY_MAX_PCT}% above intrinsic {Intrinsic:.2f})")
        elif Premium < UnderpayLimit:
            Details["checks_failed"].append(
                f"INTRINSIC_UNDERPAY: premium {Premium:.2f} < {UnderpayLimit:.2f} "
                f"({INTRINSIC_UNDERPAY_MIN_PCT}% below intrinsic {Intrinsic:.2f})")
        else:
            Details["checks_passed"].append("INTRINSIC_OK")
    else:
        # Strike >= Spot — not really ITM, flag it
        Details["checks_failed"].append(
            f"NOT_ITM: strike {Strike} >= spot {Spot:.2f}, intrinsic=0")

    # ── Check 2: Black-Scholes theoretical price ────────────────
    # Get IV: try India VIX first, then imply from market price, else fallback
    IV = None

    # Try India VIX as proxy for IV
    if Kite is not None:
        try:
            VixData = Kite.ltp([VIX_LTP_KEY])
            VixLevel = float(VixData[VIX_LTP_KEY]["last_price"])
            IV = VixLevel / 100.0  # VIX is in percentage, BS needs decimal
            Details["iv_used"] = f"VIX={VixLevel:.1f}"
        except Exception:
            pass

    # If no VIX, try to imply IV from the market premium itself
    # (cross-check: compute IV → compute BS price → compare)
    if IV is None:
        ImpliedIV = bsImpliedVol(Premium, Spot, Strike, T, "CE")
        if ImpliedIV is not None:
            IV = ImpliedIV
            Details["implied_iv"] = ImpliedIV
            Details["iv_used"] = f"implied={ImpliedIV:.3f}"
        else:
            IV = BS_FALLBACK_IV
            Details["iv_used"] = f"fallback={BS_FALLBACK_IV}"

    # Compute BS theoretical price
    try:
        BsTheo = bsPrice(Spot, Strike, T, IV, "CE")
        Details["bs_theo"] = BsTheo

        if BsTheo > 0:
            DeviationPct = abs(Premium - BsTheo) / BsTheo * 100
            Details["bs_deviation_pct"] = DeviationPct

            if DeviationPct > BS_DEVIATION_MAX_PCT:
                Details["checks_failed"].append(
                    f"BS_DEVIATION: premium {Premium:.2f} vs theo {BsTheo:.2f} "
                    f"(dev={DeviationPct:.1f}% > {BS_DEVIATION_MAX_PCT}%)")
            else:
                Details["checks_passed"].append(
                    f"BS_OK (theo={BsTheo:.2f}, dev={DeviationPct:.1f}%)")
        else:
            Details["checks_passed"].append("BS_SKIP (theo<=0)")
    except Exception as E:
        Details["checks_failed"].append(f"BS_ERROR: {E}")

    Passed = len(Details["checks_failed"]) == 0
    return Passed, Details


# ─── Position Sizing ────────────────────────────────────────────────

def LoadVolBudgets():
    """Compute ITM call daily vol budgets from effective capital.

    Capital formula (shared with PlaceOptionsSystemsV2 and forecast_orchestrator):
        effective = base_capital + cumulative_realized + eod_unrealized
    Reads from realized_pnl_accumulator.json (EOD JSON), falls back to DB.
    """
    with open(CONFIG_PATH) as F:
        Cfg = json.load(F)
    Acct = Cfg["account"]
    BaseCapital = Acct["base_capital"]

    # Read realized + unrealized from EOD JSON accumulator, fall back to DB
    CumulativeRealized = 0.0
    EodUnrealized = 0.0
    _PnlPath = Path(__file__).parent / "realized_pnl_accumulator.json"
    try:
        from Directories import workInputRoot
        _PnlPath = Path(workInputRoot) / "realized_pnl_accumulator.json"
    except Exception:
        pass
    try:
        with open(_PnlPath, "r") as F2:
            PnlData = json.load(F2)
        CumulativeRealized = float(PnlData.get("cumulative_realized_pnl") or 0.0)
        EodUnrealized = float(PnlData.get("eod_unrealized") or 0.0)
    except (FileNotFoundError, json.JSONDecodeError):
        CumulativeRealized = db.GetCumulativeRealizedPnl()

    EffectiveCapital = BaseCapital + CumulativeRealized + EodUnrealized
    Logger.info("ITM call effective capital: base=%d + realized=%.0f + unrealized=%.0f = %.0f",
                BaseCapital, CumulativeRealized, EodUnrealized, EffectiveCapital)

    Budgets = {}
    for IndexName, IdxCfg in ITM_CONFIG.items():
        AllocKey = IdxCfg["alloc_key"]
        OptAlloc = Cfg.get("options_allocation", {}).get(AllocKey)
        if OptAlloc is None:
            Logger.warning("No options_allocation entry for %s, skipping", AllocKey)
            continue
        Budgets[IndexName] = compute_daily_vol_target(
            EffectiveCapital, Acct["annual_vol_target_pct"],
            OptAlloc["vol_weights"]
        )
        Logger.info("[%s] Daily vol budget: %.2f", IndexName, Budgets[IndexName])

    return Budgets, EffectiveCapital


def ComputePositionSizeITM(Premium, LotSize, KValue, DailyVolBudget,
                            MaxPremiumOutlay=None):
    """Compute number of lots for a single ITM call (per-index).

    dailyVolPerLot = k × premium × lotSize
    lots_vol = floor(budget / dailyVolPerLot)
    lots_cap = floor(MaxPremiumOutlay / (premium × lotSize))   [if cap provided]
    finalLots = max(1, min(lots_vol, lots_cap))

    Uses floor (not round) because positions are held to expiry with no
    stoploss — overshooting the vol budget compounds over ~22 trading days.

    Args:
        MaxPremiumOutlay: optional INR cap on total premium spend per index.
            If None, no cap applied. If set (e.g. 3% of capital), final lots
            cannot exceed floor(MaxPremiumOutlay / cost_per_lot).
    """
    if Premium <= 0 or LotSize <= 0 or KValue <= 0:
        return {"finalLots": 0, "skipped": True, "skipReason": "invalid inputs"}

    DailyVolPerLot = KValue * Premium * LotSize
    if DailyVolPerLot <= 0:
        return {"finalLots": 0, "skipped": True, "skipReason": "dailyVolPerLot zero"}

    LotsVol = int(DailyVolBudget / DailyVolPerLot)  # floor — never exceed budget
    CostPerLot = Premium * LotSize

    LotsCap = None
    if MaxPremiumOutlay is not None and MaxPremiumOutlay > 0:
        LotsCap = int(MaxPremiumOutlay / CostPerLot)

    if LotsCap is not None:
        AllowedLots = min(LotsVol, LotsCap)
        BindingConstraint = "vol-target" if LotsVol <= LotsCap else "premium-cap"
    else:
        AllowedLots = LotsVol
        BindingConstraint = "vol-target"

    FinalLots = max(1, AllowedLots)  # always at least 1 lot

    return {
        "finalLots": FinalLots,
        "allowedLots": AllowedLots,
        "lotsVol": LotsVol,
        "lotsCap": LotsCap,
        "bindingConstraint": BindingConstraint,
        "dailyVolPerLot": DailyVolPerLot,
        "costPerLot": CostPerLot,
        "premium": Premium,
        "kValue": KValue,
        "dailyVolBudget": DailyVolBudget,
        "maxPremiumOutlay": MaxPremiumOutlay,
        "skipped": False,
        "skipReason": None,
    }


# ─── Pooled Allocation (Balance-preserving, NIFTY tiebreaker) ──────────

POOL_ROUNDUP_THRESHOLD = 0.80  # accept up to 20% over-budget per lot
PRIMARY_INDEX = "NIFTY"        # tiebreaker preference


def AllocateLotsBalanced(SizingInputs, PoolRoundupThreshold=POOL_ROUNDUP_THRESHOLD,
                          PrimaryIndex=PRIMARY_INDEX):
    """Pool the per-index daily vol budgets and allocate lots across indices.

    Algorithm:
      1. Floor each index independently using its own per-index budget (min 1 each).
      2. Pool the leftover budget from both indices.
      3. Iteratively add lots:
         a) Identify primary index = the one with FEWER current lots
            (tie → PrimaryIndex, e.g. NIFTY).
         b) Try to add 1 lot to primary if leftover ≥ threshold × primary.dvpl
            AND new outlay ≤ primary.cap.
         c) If primary doesn't fit, fall back to secondary (utilization).
         d) Stop if neither fits.

    Args:
        SizingInputs: dict of {index_name: {dvpl, cost_per_lot, max_outlay,
                                            daily_budget_per_idx, floor_lots}}
                      (floor_lots already computed by ComputePositionSizeITM)
        PoolRoundupThreshold: 0.80 means buy lot if leftover ≥ 80% of dvpl
        PrimaryIndex: index name to favor on ties (default "NIFTY")

    Returns:
        {
          "allocations": {index_name: final_lots},
          "iterations": [list of dicts describing each round],
          "pooled_budget": total pool,
          "vol_used_total": sum of dvpl × lots,
          "leftover_pool": remaining unused vol budget,
          "over_budget": amount over (positive if pool exceeded),
        }
    """
    if not SizingInputs:
        return {"allocations": {}, "iterations": [], "pooled_budget": 0,
                "vol_used_total": 0, "leftover_pool": 0, "over_budget": 0}

    # Initialize from floor lots
    Allocations = {name: meta["floor_lots"] for name, meta in SizingInputs.items()}
    PooledBudget = sum(meta["daily_budget_per_idx"] for meta in SizingInputs.values())
    UsedTotal = sum(Allocations[name] * meta["dvpl"] for name, meta in SizingInputs.items())
    Leftover = PooledBudget - UsedTotal
    Iterations = []

    Names = list(SizingInputs.keys())

    while True:
        # Identify primary (fewer current lots), tie → PrimaryIndex
        sorted_by_lots = sorted(Names, key=lambda n: (Allocations[n],
                                                       0 if n == PrimaryIndex else 1))
        primary = sorted_by_lots[0]
        secondary = sorted_by_lots[1] if len(sorted_by_lots) > 1 else None

        Added = False
        for which in [primary] + ([secondary] if secondary else []):
            meta = SizingInputs[which]
            need = meta["dvpl"]
            new_outlay = (Allocations[which] + 1) * meta["cost_per_lot"]
            cap_ok = (meta["max_outlay"] is None) or (new_outlay <= meta["max_outlay"])
            threshold_ok = Leftover >= PoolRoundupThreshold * need

            if threshold_ok and cap_ok:
                Allocations[which] += 1
                Leftover -= need
                Iterations.append({
                    "round": len(Iterations) + 1,
                    "tried": which,
                    "preference": "primary" if which == primary else "fallback",
                    "need": need,
                    "had": Leftover + need,
                    "pct_of_need": (Leftover + need) / need,
                    "added": True,
                    "new_lots": dict(Allocations),
                })
                Added = True
                break
            else:
                # Log the rejection (kept terse)
                Iterations.append({
                    "round": len(Iterations) + 1,
                    "tried": which,
                    "preference": "primary" if which == primary else "fallback",
                    "need": need,
                    "had": Leftover,
                    "pct_of_need": Leftover / need if need > 0 else 0,
                    "added": False,
                    "reason": "below threshold" if not threshold_ok else "premium cap",
                })
        if not Added:
            break

    UsedTotal = sum(Allocations[name] * meta["dvpl"] for name, meta in SizingInputs.items())
    OverBudget = max(0, UsedTotal - PooledBudget)

    return {
        "allocations": Allocations,
        "iterations": Iterations,
        "pooled_budget": PooledBudget,
        "vol_used_total": UsedTotal,
        "leftover_pool": max(0, Leftover),
        "over_budget": OverBudget,
        "utilization_pct": (UsedTotal / PooledBudget * 100) if PooledBudget > 0 else 0,
    }


def CountTradingDaysUntilExpiry(ExpiryDate, FromDate=None):
    """Count trading days from FromDate (exclusive) to ExpiryDate (inclusive)."""
    if FromDate is None:
        FromDate = date.today()
    if isinstance(ExpiryDate, datetime):
        ExpiryDate = ExpiryDate.date()
    if isinstance(FromDate, datetime):
        FromDate = FromDate.date()
    Count = 0
    Current = FromDate + timedelta(days=1)
    while Current <= ExpiryDate:
        if IsTradingDay(Current):
            Count += 1
        Current += timedelta(days=1)
    return Count


# ─── State Management ───────────────────────────────────────────────

DEFAULT_STATE = {
    "NIFTY": {
        "status": "NONE",
        "current_contract": None,
        "current_expiry": None,
        "lots": 0,
        "quantity": 0,
        "entry_price": 0.0,
        "entry_date": None,
        "order_tag": ORDER_TAG,
    },
    "BANKNIFTY": {
        "status": "NONE",
        "current_contract": None,
        "current_expiry": None,
        "lots": 0,
        "quantity": 0,
        "entry_price": 0.0,
        "entry_date": None,
        "order_tag": ORDER_TAG,
    },
}


def LoadState():
    """Load state from JSON file. Returns default state if file missing/corrupt."""
    if not STATE_FILE_PATH.exists():
        Logger.info("State file not found, using defaults")
        return json.loads(json.dumps(DEFAULT_STATE))
    try:
        with open(STATE_FILE_PATH) as F:
            State = json.load(F)
        # Ensure both indices exist
        for Idx in ["NIFTY", "BANKNIFTY"]:
            if Idx not in State:
                State[Idx] = json.loads(json.dumps(DEFAULT_STATE[Idx]))
        return State
    except (json.JSONDecodeError, KeyError) as E:
        Logger.error("State file corrupt (%s), using defaults", E)
        return json.loads(json.dumps(DEFAULT_STATE))


def SaveState(State):
    """Persist state to JSON file."""
    with open(STATE_FILE_PATH, "w") as F:
        json.dump(State, F, indent=2, default=str)
    Logger.info("State saved to %s", STATE_FILE_PATH)


# ─── State Recovery from Positions ──────────────────────────────────

def RecoverStateFromPositions(Kite, IndexName, CurrentMonthExpiry):
    """Attempt to recover state by scanning broker positions.

    Matches by: (1) index prefix, (2) CE type, (3) NRML product,
    (4) NFO exchange, (5) expiry = current month's monthly expiry.
    Validates via kite.orders() for ITM_ROLL order tag.
    Requires exactly 1 match or returns None.
    """
    Logger.info("[%s] Attempting state recovery from broker positions", IndexName)

    try:
        Positions = Kite.positions()
    except Exception as E:
        Logger.error("[%s] Failed to fetch positions: %s", IndexName, E)
        return None

    NetPositions = Positions.get("net", [])

    # Filter for matching CE positions
    Matches = []
    for Pos in NetPositions:
        Symbol = Pos.get("tradingsymbol", "")
        if not Symbol.startswith(IndexName):
            continue
        if not Symbol.endswith("CE"):
            continue
        if Pos.get("product") != "NRML":
            continue
        if Pos.get("exchange") != "NFO":
            continue
        Qty = int(Pos.get("quantity", 0))
        if Qty <= 0:
            continue
        # Check expiry matches current month
        PosExpiry = Pos.get("expiry")
        if PosExpiry and isinstance(PosExpiry, date):
            if PosExpiry != CurrentMonthExpiry:
                continue
        Matches.append(Pos)

    if len(Matches) == 0:
        Logger.warning("[%s] No matching positions found for recovery", IndexName)
        return None

    if len(Matches) > 1:
        # Try to disambiguate via order tag
        try:
            Orders = Kite.orders()
            TaggedSymbols = set()
            for Ord in Orders:
                if Ord.get("tag") == ORDER_TAG:
                    TaggedSymbols.add(Ord.get("tradingsymbol"))

            TaggedMatches = [M for M in Matches if M["tradingsymbol"] in TaggedSymbols]
            if len(TaggedMatches) == 1:
                Matches = TaggedMatches
            else:
                Logger.error("[%s] Multiple positions match (%d), cannot auto-recover. "
                             "Manual state file update required.", IndexName, len(Matches))
                return None
        except Exception as E:
            Logger.error("[%s] Failed to fetch orders for disambiguation: %s", IndexName, E)
            return None

    Pos = Matches[0]
    Logger.info("[%s] Recovered position: %s qty=%s", IndexName,
                Pos["tradingsymbol"], Pos["quantity"])
    return {
        "status": "HOLDING",
        "current_contract": Pos["tradingsymbol"],
        "current_expiry": str(CurrentMonthExpiry),
        "lots": 0,  # unknown from recovery
        "quantity": int(Pos["quantity"]),
        "entry_price": float(Pos.get("average_price", 0)),
        "entry_date": None,
        "order_tag": ORDER_TAG,
    }


# ─── Order Building ─────────────────────────────────────────────────

def BuildOrderDict(IndexName, ContractSymbol, Action, Quantity):
    """Build order dict compatible with SmartChaseExecute."""
    return {
        "Tradetype": Action,
        "Exchange": "NFO",
        "Tradingsymbol": ContractSymbol,
        "Quantity": str(Quantity),
        "Variety": "REGULAR",
        "Ordertype": "LIMIT",
        "Product": "NRML",
        "Validity": "DAY",
        "Price": "0",
        "Broker": "ZERODHA",
        "User": USER,
        "ContractNameProvided": "True",
        "OrderTag": ORDER_TAG,
        "TradeFailExitRequired": "False",
    }


# ─── Email ──────────────────────────────────────────────────────────

def _fmtEmail(val, decimals=2):
    """Format a number for email display."""
    if val is None or val == "":
        return "N/A"
    try:
        return f"{float(val):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(val)


def BuildRolloverEmailHtml(IndexName, Result):
    """Build detailed HTML email for ITM call rollover, matching straddle V2 style."""
    Now = datetime.now()
    IsSuccess = Result.get("success", False)

    # ── Colour palette (matches straddle V2) ──
    navy = "#003366"
    accent = "#2E75B6"
    green = "#27AE60"
    red = "#E74C3C"
    grey_bg = "#F8F9FA"
    border_col = "#DEE2E6"

    StatusColor = green if IsSuccess else red
    StatusText = "ROLLOVER COMPLETE" if IsSuccess else "ROLLOVER FAILED"

    Leg1 = Result.get("leg1") or {}
    Leg2 = Result.get("leg2") or {}
    SizeResult = Result.get("size_result") or {}
    SelectionMeta = Result.get("selection") or {}
    Best = SelectionMeta.get("best") or {}
    AllCandidates = SelectionMeta.get("all_candidates") or []
    RejectedStrikes = SelectionMeta.get("rejected") or []

    DTE = Result.get("dte", "?")
    KValue = Result.get("k_value", 0)
    LotSize = Result.get("lot_size", "?")
    Premium = Result.get("premium", 0)
    Spot = Result.get("spot", 0)
    Strike = Result.get("strike", "—")
    DailyVolBudget = Result.get("daily_vol_budget", 0)
    EffCapital = Result.get("effective_capital", "?")
    NextExpiry = Result.get("next_expiry", "?")
    CurrentExpiry = Result.get("current_expiry", "?")

    Html = f"""
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin:0;padding:0;font-family:Arial,Helvetica,sans-serif;background:#EAECEE;">
      <div style="max-width:680px;margin:20px auto;background:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1);">

        <!-- Header -->
        <div style="background:{navy};padding:20px 28px;">
          <h1 style="margin:0;color:#FFFFFF;font-size:20px;letter-spacing:0.5px;">
            ITM Call Rollover &mdash; {IndexName}
          </h1>
          <p style="margin:6px 0 0;color:#AAC4E0;font-size:13px;">
            Monthly Roll &bull; {Now.strftime('%d %b %Y, %I:%M %p')} &bull; {CurrentExpiry} &rarr; {NextExpiry}
          </p>
        </div>

        <!-- Status Banner -->
        <div style="background:{StatusColor};padding:10px 28px;">
          <span style="color:#FFFFFF;font-size:13px;font-weight:600;">
            {StatusText}
          </span>
        </div>

        <!-- Contract & Market Data -->
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Contract &amp; Market Data
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">New Contract</td>
              <td style="padding:8px 12px;font-family:monospace;">{Result.get('symbol', '—')}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Spot Price</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(Spot, 1)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Strike Selected</td>
              <td style="padding:8px 12px;">{Strike}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">ITM Depth</td>
              <td style="padding:8px 12px;">{f"{((Spot - Strike) / Spot * 100):.1f}%" if isinstance(Strike, (int, float)) and Spot > 0 else "—"}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Premium (mid)</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(Premium)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">BS Theoretical</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(Best.get('bs_theo'))}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Value Score</td>
              <td style="padding:8px 12px;font-weight:700;color:{green if Best.get('value_pct', 0) <= 0 else red};">{_fmtEmail(Best.get('value_pct'), 1)}%</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Spread</td>
              <td style="padding:8px 12px;">{_fmtEmail(Best.get('spread_pct'), 1)}%</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">DTE (trading days)</td>
              <td style="padding:8px 12px;">{DTE}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Lot Size</td>
              <td style="padding:8px 12px;">{LotSize}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Effective Capital</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(EffCapital, 0)}</td>
            </tr>
          </table>
        </div>
    """

    # ── Position Sizing Formula ──
    DailyVolPerLot = SizeResult.get("dailyVolPerLot", 0)
    AllowedLots = SizeResult.get("allowedLots", "?")
    FinalLots = SizeResult.get("finalLots", "?")
    TotalQty = FinalLots * LotSize if isinstance(FinalLots, int) and isinstance(LotSize, int) else "?"

    Html += f"""
        <!-- Position Sizing Formula -->
        <div style="padding:20px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Position Sizing Formula
          </h2>

          <!-- Step 1: dailyVolPerLot -->
          <div style="background:{grey_bg};border:1px solid {border_col};border-radius:6px;padding:16px 18px;margin-bottom:12px;">
            <p style="margin:0 0 6px;font-weight:700;font-size:13px;color:{navy};">Step 1: Daily Volatility Per Lot</p>
            <p style="margin:0 0 4px;font-size:11px;color:#666;">How much P&amp;L volatility does one lot produce on a single day?</p>
            <table style="border-collapse:collapse;font-size:13px;margin-top:8px;">
              <tr>
                <td style="padding:4px 0;font-family:monospace;color:#333;">dailyVolPerLot</td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#333;">K &times; premium &times; lotSize</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#555;">{_fmtEmail(KValue, 4)} &times; {_fmtEmail(Premium)} &times; {LotSize}</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;font-weight:700;font-size:15px;color:{navy};">\u20B9{_fmtEmail(DailyVolPerLot)}</td>
              </tr>
            </table>
          </div>

          <!-- Step 2: allowedLots -->
          <div style="background:{grey_bg};border:1px solid {border_col};border-radius:6px;padding:16px 18px;margin-bottom:12px;">
            <p style="margin:0 0 6px;font-weight:700;font-size:13px;color:{navy};">Step 2: Allowed Lots</p>
            <p style="margin:0 0 4px;font-size:11px;color:#666;">How many lots fit within the daily volatility budget? Uses floor() &mdash; never exceeds budget.</p>
            <table style="border-collapse:collapse;font-size:13px;margin-top:8px;">
              <tr>
                <td style="padding:4px 0;font-family:monospace;color:#333;">allowedLots</td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#333;">floor(budget / dailyVolPerLot)</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#555;">floor(\u20B9{_fmtEmail(DailyVolBudget, 0)} / \u20B9{_fmtEmail(DailyVolPerLot)})</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;font-weight:700;font-size:15px;color:{navy};">{AllowedLots} lots</td>
              </tr>
            </table>
          </div>

          <!-- Step 3: finalLots -->
          <div style="background:#E8F5E9;border:2px solid {green};border-radius:6px;padding:16px 18px;">
            <p style="margin:0 0 6px;font-weight:700;font-size:13px;color:{navy};">Step 3: Final Lots (min 1)</p>
            <p style="margin:0 0 4px;font-size:11px;color:#666;">Always at least 1 lot. No upper cap &mdash; vol budget is the only constraint.</p>
            <table style="border-collapse:collapse;font-size:13px;margin-top:8px;">
              <tr>
                <td style="padding:4px 0;font-family:monospace;color:#333;">finalLots</td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#333;">max(1, allowedLots)</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#555;">max(1, {AllowedLots})</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-weight:700;font-size:18px;color:{green};">{FinalLots} lots &nbsp;({TotalQty} qty)</td>
              </tr>
            </table>
          </div>
        </div>

        <!-- K Value section (dynamic or static) -->
        <div style="padding:24px 28px 0;">
    """

    # Branch: Dynamic K (full breakdown) vs Static K (existing format)
    KMetadata = Result.get("k_metadata")
    if KMetadata and KMetadata.get("source") == "dynamic":
        Greeks = KMetadata.get("optGreeks", {})
        IvShockBase = (KMetadata.get("ivShockBase") or 0) * 100
        IvShockVix = (KMetadata.get("ivShockVixAddon") or 0) * 100
        IvShockRegime = (KMetadata.get("ivShockRegimeAddon") or 0) * 100
        IvShockTotal = (KMetadata.get("ivShockTotal") or 0) * 100
        VixLevel = KMetadata.get("vixLevel", "?")
        RegimeRatio = KMetadata.get("regimeRatio", "?")
        OptIV = (KMetadata.get("optIV") or 0) * 100
        ExpMove = KMetadata.get("expectedMove", 0) or 0
        BindingScen = KMetadata.get("kBindingScenario", "?")
        KBase = KMetadata.get("kBase", 0) or 0
        KStress = KMetadata.get("kStressMove", 0) or 0
        KCrush = KMetadata.get("kVegaCrush", 0) or 0
        PnlBreak = KMetadata.get("pnlBreakdown", {})
        BasePnl = PnlBreak.get("basePnl", 0) or 0
        StressPnl = PnlBreak.get("stressMovePnl", 0) or 0
        VegaCrushPnl = PnlBreak.get("vegaCrushPnl", 0) or 0
        BindStyleBase = f"background:{accent};color:#FFF;font-weight:600;" if BindingScen == "kBase" else ""
        BindStyleStress = f"background:{accent};color:#FFF;font-weight:600;" if BindingScen == "kStressMove" else f"background:{grey_bg};"
        BindStyleCrush = f"background:{accent};color:#FFF;font-weight:600;" if BindingScen == "kVegaCrush" else ""
        ColorBase = "#FFF" if BindingScen == "kBase" else red
        ColorStress = "#FFF" if BindingScen == "kStressMove" else red
        ColorCrush = "#FFF" if BindingScen == "kVegaCrush" else red
        ArrowBase = " \u25C0 binds" if BindingScen == "kBase" else ""
        ArrowStress = " \u25C0 binds" if BindingScen == "kStressMove" else ""
        ArrowCrush = " \u25C0 binds" if BindingScen == "kVegaCrush" else ""
        RegimeColor = red if IvShockRegime < 0 else "#333"
        RegimeSign = "+" if IvShockRegime >= 0 else ""
        SpotMovePct = ExpMove / Spot * 100 if Spot > 0 else 0
        Util = (DailyVolPerLot * FinalLots / DailyVolBudget * 100
                if DailyVolBudget > 0 and isinstance(FinalLots, int) else 0)

        Html += f"""
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Dynamic K Computation
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">Solved IV</td>
              <td style="padding:8px 12px;font-weight:700;color:{navy};">{_fmtEmail(OptIV, 2)}%</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Delta (\u0394)</td>
              <td style="padding:8px 12px;">{_fmtEmail(Greeks.get('delta'), 4)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Gamma (\u0393)</td>
              <td style="padding:8px 12px;">{_fmtEmail(Greeks.get('gamma'), 6)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Vega</td>
              <td style="padding:8px 12px;">{_fmtEmail(Greeks.get('vega'), 1)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Theta (per day)</td>
              <td style="padding:8px 12px;">{_fmtEmail(Greeks.get('theta'), 2)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">1\u03c3 daily move</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(ExpMove, 0)}/share ({_fmtEmail(SpotMovePct, 2)}%)</td>
            </tr>
          </table>

          <table style="width:100%;border-collapse:collapse;font-size:11px;margin-top:14px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:4px 10px;font-weight:600;">Scenario</td>
              <td style="padding:4px 10px;font-weight:600;text-align:center;">Spot move</td>
              <td style="padding:4px 10px;font-weight:600;text-align:center;">IV chg</td>
              <td style="padding:4px 10px;font-weight:600;text-align:right;">P&amp;L/share</td>
              <td style="padding:4px 10px;font-weight:600;text-align:right;">K</td>
            </tr>
            <tr style="{BindStyleBase}">
              <td style="padding:6px 10px;">kBase{ArrowBase}</td>
              <td style="padding:6px 10px;text-align:center;">-1\u03c3 (-\u20B9{_fmtEmail(ExpMove, 0)})</td>
              <td style="padding:6px 10px;text-align:center;">0</td>
              <td style="padding:6px 10px;text-align:right;color:{ColorBase};">{_fmtEmail(BasePnl)}</td>
              <td style="padding:6px 10px;text-align:right;">{_fmtEmail(KBase, 4)}</td>
            </tr>
            <tr style="{BindStyleStress}">
              <td style="padding:6px 10px;">kStressMove{ArrowStress}</td>
              <td style="padding:6px 10px;text-align:center;">-1.5\u03c3 (-\u20B9{_fmtEmail(ExpMove*1.5, 0)})</td>
              <td style="padding:6px 10px;text-align:center;">0</td>
              <td style="padding:6px 10px;text-align:right;color:{ColorStress};">{_fmtEmail(StressPnl)}</td>
              <td style="padding:6px 10px;text-align:right;">{_fmtEmail(KStress, 4)}</td>
            </tr>
            <tr style="{BindStyleCrush}">
              <td style="padding:6px 10px;">kVegaCrush{ArrowCrush}</td>
              <td style="padding:6px 10px;text-align:center;">-1\u03c3 (-\u20B9{_fmtEmail(ExpMove, 0)})</td>
              <td style="padding:6px 10px;text-align:center;">-{_fmtEmail(IvShockTotal, 0)} vp</td>
              <td style="padding:6px 10px;text-align:right;color:{ColorCrush};">{_fmtEmail(VegaCrushPnl)}</td>
              <td style="padding:6px 10px;text-align:right;">{_fmtEmail(KCrush, 4)}</td>
            </tr>
          </table>

          <div style="background:#E8F5E9;border:2px solid {green};border-radius:6px;padding:16px 18px;margin-top:14px;">
            <p style="margin:0 0 6px;font-weight:700;font-size:13px;color:{navy};">
              K_use = max(kBase, kStressMove, kVegaCrush)
            </p>
            <table style="border-collapse:collapse;font-size:13px;margin-top:8px;">
              <tr>
                <td style="padding:4px 0;font-family:monospace;">max({_fmtEmail(KBase, 4)}, {_fmtEmail(KStress, 4)}, {_fmtEmail(KCrush, 4)})</td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-weight:700;font-size:18px;color:{green};">{_fmtEmail(KValue, 4)}</td>
              </tr>
            </table>
            <p style="margin:8px 0 0;font-size:11px;color:#666;">
              Daily vol budget: \u20B9{_fmtEmail(DailyVolBudget, 0)} &bull; Daily vol per lot: \u20B9{_fmtEmail(DailyVolPerLot)}
              &bull; Utilization: {_fmtEmail(Util, 1)}%
            </p>
          </div>
        </div>

        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            IV Shock Construction
          </h2>
          <div style="background:{grey_bg};border:1px solid {border_col};border-radius:6px;padding:16px 18px;">
            <table style="border-collapse:collapse;font-size:13px;width:100%;">
              <tr>
                <td style="padding:6px 0;width:65%;">Base shock (DTE bucket {DTE} trading days)</td>
                <td style="padding:6px 0;text-align:right;font-family:monospace;">+{_fmtEmail(IvShockBase, 0)} vp</td>
              </tr>
              <tr>
                <td style="padding:6px 0;">+ VIX addon (VIX = {VixLevel})</td>
                <td style="padding:6px 0;text-align:right;font-family:monospace;">+{_fmtEmail(IvShockVix, 0)} vp</td>
              </tr>
              <tr>
                <td style="padding:6px 0;">+ Regime addon (vol_recent / vol_baseline = {RegimeRatio}\u00d7)</td>
                <td style="padding:6px 0;text-align:right;font-family:monospace;color:{RegimeColor};">{RegimeSign}{_fmtEmail(IvShockRegime, 0)} vp</td>
              </tr>
              <tr><td colspan="2" style="border-top:1px solid {border_col};padding-top:8px;"></td></tr>
              <tr>
                <td style="padding:6px 0;font-weight:700;color:{navy};">Total IV shock applied</td>
                <td style="padding:6px 0;text-align:right;font-weight:700;font-size:15px;color:{navy};font-family:monospace;">{_fmtEmail(IvShockTotal, 0)} vp = {_fmtEmail(IvShockTotal/100, 2)} decimal</td>
              </tr>
            </table>
          </div>
        </div>
    """
    else:
        Html += f"""
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            K Value &mdash; STATIC
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">K for Sizing</td>
              <td style="padding:8px 12px;font-weight:700;font-size:15px;color:{navy};">{_fmtEmail(KValue, 4)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Source</td>
              <td style="padding:8px 12px;">K_TABLE_SINGLE (DTE={DTE})</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Daily Vol Budget</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(DailyVolBudget, 0)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Daily Vol / Lot</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(DailyVolPerLot)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Budget Utilisation</td>
              <td style="padding:8px 12px;">{_fmtEmail(DailyVolPerLot * FinalLots / DailyVolBudget * 100 if DailyVolBudget > 0 and isinstance(FinalLots, int) else 0, 1)}%</td>
            </tr>
          </table>
    """
        KTableRows = ""
        for MinDte, MaxDte, KVal in K_TABLE_SINGLE:
            Label = f"{MinDte} DTE" if MinDte == MaxDte else f"{MinDte}\u2013{MaxDte} DTE"
            IsActive = isinstance(DTE, int) and MinDte <= DTE <= MaxDte
            Style = f"background:{accent};color:#FFF;font-weight:600;" if IsActive else ""
            Arrow = " \u25C0" if IsActive else ""
            KTableRows += f'<tr><td style="padding:4px 10px;{Style}">{Label}</td><td style="padding:4px 10px;text-align:center;{Style}">{KVal}{Arrow}</td></tr>'

        Html += f"""
          <table style="width:100%;border-collapse:collapse;font-size:11px;margin-top:12px;">
            <tr style="background:{navy};color:#FFF;"><td style="padding:4px 10px;font-weight:600;" colspan="2">K_TABLE_SINGLE</td></tr>
            {KTableRows}
          </table>
        </div>
    """

    AllocMeta = Result.get("allocation_meta")
    if AllocMeta and AllocMeta.get("pool"):
        Pool = AllocMeta["pool"]
        IterRows = ""
        for It in Pool.get("iterations", []):
            BgStyle = "background:#E8F5E9;" if It.get("added") else f"background:{grey_bg};"
            ActionColor = green if It.get("added") else red
            ActionLabel = f"+1 {It['tried']} \u2705" if It.get("added") else f"\u2716 {It.get('reason', 'rejected')}"
            PrefStr = It.get("preference", "?")
            IterRows += f"""
              <tr style="{BgStyle}">
                <td style="padding:6px 10px;">{It['round']}</td>
                <td style="padding:6px 10px;">{It['tried']}</td>
                <td style="padding:6px 10px;">{PrefStr}</td>
                <td style="padding:6px 10px;text-align:right;font-family:monospace;">\u20B9{_fmtEmail(It['need'], 0)}</td>
                <td style="padding:6px 10px;text-align:right;font-family:monospace;">\u20B9{_fmtEmail(It['had'], 0)} ({_fmtEmail(It['pct_of_need']*100, 0)}%)</td>
                <td style="padding:6px 10px;text-align:center;color:{ActionColor};font-weight:600;">{ActionLabel}</td>
              </tr>
            """

        Allocations = Pool.get("allocations", {})
        AllocStr = " + ".join(f"{v} {k}" for k, v in Allocations.items())

        Html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Pooled Allocation (Balance-preserving, NIFTY tiebreaker)
          </h2>

          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">Pooled Daily Budget</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(Pool.get('pooled_budget', 0), 0)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Vol Used (this allocation)</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(Pool.get('vol_used_total', 0), 0)} ({_fmtEmail(Pool.get('utilization_pct', 0), 1)}%)</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Pool Leftover (unused)</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(Pool.get('leftover_pool', 0), 0)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Round-up Threshold</td>
              <td style="padding:8px 12px;">{int(POOL_ROUNDUP_THRESHOLD*100)}% of dvpl (allows up to {int((1-POOL_ROUNDUP_THRESHOLD)*100)}% over-budget per lot)</td>
            </tr>
          </table>

          <table style="width:100%;border-collapse:collapse;font-size:11px;margin-top:14px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:4px 10px;font-weight:600;">Iter</td>
              <td style="padding:4px 10px;font-weight:600;">Tried</td>
              <td style="padding:4px 10px;font-weight:600;">Pref</td>
              <td style="padding:4px 10px;font-weight:600;text-align:right;">Need</td>
              <td style="padding:4px 10px;font-weight:600;text-align:right;">Available (% of need)</td>
              <td style="padding:4px 10px;font-weight:600;text-align:center;">Action</td>
            </tr>
            {IterRows}
          </table>

          <div style="background:#E8F5E9;border:2px solid {green};border-radius:6px;padding:16px 18px;margin-top:12px;">
            <p style="margin:0;font-weight:700;font-size:14px;color:{navy};">
              Final Pooled Allocation: {AllocStr}
            </p>
            <p style="margin:6px 0 0;font-size:11px;color:#666;">
              This index ({IndexName}): {Allocations.get(IndexName, '?')} lots
            </p>
          </div>
        </div>
    """

    # ── Strike Selection Candidates ──
    if AllCandidates or RejectedStrikes:
        CandidateRows = ""
        for C in AllCandidates:
            IsBest = C.get("strike") == Strike
            RowStyle = f"background:#E8F5E9;font-weight:600;" if IsBest else ""
            BsT = C.get("bs_theo")
            BsStr = f"\u20B9{_fmtEmail(BsT)}" if BsT else "N/A"
            Tag = " \u2705" if IsBest else ""
            CandidateRows += f"""
            <tr style="{RowStyle}">
              <td style="padding:6px 10px;">{C['strike']}{Tag}</td>
              <td style="padding:6px 10px;">\u20B9{_fmtEmail(C['premium'])}</td>
              <td style="padding:6px 10px;">{BsStr}</td>
              <td style="padding:6px 10px;color:{green if C['value_pct'] <= 0 else red};">{_fmtEmail(C['value_pct'], 1)}%</td>
              <td style="padding:6px 10px;">{_fmtEmail(C['spread_pct'], 1)}%</td>
            </tr>"""

        for RejStrike, RejDetails in RejectedStrikes:
            Reasons = "; ".join(RejDetails.get("checks_failed", []))
            CandidateRows += f"""
            <tr style="color:#999;text-decoration:line-through;">
              <td style="padding:6px 10px;">{RejStrike}</td>
              <td style="padding:6px 10px;" colspan="3">{Reasons}</td>
              <td style="padding:6px 10px;">\u274C</td>
            </tr>"""

        Html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Strike Selection &mdash; All Candidates
          </h2>
          <p style="margin:0 0 10px;color:#555;font-size:12px;">
            Ranked by value score (cheapest vs BS theoretical). Max spread: {MAX_SPREAD_PCT}%.
            Intrinsic band: [{INTRINSIC_UNDERPAY_MIN_PCT}%, +{INTRINSIC_OVERPAY_MAX_PCT}%]. BS deviation limit: {BS_DEVIATION_MAX_PCT}%.
          </p>
          <table style="width:100%;border-collapse:collapse;font-size:12px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;">Strike</td>
              <td style="padding:6px 10px;font-weight:600;">Premium</td>
              <td style="padding:6px 10px;font-weight:600;">BS Theo</td>
              <td style="padding:6px 10px;font-weight:600;">Value %</td>
              <td style="padding:6px 10px;font-weight:600;">Spread %</td>
            </tr>
            {CandidateRows}
          </table>
        </div>
        """

    # ── Price Validation Detail for Selected Strike ──
    BestVal = Best.get("validation") or {}
    if BestVal:
        Intrinsic = BestVal.get("intrinsic", "?")
        IVUsed = BestVal.get("iv_used", "?")
        BsTheo = BestVal.get("bs_theo")

        Html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Price Validation &mdash; Selected Strike
          </h2>

          <!-- Layer 1: Intrinsic -->
          <div style="background:{grey_bg};border:1px solid {border_col};border-radius:6px;padding:16px 18px;margin-bottom:12px;">
            <p style="margin:0 0 6px;font-weight:700;font-size:13px;color:{navy};">Layer 1: Intrinsic Value Check</p>
            <table style="border-collapse:collapse;font-size:13px;margin-top:8px;">
              <tr>
                <td style="padding:4px 0;color:#666;width:45%;">Intrinsic (Spot &minus; Strike)</td>
                <td style="padding:4px 0;font-family:monospace;">\u20B9{_fmtEmail(Intrinsic)}</td>
              </tr>
              <tr>
                <td style="padding:4px 0;color:#666;">Lower limit ({INTRINSIC_UNDERPAY_MIN_PCT}% below)</td>
                <td style="padding:4px 0;font-family:monospace;">\u20B9{_fmtEmail(float(Intrinsic) * (1 - INTRINSIC_UNDERPAY_MIN_PCT / 100) if isinstance(Intrinsic, (int, float)) else 0)}</td>
              </tr>
              <tr>
                <td style="padding:4px 0;color:#666;">Upper limit ({INTRINSIC_OVERPAY_MAX_PCT}% above)</td>
                <td style="padding:4px 0;font-family:monospace;">\u20B9{_fmtEmail(float(Intrinsic) * (1 + INTRINSIC_OVERPAY_MAX_PCT / 100) if isinstance(Intrinsic, (int, float)) else 0)}</td>
              </tr>
              <tr>
                <td style="padding:4px 0;color:#666;">Market Premium</td>
                <td style="padding:4px 0;font-family:monospace;font-weight:700;">\u20B9{_fmtEmail(Premium)}</td>
              </tr>
              <tr>
                <td style="padding:4px 0;color:#666;">Result</td>
                <td style="padding:4px 0;font-weight:700;color:{green};">\u2705 PASS</td>
              </tr>
            </table>
          </div>

          <!-- Layer 2: BS Theoretical -->
          <div style="background:{grey_bg};border:1px solid {border_col};border-radius:6px;padding:16px 18px;">
            <p style="margin:0 0 6px;font-weight:700;font-size:13px;color:{navy};">Layer 2: Black-Scholes Check</p>
            <table style="border-collapse:collapse;font-size:13px;margin-top:8px;">
              <tr>
                <td style="padding:4px 0;color:#666;width:45%;">IV Source</td>
                <td style="padding:4px 0;font-family:monospace;">{IVUsed}</td>
              </tr>
              <tr>
                <td style="padding:4px 0;color:#666;">BS Theoretical Price</td>
                <td style="padding:4px 0;font-family:monospace;">\u20B9{_fmtEmail(BsTheo)}</td>
              </tr>
              <tr>
                <td style="padding:4px 0;color:#666;">Market Premium</td>
                <td style="padding:4px 0;font-family:monospace;font-weight:700;">\u20B9{_fmtEmail(Premium)}</td>
              </tr>
              <tr>
                <td style="padding:4px 0;color:#666;">Deviation</td>
                <td style="padding:4px 0;font-family:monospace;">{_fmtEmail(abs(Premium - BsTheo) / BsTheo * 100 if isinstance(BsTheo, (int, float)) and BsTheo > 0 else 0, 1)}%  (limit: {BS_DEVIATION_MAX_PCT}%)</td>
              </tr>
              <tr>
                <td style="padding:4px 0;color:#666;">Result</td>
                <td style="padding:4px 0;font-weight:700;color:{green};">\u2705 PASS</td>
              </tr>
            </table>
          </div>
        </div>
        """

    DASH = "\u2014"  # em-dash default (extracted for Python 3.9 f-string compat)

    # ── Leg 1 (Exit) ──
    if Leg1:
        Pnl = Leg1.get("realized_pnl")
        PnlStr = f"\u20B9{Pnl:+,.0f}" if Pnl is not None else "\u2014"
        PnlColor = green if Pnl and Pnl >= 0 else red
        EntryPrice = Leg1.get("entry_price", 0)

        Html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Leg 1 &mdash; EXIT (Sell Current Month)
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">Contract</td>
              <td style="padding:8px 12px;font-family:monospace;">{Leg1.get('contract', DASH)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Quantity</td>
              <td style="padding:8px 12px;">{Leg1.get('quantity', DASH)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Fill Price</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(Leg1.get('fill_price'))}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Slippage</td>
              <td style="padding:8px 12px;">{Leg1.get('slippage', 0):+.2f} ticks</td>
            </tr>
            <tr style="background:#FFF8E1;">
              <td style="padding:8px 12px;font-weight:600;">Realized P&amp;L</td>
              <td style="padding:8px 12px;font-weight:700;font-size:15px;color:{PnlColor};">{PnlStr}</td>
            </tr>
          </table>
        </div>
        """

    # ── Leg 2 (Entry) ──
    if Leg2:
        Html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Leg 2 &mdash; ENTRY (Buy Next Month)
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">Contract</td>
              <td style="padding:8px 12px;font-family:monospace;">{Leg2.get('contract', DASH)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Quantity</td>
              <td style="padding:8px 12px;">{Leg2.get('quantity', DASH)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Lots</td>
              <td style="padding:8px 12px;font-weight:700;font-size:15px;color:{navy};">{Leg2.get('lots', DASH)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Premium (mid)</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(Leg2.get('premium'))}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Fill Price</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(Leg2.get('fill_price'))}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Slippage</td>
              <td style="padding:8px 12px;">{Leg2.get('slippage', 0):+.2f} ticks</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Expiry</td>
              <td style="padding:8px 12px;">{Leg2.get('expiry', DASH)}</td>
            </tr>
          </table>
        </div>
        """

    # ── Roll Summary ──
    RollSpread = Result.get("roll_spread")
    Pnl = Leg1.get("realized_pnl") if Leg1 else None

    Html += f"""
        <div style="padding:24px 28px;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Roll Summary
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
    """
    if RollSpread is not None:
        Html += f"""
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">Roll Spread (new &minus; old)</td>
              <td style="padding:8px 12px;font-weight:700;font-size:15px;">\u20B9{RollSpread:+,.2f}</td>
            </tr>
        """
    if Pnl is not None:
        PnlColor = green if Pnl >= 0 else red
        Html += f"""
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Realized P&amp;L (closed leg)</td>
              <td style="padding:8px 12px;font-weight:700;font-size:15px;color:{PnlColor};">\u20B9{Pnl:+,.0f}</td>
            </tr>
        """
    Html += f"""
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Order Tag</td>
              <td style="padding:8px 12px;font-family:monospace;">{ORDER_TAG}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Account</td>
              <td style="padding:8px 12px;">{USER} (Zerodha)</td>
            </tr>
          </table>
        </div>

        <!-- Footer -->
        <div style="background:{grey_bg};padding:12px 28px;border-top:1px solid {border_col};font-size:11px;color:#999;">
          ITM Call Rollover System &bull; Automated &bull; {Now.strftime('%d %b %Y %H:%M')}
        </div>
      </div>
    </body>
    </html>"""

    return Html


def BuildCombinedPortfolioEmail(Results, Allocation, FullCfg):
    """Build a portfolio-summary email sent ONCE per cycle after all indices done.

    Shows aggregated outlay, vol exposure, worst-day MTM, and capital usage
    across all indices that participated in the pooled allocation.
    """
    Now = datetime.now()
    navy = "#003366"
    accent = "#2E75B6"
    green = "#27AE60"
    red = "#E74C3C"
    grey_bg = "#F8F9FA"
    border_col = "#DEE2E6"

    Capital = FullCfg.get("account", {}).get("base_capital", 0)

    rows_html = ""
    total_outlay = 0
    total_qty = 0
    total_lots = 0
    total_worst_mtm = 0
    row_idx = 0

    for IndexName, Result in Results.items():
        if not Result.get("success"):
            continue
        Lots = Result.get("size_result", {}).get("finalLots", 0)
        Leg2 = Result.get("leg2") or {}
        Qty = Leg2.get("quantity", 0)
        if not Qty and Lots and Result.get("lot_size"):
            Qty = Lots * Result["lot_size"]
        Premium = Result.get("premium", 0)
        Outlay = Qty * Premium
        KUse = Result.get("k_value", 0)
        WorstMtm = KUse * Premium * Qty
        total_outlay += Outlay
        total_qty += Qty
        total_lots += Lots
        total_worst_mtm += WorstMtm
        bg = grey_bg if row_idx % 2 == 0 else ""
        row_idx += 1
        rows_html += f'<tr style="background:{bg};"><td style="padding:8px 12px;font-weight:600;">{IndexName}</td><td style="padding:8px 12px;text-align:right;">{Lots}</td><td style="padding:8px 12px;text-align:right;">{Qty}</td><td style="padding:8px 12px;text-align:right;font-family:monospace;">₹{_fmtEmail(Premium, 2)}</td><td style="padding:8px 12px;text-align:right;font-family:monospace;">₹{_fmtEmail(Outlay, 0)}</td><td style="padding:8px 12px;text-align:right;color:{red};font-family:monospace;">-₹{_fmtEmail(WorstMtm, 0)}</td></tr>'

    pool_util = Allocation.get("utilization_pct", 0)
    pool_used = Allocation.get("vol_used_total", 0)
    pool_total = Allocation.get("pooled_budget", 0)
    pool_left = Allocation.get("leftover_pool", 0)

    Html = f"""
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin:0;padding:0;font-family:Arial,Helvetica,sans-serif;background:#EAECEE;">
      <div style="max-width:680px;margin:20px auto;background:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1);">
        <div style="background:{navy};padding:20px 28px;">
          <h1 style="margin:0;color:#FFFFFF;font-size:20px;letter-spacing:0.5px;">
            ITM Call Combined Portfolio
          </h1>
          <p style="margin:6px 0 0;color:#AAC4E0;font-size:13px;">
            Cycle Summary &bull; {Now.strftime('%d %b %Y, %I:%M %p')}
          </p>
        </div>
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Per-Index Breakdown
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:8px 12px;font-weight:600;">Index</td>
              <td style="padding:8px 12px;text-align:right;font-weight:600;">Lots</td>
              <td style="padding:8px 12px;text-align:right;font-weight:600;">Qty</td>
              <td style="padding:8px 12px;text-align:right;font-weight:600;">Premium</td>
              <td style="padding:8px 12px;text-align:right;font-weight:600;">Outlay</td>
              <td style="padding:8px 12px;text-align:right;font-weight:600;">Worst MTM</td>
            </tr>
            {rows_html}
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:10px 12px;font-weight:700;">COMBINED</td>
              <td style="padding:10px 12px;text-align:right;font-weight:700;">{total_lots}</td>
              <td style="padding:10px 12px;text-align:right;font-weight:700;">{total_qty}</td>
              <td style="padding:10px 12px;"></td>
              <td style="padding:10px 12px;text-align:right;font-weight:700;font-family:monospace;">₹{_fmtEmail(total_outlay, 0)}</td>
              <td style="padding:10px 12px;text-align:right;font-weight:700;font-family:monospace;">-₹{_fmtEmail(total_worst_mtm, 0)}</td>
            </tr>
          </table>
        </div>
        <div style="padding:20px 28px 0;">
          <div style="background:#E8F5E9;border:2px solid {green};border-radius:6px;padding:16px 18px;">
            <table style="border-collapse:collapse;font-size:13px;width:100%;">
              <tr>
                <td style="padding:6px 0;font-weight:600;color:{navy};">Combined outlay</td>
                <td style="padding:6px 0;text-align:right;font-weight:700;font-family:monospace;">₹{_fmtEmail(total_outlay, 0)} ({_fmtEmail(total_outlay/Capital*100 if Capital else 0, 2)}% capital)</td>
              </tr>
              <tr>
                <td style="padding:6px 0;font-weight:600;color:{navy};">Vol budget utilization</td>
                <td style="padding:6px 0;text-align:right;font-weight:700;">₹{_fmtEmail(pool_used, 0)} / ₹{_fmtEmail(pool_total, 0)} ({_fmtEmail(pool_util, 1)}%)</td>
              </tr>
              <tr>
                <td style="padding:6px 0;font-weight:600;color:{navy};">Pool leftover (unused)</td>
                <td style="padding:6px 0;text-align:right;font-weight:700;font-family:monospace;">₹{_fmtEmail(pool_left, 0)}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;font-weight:600;color:{navy};">Worst-day MTM (sum of binding K)</td>
                <td style="padding:6px 0;text-align:right;font-weight:700;color:{red};font-family:monospace;">-₹{_fmtEmail(total_worst_mtm, 0)} ({_fmtEmail(-total_worst_mtm/Capital*100 if Capital else 0, 2)}%)</td>
              </tr>
              <tr>
                <td style="padding:6px 0;font-weight:600;color:{navy};">Max loss at expiry</td>
                <td style="padding:6px 0;text-align:right;font-weight:700;color:{red};font-family:monospace;">-₹{_fmtEmail(total_outlay, 0)} ({_fmtEmail(-total_outlay/Capital*100 if Capital else 0, 2)}%)</td>
              </tr>
            </table>
          </div>
        </div>
        <div style="padding:24px 28px;background:{grey_bg};border-top:1px solid {border_col};margin-top:24px;">
          <p style="margin:0;font-size:11px;color:#888;">
            Generated by ITM Call Rollover &mdash; Pooled allocation framework
          </p>
        </div>
      </div>
    </body>
    </html>
    """
    return Html


def SendEmail(Subject, HtmlBody):
    """Send email notification. Failures do NOT block trading."""
    if not EMAIL_NOTIFY_ENABLED:
        return
    try:
        Msg = MIMEMultipart("alternative")
        Msg["Subject"] = Subject
        Msg["From"] = EMAIL_FROM
        Msg["To"] = EMAIL_TO
        Msg["X-Priority"] = "1"
        Msg["Importance"] = "High"
        Msg.attach(MIMEText(HtmlBody, "html"))

        with smtplib.SMTP_SSL(EMAIL_SMTP, EMAIL_PORT) as Server:
            Server.login(EMAIL_FROM, EMAIL_FROM_PASSWORD)
            Server.send_message(Msg)
        Logger.info("Email sent: %s", Subject)
    except Exception as E:
        Logger.warning("Failed to send email: %s", E)


# ─── Execution Config ───────────────────────────────────────────────

def LoadExecConfig(ConfigKey):
    """Load SmartChase execution config for an index."""
    try:
        with open(EXEC_CONFIG_PATH) as F:
            AllConfig = json.load(F)
        Cfg = AllConfig.get(ConfigKey, {})
        return Cfg.get("execution", {})
    except FileNotFoundError:
        Logger.warning("options_execution_config.json not found, using empty config")
        return {}


# ─── Core Execution ─────────────────────────────────────────────────

def ExecuteRollover(Kite, IndexName, State, DryRun=False, FirstRun=False,
                     OverrideFinalLots=None, OverrideKMetadata=None,
                     OverrideAllocationMeta=None):
    """Execute two-leg ITM call rollover for one index.

    Returns a result dict with success status and fill details.

    Args:
        OverrideFinalLots: if provided (from pooled allocation), skip per-index
            sizing and use this lot count.
        OverrideKMetadata: dynamic K metadata (from resolveKLongSingle) for the email.
        OverrideAllocationMeta: pooled allocation metadata for the email.
    """
    IdxCfg = ITM_CONFIG[IndexName]
    IdxState = State[IndexName]
    Tag = f"[{IndexName}]"

    Result = {
        "success": False,
        "index": IndexName,
        "leg1": None,
        "leg2": None,
        "roll_spread": None,
    }

    # ── Step 1: Load vol budget ──────────────────────────────────
    Budgets, EffCapital = LoadVolBudgets()
    DailyVolBudget = Budgets.get(IndexName)
    if DailyVolBudget is None:
        Logger.error("%s No vol budget configured, aborting", Tag)
        return Result
    Result["daily_vol_budget"] = DailyVolBudget

    # ── Step 2: Fetch spot ───────────────────────────────────────
    try:
        SpotData = Kite.ltp([IdxCfg["underlying_ltp_key"]])
        Spot = float(SpotData[IdxCfg["underlying_ltp_key"]]["last_price"])
    except Exception as E:
        Logger.error("%s Failed to fetch spot: %s", Tag, E)
        return Result
    Result["spot"] = Spot
    Logger.info("%s Spot=%.1f", Tag, Spot)

    # ── Step 3: Get instruments and expiries ─────────────────────
    Instruments = GetInstrumentsCached(Kite, IdxCfg["exchange"])
    OptSegment = GetOptSegmentForExchange(IdxCfg["exchange"])
    MonthlyExpiries = GetMonthlyExpiries(Instruments, IndexName, OptSegment)

    CurrentExpiry = GetCurrentMonthExpiry(MonthlyExpiries)
    if CurrentExpiry is None:
        Logger.error("%s Cannot determine current month expiry", Tag)
        return Result

    NextExpiry = GetNextMonthExpiry(MonthlyExpiries, CurrentExpiry)
    if NextExpiry is None:
        Logger.error("%s Cannot determine next month expiry", Tag)
        return Result

    Logger.info("%s Current monthly expiry=%s, next=%s", Tag, CurrentExpiry, NextExpiry)

    # ── Step 4: Select ITM strike ────────────────────────────────
    # First run buys current month (liquid); normal rollover buys next month
    TargetExpiry = CurrentExpiry if FirstRun else NextExpiry
    Logger.info("%s Target expiry for entry: %s (%s)",
                Tag, TargetExpiry, "first-run → current month" if FirstRun else "rollover → next month")

    Candidates = ComputeITMCallCandidates(Spot, IdxCfg["strike_step"],
                                           IdxCfg["itm_pct_min"], IdxCfg["itm_pct_max"])
    Logger.info("%s ITM candidates: %s", Tag, Candidates)

    Strike, Symbol, LotSize, Premium, SelectionMeta = SelectBestITMStrike(
        Kite, Instruments, IndexName, IdxCfg["exchange"], OptSegment,
        TargetExpiry, Candidates, Spot=Spot
    )
    Result["strike"] = Strike
    Result["lot_size"] = LotSize
    Result["premium"] = Premium
    Result["symbol"] = Symbol
    Result["selection"] = SelectionMeta
    Result["effective_capital"] = EffCapital
    Result["current_expiry"] = str(CurrentExpiry)
    Result["next_expiry"] = str(NextExpiry)

    # ── Step 5: Position sizing ──────────────────────────────────
    DTE = CountTradingDaysUntilExpiry(TargetExpiry)
    Result["dte"] = DTE

    # Load full config to check useDynamicK and premium-cap settings
    with open(CONFIG_PATH) as F:
        FullCfg = json.load(F)
    AllocCfg = FullCfg.get("options_allocation", {}).get(IdxCfg["alloc_key"], {})
    UseDynamicK = AllocCfg.get("useDynamicK", False)
    MaxPremiumPct = AllocCfg.get("max_premium_pct_of_capital", None)
    MaxPremiumOutlay = (FullCfg["account"]["base_capital"] * MaxPremiumPct) if MaxPremiumPct else None
    Result["use_dynamic_k"] = UseDynamicK
    Result["max_premium_outlay"] = MaxPremiumOutlay

    # Resolve K (dynamic or static)
    KMetadata = None
    if OverrideKMetadata is not None:
        # Pooled allocator has already resolved K; use those values
        KMetadata = OverrideKMetadata
        KValue = KMetadata.get("kForSizing", lookupK(DTE, K_TABLE_SINGLE))
        Logger.info("%s Using pre-computed K from pooled pre-pass: K=%.4f (binding=%s)",
                    Tag, KValue, KMetadata.get("kBindingScenario", "?"))
    elif UseDynamicK:
        # Per-index dynamic K (no pooling)
        StaticK = lookupK(DTE, K_TABLE_SINGLE)
        RegimeRecentDays = AllocCfg.get("regimeSignal", {}).get("recent_window", 20)
        RegimeBaselineDays = AllocCfg.get("regimeSignal", {}).get("baseline_window", 100)
        KValue, KMetadata = resolveKLongSingle(
            kite=Kite, optSymbol=Symbol, exchange=IdxCfg["exchange"],
            underlying=IndexName, sizingDte=DTE,
            premium=Premium, lotSize=LotSize, expiryDate=TargetExpiry,
            optionType="CE", staticKFallback=StaticK,
            regimeRecentDays=RegimeRecentDays, regimeBaselineDays=RegimeBaselineDays,
        )
        if KValue is None:
            Logger.error("%s Dynamic K resolution failed and no fallback", Tag)
            return Result
    else:
        KValue = lookupK(DTE, K_TABLE_SINGLE)

    Result["k_value"] = KValue
    Result["k_metadata"] = KMetadata

    SizeResult = ComputePositionSizeITM(
        Premium, LotSize, KValue, DailyVolBudget,
        MaxPremiumOutlay=MaxPremiumOutlay,
    )
    Result["size_result"] = SizeResult
    if SizeResult["skipped"]:
        Logger.error("%s Position sizing skipped: %s", Tag, SizeResult["skipReason"])
        return Result

    # Use override lots if provided (pooled allocation), else per-index final lots
    if OverrideFinalLots is not None:
        FinalLots = OverrideFinalLots
        Logger.info("%s Override lots from pooled allocator: %d (per-index would have been %d)",
                    Tag, OverrideFinalLots, SizeResult["finalLots"])
    else:
        FinalLots = SizeResult["finalLots"]

    Result["allocation_meta"] = OverrideAllocationMeta  # may be None

    Quantity = FinalLots * LotSize
    Logger.info("%s Sizing: lots=%d qty=%d premium=%.2f K=%.3f dailyVol/lot=%.0f budget=%.0f%s",
                Tag, FinalLots, Quantity, Premium, KValue,
                SizeResult["dailyVolPerLot"], DailyVolBudget,
                f" cap=Rs{MaxPremiumOutlay:.0f}" if MaxPremiumOutlay else "")

    ExecConfig = LoadExecConfig(IdxCfg["exec_config_key"])

    if DryRun:
        Logger.info("%s [DRY RUN] Would exit %s qty=%s, then buy %s qty=%d",
                    Tag, IdxState.get("current_contract", "N/A"),
                    IdxState.get("quantity", 0), Symbol, Quantity)
        Result["success"] = True
        Result["leg2"] = {
            "contract": Symbol, "quantity": Quantity, "lots": FinalLots,
            "premium": Premium, "fill_price": Premium, "slippage": 0,
            "expiry": str(NextExpiry),
        }
        return Result

    # ── Step 6: Log to DB ────────────────────────────────────────
    OldContract = IdxState.get("current_contract", "")
    OldQty = IdxState.get("quantity", 0)
    RowId = db.LogITMCallRollover(
        IndexName, str(CurrentExpiry), OldContract, Symbol,
        OldQty, Quantity, DailyVolBudget, KValue,
        Broker="ZERODHA", UserAccount=USER
    )

    # ── Step 7: LEG 1 — Exit current position ────────────────────
    Leg1FillPrice = None
    if IdxState.get("status") == "HOLDING" and not FirstRun:
        Leg1Contract = IdxState["current_contract"]
        Leg1Qty = IdxState["quantity"]
        Logger.info("%s LEG 1: SELL %s qty=%d", Tag, Leg1Contract, Leg1Qty)

        Leg1Order = BuildOrderDict(IndexName, Leg1Contract, "SELL", Leg1Qty)
        Leg1Success, Leg1OrderId, Leg1FillInfo = SmartChaseExecute(
            Kite, Leg1Order, ExecConfig, IsEntry=False, Broker="ZERODHA", ATR=0
        )

        Leg1FillPrice = Leg1FillInfo.get("fill_price", 0) if Leg1FillInfo else 0
        Leg1Slippage = Leg1FillInfo.get("slippage", 0) if Leg1FillInfo else 0

        if not Leg1Success:
            Logger.error("%s LEG 1 FAILED", Tag)
            db.UpdateITMCallRolloverStatus(RowId, "LEG1_FAILED",
                                            leg1_fill_price=Leg1FillPrice,
                                            leg1_slippage=Leg1Slippage)
            Result["leg1"] = {"contract": Leg1Contract, "quantity": Leg1Qty,
                              "fill_price": Leg1FillPrice, "slippage": Leg1Slippage}
            return Result

        # Realize P&L
        EntryPrice = float(IdxState.get("entry_price", 0))
        RealizedPnl = None
        if EntryPrice > 0:
            RealizedPnl = (Leg1FillPrice - EntryPrice) * Leg1Qty * 1.0  # point_value=1 for options
            db.RealizePnl(
                f"{IndexName}_ITM_CALL", Leg1FillPrice, Leg1Qty, 1.0,
                Category="options", WasLong=True
            )
            Logger.info("%s LEG 1 P&L: entry=%.2f exit=%.2f qty=%d pnl=%.0f",
                        Tag, EntryPrice, Leg1FillPrice, Leg1Qty, RealizedPnl)

        db.UpdateITMCallRolloverStatus(RowId, "LEG1_DONE",
                                        leg1_order_id=str(Leg1OrderId),
                                        leg1_fill_price=Leg1FillPrice,
                                        leg1_slippage=Leg1Slippage,
                                        realized_pnl=RealizedPnl)

        # Log to options order log
        db.LogOptionsSmartChaseOrder(
            IndexName, "ITM_CALL_ROLLOVER", "EXIT", Leg1Contract, "SELL",
            Leg1Qty, BrokerOrderId=str(Leg1OrderId), FillInfo=Leg1FillInfo
        )

        Result["leg1"] = {
            "contract": Leg1Contract, "quantity": Leg1Qty,
            "fill_price": Leg1FillPrice, "slippage": Leg1Slippage,
            "realized_pnl": RealizedPnl,
        }
    else:
        Logger.info("%s Skipping LEG 1 (first run or no existing position)", Tag)

    # ── Step 8: LEG 2 — Buy next month ITM call ─────────────────
    Logger.info("%s LEG 2: BUY %s qty=%d", Tag, Symbol, Quantity)

    Leg2Order = BuildOrderDict(IndexName, Symbol, "BUY", Quantity)
    Leg2Success, Leg2OrderId, Leg2FillInfo = SmartChaseExecute(
        Kite, Leg2Order, ExecConfig, IsEntry=True, Broker="ZERODHA", ATR=0
    )

    Leg2FillPrice = Leg2FillInfo.get("fill_price", 0) if Leg2FillInfo else 0
    Leg2Slippage = Leg2FillInfo.get("slippage", 0) if Leg2FillInfo else 0

    if not Leg2Success:
        Logger.error("%s LEG 2 FAILED — position is FLAT", Tag)
        db.UpdateITMCallRolloverStatus(RowId, "LEG2_FAILED",
                                        leg2_fill_price=Leg2FillPrice,
                                        leg2_slippage=Leg2Slippage)
        Result["leg2"] = {"contract": Symbol, "quantity": Quantity,
                          "fill_price": Leg2FillPrice, "slippage": Leg2Slippage}
        SendEmail(f"CRITICAL: {IndexName} ITM Call LEG 2 FAILED — FLAT",
                  BuildRolloverEmailHtml(IndexName, Result))
        return Result

    # Update cost basis for new position
    if Leg2FillPrice > 0:
        db.UpdateCostBasis(f"{IndexName}_ITM_CALL", Leg2FillPrice, Quantity, 1.0)

    # Compute roll spread
    RollSpread = None
    if Leg1FillPrice and Leg1FillPrice > 0:
        RollSpread = Leg2FillPrice - Leg1FillPrice

    db.UpdateITMCallRolloverStatus(RowId, "COMPLETE",
                                    new_contract=Symbol,
                                    leg2_order_id=str(Leg2OrderId),
                                    leg2_fill_price=Leg2FillPrice,
                                    leg2_slippage=Leg2Slippage,
                                    roll_spread=RollSpread,
                                    executed_at=datetime.now().isoformat())

    # Log to options order log
    db.LogOptionsSmartChaseOrder(
        IndexName, "ITM_CALL_ROLLOVER", "ENTRY", Symbol, "BUY",
        Quantity, BrokerOrderId=str(Leg2OrderId), FillInfo=Leg2FillInfo
    )

    Result["leg2"] = {
        "contract": Symbol, "quantity": Quantity, "lots": FinalLots,
        "premium": Premium, "fill_price": Leg2FillPrice, "slippage": Leg2Slippage,
        "expiry": str(TargetExpiry),
    }
    Result["roll_spread"] = RollSpread
    Result["success"] = True

    # ── Step 9: Update state ─────────────────────────────────────
    State[IndexName] = {
        "status": "HOLDING",
        "current_contract": Symbol,
        "current_expiry": str(TargetExpiry),
        "lots": FinalLots,
        "quantity": Quantity,
        "entry_price": Leg2FillPrice,
        "entry_date": str(date.today()),
        "order_tag": ORDER_TAG,
    }
    SaveState(State)

    Logger.info("%s ROLLOVER COMPLETE: %s → %s", Tag,
                IdxState.get("current_contract", "N/A"), Symbol)
    return Result


# ─── Status Display ─────────────────────────────────────────────────

def PrintStatus():
    """Print current state and recent rollovers."""
    State = LoadState()
    print("\n=== ITM Call Rollover Status ===\n")

    for Idx in ["NIFTY", "BANKNIFTY"]:
        S = State[Idx]
        print(f"  {Idx}:")
        print(f"    Status:   {S['status']}")
        print(f"    Contract: {S.get('current_contract', '—')}")
        print(f"    Expiry:   {S.get('current_expiry', '—')}")
        print(f"    Lots:     {S.get('lots', 0)}")
        print(f"    Quantity: {S.get('quantity', 0)}")
        print(f"    Entry:    ₹{S.get('entry_price', 0):,.2f}")
        print(f"    Date:     {S.get('entry_date', '—')}")
        print()

    # Vol budgets
    try:
        Budgets, EffCap = LoadVolBudgets()
        print(f"  Effective Capital: ₹{EffCap:,.0f}")
        for Idx, Budget in Budgets.items():
            print(f"  {Idx} Daily Vol Budget: ₹{Budget:,.0f}")
        print()
    except Exception as E:
        print(f"  (Could not load vol budgets: {E})\n")

    # Recent rollovers
    try:
        db.InitDB()
        Recent = db.GetRecentITMCallRollovers(limit=10)
        if Recent:
            print("  Recent Rollovers:")
            for R in Recent:
                print(f"    {R['created_at']} | {R['instrument']} | {R['status']} | "
                      f"{R.get('old_contract', '—')} → {R.get('new_contract', '—')}")
        print()
    except Exception:
        pass


# ─── Coordinated (Pooled) Entry Orchestration ──────────────────────

def PrepareSizingForIndex(Kite, IndexName, FirstRun=False):
    """Phase-1 of pooled entry: gather sizing inputs without executing orders.

    Returns dict with: spot, target_expiry, dte, strike, symbol, lot_size, premium,
                       k_value, k_metadata, dvpl, cost_per_lot, max_outlay,
                       daily_budget, floor_lots, size_result, eff_capital, selection
    Or returns None on failure (logs error).
    """
    IdxCfg = ITM_CONFIG[IndexName]
    Tag = f"[{IndexName}]"

    # Load vol budget
    Budgets, EffCapital = LoadVolBudgets()
    DailyVolBudget = Budgets.get(IndexName)
    if DailyVolBudget is None:
        Logger.error("%s [PREPARE] No vol budget", Tag)
        return None

    # Fetch spot
    try:
        SpotData = Kite.ltp([IdxCfg["underlying_ltp_key"]])
        Spot = float(SpotData[IdxCfg["underlying_ltp_key"]]["last_price"])
    except Exception as E:
        Logger.error("%s [PREPARE] Spot fetch failed: %s", Tag, E)
        return None

    # Get instruments and expiries
    Instruments = GetInstrumentsCached(Kite, IdxCfg["exchange"])
    OptSegment = GetOptSegmentForExchange(IdxCfg["exchange"])
    MonthlyExpiries = GetMonthlyExpiries(Instruments, IndexName, OptSegment)
    CurrentExpiry = GetCurrentMonthExpiry(MonthlyExpiries)
    if CurrentExpiry is None:
        Logger.error("%s [PREPARE] Cannot determine current month expiry", Tag)
        return None
    NextExpiry = GetNextMonthExpiry(MonthlyExpiries, CurrentExpiry)
    if NextExpiry is None:
        Logger.error("%s [PREPARE] Cannot determine next month expiry", Tag)
        return None
    TargetExpiry = CurrentExpiry if FirstRun else NextExpiry

    # Select strike
    Candidates = ComputeITMCallCandidates(Spot, IdxCfg["strike_step"],
                                           IdxCfg["itm_pct_min"], IdxCfg["itm_pct_max"])
    try:
        Strike, Symbol, LotSize, Premium, SelectionMeta = SelectBestITMStrike(
            Kite, Instruments, IndexName, IdxCfg["exchange"], OptSegment,
            TargetExpiry, Candidates, Spot=Spot
        )
    except Exception as E:
        Logger.error("%s [PREPARE] Strike selection failed: %s", Tag, E)
        return None

    # Resolve K — dynamic if configured
    DTE = CountTradingDaysUntilExpiry(TargetExpiry)
    with open(CONFIG_PATH) as F:
        FullCfg = json.load(F)
    AllocCfg = FullCfg.get("options_allocation", {}).get(IdxCfg["alloc_key"], {})
    UseDynamicK = AllocCfg.get("useDynamicK", False)
    MaxPremiumPct = AllocCfg.get("max_premium_pct_of_capital", None)
    MaxPremiumOutlay = (FullCfg["account"]["base_capital"] * MaxPremiumPct) if MaxPremiumPct else None

    KMetadata = None
    if UseDynamicK:
        StaticK = lookupK(DTE, K_TABLE_SINGLE)
        RegimeRecentDays = AllocCfg.get("regimeSignal", {}).get("recent_window", 20)
        RegimeBaselineDays = AllocCfg.get("regimeSignal", {}).get("baseline_window", 100)
        KValue, KMetadata = resolveKLongSingle(
            kite=Kite, optSymbol=Symbol, exchange=IdxCfg["exchange"],
            underlying=IndexName, sizingDte=DTE,
            premium=Premium, lotSize=LotSize, expiryDate=TargetExpiry,
            optionType="CE", staticKFallback=StaticK,
            regimeRecentDays=RegimeRecentDays, regimeBaselineDays=RegimeBaselineDays,
        )
        if KValue is None:
            Logger.error("%s [PREPARE] K resolution failed", Tag)
            return None
    else:
        KValue = lookupK(DTE, K_TABLE_SINGLE)

    # Compute initial sizing (per-index floor)
    SizeResult = ComputePositionSizeITM(
        Premium, LotSize, KValue, DailyVolBudget,
        MaxPremiumOutlay=MaxPremiumOutlay,
    )
    if SizeResult["skipped"]:
        Logger.error("%s [PREPARE] Sizing skipped: %s", Tag, SizeResult["skipReason"])
        return None

    return {
        "index": IndexName,
        "spot": Spot,
        "current_expiry": CurrentExpiry,
        "next_expiry": NextExpiry,
        "target_expiry": TargetExpiry,
        "dte": DTE,
        "strike": Strike,
        "symbol": Symbol,
        "lot_size": LotSize,
        "premium": Premium,
        "selection": SelectionMeta,
        "k_value": KValue,
        "k_metadata": KMetadata,
        "use_dynamic_k": UseDynamicK,
        "dvpl": SizeResult["dailyVolPerLot"],
        "cost_per_lot": SizeResult["costPerLot"],
        "max_outlay": MaxPremiumOutlay,
        "daily_budget_per_idx": DailyVolBudget,
        "floor_lots": SizeResult["finalLots"],
        "size_result": SizeResult,
        "eff_capital": EffCapital,
    }


def RunCoordinatedRollover(Kite, State, Indices, DryRun=False, FirstRun=False):
    """Pooled-allocation entry orchestration:
      1. Phase 1: Prepare sizing for each index (computes K, dvpl, floor lots).
      2. Phase 2: Run AllocateLotsBalanced across all indices.
      3. Phase 3: Execute trades using the joint allocation.

    Returns (results_dict, allocation_dict).
    """
    Logger.info("=" * 60)
    Logger.info("RUNNING POOLED-ALLOCATION ENTRY (useDynamicK=true)")

    # ── Phase 1: Prepare ──
    Prepared = {}
    for IndexName in Indices:
        Logger.info("[%s] Preparing sizing inputs...", IndexName)
        prep = PrepareSizingForIndex(Kite, IndexName, FirstRun=FirstRun)
        if prep is None:
            Logger.error("[%s] Preparation failed, will skip", IndexName)
            continue
        Prepared[IndexName] = prep

    if not Prepared:
        Logger.error("All indices failed preparation; aborting coordinated rollover")
        return {}, {}

    # Build SizingInputs for AllocateLotsBalanced
    SizingInputs = {
        name: {
            "dvpl": p["dvpl"],
            "cost_per_lot": p["cost_per_lot"],
            "max_outlay": p["max_outlay"],
            "daily_budget_per_idx": p["daily_budget_per_idx"],
            "floor_lots": p["floor_lots"],
        }
        for name, p in Prepared.items()
    }

    # ── Phase 2: Allocate ──
    Allocation = AllocateLotsBalanced(SizingInputs)
    Logger.info("[POOL] Pooled allocation result: %s", Allocation["allocations"])
    Logger.info("[POOL] Vol used: Rs %.0f / %.0f (%.1f%%), leftover Rs %.0f",
                Allocation["vol_used_total"], Allocation["pooled_budget"],
                Allocation["utilization_pct"], Allocation["leftover_pool"])

    # ── Phase 3: Execute ──
    Results = {}
    for IndexName, FinalLots in Allocation["allocations"].items():
        prep = Prepared[IndexName]
        Result = ExecuteRollover(
            Kite, IndexName, State, DryRun=DryRun, FirstRun=FirstRun,
            OverrideFinalLots=FinalLots,
            OverrideKMetadata=prep["k_metadata"],
            OverrideAllocationMeta={
                "pool": Allocation,
                "this_index_inputs": SizingInputs[IndexName],
            },
        )
        Results[IndexName] = Result

    return Results, Allocation


# ─── Main ───────────────────────────────────────────────────────────

def main():
    Parser = argparse.ArgumentParser(description="ITM Monthly Call Rollover")
    Parser.add_argument("--dry-run", action="store_true", help="Log decisions, no orders")
    Parser.add_argument("--force", action="store_true", help="Force rollover regardless of date")
    Parser.add_argument("--first-run", action="store_true", help="Cold start: buy only, no exit")
    Parser.add_argument("--index", type=str, default=None,
                        help="Run for one index only (NIFTY or BANKNIFTY)")
    Parser.add_argument("--status", action="store_true", help="Print current state")
    Args = Parser.parse_args()

    # Logging setup
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Path(WorkDirectory) / "itm_call_rollover.log"),
        ]
    )

    if Args.status:
        PrintStatus()
        return

    Logger.info("=" * 60)
    Logger.info("ITM Call Rollover started | dry_run=%s force=%s first_run=%s index=%s",
                Args.dry_run, Args.force, Args.first_run, Args.index)

    # Initialize DB
    db.InitDB()

    # Establish session
    try:
        Kite = EstablishKiteSession()
    except Exception as E:
        Logger.error("Failed to establish Kite session: %s", E)
        SendEmail("ITM Call Rollover: SESSION FAILED", f"<p>Failed to connect: {E}</p>")
        sys.exit(1)

    # Determine which indices to process
    Indices = ["NIFTY", "BANKNIFTY"]
    if Args.index:
        if Args.index.upper() not in Indices:
            Logger.error("Invalid index: %s (must be NIFTY or BANKNIFTY)", Args.index)
            sys.exit(1)
        Indices = [Args.index.upper()]

    # Load state
    State = LoadState()

    # Get instruments
    Instruments = GetInstrumentsCached(Kite, "NFO")
    OptSegment = GetOptSegmentForExchange("NFO")

    AllResults = {}

    # Pre-filter indices: skip those that aren't on monthly expiry day (unless --force)
    EligibleIndices = []
    for IndexName in Indices:
        try:
            IsExpiry, ExpiryDate = IsMonthlyExpiryDay(Instruments, IndexName, OptSegment)
            if not IsExpiry and not Args.force:
                Logger.info("[%s] Not monthly expiry day, skipping", IndexName)
                continue
            if Args.force and not IsExpiry:
                Logger.info("[%s] --force flag: proceeding despite not expiry day", IndexName)

            # Crash recovery check
            IncompleteRollovers = db.GetIncompleteITMCallRollovers(IndexName)
            if IncompleteRollovers:
                Logger.warning("[%s] %d incomplete rollovers found, treating as first-run",
                               IndexName, len(IncompleteRollovers))
                Args.first_run = True

            # State recovery if needed
            if State[IndexName]["status"] == "NONE" and not Args.first_run:
                MonthlyExpiries = GetMonthlyExpiries(Instruments, IndexName, OptSegment)
                CurrentExpiry = GetCurrentMonthExpiry(MonthlyExpiries)
                if CurrentExpiry:
                    Recovered = RecoverStateFromPositions(Kite, IndexName, CurrentExpiry)
                    if Recovered:
                        State[IndexName] = Recovered
                        SaveState(State)
                        Logger.info("[%s] State recovered from positions", IndexName)

            EligibleIndices.append(IndexName)
        except Exception as E:
            Logger.exception("[%s] Eligibility check failed: %s", IndexName, E)
            AllResults[IndexName] = {"success": False, "error": str(E)}

    # Decide flow: pooled-allocation if ANY eligible index has useDynamicK enabled
    UseCoordinated = False
    if EligibleIndices:
        try:
            with open(CONFIG_PATH) as F:
                FullCfg = json.load(F)
            for IndexName in EligibleIndices:
                AllocKey = ITM_CONFIG[IndexName]["alloc_key"]
                if FullCfg.get("options_allocation", {}).get(AllocKey, {}).get("useDynamicK"):
                    UseCoordinated = True
                    break
        except Exception:
            UseCoordinated = False

    if UseCoordinated and len(EligibleIndices) >= 1:
        Logger.info("=" * 60)
        Logger.info("Using POOLED ALLOCATION flow (useDynamicK enabled)")
        Results, Allocation = RunCoordinatedRollover(
            Kite, State, EligibleIndices, DryRun=Args.dry_run, FirstRun=Args.first_run
        )
        for IndexName, Result in Results.items():
            AllResults[IndexName] = Result
            StatusStr = "SUCCESS" if Result.get("success") else "FAILED"
            SendEmail(
                f"ITM Call {IndexName}: {StatusStr}",
                BuildRolloverEmailHtml(IndexName, Result)
            )

        # Send combined portfolio email
        try:
            CombinedHtml = BuildCombinedPortfolioEmail(Results, Allocation, FullCfg)
            SendEmail("ITM Call Combined Portfolio Summary", CombinedHtml)
        except Exception as E:
            Logger.warning("Failed to send combined portfolio email: %s", E)
    else:
        # Static-K per-index path (unchanged)
        Logger.info("Using STATIC K per-index flow (useDynamicK not enabled)")
        for IndexName in EligibleIndices:
            try:
                Result = ExecuteRollover(Kite, IndexName, State, DryRun=Args.dry_run,
                                         FirstRun=Args.first_run)
                AllResults[IndexName] = Result
                StatusStr = "SUCCESS" if Result["success"] else "FAILED"
                SendEmail(
                    f"ITM Call {IndexName}: {StatusStr}",
                    BuildRolloverEmailHtml(IndexName, Result)
                )
            except Exception as E:
                Logger.exception("[%s] Unhandled error: %s", IndexName, E)
                AllResults[IndexName] = {"success": False, "error": str(E)}
                SendEmail(
                    f"ITM Call {IndexName}: ERROR",
                    f"<p style='color:red;font-weight:bold'>Unhandled error: {E}</p>"
                )

    # Summary
    Logger.info("=" * 60)
    for Idx, Res in AllResults.items():
        Logger.info("  %s: %s", Idx, "SUCCESS" if Res.get("success") else "FAILED")
    Logger.info("ITM Call Rollover finished")


if __name__ == "__main__":
    main()
