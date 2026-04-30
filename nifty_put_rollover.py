"""
nifty_put_rollover.py — Automated Monthly NIFTY Put Buying System.

Standalone process that manages a monthly long 1% ITM NIFTY put position as
portfolio downside protection. On monthly expiry day, exits the current
month's put and rolls into the next month's put.

Strategy:
  - Always long a 1% ITM monthly NIFTY put (strike ~1% above spot)
  - Round 100-step strikes only (liquidity gate)
  - Premium-at-risk sizing: lots = floor(monthly_budget / (premium × lotSize))
  - Skip rule: abort cleanly when 1 lot exceeds monthly budget
  - No K-table, no Greeks-based sizing — premium is the cost and the cap
  - Executes via SmartChaseExecute for optimal fills

Usage:
    python nifty_put_rollover.py                  # Normal run (3:00 PM expiry day)
    python nifty_put_rollover.py --dry-run        # Log decisions, no orders
    python nifty_put_rollover.py --force          # Force rollover regardless of date
    python nifty_put_rollover.py --first-run      # Cold start: buy only, no exit
    python nifty_put_rollover.py --status         # Print current state
"""

import argparse
import json
import logging
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
from PlaceOptionsSystemsV2 import bsPrice, bsImpliedVol
import forecast_db as db

Logger = logging.getLogger("nifty_put_rollover")

# ─── Configuration ───────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent / "instrument_config.json"
EXEC_CONFIG_PATH = Path(__file__).parent / "options_execution_config.json"
STATE_FILE_PATH = Path(WorkDirectory) / "nifty_put_state.json"

ORDER_TAG = "PUT_ROLL"
USER = "OFS653"
INDEX_NAME = "NIFTY"

PUT_CONFIG = {
    "NIFTY": {
        "underlying_ltp_key": "NSE:NIFTY 50",
        "exchange": "NFO",
        "strike_step": 100,        # round 100-step strikes only (liquidity gate)
        "itm_pct_min": 0.5,         # min 0.5% ITM (strike above spot)
        "itm_pct_max": 1.5,         # max 1.5% ITM
        "exec_config_key": "NIFTY_OPT",
        "alloc_key": "NIFTY_PUT_BUY",
    },
}

VIX_LTP_KEY = "NSE:INDIA VIX"

# Email (same SMTP config as ITM call rollover and straddle V2)
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
    ApiKey = Lines[2].strip()
    with open(KiteEshitaLoginAccessToken, "r") as f:
        AccessToken = f.read().strip()
    Kite = KiteConnect(api_key=ApiKey)
    Kite.set_access_token(AccessToken)
    return Kite


# ─── Trading Day Utilities ──────────────────────────────────────────

def IsTradingDay(D):
    """True if D is a weekday and not a holiday."""
    if D.weekday() >= 5:
        return False
    return not CheckForDateHoliday(D)


# ─── Monthly Expiry Detection ───────────────────────────────────────

def GetMonthlyExpiries(Instruments, IndexName, OptSegment):
    """Find monthly expiries (last expiry of each month) for an index.

    Returns a sorted list of date objects.
    """
    Today = date.today()
    AllExpiries = set()
    for Ins in Instruments:
        if Ins.get("segment") != OptSegment:
            continue
        if Ins.get("name") != IndexName:
            continue
        Exp = Ins.get("expiry")
        if isinstance(Exp, datetime):
            Exp = Exp.date()
        if isinstance(Exp, date) and Exp >= Today:
            AllExpiries.add(Exp)

    # Group by (year, month) and pick the LAST expiry of each month
    MonthGroups = {}
    for Exp in sorted(AllExpiries):
        Key = (Exp.year, Exp.month)
        MonthGroups[Key] = Exp  # later expiries overwrite (sorted ascending)

    return sorted(MonthGroups.values())


def IsMonthlyExpiryDay(Instruments, IndexName, OptSegment):
    """Check if today is a monthly expiry day for the given index.

    Returns (bool, expiry_date_or_None).
    """
    Today = date.today()
    MonthlyExpiries = GetMonthlyExpiries(Instruments, IndexName, OptSegment)
    for Exp in MonthlyExpiries:
        if Exp == Today:
            return True, Exp
    return False, None


def GetCurrentMonthExpiry(MonthlyExpiries):
    """Return the monthly expiry in the current calendar month, or None."""
    Today = date.today()
    for Exp in MonthlyExpiries:
        if Exp.year == Today.year and Exp.month == Today.month:
            return Exp
    return None


def GetNextMonthExpiry(MonthlyExpiries, CurrentExpiry):
    """Return the first monthly expiry strictly after CurrentExpiry."""
    for Exp in MonthlyExpiries:
        if Exp > CurrentExpiry:
            return Exp
    return None


# ─── Strike Selection ───────────────────────────────────────────────

def ComputePutCandidates(Spot, StrikeStep=100, ITMPctMin=0.5, ITMPctMax=1.5):
    """Generate 1% ITM put strike candidates at round 100-step strikes only.

    ITM put = strike ABOVE spot. 1% ITM = strike at 101% of spot.
    Returns list of strike values (ints), sorted ascending.

    Round 100-step only because non-round NIFTY put strikes (50-step boundaries
    like 24050, 24150) have wide spreads and minimal OI — practically untradeable.
    """
    import math
    LowStrike = Spot * (100 + ITMPctMin) / 100   # 0.5% ITM = lower bound
    HighStrike = Spot * (100 + ITMPctMax) / 100   # 1.5% ITM = upper bound

    MinStrike = math.ceil(LowStrike / StrikeStep) * StrikeStep
    MaxStrike = math.floor(HighStrike / StrikeStep) * StrikeStep

    Candidates = list(range(MinStrike, MaxStrike + StrikeStep, StrikeStep))

    if not Candidates:
        # Widen to 0.25-2% ITM if initial range produces nothing
        Logger.warning("No candidates in %.1f-%.1f%% ITM range (spot=%.0f), widening to 0.25-2%%",
                       ITMPctMin, ITMPctMax, Spot)
        LowStrike = Spot * 1.0025
        HighStrike = Spot * 1.02
        MinStrike = math.ceil(LowStrike / StrikeStep) * StrikeStep
        MaxStrike = math.floor(HighStrike / StrikeStep) * StrikeStep
        Candidates = list(range(MinStrike, MaxStrike + StrikeStep, StrikeStep))

    return Candidates


def SelectBestPutStrike(Kite, Instruments, IndexName, Exchange, OptSegment,
                        ExpiryDate, Candidates, Spot=None):
    """From candidate strikes, pick the one with best validation pass + tightest spread.

    Validates each candidate against intrinsic value and Black-Scholes
    theoretical price. Filters by spread < 3% and min OI threshold.

    Returns (strike, tradingsymbol, lotSize, premium, SelectionMeta) or raises.
    """
    # Filter instruments to matching PE contracts at the target expiry
    MatchingInstruments = {}
    for Ins in Instruments:
        if Ins.get("segment") != OptSegment:
            continue
        if Ins.get("name") != IndexName:
            continue
        if Ins.get("expiry") != ExpiryDate:
            continue
        if Ins.get("instrument_type") != "PE":
            continue
        Strike = int(float(Ins.get("strike", 0)))
        if Strike in Candidates:
            MatchingInstruments[Strike] = Ins

    if not MatchingInstruments:
        raise Exception(f"No PE instruments found for {IndexName} expiry={ExpiryDate} "
                        f"strikes={Candidates}")

    # Fetch quotes for all candidates
    QuoteKeys = []
    StrikeToKey = {}
    for Strike, Ins in MatchingInstruments.items():
        Key = f"{Exchange}:{Ins['tradingsymbol']}"
        QuoteKeys.append(Key)
        StrikeToKey[Key] = Strike

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

            # Get bid-ask for BUY side (we are buying puts)
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
                Ltp = float(Q.get("last_price", 0))
                if Ltp > 0:
                    Mid = Ltp
                    SpreadPct = -1   # signal: no live depth, using LTP
                    Premium = Ltp
                    Logger.warning("[%s] PE Strike %d: no order book depth, using LTP %.2f",
                                   IndexName, Strike, Ltp)
                else:
                    SpreadPct = 999

            # Filter: skip if spread is too wide
            if SpreadPct > MAX_SPREAD_PCT:
                Logger.warning("[%s] PE Strike %d skipped: spread %.1f%% > %.1f%% limit",
                               IndexName, Strike, SpreadPct, MAX_SPREAD_PCT)
                RejectedStrikes.append((Strike, {"checks_failed": [
                    f"SPREAD_TOO_WIDE: {SpreadPct:.1f}% > {MAX_SPREAD_PCT}%"]}))
                continue

            # Filter: skip if OI is too low
            Oi = int(Q.get("oi", 0) or 0)
            if Oi < MIN_OI_THRESHOLD:
                Logger.warning("[%s] PE Strike %d skipped: OI %d < %d threshold",
                               IndexName, Strike, Oi, MIN_OI_THRESHOLD)
                RejectedStrikes.append((Strike, {"checks_failed": [
                    f"LOW_OI: {Oi} < {MIN_OI_THRESHOLD}"]}))
                continue

            # Validate contract price against intrinsic + BS theoretical
            if Spot is not None and Spot > 0:
                ValPassed, ValDetails = ValidatePutContractPrice(
                    Spot, Strike, Premium, ExpiryDate, Kite)
                if not ValPassed:
                    Logger.warning("[%s] PE Strike %d REJECTED: %s",
                                   IndexName, Strike,
                                   "; ".join(ValDetails["checks_failed"]))
                    RejectedStrikes.append((Strike, ValDetails))
                    continue
            else:
                ValDetails = {"bs_theo": None}

            # Value score: how cheap is market premium vs BS theoretical?
            BsTheo = ValDetails.get("bs_theo") if ValDetails else None
            if BsTheo and BsTheo > 0:
                ValuePct = (Premium - BsTheo) / BsTheo * 100  # negative = cheap
            else:
                ValuePct = 0

            ValidCandidates.append({
                "strike": Strike,
                "symbol": Ins["tradingsymbol"],
                "lot_size": int(Ins.get("lot_size", 1)),
                "premium": Premium,
                "spread_pct": SpreadPct,
                "value_pct": ValuePct,
                "bs_theo": BsTheo,
                "oi": Oi,
                "validation": ValDetails,
            })

    if RejectedStrikes:
        Logger.warning("[%s] %d candidate(s) rejected by validation",
                       IndexName, len(RejectedStrikes))

    if not ValidCandidates:
        RejectSummary = "; ".join(
            f"{s}: {'; '.join(d['checks_failed'])}" for s, d in RejectedStrikes
        )
        raise Exception(f"No valid PE contracts for {IndexName} expiry={ExpiryDate}. "
                        f"Rejected: {RejectSummary}")

    # Pick the candidate closest to 1% ITM moneyness (target), breaking ties by best value
    TargetMoneyness = 1.0  # 1% ITM
    def Distance(c):
        Mn = (c["strike"] - Spot) / Spot * 100 if Spot else 0
        return abs(Mn - TargetMoneyness)
    ValidCandidates.sort(key=lambda c: (Distance(c), c["value_pct"]))
    Best = ValidCandidates[0]

    for C in ValidCandidates:
        Marker = " <<<" if C is Best else ""
        Logger.info("[%s]   PE strike=%d premium=%.2f bs_theo=%s value=%.1f%% spread=%.1f%% OI=%d%s",
                    IndexName, C["strike"], C["premium"],
                    f"{C['bs_theo']:.2f}" if C["bs_theo"] else "N/A",
                    C["value_pct"], C["spread_pct"], C["oi"], Marker)

    BsTheoVal = Best.get("bs_theo")
    BsTheoStr = f"{BsTheoVal:.2f}" if isinstance(BsTheoVal, (int, float)) else "N/A"
    Logger.info("[%s] Selected PE strike=%d symbol=%s premium=%.2f value=%.1f%% "
                "spread=%.1f%% OI=%d lotSize=%d bs_theo=%s",
                IndexName, Best["strike"], Best["symbol"], Best["premium"],
                Best["value_pct"], Best["spread_pct"], Best["oi"],
                Best["lot_size"], BsTheoStr)

    SelectionMeta = {
        "best": Best,
        "all_candidates": ValidCandidates,
        "rejected": RejectedStrikes,
    }
    return Best["strike"], Best["symbol"], Best["lot_size"], Best["premium"], SelectionMeta


# ─── Contract Price Validation ──────────────────────────────────────

# Thresholds for price validation
# Note: For 1% ITM puts at ~30 DTE, time value dominates (premium can be 3-7x
# intrinsic). The intrinsic-overpay check is therefore set very loose — its
# purpose is only to catch obvious data corruption (e.g. premium 50x intrinsic).
# The BS-deviation check is the meaningful price-sanity gate.
INTRINSIC_OVERPAY_MAX_PCT = 1500.0  # only catches data corruption
INTRINSIC_UNDERPAY_MIN_PCT = 5.0    # reject if market premium < intrinsic * (1 - this/100)
BS_DEVIATION_MAX_PCT = 15.0         # reject if |market - BS_theo| > this % of BS_theo
BS_FALLBACK_IV = 0.15               # 15% annualised — fallback if VIX unavailable
MAX_SPREAD_PCT = 3.0                # skip candidates with spread wider than this %
MIN_OI_THRESHOLD = 5000             # skip illiquid strikes


def ValidatePutContractPrice(Spot, Strike, Premium, ExpiryDate, Kite=None):
    """Validate that a quoted put premium is reasonable.

    Two checks:
      1. Intrinsic value: premium must be at least 95% of intrinsic and not more
         than 160% (puts at 1% ITM have substantial time value)
      2. Black-Scholes: premium must be within 15% of BS theoretical price
         (uses India VIX for IV if available, else implied IV, else 15% fallback)

    Returns:
        (passed: bool, details: dict)
    """
    Today = date.today()
    DaysToExpiry = (ExpiryDate - Today).days
    T = max(DaysToExpiry / 365.0, 1e-6)

    Intrinsic = max(Strike - Spot, 0.0)   # PUT intrinsic = strike - spot
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
        # Strike <= Spot — not really ITM, flag it
        Details["checks_failed"].append(
            f"NOT_ITM: strike {Strike} <= spot {Spot:.2f}, intrinsic=0")

    # ── Check 2: Black-Scholes theoretical price ────────────────
    IV = None

    # Try India VIX as proxy for IV
    if Kite is not None:
        try:
            VixData = Kite.ltp([VIX_LTP_KEY])
            VixLevel = float(VixData[VIX_LTP_KEY]["last_price"])
            IV = VixLevel / 100.0
            Details["iv_used"] = f"VIX={VixLevel:.1f}"
        except Exception:
            pass

    # Fall back to imply IV from market premium
    if IV is None:
        ImpliedIV = bsImpliedVol(Premium, Spot, Strike, T, "PE")
        if ImpliedIV is not None:
            IV = ImpliedIV
            Details["implied_iv"] = ImpliedIV
            Details["iv_used"] = f"implied={ImpliedIV:.3f}"
        else:
            IV = BS_FALLBACK_IV
            Details["iv_used"] = f"fallback={BS_FALLBACK_IV}"

    # Compute BS theoretical price
    try:
        BsTheo = bsPrice(Spot, Strike, T, IV, "PE")
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


# ─── Position Sizing — Premium-at-Risk ──────────────────────────────

def LoadMonthlyBudget():
    """Compute the monthly premium budget for NIFTY puts.

    Capital formula (shared with PlaceOptionsSystemsV2 and itm_call_rollover):
        effective = base_capital + cumulative_realized + eod_unrealized

    Then:
        annual_vol = effective × annual_vol_pct × Π(weights)
        monthly_premium_budget = annual_vol / 12

    Note: We bypass the daily-vol-target framework entirely for puts because
    long options aren't meaningful daily-vol contributors (theta drift +
    bounded payoff). The annual allocation IS the worst-case yearly cost cap.
    """
    with open(CONFIG_PATH) as F:
        Cfg = json.load(F)
    Acct = Cfg["account"]
    BaseCapital = Acct["base_capital"]

    # Read realized + unrealized from EOD JSON accumulator, fall back to DB
    CumulativeRealized = 0.0
    EodUnrealized = 0.0
    PnlPath = Path(__file__).parent / "realized_pnl_accumulator.json"
    try:
        from Directories import workInputRoot
        PnlPath = Path(workInputRoot) / "realized_pnl_accumulator.json"
    except Exception:
        pass
    try:
        with open(PnlPath, "r") as F2:
            PnlData = json.load(F2)
        CumulativeRealized = float(PnlData.get("cumulative_realized_pnl") or 0.0)
        EodUnrealized = float(PnlData.get("eod_unrealized") or 0.0)
    except (FileNotFoundError, json.JSONDecodeError):
        try:
            CumulativeRealized = db.GetCumulativeRealizedPnl()
        except Exception:
            CumulativeRealized = 0.0

    EffectiveCapital = BaseCapital + CumulativeRealized + EodUnrealized
    Logger.info("Put effective capital: base=%d + realized=%.0f + unrealized=%.0f = %.0f",
                BaseCapital, CumulativeRealized, EodUnrealized, EffectiveCapital)

    AllocKey = PUT_CONFIG[INDEX_NAME]["alloc_key"]
    OptAlloc = Cfg.get("options_allocation", {}).get(AllocKey)
    if OptAlloc is None:
        raise ValueError(f"No options_allocation entry for {AllocKey}")

    DailyVol = compute_daily_vol_target(
        EffectiveCapital, Acct["annual_vol_target_pct"], OptAlloc["vol_weights"]
    )
    AnnualVol = DailyVol * 16  # ANNUALIZATION_FACTOR
    MonthlyBudget = AnnualVol / 12

    Logger.info("[%s] Daily vol: %.0f → Annual: %.0f → Monthly budget: %.0f",
                INDEX_NAME, DailyVol, AnnualVol, MonthlyBudget)

    return MonthlyBudget, EffectiveCapital, DailyVol, AnnualVol


# Skip rule tolerance: if 1 lot costs up to 30% more than the monthly budget,
# still buy 1 lot. Skip only when premium is unreasonably expensive (>30% over).
# Rationale: strict skip is too brittle when premium is just slightly over
# budget. Real-world VIX moves cause +5-25% premium swings; we want to keep
# rolling protection unless the market is truly stressed.
BUDGET_OVERSHOOT_TOLERANCE_PCT = 30.0


def ComputePositionSizePut(Premium, LotSize, MonthlyBudget):
    """Compute number of put lots using premium-at-risk sizing.

    Formula:
        cost_per_lot = premium × lot_size
        max_tolerated  = monthly_budget × (1 + BUDGET_OVERSHOOT_TOLERANCE_PCT/100)
        if cost_per_lot > max_tolerated: skip
        lots = max(1, floor(monthly_budget / cost_per_lot))

    Skip rule handles high-VIX regimes — when premiums are >30% above the
    monthly budget, we abort cleanly rather than chasing expensive insurance
    at peak fear. Within the 30% tolerance, we still buy 1 lot to maintain
    continuous protection across moderate VIX moves.

    Uses floor (not round) to avoid systematic budget overshoot across rolls.
    """
    if Premium <= 0 or LotSize <= 0:
        return {"finalLots": 0, "skipped": True, "skipReason": "invalid inputs"}
    if MonthlyBudget <= 0:
        return {"finalLots": 0, "skipped": True, "skipReason": "monthly budget zero or negative"}

    CostPerLot = Premium * LotSize
    MaxTolerated = MonthlyBudget * (1 + BUDGET_OVERSHOOT_TOLERANCE_PCT / 100)

    if CostPerLot > MaxTolerated:
        return {
            "finalLots": 0, "skipped": True,
            "skipReason": (f"cost_per_lot=Rs{CostPerLot:,.0f} > "
                           f"max_tolerated=Rs{MaxTolerated:,.0f} "
                           f"({BUDGET_OVERSHOOT_TOLERANCE_PCT:.0f}% above budget Rs{MonthlyBudget:,.0f}, "
                           f"premium too high, skip this month)"),
            "costPerLot": CostPerLot, "premium": Premium,
            "monthlyBudget": MonthlyBudget,
            "maxTolerated": MaxTolerated,
        }

    # Within tolerance — buy at least 1 lot, more if budget allows cleanly
    if CostPerLot > MonthlyBudget:
        # Cost exceeds budget but within 30% tolerance → buy 1 lot
        FinalLots = 1
        Lots = 0  # nominal floor would have given 0
    else:
        Lots = int(MonthlyBudget / CostPerLot)   # floor
        FinalLots = max(1, Lots)

    BudgetUsedPct = (CostPerLot * FinalLots / MonthlyBudget) * 100

    return {
        "finalLots": FinalLots,
        "allowedLots": Lots,
        "costPerLot": CostPerLot,
        "premium": Premium,
        "lotSize": LotSize,
        "monthlyBudget": MonthlyBudget,
        "maxTolerated": MaxTolerated,
        "budgetUsedPct": BudgetUsedPct,
        "skipped": False,
        "skipReason": None,
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
}


def LoadState():
    """Load state from JSON file. Returns default state if file missing/corrupt."""
    if not STATE_FILE_PATH.exists():
        Logger.info("State file not found, using defaults")
        return json.loads(json.dumps(DEFAULT_STATE))
    try:
        with open(STATE_FILE_PATH) as F:
            State = json.load(F)
        if INDEX_NAME not in State:
            State[INDEX_NAME] = json.loads(json.dumps(DEFAULT_STATE[INDEX_NAME]))
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

    Matches by: (1) index prefix, (2) PE type, (3) NRML product,
    (4) NFO exchange, (5) expiry = current month's monthly expiry.
    Disambiguates via PUT_ROLL order tag if multiple matches.
    """
    Logger.info("[%s] Attempting put state recovery from broker positions", IndexName)

    try:
        Positions = Kite.positions()
    except Exception as E:
        Logger.error("[%s] Failed to fetch positions: %s", IndexName, E)
        return None

    NetPositions = Positions.get("net", [])

    Matches = []
    for Pos in NetPositions:
        Symbol = Pos.get("tradingsymbol", "")
        if not Symbol.startswith(IndexName):
            continue
        if not Symbol.endswith("PE"):
            continue
        if Pos.get("product") != "NRML":
            continue
        if Pos.get("exchange") != "NFO":
            continue
        Qty = int(Pos.get("quantity", 0))
        if Qty <= 0:
            continue
        PosExpiry = Pos.get("expiry")
        if PosExpiry and isinstance(PosExpiry, date):
            if PosExpiry != CurrentMonthExpiry:
                continue
        Matches.append(Pos)

    if len(Matches) == 0:
        Logger.warning("[%s] No matching PE positions found for recovery", IndexName)
        return None

    if len(Matches) > 1:
        # Disambiguate via order tag
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
                Logger.error("[%s] Multiple PE positions match (%d), cannot auto-recover. "
                             "Manual state file update required.", IndexName, len(Matches))
                return None
        except Exception as E:
            Logger.error("[%s] Failed to fetch orders for disambiguation: %s", IndexName, E)
            return None

    Pos = Matches[0]
    Logger.info("[%s] Recovered PE position: %s qty=%s", IndexName,
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
    """Build HTML email for NIFTY put rollover."""
    Now = datetime.now()
    IsSuccess = Result.get("success", False)
    IsSkipped = Result.get("skipped", False)

    navy = "#003366"
    accent = "#2E75B6"
    green = "#27AE60"
    red = "#E74C3C"
    amber = "#F39C12"
    grey_bg = "#F8F9FA"

    if IsSkipped:
        StatusColor = amber
        StatusText = "ROLLOVER SKIPPED"
    elif IsSuccess:
        StatusColor = green
        StatusText = "ROLLOVER COMPLETE"
    else:
        StatusColor = red
        StatusText = "ROLLOVER FAILED"

    Leg1 = Result.get("leg1") or {}
    Leg2 = Result.get("leg2") or {}
    SizeResult = Result.get("size_result") or {}
    SelectionMeta = Result.get("selection") or {}
    Best = SelectionMeta.get("best") or {}
    AllCandidates = SelectionMeta.get("all_candidates") or []
    RejectedStrikes = SelectionMeta.get("rejected") or []

    DTE = Result.get("dte", "?")
    LotSize = Result.get("lot_size", "?")
    Premium = Result.get("premium", 0)
    Spot = Result.get("spot", 0)
    Strike = Result.get("strike", "—")
    MonthlyBudget = Result.get("monthly_budget", 0)
    EffCapital = Result.get("effective_capital", "?")
    NextExpiry = Result.get("next_expiry", "?")
    CurrentExpiry = Result.get("current_expiry", "?")
    SkipReason = Result.get("skip_reason") or SizeResult.get("skipReason", "")

    Html = f"""
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin:0;padding:0;font-family:Arial,Helvetica,sans-serif;background:#EAECEE;">
      <div style="max-width:680px;margin:20px auto;background:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1);">

        <!-- Header -->
        <div style="background:{navy};padding:20px 28px;">
          <h1 style="margin:0;color:#FFFFFF;font-size:20px;letter-spacing:0.5px;">
            NIFTY Put Buy &mdash; Monthly Roll
          </h1>
          <p style="margin:6px 0 0;color:#AAC4E0;font-size:13px;">
            {Now.strftime('%d %b %Y, %I:%M %p')} &bull; {CurrentExpiry} &rarr; {NextExpiry}
          </p>
        </div>

        <!-- Status Banner -->
        <div style="background:{StatusColor};padding:10px 28px;">
          <span style="color:#FFFFFF;font-size:13px;font-weight:600;">{StatusText}</span>
        </div>
    """

    # Skip banner with reason
    if IsSkipped and SkipReason:
        Html += f"""
        <div style="padding:18px 28px;background:#FFF6E5;border-left:4px solid {amber};">
          <div style="font-weight:600;color:{navy};font-size:14px;margin-bottom:4px;">Skip reason</div>
          <div style="font-family:monospace;color:#555;font-size:13px;">{SkipReason}</div>
        </div>
        """

    # ── Contract & Market Data ──
    Html += f"""
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
              <td style="padding:8px 12px;">{f"{((Strike - Spot) / Spot * 100):.1f}%" if isinstance(Strike, (int, float)) and Spot > 0 else "—"}</td>
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
              <td style="padding:8px 12px;font-weight:600;">Spread</td>
              <td style="padding:8px 12px;">{_fmtEmail(Best.get('spread_pct'), 1)}%</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Open Interest</td>
              <td style="padding:8px 12px;">{_fmtEmail(Best.get('oi'), 0)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">DTE (trading days)</td>
              <td style="padding:8px 12px;">{DTE}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Lot Size</td>
              <td style="padding:8px 12px;">{LotSize}</td>
            </tr>
          </table>
        </div>
    """

    # ── Position Sizing — Premium-at-Risk ──
    CostPerLot = SizeResult.get("costPerLot", 0)
    FinalLots = SizeResult.get("finalLots", 0)
    BudgetUsedPct = SizeResult.get("budgetUsedPct", 0)

    Html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Position Sizing &mdash; Premium-at-Risk
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">Effective Capital</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(EffCapital, 0)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Monthly Premium Budget</td>
              <td style="padding:8px 12px;">\u20B9{_fmtEmail(MonthlyBudget, 0)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Cost / Lot</td>
              <td style="padding:8px 12px;">{LotSize} \u00D7 \u20B9{_fmtEmail(Premium)} = \u20B9{_fmtEmail(CostPerLot, 0)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Lots</td>
              <td style="padding:8px 12px;">floor(\u20B9{_fmtEmail(MonthlyBudget, 0)} / \u20B9{_fmtEmail(CostPerLot, 0)}) = <b>{FinalLots}</b></td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Total Quantity</td>
              <td style="padding:8px 12px;font-weight:700;color:{navy};">{FinalLots * (LotSize if isinstance(LotSize, int) else 0)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Budget Utilisation</td>
              <td style="padding:8px 12px;">{_fmtEmail(BudgetUsedPct, 1)}%</td>
            </tr>
          </table>
        </div>
    """

    # ── Strike Candidates Table ──
    if AllCandidates:
        Html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Strike Candidates
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:12px;">
            <tr style="background:{navy};color:#FFF;">
              <th style="padding:6px 10px;text-align:left;">Strike</th>
              <th style="padding:6px 10px;text-align:right;">Moneyness</th>
              <th style="padding:6px 10px;text-align:right;">Premium</th>
              <th style="padding:6px 10px;text-align:right;">BS Theo</th>
              <th style="padding:6px 10px;text-align:right;">Value%</th>
              <th style="padding:6px 10px;text-align:right;">Spread%</th>
              <th style="padding:6px 10px;text-align:right;">OI</th>
            </tr>
        """
        for C in AllCandidates:
            IsBest = (Best and C.get("strike") == Best.get("strike"))
            RowStyle = f"background:{accent};color:#FFF;font-weight:600;" if IsBest else f"background:{grey_bg};"
            Mn = (C["strike"] - Spot) / Spot * 100 if Spot else 0
            Html += f"""
            <tr style="{RowStyle}">
              <td style="padding:6px 10px;">{C['strike']}{' &lt;&lt;&lt;' if IsBest else ''}</td>
              <td style="padding:6px 10px;text-align:right;">{Mn:+.1f}%</td>
              <td style="padding:6px 10px;text-align:right;">\u20B9{_fmtEmail(C.get('premium'))}</td>
              <td style="padding:6px 10px;text-align:right;">\u20B9{_fmtEmail(C.get('bs_theo'))}</td>
              <td style="padding:6px 10px;text-align:right;">{_fmtEmail(C.get('value_pct'), 1)}%</td>
              <td style="padding:6px 10px;text-align:right;">{_fmtEmail(C.get('spread_pct'), 1)}%</td>
              <td style="padding:6px 10px;text-align:right;">{_fmtEmail(C.get('oi'), 0)}</td>
            </tr>
            """
        Html += "</table></div>"

    # ── Rejected Strikes ──
    if RejectedStrikes:
        Html += f"""
        <div style="padding:18px 28px 0;">
          <h3 style="margin:0 0 8px;color:{red};font-size:14px;">Rejected Strikes</h3>
          <ul style="font-size:12px;color:#555;padding-left:20px;margin:0;">
        """
        for Strike, ValDetails in RejectedStrikes:
            Reasons = "; ".join(ValDetails.get("checks_failed", []))
            Html += f"<li><b>{Strike}</b>: {Reasons}</li>"
        Html += "</ul></div>"

    # ── Leg Details ──
    if Leg1:
        Pnl = Leg1.get("realized_pnl")
        PnlColor = green if Pnl and Pnl >= 0 else red
        PnlStr = f"<span style='color:{PnlColor};font-weight:700;'>\u20B9{_fmtEmail(Pnl, 0)}</span>" if Pnl is not None else "—"
        Html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            LEG 1 &mdash; Exit (SELL)
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};"><td style="padding:6px 12px;font-weight:600;">Contract</td><td style="padding:6px 12px;font-family:monospace;">{Leg1.get('contract', '—')}</td></tr>
            <tr><td style="padding:6px 12px;font-weight:600;">Quantity</td><td style="padding:6px 12px;">{Leg1.get('quantity', '—')}</td></tr>
            <tr style="background:{grey_bg};"><td style="padding:6px 12px;font-weight:600;">Fill Price</td><td style="padding:6px 12px;">\u20B9{_fmtEmail(Leg1.get('fill_price'))}</td></tr>
            <tr><td style="padding:6px 12px;font-weight:600;">Slippage</td><td style="padding:6px 12px;">{_fmtEmail(Leg1.get('slippage'))}</td></tr>
            <tr style="background:{grey_bg};"><td style="padding:6px 12px;font-weight:600;">Realised P&amp;L</td><td style="padding:6px 12px;">{PnlStr}</td></tr>
          </table>
        </div>
        """

    if Leg2:
        Html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            LEG 2 &mdash; Entry (BUY)
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};"><td style="padding:6px 12px;font-weight:600;">Contract</td><td style="padding:6px 12px;font-family:monospace;">{Leg2.get('contract', '—')}</td></tr>
            <tr><td style="padding:6px 12px;font-weight:600;">Lots / Quantity</td><td style="padding:6px 12px;">{Leg2.get('lots', '—')} / {Leg2.get('quantity', '—')}</td></tr>
            <tr style="background:{grey_bg};"><td style="padding:6px 12px;font-weight:600;">Fill Price</td><td style="padding:6px 12px;">\u20B9{_fmtEmail(Leg2.get('fill_price'))}</td></tr>
            <tr><td style="padding:6px 12px;font-weight:600;">Slippage</td><td style="padding:6px 12px;">{_fmtEmail(Leg2.get('slippage'))}</td></tr>
            <tr style="background:{grey_bg};"><td style="padding:6px 12px;font-weight:600;">Expiry</td><td style="padding:6px 12px;">{Leg2.get('expiry', '—')}</td></tr>
          </table>
        </div>
        """

    # ── Footer ──
    Html += f"""
        <div style="padding:24px 28px;color:#888;font-size:11px;border-top:1px solid #DEE2E6;margin-top:24px;">
          NIFTY Put Buy Rollover &bull; Premium-at-risk sizing &bull; OFS653 &bull; {Now.strftime('%Y-%m-%d %H:%M')}
        </div>
      </div>
    </body>
    </html>
    """
    return Html


def SendEmail(Subject, HtmlBody):
    """Send an HTML email via Gmail SMTP. Failures are non-blocking."""
    if not EMAIL_NOTIFY_ENABLED:
        return
    try:
        Msg = MIMEMultipart("alternative")
        Msg["From"] = EMAIL_FROM
        Msg["To"] = EMAIL_TO
        Msg["Subject"] = Subject
        Msg.attach(MIMEText(HtmlBody, "html"))

        with smtplib.SMTP_SSL(EMAIL_SMTP, EMAIL_PORT) as Server:
            Server.login(EMAIL_FROM, EMAIL_FROM_PASSWORD)
            Server.send_message(Msg)
        Logger.info("Email sent: %s", Subject)
    except Exception as E:
        Logger.warning("Failed to send email: %s", E)


# ─── Execution Config ───────────────────────────────────────────────

def LoadExecConfig(ConfigKey):
    """Load SmartChase execution config."""
    try:
        with open(EXEC_CONFIG_PATH) as F:
            AllConfig = json.load(F)
        Cfg = AllConfig.get(ConfigKey, {})
        return Cfg.get("execution", {})
    except FileNotFoundError:
        Logger.warning("options_execution_config.json not found, using empty config")
        return {}


# ─── Core Execution ─────────────────────────────────────────────────

def ExecuteRollover(Kite, IndexName, State, DryRun=False, FirstRun=False):
    """Execute two-leg NIFTY put rollover.

    Returns a result dict with success/skipped status and fill details.
    """
    IdxCfg = PUT_CONFIG[IndexName]
    IdxState = State[IndexName]
    Tag = f"[{IndexName}]"

    Result = {
        "success": False,
        "skipped": False,
        "index": IndexName,
        "leg1": None,
        "leg2": None,
        "roll_spread": None,
    }

    # ── Step 1: Load monthly budget ──────────────────────────────
    try:
        MonthlyBudget, EffCapital, DailyVol, AnnualVol = LoadMonthlyBudget()
    except Exception as E:
        Logger.error("%s Failed to load monthly budget: %s", Tag, E)
        Result["error"] = str(E)
        return Result
    Result["monthly_budget"] = MonthlyBudget
    Result["effective_capital"] = EffCapital
    Result["daily_vol"] = DailyVol
    Result["annual_vol"] = AnnualVol

    # ── Step 2: Fetch spot ───────────────────────────────────────
    try:
        SpotData = Kite.ltp([IdxCfg["underlying_ltp_key"]])
        Spot = float(SpotData[IdxCfg["underlying_ltp_key"]]["last_price"])
    except Exception as E:
        Logger.error("%s Failed to fetch spot: %s", Tag, E)
        Result["error"] = str(E)
        return Result
    Result["spot"] = Spot
    Logger.info("%s Spot=%.1f", Tag, Spot)

    # ── Step 3: Get instruments and expiries ─────────────────────
    Instruments = GetInstrumentsCached(Kite, IdxCfg["exchange"])
    OptSegment = GetOptSegmentForExchange(IdxCfg["exchange"])
    MonthlyExpiries = GetMonthlyExpiries(Instruments, IndexName, OptSegment)

    if not MonthlyExpiries:
        Logger.error("%s No monthly expiries available", Tag)
        return Result

    CurrentExpiry = GetCurrentMonthExpiry(MonthlyExpiries)
    NextExpiry = GetNextMonthExpiry(MonthlyExpiries, CurrentExpiry) if CurrentExpiry else None

    # Fallback for off-cycle runs (e.g. --force on a non-expiry day after current
    # month's expiry has already passed): use the soonest available expiry as
    # both current and next, treating the run as a cold-start equivalent.
    if CurrentExpiry is None:
        Logger.warning("%s No current-month expiry available (likely after this month's "
                       "expiry day). Using next available expiry as target.", Tag)
        CurrentExpiry = MonthlyExpiries[0]
        NextExpiry = MonthlyExpiries[1] if len(MonthlyExpiries) > 1 else CurrentExpiry
        FirstRun = True   # no current-month position to sell

    if NextExpiry is None:
        Logger.error("%s Cannot determine next month expiry", Tag)
        return Result

    Logger.info("%s Current monthly expiry=%s, next=%s", Tag, CurrentExpiry, NextExpiry)

    # ── Step 4: Select put strike ────────────────────────────────
    TargetExpiry = CurrentExpiry if FirstRun else NextExpiry
    Logger.info("%s Target expiry: %s (%s)", Tag, TargetExpiry,
                "first-run → current month" if FirstRun else "rollover → next month")

    Candidates = ComputePutCandidates(Spot, IdxCfg["strike_step"],
                                       IdxCfg["itm_pct_min"], IdxCfg["itm_pct_max"])
    Logger.info("%s Put candidates: %s", Tag, Candidates)

    try:
        Strike, Symbol, LotSize, Premium, SelectionMeta = SelectBestPutStrike(
            Kite, Instruments, IndexName, IdxCfg["exchange"], OptSegment,
            TargetExpiry, Candidates, Spot=Spot
        )
    except Exception as E:
        Logger.error("%s Strike selection failed: %s", Tag, E)
        Result["error"] = str(E)
        return Result

    Result["strike"] = Strike
    Result["lot_size"] = LotSize
    Result["premium"] = Premium
    Result["symbol"] = Symbol
    Result["selection"] = SelectionMeta
    Result["current_expiry"] = str(CurrentExpiry)
    Result["next_expiry"] = str(NextExpiry)

    DTE = CountTradingDaysUntilExpiry(TargetExpiry)
    Result["dte"] = DTE

    # ── Step 5: Position sizing (premium-at-risk) ────────────────
    SizeResult = ComputePositionSizePut(Premium, LotSize, MonthlyBudget)
    Result["size_result"] = SizeResult

    if SizeResult["skipped"]:
        # Skip rule triggered — log and abort cleanly (not an error)
        Logger.warning("%s SKIPPING this month: %s", Tag, SizeResult["skipReason"])
        Result["skipped"] = True
        Result["skip_reason"] = SizeResult["skipReason"]

        # Log skip to DB for audit trail
        if not DryRun:
            try:
                db.LogNiftyPutRollover(
                    IndexName, str(CurrentExpiry),
                    IdxState.get("current_contract", ""),
                    Symbol, IdxState.get("quantity", 0), 0,
                    MonthlyBudget=MonthlyBudget,
                    CostPerLot=SizeResult.get("costPerLot"),
                    Premium=Premium, Broker="ZERODHA", UserAccount=USER,
                    SkipReason=SizeResult["skipReason"]
                )
            except Exception as E:
                Logger.warning("%s Failed to log skip to DB: %s", Tag, E)

        return Result

    FinalLots = SizeResult["finalLots"]
    Quantity = FinalLots * LotSize
    Logger.info("%s Sizing: lots=%d qty=%d premium=%.2f cost/lot=%.0f budget=%.0f used=%.1f%%",
                Tag, FinalLots, Quantity, Premium,
                SizeResult["costPerLot"], MonthlyBudget,
                SizeResult["budgetUsedPct"])

    ExecConfig = LoadExecConfig(IdxCfg["exec_config_key"])

    if DryRun:
        Logger.info("%s [DRY RUN] Would exit %s qty=%s, then buy %s qty=%d",
                    Tag, IdxState.get("current_contract", "N/A"),
                    IdxState.get("quantity", 0), Symbol, Quantity)
        Result["success"] = True
        Result["leg2"] = {
            "contract": Symbol, "quantity": Quantity, "lots": FinalLots,
            "premium": Premium, "fill_price": Premium, "slippage": 0,
            "expiry": str(TargetExpiry),
        }
        return Result

    # ── Step 6: Log to DB ────────────────────────────────────────
    OldContract = IdxState.get("current_contract", "")
    OldQty = IdxState.get("quantity", 0)
    RowId = db.LogNiftyPutRollover(
        IndexName, str(CurrentExpiry), OldContract, Symbol,
        OldQty, Quantity,
        MonthlyBudget=MonthlyBudget,
        CostPerLot=SizeResult["costPerLot"],
        Premium=Premium,
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
            db.UpdateNiftyPutRolloverStatus(RowId, "LEG1_FAILED",
                                             leg1_fill_price=Leg1FillPrice,
                                             leg1_slippage=Leg1Slippage)
            Result["leg1"] = {"contract": Leg1Contract, "quantity": Leg1Qty,
                              "fill_price": Leg1FillPrice, "slippage": Leg1Slippage}
            return Result

        # Realize P&L
        EntryPrice = float(IdxState.get("entry_price", 0))
        RealizedPnl = None
        if EntryPrice > 0:
            RealizedPnl = (Leg1FillPrice - EntryPrice) * Leg1Qty * 1.0
            try:
                db.RealizePnl(
                    f"{IndexName}_PUT_BUY", Leg1FillPrice, Leg1Qty, 1.0,
                    Category="options", WasLong=True
                )
            except Exception as E:
                Logger.warning("%s RealizePnl failed: %s", Tag, E)
            Logger.info("%s LEG 1 P&L: entry=%.2f exit=%.2f qty=%d pnl=%.0f",
                        Tag, EntryPrice, Leg1FillPrice, Leg1Qty, RealizedPnl)

        db.UpdateNiftyPutRolloverStatus(RowId, "LEG1_DONE",
                                         leg1_order_id=str(Leg1OrderId),
                                         leg1_fill_price=Leg1FillPrice,
                                         leg1_slippage=Leg1Slippage,
                                         realized_pnl=RealizedPnl)

        try:
            db.LogOptionsSmartChaseOrder(
                IndexName, "PUT_BUY_ROLLOVER", "EXIT", Leg1Contract, "SELL",
                Leg1Qty, BrokerOrderId=str(Leg1OrderId), FillInfo=Leg1FillInfo
            )
        except Exception as E:
            Logger.warning("%s LogOptionsSmartChaseOrder failed: %s", Tag, E)

        Result["leg1"] = {
            "contract": Leg1Contract, "quantity": Leg1Qty,
            "fill_price": Leg1FillPrice, "slippage": Leg1Slippage,
            "realized_pnl": RealizedPnl,
        }
    else:
        Logger.info("%s Skipping LEG 1 (first run or no existing position)", Tag)

    # ── Step 8: LEG 2 — Buy next month put ──────────────────────
    Logger.info("%s LEG 2: BUY %s qty=%d", Tag, Symbol, Quantity)

    Leg2Order = BuildOrderDict(IndexName, Symbol, "BUY", Quantity)
    Leg2Success, Leg2OrderId, Leg2FillInfo = SmartChaseExecute(
        Kite, Leg2Order, ExecConfig, IsEntry=True, Broker="ZERODHA", ATR=0
    )

    Leg2FillPrice = Leg2FillInfo.get("fill_price", 0) if Leg2FillInfo else 0
    Leg2Slippage = Leg2FillInfo.get("slippage", 0) if Leg2FillInfo else 0

    if not Leg2Success:
        Logger.error("%s LEG 2 FAILED — position is FLAT", Tag)
        db.UpdateNiftyPutRolloverStatus(RowId, "LEG2_FAILED",
                                         leg2_fill_price=Leg2FillPrice,
                                         leg2_slippage=Leg2Slippage)
        Result["leg2"] = {"contract": Symbol, "quantity": Quantity,
                          "fill_price": Leg2FillPrice, "slippage": Leg2Slippage}
        SendEmail(f"CRITICAL: NIFTY Put Buy LEG 2 FAILED — FLAT",
                  BuildRolloverEmailHtml(IndexName, Result))
        return Result

    # Update cost basis
    if Leg2FillPrice > 0:
        try:
            db.UpdateCostBasis(f"{IndexName}_PUT_BUY", Leg2FillPrice, Quantity, 1.0)
        except Exception as E:
            Logger.warning("%s UpdateCostBasis failed: %s", Tag, E)

    # Roll spread (price difference between exit and entry)
    RollSpread = None
    if Leg1FillPrice and Leg1FillPrice > 0:
        RollSpread = Leg2FillPrice - Leg1FillPrice

    db.UpdateNiftyPutRolloverStatus(RowId, "COMPLETE",
                                     new_contract=Symbol,
                                     leg2_order_id=str(Leg2OrderId),
                                     leg2_fill_price=Leg2FillPrice,
                                     leg2_slippage=Leg2Slippage,
                                     roll_spread=RollSpread,
                                     executed_at=datetime.now().isoformat())

    try:
        db.LogOptionsSmartChaseOrder(
            IndexName, "PUT_BUY_ROLLOVER", "ENTRY", Symbol, "BUY",
            Quantity, BrokerOrderId=str(Leg2OrderId), FillInfo=Leg2FillInfo
        )
    except Exception as E:
        Logger.warning("%s LogOptionsSmartChaseOrder failed: %s", Tag, E)

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
    """Print current state and recent put rollovers."""
    State = LoadState()
    print("\n=== NIFTY Put Buy Rollover Status ===\n")

    S = State[INDEX_NAME]
    print(f"  {INDEX_NAME}:")
    print(f"    Status:   {S['status']}")
    print(f"    Contract: {S.get('current_contract', '—')}")
    print(f"    Expiry:   {S.get('current_expiry', '—')}")
    print(f"    Lots:     {S.get('lots', 0)}")
    print(f"    Quantity: {S.get('quantity', 0)}")
    print(f"    Entry:    ₹{S.get('entry_price', 0):,.2f}")
    print(f"    Date:     {S.get('entry_date', '—')}")
    print()

    # Budget
    try:
        MonthlyBudget, EffCap, DailyVol, AnnualVol = LoadMonthlyBudget()
        print(f"  Effective Capital:    ₹{EffCap:,.0f}")
        print(f"  Daily Vol Target:     ₹{DailyVol:,.0f}")
        print(f"  Annual Allocation:    ₹{AnnualVol:,.0f}")
        print(f"  Monthly Premium Cap:  ₹{MonthlyBudget:,.0f}")
        print()
    except Exception as E:
        print(f"  (Could not load monthly budget: {E})\n")

    # Recent rollovers
    try:
        db.InitDB()
        Recent = db.GetRecentNiftyPutRollovers(limit=10)
        if Recent:
            print("  Recent Rollovers:")
            for R in Recent:
                Skip = f" [SKIP: {R.get('skip_reason', '')[:40]}]" if R.get('status') == 'PENDING' and R.get('skip_reason') else ""
                print(f"    {R['created_at']} | {R['status']} | "
                      f"{R.get('old_contract', '—')} → {R.get('new_contract', '—')}{Skip}")
        else:
            print("  No recent rollovers logged.")
        print()
    except Exception:
        pass


# ─── Main ───────────────────────────────────────────────────────────

def main():
    Parser = argparse.ArgumentParser(description="NIFTY Monthly Put Buy Rollover")
    Parser.add_argument("--dry-run", action="store_true", help="Log decisions, no orders")
    Parser.add_argument("--force", action="store_true", help="Force rollover regardless of date")
    Parser.add_argument("--first-run", action="store_true", help="Cold start: buy only, no exit")
    Parser.add_argument("--status", action="store_true", help="Print current state")
    Args = Parser.parse_args()

    # Logging setup
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Path(WorkDirectory) / "nifty_put_rollover.log"),
        ]
    )

    if Args.status:
        PrintStatus()
        return

    Logger.info("=" * 60)
    Logger.info("NIFTY Put Buy Rollover started | dry_run=%s force=%s first_run=%s",
                Args.dry_run, Args.force, Args.first_run)

    # Initialize DB
    db.InitDB()

    # Establish session
    try:
        Kite = EstablishKiteSession()
    except Exception as E:
        Logger.error("Failed to establish Kite session: %s", E)
        SendEmail("NIFTY Put Buy Rollover: SESSION FAILED",
                  f"<p>Failed to connect: {E}</p>")
        sys.exit(1)

    # Load state
    State = LoadState()

    # Get instruments
    Instruments = GetInstrumentsCached(Kite, "NFO")
    OptSegment = GetOptSegmentForExchange("NFO")

    Result = None

    try:
        # Check if today is monthly expiry
        IsExpiry, ExpiryDate = IsMonthlyExpiryDay(Instruments, INDEX_NAME, OptSegment)

        if not IsExpiry and not Args.force:
            Logger.info("[%s] Not monthly expiry day, skipping", INDEX_NAME)
            sys.exit(0)

        if Args.force and not IsExpiry:
            Logger.info("[%s] --force flag: proceeding despite not expiry day", INDEX_NAME)

        # Crash recovery — detect incomplete LEG1_DONE rollovers
        IncompleteRollovers = db.GetIncompleteNiftyPutRollovers(INDEX_NAME)
        if IncompleteRollovers:
            Logger.warning("[%s] Found %d incomplete put rollovers (LEG1_DONE), "
                           "will skip leg 1 and retry leg 2", INDEX_NAME, len(IncompleteRollovers))
            Args.first_run = True

        # State recovery if needed
        if State[INDEX_NAME]["status"] == "NONE" and not Args.first_run:
            MonthlyExpiries = GetMonthlyExpiries(Instruments, INDEX_NAME, OptSegment)
            CurrentExpiry = GetCurrentMonthExpiry(MonthlyExpiries)
            if CurrentExpiry:
                Recovered = RecoverStateFromPositions(Kite, INDEX_NAME, CurrentExpiry)
                if Recovered:
                    State[INDEX_NAME] = Recovered
                    SaveState(State)
                    Logger.info("[%s] State recovered from broker positions", INDEX_NAME)

        # Execute rollover
        Result = ExecuteRollover(Kite, INDEX_NAME, State,
                                  DryRun=Args.dry_run, FirstRun=Args.first_run)

        # Send result email
        if Result.get("skipped"):
            StatusStr = "SKIPPED"
        elif Result.get("success"):
            StatusStr = "SUCCESS"
        else:
            StatusStr = "FAILED"
        SendEmail(
            f"NIFTY Put Buy: {StatusStr}",
            BuildRolloverEmailHtml(INDEX_NAME, Result)
        )

    except Exception as E:
        Logger.exception("[%s] Unhandled error: %s", INDEX_NAME, E)
        SendEmail(
            f"NIFTY Put Buy: ERROR",
            f"<p style='color:red;font-weight:bold'>Unhandled error: {E}</p>"
        )

    Logger.info("=" * 60)
    if Result:
        if Result.get("skipped"):
            Logger.info("Result: SKIPPED — %s", Result.get("skip_reason", ""))
        elif Result.get("success"):
            Logger.info("Result: SUCCESS")
        else:
            Logger.info("Result: FAILED")
    Logger.info("NIFTY Put Buy Rollover finished")


if __name__ == "__main__":
    main()
