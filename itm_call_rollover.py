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
from PlaceOptionsSystemsV2 import lookupK, K_TABLE_SINGLE
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
                        ExpiryDate, Candidates):
    """From candidate strikes, pick the one with tightest bid-ask spread.

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

    BestStrike = None
    BestSymbol = None
    BestLotSize = None
    BestPremium = None
    BestSpreadPct = float("inf")

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

            Depth = Q.get("depth", {})
            Buys = Depth.get("buy", [])
            Sells = Depth.get("sell", [])
            Bid = float(Buys[0].get("price", 0)) if Buys else 0
            Ask = float(Sells[0].get("price", 0)) if Sells else 0

            if Bid > 0 and Ask > 0:
                Mid = (Bid + Ask) / 2
                SpreadPct = (Ask - Bid) / Mid * 100 if Mid > 0 else 999
            else:
                SpreadPct = 999

            if SpreadPct < BestSpreadPct:
                BestSpreadPct = SpreadPct
                BestStrike = Strike
                BestSymbol = Ins["tradingsymbol"]
                BestLotSize = int(Ins.get("lot_size", 1))
                BestPremium = Premium

    if BestSymbol is None:
        raise Exception(f"No liquid CE contracts found for {IndexName} expiry={ExpiryDate}")

    Logger.info("[%s] Selected strike=%d symbol=%s premium=%.2f spread=%.1f%% lotSize=%d",
                IndexName, BestStrike, BestSymbol, BestPremium, BestSpreadPct, BestLotSize)
    return BestStrike, BestSymbol, BestLotSize, BestPremium


# ─── Position Sizing ────────────────────────────────────────────────

def LoadVolBudgets():
    """Compute ITM call daily vol budgets from effective capital."""
    with open(CONFIG_PATH) as F:
        Cfg = json.load(F)
    Acct = Cfg["account"]
    BaseCapital = Acct["base_capital"]

    # Read realized + unrealized from EOD JSON, fall back to DB
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
        CumulativeRealized = float(PnlData.get("cumulative_realized_pnl", 0.0))
        EodUnrealized = float(PnlData.get("eod_unrealized", 0.0))
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


def ComputePositionSizeITM(Premium, LotSize, KValue, DailyVolBudget):
    """Compute number of lots for a single ITM call.

    dailyVolPerLot = k × premium × lotSize
    lots = round(budget / dailyVolPerLot)
    """
    if Premium <= 0 or LotSize <= 0 or KValue <= 0:
        return {"finalLots": 0, "skipped": True, "skipReason": "invalid inputs"}

    DailyVolPerLot = KValue * Premium * LotSize
    if DailyVolPerLot <= 0:
        return {"finalLots": 0, "skipped": True, "skipReason": "dailyVolPerLot zero"}

    AllowedLots = int(DailyVolBudget / DailyVolPerLot + 0.5)  # round half-up
    FinalLots = max(1, AllowedLots)  # always at least 1 lot

    return {
        "finalLots": FinalLots,
        "allowedLots": AllowedLots,
        "dailyVolPerLot": DailyVolPerLot,
        "premium": Premium,
        "kValue": KValue,
        "dailyVolBudget": DailyVolBudget,
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

def BuildRolloverEmailHtml(IndexName, Result):
    """Build detailed HTML email for ITM call rollover."""
    Now = datetime.now()
    IsSuccess = Result.get("success", False)
    StatusColor = "#27AE60" if IsSuccess else "#E74C3C"
    StatusText = "ROLLOVER COMPLETE" if IsSuccess else "ROLLOVER FAILED"

    Leg1 = Result.get("leg1", {})
    Leg2 = Result.get("leg2", {})

    Html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:auto;border:1px solid #ddd;border-radius:8px;overflow:hidden">
      <div style="background:#003366;color:white;padding:16px 20px">
        <h2 style="margin:0">ITM Call Rollover — {IndexName}</h2>
        <p style="margin:4px 0 0;opacity:0.8;font-size:13px">{Now.strftime('%d %b %Y %H:%M:%S')}</p>
      </div>

      <div style="background:{StatusColor};color:white;padding:10px 20px;font-weight:bold;font-size:15px">
        {StatusText}
      </div>

      <div style="padding:16px 20px">
        <table style="width:100%;border-collapse:collapse;font-size:14px">
          <tr style="border-bottom:1px solid #eee">
            <td style="padding:8px 4px;color:#666">Vol Budget</td>
            <td style="padding:8px 4px;font-weight:bold;text-align:right">₹{Result.get('daily_vol_budget', 0):,.0f}</td>
          </tr>
          <tr style="border-bottom:1px solid #eee">
            <td style="padding:8px 4px;color:#666">K Value (DTE={Result.get('dte', '?')})</td>
            <td style="padding:8px 4px;font-weight:bold;text-align:right">{Result.get('k_value', 0):.3f}</td>
          </tr>
          <tr style="border-bottom:1px solid #eee">
            <td style="padding:8px 4px;color:#666">Spot Price</td>
            <td style="padding:8px 4px;font-weight:bold;text-align:right">₹{Result.get('spot', 0):,.1f}</td>
          </tr>
          <tr style="border-bottom:1px solid #eee">
            <td style="padding:8px 4px;color:#666">Strike Selected</td>
            <td style="padding:8px 4px;font-weight:bold;text-align:right">{Result.get('strike', '—')}</td>
          </tr>
        </table>
      </div>
    """

    # Leg 1 (Exit)
    if Leg1:
        Pnl = Leg1.get("realized_pnl")
        PnlStr = f"₹{Pnl:+,.0f}" if Pnl is not None else "—"
        PnlColor = "#27AE60" if Pnl and Pnl >= 0 else "#E74C3C"
        Html += f"""
      <div style="padding:0 20px 16px">
        <h3 style="color:#003366;margin:12px 0 8px;font-size:14px">LEG 1 — EXIT</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px;background:#F8F9FA;border-radius:4px">
          <tr><td style="padding:6px 8px;color:#666">Contract</td><td style="padding:6px 8px;text-align:right">{Leg1.get('contract', '—')}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Quantity</td><td style="padding:6px 8px;text-align:right">{Leg1.get('quantity', '—')}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Fill Price</td><td style="padding:6px 8px;text-align:right">₹{Leg1.get('fill_price', 0):,.2f}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Slippage</td><td style="padding:6px 8px;text-align:right">{Leg1.get('slippage', 0):+.2f}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Realized P&L</td><td style="padding:6px 8px;text-align:right;color:{PnlColor};font-weight:bold">{PnlStr}</td></tr>
        </table>
      </div>
        """

    # Leg 2 (Entry)
    if Leg2:
        Html += f"""
      <div style="padding:0 20px 16px">
        <h3 style="color:#003366;margin:12px 0 8px;font-size:14px">LEG 2 — ENTRY</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px;background:#F8F9FA;border-radius:4px">
          <tr><td style="padding:6px 8px;color:#666">Contract</td><td style="padding:6px 8px;text-align:right">{Leg2.get('contract', '—')}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Quantity</td><td style="padding:6px 8px;text-align:right">{Leg2.get('quantity', '—')}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Lots</td><td style="padding:6px 8px;text-align:right">{Leg2.get('lots', '—')}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Premium</td><td style="padding:6px 8px;text-align:right">₹{Leg2.get('premium', 0):,.2f}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Fill Price</td><td style="padding:6px 8px;text-align:right">₹{Leg2.get('fill_price', 0):,.2f}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Slippage</td><td style="padding:6px 8px;text-align:right">{Leg2.get('slippage', 0):+.2f}</td></tr>
          <tr><td style="padding:6px 8px;color:#666">Expiry</td><td style="padding:6px 8px;text-align:right">{Leg2.get('expiry', '—')}</td></tr>
        </table>
      </div>
        """

    # Roll spread
    RollSpread = Result.get("roll_spread")
    if RollSpread is not None:
        Html += f"""
      <div style="padding:0 20px 16px">
        <table style="width:100%;font-size:13px">
          <tr><td style="padding:6px 4px;color:#666">Roll Spread (entry - exit)</td>
              <td style="padding:6px 4px;text-align:right;font-weight:bold">₹{RollSpread:+,.2f}</td></tr>
        </table>
      </div>
        """

    Html += "</div>"
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

def ExecuteRollover(Kite, IndexName, State, DryRun=False, FirstRun=False):
    """Execute two-leg ITM call rollover for one index.

    Returns a result dict with success status and fill details.
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

    # ── Step 4: Select ITM strike for next month ─────────────────
    Candidates = ComputeITMCallCandidates(Spot, IdxCfg["strike_step"],
                                           IdxCfg["itm_pct_min"], IdxCfg["itm_pct_max"])
    Logger.info("%s ITM candidates: %s", Tag, Candidates)

    Strike, Symbol, LotSize, Premium = SelectBestITMStrike(
        Kite, Instruments, IndexName, IdxCfg["exchange"], OptSegment,
        NextExpiry, Candidates
    )
    Result["strike"] = Strike

    # ── Step 5: Position sizing ──────────────────────────────────
    DTE = CountTradingDaysUntilExpiry(NextExpiry)
    KValue = lookupK(DTE, K_TABLE_SINGLE)
    Result["dte"] = DTE
    Result["k_value"] = KValue

    SizeResult = ComputePositionSizeITM(Premium, LotSize, KValue, DailyVolBudget)
    if SizeResult["skipped"]:
        Logger.error("%s Position sizing skipped: %s", Tag, SizeResult["skipReason"])
        return Result

    FinalLots = SizeResult["finalLots"]
    Quantity = FinalLots * LotSize
    Logger.info("%s Sizing: lots=%d qty=%d premium=%.2f K=%.3f dailyVol/lot=%.0f budget=%.0f",
                Tag, FinalLots, Quantity, Premium, KValue,
                SizeResult["dailyVolPerLot"], DailyVolBudget)

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
        "expiry": str(NextExpiry),
    }
    Result["roll_spread"] = RollSpread
    Result["success"] = True

    # ── Step 9: Update state ─────────────────────────────────────
    State[IndexName] = {
        "status": "HOLDING",
        "current_contract": Symbol,
        "current_expiry": str(NextExpiry),
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

    for IndexName in Indices:
        Logger.info("-" * 40)
        Logger.info("Processing %s", IndexName)

        try:
            # Check if today is monthly expiry
            IsExpiry, ExpiryDate = IsMonthlyExpiryDay(Instruments, IndexName, OptSegment)

            if not IsExpiry and not Args.force:
                Logger.info("[%s] Not monthly expiry day, skipping", IndexName)
                continue

            if Args.force and not IsExpiry:
                Logger.info("[%s] --force flag: proceeding despite not expiry day", IndexName)

            # Check crash recovery
            IncompleteRollovers = db.GetIncompleteITMCallRollovers(IndexName)
            if IncompleteRollovers:
                Logger.warning("[%s] Found %d incomplete rollovers (LEG1_DONE), "
                               "will skip leg 1 and retry leg 2", IndexName, len(IncompleteRollovers))
                # Treat as first-run (skip leg 1) since position is flat after leg 1
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

            # Execute rollover
            Result = ExecuteRollover(Kite, IndexName, State, DryRun=Args.dry_run,
                                     FirstRun=Args.first_run)
            AllResults[IndexName] = Result

            # Send email for this index
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
            # Continue to next index (independent execution)
            continue

    # Summary
    Logger.info("=" * 60)
    for Idx, Res in AllResults.items():
        Logger.info("  %s: %s", Idx, "SUCCESS" if Res.get("success") else "FAILED")
    Logger.info("ITM Call Rollover finished")


if __name__ == "__main__":
    main()
