"""
PlaceOptionsSystemsV2.py - Config-driven ATM short straddle options engine.

Replaces the legacy strategy layer in PlaceFNOTradesKite.py with 4 clean systems:
  - N_STD_4D_30SL_I   (NIFTY early, 3-4 DTE, max 5 lots)
  - N_STD_2D_55SL_I   (NIFTY late, 2 DTE, max 3 lots)
  - SX_STD_4D_20SL_I  (SENSEX early, 3-4 DTE, max 4 lots)
  - SX_STD_2D_100SL_I (SENSEX late, 2 DTE, max 2 lots)

Sizing uses daily-volatility formula with DTE-dependent k tables.
State machine enforces early->late lifecycle per underlying.
Reuses existing broker utilities: order(), Set_Gtt(), FetchContractName().
"""

import json
import math
import csv
import sys
import time
import logging
from datetime import datetime, date, timedelta
from pathlib import Path

from FetchOptionContractName import (
    FetchContractName,
    GetKiteClient,
    GetDerivativesExchange,
    GetInstrumentsCached,
    SelectExpiryDateFromInstruments,
    GetOptSegmentForExchange,
)
from Server_Order_Place import order
from Set_Gtt_Exit import Set_Gtt
from Holidays import CheckForDateHoliday
from Directories import WorkDirectory


# ---------------------------------------------------------------------------
# SECTION 1: Constants and K Tables
# ---------------------------------------------------------------------------

LOT_SIZES = {
    "NIFTY": 65,
    "SENSEX": 20,
}

EXCHANGE_MAP = {
    "NIFTY": "NFO",
    "SENSEX": "BFO",
}

EXPIRY_DAY_MAP = {
    "NIFTY": "1",     # Tuesday (weekday int used by FetchOptionName)
    "SENSEX": "3",    # Thursday (resolved to actual expiry by SelectExpiryDateFromInstruments)
}

UNDERLYING_LTP_KEY = {
    "NIFTY": "NSE:NIFTY 50",
    "SENSEX": "BSE:SENSEX",
}

# K values for ATM short straddle by DTE bucket
# Format: (minDte, maxDte, kValue)
#
# Theory: Straddle ≈ 0.8 × S × σ × √T  (Black-Scholes approximation)
#   where S = spot, σ = annualised IV, T = time to expiry in years.
#   Daily expected move = S × σ / √365.
#   So: straddle / daily_move ≈ 0.8 × √T_days
#   Inverting: daily_vol_fraction ≈ 1 / (0.8 × √T) ≈ 1/√T (simplified)
#
# K represents what fraction of the straddle premium is at risk per day.
# Higher K = higher daily risk per lot = fewer lots allowed = more conservative.
#
# IMPORTANT: At entry, K is looked up using the EXIT DTE (not entry DTE),
# because the worst-case gamma exposure occurs near the end of the hold
# period — that's the binding constraint for position sizing.
#   4D strategy: enters DTE 3-4, exits DTE 2 → sized at K(DTE=2) = 0.70
#   2D strategy: enters DTE 2, expires DTE 0 → sized at K(DTE=1) = 1.00
#                (passes through DTE=1 where gamma is at peak)
K_TABLE_STRADDLE = [
    (5, 7, 0.40),   # 1/√6 ≈ 0.41
    (3, 4, 0.50),   # 1/√4 = 0.50
    (2, 2, 0.70),   # 1/√2 ≈ 0.71
    (1, 1, 1.00),   # 1/√1 = 1.00
]

# K values for single call/put (future extensibility)
K_TABLE_SINGLE = [
    (5, 7, 0.50),
    (3, 4, 0.60),
    (2, 2, 0.80),
    (1, 1, 1.00),
]

STATE_FILE_PATH = Path(WorkDirectory) / "v2_state.json"
ENTRY_LOG_PATH = Path(WorkDirectory) / "v2_entry_log.csv"
EXIT_LOG_PATH = Path(WorkDirectory) / "v2_exit_log.csv"


def lookupK(dte, kTable):
    """Return the k multiplier for a given trading DTE from the provided k table."""
    for minDte, maxDte, kValue in kTable:
        if minDte <= dte <= maxDte:
            return kValue
    raise ValueError(f"No k value found for DTE={dte}. Must be between 1 and 7.")


def computeTradingDte(today, expiryDate):
    """Count trading days between today and expiryDate (excluding today, including expiryDate).
    Skips weekends and market holidays."""
    tradingDays = 0
    current = today + timedelta(days=1)
    while current <= expiryDate:
        if current.weekday() < 5 and not CheckForDateHoliday(current):
            tradingDays += 1
        current += timedelta(days=1)
    return tradingDays


def isWithinTimeWindow(targetTime, beforeMinutes=2, afterMinutes=5):
    """Check if current time is within the entry/exit window.
    - If before target but within beforeMinutes: SLEEP until target, then return True
    - If after target but within afterMinutes: return True immediately
    - Otherwise: return False
    """
    now = datetime.now()
    parts = targetTime.split(":")
    targetDt = now.replace(hour=int(parts[0]), minute=int(parts[1]), second=0, microsecond=0)
    diff = (now - targetDt).total_seconds()  # positive = after target, negative = before

    if diff < 0 and abs(diff) <= beforeMinutes * 60:
        import time
        sleepSecs = abs(diff)
        print(f"[TIME] Waiting {sleepSecs:.0f}s until {targetTime}...")
        time.sleep(sleepSecs)
        return True
    elif diff >= 0 and diff <= afterMinutes * 60:
        return True
    else:
        return False


# ---------------------------------------------------------------------------
# SECTION 2: Strategy Configuration
# ---------------------------------------------------------------------------

STRATEGY_CONFIGS = {
    "N_STD_4D_30SL_I": {
        "underlying": "NIFTY",
        "phaseType": "early",
        "targetDteBucket": (3, 4),
        "exitDte": 2,              # exit when DTE drops to 2 (Fri for Tue expiry)
        "maxLots": 5,
        "stopLossTriggerPercent": 30,
        "stopLossOrderPlacePercent": 45,
        "kTable": K_TABLE_STRADDLE,
        "strategyType": "straddle",
        "entryTime": "09:30",
        "exitTime": "12:30",
        "orderTag": "V2-N-STD-4D-30SL",
    },
    "N_STD_2D_55SL_I": {
        "underlying": "NIFTY",
        "phaseType": "late",
        "targetDteBucket": (2, 2),
        "exitDte": 0,              # exit on expiry day (Tue)
        "letExpire": True,         # don't place exit orders, let options expire worthless
        "maxLots": 3,
        "stopLossTriggerPercent": 55,
        "stopLossOrderPlacePercent": 80,
        "kTable": K_TABLE_STRADDLE,
        "strategyType": "straddle",
        "entryTime": "12:30",
        "exitTime": "15:29",
        "orderTag": "V2-N-STD-2D-55SL",
    },
    "SX_STD_4D_20SL_I": {
        "underlying": "SENSEX",
        "phaseType": "early",
        "targetDteBucket": (3, 4),
        "exitDte": 2,              # exit when DTE drops to 2 (Tue for Thu expiry)
        "maxLots": 4,
        "stopLossTriggerPercent": 20,
        "stopLossOrderPlacePercent": 35,
        "kTable": K_TABLE_STRADDLE,
        "strategyType": "straddle",
        "entryTime": "09:30",
        "exitTime": "12:30",
        "orderTag": "V2-SX-STD-4D-20SL",
    },
    "SX_STD_2D_100SL_I": {
        "underlying": "SENSEX",
        "phaseType": "late",
        "targetDteBucket": (2, 2),
        "exitDte": 0,              # exit on expiry day (Thu)
        "letExpire": True,         # don't place exit orders, let options expire worthless
        "maxLots": 6,
        "stopLossTriggerPercent": 100,
        "stopLossOrderPlacePercent": 140,
        "kTable": K_TABLE_STRADDLE,
        "strategyType": "straddle",
        "entryTime": "12:30",
        "exitTime": "15:29",
        "orderTag": "V2-SX-STD-2D-100SL",
    },
}

# Daily vol budgets per underlying (INR) - configurable
DAILY_VOL_BUDGETS = {
    "NIFTY": 63984,
    "SENSEX": 63984,
}

# Portfolio-level cap across all underlyings (INR)
# Set to sum of individual budgets by default. Lower this for tighter control.
PORTFOLIO_DAILY_VOL_CAP = 127968


# ---------------------------------------------------------------------------
# SECTION 3: State Machine
# ---------------------------------------------------------------------------
#
# States per underlying:
#   noPosition      - nothing open
#   earlyOpen       - early phase position is live
#   lateOpen        - late phase position is live
#   completedCycle  - both phases done for this expiry cycle
#   repairRequired  - mismatch detected (partial fill, recon, GTT fail, exit not flat)
#
# Transitions:
#   noPosition      -> earlyOpen       (early entry)
#   earlyOpen       -> noPosition      (early exit: SL or time-exit at 12:30)
#   noPosition      -> lateOpen        (late entry at 12:30)
#   lateOpen        -> completedCycle  (late exit: SL or time-exit at 15:29)
#   completedCycle  -> noPosition      (auto-reset when new expiry cycle begins)
#   *any*           -> repairRequired  (partial fill, recon mismatch, GTT fail, exit not flat)
#   repairRequired  -> noPosition      (manual --exit only)

DEFAULT_UNDERLYING_STATE = {
    "currentState": "noPosition",
    "activeStrategy": None,
    "activeLots": 0,
    "activeContracts": [],
    "activeQuantity": 0,
    "entryTimestamp": None,
    "expiryDate": None,
    "lastCycleExpiry": None,
    "positionIntegrity": "healthy",   # "healthy" or "partial"
    "gttProtected": True,             # False if Set_Gtt() failed on any leg
    "activeGttIds": [],               # GTT trigger IDs for cancellation on exit
    "lastEntryKey": None,             # idempotency: "{strategy}_{expiry}_{phase}_{today}"
    "lastExitKey": None,              # idempotency: "{underlying}_{expiry}_{phase}_{today}"
}


def loadState():
    """Load state from disk. Returns dict keyed by underlying."""
    if STATE_FILE_PATH.exists():
        with open(STATE_FILE_PATH, "r") as f:
            return json.load(f)
    return {
        "NIFTY": dict(DEFAULT_UNDERLYING_STATE),
        "SENSEX": dict(DEFAULT_UNDERLYING_STATE),
    }


def saveState(state):
    """Persist state to disk as JSON."""
    with open(STATE_FILE_PATH, "w") as f:
        json.dump(state, f, indent=2, default=str)


def canOpenPosition(state, underlying, phaseType):
    """Check if a position can be opened for the given underlying and phase.
    Returns (bool, reason)."""
    currentState = state[underlying]["currentState"]

    # Block all entries when in repairRequired state
    if currentState == "repairRequired":
        return False, f"repairRequired - manual repair needed (use --exit={underlying})"

    # Block all new entries if a partial fill is detected anywhere for this underlying
    integrity = state[underlying].get("positionIntegrity", "healthy")
    if integrity == "partial":
        return False, f"positionIntegrity=partial - manual repair required (use --exit={underlying} first)"

    if phaseType == "early":
        if currentState in ("noPosition", "completedCycle"):
            return True, f"{currentState} - early can open"
        return False, f"state is {currentState}, early cannot open"

    if phaseType == "late":
        if currentState in ("noPosition", "completedCycle"):
            return True, f"{currentState} - late can open"
        if currentState == "earlyOpen":
            return False, "earlyOpen - late blocked until early exits"
        if currentState == "lateOpen":
            return False, "lateOpen - already in late position"

    return False, f"unknown phase {phaseType}"


def transitionToOpen(state, underlying, phaseType, strategyName, lots, contracts, quantity, expiryDate,
                     positionIntegrity="healthy", gttProtected=True, gttIds=None):
    """Transition state to open position."""
    newState = "earlyOpen" if phaseType == "early" else "lateOpen"
    state[underlying]["currentState"] = newState
    state[underlying]["activeStrategy"] = strategyName
    state[underlying]["activeLots"] = lots
    state[underlying]["activeContracts"] = contracts
    state[underlying]["activeQuantity"] = quantity
    state[underlying]["entryTimestamp"] = datetime.now().isoformat()
    state[underlying]["expiryDate"] = expiryDate.isoformat() if isinstance(expiryDate, date) else expiryDate
    state[underlying]["positionIntegrity"] = positionIntegrity
    state[underlying]["gttProtected"] = gttProtected
    state[underlying]["activeGttIds"] = gttIds or []
    saveState(state)


def transitionToExit(state, underlying, reason):
    """Transition state from open to closed. Returns previous state info for logging."""
    previousState = dict(state[underlying])
    oldState = state[underlying]["currentState"]

    if oldState == "earlyOpen":
        state[underlying]["currentState"] = "noPosition"
    elif oldState == "lateOpen":
        state[underlying]["currentState"] = "completedCycle"
        state[underlying]["lastCycleExpiry"] = state[underlying]["expiryDate"]
    elif oldState == "repairRequired":
        state[underlying]["currentState"] = "noPosition"

    state[underlying]["activeStrategy"] = None
    state[underlying]["activeLots"] = 0
    state[underlying]["activeContracts"] = []
    state[underlying]["activeQuantity"] = 0
    state[underlying]["entryTimestamp"] = None
    state[underlying]["expiryDate"] = None
    state[underlying]["positionIntegrity"] = "healthy"
    state[underlying]["gttProtected"] = True
    state[underlying]["activeGttIds"] = []
    saveState(state)
    return previousState


def resetCompletedCycleIfNewExpiry(state, underlying, nextExpiryDate):
    """Reset completedCycle state when a new expiry cycle begins.
    Also handles letExpire safety net: if state is lateOpen but the position's
    expiry has passed (e.g. 15:29 cron missed on expiry day), auto-transition."""
    currentState = state[underlying]["currentState"]
    nextExpiryStr = nextExpiryDate.isoformat() if isinstance(nextExpiryDate, date) else nextExpiryDate

    if currentState == "completedCycle":
        lastExpiry = state[underlying].get("lastCycleExpiry")
        if lastExpiry != nextExpiryStr:
            state[underlying]["currentState"] = "noPosition"
            state[underlying]["lastCycleExpiry"] = None
            saveState(state)
        return

    # Safety net for letExpire: if lateOpen and position's expiry has passed, auto-complete
    if currentState == "lateOpen":
        positionExpiryStr = state[underlying].get("expiryDate")
        if positionExpiryStr and positionExpiryStr != nextExpiryStr:
            activeStrat = state[underlying].get("activeStrategy")
            stratConfig = STRATEGY_CONFIGS.get(activeStrat, {})
            if stratConfig.get("letExpire", False):
                print(f"[V2 RUNNER] {underlying}: letExpire safety net - expiry {positionExpiryStr} "
                      f"has passed, auto-transitioning to completedCycle")
                currentLots = state[underlying].get("activeLots", 0)
                logExit(activeStrat, currentState, "letExpire_autoReset",
                        currentLots, 0, exitStatus="expired_late", failedLegs=[])
                transitionToExit(state, underlying, "letExpire_autoReset")
                # Immediately reset to noPosition since this is already a new expiry cycle
                lastExpiry = state[underlying].get("lastCycleExpiry")
                if lastExpiry != nextExpiryStr:
                    state[underlying]["currentState"] = "noPosition"
                    state[underlying]["lastCycleExpiry"] = None
                    saveState(state)
                    print(f"[V2 RUNNER] {underlying}: new expiry cycle ({nextExpiryStr}), "
                          f"auto-reset to noPosition")


# ---------------------------------------------------------------------------
# SECTION 3.5: Position Reconciliation and Flat Verification
# ---------------------------------------------------------------------------

def reconcilePositions(kite, state, dryRun=False):
    """Compare broker positions (kite.positions()) against local state.
    - 'Broker has position, state says no' → repairRequired (definite problem)
    - 'State has position, broker appears flat' → WARN ONLY (could be API glitch)
    In dryRun mode, prints mismatches but does not modify state."""
    try:
        positions = kite.positions()
        netPositions = positions.get("net", [])
    except Exception as e:
        print(f"[RECONCILE] WARN: kite.positions() failed: {e}. Skipping reconciliation.")
        return

    # Debug: log raw position count and exchanges for diagnostics
    exchangeSummary = {}
    for p in netPositions:
        ex = p.get("exchange", "?")
        qty = p.get("quantity", 0)
        if qty != 0:
            exchangeSummary[ex] = exchangeSummary.get(ex, 0) + 1
    print(f"[RECONCILE] Broker net positions: {len(netPositions)} total, "
          f"non-zero by exchange: {exchangeSummary if exchangeSummary else 'none'}")

    # Build lookup of all non-zero broker positions by tradingsymbol
    brokerPositionsBySymbol = {}
    for p in netPositions:
        sym = p.get("tradingsymbol", "")
        qty = p.get("quantity", 0)
        if sym and qty != 0:
            brokerPositionsBySymbol[sym] = qty

    for underlying in ["NIFTY", "SENSEX"]:
        localState = state[underlying]["currentState"]
        hasLocalPosition = localState in ("earlyOpen", "lateOpen")
        localContracts = state[underlying].get("activeContracts", [])

        if hasLocalPosition and localContracts:
            # Check if each specific contract from state exists on broker with non-zero qty
            missing = [c for c in localContracts if c not in brokerPositionsBySymbol]
            found = [c for c in localContracts if c in brokerPositionsBySymbol]

            if missing and not found:
                # All contracts missing — warn only (could be API glitch)
                print(f"[RECONCILE] {underlying}: ⚠ WARNING - state={localState}, "
                      f"but none of {localContracts} found on broker. "
                      f"Could be API issue or SL triggered. Verify manually.")
            elif missing:
                # Some contracts missing — partial mismatch, warn
                print(f"[RECONCILE] {underlying}: ⚠ WARNING - state={localState}, "
                      f"missing on broker: {missing}, found: {found}. Verify manually.")
            else:
                # All contracts found on broker
                brokerQtys = {c: brokerPositionsBySymbol[c] for c in localContracts}
                print(f"[RECONCILE] {underlying}: OK (state={localState}, "
                      f"contracts verified: {brokerQtys})")

        elif hasLocalPosition and not localContracts:
            print(f"[RECONCILE] {underlying}: ⚠ WARNING - state={localState} "
                  f"but no activeContracts in state. Verify manually.")

        elif not hasLocalPosition:
            # Check if broker has any positions for this underlying we don't know about
            # Filter by exact underlying prefix to avoid false matches (e.g. BANKNIFTY ≠ NIFTY)
            exchange = EXCHANGE_MAP[underlying]
            unexpectedContracts = [p for p in netPositions
                                   if p.get("exchange") == exchange
                                   and p.get("quantity", 0) != 0
                                   and p.get("tradingsymbol", "").startswith(underlying)
                                   and (p.get("tradingsymbol", "").endswith("CE")
                                        or p.get("tradingsymbol", "").endswith("PE"))]
            if unexpectedContracts:
                contractDetail = {p["tradingsymbol"]: p["quantity"] for p in unexpectedContracts}
                print(f"[RECONCILE] {underlying}: ⚠ WARNING - state={localState} but broker has "
                      f"{len(unexpectedContracts)} open {underlying} options: {contractDetail}. "
                      f"May belong to another system. V2 will proceed normally.")
            else:
                print(f"[RECONCILE] {underlying}: OK (state={localState}, no broker positions)")


def verifyFlatPosition(kite, underlying, contracts):
    """After exit orders confirmed, verify broker positions are actually flat.
    Returns True if all specified contracts have zero quantity."""
    try:
        positions = kite.positions()
        netPositions = positions.get("net", [])
    except Exception as e:
        print(f"[FLAT CHECK] {underlying}: kite.positions() failed: {e}")
        return False

    for contract in contracts:
        matching = [p for p in netPositions if p.get("tradingsymbol") == contract]
        if matching and matching[0].get("quantity", 0) != 0:
            print(f"[FLAT CHECK] {underlying}: {contract} still has qty={matching[0]['quantity']}")
            return False

    print(f"[FLAT CHECK] {underlying}: confirmed flat for {contracts}")
    return True


# ---------------------------------------------------------------------------
# SECTION 4: Risk Sizing Engine
# ---------------------------------------------------------------------------

def computePositionSize(callPremium, putPremium, lotSize, kValue, dailyVolBudget, maxLots):
    """Compute the number of lots to trade based on daily-vol sizing formula.

    dailyVolPerLot = k * (callPremium + putPremium) * lotSize
    allowedLots = int(dailyVolBudget / dailyVolPerLot + 0.5)   # round half-up (2.5 → 3)
    finalLots = min(allowedLots, maxLots)
    """
    combinedPremium = callPremium + putPremium

    if combinedPremium <= 0:
        return {
            "finalLots": 0, "allowedLots": 0, "dailyVolPerLot": 0.0,
            "combinedPremium": 0.0, "skipped": True,
            "skipReason": "combinedPremium is zero or negative",
        }

    dailyVolPerLot = kValue * combinedPremium * lotSize

    if dailyVolPerLot <= 0:
        return {
            "finalLots": 0, "allowedLots": 0, "dailyVolPerLot": 0.0,
            "combinedPremium": combinedPremium, "skipped": True,
            "skipReason": "dailyVolPerLot computed as zero",
        }

    allowedLots = int(dailyVolBudget / dailyVolPerLot + 0.5)  # round half-up (2.5 → 3)
    finalLots = min(allowedLots, maxLots)

    if finalLots <= 0:
        return {
            "finalLots": 0, "allowedLots": allowedLots,
            "dailyVolPerLot": dailyVolPerLot, "combinedPremium": combinedPremium,
            "skipped": True,
            "skipReason": f"allowedLots={allowedLots}, finalLots=0 after budget constraint",
        }

    return {
        "finalLots": finalLots, "allowedLots": allowedLots,
        "dailyVolPerLot": dailyVolPerLot, "combinedPremium": combinedPremium,
        "skipped": False, "skipReason": None,
    }


def computePortfolioDailyVolUsed(state):
    """Estimate current portfolio-level daily vol exposure from open positions.
    Uses stored sizing data to approximate. Returns total INR daily vol committed."""
    totalUsed = 0.0
    for underlying in ["NIFTY", "SENSEX"]:
        uState = state[underlying]
        if uState["currentState"] in ("earlyOpen", "lateOpen"):
            # Approximate: activeLots * per-underlying budget / maxLots of the active strategy
            activeLots = uState.get("activeLots", 0)
            activeStrat = uState.get("activeStrategy")
            if activeStrat and activeStrat in STRATEGY_CONFIGS:
                maxLots = STRATEGY_CONFIGS[activeStrat]["maxLots"]
                budget = DAILY_VOL_BUDGETS[underlying]
                # Pro-rata: if we used N lots out of max M with budget B,
                # our committed vol is roughly (N/M) * B
                totalUsed += (activeLots / maxLots) * budget if maxLots > 0 else 0
    return totalUsed


# ---------------------------------------------------------------------------
# SECTION 5: Premium and Expiry Fetching
# ---------------------------------------------------------------------------

def getNextExpiryAndDte(kite, underlying):
    """Get the next expiry date and current trading DTE for an underlying."""
    exchange = EXCHANGE_MAP[underlying]
    optSegment = GetOptSegmentForExchange(exchange)
    instrumentsOpt = GetInstrumentsCached(kite, exchange)

    expiryDate = SelectExpiryDateFromInstruments(
        instrumentsOpt=instrumentsOpt,
        indexName=underlying,
        optionType="WeeklyOption",
        expiryWeekdayInt=int(EXPIRY_DAY_MAP[underlying]),
        optSegment=optSegment,
    )

    today = date.today()
    tradingDte = computeTradingDte(today, expiryDate)
    return expiryDate, tradingDte


def fetchOptionPremiums(kite, ceContractName, peContractName, exchange):
    """Fetch live premiums for the CE and PE contracts via kite.ltp()."""
    ceKey = f"{exchange}:{ceContractName}"
    peKey = f"{exchange}:{peContractName}"

    ltpData = kite.ltp([ceKey, peKey])

    callPremium = float(ltpData[ceKey]["last_price"])
    putPremium = float(ltpData[peKey]["last_price"])

    return callPremium, putPremium


# ---------------------------------------------------------------------------
# SECTION 6: OrderDetails Builder
# ---------------------------------------------------------------------------

def buildOrderDetails(strategyName, config, quantity, tradeType="SELL"):
    """Build an OrderDetails dict compatible with FetchContractName, order(), Set_Gtt()."""
    underlying = config["underlying"]

    return {
        "Tradetype": tradeType,
        "Exchange": EXCHANGE_MAP[underlying],
        "Tradingsymbol": underlying,
        "Quantity": str(quantity),
        "Variety": "REGULAR",
        "Ordertype": "MARKET",
        "Product": "NRML",
        "Validity": "DAY",
        "Price": 0.0,
        "Symboltoken": "",
        "Squareoff": "",
        "Stoploss": "",
        "Broker": "",
        "Netposition": "",
        "OptionExpiryDay": EXPIRY_DAY_MAP[underlying],
        "OptionContractStrikeFromATMPercent": "0",
        "Trigger": "1",
        "StopLossTriggerPercent": str(config["stopLossTriggerPercent"]),
        "StopLossOrderPlacePercent": str(config["stopLossOrderPlacePercent"]),
        "CallStrikeRequired": "True" if config["strategyType"] == "straddle" else "False",
        "PutStrikeRequired": "True" if config["strategyType"] == "straddle" else "False",
        "Hedge": "False",
        "OrderTag": config["orderTag"],
        "TradeFailExitRequired": "False",
        "User": "OFS653",
    }


# ---------------------------------------------------------------------------
# SECTION 7: Execution Engine
# ---------------------------------------------------------------------------

def verifyOrderFill(kiteClient, orderId, maxWaitSeconds=6):
    """Check if an order has been filled by polling order_history().
    Returns (isFilled, finalStatus)."""
    if not orderId or orderId == 0:
        return False, "no_order_id"

    pollIntervals = [2, 2, 2]  # poll at 2s, 4s, 6s
    elapsed = 0
    for waitTime in pollIntervals:
        if elapsed >= maxWaitSeconds:
            break
        time.sleep(waitTime)
        elapsed += waitTime
        try:
            history = kiteClient.order_history(order_id=orderId)
            if history:
                latestStatus = history[-1].get("status", "")
                if latestStatus == "COMPLETE":
                    return True, "COMPLETE"
                if latestStatus in ("REJECTED", "CANCELLED"):
                    return False, latestStatus
                # Still pending, continue polling
        except Exception as e:
            logging.warning(f"order_history check failed for orderId={orderId}: {e}")

    # Final check
    try:
        history = kiteClient.order_history(order_id=orderId)
        if history:
            latestStatus = history[-1].get("status", "")
            return latestStatus == "COMPLETE", latestStatus
    except Exception as e:
        logging.warning(f"Final order_history check failed for orderId={orderId}: {e}")

    return False, "UNKNOWN"


def executeTimeExit(underlying, state, kite, forceIdempotency=False):
    """Close remaining legs of the active position via BUY MARKET orders.
    Verifies fills via order_history() and flat position before transitioning state.
    Returns dict with 'success' bool and 'allOrdersFilled' bool."""
    underlyingState = state[underlying]
    strategyName = underlyingState["activeStrategy"]
    contracts = underlyingState["activeContracts"]
    quantity = underlyingState["activeQuantity"]
    currentLots = underlyingState["activeLots"]
    currentStateName = underlyingState["currentState"]

    # Idempotency check: skip if this exit was already executed today
    if not forceIdempotency:
        phaseType = "early" if currentStateName == "earlyOpen" else "late"
        expiryDate = underlyingState.get("expiryDate", "")
        exitKey = f"{underlying}_{expiryDate}_{phaseType}_{date.today().isoformat()}"
        lastExitKey = underlyingState.get("lastExitKey")
        if lastExitKey == exitKey:
            print(f"[EXIT] {underlying}: IDEMPOTENCY SKIP - already exited today (key={exitKey})")
            return {"success": True, "allOrdersFilled": True, "reason": "idempotency_skip"}

    if not contracts:
        print(f"[EXIT] {underlying}: no active contracts to exit")
        return {"success": False, "allOrdersFilled": False, "reason": "no contracts"}

    exchange = EXCHANGE_MAP[underlying]
    gttsAlreadyCancelled = False

    # Pre-flight: verify position exists on broker before placing exit orders
    # Without this, if SL GTT already triggered, we'd create an unwanted LONG position
    try:
        netPositions = kite.positions()["net"]
        brokerQtyBySymbol = {p["tradingsymbol"]: p["quantity"] for p in netPositions}
        missingOnBroker = [c for c in contracts if brokerQtyBySymbol.get(c, 0) >= 0]
        survivingOnBroker = [c for c in contracts if brokerQtyBySymbol.get(c, 0) < 0]

        if missingOnBroker:
            # Cancel ALL GTTs first (covers both full-SL and partial-SL scenarios)
            for gttId in underlyingState.get("activeGttIds", []):
                try:
                    kite.delete_gtt(gttId)
                    print(f"[EXIT] {underlying}: cancelled orphaned GTT {gttId}")
                except Exception as e:
                    print(f"[EXIT] {underlying}: GTT cancel failed for {gttId}: {e}")
            gttsAlreadyCancelled = True

            if not survivingOnBroker:
                # Scenario A: ALL legs already closed by SL — nothing to exit
                print(f"[EXIT] {underlying}: ALL legs already closed by SL: {missingOnBroker}")
                logExit(strategyName, currentStateName, "broker_flat_autoReset",
                        currentLots, 0, exitStatus="broker_already_flat", failedLegs=[])
                transitionToExit(state, underlying, "broker_flat_autoReset")
                return {"success": True, "allOrdersFilled": True, "reason": "broker_already_flat"}
            else:
                # Scenario B: PARTIAL SL — some legs survive, close them
                print(f"[EXIT] {underlying}: PARTIAL SL detected.")
                print(f"[EXIT] {underlying}:   already closed by SL: {missingOnBroker}")
                print(f"[EXIT] {underlying}:   still short (closing now): {survivingOnBroker}")
                contracts = survivingOnBroker
                # Fall through to normal exit flow below
    except Exception as e:
        print(f"[EXIT] {underlying}: *** ABORTED *** could not verify broker positions: {e}")
        return {"success": False, "allOrdersFilled": False, "reason": f"preflight_failed: {e}"}

    # Cancel active GTTs before exiting to prevent orphaned triggers
    if not gttsAlreadyCancelled:
        for gttId in underlyingState.get("activeGttIds", []):
            try:
                kite.delete_gtt(gttId)
                print(f"[EXIT] {underlying}: cancelled GTT {gttId}")
            except Exception as e:
                print(f"[EXIT] {underlying}: GTT cancel failed for {gttId}: {e} (continuing with exit)")

    print(f"[EXIT] {underlying}: closing {len(contracts)} legs, qty={quantity}")

    # Phase 1: Submit all exit orders
    submittedOrders = []  # (contract, orderId, accepted)

    for contract in contracts:
        exitOrderDetails = {
            "Tradetype": "BUY",
            "Exchange": exchange,
            "Tradingsymbol": contract,
            "Quantity": str(quantity),
            "Variety": "REGULAR",
            "Ordertype": "MARKET",
            "Product": "NRML",
            "Validity": "DAY",
            "Price": 0.0,
            "Symboltoken": "",
            "Squareoff": "",
            "Stoploss": "",
            "Broker": "",
            "Netposition": "",
            "OrderTag": f"V2-EXIT-{underlying}",
            "TradeFailExitRequired": "False",
            "User": "OFS653",
        }
        try:
            orderId = order(exitOrderDetails)
            accepted = orderId != 0
            submittedOrders.append((contract, orderId, accepted))
            print(f"[EXIT] {underlying}: BUY {contract} orderId={orderId} accepted={accepted}")
        except Exception as e:
            submittedOrders.append((contract, None, False))
            print(f"[EXIT] {underlying}: FAILED to submit {contract}: {e}")
            logging.warning(f"Exit order submission failed for {contract}: {e}")

    # Check if all orders were at least accepted
    allAccepted = all(ok for _, _, ok in submittedOrders)
    rejectedLegs = [c for c, _, ok in submittedOrders if not ok]

    if not allAccepted:
        print(f"[EXIT] {underlying}: *** NOT ALL EXIT ORDERS ACCEPTED *** rejected: {rejectedLegs}")
        print(f"[EXIT] {underlying}: state NOT transitioned - manual cleanup required")
        logExit(strategyName, currentStateName, "timeExit_REJECTED", currentLots, currentLots,
                exitStatus="rejected", failedLegs=rejectedLegs)
        return {"success": False, "allOrdersFilled": False, "reason": f"rejected legs: {rejectedLegs}"}

    # Phase 2: Verify all accepted orders actually filled
    # Uses the runner's kite client (Eshita account - same account orders are placed on)
    print(f"[EXIT] {underlying}: all orders accepted, verifying fills...")
    unfilledLegs = []
    for contract, orderId, _ in submittedOrders:
        isFilled, fillStatus = verifyOrderFill(kite, orderId)
        print(f"[EXIT] {underlying}: {contract} orderId={orderId} fillStatus={fillStatus}")
        if not isFilled:
            unfilledLegs.append(contract)

    if unfilledLegs:
        print(f"[EXIT] {underlying}: *** EXIT ORDERS NOT FILLED *** unfilled: {unfilledLegs}")
        print(f"[EXIT] {underlying}: state NOT transitioned - position may still be open")
        logExit(strategyName, currentStateName, "timeExit_UNFILLED", currentLots, currentLots,
                exitStatus="unfilled", failedLegs=unfilledLegs)
        return {"success": False, "allOrdersFilled": False, "reason": f"unfilled legs: {unfilledLegs}"}

    # Phase 3: Verify broker positions are actually flat
    print(f"[EXIT] {underlying}: all orders filled, verifying flat position...")
    time.sleep(1)  # allow broker to settle
    isFlat = verifyFlatPosition(kite, underlying, contracts)

    if not isFlat:
        print(f"[EXIT] {underlying}: *** NOT FLAT after confirmed fills *** → repairRequired")
        state[underlying]["currentState"] = "repairRequired"
        state[underlying]["positionIntegrity"] = "partial"
        saveState(state)
        logExit(strategyName, currentStateName, "timeExit_NOT_FLAT", currentLots, currentLots,
                exitStatus="notFlat", failedLegs=[])
        return {"success": False, "allOrdersFilled": True, "reason": "not_flat_after_exit"}

    # All exit orders confirmed filled AND position verified flat — safe to transition state
    print(f"[EXIT] {underlying}: all exit orders CONFIRMED FILLED + FLAT VERIFIED")
    # Store idempotency key before transition (transition clears fields)
    phaseType = "early" if currentStateName == "earlyOpen" else "late"
    expiryDate = underlyingState.get("expiryDate", "")
    exitKey = f"{underlying}_{expiryDate}_{phaseType}_{date.today().isoformat()}"
    previousState = transitionToExit(state, underlying, "timeExit")
    state[underlying]["lastExitKey"] = exitKey
    saveState(state)
    logExit(strategyName, currentStateName, "timeExit", currentLots, 0,
            exitStatus="confirmedFlatVerified", failedLegs=[])
    return {"success": True, "allOrdersFilled": True, "reason": "confirmed_flat"}


def executeEntry(strategyName, config, state, kite, dte, expiryDate, dryRun=False,
                 forceIdempotency=False):
    """Execute a full entry: size -> fetch contract -> place order -> set GTT -> update state.
    If dryRun=True, does everything except order(), Set_Gtt(), and state transitions."""
    underlying = config["underlying"]
    exchange = EXCHANGE_MAP[underlying]
    lotSize = LOT_SIZES[underlying]
    dailyVolBudget = DAILY_VOL_BUDGETS[underlying]
    tag = "[DRY RUN] " if dryRun else ""

    # Idempotency check: skip if this entry was already executed today
    if not dryRun and not forceIdempotency:
        entryKey = f"{strategyName}_{expiryDate}_{config['phaseType']}_{date.today().isoformat()}"
        lastEntryKey = state[underlying].get("lastEntryKey")
        if lastEntryKey == entryKey:
            print(f"{tag}[ENTRY] {strategyName}: IDEMPOTENCY SKIP - already entered today (key={entryKey})")
            return {"success": False, "reason": "idempotency_skip"}

    # Step 1: Lookup K value using exit DTE (worst-case gamma during hold)
    # 4D strategy enters DTE=4, exits DTE=2 → size at K(DTE=2) = 0.70
    # 2D strategy enters DTE=2, expires DTE=0 → passes through DTE=1 (peak gamma) → K(DTE=1) = 1.00
    exitDteConfig = config.get("exitDte", 0)
    sizingDte = exitDteConfig if exitDteConfig >= 1 else 1
    try:
        kValue = lookupK(sizingDte, config["kTable"])
    except ValueError as e:
        print(f"{tag}[ENTRY] {strategyName}: {e}")
        return {"success": False, "reason": str(e)}

    # Step 2: Build OrderDetails (1 lot placeholder for contract resolution)
    orderDetails = buildOrderDetails(strategyName, config, quantity=lotSize)

    # Step 3: Fetch contract names via existing FetchContractName
    try:
        contractResult = FetchContractName(orderDetails)
    except Exception as e:
        print(f"{tag}[ENTRY] {strategyName}: FetchContractName failed: {e}")
        return {"success": False, "reason": f"FetchContractName failed: {e}"}

    if isinstance(contractResult, tuple) and len(contractResult) == 2:
        ceSymbol, peSymbol = contractResult
    else:
        reason = f"FetchContractName returned unexpected: {contractResult}"
        print(f"{tag}[ENTRY] {strategyName}: {reason}")
        return {"success": False, "reason": reason}

    # Step 4: Fetch premiums for sizing
    try:
        callPremium, putPremium = fetchOptionPremiums(kite, ceSymbol, peSymbol, exchange)
    except Exception as e:
        print(f"{tag}[ENTRY] {strategyName}: premium fetch failed: {e}")
        return {"success": False, "reason": f"premium fetch failed: {e}"}

    # Step 5: Compute position size
    sizeResult = computePositionSize(
        callPremium=callPremium,
        putPremium=putPremium,
        lotSize=lotSize,
        kValue=kValue,
        dailyVolBudget=dailyVolBudget,
        maxLots=config["maxLots"],
    )

    if sizeResult["skipped"]:
        logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
                 sizeResult, expiryDate, ceSymbol, peSymbol, skipped=True,
                 skipReason=sizeResult["skipReason"])
        print(f"{tag}[ENTRY] {strategyName}: SKIPPED - {sizeResult['skipReason']}")
        return {"success": False, "reason": sizeResult["skipReason"]}

    finalLots = sizeResult["finalLots"]
    totalQuantity = finalLots * lotSize

    # Portfolio-level daily vol cap check
    proposedDailyVol = sizeResult["dailyVolPerLot"] * finalLots
    currentPortfolioVol = computePortfolioDailyVolUsed(state)
    if currentPortfolioVol + proposedDailyVol > PORTFOLIO_DAILY_VOL_CAP:
        reason = (f"portfolio cap exceeded: current={round(currentPortfolioVol, 2)} + "
                  f"proposed={round(proposedDailyVol, 2)} > cap={PORTFOLIO_DAILY_VOL_CAP}")
        logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
                 sizeResult, expiryDate, ceSymbol, peSymbol, skipped=True, skipReason=reason)
        print(f"{tag}[ENTRY] {strategyName}: SKIPPED - {reason}")
        return {"success": False, "reason": reason}

    # ── DRY RUN: print what would happen and return ──
    if dryRun:
        print(f"\n{tag}{'='*60}")
        print(f"{tag}[ENTRY] {strategyName}: WOULD EXECUTE:")
        print(f"{tag}  Underlying:      {underlying}")
        print(f"{tag}  Phase:           {config['phaseType']}")
        print(f"{tag}  Trading DTE:     {dte}")
        print(f"{tag}  Sizing DTE:      {sizingDte} (K sized at exit-DTE risk)")
        print(f"{tag}  K value:         {kValue}")
        print(f"{tag}  CE contract:     {ceSymbol}")
        print(f"{tag}  PE contract:     {peSymbol}")
        print(f"{tag}  CE premium:      {callPremium}")
        print(f"{tag}  PE premium:      {putPremium}")
        print(f"{tag}  Combined:        {sizeResult['combinedPremium']}")
        print(f"{tag}  DailyVol/lot:    {round(sizeResult['dailyVolPerLot'], 2)}")
        print(f"{tag}  Budget:          {dailyVolBudget}")
        print(f"{tag}  Allowed lots:    {sizeResult['allowedLots']}")
        print(f"{tag}  Max lots (cap):  {config['maxLots']}")
        print(f"{tag}  Final lots:      {finalLots}")
        print(f"{tag}  Total quantity:  {totalQuantity}")
        print(f"{tag}  SL trigger%:     {config['stopLossTriggerPercent']}")
        print(f"{tag}  SL order%:       {config['stopLossOrderPlacePercent']}")
        print(f"{tag}  Expiry:          {expiryDate}")
        print(f"{tag}  Order tag:       {config['orderTag']}")
        print(f"{tag}  User:            OFS653")
        print(f"{tag}{'='*60}")
        print(f"{tag}[ENTRY] {strategyName}: NO ORDERS PLACED (dry run)")
        return {
            "success": True, "strategyName": strategyName,
            "ceSymbol": ceSymbol, "peSymbol": peSymbol,
            "finalLots": finalLots, "totalQuantity": totalQuantity,
            "dryRun": True,
        }

    contracts = []

    # Step 6: Place CE order
    ceOrderDetails = buildOrderDetails(strategyName, config, quantity=totalQuantity)
    ceOrderDetails["Tradingsymbol"] = ceSymbol

    try:
        ceOrderId = order(ceOrderDetails)
        contracts.append(ceSymbol)
        print(f"[ENTRY] {strategyName}: CE {ceSymbol} placed, orderId={ceOrderId}")
    except Exception as e:
        logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
                 sizeResult, expiryDate, ceSymbol, peSymbol, skipped=True,
                 skipReason=f"CE order failed: {e}")
        print(f"[ENTRY] {strategyName}: CE order FAILED: {e}")
        return {"success": False, "reason": f"CE order failed: {e}"}

    # Step 7: Place PE order
    peOrderDetails = buildOrderDetails(strategyName, config, quantity=totalQuantity)
    peOrderDetails["Tradingsymbol"] = peSymbol

    try:
        peOrderId = order(peOrderDetails)
        contracts.append(peSymbol)
        print(f"[ENTRY] {strategyName}: PE {peSymbol} placed, orderId={peOrderId}")
    except Exception as e:
        # Partial fill: CE placed but PE failed — DANGEROUS naked short position
        logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
                 sizeResult, expiryDate, ceSymbol, peSymbol, skipped=True,
                 skipReason=f"PARTIAL FILL - PE order failed after CE placed (ceOrderId={ceOrderId}): {e}",
                 gttProtected=False, positionIntegrity="partial")
        print(f"[ENTRY] {strategyName}: *** PARTIAL FILL *** PE FAILED after CE placed.")
        print(f"[ENTRY] {strategyName}: NAKED SHORT {ceSymbol} - MANUAL INTERVENTION REQUIRED")
        print(f"[ENTRY] {strategyName}: Use --exit={underlying} to flatten, then retry")
        transitionToOpen(state, underlying, config["phaseType"], strategyName,
                         finalLots, [ceSymbol], totalQuantity, expiryDate,
                         positionIntegrity="partial", gttProtected=False)
        return {"success": False, "reason": f"Partial fill: CE ok, PE failed: {e}"}

    # Step 8: Set GTT exit orders for both legs
    gttOk = True
    gttIds = []
    try:
        ceGttId = Set_Gtt(ceOrderDetails)
        if ceGttId:
            gttIds.append(ceGttId)
    except Exception as e:
        gttOk = False
        logging.warning(f"GTT set failed for CE {ceSymbol}: {e}")
        print(f"[ENTRY] {strategyName}: GTT FAILED for CE - POSITION UNPROTECTED: {e}")

    try:
        peGttId = Set_Gtt(peOrderDetails)
        if peGttId:
            gttIds.append(peGttId)
    except Exception as e:
        gttOk = False
        logging.warning(f"GTT set failed for PE {peSymbol}: {e}")
        print(f"[ENTRY] {strategyName}: GTT FAILED for PE - POSITION UNPROTECTED: {e}")

    if gttIds:
        print(f"[ENTRY] {strategyName}: GTT IDs captured: {gttIds}")

    if not gttOk:
        # GTT failure = unprotected short straddle → dangerous, require manual repair
        print(f"[ENTRY] {strategyName}: *** GTT FAILED *** Position is UNPROTECTED → repairRequired")
        print(f"[ENTRY] {strategyName}: Manually set GTT via Kite web, or use --exit={underlying}")
        transitionToOpen(state, underlying, config["phaseType"], strategyName,
                         finalLots, contracts, totalQuantity, expiryDate,
                         positionIntegrity="partial", gttProtected=False, gttIds=gttIds)
        state[underlying]["currentState"] = "repairRequired"
        saveState(state)
        logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
                 sizeResult, expiryDate, ceSymbol, peSymbol, skipped=False,
                 skipReason="GTT_FAILED", gttProtected=False, positionIntegrity="partial")
        return {
            "success": False, "strategyName": strategyName,
            "ceSymbol": ceSymbol, "peSymbol": peSymbol,
            "finalLots": finalLots, "totalQuantity": totalQuantity,
            "gttProtected": False, "reason": "GTT failed - repairRequired",
        }

    # Step 9: Transition state (position is healthy since both legs filled AND GTT set)
    transitionToOpen(state, underlying, config["phaseType"], strategyName,
                     finalLots, contracts, totalQuantity, expiryDate,
                     positionIntegrity="healthy", gttProtected=True, gttIds=gttIds)

    # Step 10: Store idempotency key
    entryKey = f"{strategyName}_{expiryDate}_{config['phaseType']}_{date.today().isoformat()}"
    state[underlying]["lastEntryKey"] = entryKey
    saveState(state)

    # Step 11: Log entry
    logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
             sizeResult, expiryDate, ceSymbol, peSymbol, skipped=False, skipReason=None,
             gttProtected=gttOk, positionIntegrity="healthy")

    print(f"[ENTRY] {strategyName}: SUCCESS - {finalLots} lots, qty={totalQuantity}, "
          f"CE={ceSymbol}, PE={peSymbol}, gttProtected={gttOk}")

    return {
        "success": True, "strategyName": strategyName,
        "ceSymbol": ceSymbol, "peSymbol": peSymbol,
        "finalLots": finalLots, "totalQuantity": totalQuantity,
        "gttProtected": gttOk,
    }


# ---------------------------------------------------------------------------
# SECTION 8: Logging
# ---------------------------------------------------------------------------

ENTRY_LOG_FIELDS = [
    "timestamp", "strategyName", "underlying", "phaseType", "currentDte",
    "selectedK", "callPremium", "putPremium", "combinedPremium", "lotSize",
    "dailyVolPerLot", "dailyVolBudget", "allowedLots", "strategyMaxLots",
    "finalLots", "selectedExpiry", "ceContract", "peContract", "skipped", "skipReason",
    "gttProtected", "positionIntegrity",
]

EXIT_LOG_FIELDS = [
    "timestamp", "strategyName", "currentState", "reasonForExit",
    "currentLots", "targetLotsAfterExit", "exitStatus", "failedLegs",
]


def logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
             sizeResult, expiryDate, ceSymbol, peSymbol, skipped, skipReason,
             gttProtected=True, positionIntegrity="healthy"):
    """Write a structured entry log line to CSV and stdout."""
    underlying = config["underlying"]
    row = {
        "timestamp": datetime.now().isoformat(),
        "strategyName": strategyName,
        "underlying": underlying,
        "phaseType": config["phaseType"],
        "currentDte": dte,
        "selectedK": kValue,
        "callPremium": callPremium,
        "putPremium": putPremium,
        "combinedPremium": sizeResult["combinedPremium"],
        "lotSize": LOT_SIZES[underlying],
        "dailyVolPerLot": round(sizeResult["dailyVolPerLot"], 2),
        "dailyVolBudget": DAILY_VOL_BUDGETS[underlying],
        "allowedLots": sizeResult["allowedLots"],
        "strategyMaxLots": config["maxLots"],
        "finalLots": sizeResult["finalLots"],
        "selectedExpiry": str(expiryDate),
        "ceContract": ceSymbol,
        "peContract": peSymbol,
        "skipped": skipped,
        "skipReason": skipReason or "",
        "gttProtected": gttProtected,
        "positionIntegrity": positionIntegrity,
    }

    fileExists = ENTRY_LOG_PATH.exists()
    with open(ENTRY_LOG_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ENTRY_LOG_FIELDS)
        if not fileExists:
            writer.writeheader()
        writer.writerow(row)

    print(f"[ENTRY LOG] {json.dumps(row, default=str)}")


def logExit(strategyName, currentState, reasonForExit, currentLots, targetLotsAfterExit,
            exitStatus="unknown", failedLegs=None):
    """Write a structured exit log line to CSV and stdout."""
    row = {
        "timestamp": datetime.now().isoformat(),
        "strategyName": strategyName or "",
        "currentState": currentState,
        "reasonForExit": reasonForExit,
        "currentLots": currentLots,
        "targetLotsAfterExit": targetLotsAfterExit,
        "exitStatus": exitStatus,
        "failedLegs": json.dumps(failedLegs or []),
    }

    fileExists = EXIT_LOG_PATH.exists()
    with open(EXIT_LOG_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXIT_LOG_FIELDS)
        if not fileExists:
            writer.writeheader()
        writer.writerow(row)

    print(f"[EXIT LOG] {json.dumps(row, default=str)}")


# ---------------------------------------------------------------------------
# SECTION 9: Override and CLI
# ---------------------------------------------------------------------------

def handleOverride(overrideArg):
    """Parse override argument. Returns list of (strategyName, forceFlag) tuples."""
    if not overrideArg:
        return []

    results = []
    parts = overrideArg.split(",")

    for part in parts:
        part = part.strip()
        force = False
        if part.endswith(":force"):
            force = True
            part = part.replace(":force", "")

        if part == "ALL":
            for name in STRATEGY_CONFIGS:
                results.append((name, force))
        elif part in ("NIFTY", "SENSEX"):
            for name, cfg in STRATEGY_CONFIGS.items():
                if cfg["underlying"] == part:
                    results.append((name, force))
        elif part in STRATEGY_CONFIGS:
            results.append((part, force))
        else:
            print(f"[WARN] Unknown override target: {part}")

    return results


def handleManualExit(exitArg, state, kite):
    """Handle --exit=UNDERLYING manual exit command.
    Also handles repairRequired state — attempts exit of known contracts, resets to noPosition."""
    underlying = exitArg.strip().upper()
    if underlying not in ("NIFTY", "SENSEX"):
        print(f"[EXIT] Unknown underlying: {exitArg}. Use NIFTY or SENSEX.")
        return

    currentState = state[underlying]["currentState"]
    if currentState in ("noPosition", "completedCycle"):
        print(f"[EXIT] {underlying}: no open position (state={currentState})")
        return

    if currentState == "repairRequired":
        contracts = state[underlying].get("activeContracts", [])
        if contracts:
            print(f"[EXIT] {underlying}: repairRequired with contracts {contracts} - attempting exit")
            executeTimeExit(underlying, state, kite, forceIdempotency=True)
        else:
            print(f"[EXIT] {underlying}: repairRequired with no known contracts - resetting to noPosition")
            transitionToExit(state, underlying, "manual_repair_reset")
        return

    # Manual exit always bypasses idempotency
    executeTimeExit(underlying, state, kite, forceIdempotency=True)


# ---------------------------------------------------------------------------
# SECTION 10: Main Runner
# ---------------------------------------------------------------------------

def runV2(overrideArg=None, exitArg=None, stateOnly=False, dryRun=False):
    """Main runner - called by cron or manually.
    If dryRun=True, connects to broker, resolves contracts, computes sizing,
    but does NOT place orders, set GTTs, or modify state."""
    mode = "DRY RUN" if dryRun else "LIVE"
    print(f"\n[V2 RUNNER] Starting at {datetime.now().isoformat()} (mode={mode})")

    # Load state
    state = loadState()

    # Handle --state
    if stateOnly:
        print(json.dumps(state, indent=2, default=str))
        return

    # Get Kite client
    try:
        kite = GetKiteClient()
    except Exception as e:
        print(f"[V2 RUNNER] FATAL: Could not get Kite client: {e}")
        return

    # Handle --exit
    if exitArg:
        handleManualExit(exitArg, state, kite)
        return

    # Startup position reconciliation: compare broker positions vs local state
    print("\n[V2 RUNNER] Running position reconciliation...")
    reconcilePositions(kite, state, dryRun=dryRun)
    state = loadState()

    # Parse overrides
    overrides = handleOverride(overrideArg) if overrideArg else []
    overrideNames = {name for name, _ in overrides}
    forceFlags = {name: force for name, force in overrides}

    # Process each underlying
    for underlying in ["NIFTY", "SENSEX"]:
        print(f"\n[V2 RUNNER] Processing {underlying}")

        # Get next expiry and DTE
        try:
            expiryDate, dte = getNextExpiryAndDte(kite, underlying)
        except Exception as e:
            print(f"[V2 RUNNER] ERROR: Could not get expiry for {underlying}: {e}")
            continue

        print(f"[V2 RUNNER] {underlying}: nextExpiry={expiryDate}, tradingDTE={dte}")

        # Reset completedCycle if new expiry
        resetCompletedCycleIfNewExpiry(state, underlying, expiryDate)

        # ── Scheduled exit check ──
        # Exit only when BOTH conditions are met:
        #   1. Current time is within the exitTime window (±2 min)
        #   2. Current DTE (from position's own expiry) has dropped to exitDte or below
        # This makes these carry trades, not intraday: 4D enters Wed, holds overnight, exits Fri.
        currentStateName = state[underlying]["currentState"]
        if currentStateName in ("earlyOpen", "lateOpen", "repairRequired") and not dryRun:
            activeStratName = state[underlying].get("activeStrategy")
            activeConfig = STRATEGY_CONFIGS.get(activeStratName)
            if activeConfig and isWithinTimeWindow(activeConfig["exitTime"]):
                # Compute DTE using position's own stored expiry (not next market expiry)
                positionExpiryStr = state[underlying].get("expiryDate")
                if positionExpiryStr:
                    positionExpiry = date.fromisoformat(positionExpiryStr)
                    positionDte = computeTradingDte(date.today(), positionExpiry)
                    exitDte = activeConfig.get("exitDte", 0)
                    if positionDte <= exitDte:
                        if activeConfig.get("letExpire", False):
                            # Let options expire worthless — no exit orders, just transition state
                            currentLots = state[underlying].get("activeLots", 0)
                            print(f"[V2 RUNNER] {underlying}: LET EXPIRE - {activeStratName} "
                                  f"DTE={positionDte}, skipping exit orders (saving commission)")
                            logExit(activeStratName, currentStateName, "letExpire",
                                    currentLots, 0, exitStatus="expired", failedLegs=[])
                            transitionToExit(state, underlying, "letExpire")
                            state = loadState()
                        else:
                            print(f"[V2 RUNNER] {underlying}: SCHEDULED EXIT - {activeStratName} "
                                  f"exitTime={activeConfig['exitTime']}, DTE={positionDte} ≤ exitDte={exitDte}")
                            exitResult = executeTimeExit(underlying, state, kite)
                            if exitResult["success"]:
                                print(f"[V2 RUNNER] {underlying}: scheduled exit completed ({exitResult['reason']})")
                            else:
                                print(f"[V2 RUNNER] {underlying}: scheduled exit FAILED: {exitResult['reason']}")
                            state = loadState()
                    else:
                        print(f"[V2 RUNNER] {underlying}: exitTime matches but DTE={positionDte} > "
                              f"exitDte={exitDte}, holding position")

        # Collect strategies for this underlying, sorted: early first then late
        strategies = [
            (name, cfg) for name, cfg in STRATEGY_CONFIGS.items()
            if cfg["underlying"] == underlying
        ]
        strategies.sort(key=lambda x: 0 if x[1]["phaseType"] == "early" else 1)

        for stratName, cfg in strategies:
            minDte, maxDte = cfg["targetDteBucket"]
            isOverride = stratName in overrideNames
            isForce = forceFlags.get(stratName, False)

            # Check DTE eligibility
            dteEligible = minDte <= dte <= maxDte

            if not dteEligible and not isOverride:
                print(f"[V2 RUNNER] {stratName}: DTE={dte} outside ({minDte}-{maxDte}), skipping")
                continue

            # Check time eligibility (skip for overrides)
            if not isOverride:
                entryTimeMatch = isWithinTimeWindow(cfg["entryTime"])
                if not entryTimeMatch:
                    print(f"[V2 RUNNER] {stratName}: not in entry time window ({cfg['entryTime']}), skipping")
                    continue

            # For late phase: if early is still open, auto-exit early first
            currentState = state[underlying]["currentState"]
            if cfg["phaseType"] == "late" and currentState == "earlyOpen" and not dryRun:
                print(f"[V2 RUNNER] {underlying}: HANDOFF - auto-exiting early phase before late entry")
                exitResult = executeTimeExit(underlying, state, kite)
                if not exitResult["success"]:
                    print(f"[V2 RUNNER] {stratName}: early exit failed ({exitResult['reason']}), skipping late entry")
                    continue
                if not exitResult["allOrdersFilled"]:
                    print(f"[V2 RUNNER] {stratName}: early exit orders not all filled, NOT safe to enter late")
                    continue
                # Reload state after exit
                state = loadState()
                # Verify we are actually in noPosition before proceeding
                if state[underlying]["currentState"] != "noPosition":
                    print(f"[V2 RUNNER] {stratName}: state after exit is {state[underlying]['currentState']}, expected noPosition. Skipping late entry.")
                    continue

            # Check state machine
            canOpen, reason = canOpenPosition(state, underlying, cfg["phaseType"])

            if not canOpen:
                # Force can bypass time/DTE/state blocks but NEVER bypass safety states
                currentCheckState = state[underlying].get("currentState")
                integrity = state[underlying].get("positionIntegrity", "healthy")
                if currentCheckState == "repairRequired" or integrity == "partial":
                    print(f"[V2 RUNNER] {stratName}: *** BLOCKED *** state={currentCheckState}, "
                          f"integrity={integrity} (even with force). Use --exit={underlying} first.")
                    continue
                if not isForce:
                    print(f"[V2 RUNNER] {stratName}: state blocks entry: {reason}")
                    continue
                print(f"[V2 RUNNER] {stratName}: state blocks but FORCE override active, proceeding")

            # Execute entry
            dryTag = "[DRY RUN] " if dryRun else ""
            print(f"{dryTag}[V2 RUNNER] {stratName}: EXECUTING ENTRY (DTE={dte}, phase={cfg['phaseType']})")
            result = executeEntry(stratName, cfg, state, kite, dte, expiryDate, dryRun=dryRun)

            if result["success"]:
                print(f"[V2 RUNNER] {stratName}: SUCCESS")
            else:
                print(f"[V2 RUNNER] {stratName}: FAILED - {result['reason']}")

            # Reload state after entry (may have been updated)
            state = loadState()

    print(f"\n[V2 RUNNER] Completed at {datetime.now().isoformat()}")
    print(f"[V2 RUNNER] Final state:\n{json.dumps(state, indent=2, default=str)}")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    overrideArg = None
    exitArg = None
    stateOnly = False
    dryRun = False

    overrideParts = []
    for arg in sys.argv[1:]:
        if arg.startswith("--override="):
            overrideParts.append(arg.split("=", 1)[1])
        elif arg.startswith("--exit="):
            exitArg = arg.split("=", 1)[1]
        elif arg == "--state":
            stateOnly = True
        elif arg == "--dry-run":
            dryRun = True

    overrideArg = ",".join(overrideParts) if overrideParts else None
    runV2(overrideArg=overrideArg, exitArg=exitArg, stateOnly=stateOnly, dryRun=dryRun)
