"""
daily_pnl_report.py — End-of-day P&L email report.

Sends a daily email summarizing:
  - Realized P&L from closed trades
  - Unrealized P&L change on open positions (MTM)
  - All trades executed today
  - All open positions with LTP and cost basis
  - Effective capital used for position sizing

Usage:
  python daily_pnl_report.py               # send today's report
  python daily_pnl_report.py --dry-run     # print HTML to stdout
  python daily_pnl_report.py --date 2026-03-28  # report for a specific date

Cron: 45 23 * * 1-5  (23:45 IST, after MCX close)
"""

import json
import logging
import argparse
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict

import pandas as pd

import forecast_db as db
from Kite_Server_Order_Handler import EstablishConnectionKiteAPI
from Server_Order_Handler import EstablishConnectionAngelAPI
from rollover_monitor import _LoadEmailConfig, _SendEmail, _BuildEmailHtml
from Directories import workInputRoot, ZerodhaInstrumentDirectory, AngelInstrumentDirectory

Logger = logging.getLogger("daily_pnl_report")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

CONFIG_PATH = Path(__file__).parent / "instrument_config.json"
STATE_FILE_PATH = Path(workInputRoot) / "v2_state.json"

# ─── Formatting Helpers ────────────────────────────────────────────

GREEN = "#00c853"
RED = "#ff1744"
AMBER = "#ff9100"
MUTED = "#8892a0"


def _FmtINR(Amount, Decimals=0):
    """Format a number as INR string with sign. e.g. +1,23,456 or -5,678."""
    Sign = "+" if Amount >= 0 else "-"
    Abs = abs(Amount)
    if Decimals == 0:
        S = f"{int(round(Abs)):,}"
    else:
        S = f"{Abs:,.{Decimals}f}"
    return f"{Sign}{S}"


def _FmtPlain(Amount, Decimals=0):
    """Format a number without sign prefix. e.g. 9,999,999."""
    Abs = abs(Amount)
    if Decimals == 0:
        return f"{int(round(Abs)):,}"
    return f"{Abs:,.{Decimals}f}"


def _PnlColor(Amount):
    """Green for profit, red for loss."""
    if Amount > 0:
        return GREEN
    elif Amount < 0:
        return RED
    return MUTED


def _FmtTime(CreatedAt):
    """Extract HH:MM from a datetime string."""
    if not CreatedAt:
        return ""
    try:
        Dt = datetime.fromisoformat(CreatedAt.replace("Z", "+00:00"))
        # Convert UTC to IST
        Dt = Dt + timedelta(hours=5, minutes=30)
        return Dt.strftime("%H:%M")
    except Exception:
        return str(CreatedAt)[-8:-3] if len(str(CreatedAt)) > 8 else ""


# ─── LTP Fetching ─────────────────────────────────────────────────


def _ResolveKiteContract(InstrumentName, Exchange, InstrumentType="FUT"):
    """Resolve GOLDM → GOLDM25APRFUT from ZerodhaInstruments.csv."""
    try:
        Df = pd.read_csv(ZerodhaInstrumentDirectory, delimiter=",")
        Df["expiry"] = pd.to_datetime(Df["expiry"].str.title(), format="%Y-%m-%d", errors="coerce")
        Today = datetime.now()
        Filtered = Df[
            (Df["name"] == InstrumentName) &
            (Df["exch_seg"] == Exchange) &
            (Df["instrumenttype"] == InstrumentType) &
            (Df["expiry"] > Today)
        ].sort_values(by="expiry", ascending=True).head(1)
        if not Filtered.empty:
            return Filtered.iloc[0]["symbol"], str(Filtered.iloc[0]["token"])
    except Exception as e:
        Logger.warning("Failed to resolve Kite contract for %s: %s", InstrumentName, e)
    return None, None


def _ResolveAngelContract(InstrumentName, Exchange, InstrumentType="FUT"):
    """Resolve DHANIYA → DHANIYA25APR2025 from AngelInstrumentDetails.csv."""
    try:
        Df = pd.read_csv(AngelInstrumentDirectory, delimiter=",")
        Df["expiry"] = pd.to_datetime(Df["expiry"].str.title(), format="%d%b%Y", errors="coerce")
        Today = datetime.now()
        Filtered = Df[
            (Df["name"] == InstrumentName) &
            (Df["exch_seg"] == Exchange) &
            (Df["instrumenttype"] == InstrumentType) &
            (Df["expiry"] > Today)
        ].sort_values(by="expiry", ascending=True).head(1)
        if not Filtered.empty:
            return Filtered.iloc[0]["symbol"], str(Filtered.iloc[0]["token"])
    except Exception as e:
        Logger.warning("Failed to resolve Angel contract for %s: %s", InstrumentName, e)
    return None, None


def _FetchAllLTPs(InstrumentConfig):
    """Fetch LTP for all open futures positions + open options from brokers.

    Returns dict: {instrument_name: ltp} e.g. {"GOLDM": 73100.0, "NIFTY_OPT_CE": 45.0}
    """
    LTPs = {}
    OpenPositions = db.GetAllOpenPositions()
    OpenInstruments = {p["instrument"] for p in OpenPositions}

    # Load instrument config for exchange/broker mapping
    Instruments = InstrumentConfig.get("instruments", {})

    # ── Kite (Zerodha) batch LTP ──
    KiteSymbols = {}  # {exchange_key: instrument_name}
    for InstName in OpenInstruments:
        Cfg = Instruments.get(InstName)
        if not Cfg or Cfg.get("broker") != "ZERODHA":
            continue
        Symbol, Token = _ResolveKiteContract(InstName, Cfg["exchange"])
        if Symbol:
            Key = f"{Cfg['exchange']}:{Symbol}"
            KiteSymbols[Key] = InstName

    if KiteSymbols:
        try:
            Kite = EstablishConnectionKiteAPI({"User": "YD6016"})
            LtpData = Kite.ltp(list(KiteSymbols.keys()))
            for Key, InstName in KiteSymbols.items():
                if Key in LtpData:
                    LTPs[InstName] = float(LtpData[Key]["last_price"])
        except Exception as e:
            Logger.error("Kite LTP fetch failed: %s", e)

    # ── Angel (NCDEX) individual LTP ──
    AngelInstruments = []
    for InstName in OpenInstruments:
        Cfg = Instruments.get(InstName)
        if not Cfg or Cfg.get("broker") != "ANGEL":
            continue
        Symbol, Token = _ResolveAngelContract(InstName, Cfg["exchange"])
        if Symbol and Token:
            AngelInstruments.append((InstName, Cfg["exchange"], Symbol, Token))

    if AngelInstruments:
        try:
            SmartApi = EstablishConnectionAngelAPI({"User": "E51339915"})
            for InstName, Exchange, Symbol, Token in AngelInstruments:
                try:
                    Data = SmartApi.ltpData(exchange=Exchange, tradingsymbol=Symbol, symboltoken=Token)
                    if Data and Data.get("data") and Data["data"].get("ltp") not in (None, ""):
                        LTPs[InstName] = float(Data["data"]["ltp"])
                except Exception as e:
                    Logger.warning("Angel LTP failed for %s: %s", InstName, e)
        except Exception as e:
            Logger.error("Angel session failed: %s", e)

    # ── Options LTP from Kite ──
    OptionsLTPs = _FetchOptionsLTPs()
    LTPs.update(OptionsLTPs)

    return LTPs


def _FetchOptionsLTPs():
    """Fetch LTP for open options positions from v2_state.json."""
    LTPs = {}
    try:
        if not STATE_FILE_PATH.exists():
            return LTPs
        with open(STATE_FILE_PATH) as f:
            State = json.load(f)

        KiteKeys = {}  # {exchange_key: options_instrument_name}
        ExchangeMap = {"NIFTY": "NFO", "SENSEX": "BFO"}

        for Underlying, UState in State.items():
            if UState.get("activeLots", 0) <= 0:
                continue
            Contracts = UState.get("activeContracts", [])
            Exchange = ExchangeMap.get(Underlying, "NFO")
            for Contract in Contracts:
                Leg = "CE" if "CE" in Contract.upper() else "PE"
                OptKey = f"{Underlying}_OPT_{Leg}"
                Key = f"{Exchange}:{Contract}"
                KiteKeys[Key] = OptKey

        if KiteKeys:
            Kite = EstablishConnectionKiteAPI({"User": "YD6016"})
            LtpData = Kite.ltp(list(KiteKeys.keys()))
            for Key, OptKey in KiteKeys.items():
                if Key in LtpData:
                    LTPs[OptKey] = float(LtpData[Key]["last_price"])
    except Exception as e:
        Logger.warning("Options LTP fetch failed: %s", e)
    return LTPs


# ─── Unrealized P&L Computation ───────────────────────────────────


def _ComputeUnrealizedPnl(OpenPositions, LTPs):
    """Compute unrealized P&L for each open position.

    Returns list of dicts with: instrument, confirmed_qty, avg_entry_price,
    point_value, ltp, unrealized_pnl, direction.
    """
    Results = []
    for Pos in OpenPositions:
        Inst = Pos["instrument"]
        Qty = Pos["confirmed_qty"]
        AvgEntry = Pos["avg_entry_price"]
        PV = Pos["point_value"]
        Ltp = LTPs.get(Inst, 0)

        if Ltp == 0 or AvgEntry == 0:
            UnrealizedPnl = 0
        elif Qty > 0:
            UnrealizedPnl = (Ltp - AvgEntry) * abs(Qty) * PV
        else:
            UnrealizedPnl = (AvgEntry - Ltp) * abs(Qty) * PV

        Direction = "LONG" if Qty > 0 else "SHORT"
        Results.append({
            "instrument": Inst,
            "confirmed_qty": Qty,
            "avg_entry_price": AvgEntry,
            "point_value": PV,
            "ltp": Ltp,
            "unrealized_pnl": UnrealizedPnl,
            "direction": Direction,
        })
    return Results


# ─── Card Builders ─────────────────────────────────────────────────


def _BuildSummaryCard(ReportDate, TradeCount, RealizedTotal, UnrealizedChange,
                      TotalDailyPnl, CumulativePnl, OpenCount, EffectiveCapital):
    """Card 1: Daily Summary."""
    return {
        "title": "Daily Summary",
        "icon": "\U0001f4ca",
        "rows": [
            ("Report Date", ReportDate),
            ("Trades Today", str(TradeCount)),
            ("Realized P&L", f"\u20b9{_FmtINR(RealizedTotal)}", _PnlColor(RealizedTotal), True),
            ("Unrealized P&L Change", f"\u20b9{_FmtINR(UnrealizedChange)}", _PnlColor(UnrealizedChange), True),
            ("Total Daily P&L", f"\u20b9{_FmtINR(TotalDailyPnl)}", _PnlColor(TotalDailyPnl), True),
            ("Cumulative Realized (All Time)", f"\u20b9{_FmtINR(CumulativePnl)}",
             _PnlColor(CumulativePnl), False),
            ("Open Positions", str(OpenCount)),
            ("Effective Capital", f"\u20b9{_FmtPlain(EffectiveCapital)}", None, True),
        ],
    }


def _BuildClosedPositionsCard(PnlRows):
    """Card 2: Closed positions — options combined per underlying."""
    if not PnlRows:
        return None

    # Group options by underlying, keep futures individual
    Combined = defaultdict(lambda: {"pnl": 0, "legs": []})
    FuturesRows = []

    for R in PnlRows:
        Inst = R["instrument"]
        if "_OPT_" in Inst:
            Underlying = Inst.split("_OPT_")[0]
            Leg = Inst.split("_OPT_")[1]
            Combined[Underlying]["pnl"] += R["pnl_inr"]
            Combined[Underlying]["legs"].append(
                f"{Leg}: {R['entry_price']:.1f}\u2192{R['exit_price']:.1f} ({_FmtINR(R['pnl_inr'])})"
            )
        else:
            FuturesRows.append(R)

    Rows = []
    for R in FuturesRows:
        Pnl = R["pnl_inr"]
        Rows.append((
            f"{R['instrument']} (Futures)",
            f"{R['close_qty']} lots | {R['entry_price']:.2f} \u2192 {R['exit_price']:.2f} | \u20b9{_FmtINR(Pnl)}",
            _PnlColor(Pnl),
        ))

    for Underlying, Data in Combined.items():
        Pnl = Data["pnl"]
        LegDetail = " | ".join(Data["legs"])
        Rows.append((
            f"{Underlying} Straddle",
            f"{LegDetail} | Net: \u20b9{_FmtINR(Pnl)}",
            _PnlColor(Pnl),
        ))

    return {"title": "Closed Positions", "icon": "\u2705", "rows": Rows}


def _BuildFuturesTradesCard(Orders):
    """Card 3: Futures trades today."""
    if not Orders:
        return None
    Rows = []
    for O in Orders:
        Fill = f"Fill: {O['fill_price']:.2f}" if O.get("fill_price") else "Fill: N/A"
        Slip = f"Slip: {O['slippage']:.2f}" if O.get("slippage") else ""
        Time = _FmtTime(O.get("created_at", ""))
        Detail = " | ".join(filter(None, [Fill, Slip, Time]))
        Rows.append((
            f"{O['instrument']} {O['action']} {O['qty']}",
            Detail,
        ))
    return {"title": "Futures Trades", "icon": "\u21c5", "rows": Rows}


def _BuildOptionsTradesCard(Orders):
    """Card 4: Options trades today."""
    if not Orders:
        return None
    Rows = []
    for O in Orders:
        Fill = f"Fill: {O['fill_price']:.2f}" if O.get("fill_price") else "Fill: N/A"
        Slip = f"Slip: {O['slippage']:.2f}" if O.get("slippage") else ""
        Time = _FmtTime(O.get("created_at", ""))
        Detail = " | ".join(filter(None, [Fill, Slip, Time]))
        Rows.append((
            f"{O['underlying']} {O['leg']} {O['action']} {O['qty']}",
            Detail,
        ))
    return {"title": "Options Trades", "icon": "\U0001f4b9", "rows": Rows}


def _BuildOpenFuturesCard(UnrealizedPositions, PrevSnapshot):
    """Card 5: Open futures positions with unrealized P&L and daily change."""
    FuturesPos = [P for P in UnrealizedPositions if "_OPT_" not in P["instrument"]]
    if not FuturesPos:
        return {"title": "Open Futures Positions", "icon": "\U0001f4bc",
                "rows": [("Status", "No open futures positions", MUTED)]}

    Rows = []
    for P in FuturesPos:
        Inst = P["instrument"]
        PrevUnrealized = PrevSnapshot.get(Inst, 0)
        Change = P["unrealized_pnl"] - PrevUnrealized

        Ltp = f"LTP: {P['ltp']:.2f}" if P["ltp"] > 0 else "LTP: N/A"
        Rows.append((
            Inst,
            f"{P['direction']} {abs(P['confirmed_qty'])} @ {P['avg_entry_price']:.2f} | {Ltp}",
        ))
        Rows.append((
            f"  Unrealized P&L",
            f"\u20b9{_FmtINR(P['unrealized_pnl'])} (today: \u20b9{_FmtINR(Change)})",
            _PnlColor(Change), True,
        ))
    return {"title": "Open Futures Positions", "icon": "\U0001f4bc", "rows": Rows}


def _BuildOpenOptionsCard(UnrealizedPositions, PrevSnapshot):
    """Card 6: Open options positions combined per underlying."""
    OptionsPos = [P for P in UnrealizedPositions if "_OPT_" in P["instrument"]]
    if not OptionsPos:
        # Check v2_state.json for state info even if no cost basis
        try:
            if STATE_FILE_PATH.exists():
                with open(STATE_FILE_PATH) as f:
                    State = json.load(f)
                for Ul, Us in State.items():
                    if Us.get("activeLots", 0) > 0:
                        return {"title": "Open Options Positions", "icon": "\U0001f6e1\ufe0f",
                                "rows": [(Ul, f"{Us['activeLots']} lots | {', '.join(Us.get('activeContracts', []))}",)]}
        except Exception:
            pass
        return {"title": "Open Options Positions", "icon": "\U0001f6e1\ufe0f",
                "rows": [("Status", "No open options positions", MUTED)]}

    # Group by underlying
    ByUnderlying = defaultdict(lambda: {"unrealized": 0, "prev_unrealized": 0, "legs": []})
    for P in OptionsPos:
        Parts = P["instrument"].split("_OPT_")
        Underlying = Parts[0]
        Leg = Parts[1] if len(Parts) > 1 else "?"
        ByUnderlying[Underlying]["unrealized"] += P["unrealized_pnl"]
        ByUnderlying[Underlying]["prev_unrealized"] += PrevSnapshot.get(P["instrument"], 0)
        LtpStr = f"{P['ltp']:.2f}" if P["ltp"] > 0 else "N/A"
        ByUnderlying[Underlying]["legs"].append(
            f"{Leg}: Entry {P['avg_entry_price']:.1f}, LTP {LtpStr}"
        )

    # Enrich with state file data
    StateInfo = {}
    try:
        if STATE_FILE_PATH.exists():
            with open(STATE_FILE_PATH) as f:
                StateInfo = json.load(f)
    except Exception:
        pass

    Rows = []
    for Underlying, Data in ByUnderlying.items():
        Change = Data["unrealized"] - Data["prev_unrealized"]
        Lots = StateInfo.get(Underlying, {}).get("activeLots", "?")
        LegDetail = " | ".join(Data["legs"])
        Rows.append((
            f"{Underlying} ({Lots} lots)",
            LegDetail,
        ))
        Rows.append((
            f"  Unrealized P&L",
            f"\u20b9{_FmtINR(Data['unrealized'])} (today: \u20b9{_FmtINR(Change)})",
            _PnlColor(Change), True,
        ))
    return {"title": "Open Options Positions", "icon": "\U0001f6e1\ufe0f", "rows": Rows}


def _BuildEffectiveCapitalCard(BaseCap, CumulativePnl, EffectiveCap):
    """Card 7: Effective capital breakdown."""
    return {
        "title": "Effective Capital",
        "icon": "\U0001f3e6",
        "rows": [
            ("Base Capital", f"\u20b9{_FmtPlain(BaseCap)}"),
            ("Cumulative Realized P&L", f"\u20b9{_FmtINR(CumulativePnl)}", _PnlColor(CumulativePnl)),
            ("Effective Capital (in use)", f"\u20b9{_FmtPlain(EffectiveCap)}", None, True),
        ],
    }


# ─── Main Report Logic ────────────────────────────────────────────


def GenerateDailyReport(DryRun=False, DateStr=None):
    """Generate and send the daily P&L report."""
    db.InitDB()

    if DateStr is None:
        DateStr = date.today().strftime("%Y-%m-%d")

    Logger.info("Generating daily P&L report for %s (dry_run=%s)", DateStr, DryRun)

    # Load config
    with open(CONFIG_PATH) as f:
        FullConfig = json.load(f)

    BaseCapital = FullConfig["account"]["base_capital"]
    CumulativePnl = db.GetCumulativeRealizedPnl()
    EffectiveCapital = BaseCapital + CumulativePnl

    # ── Query data ──
    RealizedRows = db.GetTodayRealizedPnl(DateStr)
    FuturesOrders = db.GetTodayFuturesOrders(DateStr)
    OptionsOrders = db.GetTodayOptionsOrders(DateStr)
    OpenPositions = db.GetAllOpenPositions()

    RealizedTotal = sum(r["pnl_inr"] for r in RealizedRows)
    TradeCount = len(FuturesOrders) + len(OptionsOrders)

    # ── Fetch LTPs and compute unrealized P&L ──
    LTPs = _FetchAllLTPs(FullConfig)
    UnrealizedPositions = _ComputeUnrealizedPnl(OpenPositions, LTPs)

    # ── Snapshot comparison for unrealized change ──
    PrevSnapshot = db.GetPreviousSnapshot(DateStr)
    TotalUnrealizedNow = sum(p["unrealized_pnl"] for p in UnrealizedPositions)
    TotalUnrealizedPrev = sum(PrevSnapshot.get(p["instrument"], 0) for p in UnrealizedPositions)
    # Also account for positions that existed yesterday but are now closed
    for Inst, PrevVal in PrevSnapshot.items():
        if not any(p["instrument"] == Inst for p in UnrealizedPositions):
            TotalUnrealizedPrev += PrevVal
    UnrealizedChange = TotalUnrealizedNow - TotalUnrealizedPrev

    TotalDailyPnl = RealizedTotal + UnrealizedChange

    # ── Save today's snapshot ──
    Snapshots = [
        {
            "instrument": P["instrument"],
            "confirmed_qty": P["confirmed_qty"],
            "avg_entry_price": P["avg_entry_price"],
            "ltp": P["ltp"],
            "unrealized_pnl": P["unrealized_pnl"],
        }
        for P in UnrealizedPositions
    ]
    db.SaveDailySnapshot(DateStr, Snapshots)

    # ── Build cards ──
    Cards = []
    OpenCount = len([p for p in OpenPositions if "_OPT_" not in p["instrument"]])

    Cards.append(_BuildSummaryCard(
        DateStr, TradeCount, RealizedTotal, UnrealizedChange,
        TotalDailyPnl, CumulativePnl, OpenCount, EffectiveCapital
    ))

    ClosedCard = _BuildClosedPositionsCard(RealizedRows)
    if ClosedCard:
        Cards.append(ClosedCard)

    FuturesCard = _BuildFuturesTradesCard(FuturesOrders)
    if FuturesCard:
        Cards.append(FuturesCard)

    OptionsCard = _BuildOptionsTradesCard(OptionsOrders)
    if OptionsCard:
        Cards.append(OptionsCard)

    Cards.append(_BuildOpenFuturesCard(UnrealizedPositions, PrevSnapshot))
    Cards.append(_BuildOpenOptionsCard(UnrealizedPositions, PrevSnapshot))
    Cards.append(_BuildEffectiveCapitalCard(BaseCapital, CumulativePnl, EffectiveCapital))

    # ── Build email ──
    DateDisplay = datetime.strptime(DateStr, "%Y-%m-%d").strftime("%d %b %Y")
    Title = f"Daily P&L Report \u2014 {DateDisplay}"
    Subtitle = f"Total Daily P&L: \u20b9{_FmtINR(TotalDailyPnl)}"

    Html = _BuildEmailHtml(Title, Subtitle, Cards, FooterLabel="Daily P&L Report")
    Subject = f"Daily P&L Report | {DateDisplay} | \u20b9{_FmtINR(TotalDailyPnl)}"

    if DryRun:
        print(Html)
        Logger.info("Dry run — HTML printed to stdout, email not sent")
    else:
        _SendEmail(Subject, Html)
        Logger.info("Daily P&L report sent for %s", DateStr)

    return {
        "realized_total": RealizedTotal,
        "unrealized_change": UnrealizedChange,
        "total_daily_pnl": TotalDailyPnl,
        "effective_capital": EffectiveCapital,
    }


def main():
    Parser = argparse.ArgumentParser(description="Daily P&L Email Report")
    Parser.add_argument("--dry-run", action="store_true", help="Print HTML, don't email")
    Parser.add_argument("--date", type=str, default=None,
                        help="Report date (YYYY-MM-DD). Defaults to today.")
    Args = Parser.parse_args()

    try:
        GenerateDailyReport(DryRun=Args.dry_run, DateStr=Args.date)
    except Exception as e:
        Logger.error("Daily P&L report failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
