"""
daily_pnl_report.py — End-of-day P&L email report.

Everything from the broker. Zero database usage.

Sends a styled HTML email with:
  1. Daily Swing (hero) — how much the account moved today
     (LTP - Previous Close) x Lots x Point Value per position, summed.
  2. Total Unrealized — overall embedded P&L across all open positions
     (LTP - Average Entry) x Lots x Point Value per position, summed.
  3. Open Positions — each with Entry, Prev Close, LTP, total P&L badge,
     and today's swing. Options grouped by underlying.
  4. Today's Trades — completed orders from broker order history.

Broker Accounts:
  - YD6016 (Kite/Zerodha)  — MCX futures (SILVERMIC, ZINCMINI, etc.)
  - AABM826021 (Angel)     — NCDEX futures (GUARSEED, DHANIYA, COCUDAKL, etc.)
  - OFS653 (Kite/Zerodha)  — Index options NFO/BFO (NIFTY, SENSEX, BANKNIFTY)

Key implementation notes:
  - Angel netqty is in units; divided by QuantityMultiplier to get lots.
  - Angel carry-forward entry uses cfbuyavgprice/cfsellavgprice (not buyavgprice).
  - Kite prev close = close_price field; Angel prev close = close field.
  - Option direction from qty sign (positive=LONG, negative=SHORT).
  - TURMERIC matched via ReconciliationPrefixes (TMCFGRNZM -> TURMERIC).
  - Post-midnight (before 09:00 IST): uses previous trading day.

Usage:
  python daily_pnl_report.py               # send today's report
  python daily_pnl_report.py --dry-run     # print HTML to stdout
  python daily_pnl_report.py --date 2026-04-01  # report for a specific date

Cron: 45 23 * * 1-5  (23:45 IST, Mon-Fri, after MCX close)
"""

import json
import logging
import argparse
import sys
import html as _html
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict

import pandas as pd

from Server_Order_Handler import EstablishConnectionAngelAPI
from rollover_monitor import _SendEmail, _EstablishKiteSession
from Directories import workInputRoot, ZerodhaInstrumentDirectory, AngelInstrumentDirectory

Logger = logging.getLogger("daily_pnl_report")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

CONFIG_PATH = Path(__file__).parent / "instrument_config.json"
STATE_FILE_PATH = Path(workInputRoot) / "v2_state.json"

# ─── Colors ───────────────────────────────────────────────────────

GREEN = "#16a34a"
RED = "#dc2626"
MUTED = "#94a3b8"
NAVY = "#0f172a"
SLATE = "#64748b"
BORDER = "#e2e8f0"
BG = "#f8fafc"


# ─── Formatting ──────────────────────────────────────────────────

def _FmtINR(Amount, Decimals=0):
    Sign = "+" if Amount >= 0 else "-"
    Abs = abs(Amount)
    if Decimals == 0:
        return f"{Sign}{int(round(Abs)):,}"
    return f"{Sign}{Abs:,.{Decimals}f}"


def _FmtPlain(Amount, Decimals=0):
    Abs = abs(Amount)
    if Decimals == 0:
        return f"{int(round(Abs)):,}"
    return f"{Abs:,.{Decimals}f}"


def _PnlColor(Amount):
    if Amount > 0: return GREEN
    if Amount < 0: return RED
    return MUTED


def _PnlBg(Amount):
    if Amount > 0: return "#f0fdf4"
    if Amount < 0: return "#fef2f2"
    return "#f8fafc"


# ─── Symbol Matching ─────────────────────────────────────────────

def _IsIndexOption(Symbol):
    S = Symbol.upper()
    return any(S.startswith(P) for P in ("NIFTY", "SENSEX", "BANKEX", "BANKNIFTY")) and (
        S.endswith("CE") or S.endswith("PE"))


def _MatchToInstrument(TradingSymbol, Exchange, Broker, Instruments):
    """Match broker symbol to instrument_config via CSV lookup, then prefix fallback."""
    CsvPath = ZerodhaInstrumentDirectory if Broker == "ZERODHA" else AngelInstrumentDirectory
    try:
        Df = pd.read_csv(CsvPath, delimiter=",", low_memory=False)
        Match = Df[(Df["symbol"] == TradingSymbol) & (Df["exch_seg"] == Exchange)]
        if not Match.empty:
            Name = Match.iloc[0]["name"]
            for InstName, Cfg in Instruments.items():
                if InstName == Name and Cfg.get("exchange") == Exchange:
                    return InstName, Cfg
    except Exception as e:
        Logger.warning("CSV match failed for %s: %s", TradingSymbol, e)

    for InstName, Cfg in Instruments.items():
        if TradingSymbol.upper().startswith(InstName.upper()):
            if Cfg.get("exchange") == Exchange:
                return InstName, Cfg

    # Fallback: match via ReconciliationPrefixes (e.g. TMCFGRNZM → TURMERIC)
    for InstName, Cfg in Instruments.items():
        Prefixes = Cfg.get("order_routing", {}).get("ReconciliationPrefixes", [])
        for Prefix in Prefixes:
            if TradingSymbol.upper().startswith(Prefix.upper()):
                if Cfg.get("exchange") == Exchange:
                    return InstName, Cfg

    return None, None


# ─── Fetch Open Positions ────────────────────────────────────────

def _CalcPnl(Direction, Price1, Price2, Qty, PV):
    """P&L from Price1 to Price2. LONG: (P2-P1)*Q*PV, SHORT: (P1-P2)*Q*PV."""
    if Direction == "LONG":
        return (Price2 - Price1) * Qty * PV
    return (Price1 - Price2) * Qty * PV


def _FetchOpenPositions(FullConfig):
    """Fetch open positions from all broker accounts.

    For each position returns:
      pnl         = (LTP - Entry) x Qty x PV  (total unrealized)
      daily_swing = (LTP - Prev Close) x Qty x PV  (today's movement)
      prev_close  = previous day's closing price from broker
    """
    Instruments = FullConfig.get("instruments", {})
    Positions = []

    # ── Kite YD6016 — MCX futures ──
    try:
        Kite = _EstablishKiteSession("YD6016")
        for Pos in Kite.positions().get("net", []):
            Qty = Pos.get("quantity", 0)
            if Qty == 0 or Pos.get("product") != "NRML":
                continue
            Symbol = Pos.get("tradingsymbol", "")
            Exchange = Pos.get("exchange", "")
            if _IsIndexOption(Symbol):
                continue

            InstName, Cfg = _MatchToInstrument(Symbol, Exchange, "ZERODHA", Instruments)
            if not InstName:
                Logger.warning("Unmatched Kite position: %s (%s)", Symbol, Exchange)
                continue

            AvgPrice = float(Pos.get("average_price", 0))
            Ltp = float(Pos.get("last_price", 0))
            PrevClose = float(Pos.get("close_price", 0) or 0)
            OvernightQty = abs(int(Pos.get("overnight_quantity", 0)))
            DayBuyQty = int(Pos.get("day_buy_quantity", 0) or 0)
            DaySellQty = int(Pos.get("day_sell_quantity", 0) or 0)
            DayBuyPrice = float(Pos.get("day_buy_price", 0) or 0)
            DaySellPrice = float(Pos.get("day_sell_price", 0) or 0)
            PV = Cfg.get("point_value", 1)
            Direction = "LONG" if Qty > 0 else "SHORT"
            AbsQty = abs(Qty)

            Pnl = _CalcPnl(Direction, AvgPrice, Ltp, AbsQty, PV)

            # ── Split swing: carried lots from prev_close, new lots from entry ──
            if Direction == "LONG":
                CarriedQty = max(0, OvernightQty - DaySellQty)
                NewQty = max(0, AbsQty - CarriedQty)
                NewEntryPrice = DayBuyPrice
            else:
                CarriedQty = max(0, OvernightQty - DayBuyQty)
                NewQty = max(0, AbsQty - CarriedQty)
                NewEntryPrice = DaySellPrice

            SwingBase = PrevClose if PrevClose > 0 else AvgPrice
            CarriedSwing = _CalcPnl(Direction, SwingBase, Ltp, CarriedQty, PV)
            NewSwing = _CalcPnl(Direction, NewEntryPrice, Ltp, NewQty, PV) if NewQty > 0 else 0
            DailySwing = CarriedSwing + NewSwing
            IsNewToday = (CarriedQty == 0)

            Positions.append({
                "instrument": InstName, "tradingsymbol": Symbol,
                "direction": Direction, "qty": AbsQty,
                "avg_entry": round(AvgPrice, 2), "prev_close": round(PrevClose, 2),
                "ltp": round(Ltp, 2), "point_value": PV,
                "pnl": round(Pnl, 2), "daily_swing": round(DailySwing, 2),
                "broker": "ZERODHA", "is_new_today": IsNewToday,
            })
        Logger.info("Kite YD6016: %d open futures", sum(1 for p in Positions if p["broker"] == "ZERODHA"))
    except Exception as e:
        Logger.error("Kite YD6016 fetch failed: %s", e)

    # ── Angel AABM826021 — NCDEX futures ──
    try:
        SmartApi = EstablishConnectionAngelAPI({"User": "AABM826021"})
        RawResponse = SmartApi.position()
        RawPositions = RawResponse.get("data", []) if isinstance(RawResponse, dict) else []
        if RawPositions is None:
            RawPositions = []

        for Pos in RawPositions:
            Qty = int(Pos.get("netqty", 0))
            if Qty == 0 or Pos.get("producttype") != "CARRYFORWARD":
                continue
            Symbol = Pos.get("tradingsymbol", "")
            Exchange = Pos.get("exchange", "")

            InstName, Cfg = _MatchToInstrument(Symbol, Exchange, "ANGEL", Instruments)
            if not InstName:
                Logger.warning("Unmatched Angel position: %s (%s)", Symbol, Exchange)
                continue

            Ltp = float(Pos.get("ltp", 0))
            PrevClose = float(Pos.get("close", 0) or 0)
            PV = Cfg.get("point_value", 1)
            Direction = "LONG" if Qty > 0 else "SHORT"
            AbsQty = abs(Qty)

            # Angel netqty is in units, but point_value is per lot.
            # Divide by QuantityMultiplier to get lots.
            QtyMult = Cfg.get("order_routing", {}).get("QuantityMultiplier", 1)
            Lots = AbsQty / QtyMult if QtyMult else AbsQty

            # ── Entry price: use blended average (totalbuyavgprice) to match broker ──
            # totalbuyavgprice includes both carry-forward AND today's trades.
            CfBuyPrice = float(Pos.get("cfbuyavgprice", 0) or 0)
            CfSellPrice = float(Pos.get("cfsellavgprice", 0) or 0)
            if Direction == "LONG":
                AvgPrice = float(
                    Pos.get("totalbuyavgprice", 0) or
                    CfBuyPrice or
                    Pos.get("buyavgprice", 0) or
                    Pos.get("avgnetprice", 0) or 0
                )
            else:
                AvgPrice = float(
                    Pos.get("totalsellavgprice", 0) or
                    CfSellPrice or
                    Pos.get("sellavgprice", 0) or
                    Pos.get("avgnetprice", 0) or 0
                )

            # ── Swing: split carried lots vs new-today lots ──
            # Carried lots swing from prev_close, new lots swing from today's buy/sell price.
            CfBuyQty = int(Pos.get("cfbuyqty", 0) or 0)
            CfSellQty = int(Pos.get("cfsellqty", 0) or 0)
            BuyQty = int(Pos.get("buyqty", 0) or 0)
            SellQty = int(Pos.get("sellqty", 0) or 0)
            TodayBuyPrice = float(Pos.get("buyavgprice", 0) or 0)
            TodaySellPrice = float(Pos.get("sellavgprice", 0) or 0)

            if Direction == "LONG":
                # Sells reduce carried qty first (FIFO)
                CarriedUnits = max(0, CfBuyQty - SellQty)
                NewUnits = max(0, AbsQty - CarriedUnits)
                NewEntryPrice = TodayBuyPrice
            else:
                CarriedUnits = max(0, CfSellQty - BuyQty)
                NewUnits = max(0, AbsQty - CarriedUnits)
                NewEntryPrice = TodaySellPrice

            CarriedLots = CarriedUnits / QtyMult if QtyMult else CarriedUnits
            NewLots = NewUnits / QtyMult if QtyMult else NewUnits
            IsNewToday = (CarriedUnits == 0)

            SwingBase = PrevClose if PrevClose > 0 else AvgPrice
            CarriedSwing = _CalcPnl(Direction, SwingBase, Ltp, CarriedLots, PV)
            NewSwing = _CalcPnl(Direction, NewEntryPrice, Ltp, NewLots, PV) if NewLots > 0 else 0
            DailySwing = CarriedSwing + NewSwing

            Pnl = _CalcPnl(Direction, AvgPrice, Ltp, Lots, PV)
            Logger.debug("  Angel %s: cf_units=%d new_units=%d cf_lots=%.1f new_lots=%.1f",
                         InstName, CarriedUnits, NewUnits, CarriedLots, NewLots)

            Positions.append({
                "instrument": InstName, "tradingsymbol": Symbol,
                "direction": Direction, "qty": AbsQty, "lots": Lots,
                "avg_entry": round(AvgPrice, 2), "prev_close": round(PrevClose, 2),
                "ltp": round(Ltp, 2), "point_value": PV,
                "pnl": round(Pnl, 2), "daily_swing": round(DailySwing, 2),
                "broker": "ANGEL", "is_new_today": IsNewToday,
            })
        Logger.info("Angel: %d open NCDEX positions", sum(1 for p in Positions if p["broker"] == "ANGEL"))
    except Exception as e:
        Logger.error("Angel positions fetch failed: %s", e)

    # ── Kite OFS653 — Options (NFO/BFO) ──
    try:
        Kite = _EstablishKiteSession("OFS653")
        for Pos in Kite.positions().get("net", []):
            Qty = Pos.get("quantity", 0)
            if Qty == 0 or Pos.get("product") != "NRML":
                continue
            Symbol = Pos.get("tradingsymbol", "")
            if not _IsIndexOption(Symbol):
                continue

            S = Symbol.upper()
            if S.startswith("BANKNIFTY"):
                Underlying = "BANKNIFTY"
            elif S.startswith("NIFTY"):
                Underlying = "NIFTY"
            elif S.startswith("SENSEX"):
                Underlying = "SENSEX"
            elif S.startswith("BANKEX"):
                Underlying = "BANKEX"
            else:
                continue
            Leg = "CE" if S.endswith("CE") else "PE"

            AvgPrice = float(Pos.get("average_price", 0))
            Ltp = float(Pos.get("last_price", 0))
            PrevClose = float(Pos.get("close_price", 0) or 0)
            OvernightQty = abs(int(Pos.get("overnight_quantity", 0)))
            DayBuyQty = int(Pos.get("day_buy_quantity", 0) or 0)
            DaySellQty = int(Pos.get("day_sell_quantity", 0) or 0)
            DayBuyPrice = float(Pos.get("day_buy_price", 0) or 0)
            DaySellPrice = float(Pos.get("day_sell_price", 0) or 0)
            AbsQty = abs(Qty)
            Direction = "LONG" if Qty > 0 else "SHORT"

            Pnl = _CalcPnl(Direction, AvgPrice, Ltp, AbsQty, 1.0)

            # Split swing: carried from prev_close, new from today's entry
            if Direction == "LONG":
                CarriedQty = max(0, OvernightQty - DaySellQty)
                NewQty = max(0, AbsQty - CarriedQty)
                NewEntryPrice = DayBuyPrice
            else:
                CarriedQty = max(0, OvernightQty - DayBuyQty)
                NewQty = max(0, AbsQty - CarriedQty)
                NewEntryPrice = DaySellPrice

            SwingBase = PrevClose if PrevClose > 0 else AvgPrice
            CarriedSwing = _CalcPnl(Direction, SwingBase, Ltp, CarriedQty, 1.0)
            NewSwing = _CalcPnl(Direction, NewEntryPrice, Ltp, NewQty, 1.0) if NewQty > 0 else 0
            DailySwing = CarriedSwing + NewSwing
            IsNewToday = (CarriedQty == 0)

            Positions.append({
                "instrument": f"{Underlying}_OPT_{Leg}", "tradingsymbol": Symbol,
                "direction": Direction, "qty": AbsQty,
                "avg_entry": round(AvgPrice, 2), "prev_close": round(PrevClose, 2),
                "ltp": round(Ltp, 2), "point_value": 1.0,
                "pnl": round(Pnl, 2), "daily_swing": round(DailySwing, 2),
                "broker": "ZERODHA", "is_new_today": IsNewToday,
            })
        Logger.info("Kite OFS653: %d open options", sum(1 for p in Positions if "_OPT_" in p["instrument"]))
    except Exception as e:
        Logger.warning("Options fetch failed: %s", e)

    Logger.info("Total open positions: %d", len(Positions))
    return Positions


# ─── Fetch Today's Orders ────────────────────────────────────────

def _FetchTodayOrders(FullConfig):
    """Fetch today's completed orders from all broker accounts."""
    Instruments = FullConfig.get("instruments", {})
    FuturesOrders = []
    OptionsOrders = []

    # ── Kite orders — YD6016 + OFS653 ──
    for KiteUser in ["YD6016", "OFS653"]:
        try:
            Kite = _EstablishKiteSession(KiteUser)
            for O in Kite.orders():
                if O.get("status") != "COMPLETE" or O.get("product") != "NRML":
                    continue
                Symbol = O.get("tradingsymbol", "")
                Exchange = O.get("exchange", "")
                Qty = O.get("filled_quantity", 0) or O.get("quantity", 0)
                AvgPrice = float(O.get("average_price", 0))
                Action = O.get("transaction_type", "")
                OrderTime = O.get("order_timestamp", "")
                if isinstance(OrderTime, datetime):
                    OrderTime = OrderTime.strftime("%H:%M")
                else:
                    OrderTime = str(OrderTime)[-8:-3] if len(str(OrderTime)) > 8 else ""

                if _IsIndexOption(Symbol):
                    S = Symbol.upper()
                    if S.startswith("BANKNIFTY"): Underlying = "BANKNIFTY"
                    elif S.startswith("NIFTY"): Underlying = "NIFTY"
                    elif S.startswith("SENSEX"): Underlying = "SENSEX"
                    else: Underlying = Symbol[:6]
                    OptionsOrders.append({
                        "contract": Symbol, "underlying": Underlying,
                        "leg": "CE" if S.endswith("CE") else "PE",
                        "action": Action, "qty": Qty,
                        "fill_price": AvgPrice, "time": OrderTime, "broker": "ZERODHA",
                    })
                else:
                    InstName, _ = _MatchToInstrument(Symbol, Exchange, "ZERODHA", Instruments)
                    FuturesOrders.append({
                        "instrument": InstName or Symbol,
                        "action": Action, "qty": Qty,
                        "fill_price": AvgPrice, "time": OrderTime, "broker": "ZERODHA",
                    })
            Logger.info("Kite %s: orders fetched", KiteUser)
        except Exception as e:
            Logger.error("Kite %s orders failed: %s", KiteUser, e)

    # ── Angel orders ──
    try:
        SmartApi = EstablishConnectionAngelAPI({"User": "AABM826021"})
        RawResponse = SmartApi.orderBook()
        RawOrders = RawResponse.get("data", []) if isinstance(RawResponse, dict) else []
        if RawOrders is None:
            RawOrders = []
        for O in RawOrders:
            if str(O.get("status", "")).lower() != "complete":
                continue
            if O.get("producttype") != "CARRYFORWARD":
                continue
            Symbol = O.get("tradingsymbol", "")
            Exchange = O.get("exchange", "")
            Qty = int(O.get("filledshares", 0) or O.get("quantity", 0))
            AvgPrice = float(O.get("averageprice", 0) or 0)
            Action = O.get("transactiontype", "")
            OrderTime = O.get("updatetime", "") or O.get("ordertime", "")
            if len(str(OrderTime)) > 5:
                OrderTime = str(OrderTime)[-8:-3]

            InstName, _ = _MatchToInstrument(Symbol, Exchange, "ANGEL", Instruments)
            FuturesOrders.append({
                "instrument": InstName or Symbol,
                "action": Action, "qty": Qty,
                "fill_price": AvgPrice, "time": OrderTime, "broker": "ANGEL",
            })
        Logger.info("Angel: orders fetched")
    except Exception as e:
        Logger.error("Angel orders failed: %s", e)

    return FuturesOrders, OptionsOrders


# ─── HTML Report ─────────────────────────────────────────────────

def _BuildReportHtml(D):
    """Build the full HTML email."""
    DateDisplay = datetime.strptime(D["date"], "%Y-%m-%d").strftime("%d %b %Y")
    DailySwing = D["total_daily_swing"]
    TotalPnl = D["total_pnl"]
    HeroColor = _PnlColor(DailySwing)
    HeroBg = "#0d3320" if DailySwing >= 0 else "#3b1119"

    DarkStyle = """
    @media (prefers-color-scheme: dark) {
        .pnl-body { background-color: #0d1117 !important; }
        .pnl-wrap { background-color: #161b22 !important; border-color: #30363d !important; }
        .pnl-section { background-color: #161b22 !important; }
        .pnl-row td, .pnl-row th { border-color: #21262d !important; color: #e6edf3 !important; }
        .pnl-footer { color: #484f58 !important; border-color: #21262d !important; }
    }
    """

    # ── Header ──
    HeaderHtml = f"""
    <div style="background:linear-gradient(135deg,#0f172a,#1e3a5f);
        padding:28px 24px 20px;text-align:center;">
        <div style="font-size:12px;font-weight:600;color:rgba(255,255,255,0.5);
            text-transform:uppercase;letter-spacing:1.5px;">Daily P&L Report</div>
        <div style="font-size:15px;color:rgba(255,255,255,0.8);margin-top:4px;">{DateDisplay}</div>
        <div style="margin-top:14px;display:inline-block;background:{HeroBg};
            padding:10px 28px;border-radius:10px;">
            <span style="font-size:28px;font-weight:800;color:{HeroColor};
                letter-spacing:0.5px;">\u20b9{_FmtINR(DailySwing)}</span>
        </div>
        <div style="font-size:11px;color:rgba(255,255,255,0.4);margin-top:6px;">
            TODAY'S P&L SWING</div>
    </div>"""

    # ── Quick Stats ──
    UnrealizedColor = _PnlColor(TotalPnl)
    StatsHtml = f"""
    <table style="width:100%;border-collapse:collapse;border-bottom:1px solid {BORDER};">
        <tr>
            <td style="width:33%;text-align:center;padding:14px 8px;">
                <div style="font-size:11px;color:{SLATE};text-transform:uppercase;">Positions</div>
                <div style="font-size:17px;font-weight:700;color:{NAVY};">{D["position_count"]}</div>
            </td>
            <td style="width:34%;text-align:center;padding:14px 8px;border-left:1px solid {BORDER};border-right:1px solid {BORDER};">
                <div style="font-size:11px;color:{SLATE};text-transform:uppercase;">Trades Today</div>
                <div style="font-size:17px;font-weight:700;color:{NAVY};">{D["trade_count"]}</div>
            </td>
            <td style="width:33%;text-align:center;padding:14px 8px;">
                <div style="font-size:11px;color:{SLATE};text-transform:uppercase;">Total Unrealized</div>
                <div style="font-size:17px;font-weight:700;color:{UnrealizedColor};">\u20b9{_FmtINR(TotalPnl)}</div>
            </td>
        </tr>
    </table>"""

    # ── Open Futures ──
    FutPos = [P for P in D["positions"] if "_OPT_" not in P["instrument"]]
    FutHtml = _SectionHeader("Open Futures")
    if FutPos:
        for P in FutPos:
            FutHtml += _PositionRow(P)
    else:
        FutHtml += _EmptyRow("No open futures")

    # ── Open Options ──
    OptPos = [P for P in D["positions"] if "_OPT_" in P["instrument"]]
    OptHtml = _SectionHeader("Open Options")
    if OptPos:
        ByUnderlying = defaultdict(lambda: {"pnl": 0, "daily_swing": 0, "legs": [], "max_qty": 0})
        for P in OptPos:
            Parts = P["instrument"].split("_OPT_")
            Underlying = Parts[0]
            Leg = Parts[1] if len(Parts) > 1 else "?"
            ByUnderlying[Underlying]["pnl"] += P["pnl"]
            ByUnderlying[Underlying]["daily_swing"] += P["daily_swing"]
            # Track max qty across legs to derive lot count
            ByUnderlying[Underlying]["max_qty"] = max(
                ByUnderlying[Underlying]["max_qty"], P["qty"])
            LtpStr = f"{P['ltp']:.2f}" if P["ltp"] > 0 else "N/A"
            DirTag = "L" if P["direction"] == "LONG" else "S"
            ByUnderlying[Underlying]["legs"].append(
                f"{Leg}({DirTag}): {P['avg_entry']:.1f} \u2192 {LtpStr} ({P['qty']} qty)")

        LOT_SIZES = {"NIFTY": 65, "BANKNIFTY": 15, "SENSEX": 20, "BANKEX": 15}

        for Underlying, Combo in ByUnderlying.items():
            PnlColor = _PnlColor(Combo["pnl"])
            PnlBgC = _PnlBg(Combo["pnl"])
            SwingColor = _PnlColor(Combo["daily_swing"])
            LotSize = LOT_SIZES.get(Underlying, 1)
            Lots = int(Combo["max_qty"] / LotSize) if LotSize else Combo["max_qty"]
            OptHtml += f"""
            <tr><td style="padding:12px 20px;border-bottom:1px solid {BORDER};">
                <div style="display:flex;justify-content:space-between;align-items:center;">
                    <div>
                        <span style="font-size:14px;font-weight:700;color:{NAVY};">{Underlying}</span>
                        <span style="display:inline-block;background:#eff6ff;color:#1d4ed8;
                            font-size:10px;font-weight:700;padding:2px 8px;border-radius:4px;
                            margin-left:8px;">{Lots} LOTS</span>
                    </div>
                    <span style="display:inline-block;background:{PnlBgC};color:{PnlColor};
                        font-size:13px;font-weight:600;padding:3px 10px;border-radius:6px;">
                        \u20b9{_FmtINR(Combo["pnl"])}</span>
                </div>
                <div style="margin-top:4px;font-size:12px;color:{SLATE};">
                    {" &middot; ".join(_html.escape(l) for l in Combo["legs"])}
                </div>
                <div style="margin-top:4px;">
                    <span style="font-size:12px;color:{SLATE};">Today: </span>
                    <b style="font-size:12px;color:{SwingColor};">\u20b9{_FmtINR(Combo["daily_swing"])}</b>
                </div>
            </td></tr>"""
    else:
        OptHtml += _EmptyRow("No open options")

    # ── Divider ──
    DivHtml = f'<tr><td style="padding:0 20px;"><div style="border-top:1px solid {BORDER};"></div></td></tr>'

    # ── Futures Trades ──
    FutTradesHtml = ""
    if D["futures_orders"]:
        FutTradesHtml += _SectionHeader("Futures Trades")
        FutTradesHtml += '<tr><td style="padding:0 12px;"><table style="width:100%;border-collapse:collapse;">'
        FutTradesHtml += _HeaderRow(["Instrument", "Action", "Qty", "Fill", "Broker", "Time"])
        for O in D["futures_orders"]:
            ActionColor = GREEN if O["action"] == "BUY" else RED
            Fill = f"{O['fill_price']:.2f}" if O.get("fill_price") else "N/A"
            FutTradesHtml += _DataRow([
                (str(O["instrument"]), NAVY, "600"),
                (O["action"], ActionColor, "600"),
                (str(O["qty"]), NAVY, ""),
                (Fill, NAVY, "600"),
                (O.get("broker", "")[:3], SLATE, ""),
                (O.get("time", ""), SLATE, ""),
            ])
        FutTradesHtml += '</table></td></tr>'
        FutTradesHtml += DivHtml

    # ── Options Trades ──
    OptTradesHtml = ""
    if D["options_orders"]:
        OptTradesHtml += _SectionHeader("Options Trades")
        OptTradesHtml += '<tr><td style="padding:0 12px;"><table style="width:100%;border-collapse:collapse;">'
        OptTradesHtml += _HeaderRow(["Contract", "Leg", "Action", "Qty", "Fill", "Time"])
        for O in D["options_orders"]:
            ActionColor = GREEN if O["action"] == "BUY" else RED
            Fill = f"{O['fill_price']:.2f}" if O.get("fill_price") else "N/A"
            OptTradesHtml += _DataRow([
                (O.get("contract", ""), NAVY, "600"),
                (O.get("leg", ""), NAVY, ""),
                (O["action"], ActionColor, "600"),
                (str(O["qty"]), NAVY, ""),
                (Fill, NAVY, "600"),
                (O.get("time", ""), SLATE, ""),
            ])
        OptTradesHtml += '</table></td></tr>'
        OptTradesHtml += DivHtml

    # ── Assemble ──
    BodyContent = f"""
    <table style="width:100%;border-collapse:collapse;" class="pnl-section">
        {StatsHtml}
        {FutHtml}
        {OptHtml}
        {DivHtml}
        {FutTradesHtml}
        {OptTradesHtml}
    </table>"""

    Now = datetime.now().strftime("%Y-%m-%d %H:%M IST")
    Html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<style>{DarkStyle}
    * {{ margin:0; padding:0; box-sizing:border-box; }}
</style></head>
<body class="pnl-body" style="background:{BG};margin:0;padding:0;font-family:-apple-system,
    BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
<div style="max-width:600px;margin:0 auto;padding:20px 12px;">
    <div class="pnl-wrap" style="background:#ffffff;border-radius:16px;overflow:hidden;
        border:1px solid {BORDER};box-shadow:0 4px 12px rgba(0,0,0,0.08);">
        {HeaderHtml}
        {BodyContent}
        <div class="pnl-footer" style="padding:14px;text-align:center;font-size:11px;color:{MUTED};
            border-top:1px solid {BORDER};">
            Auto-generated &bull; {Now}
        </div>
    </div>
</div></body></html>"""

    return Html


def _SectionHeader(Title):
    return (f'<tr><td colspan="99" style="padding:20px 20px 8px;font-size:11px;'
            f'font-weight:700;color:{SLATE};text-transform:uppercase;'
            f'letter-spacing:1.2px;">{_html.escape(Title)}</td></tr>')


def _EmptyRow(Text):
    return (f'<tr><td style="padding:16px 20px;text-align:center;font-size:13px;'
            f'color:{MUTED};font-style:italic;">{_html.escape(Text)}</td></tr>')


def _PositionRow(P):
    """Render one open futures position."""
    DirColor = GREEN if P["direction"] == "LONG" else RED
    DirBg = "#f0fdf4" if P["direction"] == "LONG" else "#fef2f2"
    PnlColor = _PnlColor(P["pnl"])
    PnlBgColor = _PnlBg(P["pnl"])
    SwingColor = _PnlColor(P["daily_swing"])
    LtpStr = f"{P['ltp']:.2f}" if P["ltp"] > 0 else "N/A"
    PrevCloseStr = f"{P['prev_close']:.2f}" if P["prev_close"] > 0 else "N/A"
    IsNew = P.get("is_new_today", False)
    NewBadge = (f'<span style="display:inline-block;background:#fef3c7;color:#92400e;'
                f'font-size:9px;font-weight:700;padding:2px 6px;border-radius:4px;'
                f'margin-left:6px;">NEW</span>') if IsNew else ""
    return f"""
    <tr><td style="padding:12px 20px;border-bottom:1px solid {BORDER};">
        <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
                <span style="font-size:14px;font-weight:700;color:{NAVY};">{_html.escape(P["instrument"])}</span>
                <span style="display:inline-block;background:{DirBg};color:{DirColor};
                    font-size:10px;font-weight:700;padding:2px 8px;border-radius:4px;
                    margin-left:8px;">{P["direction"]}</span>{NewBadge}
            </div>
            <span style="display:inline-block;background:{PnlBgColor};color:{PnlColor};
                font-size:13px;font-weight:600;padding:3px 10px;border-radius:6px;">
                \u20b9{_FmtINR(P["pnl"])}</span>
        </div>
        <div style="margin-top:6px;display:flex;gap:16px;flex-wrap:wrap;">
            <span style="font-size:12px;color:{SLATE};">Qty: <b style="color:{NAVY};">{int(P.get("lots", P["qty"]))}</b></span>
            <span style="font-size:12px;color:{SLATE};">Entry: <b style="color:{NAVY};">{P["avg_entry"]:.2f}</b></span>
            <span style="font-size:12px;color:{SLATE};">Prev Close: <b style="color:{NAVY};">{PrevCloseStr}</b></span>
            <span style="font-size:12px;color:{SLATE};">LTP: <b style="color:{NAVY};">{LtpStr}</b></span>
        </div>
        <div style="margin-top:4px;">
            <span style="font-size:12px;color:{SLATE};">Today: </span>
            <b style="font-size:12px;color:{SwingColor};">\u20b9{_FmtINR(P["daily_swing"])}</b>
        </div>
    </td></tr>"""


def _HeaderRow(Cols):
    Cells = "".join(
        f'<th style="padding:8px 12px;background:{BG};font-weight:600;font-size:12px;'
        f'color:{NAVY};text-align:left;border-bottom:1px solid {BORDER};">'
        f'{_html.escape(C)}</th>' for C in Cols
    )
    return f"<tr>{Cells}</tr>"


def _DataRow(Cells):
    """Cells = list of (text, color, weight)."""
    Parts = "".join(
        f'<td style="padding:8px 12px;font-size:12px;color:{C};'
        f'{"font-weight:" + W + ";" if W else ""}'
        f'border-bottom:1px solid {BORDER};">{_html.escape(T)}</td>'
        for T, C, W in Cells
    )
    return f'<tr class="pnl-row">{Parts}</tr>'


# ─── Main ────────────────────────────────────────────────────────

def GenerateDailyReport(DryRun=False, DateStr=None):
    if DateStr is None:
        Now = datetime.now()
        if Now.hour < 9:
            DateStr = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            Logger.info("Post-midnight, using previous trading day: %s", DateStr)
        else:
            DateStr = date.today().strftime("%Y-%m-%d")

    Logger.info("Generating report for %s (dry_run=%s)", DateStr, DryRun)

    with open(CONFIG_PATH) as f:
        FullConfig = json.load(f)

    # Fetch from brokers
    Positions = _FetchOpenPositions(FullConfig)
    FuturesOrders, OptionsOrders = _FetchTodayOrders(FullConfig)

    # Total unrealized P&L = sum of (LTP - Entry) across all positions
    TotalPnl = sum(P["pnl"] for P in Positions)
    # Daily swing = sum of (LTP - Prev Close) across all positions
    TotalDailySwing = sum(P["daily_swing"] for P in Positions)

    Logger.info("Total unrealized: %.2f | Daily swing: %.2f", TotalPnl, TotalDailySwing)

    # Log each position for verification
    for P in Positions:
        LotsStr = f" | lots={P['lots']}" if "lots" in P else ""
        NewStr = " | NEW_TODAY" if P.get("is_new_today") else ""
        Logger.info("  %s | %s | qty=%d%s | entry=%.2f | prev_close=%.2f | ltp=%.2f | pv=%.1f | pnl=%.2f | swing=%.2f%s",
                     P["instrument"], P["direction"], P["qty"], LotsStr,
                     P["avg_entry"], P["prev_close"], P["ltp"],
                     P["point_value"], P["pnl"], P["daily_swing"], NewStr)

    ReportData = {
        "date": DateStr,
        "positions": Positions,
        "total_pnl": TotalPnl,
        "total_daily_swing": TotalDailySwing,
        "position_count": len(Positions),
        "trade_count": len(FuturesOrders) + len(OptionsOrders),
        "futures_orders": FuturesOrders,
        "options_orders": OptionsOrders,
    }

    Html = _BuildReportHtml(ReportData)
    DateDisplay = datetime.strptime(DateStr, "%Y-%m-%d").strftime("%d %b %Y")
    Subject = f"Daily P&L Report | {DateDisplay} | \u20b9{_FmtINR(TotalDailySwing)}"

    if DryRun:
        print(Html)
        Logger.info("Dry run — HTML printed, email not sent")
    else:
        _SendEmail(Subject, Html)
        Logger.info("Report sent for %s", DateStr)


def main():
    Parser = argparse.ArgumentParser(description="Daily P&L Email Report")
    Parser.add_argument("--dry-run", action="store_true")
    Parser.add_argument("--date", type=str, default=None)
    Args = Parser.parse_args()

    try:
        GenerateDailyReport(DryRun=Args.dry_run, DateStr=Args.date)
    except Exception as e:
        Logger.error("Report failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
