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

from Server_Order_Handler import EstablishConnectionAngelAPI
from rollover_monitor import _LoadEmailConfig, _SendEmail, _EstablishKiteSession
from Directories import workInputRoot, ZerodhaInstrumentDirectory, AngelInstrumentDirectory

Logger = logging.getLogger("daily_pnl_report")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

CONFIG_PATH = Path(__file__).parent / "instrument_config.json"
STATE_FILE_PATH = Path(workInputRoot) / "v2_state.json"

# ─── Formatting Helpers ────────────────────────────────────────────

GREEN = "#16a34a"
RED = "#dc2626"
MUTED = "#94a3b8"
NAVY = "#0f172a"
SLATE = "#64748b"
BORDER = "#e2e8f0"
CARD_BG = "#ffffff"
BG = "#f8fafc"
HEADER_FROM = "#0f172a"
HEADER_TO = "#1e3a5f"


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
    if Amount > 0:
        return GREEN
    elif Amount < 0:
        return RED
    return MUTED


def _PnlBg(Amount):
    if Amount > 0:
        return "#f0fdf4"
    elif Amount < 0:
        return "#fef2f2"
    return "#f8fafc"


def _PnlBadge(Amount, Large=False):
    """Render a colored P&L badge."""
    Color = _PnlColor(Amount)
    Bg = _PnlBg(Amount)
    Size = "18px" if Large else "13px"
    Pad = "6px 14px" if Large else "3px 10px"
    Weight = "700" if Large else "600"
    return (f'<span style="display:inline-block;background:{Bg};color:{Color};'
            f'font-size:{Size};font-weight:{Weight};padding:{Pad};'
            f'border-radius:6px;letter-spacing:0.3px;">'
            f'\u20b9{_FmtINR(Amount)}</span>')


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


# ─── Broker Position Fetching ─────────────────────────────────────


def _IsIndexOption(Symbol):
    """Return True if symbol looks like an index option (NIFTY/SENSEX/BANKEX CE/PE)."""
    S = Symbol.upper()
    return any(S.startswith(P) for P in ("NIFTY", "SENSEX", "BANKEX", "BANKNIFTY")) and (
        S.endswith("CE") or S.endswith("PE"))


def _MatchPositionToInstrument(TradingSymbol, Exchange, Broker, Instruments):
    """Match a broker position's tradingsymbol to our instrument config.

    Uses CSV lookup (symbol → name), then falls back to prefix matching.
    Returns (InstrumentName, Config) or (None, None).
    """
    if Broker == "ZERODHA":
        try:
            Df = pd.read_csv(ZerodhaInstrumentDirectory, delimiter=",")
            Match = Df[(Df["symbol"] == TradingSymbol) & (Df["exch_seg"] == Exchange)]
            if not Match.empty:
                Name = Match.iloc[0]["name"]
                for InstName, Cfg in Instruments.items():
                    if InstName == Name and Cfg.get("exchange") == Exchange:
                        return InstName, Cfg
        except Exception as e:
            Logger.warning("CSV match failed for %s: %s", TradingSymbol, e)

    elif Broker == "ANGEL":
        try:
            Df = pd.read_csv(AngelInstrumentDirectory, delimiter=",", low_memory=False)
            Match = Df[(Df["symbol"] == TradingSymbol) & (Df["exch_seg"] == Exchange)]
            if not Match.empty:
                Name = Match.iloc[0]["name"]
                for InstName, Cfg in Instruments.items():
                    if InstName == Name and Cfg.get("exchange") == Exchange:
                        return InstName, Cfg
        except Exception as e:
            Logger.warning("CSV match failed for %s: %s", TradingSymbol, e)

    # Fallback: prefix match
    for InstName, Cfg in Instruments.items():
        if TradingSymbol.upper().startswith(InstName.upper()):
            if Cfg.get("exchange") == Exchange:
                return InstName, Cfg

    return None, None


def _FetchBrokerData(FullConfig):
    """Fetch all positions + daily M2M directly from broker APIs.

    Returns (OpenPositions, DailyMtm):
      - OpenPositions: list of dicts for display (instrument, qty, avg, ltp, unrealized, etc.)
      - DailyMtm: float — total daily P&L across all accounts from broker's own m2m/pnl fields
    """
    Instruments = FullConfig.get("instruments", {})
    Positions = []
    DailyMtm = 0.0

    # ── Kite (Zerodha) — MCX futures via YD6016 ──
    try:
        Kite = _EstablishKiteSession("YD6016")
        AllPositions = Kite.positions()
        # Day positions include ALL instruments that had activity today (even closed)
        # Each has 'm2m' = broker's daily mark-to-market P&L
        DayPositions = AllPositions.get("day", [])
        for Pos in DayPositions:
            if Pos.get("product") != "NRML":
                continue
            if _IsIndexOption(Pos.get("tradingsymbol", "")):
                continue
            M2m = float(Pos.get("m2m", 0) or Pos.get("pnl", 0) or 0)
            DailyMtm += M2m

        # Net positions for open display
        NetPositions = AllPositions.get("net", [])
        for Pos in NetPositions:
            Qty = Pos.get("quantity", 0)
            if Qty == 0 or Pos.get("product") != "NRML":
                continue
            Symbol = Pos.get("tradingsymbol", "")
            Exchange = Pos.get("exchange", "")
            if _IsIndexOption(Symbol):
                continue

            InstName, Cfg = _MatchPositionToInstrument(Symbol, Exchange, "ZERODHA", Instruments)
            if not InstName:
                Logger.warning("Unmatched Kite position: %s (%s)", Symbol, Exchange)
                continue

            AvgPrice = float(Pos.get("average_price", 0))
            Ltp = float(Pos.get("last_price", 0))
            PV = Cfg.get("point_value", 1)
            Unrealized = float(Pos.get("unrealised", 0) or 0)
            # If broker didn't give us unrealised, compute it
            if Unrealized == 0 and AvgPrice > 0 and Ltp > 0:
                if Qty > 0:
                    Unrealized = (Ltp - AvgPrice) * abs(Qty) * PV
                else:
                    Unrealized = (AvgPrice - Ltp) * abs(Qty) * PV
            M2m = float(Pos.get("m2m", 0) or 0)

            Positions.append({
                "instrument": InstName,
                "confirmed_qty": Qty,
                "avg_entry_price": AvgPrice,
                "point_value": PV,
                "ltp": Ltp,
                "unrealized_pnl": round(Unrealized, 2),
                "day_m2m": round(M2m, 2),
                "direction": "LONG" if Qty > 0 else "SHORT",
                "tradingsymbol": Symbol,
                "broker": "ZERODHA",
            })
        Logger.info("Kite YD6016: %d open positions, day M2M from %d day positions",
                     sum(1 for p in Positions if p["broker"] == "ZERODHA"), len(DayPositions))
    except Exception as e:
        Logger.error("Kite YD6016 positions fetch failed: %s", e)

    # ── Angel (NCDEX) ──
    AngelMtm = 0.0
    try:
        SmartApi = EstablishConnectionAngelAPI({"User": "AABM826021"})
        RawResponse = SmartApi.position()
        RawPositions = RawResponse.get("data", []) if isinstance(RawResponse, dict) else []
        if RawPositions is None:
            RawPositions = []

        # Log available keys from first position so we can verify fields
        if RawPositions:
            Logger.info("Angel position keys: %s", list(RawPositions[0].keys()))

        for Pos in RawPositions:
            ProdType = Pos.get("producttype", "")
            if ProdType != "CARRYFORWARD":
                continue

            # Try broker's own P&L field
            PosPnl = float(Pos.get("pnl", 0) or Pos.get("realised", 0) or 0)
            PosUnrealised = float(Pos.get("unrealised", 0) or 0)
            AngelMtm += PosPnl + PosUnrealised

            Qty = int(Pos.get("netqty", 0))
            if Qty == 0:
                continue

            Symbol = Pos.get("tradingsymbol", "")
            Exchange = Pos.get("exchange", "")

            InstName, Cfg = _MatchPositionToInstrument(Symbol, Exchange, "ANGEL", Instruments)
            if not InstName:
                Logger.warning("Unmatched Angel position: %s (%s)", Symbol, Exchange)
                continue

            Ltp = float(Pos.get("ltp", 0))
            if Qty > 0:
                AvgPrice = float(Pos.get("buyavgprice", 0) or Pos.get("avgnetprice", 0) or 0)
            else:
                AvgPrice = float(Pos.get("sellavgprice", 0) or Pos.get("avgnetprice", 0) or 0)
            PV = Cfg.get("point_value", 1)
            if AvgPrice > 0 and Ltp > 0:
                if Qty > 0:
                    Unrealized = (Ltp - AvgPrice) * abs(Qty) * PV
                else:
                    Unrealized = (AvgPrice - Ltp) * abs(Qty) * PV
            else:
                Unrealized = 0

            Positions.append({
                "instrument": InstName,
                "confirmed_qty": Qty,
                "avg_entry_price": AvgPrice,
                "point_value": PV,
                "ltp": Ltp,
                "unrealized_pnl": round(Unrealized, 2),
                "day_m2m": round(PosPnl + PosUnrealised, 2),
                "direction": "LONG" if Qty > 0 else "SHORT",
                "tradingsymbol": Symbol,
                "broker": "ANGEL",
            })
        DailyMtm += AngelMtm
        Logger.info("Angel: %d open positions, Angel M2M=%.2f",
                     sum(1 for p in Positions if p["broker"] == "ANGEL"), AngelMtm)
    except Exception as e:
        Logger.error("Angel positions fetch failed: %s", e)

    # ── Options from Kite OFS653 (NFO/BFO) ──
    try:
        Kite = _EstablishKiteSession("OFS653")
        AllPositions = Kite.positions()

        # Day positions for M2M (includes closed options)
        DayPositions = AllPositions.get("day", [])
        OptMtm = 0.0
        for Pos in DayPositions:
            if Pos.get("product") != "NRML":
                continue
            M2m = float(Pos.get("m2m", 0) or Pos.get("pnl", 0) or 0)
            OptMtm += M2m
        DailyMtm += OptMtm

        # Net positions for open display
        NetPositions = AllPositions.get("net", [])
        for Pos in NetPositions:
            Qty = Pos.get("quantity", 0)
            if Qty == 0 or Pos.get("product", "") != "NRML":
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
            InstName = f"{Underlying}_OPT_{Leg}"

            AvgPrice = float(Pos.get("average_price", 0))
            Ltp = float(Pos.get("last_price", 0))
            Unrealized = float(Pos.get("unrealised", 0) or 0)
            if Unrealized == 0 and AvgPrice > 0 and Ltp > 0:
                Unrealized = (AvgPrice - Ltp) * abs(Qty) * 1.0
            M2m = float(Pos.get("m2m", 0) or 0)

            Positions.append({
                "instrument": InstName,
                "confirmed_qty": Qty,
                "avg_entry_price": AvgPrice,
                "point_value": 1.0,
                "ltp": Ltp,
                "unrealized_pnl": round(Unrealized, 2),
                "day_m2m": round(M2m, 2),
                "direction": "SHORT",
                "tradingsymbol": Symbol,
                "broker": "ZERODHA",
            })
        Logger.info("Kite OFS653: options M2M=%.2f", OptMtm)
    except Exception as e:
        Logger.warning("Options position fetch failed: %s", e)

    Logger.info("Total broker positions: %d, Total daily M2M: %.2f", len(Positions), DailyMtm)
    return Positions, DailyMtm


# ─── Broker Order Fetching ────────────────────────────────────────


def _FetchBrokerOrders(FullConfig):
    """Fetch today's completed orders from broker APIs.

    Returns (FuturesOrders, OptionsOrders) — lists of dicts with unified schema.
    """
    Instruments = FullConfig.get("instruments", {})
    FuturesOrders = []
    OptionsOrders = []

    # ── Kite orders — YD6016 (MCX futures) + OFS653 (options) ──
    for KiteUser in ["YD6016", "OFS653"]:
        try:
            Kite = _EstablishKiteSession(KiteUser)
            AllOrders = Kite.orders()
            CompletedCount = 0
            for O in AllOrders:
                if O.get("status") != "COMPLETE":
                    continue
                if O.get("product") != "NRML":
                    continue
                CompletedCount += 1
                Symbol = O.get("tradingsymbol", "")
                Exchange = O.get("exchange", "")
                Qty = O.get("filled_quantity", 0) or O.get("quantity", 0)
                AvgPrice = float(O.get("average_price", 0))
                Action = O.get("transaction_type", "")  # BUY/SELL
                OrderTime = O.get("order_timestamp", "")
                if isinstance(OrderTime, datetime):
                    OrderTime = OrderTime.strftime("%H:%M")
                else:
                    OrderTime = str(OrderTime)[-8:-3] if len(str(OrderTime)) > 8 else ""

                if _IsIndexOption(Symbol):
                    S = Symbol.upper()
                    if S.startswith("NIFTY"):
                        Underlying = "NIFTY"
                    elif S.startswith("SENSEX"):
                        Underlying = "SENSEX"
                    elif S.startswith("BANKNIFTY"):
                        Underlying = "BANKNIFTY"
                    else:
                        Underlying = Symbol[:6]
                    Leg = "CE" if S.endswith("CE") else "PE"
                    OptionsOrders.append({
                        "underlying": Underlying,
                        "leg": Leg,
                        "contract": Symbol,
                        "action": Action,
                        "qty": Qty,
                        "fill_price": AvgPrice,
                        "time": OrderTime,
                        "broker": "ZERODHA",
                    })
                else:
                    InstName, _ = _MatchPositionToInstrument(Symbol, Exchange, "ZERODHA", Instruments)
                    FuturesOrders.append({
                        "instrument": InstName or Symbol,
                        "action": Action,
                        "qty": Qty,
                        "fill_price": AvgPrice,
                        "time": OrderTime,
                        "broker": "ZERODHA",
                    })
            Logger.info("Kite %s: %d completed orders", KiteUser, CompletedCount)
        except Exception as e:
            Logger.error("Kite %s orders fetch failed: %s", KiteUser, e)

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
            Action = O.get("transactiontype", "")  # BUY/SELL
            OrderTime = O.get("updatetime", "") or O.get("ordertime", "")
            if len(str(OrderTime)) > 5:
                OrderTime = str(OrderTime)[-8:-3]

            InstName, _ = _MatchPositionToInstrument(Symbol, Exchange, "ANGEL", Instruments)
            FuturesOrders.append({
                "instrument": InstName or Symbol,
                "action": Action,
                "qty": Qty,
                "fill_price": AvgPrice,
                "time": OrderTime,
                "broker": "ANGEL",
            })
        Logger.info("Angel: fetched %d completed orders", len([o for o in RawOrders if str(o.get("status", "")).lower() == "complete"]))
    except Exception as e:
        Logger.error("Angel orders fetch failed: %s", e)

    return FuturesOrders, OptionsOrders


# ─── HTML Builder ─────────────────────────────────────────────────

import html as _html


def _SectionHeader(Title):
    return (f'<tr><td colspan="99" style="padding:20px 20px 8px;font-size:11px;'
            f'font-weight:700;color:{SLATE};text-transform:uppercase;'
            f'letter-spacing:1.2px;">{_html.escape(Title)}</td></tr>')


def _Divider():
    return f'<tr><td colspan="99" style="padding:0 20px;"><div style="border-top:1px solid {BORDER};"></div></td></tr>'


def _StatBox(Label, Value, Color=None):
    """A single stat inside the 3-column summary strip."""
    C = Color or NAVY
    return (f'<td style="width:33.3%;text-align:center;padding:16px 8px;">'
            f'<div style="font-size:11px;font-weight:600;color:{SLATE};text-transform:uppercase;'
            f'letter-spacing:0.8px;margin-bottom:4px;">{_html.escape(Label)}</div>'
            f'<div style="font-size:17px;font-weight:700;color:{C};">\u20b9{Value}</div>'
            f'</td>')


def _TradeRow(Cells, IsHeader=False):
    """Render a table row for trades. Cells = list of (text, align, width)."""
    Tag = "th" if IsHeader else "td"
    Bg = f"background:{BG};" if IsHeader else ""
    Weight = "font-weight:600;" if IsHeader else ""
    Parts = []
    for Text, Align, Width in Cells:
        Parts.append(
            f'<{Tag} style="padding:8px 12px;{Bg}{Weight}font-size:12px;color:{NAVY};'
            f'text-align:{Align};width:{Width};border-bottom:1px solid {BORDER};">'
            f'{_html.escape(str(Text))}</{Tag}>'
        )
    return "<tr>" + "".join(Parts) + "</tr>"


def _PositionBlock(Instrument, Direction, Qty, AvgEntry, Ltp, Unrealized, Change):
    """Render a single open position as a mini-card row."""
    DirColor = GREEN if Direction == "LONG" else RED
    DirBg = "#f0fdf4" if Direction == "LONG" else "#fef2f2"
    LtpStr = f"{Ltp:.2f}" if Ltp > 0 else "N/A"
    if Change is not None:
        TodayStr = f'<span style="font-size:12px;color:{SLATE};">Today: <b style="color:{_PnlColor(Change)};">\u20b9{_FmtINR(Change)}</b></span>'
    else:
        TodayStr = f'<span style="font-size:12px;color:{MUTED};">Today: N/A</span>'
    return f"""
    <tr><td style="padding:12px 20px;border-bottom:1px solid {BORDER};">
        <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
                <span style="font-size:14px;font-weight:700;color:{NAVY};">{_html.escape(Instrument)}</span>
                <span style="display:inline-block;background:{DirBg};color:{DirColor};
                    font-size:10px;font-weight:700;padding:2px 8px;border-radius:4px;
                    margin-left:8px;text-transform:uppercase;">{Direction}</span>
            </div>
            <div style="text-align:right;">{_PnlBadge(Unrealized)}</div>
        </div>
        <div style="margin-top:6px;display:flex;gap:24px;">
            <span style="font-size:12px;color:{SLATE};">Qty: <b style="color:{NAVY};">{abs(Qty)}</b></span>
            <span style="font-size:12px;color:{SLATE};">Entry: <b style="color:{NAVY};">{AvgEntry:.2f}</b></span>
            <span style="font-size:12px;color:{SLATE};">LTP: <b style="color:{NAVY};">{LtpStr}</b></span>
            {TodayStr}
        </div>
    </td></tr>"""


def _ClosedRow(Instrument, Label, Qty, Entry, Exit, Pnl):
    """Render a closed position row."""
    Arrow = "\u2192"
    return f"""
    <tr><td style="padding:10px 20px;border-bottom:1px solid {BORDER};">
        <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
                <span style="font-size:13px;font-weight:600;color:{NAVY};">{_html.escape(Instrument)}</span>
                <span style="font-size:11px;color:{SLATE};margin-left:6px;">{_html.escape(Label)}</span>
            </div>
            <div>{_PnlBadge(Pnl)}</div>
        </div>
        <div style="margin-top:4px;font-size:12px;color:{SLATE};">
            {Qty} lots &middot; {Entry:.2f} {Arrow} {Exit:.2f}
        </div>
    </td></tr>"""


def _EmptyRow(Text):
    return (f'<tr><td style="padding:16px 20px;text-align:center;font-size:13px;'
            f'color:{MUTED};font-style:italic;">{_html.escape(Text)}</td></tr>')


def _BuildReportHtml(Data):
    """Build the full P&L report HTML."""
    D = Data
    DateDisplay = datetime.strptime(D["date"], "%Y-%m-%d").strftime("%d %b %Y")
    TotalPnl = D["daily_mtm"]
    HeroColor = _PnlColor(TotalPnl)
    HeroBg = "#0d3320" if TotalPnl >= 0 else "#3b1119"

    DarkStyle = """
    @media (prefers-color-scheme: dark) {
        .pnl-body { background-color: #0d1117 !important; }
        .pnl-wrap { background-color: #161b22 !important; border-color: #30363d !important; }
        .pnl-section { background-color: #161b22 !important; }
        .pnl-stat-label { color: #8b949e !important; }
        .pnl-stat-value { color: #e6edf3 !important; }
        .pnl-row td, .pnl-row th { border-color: #21262d !important; color: #e6edf3 !important; }
        .pnl-section-title td { color: #8b949e !important; }
        .pnl-divider div { border-color: #21262d !important; }
        .pnl-footer { color: #484f58 !important; border-color: #21262d !important; }
    }
    """

    # ── Header with hero P&L ──
    HeaderHtml = f"""
    <div style="background:linear-gradient(135deg,{HEADER_FROM},{HEADER_TO});
        padding:28px 24px 20px;text-align:center;">
        <div style="font-size:12px;font-weight:600;color:rgba(255,255,255,0.5);
            text-transform:uppercase;letter-spacing:1.5px;">Daily P&L Report</div>
        <div style="font-size:15px;color:rgba(255,255,255,0.8);margin-top:4px;">{DateDisplay}</div>
        <div style="margin-top:14px;display:inline-block;background:{HeroBg};
            padding:10px 28px;border-radius:10px;">
            <span style="font-size:28px;font-weight:800;color:{HeroColor};
                letter-spacing:0.5px;">\u20b9{_FmtINR(TotalPnl)}</span>
        </div>
        <div style="font-size:11px;color:rgba(255,255,255,0.4);margin-top:6px;">
            TOTAL DAILY P&L</div>
    </div>"""

    # ── Summary strip: Daily MTM prominent, trades + total unrealized secondary ──
    MtmColor = _PnlColor(D["daily_mtm"])
    MtmBg = _PnlBg(D["daily_mtm"])
    SummaryHtml = f"""
    <table style="width:100%;border-collapse:collapse;border-bottom:1px solid {BORDER};">
        <tr>
            <td style="width:40%;text-align:center;padding:18px 8px;background:{MtmBg};
                border-right:1px solid {BORDER};">
                <div style="font-size:10px;font-weight:700;color:{SLATE};text-transform:uppercase;
                    letter-spacing:1px;margin-bottom:6px;">Daily MTM</div>
                <div style="font-size:22px;font-weight:800;color:{MtmColor};">\u20b9{_FmtINR(D["daily_mtm"])}</div>
            </td>
            <td style="width:60%;padding:12px 0;">
                <table style="width:100%;border-collapse:collapse;">
                    <tr>
                        {_StatBox("Trades", str(D["trade_count"]), NAVY)}
                        {_StatBox("Unrealized", _FmtINR(D["total_unrealized"]), _PnlColor(D["total_unrealized"]))}
                    </tr>
                </table>
            </td>
        </tr>
    </table>"""

    # ── Quick stats row ──
    QuickStats = f"""
    <table style="width:100%;border-collapse:collapse;border-bottom:1px solid {BORDER};">
        <tr>
            <td style="width:33.3%;text-align:center;padding:12px 8px;">
                <span style="font-size:12px;color:{SLATE};">Trades</span>
                <span style="font-size:14px;font-weight:700;color:{NAVY};margin-left:6px;">{D["trade_count"]}</span>
            </td>
            <td style="width:33.3%;text-align:center;padding:12px 8px;border-left:1px solid {BORDER};border-right:1px solid {BORDER};">
                <span style="font-size:12px;color:{SLATE};">Open</span>
                <span style="font-size:14px;font-weight:700;color:{NAVY};margin-left:6px;">{D["open_count"]}</span>
            </td>
            <td style="width:33.3%;text-align:center;padding:12px 8px;">
                <span style="font-size:12px;color:{SLATE};">Capital</span>
                <span style="font-size:14px;font-weight:700;color:{NAVY};margin-left:6px;">\u20b9{_FmtPlain(D["base_capital"])}</span>
            </td>
        </tr>
    </table>"""

    # ── Futures Trades ──
    FutTradesHtml = ""
    if D["futures_orders"]:
        FutTradesHtml += _SectionHeader("Futures Trades")
        FutTradesHtml += '<tr><td style="padding:0 12px;"><table style="width:100%;border-collapse:collapse;">'
        FutTradesHtml += _TradeRow([
            ("Instrument", "left", "30%"), ("Action", "center", "12%"),
            ("Qty", "center", "10%"), ("Fill", "right", "22%"),
            ("Broker", "center", "12%"), ("Time", "right", "14%"),
        ], IsHeader=True)
        for O in D["futures_orders"]:
            Fill = f"{O['fill_price']:.2f}" if O.get("fill_price") else "N/A"
            Time = O.get("time", "")
            Broker = O.get("broker", "")[:3]  # ZER / ANG
            ActionColor = GREEN if O["action"] == "BUY" else RED
            FutTradesHtml += f"""<tr class="pnl-row">
                <td style="padding:8px 12px;font-size:12px;font-weight:600;color:{NAVY};
                    border-bottom:1px solid {BORDER};width:30%;">{_html.escape(str(O['instrument']))}</td>
                <td style="padding:8px 12px;font-size:12px;font-weight:600;color:{ActionColor};
                    text-align:center;border-bottom:1px solid {BORDER};width:12%;">{O['action']}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:center;
                    border-bottom:1px solid {BORDER};width:10%;">{O['qty']}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:right;
                    font-weight:600;border-bottom:1px solid {BORDER};width:22%;">{Fill}</td>
                <td style="padding:8px 12px;font-size:12px;color:{SLATE};text-align:center;
                    border-bottom:1px solid {BORDER};width:12%;">{Broker}</td>
                <td style="padding:8px 12px;font-size:12px;color:{SLATE};text-align:right;
                    border-bottom:1px solid {BORDER};width:14%;">{Time}</td>
            </tr>"""
        FutTradesHtml += '</table></td></tr>'
        FutTradesHtml += _Divider()

    # ── Options Trades ──
    OptTradesHtml = ""
    if D["options_orders"]:
        OptTradesHtml += _SectionHeader("Options Trades")
        OptTradesHtml += '<tr><td style="padding:0 12px;"><table style="width:100%;border-collapse:collapse;">'
        OptTradesHtml += _TradeRow([
            ("Contract", "left", "35%"), ("Leg", "center", "10%"),
            ("Action", "center", "12%"), ("Qty", "center", "10%"),
            ("Fill", "right", "18%"), ("Time", "right", "15%"),
        ], IsHeader=True)
        for O in D["options_orders"]:
            Fill = f"{O['fill_price']:.2f}" if O.get("fill_price") else "N/A"
            Time = O.get("time", "")
            ActionColor = GREEN if O["action"] == "BUY" else RED
            OptTradesHtml += f"""<tr class="pnl-row">
                <td style="padding:8px 12px;font-size:12px;font-weight:600;color:{NAVY};
                    border-bottom:1px solid {BORDER};width:35%;">{_html.escape(O.get('contract', O.get('underlying','')))}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:center;
                    border-bottom:1px solid {BORDER};width:10%;">{O.get('leg','')}</td>
                <td style="padding:8px 12px;font-size:12px;font-weight:600;color:{ActionColor};
                    text-align:center;border-bottom:1px solid {BORDER};width:12%;">{O['action']}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:center;
                    border-bottom:1px solid {BORDER};width:10%;">{O['qty']}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:right;
                    font-weight:600;border-bottom:1px solid {BORDER};width:18%;">{Fill}</td>
                <td style="padding:8px 12px;font-size:12px;color:{SLATE};text-align:right;
                    border-bottom:1px solid {BORDER};width:15%;">{Time}</td>
            </tr>"""
        OptTradesHtml += '</table></td></tr>'
        OptTradesHtml += _Divider()

    # ── Open Futures Positions ──
    FuturesPos = [P for P in D["unrealized_positions"] if "_OPT_" not in P["instrument"]]
    OpenFutHtml = _SectionHeader("Open Futures")
    if FuturesPos:
        for P in FuturesPos:
            DayChange = P.get("day_m2m", 0)
            OpenFutHtml += _PositionBlock(
                P["instrument"], P["direction"], P["confirmed_qty"],
                P["avg_entry_price"], P["ltp"], P["unrealized_pnl"], DayChange,
            )
    else:
        OpenFutHtml += _EmptyRow("No open futures positions")

    # ── Open Options Positions ──
    OptionsPos = [P for P in D["unrealized_positions"] if "_OPT_" in P["instrument"]]
    OpenOptHtml = _SectionHeader("Open Options")
    if OptionsPos:
        ByUnderlying = defaultdict(lambda: {"unrealized": 0, "day_m2m": 0, "legs": []})
        for P in OptionsPos:
            Parts = P["instrument"].split("_OPT_")
            Underlying, Leg = Parts[0], Parts[1] if len(Parts) > 1 else "?"
            ByUnderlying[Underlying]["unrealized"] += P["unrealized_pnl"]
            ByUnderlying[Underlying]["day_m2m"] += P.get("day_m2m", 0)
            LtpStr = f"{P['ltp']:.2f}" if P["ltp"] > 0 else "N/A"
            ByUnderlying[Underlying]["legs"].append(f"{Leg}: {P['avg_entry_price']:.1f} \u2192 {LtpStr}")

        StateInfo = {}
        try:
            if STATE_FILE_PATH.exists():
                with open(STATE_FILE_PATH) as f:
                    StateInfo = json.load(f)
        except Exception:
            pass

        for Underlying, Combo in ByUnderlying.items():
            DayM2m = Combo["day_m2m"]
            TodayLine = f'<b style="color:{_PnlColor(DayM2m)};">\u20b9{_FmtINR(DayM2m)}</b>'
            Lots = StateInfo.get(Underlying, {}).get("activeLots", "?")
            Contracts = StateInfo.get(Underlying, {}).get("activeContracts", [])
            OpenOptHtml += f"""
            <tr><td style="padding:12px 20px;border-bottom:1px solid {BORDER};">
                <div style="display:flex;justify-content:space-between;align-items:center;">
                    <div>
                        <span style="font-size:14px;font-weight:700;color:{NAVY};">{Underlying}</span>
                        <span style="display:inline-block;background:#eff6ff;color:#1d4ed8;
                            font-size:10px;font-weight:700;padding:2px 8px;border-radius:4px;
                            margin-left:8px;">{Lots} LOTS</span>
                    </div>
                    <div>{_PnlBadge(Combo["unrealized"])}</div>
                </div>
                <div style="margin-top:4px;font-size:12px;color:{SLATE};">
                    {" &middot; ".join(_html.escape(l) for l in Combo["legs"])}
                </div>
                <div style="margin-top:2px;font-size:11px;color:{SLATE};">
                    {", ".join(_html.escape(c) for c in Contracts)}
                </div>
                <div style="margin-top:4px;font-size:12px;">
                    <span style="color:{SLATE};">Today:</span>
                    {TodayLine}
                </div>
            </td></tr>"""
    else:
        # Fallback: show from state file even without cost basis
        try:
            if STATE_FILE_PATH.exists():
                with open(STATE_FILE_PATH) as f:
                    StateInfo = json.load(f)
                for Ul, Us in StateInfo.items():
                    if Us.get("activeLots", 0) > 0:
                        Contracts = ", ".join(Us.get("activeContracts", []))
                        OpenOptHtml += f"""
                        <tr><td style="padding:12px 20px;border-bottom:1px solid {BORDER};">
                            <span style="font-size:14px;font-weight:700;color:{NAVY};">{Ul}</span>
                            <span style="display:inline-block;background:#eff6ff;color:#1d4ed8;
                                font-size:10px;font-weight:700;padding:2px 8px;border-radius:4px;
                                margin-left:8px;">{Us['activeLots']} LOTS</span>
                            <div style="margin-top:4px;font-size:12px;color:{SLATE};">{Contracts}</div>
                        </td></tr>"""
                        break
                else:
                    OpenOptHtml += _EmptyRow("No open options positions")
            else:
                OpenOptHtml += _EmptyRow("No open options positions")
        except Exception:
            OpenOptHtml += _EmptyRow("No open options positions")

    # ── Capital & Unrealized ──
    UnrealizedPnl = D["total_unrealized"]
    UnrealizedSign = "+" if UnrealizedPnl >= 0 else ""
    CapHtml = _SectionHeader("Capital Overview")
    CapHtml += f"""
    <tr><td style="padding:8px 20px 16px;">
        <table style="width:100%;border-collapse:collapse;">
            <tr>
                <td style="padding:6px 0;font-size:13px;color:{SLATE};">Base Capital</td>
                <td style="padding:6px 0;font-size:13px;font-weight:600;color:{NAVY};text-align:right;">
                    \u20b9{_FmtPlain(D["base_capital"])}</td>
            </tr>
            <tr>
                <td style="padding:6px 0;font-size:13px;color:{SLATE};">Total Unrealized P&L</td>
                <td style="padding:6px 0;font-size:13px;font-weight:600;color:{_PnlColor(UnrealizedPnl)};text-align:right;">
                    {UnrealizedSign}\u20b9{_FmtPlain(UnrealizedPnl)}</td>
            </tr>
        </table>
    </td></tr>"""

    # ── Assemble — MTM / open positions first, then trades ──
    BodyContent = f"""
    <table style="width:100%;border-collapse:collapse;" class="pnl-section">
        {SummaryHtml}
        {QuickStats}
        {OpenFutHtml}
        {OpenOptHtml}
        {_Divider()}
        {FutTradesHtml}
        {OptTradesHtml}
        {CapHtml}
    </table>"""

    Now = datetime.now().strftime("%Y-%m-%d %H:%M IST")
    Html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<meta name="supported-color-schemes" content="light dark">
<style>{DarkStyle}
    * {{ margin:0; padding:0; box-sizing:border-box; }}
</style></head>
<body class="pnl-body" style="background:{BG};margin:0;padding:0;font-family:-apple-system,
    BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
<div style="max-width:600px;margin:0 auto;padding:20px 12px;">
    <div class="pnl-wrap" style="background:{CARD_BG};border-radius:16px;overflow:hidden;
        border:1px solid {BORDER};box-shadow:0 4px 12px rgba(0,0,0,0.08);">
        {HeaderHtml}
        {BodyContent}
        <div class="pnl-footer" style="padding:14px;text-align:center;font-size:11px;color:{MUTED};
            border-top:1px solid {BORDER};">
            Daily P&L Report &bull; Auto-generated &bull; {Now}
        </div>
    </div>
</div></body></html>"""

    return Html


# ─── Main Report Logic ────────────────────────────────────────────


def GenerateDailyReport(DryRun=False, DateStr=None):
    """Generate and send the daily P&L report."""

    if DateStr is None:
        # If running after midnight but before market open (09:00 IST),
        # the broker still has the previous session's data — use yesterday's date
        Now = datetime.now()
        IstHour = Now.hour  # Server runs in IST
        if IstHour < 9:
            DateStr = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            Logger.info("Running after midnight, using previous trading day: %s", DateStr)
        else:
            DateStr = date.today().strftime("%Y-%m-%d")

    Logger.info("Generating daily P&L report for %s (dry_run=%s)", DateStr, DryRun)

    with open(CONFIG_PATH) as f:
        FullConfig = json.load(f)

    BaseCapital = FullConfig["account"]["base_capital"]

    # ── Fetch everything from brokers ──
    BrokerPositions, DailyMtm = _FetchBrokerData(FullConfig)
    FuturesOrders, OptionsOrders = _FetchBrokerOrders(FullConfig)
    TradeCount = len(FuturesOrders) + len(OptionsOrders)

    # Total unrealized across all open positions = embedded P&L
    TotalUnrealized = sum(P["unrealized_pnl"] for P in BrokerPositions)

    TotalDailyPnl = DailyMtm
    OpenCount = len([p for p in BrokerPositions if "_OPT_" not in p["instrument"]])

    ReportData = {
        "date": DateStr,
        "trade_count": TradeCount,
        "open_count": OpenCount,
        "daily_mtm": DailyMtm,
        "total_unrealized": TotalUnrealized,
        "base_capital": BaseCapital,
        "futures_orders": FuturesOrders,
        "options_orders": OptionsOrders,
        "unrealized_positions": BrokerPositions,
    }

    Html = _BuildReportHtml(ReportData)
    DateDisplay = datetime.strptime(DateStr, "%Y-%m-%d").strftime("%d %b %Y")
    Subject = f"Daily P&L Report | {DateDisplay} | \u20b9{_FmtINR(TotalDailyPnl)}"

    if DryRun:
        print(Html)
        Logger.info("Dry run — HTML printed to stdout, email not sent")
    else:
        _SendEmail(Subject, Html)
        Logger.info("Daily P&L report sent for %s", DateStr)

    return {
        "daily_mtm": DailyMtm,
        "total_daily_pnl": TotalDailyPnl,
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
