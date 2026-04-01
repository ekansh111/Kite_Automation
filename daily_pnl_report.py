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
from rollover_monitor import _LoadEmailConfig, _SendEmail
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
            <span style="font-size:12px;color:{SLATE};">Today: <b style="color:{_PnlColor(Change)};">\u20b9{_FmtINR(Change)}</b></span>
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
    TotalPnl = D["realized_total"] + D["unrealized_change"]
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

    # ── Summary strip: MTM prominent, realized + cumulative secondary ──
    MtmColor = _PnlColor(D["unrealized_change"])
    MtmBg = _PnlBg(D["unrealized_change"])
    SummaryHtml = f"""
    <table style="width:100%;border-collapse:collapse;border-bottom:1px solid {BORDER};">
        <tr>
            <td style="width:40%;text-align:center;padding:18px 8px;background:{MtmBg};
                border-right:1px solid {BORDER};">
                <div style="font-size:10px;font-weight:700;color:{SLATE};text-transform:uppercase;
                    letter-spacing:1px;margin-bottom:6px;">MTM Change</div>
                <div style="font-size:22px;font-weight:800;color:{MtmColor};">\u20b9{_FmtINR(D["unrealized_change"])}</div>
            </td>
            <td style="width:60%;padding:12px 0;">
                <table style="width:100%;border-collapse:collapse;">
                    <tr>
                        {_StatBox("Realized", _FmtINR(D["realized_total"]), _PnlColor(D["realized_total"]))}
                        {_StatBox("Cumulative", _FmtINR(D["cumulative_pnl"]), _PnlColor(D["cumulative_pnl"]))}
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
                <span style="font-size:14px;font-weight:700;color:{NAVY};margin-left:6px;">\u20b9{_FmtPlain(D["effective_capital"])}</span>
            </td>
        </tr>
    </table>"""

    # ── Closed Positions ──
    ClosedHtml = ""
    if D["realized_rows"]:
        ClosedHtml += _SectionHeader("Closed Positions")
        # Group options by underlying
        Combined = defaultdict(lambda: {"pnl": 0, "legs": []})
        for R in D["realized_rows"]:
            Inst = R["instrument"]
            if "_OPT_" in Inst:
                Underlying = Inst.split("_OPT_")[0]
                Leg = Inst.split("_OPT_")[1]
                Combined[Underlying]["pnl"] += R["pnl_inr"]
                Combined[Underlying]["legs"].append((Leg, R["entry_price"], R["exit_price"], R["pnl_inr"]))
            else:
                ClosedHtml += _ClosedRow(Inst, "Futures", R["close_qty"], R["entry_price"], R["exit_price"], R["pnl_inr"])

        for Underlying, Combo in Combined.items():
            LegStr = " + ".join(L[0] for L in Combo["legs"])
            AvgEntry = sum(L[1] for L in Combo["legs"]) / len(Combo["legs"]) if Combo["legs"] else 0
            AvgExit = sum(L[2] for L in Combo["legs"]) / len(Combo["legs"]) if Combo["legs"] else 0
            ClosedHtml += _ClosedRow(Underlying, f"Straddle ({LegStr})", "", AvgEntry, AvgExit, Combo["pnl"])

        ClosedHtml += _Divider()

    # ── Futures Trades ──
    FutTradesHtml = ""
    if D["futures_orders"]:
        FutTradesHtml += _SectionHeader("Futures Trades")
        FutTradesHtml += '<tr><td style="padding:0 12px;"><table style="width:100%;border-collapse:collapse;">'
        FutTradesHtml += _TradeRow([
            ("Instrument", "left", "30%"), ("Action", "center", "15%"),
            ("Qty", "center", "10%"), ("Fill", "right", "20%"),
            ("Slip", "right", "10%"), ("Time", "right", "15%"),
        ], IsHeader=True)
        for O in D["futures_orders"]:
            Fill = f"{O['fill_price']:.2f}" if O.get("fill_price") else "N/A"
            Slip = f"{O['slippage']:.2f}" if O.get("slippage") else "-"
            Time = _FmtTime(O.get("created_at", ""))
            ActionColor = GREEN if O["action"] == "BUY" else RED
            FutTradesHtml += f"""<tr class="pnl-row">
                <td style="padding:8px 12px;font-size:12px;font-weight:600;color:{NAVY};
                    border-bottom:1px solid {BORDER};width:30%;">{O['instrument']}</td>
                <td style="padding:8px 12px;font-size:12px;font-weight:600;color:{ActionColor};
                    text-align:center;border-bottom:1px solid {BORDER};width:15%;">{O['action']}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:center;
                    border-bottom:1px solid {BORDER};width:10%;">{O['qty']}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:right;
                    font-weight:600;border-bottom:1px solid {BORDER};width:20%;">{Fill}</td>
                <td style="padding:8px 12px;font-size:12px;color:{SLATE};text-align:right;
                    border-bottom:1px solid {BORDER};width:10%;">{Slip}</td>
                <td style="padding:8px 12px;font-size:12px;color:{SLATE};text-align:right;
                    border-bottom:1px solid {BORDER};width:15%;">{Time}</td>
            </tr>"""
        FutTradesHtml += '</table></td></tr>'
        FutTradesHtml += _Divider()

    # ── Options Trades ──
    OptTradesHtml = ""
    if D["options_orders"]:
        OptTradesHtml += _SectionHeader("Options Trades")
        OptTradesHtml += '<tr><td style="padding:0 12px;"><table style="width:100%;border-collapse:collapse;">'
        OptTradesHtml += _TradeRow([
            ("Underlying", "left", "20%"), ("Leg", "center", "10%"),
            ("Action", "center", "12%"), ("Qty", "center", "10%"),
            ("Fill", "right", "20%"), ("Time", "right", "15%"),
        ], IsHeader=True)
        for O in D["options_orders"]:
            Fill = f"{O['fill_price']:.2f}" if O.get("fill_price") else "N/A"
            Time = _FmtTime(O.get("created_at", ""))
            ActionColor = GREEN if O["action"] == "BUY" else RED
            OptTradesHtml += f"""<tr class="pnl-row">
                <td style="padding:8px 12px;font-size:12px;font-weight:600;color:{NAVY};
                    border-bottom:1px solid {BORDER};width:20%;">{O['underlying']}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:center;
                    border-bottom:1px solid {BORDER};width:10%;">{O.get('leg','')}</td>
                <td style="padding:8px 12px;font-size:12px;font-weight:600;color:{ActionColor};
                    text-align:center;border-bottom:1px solid {BORDER};width:12%;">{O['action']}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:center;
                    border-bottom:1px solid {BORDER};width:10%;">{O['qty']}</td>
                <td style="padding:8px 12px;font-size:12px;color:{NAVY};text-align:right;
                    font-weight:600;border-bottom:1px solid {BORDER};width:20%;">{Fill}</td>
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
            Prev = D["prev_snapshot"].get(P["instrument"], 0)
            Change = P["unrealized_pnl"] - Prev
            OpenFutHtml += _PositionBlock(
                P["instrument"], P["direction"], P["confirmed_qty"],
                P["avg_entry_price"], P["ltp"], P["unrealized_pnl"], Change,
            )
    else:
        OpenFutHtml += _EmptyRow("No open futures positions")

    # ── Open Options Positions ──
    OptionsPos = [P for P in D["unrealized_positions"] if "_OPT_" in P["instrument"]]
    OpenOptHtml = _SectionHeader("Open Options")
    if OptionsPos:
        ByUnderlying = defaultdict(lambda: {"unrealized": 0, "prev_unrealized": 0, "legs": []})
        for P in OptionsPos:
            Parts = P["instrument"].split("_OPT_")
            Underlying, Leg = Parts[0], Parts[1] if len(Parts) > 1 else "?"
            ByUnderlying[Underlying]["unrealized"] += P["unrealized_pnl"]
            ByUnderlying[Underlying]["prev_unrealized"] += D["prev_snapshot"].get(P["instrument"], 0)
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
            Change = Combo["unrealized"] - Combo["prev_unrealized"]
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
                    <b style="color:{_PnlColor(Change)};">\u20b9{_FmtINR(Change)}</b>
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

    # ── Effective Capital ──
    CapHtml = _SectionHeader("Effective Capital")
    PnlSign = "+" if D["cumulative_pnl"] >= 0 else ""
    CapHtml += f"""
    <tr><td style="padding:8px 20px 16px;">
        <table style="width:100%;border-collapse:collapse;">
            <tr>
                <td style="padding:6px 0;font-size:13px;color:{SLATE};">Base Capital</td>
                <td style="padding:6px 0;font-size:13px;font-weight:600;color:{NAVY};text-align:right;">
                    \u20b9{_FmtPlain(D["base_capital"])}</td>
            </tr>
            <tr>
                <td style="padding:6px 0;font-size:13px;color:{SLATE};">Cumulative Realized P&L</td>
                <td style="padding:6px 0;font-size:13px;font-weight:600;color:{_PnlColor(D['cumulative_pnl'])};text-align:right;">
                    {PnlSign}\u20b9{_FmtPlain(D["cumulative_pnl"])}</td>
            </tr>
            <tr>
                <td colspan="2" style="padding:8px 0 0;"><div style="border-top:1px solid {BORDER};"></div></td>
            </tr>
            <tr>
                <td style="padding:6px 0;font-size:14px;font-weight:700;color:{NAVY};">Effective Capital</td>
                <td style="padding:6px 0;font-size:16px;font-weight:800;color:{NAVY};text-align:right;">
                    \u20b9{_FmtPlain(D["effective_capital"])}</td>
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
        {ClosedHtml}
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
    db.InitDB()

    if DateStr is None:
        DateStr = date.today().strftime("%Y-%m-%d")

    Logger.info("Generating daily P&L report for %s (dry_run=%s)", DateStr, DryRun)

    with open(CONFIG_PATH) as f:
        FullConfig = json.load(f)

    BaseCapital = FullConfig["account"]["base_capital"]
    CumulativePnl = db.GetCumulativeRealizedPnl()
    EffectiveCapital = BaseCapital + CumulativePnl

    RealizedRows = db.GetTodayRealizedPnl(DateStr)
    FuturesOrders = db.GetTodayFuturesOrders(DateStr)
    OptionsOrders = db.GetTodayOptionsOrders(DateStr)
    OpenPositions = db.GetAllOpenPositions()

    RealizedTotal = sum(r["pnl_inr"] for r in RealizedRows)
    TradeCount = len(FuturesOrders) + len(OptionsOrders)

    LTPs = _FetchAllLTPs(FullConfig)
    UnrealizedPositions = _ComputeUnrealizedPnl(OpenPositions, LTPs)

    PrevSnapshot = db.GetPreviousSnapshot(DateStr)
    TotalUnrealizedNow = sum(p["unrealized_pnl"] for p in UnrealizedPositions)
    TotalUnrealizedPrev = sum(PrevSnapshot.get(p["instrument"], 0) for p in UnrealizedPositions)
    for Inst, PrevVal in PrevSnapshot.items():
        if not any(p["instrument"] == Inst for p in UnrealizedPositions):
            TotalUnrealizedPrev += PrevVal
    UnrealizedChange = TotalUnrealizedNow - TotalUnrealizedPrev

    TotalDailyPnl = RealizedTotal + UnrealizedChange

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

    OpenCount = len([p for p in OpenPositions if "_OPT_" not in p["instrument"]])

    ReportData = {
        "date": DateStr,
        "trade_count": TradeCount,
        "open_count": OpenCount,
        "realized_total": RealizedTotal,
        "unrealized_change": UnrealizedChange,
        "cumulative_pnl": CumulativePnl,
        "effective_capital": EffectiveCapital,
        "base_capital": BaseCapital,
        "realized_rows": RealizedRows,
        "futures_orders": FuturesOrders,
        "options_orders": OptionsOrders,
        "unrealized_positions": UnrealizedPositions,
        "prev_snapshot": PrevSnapshot,
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
