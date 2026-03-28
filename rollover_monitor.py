"""
rollover_monitor.py — Automatic Futures Contract Rollover System.

Standalone process that runs independently of Server_Start.py.
Scans all open futures positions, identifies contracts approaching expiry,
and executes two-leg rollovers (close current month, open next month)
using the SmartChaseExecute engine to minimise slippage.

Usage:
    python rollover_monitor.py                           # Normal daily run
    python rollover_monitor.py --dry-run                 # Log decisions, no orders
    python rollover_monitor.py --instrument=GOLDM --force  # Force rollover now
    python rollover_monitor.py --status                  # Print rollover status
"""

import argparse
import json
import logging
import smtplib
import sys
import time
import html as _html
from datetime import date, datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

import pandas as pd
from kiteconnect import KiteConnect

from Directories import (
    workInputRoot,
    ZerodhaInstrumentDirectory,
    AngelInstrumentDirectory,
    KiteEkanshLogin, KiteEkanshLoginAccessToken,
    KiteRashmiLogin, KiteRashmiLoginAccessToken,
    KiteEshitaLogin, KiteEshitaLoginAccessToken,
    AngelEkanshLoginCred, AngelNararushLoginCred, AngelEshitaLoginCred,
)
from Holidays import CheckForDateHoliday
import forecast_db as db
from smart_chase import SmartChaseExecute, EXCHANGE_OPEN_TIMES

Logger = logging.getLogger("rollover_monitor")

CONFIG_PATH = Path(__file__).parent / "instrument_config.json"
EMAIL_CONFIG_PATH = Path(workInputRoot) / "email_config.json"

# ─── Trading Day Utilities ───────────────────────────────────────────

def IsTradingDay(D):
    """Return True if D is a weekday and not a market holiday."""
    if D.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    if CheckForDateHoliday(D):
        return False
    return True


def CountTradingDaysUntilExpiry(ExpiryDate, FromDate=None):
    """Count trading days from FromDate (exclusive) to ExpiryDate (inclusive).

    Returns 0 if ExpiryDate <= FromDate.
    """
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


def GetNTradingDaysBefore(ExpiryDate, N):
    """Return the calendar date that is N trading days before ExpiryDate."""
    if isinstance(ExpiryDate, datetime):
        ExpiryDate = ExpiryDate.date()
    Current = ExpiryDate
    Remaining = N
    while Remaining > 0:
        Current -= timedelta(days=1)
        if IsTradingDay(Current):
            Remaining -= 1
    return Current


# ─── Broker Session Management ───────────────────────────────────────

def _EstablishKiteSession(User):
    """Create a KiteConnect session for the given user."""
    UserMap = {
        "OFS653": (KiteEshitaLogin, KiteEshitaLoginAccessToken),
        "YD6016": (KiteRashmiLogin, KiteRashmiLoginAccessToken),
        "IK6635": (KiteEkanshLogin, KiteEkanshLoginAccessToken),
    }
    LoginFile, TokenFile = UserMap.get(User, (KiteEshitaLogin, KiteEshitaLoginAccessToken))

    with open(LoginFile, "r") as f:
        Lines = f.readlines()
        ApiKey = Lines[2].strip("\n")

    Kite = KiteConnect(api_key=ApiKey)

    with open(TokenFile, "r") as f:
        AccessToken = f.read().strip()

    Kite.set_access_token(AccessToken)
    Logger.info("Kite session established for %s", User)
    return Kite


def _EstablishAngelSession(User):
    """Create a SmartConnect session for the given user."""
    from SmartApi import SmartConnect
    import pyotp

    CredMap = {
        "E51339915": AngelEkanshLoginCred,
        "R71302": AngelNararushLoginCred,
        "AABM826021": AngelEshitaLoginCred,
    }
    CredFile = CredMap.get(User)
    if CredFile is None:
        raise ValueError(f"Unknown Angel user: {User}")

    with open(CredFile, "r") as f:
        Lines = f.readlines()
        ClientCode = Lines[0].strip()
        Password = Lines[1].strip()
        ApiKey = Lines[2].strip()
        TotpSecret = Lines[3].strip()

    SmartApi = SmartConnect(api_key=ApiKey)
    Totp = pyotp.TOTP(TotpSecret).now()
    SmartApi.generateSession(ClientCode, Password, Totp)
    Logger.info("Angel session established for %s", User)
    return SmartApi


INDEX_NAMES = {"NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY", "MIDCPNIFTY", "BANKEX"}


def _IsIndexOption(TradingSymbol):
    """Return True if the symbol is an index option (CE/PE). Stock options return False."""
    SymUpper = TradingSymbol.upper()
    if not (SymUpper.endswith("CE") or SymUpper.endswith("PE")):
        return False
    for Idx in INDEX_NAMES:
        if SymUpper.startswith(Idx):
            return True
    return False


# ─── Position Scanner ────────────────────────────────────────────────

def ScanAllPositions(InstrumentConfig):
    """Fetch all open futures positions across all accounts.

    Returns list of dicts with unified schema.
    """
    # Hardcoded active accounts — do NOT derive from config
    # OFS653 (Eshita) + YD6016 (Rashmi) on Zerodha, AABM826021 (Eshita) on Angel
    ZerodhaUsers = {"OFS653", "YD6016"}
    AngelUsers = {"AABM826021"}

    Positions = []

    # Scan Zerodha accounts
    for User in ZerodhaUsers:
        try:
            Kite = _EstablishKiteSession(User)
            RawPositions = Kite.positions().get("net", [])
            for Pos in RawPositions:
                Qty = Pos.get("quantity", 0)
                Product = Pos.get("product", "")
                if Qty != 0 and Product == "NRML":
                    Symbol = Pos.get("tradingsymbol", "")
                    if _IsIndexOption(Symbol):
                        continue
                    Positions.append({
                        "tradingsymbol": Symbol,
                        "exchange": Pos.get("exchange", ""),
                        "quantity": Qty,
                        "last_price": Pos.get("last_price", 0),
                        "instrument_token": Pos.get("instrument_token", ""),
                        "product": Product,
                        "broker": "ZERODHA",
                        "user": User,
                        "_session": Kite,
                    })
        except Exception as e:
            Logger.error("Failed to fetch Zerodha positions for %s: %s", User, e)

    # Scan Angel accounts
    for User in AngelUsers:
        try:
            SmartApi = _EstablishAngelSession(User)
            RawResponse = SmartApi.position()
            RawPositions = RawResponse.get("data", []) if isinstance(RawResponse, dict) else []
            if RawPositions is None:
                RawPositions = []
            for Pos in RawPositions:
                Qty = int(Pos.get("netqty", 0))
                ProdType = Pos.get("producttype", "")
                if Qty != 0:
                    Logger.info("Angel %s: symbol=%s qty=%s product=%s",
                                User, Pos.get("tradingsymbol"), Qty, ProdType)
                if Qty != 0 and ProdType == "CARRYFORWARD":
                    Symbol = Pos.get("tradingsymbol", "")
                    if _IsIndexOption(Symbol):
                        continue
                    Positions.append({
                        "tradingsymbol": Symbol,
                        "exchange": Pos.get("exchange", ""),
                        "quantity": Qty,
                        "last_price": float(Pos.get("ltp", 0)),
                        "symboltoken": Pos.get("symboltoken", ""),
                        "product": ProdType,
                        "broker": "ANGEL",
                        "user": User,
                        "_session": SmartApi,
                    })
        except Exception as e:
            Logger.error("Failed to fetch Angel positions for %s: %s", User, e)

    Logger.info("Scanned %d open futures positions across all accounts", len(Positions))
    return Positions


def MatchPositionToInstrument(Position, InstrumentConfig):
    """Match a position's tradingsymbol to an instrument config key.

    Reads the instrument CSV to find the base name, then matches to config.
    Returns (InstrumentName, Config) or (None, None).
    """
    TradingSymbol = Position["tradingsymbol"]
    Exchange = Position["exchange"]
    Broker = Position["broker"]

    if Broker == "ZERODHA":
        try:
            Df = pd.read_csv(ZerodhaInstrumentDirectory, delimiter=",")
            Match = Df[
                (Df["symbol"] == TradingSymbol) &
                (Df["exch_seg"] == Exchange)
            ]
            if not Match.empty:
                Name = Match.iloc[0]["name"]
                for InstName, Cfg in InstrumentConfig.items():
                    if InstName == Name and Cfg.get("exchange") == Exchange:
                        return InstName, Cfg
        except Exception as e:
            Logger.error("Error matching Zerodha position %s: %s", TradingSymbol, e)

    elif Broker == "ANGEL":
        try:
            Df = pd.read_csv(AngelInstrumentDirectory, delimiter=",")
            Match = Df[
                (Df["symbol"] == TradingSymbol) &
                (Df["exch_seg"] == Exchange)
            ]
            if not Match.empty:
                Name = Match.iloc[0]["name"]
                for InstName, Cfg in InstrumentConfig.items():
                    if InstName == Name and Cfg.get("exchange") == Exchange:
                        return InstName, Cfg
        except Exception as e:
            Logger.error("Error matching Angel position %s: %s", TradingSymbol, e)

    # Fallback: try direct match on instrument name prefix
    for InstName, Cfg in InstrumentConfig.items():
        if TradingSymbol.upper().startswith(InstName.upper()):
            if Cfg.get("exchange") == Exchange:
                return InstName, Cfg

    return None, None


# ─── Expiry Date Resolver ────────────────────────────────────────────

def ResolveExpiryInfo(InstName, InstConfig, Position):
    """Resolve current contract expiry and next month contract details.

    Returns dict with: current_expiry, current_symbol, current_token,
    next_symbol, next_token, next_expiry. Or None on failure.
    """
    Exchange = InstConfig["exchange"]
    Broker = Position["broker"]
    Today = datetime.now()

    HeldSymbol = Position.get("tradingsymbol", "")

    if Broker == "ZERODHA":
        try:
            Df = pd.read_csv(ZerodhaInstrumentDirectory, delimiter=",")
            Df.rename(columns={"Unnamed: 0": "serialnumber"}, inplace=True)
            Df["expiry"] = pd.to_datetime(Df["expiry"], format="%Y-%m-%d", errors="coerce")

            # Match the actual held contract by tradingsymbol
            HeldMatch = Df[
                (Df["symbol"] == HeldSymbol) &
                (Df["exch_seg"] == Exchange)
            ]
            if HeldMatch.empty:
                Logger.warning("%s: Could not find held contract %s in CSV", InstName, HeldSymbol)
                return None

            CurrentContract = HeldMatch.iloc[0]
            HeldExpiry = CurrentContract["expiry"]

            # Find next month contract (expiry strictly after held contract's expiry)
            FutContracts = Df[
                (Df["name"] == InstName) &
                (Df["exch_seg"] == Exchange) &
                (Df["instrumenttype"] == "FUT") &
                (Df["expiry"] > HeldExpiry)
            ].sort_values(by="expiry", ascending=True)

            NextContract = FutContracts.iloc[0] if not FutContracts.empty else None

            Result = {
                "current_expiry": CurrentContract["expiry"].to_pydatetime(),
                "current_symbol": CurrentContract["symbol"],
                "current_token": str(CurrentContract["token"]),
                "next_symbol": NextContract["symbol"] if NextContract is not None else None,
                "next_token": str(NextContract["token"]) if NextContract is not None else None,
                "next_expiry": NextContract["expiry"].to_pydatetime() if NextContract is not None else None,
            }
            return Result
        except Exception as e:
            Logger.error("%s: Failed to resolve Zerodha expiry: %s", InstName, e)
            return None

    elif Broker == "ANGEL":
        try:
            Df = pd.read_csv(AngelInstrumentDirectory, delimiter=",", low_memory=False)
            Df["expiry"] = pd.to_datetime(Df["expiry"], format="%d%b%Y", errors="coerce")

            # Match the actual held contract by tradingsymbol
            HeldMatch = Df[
                (Df["symbol"] == HeldSymbol) &
                (Df["exch_seg"] == Exchange)
            ]
            if HeldMatch.empty:
                Logger.warning("%s: Could not find held contract %s in Angel CSV", InstName, HeldSymbol)
                return None

            CurrentContract = HeldMatch.iloc[0]
            HeldExpiry = CurrentContract["expiry"]

            # Find next month contract (expiry strictly after held contract's expiry)
            FutContracts = Df[
                (Df["name"] == InstName) &
                (Df["exch_seg"] == Exchange) &
                (Df["instrumenttype"].isin(["FUTCOM", "FUTIDX"])) &
                (Df["expiry"] > HeldExpiry)
            ].sort_values(by="expiry", ascending=True)

            NextContract = FutContracts.iloc[0] if not FutContracts.empty else None

            Result = {
                "current_expiry": CurrentContract["expiry"].to_pydatetime(),
                "current_symbol": CurrentContract["symbol"],
                "current_token": str(CurrentContract["token"]),
                "next_symbol": NextContract["symbol"] if NextContract is not None else None,
                "next_token": str(NextContract["token"]) if NextContract is not None else None,
                "next_expiry": NextContract["expiry"].to_pydatetime() if NextContract is not None else None,
            }
            return Result
        except Exception as e:
            Logger.error("%s: Failed to resolve Angel expiry: %s", InstName, e)
            return None

    return None


# ─── Rollover Decision Engine ────────────────────────────────────────

def EvaluateRolloverNeed(InstName, InstConfig, ExpiryInfo, Position):
    """Decide whether rollover action is needed.

    Returns one of: NO_ACTION, ALERT_ONLY, EXECUTE_NOW, RECOVER_LEG2
    """
    RolloverCfg = InstConfig.get("rollover", {})
    if not RolloverCfg.get("enabled", False):
        return "NO_ACTION"

    ExpiryDate = ExpiryInfo["current_expiry"]
    TradingDaysLeft = CountTradingDaysUntilExpiry(ExpiryDate)
    AlertDays = RolloverCfg.get("alert_days_before_expiry", 4)
    ExecuteDays = RolloverCfg.get("execute_days_before_expiry", 3)

    Logger.info(
        "%s: Expiry=%s, TradingDaysLeft=%d, AlertDays=%d, ExecuteDays=%d",
        InstName, ExpiryDate.strftime("%Y-%m-%d"), TradingDaysLeft, AlertDays, ExecuteDays
    )

    # Check DB for existing rollover attempts
    ExpiryStr = ExpiryDate.strftime("%Y-%m-%d")
    PriorRollovers = db.GetPendingRollovers(InstName, ExpiryStr)

    for Row in PriorRollovers:
        if Row["status"] == "COMPLETE":
            Logger.info("%s: Rollover already completed for expiry %s", InstName, ExpiryStr)
            return "NO_ACTION"
        if Row["status"] == "LEG1_DONE":
            Logger.info("%s: Recovering — leg 1 done, leg 2 pending", InstName)
            return "RECOVER_LEG2"
        if Row["status"] == "ABORTED":
            continue  # Ignore aborted attempts

    if TradingDaysLeft <= ExecuteDays:
        return "EXECUTE_NOW"
    elif TradingDaysLeft <= AlertDays:
        return "ALERT_ONLY"
    else:
        return "NO_ACTION"


# ─── Email Notifications ─────────────────────────────────────────────

def _LoadEmailConfig():
    """Load email config from JSON file."""
    if not EMAIL_CONFIG_PATH.exists():
        Logger.warning("Email config not found at %s", EMAIL_CONFIG_PATH)
        return None
    with open(EMAIL_CONFIG_PATH, "r") as f:
        return json.load(f)


def _SendEmail(Subject, HtmlBody, PlainBody=None):
    """Send an email using the configured SMTP settings."""
    Cfg = _LoadEmailConfig()
    if Cfg is None:
        return

    try:
        Msg = MIMEMultipart("alternative")
        Msg["Subject"] = Subject
        Msg["From"] = Cfg["sender"]
        Msg["To"] = Cfg["recipient"]
        Msg["X-Priority"] = "1"
        Msg["X-MSMail-Priority"] = "High"
        Msg["Importance"] = "High"

        if PlainBody:
            Msg.attach(MIMEText(PlainBody, "plain"))
        Msg.attach(MIMEText(HtmlBody, "html"))

        with smtplib.SMTP_SSL(Cfg.get("smtp_server", "smtp.gmail.com"),
                               Cfg.get("port", 465)) as Server:
            Server.login(Cfg["sender"], Cfg["app_password"])
            Server.send_message(Msg)

        Logger.info("Email sent: %s", Subject)
    except Exception as e:
        Logger.error("Failed to send email: %s", e)


def _BuildEmailHtml(Title, Subtitle, Cards):
    """Build a responsive HTML email with dark mode support.

    Args:
        Title: Main heading text
        Subtitle: Subheading or timestamp
        Cards: list of dicts with 'title', 'icon', 'rows' (list of (label, value, color) tuples)
    """
    # Color palette
    NAVY = "#003366"
    CARD_BG = "#ffffff"
    ACCENT_GREEN = "#00c853"
    ACCENT_RED = "#ff1744"
    ACCENT_AMBER = "#ff9100"
    MUTED = "#8892a0"
    LABEL_CLR = "#6b7b8d"
    VALUE_CLR = "#1a1a2e"
    BORDER = "#e4e8ee"
    SECTION_BG = "#f7f9fc"
    BG = "#f0f2f5"

    # Dark mode overrides
    DarkStyle = """
    @media (prefers-color-scheme: dark) {
        .email-body { background-color: #0d1117 !important; }
        .email-container { background-color: #161b22 !important; }
        .email-card { background-color: #1c2128 !important; border-color: #30363d !important; }
        .email-card-header { background-color: #21262d !important; border-color: #30363d !important; }
        .email-card-header span { color: #e6edf3 !important; }
        .email-label { color: #8b949e !important; }
        .email-value { color: #e6edf3 !important; }
        .email-row { border-color: #21262d !important; }
        .email-title { color: #e6edf3 !important; }
        .email-subtitle { color: #8b949e !important; }
        .email-footer { color: #484f58 !important; }
    }
    """

    CardHtml = ""
    for Card in Cards:
        RowsHtml = ""
        for Row in Card.get("rows", []):
            Label, Value = Row[0], Row[1]
            ValColor = Row[2] if len(Row) > 2 and Row[2] else VALUE_CLR
            IsBold = Row[3] if len(Row) > 3 else False
            FontWeight = "font-weight:600;" if IsBold else ""
            RowsHtml += f"""
            <tr>
                <td class="email-label" style="padding:10px 16px;color:{LABEL_CLR};font-size:13px;
                    font-weight:500;width:40%;border-bottom:1px solid {BORDER};vertical-align:top;"
                    class="email-row">{_html.escape(str(Label))}</td>
                <td class="email-value" style="padding:10px 16px;color:{ValColor};font-size:14px;
                    {FontWeight}border-bottom:1px solid {BORDER};"
                    class="email-row">{_html.escape(str(Value))}</td>
            </tr>"""

        CardHtml += f"""
        <div class="email-card" style="background:{CARD_BG};border-radius:12px;margin:16px 0;
            overflow:hidden;border:1px solid {BORDER};">
            <div class="email-card-header" style="background:{SECTION_BG};padding:14px 16px;
                border-bottom:1px solid {BORDER};">
                <span style="font-size:14px;font-weight:700;color:{NAVY};letter-spacing:0.3px;">
                    {Card.get('icon', '')} {_html.escape(Card.get('title', ''))}</span>
            </div>
            <table style="width:100%;border-collapse:collapse;">{RowsHtml}</table>
        </div>"""

    Html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<meta name="supported-color-schemes" content="light dark">
<style>{DarkStyle}
    * {{ margin:0; padding:0; box-sizing:border-box; }}
</style></head>
<body class="email-body" style="background:{BG};margin:0;padding:0;font-family:-apple-system,
    BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
<div style="max-width:600px;margin:0 auto;padding:20px 12px;">
    <div class="email-container" style="background:{CARD_BG};border-radius:16px;overflow:hidden;
        border:1px solid {BORDER};box-shadow:0 2px 8px rgba(0,0,0,0.06);">
        <div style="background:linear-gradient(135deg,{NAVY},#004d99);padding:24px 20px;text-align:center;">
            <h1 class="email-title" style="color:white;font-size:20px;font-weight:700;margin:0;">
                {_html.escape(Title)}</h1>
            <p class="email-subtitle" style="color:rgba(255,255,255,0.7);font-size:13px;
                margin-top:6px;">{_html.escape(Subtitle)}</p>
        </div>
        <div style="padding:8px 16px;">
            {CardHtml}
        </div>
        <div class="email-footer" style="padding:16px;text-align:center;font-size:11px;color:{MUTED};
            border-top:1px solid {BORDER};">
            Rollover Monitor &bull; Auto-generated &bull; {datetime.now().strftime('%Y-%m-%d %H:%M IST')}
        </div>
    </div>
</div></body></html>"""

    return Html


def SendAlertEmail(InstName, ExpiryInfo, Position, TradingDaysLeft):
    """Send informational alert about approaching expiry."""
    Direction = "LONG" if Position["quantity"] > 0 else "SHORT"
    Cards = [
        {
            "title": "Rollover Approaching",
            "icon": "\u26a0\ufe0f",
            "rows": [
                ("Instrument", InstName, None, True),
                ("Direction", Direction, "#00c853" if Direction == "LONG" else "#ff1744", True),
                ("Quantity", str(abs(Position["quantity"]))),
                ("Current Contract", ExpiryInfo["current_symbol"]),
                ("Expiry Date", ExpiryInfo["current_expiry"].strftime("%Y-%m-%d")),
                ("Trading Days Left", str(TradingDaysLeft), "#ff9100" if TradingDaysLeft <= 3 else None, True),
                ("Next Contract", ExpiryInfo.get("next_symbol", "N/A")),
                ("Exchange", Position["exchange"]),
                ("Broker", Position["broker"]),
                ("Account", Position["user"]),
            ],
        }
    ]
    Subject = f"\u26a0\ufe0f [Rollover Alert] {InstName} expires in {TradingDaysLeft} trading days"
    Html = _BuildEmailHtml(
        f"Rollover Alert — {InstName}",
        f"Contract {ExpiryInfo['current_symbol']} approaching expiry",
        Cards,
    )
    _SendEmail(Subject, Html)


def SendPreExecutionEmail(InstName, ExpiryInfo, Position, RolloverCfg):
    """Send 5-minute warning before execution."""
    Direction = "LONG" if Position["quantity"] > 0 else "SHORT"
    Leg1Action = "SELL" if Position["quantity"] > 0 else "BUY"
    Leg2Action = "BUY" if Position["quantity"] > 0 else "SELL"

    Cards = [
        {
            "title": "Rollover Executing in 5 Minutes",
            "icon": "\U0001f6a8",
            "rows": [
                ("Instrument", InstName, None, True),
                ("Direction", Direction, "#00c853" if Direction == "LONG" else "#ff1744", True),
                ("Quantity", str(abs(Position["quantity"]))),
                ("Last Price", f"{Position['last_price']:.2f}"),
            ],
        },
        {
            "title": "Leg 1 — Close Current Month",
            "icon": "\U0001f534",
            "rows": [
                ("Action", Leg1Action, "#ff1744" if Leg1Action == "SELL" else "#00c853", True),
                ("Contract", ExpiryInfo["current_symbol"]),
                ("Expiry", ExpiryInfo["current_expiry"].strftime("%Y-%m-%d")),
            ],
        },
        {
            "title": "Leg 2 — Open Next Month",
            "icon": "\U0001f7e2",
            "rows": [
                ("Action", Leg2Action, "#00c853" if Leg2Action == "BUY" else "#ff1744", True),
                ("Contract", ExpiryInfo.get("next_symbol", "N/A")),
                ("Next Expiry", ExpiryInfo["next_expiry"].strftime("%Y-%m-%d") if ExpiryInfo.get("next_expiry") else "N/A"),
            ],
        },
    ]
    Subject = f"\U0001f6a8 [ROLLOVER] {InstName} — {ExpiryInfo['current_symbol']} \u2192 {ExpiryInfo.get('next_symbol', '?')} in 5 min"
    Html = _BuildEmailHtml(
        f"Rollover in 5 Minutes — {InstName}",
        f"{Leg1Action} {ExpiryInfo['current_symbol']} then {Leg2Action} {ExpiryInfo.get('next_symbol', '?')}",
        Cards,
    )
    _SendEmail(Subject, Html)


def SendRolloverResultEmail(InstName, ExpiryInfo, RolloverRow, Leg1Info, Leg2Info, Success):
    """Send result email after rollover execution."""
    StatusLabel = "COMPLETE" if Success else "FAILED"
    StatusColor = "#00c853" if Success else "#ff1744"

    Cards = [
        {
            "title": "Rollover Result",
            "icon": "\u2705" if Success else "\u274c",
            "rows": [
                ("Instrument", InstName, None, True),
                ("Status", StatusLabel, StatusColor, True),
                ("Old Contract", ExpiryInfo["current_symbol"]),
                ("New Contract", ExpiryInfo.get("next_symbol", "N/A")),
            ],
        },
    ]

    if Leg1Info:
        Slip1 = Leg1Info.get("slippage")
        SlipStr1 = f"{Slip1:+.2f}" if Slip1 is not None else "N/A"
        SlipColor1 = "#00c853" if Slip1 is not None and Slip1 < 0 else "#ff1744" if Slip1 is not None and Slip1 > 0 else None
        Cards.append({
            "title": "Leg 1 — Close Current Month",
            "icon": "\U0001f534",
            "rows": [
                ("Fill Price", f"{Leg1Info.get('fill_price', 'N/A')}"),
                ("Slippage", SlipStr1, SlipColor1),
                ("Execution Mode", Leg1Info.get("execution_mode", "N/A")),
                ("Chase Iterations", str(Leg1Info.get("chase_iterations", 0))),
                ("Duration", f"{Leg1Info.get('chase_duration_seconds', 0):.1f}s"),
                ("Market Fallback", "Yes" if Leg1Info.get("market_fallback") else "No"),
            ],
        })

    if Leg2Info:
        Slip2 = Leg2Info.get("slippage")
        SlipStr2 = f"{Slip2:+.2f}" if Slip2 is not None else "N/A"
        SlipColor2 = "#00c853" if Slip2 is not None and Slip2 < 0 else "#ff1744" if Slip2 is not None and Slip2 > 0 else None
        Cards.append({
            "title": "Leg 2 — Open Next Month",
            "icon": "\U0001f7e2",
            "rows": [
                ("Fill Price", f"{Leg2Info.get('fill_price', 'N/A')}"),
                ("Slippage", SlipStr2, SlipColor2),
                ("Execution Mode", Leg2Info.get("execution_mode", "N/A")),
                ("Chase Iterations", str(Leg2Info.get("chase_iterations", 0))),
                ("Duration", f"{Leg2Info.get('chase_duration_seconds', 0):.1f}s"),
                ("Market Fallback", "Yes" if Leg2Info.get("market_fallback") else "No"),
            ],
        })

    # Roll spread card
    if Leg1Info and Leg2Info:
        L1Price = Leg1Info.get("fill_price") or 0
        L2Price = Leg2Info.get("fill_price") or 0
        if L1Price and L2Price:
            RollSpread = L2Price - L1Price
            Cards.append({
                "title": "Roll Spread",
                "icon": "\U0001f4b0",
                "rows": [
                    ("Current Month Fill", f"{L1Price:.2f}"),
                    ("Next Month Fill", f"{L2Price:.2f}"),
                    ("Spread (cost of carry)", f"{RollSpread:+.2f}",
                     "#ff1744" if RollSpread > 0 else "#00c853"),
                ],
            })

    Subject = f"{'✅' if Success else '❌'} [ROLLOVER {StatusLabel}] {InstName} — {ExpiryInfo['current_symbol']} → {ExpiryInfo.get('next_symbol', '?')}"
    Html = _BuildEmailHtml(
        f"Rollover {StatusLabel} — {InstName}",
        datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        Cards,
    )
    _SendEmail(Subject, Html)


def SendDailySummaryEmail(Results, UpcomingRollovers):
    """Send daily summary of all rollover actions and upcoming rollovers."""
    Cards = []

    if Results:
        Rows = []
        for R in Results:
            StatusIcon = "\u2705" if R["success"] else "\u274c"
            Rows.append((
                f"{StatusIcon} {R['instrument']}",
                f"{R['old_contract']} \u2192 {R.get('new_contract', '?')} — {R['status']}",
            ))
        Cards.append({"title": "Today's Rollovers", "icon": "\U0001f504", "rows": Rows})
    else:
        Cards.append({
            "title": "Today's Rollovers",
            "icon": "\U0001f504",
            "rows": [("Status", "No rollovers executed today")],
        })

    if UpcomingRollovers:
        Rows = []
        for U in UpcomingRollovers:
            Rows.append((
                U["instrument"],
                f"Expires {U['expiry']} — {U['days_left']} trading days left",
                "#ff9100" if U["days_left"] <= 3 else None,
            ))
        Cards.append({"title": "Upcoming Rollovers (next 7 days)", "icon": "\U0001f4c5", "rows": Rows})

    Subject = f"\U0001f4ca [Rollover Summary] {date.today().strftime('%Y-%m-%d')} — {len(Results)} rollover(s)"
    Html = _BuildEmailHtml(
        "Daily Rollover Summary",
        date.today().strftime("%A, %B %d, %Y"),
        Cards,
    )
    _SendEmail(Subject, Html)


# ─── Rollover Execution Engine ───────────────────────────────────────

def _BuildRolloverOrderDict(InstName, InstConfig, ContractSymbol, ContractToken,
                            Action, Quantity, Position):
    """Build an OrderDetails dict for SmartChaseExecute."""
    Routing = InstConfig.get("order_routing", {})
    QtyMultiplier = Routing.get("QuantityMultiplier", 1)

    if QtyMultiplier and QtyMultiplier != 1:
        QtyStr = f"{abs(Quantity)}*{QtyMultiplier}"
    else:
        QtyStr = str(abs(Quantity))

    OrderDict = {
        "Tradetype": Action,
        "Exchange": InstConfig["exchange"],
        "Tradingsymbol": ContractSymbol,
        "Quantity": QtyStr,
        "Variety": Routing.get("Variety", "REGULAR"),
        "Ordertype": "LIMIT",
        "Product": Routing.get("Product", "NRML"),
        "Validity": Routing.get("Validity", "DAY"),
        "Price": "0",
        "Symboltoken": str(ContractToken) if ContractToken else "",
        "Broker": InstConfig["broker"],
        "Netposition": "0",
        "User": InstConfig["user"],
        "UpdatedOrderRouting": "True",
        "ContractNameProvided": "True",
        "InstrumentType": Routing.get("InstrumentType", "FUT"),
        "DaysPostWhichSelectNextContract": Routing.get("DaysPostWhichSelectNextContract", "9"),
        "EntrySleepDuration": Routing.get("EntrySleepDuration", "60"),
        "ExitSleepDuration": Routing.get("ExitSleepDuration", "45"),
        "ConvertToMarketOrder": Routing.get("ConvertToMarketOrder", "True"),
    }

    # Angel-specific fields
    if InstConfig["broker"] == "ANGEL":
        OrderDict["Squareoff"] = "0"
        OrderDict["Stoploss"] = "0"

    return OrderDict


def _GetExecConfig(InstConfig):
    """Get execution config, applying NCDEX overrides for high urgency."""
    ExecConfig = dict(InstConfig.get("execution", {}))
    RolloverCfg = InstConfig.get("rollover", {})

    if RolloverCfg.get("liquidity_urgency") == "high":
        # Tighter timeouts for illiquid NCDEX contracts
        ExecConfig.setdefault("max_chase_seconds_exit", 30)
        ExecConfig.setdefault("max_chase_seconds_entry", 35)
        if ExecConfig.get("max_chase_seconds_exit", 30) > 25:
            ExecConfig["max_chase_seconds_exit"] = 25
        if ExecConfig.get("max_chase_seconds_entry", 35) > 30:
            ExecConfig["max_chase_seconds_entry"] = 30
        # Don't override execution_mode — let the volatility matrix decide

    return ExecConfig


def _WaitForLiquidityWindow(InstConfig):
    """Wait until the preferred liquidity window opens."""
    RolloverCfg = InstConfig.get("rollover", {})
    WindowStart = RolloverCfg.get("preferred_window_start", "10:00")
    WindowEnd = RolloverCfg.get("preferred_window_end", "11:30")

    StartH, StartM = map(int, WindowStart.split(":"))
    EndH, EndM = map(int, WindowEnd.split(":"))

    Now = datetime.now()
    WindowOpen = Now.replace(hour=StartH, minute=StartM, second=0, microsecond=0)
    WindowClose = Now.replace(hour=EndH, minute=EndM, second=0, microsecond=0)

    if Now < WindowOpen:
        WaitSecs = (WindowOpen - Now).total_seconds()
        Logger.info("Waiting %.0f seconds for liquidity window to open at %s", WaitSecs, WindowStart)
        time.sleep(WaitSecs)
    elif Now > WindowClose:
        Logger.warning("Past liquidity window (%s-%s), proceeding anyway", WindowStart, WindowEnd)


def ExecuteRollover(InstName, InstConfig, ExpiryInfo, Position, DryRun=False):
    """Execute the two-leg rollover.

    Returns (success: bool, rollover_row_id, leg1_info, leg2_info)
    """
    Qty = Position["quantity"]
    Direction = "LONG" if Qty > 0 else "SHORT"
    Broker = Position["broker"]
    Session = Position["_session"]

    ExpiryStr = ExpiryInfo["current_expiry"].strftime("%Y-%m-%d")

    # Get ATR from DB
    ATR = db.GetLatestATR(InstName)
    if ATR is None or ATR <= 0:
        Logger.warning("%s: No ATR available, using conservative default", InstName)
        ATR = Position["last_price"] * 0.02  # 2% of price as fallback

    ExecConfig = _GetExecConfig(InstConfig)

    # Log rollover to DB
    RowId = db.LogRollover(
        InstName, ExpiryStr, ExpiryInfo["current_symbol"],
        abs(Qty), Direction, Broker, Position["user"]
    )

    # Wait for liquidity window
    _WaitForLiquidityWindow(InstConfig)

    # Send pre-execution email and wait 5 minutes
    RolloverCfg = InstConfig.get("rollover", {})
    SendPreExecutionEmail(InstName, ExpiryInfo, Position, RolloverCfg)
    db.UpdateRolloverStatus(RowId, "PENDING", email_sent_at=datetime.now().isoformat())

    if DryRun:
        Logger.info("[DRY RUN] %s: Would roll %d %s from %s to %s",
                    InstName, abs(Qty), Direction,
                    ExpiryInfo["current_symbol"], ExpiryInfo.get("next_symbol"))
        db.UpdateRolloverStatus(RowId, "ABORTED")
        return True, RowId, {}, {}

    Logger.info("Waiting 5 minutes before execution...")
    time.sleep(300)

    # ── LEG 1: Close current month ──────────────────────────────────
    Leg1Action = "SELL" if Qty > 0 else "BUY"
    Leg1OrderDict = _BuildRolloverOrderDict(
        InstName, InstConfig,
        ExpiryInfo["current_symbol"],
        ExpiryInfo["current_token"],
        Leg1Action, abs(Qty), Position
    )

    Logger.info("%s LEG 1: %s %d %s", InstName, Leg1Action, abs(Qty), ExpiryInfo["current_symbol"])

    try:
        Leg1Success, Leg1OrderId, Leg1FillInfo = SmartChaseExecute(
            Session, Leg1OrderDict, ExecConfig, IsEntry=False, Broker=Broker, ATR=ATR
        )
    except Exception as e:
        Logger.error("%s LEG 1 FAILED with exception: %s", InstName, e)
        db.UpdateRolloverStatus(RowId, "LEG1_FAILED")
        SendRolloverResultEmail(InstName, ExpiryInfo, None, None, None, False)
        return False, RowId, None, None

    if not Leg1Success:
        Logger.error("%s LEG 1 FAILED — smart chase unsuccessful", InstName)
        db.UpdateRolloverStatus(
            RowId, "LEG1_FAILED",
            leg1_order_id=str(Leg1OrderId) if Leg1OrderId else None,
            leg1_fill_price=Leg1FillInfo.get("fill_price"),
            leg1_slippage=Leg1FillInfo.get("slippage"),
        )
        SendRolloverResultEmail(InstName, ExpiryInfo, None, Leg1FillInfo, None, False)
        return False, RowId, Leg1FillInfo, None

    Logger.info("%s LEG 1 FILLED @ %s", InstName, Leg1FillInfo.get("fill_price"))
    db.UpdateRolloverStatus(
        RowId, "LEG1_DONE",
        leg1_order_id=str(Leg1OrderId) if Leg1OrderId else None,
        leg1_fill_price=Leg1FillInfo.get("fill_price"),
        leg1_slippage=Leg1FillInfo.get("slippage"),
    )

    # ── LEG 2: Open next month (back-to-back) ──────────────────────
    if ExpiryInfo.get("next_symbol") is None:
        Logger.error("%s: No next month contract available!", InstName)
        db.UpdateRolloverStatus(RowId, "LEG2_FAILED")
        SendRolloverResultEmail(InstName, ExpiryInfo, None, Leg1FillInfo, None, False)
        return False, RowId, Leg1FillInfo, None

    Leg2Action = "BUY" if Qty > 0 else "SELL"
    Leg2OrderDict = _BuildRolloverOrderDict(
        InstName, InstConfig,
        ExpiryInfo["next_symbol"],
        ExpiryInfo["next_token"],
        Leg2Action, abs(Qty), Position
    )

    Logger.info("%s LEG 2: %s %d %s", InstName, Leg2Action, abs(Qty), ExpiryInfo["next_symbol"])

    try:
        Leg2Success, Leg2OrderId, Leg2FillInfo = SmartChaseExecute(
            Session, Leg2OrderDict, ExecConfig, IsEntry=True, Broker=Broker, ATR=ATR
        )
    except Exception as e:
        Logger.error("%s LEG 2 FAILED with exception: %s", InstName, e)
        db.UpdateRolloverStatus(RowId, "LEG2_FAILED")
        SendRolloverResultEmail(InstName, ExpiryInfo, None, Leg1FillInfo, None, False)
        return False, RowId, Leg1FillInfo, None

    if not Leg2Success:
        Logger.error("%s LEG 2 FAILED — smart chase unsuccessful. POSITION IS FLAT!", InstName)
        db.UpdateRolloverStatus(
            RowId, "LEG2_FAILED",
            new_contract=ExpiryInfo["next_symbol"],
            leg2_order_id=str(Leg2OrderId) if Leg2OrderId else None,
            leg2_fill_price=Leg2FillInfo.get("fill_price"),
            leg2_slippage=Leg2FillInfo.get("slippage"),
        )
        SendRolloverResultEmail(InstName, ExpiryInfo, None, Leg1FillInfo, Leg2FillInfo, False)
        return False, RowId, Leg1FillInfo, Leg2FillInfo

    # ── SUCCESS ─────────────────────────────────────────────────────
    L1Price = Leg1FillInfo.get("fill_price") or 0
    L2Price = Leg2FillInfo.get("fill_price") or 0
    RollSpread = (L2Price - L1Price) if L1Price and L2Price else None

    Logger.info(
        "%s ROLLOVER COMPLETE: %s → %s | Roll spread: %s",
        InstName, ExpiryInfo["current_symbol"], ExpiryInfo["next_symbol"],
        f"{RollSpread:+.2f}" if RollSpread is not None else "N/A"
    )

    db.UpdateRolloverStatus(
        RowId, "COMPLETE",
        new_contract=ExpiryInfo["next_symbol"],
        leg2_order_id=str(Leg2OrderId) if Leg2OrderId else None,
        leg2_fill_price=Leg2FillInfo.get("fill_price"),
        leg2_slippage=Leg2FillInfo.get("slippage"),
        roll_spread=RollSpread,
        executed_at=datetime.now().isoformat(),
    )

    # Update system_positions so orchestrator stays in sync
    try:
        SysPos = db.GetSystemPosition(InstName)
        CurrentConfirmed = SysPos.get("confirmed_qty", 0)
        CurrentTarget = SysPos.get("target_qty", 0)
        # Position hasn't changed, just the contract — keep same quantities
        db.UpdateSystemPosition(InstName, CurrentTarget, CurrentConfirmed)
        Logger.info("%s: system_positions unchanged (qty stays at %d)", InstName, CurrentConfirmed)
    except Exception as e:
        Logger.warning("%s: Could not update system_positions: %s", InstName, e)

    SendRolloverResultEmail(InstName, ExpiryInfo, None, Leg1FillInfo, Leg2FillInfo, True)
    return True, RowId, Leg1FillInfo, Leg2FillInfo


def RecoverLeg2(InstName, InstConfig, ExpiryInfo, Position, RolloverRow, DryRun=False):
    """Recover a rollover where leg 1 completed but leg 2 failed."""
    Qty = Position["quantity"]
    # Position is currently flat after leg 1, so we need to re-enter
    # Use the original direction from the rollover log
    OrigDirection = RolloverRow["direction"]
    Broker = Position["broker"]
    Session = Position["_session"]
    RowId = RolloverRow["id"]

    ATR = db.GetLatestATR(InstName)
    if ATR is None or ATR <= 0:
        ATR = Position["last_price"] * 0.02

    ExecConfig = _GetExecConfig(InstConfig)

    # Determine action from original direction
    Leg2Action = "BUY" if OrigDirection == "LONG" else "SELL"
    OrigQty = RolloverRow["quantity"]

    if DryRun:
        Logger.info("[DRY RUN] %s: Would recover leg 2: %s %d %s",
                    InstName, Leg2Action, OrigQty, ExpiryInfo.get("next_symbol"))
        return True, RowId, None, {}

    Leg2OrderDict = _BuildRolloverOrderDict(
        InstName, InstConfig,
        ExpiryInfo["next_symbol"],
        ExpiryInfo["next_token"],
        Leg2Action, OrigQty, Position
    )

    Logger.info("%s RECOVERY LEG 2: %s %d %s", InstName, Leg2Action, OrigQty, ExpiryInfo["next_symbol"])

    try:
        Leg2Success, Leg2OrderId, Leg2FillInfo = SmartChaseExecute(
            Session, Leg2OrderDict, ExecConfig, IsEntry=True, Broker=Broker, ATR=ATR
        )
    except Exception as e:
        Logger.error("%s RECOVERY LEG 2 FAILED: %s", InstName, e)
        SendRolloverResultEmail(InstName, ExpiryInfo, RolloverRow, None, None, False)
        return False, RowId, None, None

    if not Leg2Success:
        Logger.error("%s RECOVERY LEG 2 FAILED — POSITION STILL FLAT!", InstName)
        db.UpdateRolloverStatus(
            RowId, "LEG2_FAILED",
            leg2_order_id=str(Leg2OrderId) if Leg2OrderId else None,
        )
        SendRolloverResultEmail(InstName, ExpiryInfo, RolloverRow, None, Leg2FillInfo, False)
        return False, RowId, None, Leg2FillInfo

    L1Price = RolloverRow.get("leg1_fill_price") or 0
    L2Price = Leg2FillInfo.get("fill_price") or 0
    RollSpread = (L2Price - L1Price) if L1Price and L2Price else None

    Logger.info("%s RECOVERY COMPLETE: %s | Roll spread: %s",
                InstName, ExpiryInfo["next_symbol"],
                f"{RollSpread:+.2f}" if RollSpread is not None else "N/A")

    db.UpdateRolloverStatus(
        RowId, "COMPLETE",
        new_contract=ExpiryInfo["next_symbol"],
        leg2_order_id=str(Leg2OrderId) if Leg2OrderId else None,
        leg2_fill_price=Leg2FillInfo.get("fill_price"),
        leg2_slippage=Leg2FillInfo.get("slippage"),
        roll_spread=RollSpread,
        executed_at=datetime.now().isoformat(),
    )

    SendRolloverResultEmail(InstName, ExpiryInfo, RolloverRow, None, Leg2FillInfo, True)
    return True, RowId, None, Leg2FillInfo


# ─── Status Display ──────────────────────────────────────────────────

def PrintStatus(InstrumentConfig):
    """Print current rollover status to console."""
    db.InitDB()

    RecentRollovers = db.GetRecentRollovers(limit=20)
    Incomplete = db.GetIncompleteRollovers()

    print("\n=== Recent Rollovers ===")
    if RecentRollovers:
        for R in RecentRollovers:
            print(f"  {R['instrument']:15s} | {R['old_contract']:25s} → {R.get('new_contract', 'N/A'):25s} "
                  f"| {R['status']:12s} | {R.get('executed_at', R['created_at'])}")
    else:
        print("  No rollover history found.")

    print("\n=== Incomplete (Need Recovery) ===")
    if Incomplete:
        for R in Incomplete:
            print(f"  {R['instrument']:15s} | {R['old_contract']:25s} | Status: {R['status']}")
    else:
        print("  None.")

    print("\n=== Upcoming Expiries (live positions) ===")
    try:
        LivePositions = ScanAllPositions(InstrumentConfig)
    except Exception as e:
        print(f"  Failed to fetch live positions: {e}")
        print()
        return

    if not LivePositions:
        print("  No open futures positions found.")
        print()
        return

    for Pos in LivePositions:
        InstName, Cfg = MatchPositionToInstrument(Pos, InstrumentConfig)
        if not InstName:
            print(f"  {Pos['tradingsymbol']:25s} | Qty: {Pos['quantity']:>4d} | "
                  f"({Pos['broker']} {Pos['user']}) — not in config")
            continue

        RollCfg = Cfg.get("rollover", {})
        ExpiryInfo = ResolveExpiryInfo(InstName, Cfg, Pos)

        if ExpiryInfo:
            DaysLeft = CountTradingDaysUntilExpiry(ExpiryInfo["current_expiry"])
            ExecDays = RollCfg.get("execute_days_before_expiry", 3)
            AlertDays = RollCfg.get("alert_days_before_expiry", 4)
            Marker = ""
            if DaysLeft <= ExecDays:
                Marker = " <<<< EXECUTE TODAY"
            elif DaysLeft <= AlertDays:
                Marker = " << ALERT"
            print(f"  {InstName:15s} | {Pos['tradingsymbol']:25s} | Qty: {Pos['quantity']:>4d} | "
                  f"Expiry: {ExpiryInfo['current_expiry'].strftime('%Y-%m-%d')} | "
                  f"{DaysLeft} trading days left{Marker}")
        else:
            print(f"  {InstName:15s} | {Pos['tradingsymbol']:25s} | Qty: {Pos['quantity']:>4d} | "
                  f"Could not resolve expiry")

    print()


# ─── Main Entry Point ────────────────────────────────────────────────

def main():
    Parser = argparse.ArgumentParser(description="Automatic Futures Contract Rollover Monitor")
    Parser.add_argument("--dry-run", action="store_true", help="Log decisions without placing orders")
    Parser.add_argument("--instrument", type=str, help="Only process this instrument")
    Parser.add_argument("--force", action="store_true", help="Force rollover regardless of day count")
    Parser.add_argument("--status", action="store_true", help="Print rollover status and exit")
    Args = Parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                Path(__file__).parent / "logs" / f"rollover_{date.today().isoformat()}.log",
                mode="a",
            ),
        ],
    )

    # Load config
    with open(CONFIG_PATH, "r") as f:
        FullConfig = json.load(f)
    InstrumentConfig = FullConfig["instruments"]

    if Args.status:
        PrintStatus(InstrumentConfig)
        return

    # Initialize DB
    db.InitDB()

    # Ensure logs directory exists
    LogDir = Path(__file__).parent / "logs"
    LogDir.mkdir(exist_ok=True)

    # Check if today is a trading day
    Today = date.today()
    if not IsTradingDay(Today) and not Args.force:
        Logger.info("Today (%s) is not a trading day. Exiting.", Today)
        return

    Logger.info("=" * 60)
    Logger.info("Rollover Monitor started — %s %s",
                Today.isoformat(),
                "[DRY RUN]" if Args.dry_run else "[LIVE]")
    Logger.info("=" * 60)

    # Check for incomplete rollovers (crash recovery)
    IncompleteRollovers = db.GetIncompleteRollovers()
    if IncompleteRollovers:
        Logger.warning("Found %d incomplete rollovers from previous run", len(IncompleteRollovers))

    # Scan all positions
    AllPositions = ScanAllPositions(InstrumentConfig)
    if not AllPositions:
        Logger.info("No open positions found. Exiting.")
        return

    Results = []
    UpcomingRollovers = []

    for Position in AllPositions:
        InstName, InstCfg = MatchPositionToInstrument(Position, InstrumentConfig)

        if InstName is None:
            Logger.warning("Could not match position %s to any instrument config",
                          Position["tradingsymbol"])
            continue

        if Args.instrument and InstName != Args.instrument:
            continue

        # Resolve expiry
        ExpiryInfo = ResolveExpiryInfo(InstName, InstCfg, Position)
        if ExpiryInfo is None:
            Logger.warning("%s: Could not resolve expiry info", InstName)
            continue

        TradingDaysLeft = CountTradingDaysUntilExpiry(ExpiryInfo["current_expiry"])

        # Track upcoming rollovers for summary
        RollCfg = InstCfg.get("rollover", {})
        AlertDays = RollCfg.get("alert_days_before_expiry", 4)
        if TradingDaysLeft <= AlertDays + 3:
            UpcomingRollovers.append({
                "instrument": InstName,
                "expiry": ExpiryInfo["current_expiry"].strftime("%Y-%m-%d"),
                "days_left": TradingDaysLeft,
            })

        # Check for recovery from incomplete rollover
        ExpiryStr = ExpiryInfo["current_expiry"].strftime("%Y-%m-%d")
        IncompleteForThis = [
            R for R in IncompleteRollovers
            if R["instrument"] == InstName and R["expiry_date"] == ExpiryStr
        ]

        if IncompleteForThis:
            Logger.info("%s: Recovering incomplete rollover (leg 2)", InstName)
            RolloverRow = IncompleteForThis[0]
            Success, RowId, L1, L2 = RecoverLeg2(
                InstName, InstCfg, ExpiryInfo, Position, RolloverRow, DryRun=Args.dry_run
            )
            Results.append({
                "instrument": InstName,
                "old_contract": ExpiryInfo["current_symbol"],
                "new_contract": ExpiryInfo.get("next_symbol"),
                "status": "COMPLETE" if Success else "LEG2_FAILED",
                "success": Success,
            })
            continue

        # Evaluate rollover need
        if Args.force:
            Decision = "EXECUTE_NOW"
        else:
            Decision = EvaluateRolloverNeed(InstName, InstCfg, ExpiryInfo, Position)

        Logger.info("%s: Decision = %s", InstName, Decision)

        if Decision == "NO_ACTION":
            continue

        elif Decision == "ALERT_ONLY":
            SendAlertEmail(InstName, ExpiryInfo, Position, TradingDaysLeft)

        elif Decision == "EXECUTE_NOW":
            Success, RowId, L1, L2 = ExecuteRollover(
                InstName, InstCfg, ExpiryInfo, Position, DryRun=Args.dry_run
            )
            Results.append({
                "instrument": InstName,
                "old_contract": ExpiryInfo["current_symbol"],
                "new_contract": ExpiryInfo.get("next_symbol"),
                "status": "COMPLETE" if Success else "FAILED",
                "success": Success,
            })

    # Send daily summary
    if Results or UpcomingRollovers:
        SendDailySummaryEmail(Results, UpcomingRollovers)

    Logger.info("Rollover Monitor finished. %d rollovers executed.", len(Results))


if __name__ == "__main__":
    main()
