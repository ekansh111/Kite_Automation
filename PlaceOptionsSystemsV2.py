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
from Directories import (
    WorkDirectory,
    EMAIL_NOTIFY_ENABLED, EMAIL_FROM, EMAIL_FROM_PASSWORD,
    EMAIL_TO, EMAIL_SMTP, EMAIL_PORT,
)
from smart_chase import SmartChaseExecute
from forecast_db import LogOptionsSmartChaseOrder, UpdateCostBasis, RealizePnl, GetCumulativeRealizedPnl
from vol_target import compute_daily_vol_target
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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

# K values for single call/put (used by ITM call rollover strategy)
K_TABLE_SINGLE = [
    (22, 45, 0.18),  # monthly territory — deep ITM, low gamma
    (15, 21, 0.25),  # 2-3 weeks out
    (8, 14, 0.35),   # 1-2 weeks out
    (5, 7, 0.50),
    (3, 4, 0.60),
    (2, 2, 0.80),
    (1, 1, 1.00),
]

# Dynamic K constants
K_FLOOR = 0.20              # min k (most aggressive sizing allowed)
K_CEILING = 5.00            # data-quality guard (not a risk limit)
RISK_FREE_RATE = 0.07       # ~7% annualised risk-free rate for Indian market
IV_SOLVER_MIN = 0.01        # IV solver lower bound (1% annualised)
IV_SOLVER_MAX = 6.0         # IV solver upper bound (600% annualised)
QUOTE_STALE_SECONDS = 60    # quote older than this during market hours → stale
IV_SPREAD_GATE = 0.50       # reject if |ceIV - peIV| / avgIV > this (50%)
BID_ASK_SPREAD_GATE = 0.30  # reject if spread > 30% of mid-price
MIN_PREMIUM_INR = 0.50      # reject near-zero dust premiums

# IV shock: scenario-based sizing uses max(kBase, kStressMove, kStressVol, kCrash).
# The IV shock is a policy-driven vol stress assumption, not a prediction or forecast.
# It feeds kStressVol (1σ move + shock) and kCrash (1.5× move + shock).
# Formula: ivShock = baseShockByDte + vixAdd + realizedMoveAdd, capped.
# Future: + termAdd + eventAdd (zero for now, framework accommodates them).

# Base IV shock by DTE (vol points). Shorter DTE = more fragile to IV spikes.
# Calibrated to 95th-99th percentile single-day India VIX moves.
# Front-end IV spikes disproportionately more than back-end (term structure steepens in stress).
IV_SHOCK_TABLE = [
    (0,   0, 18),    # 0 DTE   → 18 vp  (gamma-dominated, extreme intraday risk)
    (1,   1, 15),    # 1 DTE   → 15 vp  (overnight gap + gamma)
    (2,   2, 12),    # 2 DTE   → 12 vp
    (3,   5, 10),    # 3-5 DTE → 10 vp
    (6,  10,  8),    # 6-10 DTE → 8 vp  (term structure attenuates)
    (11, 21,  6),    # 11-21 DTE → 6 vp (monthly territory)
    (22, 45,  4),    # 22-45 DTE → 4 vp (mean reversion dampens)
]

# VIX add-on: extra vol points based on India VIX level (additive, not multiplicative).
VIX_ADDON_TABLE = [
    (0,    14, 0),     # calm   → no add-on
    (14,   18, 2),     # normal → +2 vol points
    (18,   24, 4),     # elevated → +4 vol points
    (24,   30, 6),     # stressed → +6 vol points
    (30, 9999, 8),     # panic → +8 vol points
]
VIX_LTP_KEY = "NSE:INDIA VIX"

# Realized intraday move add-on: extra vol points based on how much spot
# has already moved today. Catches "market is crashing right now" scenarios.
INTRADAY_MOVE_ADDON_TABLE = [
    (0.0,  0.5,  0),    # flat day → no add-on
    (0.5,  1.0,  2),    # mild move → +2 vol points
    (1.0,  1.5,  4),    # significant → +4 vol points
    (1.5, 9999,  6),    # extreme → +6 vol points
]

# Cap on total IV shock to prevent runaway (vol points)
IV_SHOCK_CAP_VP = 30    # max 30 vol points = 0.30 decimal

# Stress move multiplier for kStressMove scenario
STRESS_MOVE_MULTIPLIER = 1.5

STATE_FILE_PATH = Path(WorkDirectory) / "v2_state.json"
ENTRY_LOG_PATH = Path(WorkDirectory) / "v2_entry_log.csv"
EXIT_LOG_PATH = Path(WorkDirectory) / "v2_exit_log.csv"

# ---------------------------------------------------------------------------
# Options Execution Config (smart chase params per underlying)
# ---------------------------------------------------------------------------
_OPTIONS_EXEC_CONFIG_PATH = Path(__file__).parent / "options_execution_config.json"
try:
    with open(_OPTIONS_EXEC_CONFIG_PATH) as _f:
        OPTIONS_EXEC_CONFIG = json.load(_f)
except FileNotFoundError:
    logging.warning("options_execution_config.json not found at %s, smart chase disabled for options",
                    _OPTIONS_EXEC_CONFIG_PATH)
    OPTIONS_EXEC_CONFIG = {}

UNDERLYING_TO_CONFIG_KEY = {"NIFTY": "NIFTY_OPT", "SENSEX": "SENSEX_OPT"}

# ---------------------------------------------------------------------------
# Email Notification Config — imported from Directories (password from
# KITE_EMAIL_PASSWORD env var so it never lands in source/git).
# ---------------------------------------------------------------------------


def _fmt(val, decimals=2):
    """Format a numeric value for display, return 'N/A' for empty/None."""
    if val is None or val == "":
        return "N/A"
    try:
        return f"{float(val):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(val)


def _pct(val):
    """Format as percentage string."""
    if val is None or val == "":
        return "N/A"
    try:
        return f"{float(val):.1f}%"
    except (ValueError, TypeError):
        return str(val)


def buildEntryEmailHtml(strategyName, config, dte, kValue, callPremium, putPremium,
                        sizeResult, expiryDate, ceSymbol, peSymbol, kMetadata,
                        gttOk, gttIds, state):
    """Build a formatted HTML email body for an entry order notification."""
    underlying = config["underlying"]
    km = kMetadata or {}
    now = datetime.now()

    # ── Colour palette ──
    navy = "#003366"
    accent = "#2E75B6"
    green = "#27AE60"
    red = "#E74C3C"
    grey_bg = "#F8F9FA"
    border_col = "#DEE2E6"

    statusColor = green if gttOk else red
    statusText = "PROTECTED (GTT Active)" if gttOk else "UNPROTECTED (GTT FAILED)"
    kSourceLabel = km.get("source", "static").upper()
    bindingScenario = km.get("kBindingScenario", "N/A")

    # ── Build IV shock breakdown string ──
    ivShockStr = "N/A"
    if km.get("source") == "dynamic":
        base = km.get("ivShockBase", "")
        vix = km.get("vixAddon", "")
        move = km.get("intradayAddon", "")
        total = km.get("ivShockApplied", "")
        ivShockStr = f"{_fmt(total, 0)}vp = {_fmt(base, 0)} (base) + {_fmt(vix, 0)} (VIX) + {_fmt(move, 0)} (move)"

    # ── Cross-system position summary ──
    positionRows = ""
    for ul in ["NIFTY", "SENSEX"]:
        ulState = state.get(ul, {})
        st = ulState.get("currentState", "noPosition")
        strat = ulState.get("activeStrategy", "-")
        lots = ulState.get("activeLots", 0)
        contracts = ulState.get("activeContracts", [])
        integrity = ulState.get("positionIntegrity", "healthy")
        gttProt = ulState.get("gttProtected", True)
        gttIdsList = ulState.get("activeGttIds", [])

        stateColor = green if st in ("earlyOpen", "lateOpen") else "#888"
        integrityColor = green if integrity == "healthy" else red
        gttColor = green if gttProt else red

        contractStr = "<br>".join(contracts) if contracts else "-"
        gttIdsStr = ", ".join(str(g) for g in gttIdsList) if gttIdsList else "-"

        positionRows += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid {border_col};font-weight:600;">{ul}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {border_col};color:{stateColor};">{st}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {border_col};">{strat or '-'}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {border_col};text-align:center;">{lots}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {border_col};font-size:12px;">{contractStr}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {border_col};color:{integrityColor};">{integrity}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {border_col};color:{gttColor};">{gttProt}</td>
          <td style="padding:8px 12px;border-bottom:1px solid {border_col};font-size:11px;">{gttIdsStr}</td>
        </tr>"""

    html = f"""
    <html>
    <body style="margin:0;padding:0;font-family:Arial,Helvetica,sans-serif;background:#EAECEE;">
      <div style="max-width:680px;margin:20px auto;background:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1);">

        <!-- Header -->
        <div style="background:{navy};padding:20px 28px;">
          <h1 style="margin:0;color:#FFFFFF;font-size:20px;letter-spacing:0.5px;">
            V2 Order Placed &mdash; {underlying} {config['phaseType'].upper()}
          </h1>
          <p style="margin:6px 0 0;color:#AAC4E0;font-size:13px;">
            {strategyName} &bull; {now.strftime('%d %b %Y, %I:%M %p')} &bull; Expiry {expiryDate}
          </p>
        </div>

        <!-- Status Banner -->
        <div style="background:{statusColor};padding:10px 28px;">
          <span style="color:#FFFFFF;font-size:13px;font-weight:600;">
            Position Status: {statusText}
          </span>
        </div>

        <!-- Contract & Sizing -->
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Contract &amp; Position Sizing
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">CE Contract</td>
              <td style="padding:8px 12px;font-family:monospace;">{ceSymbol}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">PE Contract</td>
              <td style="padding:8px 12px;font-family:monospace;">{peSymbol}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">CE Premium (LTP)</td>
              <td style="padding:8px 12px;">\u20B9{_fmt(callPremium)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">PE Premium (LTP)</td>
              <td style="padding:8px 12px;">\u20B9{_fmt(putPremium)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Combined Premium</td>
              <td style="padding:8px 12px;">\u20B9{_fmt(sizeResult['combinedPremium'])}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Trading DTE / Sizing DTE</td>
              <td style="padding:8px 12px;">{dte} / {km.get('sizingDte', config.get('exitDte', '?'))}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Lot Size</td>
              <td style="padding:8px 12px;">{LOT_SIZES[underlying]}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Final Lots</td>
              <td style="padding:8px 12px;font-weight:700;font-size:15px;color:{navy};">{sizeResult['finalLots']}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Total Quantity</td>
              <td style="padding:8px 12px;">{sizeResult['finalLots'] * LOT_SIZES[underlying]}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Allowed Lots (formula)</td>
              <td style="padding:8px 12px;">{sizeResult['allowedLots']}  (max cap: {config['maxLots']})</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">DailyVol / Lot</td>
              <td style="padding:8px 12px;">\u20B9{_fmt(sizeResult['dailyVolPerLot'])}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Daily Vol Budget</td>
              <td style="padding:8px 12px;">\u20B9{_fmt(DAILY_VOL_BUDGETS[underlying], 0)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">SL Trigger / Order %</td>
              <td style="padding:8px 12px;">{config['stopLossTriggerPercent']}% / {config['stopLossOrderPlacePercent']}%</td>
            </tr>
          </table>
        </div>

        <!-- Sizing Formula -->
        <div style="padding:20px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Position Sizing Formula
          </h2>

          <!-- Step 1: dailyVolPerLot -->
          <div style="background:{grey_bg};border:1px solid {border_col};border-radius:6px;padding:16px 18px;margin-bottom:12px;">
            <p style="margin:0 0 6px;font-weight:700;font-size:13px;color:{navy};">Step 1: Daily Volatility Per Lot</p>
            <p style="margin:0 0 4px;font-size:11px;color:#666;">How much P&amp;L volatility does one lot produce on a worst-case day?</p>
            <table style="border-collapse:collapse;font-size:13px;margin-top:8px;">
              <tr>
                <td style="padding:4px 0;font-family:monospace;color:#333;">dailyVolPerLot</td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#333;">K &times; combinedPremium &times; lotSize</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#555;">{_fmt(kValue, 4)} &times; {_fmt(sizeResult['combinedPremium'])} &times; {LOT_SIZES[underlying]}</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;font-weight:700;font-size:15px;color:{navy};">\u20B9{_fmt(sizeResult['dailyVolPerLot'])}</td>
              </tr>
            </table>
          </div>

          <!-- Step 2: allowedLots -->
          <div style="background:{grey_bg};border:1px solid {border_col};border-radius:6px;padding:16px 18px;margin-bottom:12px;">
            <p style="margin:0 0 6px;font-weight:700;font-size:13px;color:{navy};">Step 2: Allowed Lots</p>
            <p style="margin:0 0 4px;font-size:11px;color:#666;">How many lots fit within the daily volatility budget?</p>
            <table style="border-collapse:collapse;font-size:13px;margin-top:8px;">
              <tr>
                <td style="padding:4px 0;font-family:monospace;color:#333;">allowedLots</td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#333;">floor(budget / dailyVolPerLot)</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#555;">floor(\u20B9{_fmt(DAILY_VOL_BUDGETS[underlying], 0)} / \u20B9{_fmt(sizeResult['dailyVolPerLot'])})</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;font-weight:700;font-size:15px;color:{navy};">{sizeResult['allowedLots']} lots</td>
              </tr>
            </table>
          </div>

          <!-- Step 3: finalLots -->
          <div style="background:#E8F5E9;border:2px solid {green};border-radius:6px;padding:16px 18px;">
            <p style="margin:0 0 6px;font-weight:700;font-size:13px;color:{navy};">Step 3: Final Lots (capped)</p>
            <p style="margin:0 0 4px;font-size:11px;color:#666;">Apply the strategy&rsquo;s hard maximum lot cap.</p>
            <table style="border-collapse:collapse;font-size:13px;margin-top:8px;">
              <tr>
                <td style="padding:4px 0;font-family:monospace;color:#333;">finalLots</td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#333;">min(allowedLots, maxLots)</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-family:monospace;color:#555;">min({sizeResult['allowedLots']}, {config['maxLots']})</td>
              </tr>
              <tr>
                <td style="padding:4px 0;"></td>
                <td style="padding:4px 8px;color:#666;">=</td>
                <td style="padding:4px 0;font-weight:700;font-size:18px;color:{green};">{sizeResult['finalLots']} lots &nbsp;({sizeResult['finalLots'] * LOT_SIZES[underlying]} qty)</td>
              </tr>
            </table>
          </div>
        </div>

        <!-- Dynamic K Breakdown -->
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            K Value &mdash; {kSourceLabel}
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">K for Sizing (final)</td>
              <td style="padding:8px 12px;font-weight:700;font-size:15px;color:{navy};">{_fmt(kValue, 4)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">K Source</td>
              <td style="padding:8px 12px;">{kSourceLabel}</td>
            </tr>"""

    if km.get("source") == "dynamic":
        html += f"""
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">kBase (normal day)</td>
              <td style="padding:8px 12px;">{_fmt(km.get('kBase'), 4)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">kStressMove (1.5\u00D7 move)</td>
              <td style="padding:8px 12px;">{_fmt(km.get('kStressMove'), 4)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">kStressVol (IV spike)</td>
              <td style="padding:8px 12px;">{_fmt(km.get('kStressVol'), 4)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">kCrash (move + spike)</td>
              <td style="padding:8px 12px;">{_fmt(km.get('kCrash'), 4)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Binding Scenario</td>
              <td style="padding:8px 12px;font-weight:600;color:{accent};">{bindingScenario}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Static K (reference)</td>
              <td style="padding:8px 12px;">{_fmt(km.get('staticK'), 2)}</td>
            </tr>
          </table>
        </div>"""

        # ── kCrash Worked Calculation ──
        pnl = km.get("pnlBreakdown", {})
        stressMove = float(km.get("expectedMove", 0)) * STRESS_MOVE_MULTIPLIER
        # The premium used inside computeDynamicK (mid-price from quotes, not LTP)
        kPremiumUsed = (float(km.get("cePremiumUsed", 0)) + float(km.get("pePremiumUsed", 0)))
        html += f"""
        <div style="padding:20px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            kCrash Worked Calculation
          </h2>
          <p style="margin:0 0 10px;color:#555;font-size:12px;">
            kCrash = |P&amp;L from 1.5&times; spot move + IV shock| &divide; combined premium
          </p>

          <table style="width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;" colspan="2">Step 1: IV Shock Build-up</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;width:55%;">Base shock (DTE)</td>
              <td style="padding:6px 10px;">+{_fmt(km.get('ivShockBase'), 0)} vp</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;">VIX addon (VIX = {_fmt(km.get('vixLevel'))})</td>
              <td style="padding:6px 10px;">+{_fmt(km.get('vixAddon'), 0)} vp</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;">Intraday move addon ({_pct(km.get('intradayMovePct'))} move)</td>
              <td style="padding:6px 10px;">+{_fmt(km.get('intradayAddon'), 0)} vp</td>
            </tr>
            <tr style="border-top:2px solid {accent};">
              <td style="padding:6px 10px;font-weight:700;">Total IV Shock (cap: {IV_SHOCK_CAP_VP}vp)</td>
              <td style="padding:6px 10px;font-weight:700;">{_fmt(km.get('ivShockApplied'), 0)} vp ({_fmt(float(km.get('ivShockApplied', 0)) / 100, 4)} decimal)</td>
            </tr>
          </table>"""

        # ── Reference lookup tables with active row highlighted ──
        highlight = f"background:{accent};color:#FFF;font-weight:600;"
        sizingDte = km.get("sizingDte", "")
        vixLevel = float(km.get("vixLevel", 0)) if km.get("vixLevel") else 0
        intradayPct = float(km.get("intradayMovePct", 0)) if km.get("intradayMovePct") else 0

        # DTE base shock table
        dteRows = ""
        for lo, hi, vp in IV_SHOCK_TABLE:
            label = f"{lo} DTE" if lo == hi else f"{lo}\u2013{hi} DTE"
            isActive = isinstance(sizingDte, (int, float)) and lo <= sizingDte <= hi
            style = highlight if isActive else ""
            arrow = " \u25C0" if isActive else ""
            dteRows += f'<tr><td style="padding:4px 10px;{style}">{label}</td><td style="padding:4px 10px;text-align:center;{style}">{vp} vp{arrow}</td></tr>'

        # VIX addon table
        vixRows = ""
        for lo, hi, vp in VIX_ADDON_TABLE:
            hiLabel = f"{hi}" if hi < 9000 else "+"
            label = f"VIX {lo}\u2013{hiLabel}" if hi < 9000 else f"VIX {lo}+"
            isActive = lo <= vixLevel < hi
            style = highlight if isActive else ""
            arrow = " \u25C0" if isActive else ""
            vixRows += f'<tr><td style="padding:4px 10px;{style}">{label}</td><td style="padding:4px 10px;text-align:center;{style}">+{vp} vp{arrow}</td></tr>'

        # Intraday move addon table
        moveRows = ""
        for lo, hi, vp in INTRADAY_MOVE_ADDON_TABLE:
            hiLabel = f"{hi}%" if hi < 9000 else "+"
            label = f"{lo}\u2013{hiLabel}" if hi < 9000 else f"{lo}%+"
            isActive = lo <= intradayPct < hi
            style = highlight if isActive else ""
            arrow = " \u25C0" if isActive else ""
            moveRows += f'<tr><td style="padding:4px 10px;{style}">{label}</td><td style="padding:4px 10px;text-align:center;{style}">+{vp} vp{arrow}</td></tr>'

        html += f"""
          <div style="display:flex;gap:8px;margin-top:10px;">
          <table style="flex:1;border-collapse:collapse;font-size:11px;">
            <tr style="background:{navy};color:#FFF;"><td style="padding:4px 10px;font-weight:600;" colspan="2">Base Shock by DTE</td></tr>
            {dteRows}
          </table>
          <table style="flex:1;border-collapse:collapse;font-size:11px;">
            <tr style="background:{navy};color:#FFF;"><td style="padding:4px 10px;font-weight:600;" colspan="2">VIX Addon</td></tr>
            {vixRows}
          </table>
          <table style="flex:1;border-collapse:collapse;font-size:11px;">
            <tr style="background:{navy};color:#FFF;"><td style="padding:4px 10px;font-weight:600;" colspan="2">Intraday Move Addon</td></tr>
            {moveRows}
          </table>
          </div>

          <table style="width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;margin-top:12px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;" colspan="2">Step 2: Stress Move</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;width:55%;">Expected 1&sigma; move</td>
              <td style="padding:6px 10px;">{_fmt(km.get('expectedMove'))} pts</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;">Stress multiplier</td>
              <td style="padding:6px 10px;">&times; {STRESS_MOVE_MULTIPLIER}</td>
            </tr>
            <tr style="border-top:2px solid {accent};">
              <td style="padding:6px 10px;font-weight:700;">Crash move (&Delta;S)</td>
              <td style="padding:6px 10px;font-weight:700;">{_fmt(stressMove)} pts</td>
            </tr>
          </table>

          <table style="width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;margin-top:12px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;" colspan="3">Step 3: Crash P&amp;L (Taylor Expansion at 1.5&times; move + IV shock)</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;width:45%;">&delta; &times; &Delta;S</td>
              <td style="padding:6px 10px;width:25%;font-size:11px;color:#666;">{_fmt(km.get('posGamma',0), 6).replace('-','')}&hellip; &times; {_fmt(stressMove)}</td>
              <td style="padding:6px 10px;text-align:right;">{_fmt(pnl.get('crashDeltaPnl'), 2)}</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;">&frac12; &times; &Gamma; &times; &Delta;S&sup2;</td>
              <td style="padding:6px 10px;font-size:11px;color:#666;">0.5 &times; {_fmt(km.get('posGamma'), 6)} &times; {_fmt(stressMove)}&sup2;</td>
              <td style="padding:6px 10px;text-align:right;color:{red};">{_fmt(pnl.get('crashGammaPnl'), 2)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;">&nu; &times; &Delta;&sigma;</td>
              <td style="padding:6px 10px;font-size:11px;color:#666;">{_fmt(km.get('posVega'))} &times; {_fmt(float(km.get('ivShockApplied', 0)) / 100, 4)}</td>
              <td style="padding:6px 10px;text-align:right;color:{red};">{_fmt(pnl.get('crashVegaPnl'), 2)}</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;">&theta; &times; 1 day</td>
              <td style="padding:6px 10px;font-size:11px;color:#666;">{_fmt(km.get('posTheta'))} &times; 1</td>
              <td style="padding:6px 10px;text-align:right;color:{green};">+{_fmt(pnl.get('crashThetaPnl'), 2)}</td>
            </tr>
            <tr style="border-top:2px solid {accent};">
              <td style="padding:6px 10px;font-weight:700;" colspan="2">Crash P&amp;L (net)</td>
              <td style="padding:6px 10px;font-weight:700;text-align:right;color:{red};">{_fmt(pnl.get('crashPnl'), 2)}</td>
            </tr>
          </table>

          <table style="width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;margin-top:12px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;" colspan="3">Step 4: K Ratio</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;width:45%;">Combined premium (mid-price)</td>
              <td style="padding:6px 10px;font-size:11px;color:#666;">{_fmt(km.get('cePremiumUsed'))} + {_fmt(km.get('pePremiumUsed'))}</td>
              <td style="padding:6px 10px;text-align:right;">\u20B9{_fmt(kPremiumUsed)}</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;">|crashPnl| / premium</td>
              <td style="padding:6px 10px;font-size:11px;color:#666;">{_fmt(abs(float(pnl.get('crashPnl', 0))), 2)} / {_fmt(kPremiumUsed)}</td>
              <td style="padding:6px 10px;text-align:right;font-weight:700;font-size:14px;color:{navy};">{_fmt(km.get('kCrash'), 4)}</td>
            </tr>
          </table>

          <table style="width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;margin-top:12px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;" colspan="3">Step 5: IV Shock Impact on kCrash</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;width:45%;">kStressMove (1.5&times; move, <b>no</b> shock)</td>
              <td style="padding:6px 10px;font-size:11px;color:#666;">{_fmt(abs(float(pnl.get('stressMovePnl', 0))), 2)} / {_fmt(kPremiumUsed)}</td>
              <td style="padding:6px 10px;text-align:right;">{_fmt(km.get('kStressMove'), 4)}</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;">Vega P&amp;L from {_fmt(km.get('ivShockApplied'), 0)}vp shock</td>
              <td style="padding:6px 10px;font-size:11px;color:#666;">{_fmt(km.get('posVega'))} &times; {_fmt(float(km.get('ivShockApplied', 0)) / 100, 4)}</td>
              <td style="padding:6px 10px;text-align:right;color:{red};">{_fmt(pnl.get('crashVegaPnl'), 2)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;">Shock adds to k</td>
              <td style="padding:6px 10px;font-size:11px;color:#666;">{_fmt(abs(float(pnl.get('crashVegaPnl', 0))), 2)} / {_fmt(kPremiumUsed)}</td>
              <td style="padding:6px 10px;text-align:right;color:{red};font-weight:600;">+{_fmt(abs(float(pnl.get('crashVegaPnl', 0))) / kPremiumUsed if kPremiumUsed > 0 else 0, 4)}</td>
            </tr>
            <tr style="border-top:2px solid {accent};">
              <td style="padding:6px 10px;font-weight:700;">kCrash = kStressMove + shock impact</td>
              <td style="padding:6px 10px;font-size:11px;color:#666;">{_fmt(km.get('kStressMove'), 4)} + {_fmt(abs(float(pnl.get('crashVegaPnl', 0))) / kPremiumUsed if kPremiumUsed > 0 else 0, 4)}</td>
              <td style="padding:6px 10px;text-align:right;font-weight:700;font-size:14px;color:{navy};">{_fmt(km.get('kCrash'), 4)}</td>
            </tr>
          </table>
        </div>"""

    elif km.get("source") == "static_fallback":
        html += f"""
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Fallback Reason</td>
              <td style="padding:8px 12px;color:{red};">{km.get('fallbackReason', 'N/A')}</td>
            </tr>
          </table>
        </div>"""
    else:
        # static source — close the K value table
        html += """
          </table>
        </div>"""

    # ── Greeks & Market Data (dynamic only) ──
    if km.get("source") == "dynamic":
        html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Greeks &amp; Market Data
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">Avg IV</td>
              <td style="padding:8px 12px;">{_pct(float(km.get('avgIV', 0)) * 100 if km.get('avgIV') else '')}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">CE IV / PE IV</td>
              <td style="padding:8px 12px;">{_pct(float(km.get('ceIV', 0)) * 100 if km.get('ceIV') else '')} / {_pct(float(km.get('peIV', 0)) * 100 if km.get('peIV') else '')}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Expected 1\u03C3 Move</td>
              <td style="padding:8px 12px;">{_fmt(km.get('expectedMove'))} pts</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Position Gamma</td>
              <td style="padding:8px 12px;">{_fmt(km.get('posGamma'), 6)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Position Theta</td>
              <td style="padding:8px 12px;">{_fmt(km.get('posTheta'))}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Position Vega</td>
              <td style="padding:8px 12px;">{_fmt(km.get('posVega'))}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">VIX Level</td>
              <td style="padding:8px 12px;">{_fmt(km.get('vixLevel'))}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Intraday Move</td>
              <td style="padding:8px 12px;">{_pct(km.get('intradayMovePct'))}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">IV Shock Build-up</td>
              <td style="padding:8px 12px;font-family:monospace;font-size:12px;">{ivShockStr}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">T (years to expiry)</td>
              <td style="padding:8px 12px;">{_fmt(km.get('timeToExpiryYears'), 6)}</td>
            </tr>
          </table>

          <!-- Quote Quality -->
          <table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:12px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;">Leg</td>
              <td style="padding:6px 10px;font-weight:600;">Bid</td>
              <td style="padding:6px 10px;font-weight:600;">Ask</td>
              <td style="padding:6px 10px;font-weight:600;">Spread %</td>
              <td style="padding:6px 10px;font-weight:600;">Source</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;">CE</td>
              <td style="padding:6px 10px;">{_fmt(km.get('ceBid'))}</td>
              <td style="padding:6px 10px;">{_fmt(km.get('ceAsk'))}</td>
              <td style="padding:6px 10px;">{_pct(km.get('ceSpreadPct'))}</td>
              <td style="padding:6px 10px;">{km.get('cePremiumSource', 'N/A')}</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;">PE</td>
              <td style="padding:6px 10px;">{_fmt(km.get('peBid'))}</td>
              <td style="padding:6px 10px;">{_fmt(km.get('peAsk'))}</td>
              <td style="padding:6px 10px;">{_pct(km.get('peSpreadPct'))}</td>
              <td style="padding:6px 10px;">{km.get('pePremiumSource', 'N/A')}</td>
            </tr>
          </table>
        </div>"""

    # ── Greeks & IV Calculation Methodology (dynamic only) ──
    if km.get("source") == "dynamic":
        ceG = km.get("ceGreeks", {})
        peG = km.get("peGreeks", {})
        _spot = km.get("spot", "?")
        _ceK = km.get("ceStrike", "?")
        _peK = km.get("peStrike", "?")
        _T = km.get("timeToExpiryYears", "?")
        _ceIV = km.get("ceIV", "?")
        _peIV = km.get("peIV", "?")
        _cePrem = km.get("cePremiumUsed", "?")
        _pePrem = km.get("pePremiumUsed", "?")
        _r = RISK_FREE_RATE

        # Compute d1 values and BS verification prices for display
        try:
            import math as _m
            _TVal = float(_T)
            _sqrtT = _m.sqrt(max(_TVal, 1e-10))

            _ceIVVal = float(_ceIV)
            _d1_ce = (_m.log(float(_spot) / float(_ceK)) + (float(_r) + 0.5 * _ceIVVal ** 2) * _TVal) / (_ceIVVal * _sqrtT)
            _d2_ce = _d1_ce - _ceIVVal * _sqrtT
            _nd1_ce = _normcdf(_d1_ce)
            _nd2_ce = _normcdf(_d2_ce)
            _npd1_ce = _normpdf(_d1_ce)

            _peIVVal = float(_peIV)
            _d1_pe = (_m.log(float(_spot) / float(_peK)) + (float(_r) + 0.5 * _peIVVal ** 2) * _TVal) / (_peIVVal * _sqrtT)
            _d2_pe = _d1_pe - _peIVVal * _sqrtT
            _nd1_pe = _normcdf(_d1_pe)
            _nd2_pe = _normcdf(_d2_pe)
            _npd1_pe = _normpdf(_d1_pe)

            # Verify: plug solved IV back into BS to reproduce market premium
            _ceBSPrice = bsPrice(float(_spot), float(_ceK), _TVal, _ceIVVal, "CE", float(_r))
            _peBSPrice = bsPrice(float(_spot), float(_peK), _TVal, _peIVVal, "PE", float(_r))

            d1_ce_str = _fmt(_d1_ce, 6)
            d2_ce_str = _fmt(_d2_ce, 6)
            nd1_ce_str = _fmt(_nd1_ce, 6)
            nd2_ce_str = _fmt(_nd2_ce, 6)
            npd1_ce_str = _fmt(_npd1_ce, 6)
            ceBSStr = _fmt(_ceBSPrice, 2)
            ceErrStr = _fmt(abs(_ceBSPrice - float(_cePrem)), 4)

            d1_pe_str = _fmt(_d1_pe, 6)
            d2_pe_str = _fmt(_d2_pe, 6)
            nd1_pe_str = _fmt(_nd1_pe, 6)
            nd2_pe_str = _fmt(_nd2_pe, 6)
            npd1_pe_str = _fmt(_npd1_pe, 6)
            peBSStr = _fmt(_peBSPrice, 2)
            peErrStr = _fmt(abs(_peBSPrice - float(_pePrem)), 4)
        except Exception:
            d1_ce_str = d2_ce_str = nd1_ce_str = nd2_ce_str = npd1_ce_str = "err"
            d1_pe_str = d2_pe_str = nd1_pe_str = nd2_pe_str = npd1_pe_str = "err"
            ceBSStr = peBSStr = ceErrStr = peErrStr = "err"

        html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Greeks &amp; IV &mdash; Calculation Methodology
          </h2>

          <!-- IV Solver -->
          <div style="background:{grey_bg};border-left:4px solid {accent};padding:12px 16px;margin-bottom:14px;">
            <p style="margin:0 0 6px;font-weight:600;font-size:13px;color:{navy};">Step 1: Implied Volatility Solver</p>
            <p style="margin:0 0 8px;font-size:12px;color:#555;line-height:1.6;">
              IV is the &sigma; that makes the Black-Scholes theoretical price equal the observed market premium.
              We solve for &sigma; numerically &mdash; there is no closed-form inverse.
            </p>
            <p style="margin:0 0 8px;font-size:12px;color:#555;line-height:1.6;">
              <b>r = {_fmt(_r * 100, 1)}% (annualised risk-free rate)</b> &mdash; Indian 10Y government bond yield, used for
              discounting in the BS formula. Affects option pricing through the drift term and strike discounting.
            </p>
            <p style="margin:0;font-size:12px;color:#555;line-height:1.8;">
              <b>Phase 1 &mdash; Newton-Raphson</b> (fast, up to 50 iterations):<br>
              <span style="font-family:monospace;font-size:11px;margin-left:12px;">
                &sigma;<sub>0</sub> = 0.30 (initial guess)
              </span><br>
              <span style="font-family:monospace;font-size:11px;margin-left:12px;">
                &sigma;<sub>n+1</sub> = &sigma;<sub>n</sub> &minus; [BS(S, K, T, &sigma;<sub>n</sub>, r) &minus; P<sub>market</sub>] / vega(&sigma;<sub>n</sub>)
              </span><br>
              <span style="font-family:monospace;font-size:11px;margin-left:12px;">
                converges when |BS(&sigma;) &minus; P<sub>market</sub>| &lt; 10<sup>&minus;6</sup>
              </span><br>
              <b>Phase 2 &mdash; Bisection fallback</b> (robust, if Newton fails):<br>
              <span style="font-family:monospace;font-size:11px;margin-left:12px;">
                binary search on [{_fmt(IV_SOLVER_MIN, 2)}, {_fmt(IV_SOLVER_MAX, 1)}], up to 100 iterations
              </span>
            </p>
          </div>

          <!-- IV Solver Inputs -->
          <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:6px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;">Input</td>
              <td style="padding:6px 10px;font-weight:600;">CE</td>
              <td style="padding:6px 10px;font-weight:600;">PE</td>
              <td style="padding:6px 10px;font-weight:600;">Description</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">P<sub>market</sub></td>
              <td style="padding:6px 10px;">\u20B9{_fmt(_cePrem)}</td>
              <td style="padding:6px 10px;">\u20B9{_fmt(_pePrem)}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">Mid-price from order book (bid+ask)/2</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;font-weight:600;">S (Spot)</td>
              <td style="padding:6px 10px;">{_fmt(_spot)}</td>
              <td style="padding:6px 10px;">{_fmt(_spot)}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">Underlying last traded price</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">K (Strike)</td>
              <td style="padding:6px 10px;">{_fmt(_ceK, 0)}</td>
              <td style="padding:6px 10px;">{_fmt(_peK, 0)}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">Option strike price</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;font-weight:600;">T</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(_T, 6)}</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(_T, 6)}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">Time to expiry in years (sizingDTE / 252)</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">r</td>
              <td style="padding:6px 10px;">{_fmt(_r * 100, 1)}%</td>
              <td style="padding:6px 10px;">{_fmt(_r * 100, 1)}%</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">Annualised risk-free rate (India 10Y bond)</td>
            </tr>
          </table>

          <!-- IV Solver Output + Verification -->
          <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px;">
            <tr style="background:{accent};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;">Result</td>
              <td style="padding:6px 10px;font-weight:600;">CE</td>
              <td style="padding:6px 10px;font-weight:600;">PE</td>
              <td style="padding:6px 10px;font-weight:600;">Verification</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">Solved &sigma; (IV)</td>
              <td style="padding:6px 10px;font-weight:700;color:{navy};font-size:13px;">{_pct(float(_ceIV) * 100) if _ceIV != '?' else '?'}</td>
              <td style="padding:6px 10px;font-weight:700;color:{navy};font-size:13px;">{_pct(float(_peIV) * 100) if _peIV != '?' else '?'}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">The &sigma; that satisfies BS = P<sub>market</sub></td>
            </tr>
            <tr>
              <td style="padding:6px 10px;font-weight:600;">BS(solved &sigma;)</td>
              <td style="padding:6px 10px;font-family:monospace;">\u20B9{ceBSStr}</td>
              <td style="padding:6px 10px;font-family:monospace;">\u20B9{peBSStr}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">Plug IV back into BS formula</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">|Error|</td>
              <td style="padding:6px 10px;font-family:monospace;color:{green};">{ceErrStr}</td>
              <td style="padding:6px 10px;font-family:monospace;color:{green};">{peErrStr}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">|BS(&sigma;) &minus; P<sub>market</sub>| &nbsp;(should be &lt; 0.001)</td>
            </tr>
          </table>

          <!-- d1 / d2 intermediates -->
          <div style="background:{grey_bg};border-left:4px solid {accent};padding:12px 16px;margin-bottom:14px;">
            <p style="margin:0 0 6px;font-weight:600;font-size:13px;color:{navy};">Step 2: Black-Scholes Intermediates (d1, d2)</p>
            <p style="margin:0;font-size:12px;color:#555;line-height:1.6;font-family:monospace;">
              d1 = [ln(S/K) + (r + &frac12;&sigma;&sup2;)T] / (&sigma;&radic;T)<br>
              d2 = d1 &minus; &sigma;&radic;T
            </p>
          </div>

          <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;">Intermediate</td>
              <td style="padding:6px 10px;font-weight:600;">CE</td>
              <td style="padding:6px 10px;font-weight:600;">PE</td>
              <td style="padding:6px 10px;font-weight:600;">What it means</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">d1</td>
              <td style="padding:6px 10px;font-family:monospace;">{d1_ce_str}</td>
              <td style="padding:6px 10px;font-family:monospace;">{d1_pe_str}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">How many std devs the option is in-the-money (adjusted for drift). Drives delta and the pricing formula.</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;font-weight:600;">d2</td>
              <td style="padding:6px 10px;font-family:monospace;">{d2_ce_str}</td>
              <td style="padding:6px 10px;font-family:monospace;">{d2_pe_str}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">d1 shifted by volatility over time. Determines the probability-weighted present value of paying the strike at expiry.</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">N(d1)</td>
              <td style="padding:6px 10px;font-family:monospace;">{nd1_ce_str}</td>
              <td style="padding:6px 10px;font-family:monospace;">{nd1_pe_str}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">CDF of standard normal at d1. For a CE this equals delta &mdash; the probability-weighted hedge ratio. CE &delta; = N(d1), PE &delta; = N(d1) &minus; 1.</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;font-weight:600;">N(d2)</td>
              <td style="padding:6px 10px;font-family:monospace;">{nd2_ce_str}</td>
              <td style="padding:6px 10px;font-family:monospace;">{nd2_pe_str}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">CDF at d2. Risk-neutral probability that the option finishes in-the-money. Used to discount the strike payment.</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">N&prime;(d1)</td>
              <td style="padding:6px 10px;font-family:monospace;">{npd1_ce_str}</td>
              <td style="padding:6px 10px;font-family:monospace;">{npd1_pe_str}</td>
              <td style="padding:6px 10px;color:#666;font-size:11px;">PDF of standard normal at d1. Measures how sensitive N(d1) is to small changes &mdash; directly feeds into gamma and vega.</td>
            </tr>
          </table>

          <!-- Greeks Formulas -->
          <div style="background:{grey_bg};border-left:4px solid {accent};padding:12px 16px;margin-bottom:14px;">
            <p style="margin:0 0 6px;font-weight:600;font-size:13px;color:{navy};">Step 3: Greeks Formulas (Black-Scholes, no dividends)</p>
            <table style="border-collapse:collapse;font-size:11px;font-family:monospace;color:#555;">
              <tr><td style="padding:3px 8px 3px 0;font-weight:600;color:#333;">Delta</td><td style="padding:3px 0;">CE: N(d1) &nbsp;|&nbsp; PE: N(d1) &minus; 1</td></tr>
              <tr><td style="padding:3px 8px 3px 0;font-weight:600;color:#333;">Gamma</td><td style="padding:3px 0;">N'(d1) / (S &times; &sigma; &times; &radic;T)</td></tr>
              <tr><td style="padding:3px 8px 3px 0;font-weight:600;color:#333;">Vega</td><td style="padding:3px 0;">S &times; N'(d1) &times; &radic;T &nbsp;(raw &part;V/&part;&sigma;)</td></tr>
              <tr><td style="padding:3px 8px 3px 0;font-weight:600;color:#333;">Theta</td><td style="padding:3px 0;">CE: &minus;[S&middot;N'(d1)&middot;&sigma;/(2&radic;T)] &minus; r&middot;K&middot;e<sup>&minus;rT</sup>&middot;N(d2) &nbsp;/ 365</td></tr>
              <tr><td style="padding:3px 0;"></td><td style="padding:3px 0;">PE: &minus;[S&middot;N'(d1)&middot;&sigma;/(2&radic;T)] + r&middot;K&middot;e<sup>&minus;rT</sup>&middot;N(&minus;d2) &nbsp;/ 365</td></tr>
            </table>
          </div>

          <!-- Individual Greeks Results -->
          <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 10px;font-weight:600;">Greek</td>
              <td style="padding:6px 10px;font-weight:600;">CE</td>
              <td style="padding:6px 10px;font-weight:600;">PE</td>
              <td style="padding:6px 10px;font-weight:600;">Position (short: &minus;CE &minus; PE)</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">Delta</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(ceG.get('delta'), 6)}</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(peG.get('delta'), 6)}</td>
              <td style="padding:6px 10px;font-family:monospace;font-weight:600;">{_fmt(-(float(ceG.get('delta', 0)) + float(peG.get('delta', 0))), 6)}</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;font-weight:600;">Gamma</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(ceG.get('gamma'), 8)}</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(peG.get('gamma'), 8)}</td>
              <td style="padding:6px 10px;font-family:monospace;font-weight:600;">{_fmt(km.get('posGamma'), 8)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:6px 10px;font-weight:600;">Theta</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(ceG.get('theta'), 4)}</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(peG.get('theta'), 4)}</td>
              <td style="padding:6px 10px;font-family:monospace;font-weight:600;">{_fmt(km.get('posTheta'), 4)}</td>
            </tr>
            <tr>
              <td style="padding:6px 10px;font-weight:600;">Vega</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(ceG.get('vega'), 4)}</td>
              <td style="padding:6px 10px;font-family:monospace;">{_fmt(peG.get('vega'), 4)}</td>
              <td style="padding:6px 10px;font-family:monospace;font-weight:600;">{_fmt(km.get('posVega'), 4)}</td>
            </tr>
          </table>

          <!-- Position Greeks Interpretation -->
          <div style="background:#FFF8E1;border-left:4px solid #FFC107;padding:12px 14px;font-size:11px;color:#555;line-height:1.7;">
            <p style="margin:0 0 8px;font-weight:700;font-size:12px;color:#333;">How to read these (short straddle/strangle — you are the seller):</p>
            <p style="margin:0 0 4px;">
              <b>Delta ({_fmt(-(float(ceG.get('delta', 0)) + float(peG.get('delta', 0))), 4)})</b> &mdash;
              P&amp;L change per 1-point move in the underlying. Near-zero for ATM straddles (CE and PE deltas roughly cancel).
            </p>
            <p style="margin:0 0 4px;">
              <b>Gamma ({_fmt(km.get('posGamma'), 6)})</b> &mdash;
              How fast delta changes per 1-point move. <b>Negative</b> means large moves hurt you &mdash; the bigger the move, the worse it gets (convexity working against you).
            </p>
            <p style="margin:0 0 4px;">
              <b>Theta ({_fmt(km.get('posTheta'), 2)})</b> &mdash;
              Premium you earn per calendar day from time decay. <b>Positive</b> because you sold the options &mdash; time passing makes them cheaper, which is your profit.
            </p>
            <p style="margin:0;">
              <b>Vega ({_fmt(km.get('posVega'), 2)})</b> &mdash;
              P&amp;L change if IV moves by 1.00 (100pp). In practice: <b>per 1 vol point (1pp) rise, you lose \u20B9{_fmt(abs(float(km.get('posVega', 0))) * 0.01, 2)}</b>.
              Your crash scenario applies a {_fmt(km.get('ivShockApplied', 0), 0)}vp shock: {_fmt(abs(float(km.get('posVega', 0))), 2)} &times; {_fmt(float(km.get('ivShockApplied', 0)) / 100, 2)} = <b>\u20B9{_fmt(abs(float(km.get('posVega', 0))) * float(km.get('ivShockApplied', 0)) / 100, 2)} loss</b>.
              Negative because as a seller, rising IV makes the options you sold more expensive.
            </p>
          </div>
        </div>"""

    # ── GTT Orders ──
    gttStatusColor = green if gttOk else red
    gttIdDisplay = ", ".join(str(g) for g in gttIds) if gttIds else "None"
    html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            GTT Stop-Loss Orders
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">GTT Status</td>
              <td style="padding:8px 12px;font-weight:600;color:{gttStatusColor};">{'Set Successfully' if gttOk else 'FAILED'}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">GTT IDs</td>
              <td style="padding:8px 12px;font-family:monospace;">{gttIdDisplay}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">SL Trigger %</td>
              <td style="padding:8px 12px;">{config['stopLossTriggerPercent']}%</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">SL Order Place %</td>
              <td style="padding:8px 12px;">{config['stopLossOrderPlacePercent']}%</td>
            </tr>
          </table>
        </div>"""

    # ── Overall Position Across Systems ──
    html += f"""
        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Overall Position Across Systems
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:12px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:6px 8px;font-weight:600;">Underlying</td>
              <td style="padding:6px 8px;font-weight:600;">State</td>
              <td style="padding:6px 8px;font-weight:600;">Strategy</td>
              <td style="padding:6px 8px;font-weight:600;text-align:center;">Lots</td>
              <td style="padding:6px 8px;font-weight:600;">Contracts</td>
              <td style="padding:6px 8px;font-weight:600;">Integrity</td>
              <td style="padding:6px 8px;font-weight:600;">GTT</td>
              <td style="padding:6px 8px;font-weight:600;">GTT IDs</td>
            </tr>
            {positionRows}
          </table>
        </div>"""

    # ── Footer ──
    html += f"""
        <div style="padding:20px 28px;margin-top:20px;border-top:1px solid {border_col};text-align:center;">
          <p style="margin:0;color:#999;font-size:11px;">
            PlaceOptionsSystemsV2 &bull; Auto-generated entry notification &bull; {now.strftime('%d %b %Y %H:%M:%S')}
          </p>
        </div>
      </div>
    </body>
    </html>"""

    return html


def sendEntryEmail(strategyName, config, dte, kValue, callPremium, putPremium,
                   sizeResult, expiryDate, ceSymbol, peSymbol, kMetadata,
                   gttOk, gttIds, state):
    """Send an HTML email notification after a successful entry order placement."""
    if not EMAIL_NOTIFY_ENABLED:
        return

    try:
        underlying = config["underlying"]
        subject = (f"V2 Entry: {strategyName} | {sizeResult['finalLots']} lots | "
                   f"{underlying} | {datetime.now().strftime('%d %b %H:%M')}")

        htmlBody = buildEntryEmailHtml(
            strategyName, config, dte, kValue, callPremium, putPremium,
            sizeResult, expiryDate, ceSymbol, peSymbol, kMetadata,
            gttOk, gttIds, state)

        msg = MIMEMultipart("alternative")
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg["Subject"] = subject
        msg["X-Priority"] = "1"
        msg["X-MSMail-Priority"] = "High"
        msg["Importance"] = "High"
        msg.attach(MIMEText(htmlBody, "html"))

        server = smtplib.SMTP_SSL(EMAIL_SMTP, EMAIL_PORT)
        server.login(EMAIL_FROM, EMAIL_FROM_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        server.quit()
        print(f"[EMAIL] Entry notification sent for {strategyName}")
    except Exception as e:
        # Email failure must NEVER block the trading flow
        print(f"[EMAIL] WARNING: failed to send entry notification: {e}")
        logging.warning(f"Entry email notification failed: {e}")


def lookupK(dte, kTable):
    """Return the k multiplier for a given trading DTE from the provided k table.

    Falls back to boundary K values if DTE is outside the table range:
    - DTE above max → smallest K (most conservative sizing, largest position)
    - DTE below min → largest K (most conservative sizing, smallest position)
    """
    for minDte, maxDte, kValue in kTable:
        if minDte <= dte <= maxDte:
            return kValue
    # Fallback: use boundary K values instead of crashing
    allK = [k for _, _, k in kTable]
    maxDteInTable = max(hi for _, hi, _ in kTable)
    if dte > maxDteInTable:
        return min(allK)   # long-dated → smallest K → larger position (low daily risk)
    return max(allK)       # very short-dated → largest K → smaller position (high daily risk)


def lookupIvShock(sizingDte):
    """Return the IV shock in absolute vol points for a given sizing DTE.

    Uses IV_SHOCK_TABLE. Returns the shock as a decimal (e.g. 0.10 for 10 vol points).
    Falls back to the widest bucket if DTE is out of range.
    """
    for minDte, maxDte, shockPoints in IV_SHOCK_TABLE:
        if minDte <= sizingDte <= maxDte:
            return shockPoints / 100.0  # convert vol points to decimal
    # Fallback: use the most conservative (highest) shock
    return max(s for _, _, s in IV_SHOCK_TABLE) / 100.0


def getVixAddon(kite):
    """Fetch India VIX and return additive IV shock vol points.

    Returns:
        (addonDecimal, vixLevel) where addonDecimal is extra shock in decimal
        (e.g. 0.06 for +6 vol points), and vixLevel is the raw VIX value.
        Returns (0.0, None) on failure (fail-safe: no add-on).
    """
    try:
        vixData = kite.ltp([VIX_LTP_KEY])
        vix = float(vixData[VIX_LTP_KEY]["last_price"])
    except Exception:
        return (0.0, None)  # fail-safe: no add-on

    for lo, hi, addon in VIX_ADDON_TABLE:
        if lo <= vix < hi:
            return (addon / 100.0, round(vix, 2))
    return (max(a for _, _, a in VIX_ADDON_TABLE) / 100.0, round(vix, 2))


def getIntradayMoveAddon(kite, underlying):
    """Compute extra IV shock vol points based on intraday high-low range.

    Uses the OHLC data from kite.ohlc() to get today's high and low prices.
    The high-low range captures true intraday volatility — a market that swings
    3% down then recovers would show ~3% range, not 0% (which open-to-close gives).

    Returns:
        (addonDecimal, movePct) where addonDecimal is extra shock in decimal
        (e.g. 0.04 for +4 vol points), and movePct is the high-low range as %.
        Returns (0.0, None) on failure.
    """
    try:
        spotKey = UNDERLYING_LTP_KEY[underlying]
        ohlcData = kite.ohlc([spotKey])
        entry = ohlcData[spotKey]
        openPrice = float(entry["ohlc"]["open"])
        highPrice = float(entry["ohlc"]["high"])
        lowPrice  = float(entry["ohlc"]["low"])
    except Exception:
        return (0.0, None)  # fail-safe: no add-on

    if openPrice <= 0 or lowPrice <= 0:
        return (0.0, None)

    # Use high-low range as % of open — captures true intraday volatility
    movePct = (highPrice - lowPrice) / openPrice * 100.0

    for lo, hi, addon in INTRADAY_MOVE_ADDON_TABLE:
        if lo <= movePct < hi:
            return (addon / 100.0, round(movePct, 2))
    # Above all thresholds
    return (max(a for _, _, a in INTRADAY_MOVE_ADDON_TABLE) / 100.0, round(movePct, 2))


# ---------------------------------------------------------------------------
# SECTION 1b: Black-Scholes Pricing and Greeks (European options, no dividends)
# ---------------------------------------------------------------------------

def _normcdf(x):
    """Standard normal CDF using math.erf (no scipy dependency)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _normpdf(x):
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def bsPrice(spot, strike, T, iv, optionType, r=RISK_FREE_RATE):
    """Black-Scholes European option price (no dividends).

    Args:
        spot: underlying spot price
        strike: option strike price
        T: time to expiry in years (exact, from datetime)
        iv: annualised implied volatility as decimal (e.g. 0.14 for 14%)
        optionType: "CE" for call, "PE" for put
        r: annualised risk-free rate (default RISK_FREE_RATE)

    Returns: option price (float)

    Raises:
        ValueError: if iv <= 0
    """
    if iv <= 0:
        raise ValueError(f"bsPrice: iv must be positive, got {iv}")
    T = max(T, 1e-10)
    sqrtT = math.sqrt(T)
    d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * T) / (iv * sqrtT)
    d2 = d1 - iv * sqrtT

    if optionType == "CE":
        return spot * _normcdf(d1) - strike * math.exp(-r * T) * _normcdf(d2)
    else:
        return strike * math.exp(-r * T) * _normcdf(-d2) - spot * _normcdf(-d1)


def bsGreeks(spot, strike, T, iv, optionType, r=RISK_FREE_RATE):
    """Black-Scholes Greeks for a European option (no dividends).

    Args:
        spot, strike, T, iv, optionType, r: same as bsPrice.

    Returns dict:
        delta: ∂V/∂S
        gamma: ∂²V/∂S²
        theta: premium change per 1 calendar day (annual theta / 365)
        vega:  raw ∂V/∂σ  (so pnl_vega = vega * deltaSigma_decimal)

    Raises:
        ValueError: if iv <= 0
    """
    if iv <= 0:
        raise ValueError(f"bsGreeks: iv must be positive, got {iv}")
    T = max(T, 1e-10)
    sqrtT = math.sqrt(T)
    d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * T) / (iv * sqrtT)
    d2 = d1 - iv * sqrtT
    pdf_d1 = _normpdf(d1)

    gamma = pdf_d1 / (spot * iv * sqrtT)
    vega = spot * pdf_d1 * sqrtT  # raw ∂V/∂σ

    # Annual theta, then divide by 365 for per-calendar-day
    if optionType == "CE":
        delta = _normcdf(d1)
        theta_annual = (-(spot * pdf_d1 * iv) / (2.0 * sqrtT)
                        - r * strike * math.exp(-r * T) * _normcdf(d2))
    else:
        delta = _normcdf(d1) - 1.0
        theta_annual = (-(spot * pdf_d1 * iv) / (2.0 * sqrtT)
                        + r * strike * math.exp(-r * T) * _normcdf(-d2))

    theta = theta_annual / 365.0  # per calendar day

    return {"delta": delta, "gamma": gamma, "theta": theta, "vega": vega}


def bsImpliedVol(optionPrice, spot, strike, T, optionType, r=RISK_FREE_RATE):
    """Back out implied volatility from market premium using Newton-Raphson + bisection.

    Args:
        optionPrice: observed option market price
        spot, strike, T, optionType, r: same as bsPrice.

    Returns: annualised IV as decimal, or None if solver fails.
    """
    T = max(T, 1e-6)

    # Hard filters
    if optionPrice <= 0:
        return None
    intrinsic = max(spot - strike, 0.0) if optionType == "CE" else max(strike - spot, 0.0)
    if optionPrice < intrinsic - 0.50:
        return None

    # Phase 1: Newton-Raphson (fast convergence)
    sigma = 0.30  # initial guess
    for _ in range(50):
        price = bsPrice(spot, strike, T, sigma, optionType, r)
        vega = bsGreeks(spot, strike, T, sigma, optionType, r)["vega"]
        if vega < 1e-12:
            break  # vega too small, Newton won't converge — try bisection
        diff = price - optionPrice
        if abs(diff) < 1e-6:
            if IV_SOLVER_MIN <= sigma <= IV_SOLVER_MAX:
                return sigma
            return None
        sigma = sigma - diff / vega
        if sigma <= 0:
            break  # went negative, try bisection

    # Phase 2: Bisection fallback (robust, slower)
    lo, hi = IV_SOLVER_MIN, IV_SOLVER_MAX
    for _ in range(100):
        mid = (lo + hi) / 2.0
        price = bsPrice(spot, strike, T, mid, optionType, r)
        if abs(price - optionPrice) < 1e-6:
            return mid
        if price < optionPrice:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-8:
            break

    finalSigma = (lo + hi) / 2.0
    # Verify the final answer is reasonable
    finalPrice = bsPrice(spot, strike, T, finalSigma, optionType, r)
    if abs(finalPrice - optionPrice) < 0.50 and IV_SOLVER_MIN <= finalSigma <= IV_SOLVER_MAX:
        return finalSigma
    return None


# ---------------------------------------------------------------------------
# SECTION 1c: Quote Helpers and Instrument Lookup
# ---------------------------------------------------------------------------

def getBestPremium(quoteData):
    """Extract the best available premium from a Kite quote response.

    Preference: mid-price from best bid/ask > single-side > LTP.

    Args:
        quoteData: dict from kite.quote() for a single instrument key.

    Returns:
        (price, source, bid, ask, spreadPct) where source is "mid", "bid",
        "ask", or "ltp". bid/ask/spreadPct may be None if depth unavailable.
    """
    depth = quoteData.get("depth", {})
    buyDepth = depth.get("buy", [])
    sellDepth = depth.get("sell", [])

    bestBid = buyDepth[0].get("price", 0) if buyDepth else 0
    bestAsk = sellDepth[0].get("price", 0) if sellDepth else 0

    # Reject obviously bad depth (bid > ask means crossed book — fall through to LTP)
    if bestBid > 0 and bestAsk > 0:
        if bestBid > bestAsk:
            # Crossed book — depth is unreliable, skip to LTP
            ltp = float(quoteData.get("last_price", 0))
            return (ltp, "ltp", bestBid, bestAsk, None)
        mid = (bestBid + bestAsk) / 2.0
        spreadPct = (bestAsk - bestBid) / mid * 100.0 if mid > 0 else 999.0
        return (mid, "mid", bestBid, bestAsk, spreadPct)

    if bestBid > 0:
        return (bestBid, "bid", bestBid, None, None)

    if bestAsk > 0:
        return (bestAsk, "ask", None, bestAsk, None)

    # Fall back to LTP
    ltp = float(quoteData.get("last_price", 0))
    return (ltp, "ltp", None, None, None)


def lookupStrikeFromInstruments(tradingsymbol, exchange, kite):
    """Look up strike price from the cached instruments dump.

    Uses the authoritative instrument master — no symbol string parsing.

    Args:
        tradingsymbol: e.g. "NIFTY26MAR24000CE"
        exchange: e.g. "NFO" or "BFO"
        kite: Kite client instance (for GetInstrumentsCached)

    Returns: strike as float, or None if not found.
    """
    instruments = GetInstrumentsCached(kite, exchange)
    for inst in instruments:
        if inst.get("tradingsymbol") == tradingsymbol:
            return float(inst["strike"])
    return None


# ---------------------------------------------------------------------------
# SECTION 1d: Dynamic K Computation
# ---------------------------------------------------------------------------

def computeDynamicK(ceGreeks, peGreeks, ceIV, peIV, spot, combinedPremium,
                    lotSize, strategyType="straddle", ivShockAbsolute=0.0):
    """Compute dynamic k using scenario-based sizing: max(kBase, kStressMove, kStressVol, kCrash).

    Four scenarios, sized off the worst one:
        kBase:       1σ expected move, no IV shock  (base Greeks risk)
        kStressMove: 1.5× expected move, no IV shock (fat-tail move risk)
        kStressVol:  1σ expected move, with IV shock  (policy-driven vol stress)
        kCrash:      1.5× expected move + IV shock   (combined crash scenario)

    kForSizing = max(kBase, kStressMove, kStressVol, kCrash)

    This is scenario-based, not forecast-based: each scenario asks "what if this
    happens?" and sizing uses the worst answer. No scenario can hide behind
    another's offsetting term (e.g. theta gains can't mask gamma losses).

    NOTE: This is a one-day Taylor approximation (delta + gamma + vega + theta),
    not a full repricing engine. Near expiry, large moves can make the quadratic
    gamma term less accurate. The approximation is structurally correct but has
    limitations for very short DTE + large moves.

    Convention notes (intentional mixed convention):
        - Expected move uses 252 trading days (actual trading vol, standard for options risk)
        - Theta uses 365 calendar days (continuous time decay including weekends)
        This is deliberate — do not "fix" it.

    Args:
        ceGreeks, peGreeks: dicts with delta, gamma, theta, vega from bsGreeks()
        ceIV, peIV: annualised IV for each leg (decimal)
        spot: underlying spot price
        combinedPremium: CE premium + PE premium
        lotSize: contract lot size. Not used in current k computation (per-unit P&L
            makes lot size cancel out), retained for API symmetry / future extensions.
        strategyType: "straddle" or "single"
        ivShockAbsolute: IV shock in absolute decimal (e.g. 0.10 = 10 vol points).
            Built from additive components in resolveK.

    Returns dict or None if inputs invalid:
        kForSizing (the max), kBase, kStressMove, kStressVol, kCrash, kBindingScenario,
        kSpotSensitivity, expectedMove, avgIV, posGamma, posTheta, posVega,
        pnlBreakdown (for base scenario)
    """
    if combinedPremium <= 0 or spot <= 0:
        return None

    avgIV = (ceIV + peIV) / 2.0
    if avgIV <= 0:
        return None

    expectedMove = spot * avgIV * math.sqrt(1.0 / 252.0)  # trading-day convention

    if strategyType == "straddle":
        # Short straddle: negate because selling both legs
        posDelta = -(ceGreeks["delta"] + peGreeks["delta"])
        posGamma = -(ceGreeks["gamma"] + peGreeks["gamma"])
        posTheta = -(ceGreeks["theta"] + peGreeks["theta"])
        posVega = -(ceGreeks["vega"] + peGreeks["vega"])
    else:
        # Single leg (short): negate one leg
        greeks = ceGreeks  # caller passes the relevant leg
        posDelta = -greeks["delta"]
        posGamma = -greeks["gamma"]
        posTheta = -greeks["theta"]
        posVega = -greeks["vega"]

    deltaSigma = ivShockAbsolute  # absolute decimal (e.g. 0.10 = 10 vol points)

    def _pnl(deltaS, volShock):
        """Taylor expansion P&L per unit for a given spot move and vol shock.

        Theta is included in every scenario (including stress scenarios) because
        all are one-day horizons — even on a crash day, one day of time decay
        still occurs. This is intentional, not an oversight.
        """
        return (posDelta * deltaS
                + 0.5 * posGamma * deltaS * deltaS
                + posVega * volShock
                + posTheta * 1.0)  # theta already per calendar day

    def _rawK(pnl):
        """Convert absolute P&L to k ratio (unclamped). Clamping happens once at the end."""
        return abs(pnl) / combinedPremium

    # ── Four independent scenarios (all computed as raw, unclamped values) ──
    # Scenario 1: kBase — normal 1σ move, no IV shock
    basePnl = _pnl(expectedMove, 0.0)
    rawKBase = _rawK(basePnl)

    # Scenario 2: kStressMove — larger move (1.5×), no IV shock
    stressMovePnl = _pnl(expectedMove * STRESS_MOVE_MULTIPLIER, 0.0)
    rawKStressMove = _rawK(stressMovePnl)

    # Scenario 3: kStressVol — normal move, with policy-driven vol stress
    stressVolPnl = _pnl(expectedMove, deltaSigma)
    rawKStressVol = _rawK(stressVolPnl)

    # Scenario 4: kCrash — large move (1.5×) + IV shock combined (true "bad day")
    crashPnl = _pnl(expectedMove * STRESS_MOVE_MULTIPLIER, deltaSigma)
    rawKCrash = _rawK(crashPnl)

    # Determine binding scenario from RAW values (before clamping)
    rawScenarios = {
        "kBase": rawKBase, "kStressMove": rawKStressMove,
        "kStressVol": rawKStressVol, "kCrash": rawKCrash,
    }
    rawKForSizing = max(rawScenarios.values())
    kBindingScenario = max(rawScenarios, key=rawScenarios.get)

    # Clamp only the final sizing k (floor for prudence, ceiling for data-quality guard)
    kForSizing = max(K_FLOOR, min(K_CEILING, rawKForSizing))

    kSpotSensitivity = abs(basePnl) / abs(expectedMove) if abs(expectedMove) > 1e-10 else 0.0

    return {
        "kForSizing": round(kForSizing, 6),
        "kRaw": round(rawKForSizing, 6),         # unclamped worst scenario
        "kBase": round(rawKBase, 6),              # all scenario values are raw (unclamped)
        "kStressMove": round(rawKStressMove, 6),
        "kStressVol": round(rawKStressVol, 6),
        "kCrash": round(rawKCrash, 6),
        "kClamped": kForSizing != rawKForSizing,  # True if floor or ceiling was applied
        "kBindingScenario": kBindingScenario,
        "kSpotSensitivity": round(kSpotSensitivity, 6),
        "expectedMove": round(expectedMove, 2),
        "avgIV": round(avgIV, 6),
        "posGamma": round(posGamma, 8),
        "posTheta": round(posTheta, 4),
        "posVega": round(posVega, 4),
        "pnlBreakdown": {
            "pnlDelta": round(posDelta * expectedMove, 4),
            "pnlGamma": round(0.5 * posGamma * expectedMove * expectedMove, 4),
            "pnlVega": round(posVega * deltaSigma, 4),
            "pnlTheta": round(posTheta * 1.0, 4),
            "basePnl": round(basePnl, 4),
            "stressMovePnl": round(stressMovePnl, 4),
            "stressVolPnl": round(stressVolPnl, 4),
            "crashPnl": round(crashPnl, 4),
            # Crash-specific component P&Ls (1.5× move + IV shock)
            "crashDeltaPnl": round(posDelta * expectedMove * STRESS_MOVE_MULTIPLIER, 4),
            "crashGammaPnl": round(0.5 * posGamma * (expectedMove * STRESS_MOVE_MULTIPLIER) ** 2, 4),
            "crashVegaPnl": round(posVega * deltaSigma, 4),
            "crashThetaPnl": round(posTheta * 1.0, 4),
        },
    }


def resolveK(config, kite, ceSymbol, peSymbol, exchange, underlying,
             sizingDte, callPremium, putPremium, lotSize, expiryDate):
    """Resolve k value: dynamic if enabled and quotes are clean, static fallback otherwise.

    Args:
        config: strategy config dict (must have kTable, may have useDynamicK)
        kite: Kite client instance
        ceSymbol, peSymbol: contract trading symbols
        exchange: "NFO" or "BFO"
        underlying: "NIFTY" or "SENSEX"
        sizingDte: DTE used for static K lookup (exit DTE)
        callPremium, putPremium: premiums from fetchOptionPremiums. Not used by dynamic k
            (which fetches fresh quotes), retained for interface compatibility with executeEntry.
        lotSize: contract lot size
        expiryDate: expiry date (date object)

    Returns:
        (kValue, kMetadata) where kMetadata is a dict with source info and diagnostics.
    """
    tag = f"[{underlying}]"

    # Static K path (default)
    if not config.get("useDynamicK", False):
        staticK = lookupK(sizingDte, config["kTable"])
        return (staticK, {"source": "static", "staticK": staticK})

    # Dynamic K path
    staticK = lookupK(sizingDte, config["kTable"])  # always compute for fallback + logging

    # IV shock: additive components, capped. Feeds kStressVol and kCrash scenarios.
    # ivShock = baseShockByDte + vixAdd + realizedMoveAdd (+ termAdd + eventAdd future)
    baseIvShock = lookupIvShock(sizingDte)
    vixAddon, vixLevel = getVixAddon(kite)
    intradayAddon, intradayMovePct = getIntradayMoveAddon(kite, underlying)
    ivShockRaw = baseIvShock + vixAddon + intradayAddon
    ivShockCap = IV_SHOCK_CAP_VP / 100.0
    ivShockAbsolute = min(ivShockRaw, ivShockCap)

    print(f"{tag}[DYNAMIC-K] IV shock build-up: base={baseIvShock*100:.0f}vp "
          f"+ VIX={vixAddon*100:.0f}vp (VIX={vixLevel}) "
          f"+ intraday={intradayAddon*100:.0f}vp (range={intradayMovePct}%) "
          f"= {ivShockRaw*100:.1f}vp"
          f"{f' (CAPPED to {ivShockCap*100:.0f}vp)' if ivShockRaw > ivShockCap else ''}")

    def _fallback(reason):
        print(f"{tag}[DYNAMIC-K] Falling back to static K={staticK}: {reason}")
        return (staticK, {"source": "static_fallback", "staticK": staticK,
                          "fallbackReason": reason})

    # Step 1: Fetch spot
    try:
        spotKey = UNDERLYING_LTP_KEY[underlying]
        spotData = kite.ltp([spotKey])
        spot = float(spotData[spotKey]["last_price"])
    except Exception as e:
        return _fallback(f"spot fetch failed: {e}")

    if spot <= 0:
        return _fallback("spot price is zero or negative")

    # Step 2: Fetch full quotes for both legs
    try:
        ceKey = f"{exchange}:{ceSymbol}"
        peKey = f"{exchange}:{peSymbol}"
        quotes = kite.quote([ceKey, peKey])
        ceQuote = quotes.get(ceKey)
        peQuote = quotes.get(peKey)
        if not ceQuote or not peQuote:
            return _fallback("quote missing for one or both legs")
    except Exception as e:
        return _fallback(f"quote fetch failed: {e}")

    # Step 3: Extract premiums with quality preference
    cePremium, ceSource, ceBid, ceAsk, ceSpreadPct = getBestPremium(ceQuote)
    pePremium, peSource, peBid, peAsk, peSpreadPct = getBestPremium(peQuote)

    # Quote timestamps — track both legs, use older for staleness check
    ceQuoteTimestamp = ceQuote.get("last_trade_time", None)
    peQuoteTimestamp = peQuote.get("last_trade_time", None)
    if isinstance(ceQuoteTimestamp, str):
        try:
            ceQuoteTimestamp = datetime.fromisoformat(ceQuoteTimestamp)
        except (ValueError, TypeError):
            pass
    if isinstance(peQuoteTimestamp, str):
        try:
            peQuoteTimestamp = datetime.fromisoformat(peQuoteTimestamp)
        except (ValueError, TypeError):
            pass
    # Use the older timestamp for staleness (conservative)
    quoteTimestamp = None
    if hasattr(ceQuoteTimestamp, 'hour') and hasattr(peQuoteTimestamp, 'hour'):
        quoteTimestamp = min(ceQuoteTimestamp, peQuoteTimestamp)
    elif hasattr(ceQuoteTimestamp, 'hour'):
        quoteTimestamp = ceQuoteTimestamp
    elif hasattr(peQuoteTimestamp, 'hour'):
        quoteTimestamp = peQuoteTimestamp

    # Quote-quality gate
    if cePremium <= 0 or pePremium <= 0:
        return _fallback("premium zero or negative on one/both legs")

    if cePremium < MIN_PREMIUM_INR or pePremium < MIN_PREMIUM_INR:
        return _fallback(f"near-zero dust premium: CE={cePremium}, PE={pePremium}")

    if ceBid is not None and ceAsk is not None and ceBid > ceAsk:
        return _fallback(f"CE bid > ask: {ceBid} > {ceAsk}")
    if peBid is not None and peAsk is not None and peBid > peAsk:
        return _fallback(f"PE bid > ask: {peBid} > {peAsk}")

    if ceSpreadPct is not None and ceSpreadPct > BID_ASK_SPREAD_GATE * 100:
        return _fallback(f"CE spread too wide: {ceSpreadPct:.1f}%")
    if peSpreadPct is not None and peSpreadPct > BID_ASK_SPREAD_GATE * 100:
        return _fallback(f"PE spread too wide: {peSpreadPct:.1f}%")

    if (ceSource == "mid" and peSource == "ltp") or (ceSource == "ltp" and peSource == "mid"):
        return _fallback(f"inconsistent premium quality: CE={ceSource}, PE={peSource}")

    # Staleness check during market hours (09:15 - 15:30 IST)
    now = datetime.now()
    marketOpen = now.replace(hour=9, minute=15, second=0, microsecond=0)
    marketClose = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if hasattr(quoteTimestamp, 'hour') and marketOpen <= now <= marketClose:
        staleSec = (now - quoteTimestamp).total_seconds()
        if staleSec > QUOTE_STALE_SECONDS:
            return _fallback(f"stale quote: {staleSec:.0f}s old (limit={QUOTE_STALE_SECONDS}s)")

    # Step 4: Look up strikes from instruments cache
    ceStrike = lookupStrikeFromInstruments(ceSymbol, exchange, kite)
    peStrike = lookupStrikeFromInstruments(peSymbol, exchange, kite)
    if ceStrike is None or peStrike is None:
        return _fallback(f"strike lookup failed: CE={ceStrike}, PE={peStrike}")

    # Step 5: Compute exact time to expiry
    # Expiry is typically at 15:30 IST on expiry day
    if isinstance(expiryDate, date) and not hasattr(expiryDate, 'hour'):
        expiryDatetime = datetime(expiryDate.year, expiryDate.month, expiryDate.day, 15, 30, 0)
    else:
        expiryDatetime = expiryDate

    T = max((expiryDatetime - now).total_seconds(), 0) / (365.0 * 24 * 3600)
    if T < 1e-6:
        return _fallback(f"T too small: {T:.8f} years")

    # Step 6: Compute IV for both legs
    ceIV = bsImpliedVol(cePremium, spot, ceStrike, T, "CE")
    peIV = bsImpliedVol(pePremium, spot, peStrike, T, "PE")

    if ceIV is None or peIV is None:
        return _fallback(f"IV solver failed: ceIV={ceIV}, peIV={peIV}")

    # IV near solver bounds → suspicious
    ivBoundMargin = 0.05
    if ceIV <= IV_SOLVER_MIN * (1 + ivBoundMargin) or ceIV >= IV_SOLVER_MAX * (1 - ivBoundMargin):
        return _fallback(f"CE IV near solver bounds: {ceIV:.4f}")
    if peIV <= IV_SOLVER_MIN * (1 + ivBoundMargin) or peIV >= IV_SOLVER_MAX * (1 - ivBoundMargin):
        return _fallback(f"PE IV near solver bounds: {peIV:.4f}")

    # IV consistency gate for ATM straddle
    avgIV = (ceIV + peIV) / 2.0
    if avgIV > 0 and abs(ceIV - peIV) / avgIV > IV_SPREAD_GATE:
        return _fallback(f"CE/PE IV too far apart: CE={ceIV:.4f}, PE={peIV:.4f}, gap={abs(ceIV-peIV)/avgIV:.2%}")

    # Step 7: Compute Greeks for both legs
    ceGreeks = bsGreeks(spot, ceStrike, T, ceIV, "CE")
    peGreeks = bsGreeks(spot, peStrike, T, peIV, "PE")

    # Step 8: Compute dynamic K
    strategyType = config.get("strategyType", "straddle")
    combinedPremium = cePremium + pePremium

    result = computeDynamicK(
        ceGreeks=ceGreeks, peGreeks=peGreeks,
        ceIV=ceIV, peIV=peIV,
        spot=spot, combinedPremium=combinedPremium,
        lotSize=lotSize, strategyType=strategyType,
        ivShockAbsolute=ivShockAbsolute,
    )

    if result is None:
        return _fallback("computeDynamicK returned None")

    kValue = result["kForSizing"]
    metadata = {
        "source": "dynamic",
        "staticK": staticK,
        **result,
        "spot": round(spot, 2),
        "ceStrike": ceStrike,
        "peStrike": peStrike,
        "sizingDte": sizingDte,
        "cePremiumUsed": round(cePremium, 2),
        "pePremiumUsed": round(pePremium, 2),
        "cePremiumSource": ceSource,
        "pePremiumSource": peSource,
        "ceIV": round(ceIV, 6),
        "peIV": round(peIV, 6),
        "ceGreeks": {k: round(v, 8) for k, v in ceGreeks.items()},
        "peGreeks": {k: round(v, 8) for k, v in peGreeks.items()},
        "timeToExpiryYears": round(T, 8),
        "quoteTimestamp": str(quoteTimestamp) if quoteTimestamp else None,
        "ceQuoteTimestamp": str(ceQuoteTimestamp) if hasattr(ceQuoteTimestamp, 'hour') else None,
        "peQuoteTimestamp": str(peQuoteTimestamp) if hasattr(peQuoteTimestamp, 'hour') else None,
        "ceBid": ceBid, "ceAsk": ceAsk, "ceSpreadPct": round(ceSpreadPct, 2) if ceSpreadPct is not None else None,
        "peBid": peBid, "peAsk": peAsk, "peSpreadPct": round(peSpreadPct, 2) if peSpreadPct is not None else None,
        "ivShockApplied": round(ivShockAbsolute * 100, 1),  # total vol points applied
        "ivShockBase": round(baseIvShock * 100, 1),
        "vixLevel": vixLevel,
        "vixAddon": round(vixAddon * 100, 1),
        "intradayMovePct": intradayMovePct,
        "intradayAddon": round(intradayAddon * 100, 1),
    }

    clampTag = " [CLAMPED]" if result.get("kClamped") else ""
    print(f"{tag}[DYNAMIC-K] kForSizing={kValue:.4f}{clampTag} (raw={result['kRaw']:.4f}, "
          f"binding={result['kBindingScenario']}) "
          f"kBase={result['kBase']:.4f} kStressMove={result['kStressMove']:.4f} "
          f"kStressVol={result['kStressVol']:.4f} kCrash={result['kCrash']:.4f} "
          f"staticK={staticK:.2f} avgIV={result['avgIV']:.4f} expMove={result['expectedMove']:.2f} "
          f"ivShock={ivShockAbsolute*100:.1f}vp")

    return (kValue, metadata)


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
        "maxLots": 6,
        "stopLossTriggerPercent": 30,
        "stopLossOrderPlacePercent": 45,
        "kTable": K_TABLE_STRADDLE,
        "strategyType": "straddle",
        "entryTime": "09:30",
        "exitTime": "12:30",
        "orderTag": "V2-N-STD-4D-30SL",
        "useDynamicK": True,
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
        "useDynamicK": True,
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
        "useDynamicK": True,
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
        "useDynamicK": True,
    },
}

# Daily vol budgets per underlying (INR) — computed from instrument_config.json
_INSTRUMENT_CONFIG_PATH = Path(__file__).parent / "instrument_config.json"

REALIZED_PNL_PATH = Path(__file__).parent.parent / "Work" / "inputs" / "realized_pnl_accumulator.json"
try:
    from Directories import workInputRoot as _workInputRoot
    REALIZED_PNL_PATH = Path(_workInputRoot) / "realized_pnl_accumulator.json"
except Exception:
    pass


REALIZED_PNL_PATH = Path(__file__).parent.parent / "Work" / "inputs" / "realized_pnl_accumulator.json"
try:
    from Directories import workInputRoot as _workInputRoot
    REALIZED_PNL_PATH = Path(_workInputRoot) / "realized_pnl_accumulator.json"
except Exception:
    pass


def _load_vol_budgets():
    """Compute options daily vol budgets from effective capital.

    Capital formula: effective = base_capital + cumulative_realized + eod_unrealized
    Reads from realized_pnl_accumulator.json (EOD JSON), falls back to DB.
    """
    with open(_INSTRUMENT_CONFIG_PATH) as f:
        cfg = json.load(f)
    acct = cfg["account"]
    base_capital = acct["base_capital"]

    # Read realized + unrealized from EOD JSON, fall back to DB
    cumulative_pnl = 0.0
    eod_unrealized = 0.0
    try:
        with open(REALIZED_PNL_PATH, "r") as f:
            pnl_data = json.load(f)
        cumulative_pnl = float(pnl_data.get("cumulative_realized_pnl") or 0.0)
        eod_unrealized = float(pnl_data.get("eod_unrealized") or 0.0)
    except (FileNotFoundError, json.JSONDecodeError):
        cumulative_pnl = GetCumulativeRealizedPnl()

    effective_capital = base_capital + cumulative_pnl + eod_unrealized
    print(f"Options effective capital: base={base_capital} + realized={cumulative_pnl:.0f} + unrealized={eod_unrealized:.0f} = {effective_capital:.0f}")
    budgets = {}
    for underlying, opt_cfg in cfg.get("options_allocation", {}).items():
        budgets[underlying] = compute_daily_vol_target(
            effective_capital, acct["annual_vol_target_pct"],
            opt_cfg["vol_weights"]
        )
    return budgets, sum(budgets.values())


DAILY_VOL_BUDGETS, PORTFOLIO_DAILY_VOL_CAP = _load_vol_budgets()


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
                # Realize P&L at exit_price=0 (expired worthless)
                activeQty = state[underlying].get("activeQuantity", 0)
                if activeQty > 0:
                    RealizePnl(f"{underlying}_OPT_CE", 0, activeQty, 1.0, "options", WasLong=False)
                    RealizePnl(f"{underlying}_OPT_PE", 0, activeQty, 1.0, "options", WasLong=False)
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

def buildOrderDetails(strategyName, config, quantity, tradeType="SELL", useSmartChase=False):
    """Build an OrderDetails dict compatible with FetchContractName, order(), Set_Gtt().
    When useSmartChase=True, sets Ordertype to LIMIT (smart chase computes its own price)
    and populates Broker from options execution config.
    """
    underlying = config["underlying"]
    configKey = UNDERLYING_TO_CONFIG_KEY.get(underlying, "")
    optExecCfg = OPTIONS_EXEC_CONFIG.get(configKey, {})

    return {
        "Tradetype": tradeType,
        "Exchange": EXCHANGE_MAP[underlying],
        "Tradingsymbol": underlying,
        "Quantity": str(quantity),
        "Variety": "REGULAR",
        "Ordertype": "LIMIT" if useSmartChase else "MARKET",
        "Product": "NRML",
        "Validity": "DAY",
        "Price": 0.0,
        "Symboltoken": "",
        "Squareoff": "",
        "Stoploss": "",
        "Broker": optExecCfg.get("broker", "") if useSmartChase else "",
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
        "User": optExecCfg.get("user", "OFS653"),
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

    # Determine if smart chase is enabled for exits
    configKey = UNDERLYING_TO_CONFIG_KEY.get(underlying, "")
    optExecCfg = OPTIONS_EXEC_CONFIG.get(configKey, {})
    execParams = optExecCfg.get("execution", {})
    useSmartChase = execParams.get("use_smart_chase", False)
    broker = optExecCfg.get("broker", "ZERODHA")

    # Phase 1: Submit all exit orders
    submittedOrders = []  # (contract, orderId, accepted)

    for contract in contracts:
        leg = "CE" if "CE" in contract.upper() else "PE"
        exitOrderDetails = {
            "Tradetype": "BUY",
            "Exchange": exchange,
            "Tradingsymbol": contract,
            "Quantity": str(quantity),
            "Variety": "REGULAR",
            "Ordertype": "LIMIT" if useSmartChase else "MARKET",
            "Product": "NRML",
            "Validity": "DAY",
            "Price": 0.0,
            "Symboltoken": "",
            "Squareoff": "",
            "Stoploss": "",
            "Broker": broker if useSmartChase else "",
            "Netposition": "",
            "OrderTag": f"V2-EXIT-{underlying}",
            "TradeFailExitRequired": "False",
            "User": optExecCfg.get("user", "OFS653"),
        }
        try:
            if useSmartChase:
                exitSuccess, orderId, exitFillInfo = SmartChaseExecute(
                    kite, exitOrderDetails, execParams, IsEntry=False, Broker=broker, ATR=0
                )
                if not exitSuccess:
                    raise Exception(f"Smart chase exit failed for {contract}: "
                                    f"mode={exitFillInfo.get('execution_mode')}")
                accepted = True
                LogOptionsSmartChaseOrder(underlying, strategyName, leg, contract, "BUY",
                                         quantity, orderId, exitFillInfo)
                # Realize P&L for this leg (options are always short — WasLong=False)
                exitFillPrice = exitFillInfo.get("fill_price", 0)
                if exitFillPrice > 0:
                    RealizePnl(f"{underlying}_OPT_{leg}", exitFillPrice, quantity, 1.0, "options", WasLong=False)
                print(f"[EXIT] {underlying}: BUY {contract} orderId={orderId} "
                      f"filled via smart chase, price={exitFillInfo.get('fill_price')}")
            else:
                orderId = order(exitOrderDetails)
                accepted = orderId != 0
                print(f"[EXIT] {underlying}: BUY {contract} orderId={orderId} accepted={accepted}")
            submittedOrders.append((contract, orderId, accepted))
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
    # Smart chase orders are already verified filled by SmartChaseExecute, skip re-verification
    print(f"[EXIT] {underlying}: all orders accepted, verifying fills...")
    unfilledLegs = []
    if not useSmartChase:
        for contract, orderId, _ in submittedOrders:
            isFilled, fillStatus = verifyOrderFill(kite, orderId)
            print(f"[EXIT] {underlying}: {contract} orderId={orderId} fillStatus={fillStatus}")
            if not isFilled:
                unfilledLegs.append(contract)
    else:
        print(f"[EXIT] {underlying}: smart chase exits already verified filled, skipping poll")

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

    # Step 1: Build OrderDetails (1 lot placeholder for contract resolution)
    orderDetails = buildOrderDetails(strategyName, config, quantity=lotSize)

    # Step 2: Fetch contract names via existing FetchContractName
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

    # Step 3: Fetch premiums for sizing
    try:
        callPremium, putPremium = fetchOptionPremiums(kite, ceSymbol, peSymbol, exchange)
    except Exception as e:
        print(f"{tag}[ENTRY] {strategyName}: premium fetch failed: {e}")
        return {"success": False, "reason": f"premium fetch failed: {e}"}

    # Step 4: Resolve K value — dynamic (Greeks-based) if enabled, static fallback otherwise
    # K is sized at exit DTE (worst-case gamma during hold):
    #   4D strategy: enters DTE=4, exits DTE=2 → K(DTE=2)
    #   2D strategy: enters DTE=2, expires DTE=0 → passes through DTE=1 (peak gamma) → K(DTE=1)
    exitDteConfig = config.get("exitDte", 0)
    sizingDte = exitDteConfig if exitDteConfig >= 1 else 1
    try:
        kValue, kMetadata = resolveK(
            config=config, kite=kite,
            ceSymbol=ceSymbol, peSymbol=peSymbol,
            exchange=exchange, underlying=underlying,
            sizingDte=sizingDte,
            callPremium=callPremium, putPremium=putPremium,
            lotSize=lotSize, expiryDate=expiryDate,
        )
    except Exception as e:
        print(f"{tag}[ENTRY] {strategyName}: K resolution failed: {e}")
        return {"success": False, "reason": f"K resolution failed: {e}"}

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
                 skipReason=sizeResult["skipReason"], kMetadata=kMetadata)
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
                 sizeResult, expiryDate, ceSymbol, peSymbol, skipped=True,
                 skipReason=reason, kMetadata=kMetadata)
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
        print(f"{tag}  K source:        {kMetadata.get('source', 'unknown')}")
        if kMetadata.get("source") == "dynamic":
            print(f"{tag}  K scenarios:     kBase={kMetadata.get('kBase', 'N/A')} | "
                  f"kStressMove={kMetadata.get('kStressMove', 'N/A')} | "
                  f"kStressVol={kMetadata.get('kStressVol', 'N/A')} | "
                  f"kCrash={kMetadata.get('kCrash', 'N/A')}")
            print(f"{tag}  Binding:         {kMetadata.get('kBindingScenario', 'N/A')}")
            print(f"{tag}  K (spot sens):   {kMetadata.get('kSpotSensitivity', 'N/A')}")
            print(f"{tag}  Static K ref:    {kMetadata.get('staticK', 'N/A')}")
            print(f"{tag}  Avg IV:          {kMetadata.get('avgIV', 'N/A')}")
            print(f"{tag}  Expected move:   {kMetadata.get('expectedMove', 'N/A')}")
            print(f"{tag}  IV shock:        {kMetadata.get('ivShockApplied', 'N/A')}vp "
                  f"(base={kMetadata.get('ivShockBase', 'N/A')} "
                  f"+VIX={kMetadata.get('vixAddon', 'N/A')} "
                  f"+move={kMetadata.get('intradayAddon', 'N/A')})")
            print(f"{tag}  VIX:             {kMetadata.get('vixLevel', 'N/A')}")
            print(f"{tag}  CE IV:           {kMetadata.get('ceIV', 'N/A')}")
            print(f"{tag}  PE IV:           {kMetadata.get('peIV', 'N/A')}")
            print(f"{tag}  CE prem src:     {kMetadata.get('cePremiumSource', 'N/A')}")
            print(f"{tag}  PE prem src:     {kMetadata.get('pePremiumSource', 'N/A')}")
            print(f"{tag}  T (years):       {kMetadata.get('timeToExpiryYears', 'N/A')}")
        elif kMetadata.get("source") == "static_fallback":
            print(f"{tag}  Fallback reason: {kMetadata.get('fallbackReason', 'N/A')}")
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

    # Determine if smart chase is enabled for this underlying
    configKey = UNDERLYING_TO_CONFIG_KEY.get(underlying, "")
    optExecCfg = OPTIONS_EXEC_CONFIG.get(configKey, {})
    execParams = optExecCfg.get("execution", {})
    useSmartChase = execParams.get("use_smart_chase", False)
    broker = optExecCfg.get("broker", "ZERODHA")

    # Step 6: Place CE order
    ceOrderDetails = buildOrderDetails(strategyName, config, quantity=totalQuantity,
                                       useSmartChase=useSmartChase)
    ceOrderDetails["Tradingsymbol"] = ceSymbol

    try:
        if useSmartChase:
            ceSuccess, ceOrderId, ceFillInfo = SmartChaseExecute(
                kite, ceOrderDetails, execParams, IsEntry=True, Broker=broker, ATR=0
            )
            if not ceSuccess:
                raise Exception(f"Smart chase failed for CE: mode={ceFillInfo.get('execution_mode')}, "
                                f"iterations={ceFillInfo.get('chase_iterations')}")
            LogOptionsSmartChaseOrder(underlying, strategyName, "CE", ceSymbol, "SELL",
                                     totalQuantity, ceOrderId, ceFillInfo)
            # Track cost basis for CE leg
            ceFillPrice = ceFillInfo.get("fill_price", 0)
            if ceFillPrice > 0:
                UpdateCostBasis(f"{underlying}_OPT_CE", ceFillPrice, totalQuantity, 1.0)
        else:
            ceOrderId = order(ceOrderDetails)
            # Approximate cost basis from pre-order LTP (exact fill unknown for legacy path)
            if callPremium > 0:
                UpdateCostBasis(f"{underlying}_OPT_CE", callPremium, totalQuantity, 1.0)
        contracts.append(ceSymbol)
        print(f"[ENTRY] {strategyName}: CE {ceSymbol} placed, orderId={ceOrderId}"
              f"{' (smart chase)' if useSmartChase else ''}")
    except Exception as e:
        logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
                 sizeResult, expiryDate, ceSymbol, peSymbol, skipped=True,
                 skipReason=f"CE order failed: {e}", kMetadata=kMetadata)
        print(f"[ENTRY] {strategyName}: CE order FAILED: {e}")
        return {"success": False, "reason": f"CE order failed: {e}"}

    # Step 7: Place PE order
    peOrderDetails = buildOrderDetails(strategyName, config, quantity=totalQuantity,
                                       useSmartChase=useSmartChase)
    peOrderDetails["Tradingsymbol"] = peSymbol

    try:
        if useSmartChase:
            peSuccess, peOrderId, peFillInfo = SmartChaseExecute(
                kite, peOrderDetails, execParams, IsEntry=True, Broker=broker, ATR=0
            )
            if not peSuccess:
                raise Exception(f"Smart chase failed for PE: mode={peFillInfo.get('execution_mode')}, "
                                f"iterations={peFillInfo.get('chase_iterations')}")
            LogOptionsSmartChaseOrder(underlying, strategyName, "PE", peSymbol, "SELL",
                                     totalQuantity, peOrderId, peFillInfo)
            # Track cost basis for PE leg
            peFillPrice = peFillInfo.get("fill_price", 0)
            if peFillPrice > 0:
                UpdateCostBasis(f"{underlying}_OPT_PE", peFillPrice, totalQuantity, 1.0)
        else:
            peOrderId = order(peOrderDetails)
            # Approximate cost basis from pre-order LTP (exact fill unknown for legacy path)
            if putPremium > 0:
                UpdateCostBasis(f"{underlying}_OPT_PE", putPremium, totalQuantity, 1.0)
        contracts.append(peSymbol)
        print(f"[ENTRY] {strategyName}: PE {peSymbol} placed, orderId={peOrderId}"
              f"{' (smart chase)' if useSmartChase else ''}")
    except Exception as e:
        # Partial fill: CE placed but PE failed — DANGEROUS naked short position
        logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
                 sizeResult, expiryDate, ceSymbol, peSymbol, skipped=True,
                 skipReason=f"PARTIAL FILL - PE order failed after CE placed (ceOrderId={ceOrderId}): {e}",
                 gttProtected=False, positionIntegrity="partial", kMetadata=kMetadata)
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
                 skipReason="GTT_FAILED", gttProtected=False, positionIntegrity="partial",
                 kMetadata=kMetadata)
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
             gttProtected=gttOk, positionIntegrity="healthy", kMetadata=kMetadata)

    # Step 12: Send email notification (non-blocking — failure does not affect trade)
    sendEntryEmail(strategyName, config, dte, kValue, callPremium, putPremium,
                   sizeResult, expiryDate, ceSymbol, peSymbol, kMetadata,
                   gttOk, gttIds, state)

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
    # Dynamic K fields
    "kSource", "kForSizing", "kRaw", "kClamped", "kBase", "kStressMove", "kStressVol", "kCrash", "kBindingScenario",
    "kSpotSensitivity", "staticK",
    "avgIV", "expectedMove", "posGamma", "posTheta", "posVega",
    "ivShockApplied", "ivShockBase", "vixLevel", "vixAddon", "intradayMovePct", "intradayAddon",
    "cePremiumUsed", "pePremiumUsed", "cePremiumSource", "pePremiumSource",
    "ceIV", "peIV", "timeToExpiryYears", "quoteTimestamp",
    "ceBid", "ceAsk", "ceSpreadPct", "peBid", "peAsk", "peSpreadPct",
]

EXIT_LOG_FIELDS = [
    "timestamp", "strategyName", "currentState", "reasonForExit",
    "currentLots", "targetLotsAfterExit", "exitStatus", "failedLegs",
]


def logEntry(strategyName, config, dte, kValue, callPremium, putPremium,
             sizeResult, expiryDate, ceSymbol, peSymbol, skipped, skipReason,
             gttProtected=True, positionIntegrity="healthy", kMetadata=None):
    """Write a structured entry log line to CSV and stdout."""
    underlying = config["underlying"]
    km = kMetadata or {}
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
        # Dynamic K fields
        "kSource": km.get("source", "static"),
        "kForSizing": km.get("kForSizing", ""),
        "kRaw": km.get("kRaw", ""),
        "kClamped": km.get("kClamped", ""),
        "kBase": km.get("kBase", ""),
        "kStressMove": km.get("kStressMove", ""),
        "kStressVol": km.get("kStressVol", ""),
        "kCrash": km.get("kCrash", ""),
        "kBindingScenario": km.get("kBindingScenario", ""),
        "kSpotSensitivity": km.get("kSpotSensitivity", ""),
        "staticK": km.get("staticK", kValue),
        "avgIV": km.get("avgIV", ""),
        "expectedMove": km.get("expectedMove", ""),
        "posGamma": km.get("posGamma", ""),
        "posTheta": km.get("posTheta", ""),
        "posVega": km.get("posVega", ""),
        "ivShockApplied": km.get("ivShockApplied", ""),
        "ivShockBase": km.get("ivShockBase", ""),
        "vixLevel": km.get("vixLevel", ""),
        "vixAddon": km.get("vixAddon", ""),
        "intradayMovePct": km.get("intradayMovePct", ""),
        "intradayAddon": km.get("intradayAddon", ""),
        "cePremiumUsed": km.get("cePremiumUsed", ""),
        "pePremiumUsed": km.get("pePremiumUsed", ""),
        "cePremiumSource": km.get("cePremiumSource", ""),
        "pePremiumSource": km.get("pePremiumSource", ""),
        "ceIV": km.get("ceIV", ""),
        "peIV": km.get("peIV", ""),
        "timeToExpiryYears": km.get("timeToExpiryYears", ""),
        "quoteTimestamp": km.get("quoteTimestamp", ""),
        "ceBid": km.get("ceBid", ""),
        "ceAsk": km.get("ceAsk", ""),
        "ceSpreadPct": km.get("ceSpreadPct", ""),
        "peBid": km.get("peBid", ""),
        "peAsk": km.get("peAsk", ""),
        "peSpreadPct": km.get("peSpreadPct", ""),
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

    # Skip entries/exits on holidays and weekends
    today = date.today()
    if today.weekday() >= 5 or CheckForDateHoliday(today):
        dayType = "weekend" if today.weekday() >= 5 else f"holiday ({today})"
        print(f"[V2 RUNNER] {dayType} — skipping entry/exit processing")
        return

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
                            # Realize P&L at exit_price=0 (expired worthless = full premium kept)
                            activeQty = state[underlying].get("activeQuantity", 0)
                            if activeQty > 0:
                                RealizePnl(f"{underlying}_OPT_CE", 0, activeQty, 1.0, "options", WasLong=False)
                                RealizePnl(f"{underlying}_OPT_PE", 0, activeQty, 1.0, "options", WasLong=False)
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
