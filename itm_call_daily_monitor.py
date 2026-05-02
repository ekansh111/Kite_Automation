"""itm_call_daily_monitor.py — Daily drift monitoring for ITM call positions.

Run daily ~3:00 PM (after market settles, before close).
Computes drift metrics vs entry, sends alerts on threshold breaches.

Auto-actions: ONLY trim if outlay exceeds 4% of capital (cap breach).
All other alerts are notifications-only — human reviews and decides.

Usage:
    python itm_call_daily_monitor.py             # normal run
    python itm_call_daily_monitor.py --dry-run   # log decisions, no orders/emails
"""
import argparse
import json
import logging
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from Directories import (
    WorkDirectory,
    KiteEshitaLogin, KiteEshitaLoginAccessToken,
)
from FetchOptionContractName import GetKiteClient
from PlaceOptionsSystemsV2 import (
    bsImpliedVol, bsGreeks, computeDynamicK,
    lookupIvShock, getVixAddon, getRegimeAddon, IV_SHOCK_CAP_VP,
    UNDERLYING_LTP_KEY,
)
from itm_call_rollover import (
    ITM_CONFIG, CONFIG_PATH, STATE_FILE_PATH, LoadState, SaveState,
    LoadVolBudgets, _fmtEmail,
)

Logger = logging.getLogger("itm_call_daily_monitor")

EMAIL_FROM = "ekansh.n111@gmail.com"
EMAIL_FROM_PASSWORD = "sgwl lnvt hewf wplo"
EMAIL_TO = "ekansh.n@gmail.com"
EMAIL_SMTP = "smtp.gmail.com"
EMAIL_PORT = 465

# Alert thresholds
SPOT_DRIFT_PCT = 0.05         # ±5%
VIX_DRIFT_PCT = 0.40          # ±40%
K_DRIFT_RATIO = 2.0           # K_now / K_entry ≥ 2×
CAPITAL_DRIFT_PCT = 0.15      # ±15%
DTE_ROLL_TRIGGER = 14         # alert at DTE=14
PREMIUM_CAP_TRIM_PCT = 0.04   # trim if outlay > 4% capital


def SendEmail(Subject, HtmlBody, dry_run=False):
    """Send email notification (non-blocking)."""
    if dry_run:
        Logger.info("[DRY-RUN] Would send email: %s", Subject)
        return
    try:
        Msg = MIMEMultipart("alternative")
        Msg["Subject"] = Subject
        Msg["From"] = EMAIL_FROM
        Msg["To"] = EMAIL_TO
        Msg.attach(MIMEText(HtmlBody, "html"))
        with smtplib.SMTP_SSL(EMAIL_SMTP, EMAIL_PORT) as Server:
            Server.login(EMAIL_FROM, EMAIL_FROM_PASSWORD)
            Server.send_message(Msg)
        Logger.info("Email sent: %s", Subject)
    except Exception as E:
        Logger.warning("Email send failed: %s", E)


def BuildDailyMonitorEmail(IndexName, Drift, Position, Alerts, Recommendation):
    """Aesthetic drift-alert email matching the rollover email style."""
    Now = datetime.now()
    navy = "#003366"
    accent = "#2E75B6"
    amber = "#F39C12"
    green = "#27AE60"
    red = "#E74C3C"
    grey_bg = "#F8F9FA"
    border_col = "#DEE2E6"

    has_alerts = bool(Alerts)
    status_color = amber if has_alerts else green
    status_text = f"DRIFT ALERT — {len(Alerts)} threshold(s) breached" if has_alerts else "ALL METRICS WITHIN BANDS"

    # Drift table rows
    drift_rows = ""
    for label, entry_v, today_v, drift_pct, alert in Drift:
        drift_color = red if abs(drift_pct) > 0 and alert else "#333"
        alert_label = '<span style="color:#E74C3C;font-weight:600;">⚠ ALERT</span>' if alert else '<span style="color:#27AE60;">✓ OK</span>'
        bg = grey_bg if (drift_rows.count("<tr") % 2 == 0) else ""
        drift_rows += f'<tr style="background:{bg};"><td style="padding:8px 12px;font-weight:600;">{label}</td><td style="padding:8px 12px;text-align:right;font-family:monospace;">{entry_v}</td><td style="padding:8px 12px;text-align:right;font-family:monospace;">{today_v}</td><td style="padding:8px 12px;text-align:right;color:{drift_color};font-family:monospace;">{drift_pct:+.2f}%</td><td style="padding:8px 12px;text-align:center;">{alert_label}</td></tr>'

    Html = f"""
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin:0;padding:0;font-family:Arial,Helvetica,sans-serif;background:#EAECEE;">
      <div style="max-width:680px;margin:20px auto;background:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1);">
        <div style="background:{navy};padding:20px 28px;">
          <h1 style="margin:0;color:#FFFFFF;font-size:20px;">
            ITM Call Daily Monitor &mdash; {IndexName}
          </h1>
          <p style="margin:6px 0 0;color:#AAC4E0;font-size:13px;">
            {Now.strftime('%d %b %Y, %I:%M %p')}
          </p>
        </div>
        <div style="background:{status_color};padding:10px 28px;">
          <span style="color:#FFFFFF;font-size:13px;font-weight:600;">{status_text}</span>
        </div>

        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Drift Metrics (vs entry)
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{navy};color:#FFF;">
              <td style="padding:8px 12px;font-weight:600;">Metric</td>
              <td style="padding:8px 12px;text-align:right;font-weight:600;">Entry</td>
              <td style="padding:8px 12px;text-align:right;font-weight:600;">Today</td>
              <td style="padding:8px 12px;text-align:right;font-weight:600;">Drift</td>
              <td style="padding:8px 12px;text-align:center;font-weight:600;">Alert?</td>
            </tr>
            {drift_rows}
          </table>
        </div>

        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Position Status
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:40%;">Symbol</td>
              <td style="padding:8px 12px;font-family:monospace;">{Position.get('symbol', '—')}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Lots / Qty</td>
              <td style="padding:8px 12px;">{Position.get('lots', 0)} / {Position.get('quantity', 0)}</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Entry Premium</td>
              <td style="padding:8px 12px;">₹{_fmtEmail(Position.get('entry_premium', 0), 2)}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Current Premium</td>
              <td style="padding:8px 12px;">₹{_fmtEmail(Position.get('current_premium', 0), 2)} ({Position.get('mtm_pct', 0):+.1f}%)</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Current Outlay (MTM)</td>
              <td style="padding:8px 12px;">₹{_fmtEmail(Position.get('current_outlay', 0), 0)} ({Position.get('current_outlay_pct', 0):.2f}% capital)</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">DTE (trading days)</td>
              <td style="padding:8px 12px;">{Position.get('dte', '?')}</td>
            </tr>
          </table>
        </div>

        <div style="padding:20px 28px 24px;">
          <div style="background:{grey_bg};border:1px solid {border_col};border-radius:6px;padding:16px 18px;">
            <p style="margin:0 0 8px;font-weight:700;font-size:13px;color:{navy};">
              Recommended Review
            </p>
            <p style="margin:0;font-size:12px;color:#444;line-height:1.6;">{Recommendation}</p>
          </div>
        </div>
      </div>
    </body>
    </html>
    """
    return Html


def BuildAutoTrimEmail(IndexName, OldOutlay, NewOutlay, OldLots, NewLots, RealizedPnl,
                       Capital):
    """Email sent when premium-cap auto-trim is executed."""
    Now = datetime.now()
    navy = "#003366"
    accent = "#2E75B6"
    red = "#E74C3C"
    grey_bg = "#F8F9FA"

    Html = f"""
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin:0;padding:0;font-family:Arial,Helvetica,sans-serif;background:#EAECEE;">
      <div style="max-width:680px;margin:20px auto;background:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1);">
        <div style="background:{navy};padding:20px 28px;">
          <h1 style="margin:0;color:#FFFFFF;font-size:20px;">
            ITM Call AUTO-TRIM Executed &mdash; {IndexName}
          </h1>
          <p style="margin:6px 0 0;color:#AAC4E0;font-size:13px;">
            {Now.strftime('%d %b %Y, %I:%M %p')} &bull; Cap-breach response
          </p>
        </div>
        <div style="background:{red};padding:10px 28px;">
          <span style="color:#FFFFFF;font-size:13px;font-weight:600;">
            Outlay exceeded {int(PREMIUM_CAP_TRIM_PCT*100)}% capital trigger &mdash; trim executed
          </span>
        </div>

        <div style="padding:24px 28px 0;">
          <h2 style="margin:0 0 14px;color:{navy};font-size:16px;border-bottom:2px solid {accent};padding-bottom:6px;">
            Trim Details
          </h2>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;width:50%;">Outlay before trim</td>
              <td style="padding:8px 12px;font-family:monospace;">₹{_fmtEmail(OldOutlay, 0)} ({OldOutlay/Capital*100:.2f}% capital)</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Outlay after trim</td>
              <td style="padding:8px 12px;font-family:monospace;">₹{_fmtEmail(NewOutlay, 0)} ({NewOutlay/Capital*100:.2f}% capital)</td>
            </tr>
            <tr style="background:{grey_bg};">
              <td style="padding:8px 12px;font-weight:600;">Lots before / after</td>
              <td style="padding:8px 12px;">{OldLots} → {NewLots}</td>
            </tr>
            <tr>
              <td style="padding:8px 12px;font-weight:600;">Realized P&amp;L on trimmed lots</td>
              <td style="padding:8px 12px;font-family:monospace;color:{'#27AE60' if RealizedPnl >= 0 else red};">{'+' if RealizedPnl >= 0 else ''}₹{_fmtEmail(RealizedPnl, 0)}</td>
            </tr>
          </table>
        </div>
      </div>
    </body>
    </html>
    """
    return Html


def AnalyzeIndex(Kite, IndexName, IdxState, FullCfg):
    """Compute drift metrics for one index. Returns dict of metrics or None if no position."""
    if IdxState.get("status") != "HOLDING":
        return None

    IdxCfg = ITM_CONFIG[IndexName]
    Symbol = IdxState.get("current_contract")
    Lots = IdxState.get("lots", 0)
    LotSize = IdxState.get("lot_size") or 0
    Quantity = IdxState.get("quantity") or (Lots * LotSize)
    EntryPremium = IdxState.get("entry_price", 0)
    EntrySpot = IdxState.get("entry_spot")
    EntryVix = IdxState.get("entry_vix")
    EntryK = IdxState.get("entry_k", 0.18)
    EntryCapital = IdxState.get("entry_capital", FullCfg.get("account", {}).get("base_capital", 0))
    EntryDate = IdxState.get("entry_date")

    # Live spot
    spot_key = IdxCfg["underlying_ltp_key"]
    try:
        spot_data = Kite.ltp([spot_key])
        spot_now = float(spot_data[spot_key]["last_price"])
    except Exception as E:
        Logger.error("[%s] Spot fetch failed: %s", IndexName, E)
        return None

    # Live premium
    opt_key = f"{IdxCfg['exchange']}:{Symbol}"
    try:
        quote_data = Kite.quote([opt_key])
        prem_now = float(quote_data[opt_key].get("last_price", 0))
    except Exception as E:
        Logger.error("[%s] Premium fetch failed: %s", IndexName, E)
        prem_now = 0

    # Live VIX
    try:
        vix_now, _ = getVixAddon(Kite)  # returns (addon, level)
        vix_data = Kite.ltp(["NSE:INDIA VIX"])
        vix_now_lvl = float(vix_data["NSE:INDIA VIX"]["last_price"])
    except Exception:
        vix_now_lvl = None

    # Live capital
    Budgets, EffCapital = LoadVolBudgets()

    # Drift metrics
    spot_drift = ((spot_now - EntrySpot) / EntrySpot * 100) if EntrySpot else 0
    vix_drift = ((vix_now_lvl - EntryVix) / EntryVix * 100) if EntryVix and vix_now_lvl else 0
    capital_drift = ((EffCapital - EntryCapital) / EntryCapital * 100) if EntryCapital else 0
    mtm_pct = ((prem_now - EntryPremium) / EntryPremium * 100) if EntryPremium else 0

    current_outlay = prem_now * Quantity
    current_outlay_pct = (current_outlay / EffCapital * 100) if EffCapital else 0

    # DTE
    from itm_call_rollover import CountTradingDaysUntilExpiry
    from datetime import date
    expiry_date = IdxState.get("current_expiry")
    if isinstance(expiry_date, str):
        try:
            expiry_date = date.fromisoformat(expiry_date)
        except Exception:
            expiry_date = None
    dte_now = CountTradingDaysUntilExpiry(expiry_date) if expiry_date else None

    # Build alert list
    alerts = []
    if abs(spot_drift) > SPOT_DRIFT_PCT * 100:
        alerts.append(f"SPOT_DRIFT: {spot_drift:+.1f}% (threshold ±{int(SPOT_DRIFT_PCT*100)}%)")
    if abs(vix_drift) > VIX_DRIFT_PCT * 100:
        alerts.append(f"VIX_DRIFT: {vix_drift:+.1f}% (threshold ±{int(VIX_DRIFT_PCT*100)}%)")
    if abs(capital_drift) > CAPITAL_DRIFT_PCT * 100:
        alerts.append(f"CAPITAL_DRIFT: {capital_drift:+.1f}%")
    if dte_now is not None and dte_now == DTE_ROLL_TRIGGER:
        alerts.append(f"DTE={DTE_ROLL_TRIGGER} — consider early roll")

    cap_breach = current_outlay_pct > PREMIUM_CAP_TRIM_PCT * 100

    # Recommendation text
    if cap_breach:
        recommendation = (
            f"⚠️ HARD TRIGGER: Outlay {current_outlay_pct:.2f}% exceeds {int(PREMIUM_CAP_TRIM_PCT*100)}% cap. "
            f"Auto-trim should be executed (sell down to bring outlay below cap)."
        )
    elif alerts:
        recommendation = (
            "Significant drift from entry conditions. Review position and decide: "
            "<br/>1. Hold to expiry (preserves convexity)"
            "<br/>2. Roll up/down strike (lock in gains or reset risk)"
            "<br/>3. Trim 1+ lots if conservative"
            "<br/><br/>No automatic action taken — this is monitoring only."
        )
    else:
        recommendation = "All metrics within tolerance. No action required."

    return {
        "drift": [
            ("Spot", f"₹{_fmtEmail(EntrySpot, 2)}", f"₹{_fmtEmail(spot_now, 2)}",
             spot_drift, abs(spot_drift) > SPOT_DRIFT_PCT * 100),
            ("VIX", f"{EntryVix or '?'}", f"{vix_now_lvl or '?'}",
             vix_drift, abs(vix_drift) > VIX_DRIFT_PCT * 100),
            ("Capital", f"₹{_fmtEmail(EntryCapital, 0)}", f"₹{_fmtEmail(EffCapital, 0)}",
             capital_drift, abs(capital_drift) > CAPITAL_DRIFT_PCT * 100),
            ("DTE", str(IdxState.get("entry_dte", "?")), str(dte_now or "?"), 0,
             dte_now is not None and dte_now == DTE_ROLL_TRIGGER),
        ],
        "position": {
            "symbol": Symbol, "lots": Lots, "quantity": Quantity,
            "entry_premium": EntryPremium, "current_premium": prem_now,
            "mtm_pct": mtm_pct, "current_outlay": current_outlay,
            "current_outlay_pct": current_outlay_pct, "dte": dte_now,
        },
        "alerts": alerts,
        "cap_breach": cap_breach,
        "recommendation": recommendation,
    }


def main():
    Parser = argparse.ArgumentParser(description="ITM Call Daily Monitor")
    Parser.add_argument("--dry-run", action="store_true", help="Log only, no emails or trims")
    Args = Parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Path(WorkDirectory) / "itm_call_daily_monitor.log"),
        ]
    )

    Logger.info("=" * 60)
    Logger.info("ITM Call Daily Monitor started | dry_run=%s", Args.dry_run)

    try:
        Kite = GetKiteClient()
    except Exception as E:
        Logger.error("Failed to connect to Kite: %s", E)
        sys.exit(1)

    State = LoadState()
    with open(CONFIG_PATH) as F:
        FullCfg = json.load(F)

    for IndexName in ["NIFTY", "BANKNIFTY"]:
        IdxState = State.get(IndexName, {})
        if IdxState.get("status") != "HOLDING":
            Logger.info("[%s] Not holding, skipping", IndexName)
            continue

        Logger.info("[%s] Analyzing drift...", IndexName)
        analysis = AnalyzeIndex(Kite, IndexName, IdxState, FullCfg)
        if analysis is None:
            continue

        Logger.info("[%s] Alerts: %s | Cap breach: %s", IndexName,
                    analysis["alerts"], analysis["cap_breach"])

        # Send email if there are alerts OR cap breach
        if analysis["alerts"] or analysis["cap_breach"]:
            html = BuildDailyMonitorEmail(
                IndexName, analysis["drift"], analysis["position"],
                analysis["alerts"], analysis["recommendation"]
            )
            subject = f"[ITM-CALL ALERT] {IndexName}: {len(analysis['alerts'])} drift alert(s)"
            if analysis["cap_breach"]:
                subject = f"[ITM-CALL CAP BREACH] {IndexName}: TRIM REQUIRED"
            SendEmail(subject, html, dry_run=Args.dry_run)

        # Auto-trim on cap breach (NOT YET IMPLEMENTED — only logs warning for now)
        if analysis["cap_breach"]:
            Logger.warning("[%s] Cap breach detected. Auto-trim logic not yet implemented "
                           "(this should call ExecuteTrim with proper order placement). "
                           "Manual review required: outlay=%.2f%% capital, threshold=%.0f%%",
                           IndexName, analysis["position"]["current_outlay_pct"],
                           PREMIUM_CAP_TRIM_PCT * 100)
            # TODO: implement auto-trim that sells down to bring outlay under cap
            # For safety, this is left as alert-only until tested in dry-run thoroughly.

    Logger.info("Daily monitor finished")


if __name__ == "__main__":
    main()
