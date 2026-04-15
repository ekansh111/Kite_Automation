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
import traceback
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict

import pandas as pd

from Server_Order_Handler import EstablishConnectionAngelAPI
from rollover_monitor import _SendEmail, _EstablishKiteSession, IsTradingDay, IsAnyExchangeOpen
from Directories import workInputRoot, ZerodhaInstrumentDirectory, AngelInstrumentDirectory

Logger = logging.getLogger("daily_pnl_report")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

CONFIG_PATH = Path(__file__).parent / "instrument_config.json"
STATE_FILE_PATH = Path(workInputRoot) / "v2_state.json"
REALIZED_PNL_PATH = Path(workInputRoot) / "realized_pnl_accumulator.json"

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



# ─── Exchange Opening Times (IST) ───────────────────────────────
# MCX opens 9:00, NSE/NFO/BFO opens 9:15, NCDEX opens 10:00.
# Before an exchange opens, its prices are stale — skip that section.
EXCHANGE_OPEN = {
    "MCX":   (9, 0),   # 09:00 IST
    "NFO":   (9, 15),  # 09:15 IST
    "NCDEX": (10, 0),  # 10:00 IST
}


class _ExchangeNotOpen(Exception):
    """Raised to skip a broker section when its exchange hasn't opened yet."""
    pass


def _IsExchangeOpen(ExchangeKey):
    """Return True if the exchange has opened today (based on current IST time)."""
    Now = datetime.now()
    OpenH, OpenM = EXCHANGE_OPEN.get(ExchangeKey, (0, 0))
    return (Now.hour, Now.minute) >= (OpenH, OpenM)


def _FetchOpenPositions(FullConfig):
    """Fetch open positions from all broker accounts.

    For each position returns:
      pnl         = (LTP - Entry) x Qty x PV  (total unrealized)
      daily_swing = (LTP - Prev Close) x Qty x PV  (today's movement)
      prev_close  = previous day's closing price from broker

    Exchange time gates (IST):
      Before 09:00 — no exchanges open, skip all
      09:00-09:15  — MCX only (Kite YD6016)
      09:15-10:00  — MCX + NFO/BFO options (Kite OFS653)
      After 10:00  — all (MCX + Options + Angel NCDEX)
    """
    Instruments = FullConfig.get("instruments", {})
    Positions = []
    FetchErrors = []  # Track broker fetch failures for email warning

    # ── Kite YD6016 — MCX futures (opens 09:00) ──
    try:
        if not _IsExchangeOpen("MCX"):
            raise _ExchangeNotOpen("MCX")
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
            RawOvernightQty = int(Pos.get("overnight_quantity", 0))
            OvernightQty = abs(RawOvernightQty)
            DayBuyQty = int(Pos.get("day_buy_quantity", 0) or 0)
            DaySellQty = int(Pos.get("day_sell_quantity", 0) or 0)
            DayBuyPrice = float(Pos.get("day_buy_price", 0) or 0)
            DaySellPrice = float(Pos.get("day_sell_price", 0) or 0)
            PV = Cfg.get("point_value", 1)
            Direction = "LONG" if Qty > 0 else "SHORT"
            AbsQty = abs(Qty)

            Pnl = _CalcPnl(Direction, AvgPrice, Ltp, AbsQty, PV)

            # ── Split swing: carried lots from prev_close, new lots from entry ──
            # LIFO: today's buys and sells offset each other first,
            # only excess sells/buys eat into overnight (carried) positions.
            #
            # Direction flip detection: if overnight was opposite direction
            # (e.g. SHORT→LONG), the overnight position was fully closed by
            # today's trades.  All current qty is new.
            OvernightFlipped = (
                (Direction == "LONG" and RawOvernightQty < 0) or
                (Direction == "SHORT" and RawOvernightQty > 0)
            )

            if OvernightFlipped:
                CarriedQty = 0
                NewQty = AbsQty
                NewEntryPrice = DayBuyPrice if Direction == "LONG" else DaySellPrice
                Logger.debug("  %s: direction flipped (%s overnight → %s), "
                             "all %d lots are new today",
                             Symbol, "SHORT" if RawOvernightQty < 0 else "LONG",
                             Direction, AbsQty)
            elif Direction == "LONG":
                ExcessSells = max(0, DaySellQty - DayBuyQty)
                CarriedQty = max(0, OvernightQty - ExcessSells)
                NewQty = max(0, AbsQty - CarriedQty)
                NewEntryPrice = DayBuyPrice
            else:
                ExcessBuys = max(0, DayBuyQty - DaySellQty)
                CarriedQty = max(0, OvernightQty - ExcessBuys)
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
    except _ExchangeNotOpen:
        Logger.info("Skipping Kite YD6016 — MCX not yet open (opens 09:00)")
    except Exception as e:
        Logger.error("Kite YD6016 fetch failed: %s\n%s", e, traceback.format_exc())
        FetchErrors.append(f"Kite YD6016 (MCX): {e}")

    # ── Angel AABM826021 — NCDEX futures (opens 10:00) ──
    try:
        if not _IsExchangeOpen("NCDEX"):
            raise _ExchangeNotOpen("NCDEX")
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
                # LIFO: today's buys and sells offset each other first,
                # only excess sells eat into carry-forward positions.
                ExcessSells = max(0, SellQty - BuyQty)
                CarriedUnits = max(0, CfBuyQty - ExcessSells)
                NewUnits = max(0, AbsQty - CarriedUnits)
                NewEntryPrice = TodayBuyPrice
            else:
                ExcessBuys = max(0, BuyQty - SellQty)
                CarriedUnits = max(0, CfSellQty - ExcessBuys)
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
    except _ExchangeNotOpen:
        Logger.info("Skipping Angel AABM826021 — NCDEX not yet open (opens 10:00)")
    except Exception as e:
        Logger.error("Angel positions fetch failed: %s\n%s", e, traceback.format_exc())
        FetchErrors.append(f"Angel AABM826021 (NCDEX): {e}")

    # ── Kite OFS653 — Options NFO/BFO (opens 09:15) ──
    try:
        if not _IsExchangeOpen("NFO"):
            raise _ExchangeNotOpen("NFO")
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

            # Split swing: LIFO — today's trades offset each other first
            if Direction == "LONG":
                ExcessSells = max(0, DaySellQty - DayBuyQty)
                CarriedQty = max(0, OvernightQty - ExcessSells)
                NewQty = max(0, AbsQty - CarriedQty)
                NewEntryPrice = DayBuyPrice
            else:
                ExcessBuys = max(0, DayBuyQty - DaySellQty)
                CarriedQty = max(0, OvernightQty - ExcessBuys)
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
    except _ExchangeNotOpen:
        Logger.info("Skipping Kite OFS653 — NFO/BFO not yet open (opens 09:15)")
    except Exception as e:
        Logger.error("Options fetch failed: %s\n%s", e, traceback.format_exc())
        FetchErrors.append(f"Kite OFS653 (Options): {e}")

    Logger.info("Total open positions: %d", len(Positions))
    return Positions, FetchErrors


# ─── Realized P&L Accumulator ───────────────────────────────────

def _FetchDailyRealizedPnl():
    """Fetch today's total realized P&L from all broker accounts.

    Sums the 'realised' field from every position (including closed qty=0).
    Returns dict: {"YD6016": float, "AABM826021": float, "OFS653": float}
    """
    Result = {}

    # Kite YD6016 — MCX futures
    try:
        if not _IsExchangeOpen("MCX"):
            raise _ExchangeNotOpen("MCX")
        Kite = _EstablishKiteSession("YD6016")
        AllPositions = Kite.positions().get("net", [])
        Total = 0.0
        for P in AllPositions:
            if P.get("product") not in ("NRML", "MIS") or _IsIndexOption(P.get("tradingsymbol", "")):
                continue
            Realised = float(P.get("realised", 0))
            M2m = float(P.get("m2m", 0))
            Pnl = float(P.get("pnl", 0))
            Qty = P.get("quantity", 0)
            Symbol = P.get("tradingsymbol", "")
            Logger.debug("  Kite YD6016 %s: qty=%s realised=%.2f m2m=%.2f pnl=%.2f",
                         Symbol, Qty, Realised, M2m, Pnl)
            # For closed positions (qty=0), use m2m if realised is 0
            if Qty == 0 and Realised == 0 and M2m != 0:
                Logger.info("  Kite YD6016 %s: closed position, using m2m=%.2f instead of realised=0", Symbol, M2m)
                Total += M2m
            else:
                Total += Realised
        Result["YD6016"] = round(Total, 2)
        Logger.info("Kite YD6016 realized today: %.2f", Total)
    except _ExchangeNotOpen:
        Result["YD6016"] = 0.0
    except Exception as e:
        Logger.error("Kite YD6016 realized fetch failed: %s\n%s", e, traceback.format_exc())
        Result["YD6016"] = 0.0

    # Angel AABM826021 — NCDEX futures
    try:
        if not _IsExchangeOpen("NCDEX"):
            raise _ExchangeNotOpen("NCDEX")
        SmartApi = EstablishConnectionAngelAPI({"User": "AABM826021"})
        RawResponse = SmartApi.position()
        RawPositions = RawResponse.get("data", []) if isinstance(RawResponse, dict) else []
        if RawPositions is None:
            RawPositions = []
        Total = 0.0
        for P in RawPositions:
            ProdType = P.get("producttype", "")
            if ProdType not in ("CARRYFORWARD", "INTRADAY"):
                continue
            Realised = float(P.get("realised", 0) or 0)
            M2m = float(P.get("m2m", 0) or 0)
            Qty = int(P.get("netqty", 0))
            Symbol = P.get("tradingsymbol", "")
            Logger.debug("  Angel %s [%s]: qty=%s realised=%.2f m2m=%.2f",
                         Symbol, ProdType, Qty, Realised, M2m)
            # For closed positions (qty=0), use m2m if realised is 0
            if Qty == 0 and Realised == 0 and M2m != 0:
                Logger.info("  Angel %s: closed position, using m2m=%.2f instead of realised=0", Symbol, M2m)
                Total += M2m
            else:
                Total += Realised
        Result["AABM826021"] = round(Total, 2)
        Logger.info("Angel realized today: %.2f", Total)
    except _ExchangeNotOpen:
        Result["AABM826021"] = 0.0
    except Exception as e:
        Logger.error("Angel realized fetch failed: %s\n%s", e, traceback.format_exc())
        Result["AABM826021"] = 0.0

    # Kite OFS653 — Options
    try:
        if not _IsExchangeOpen("NFO"):
            raise _ExchangeNotOpen("NFO")
        Kite = _EstablishKiteSession("OFS653")
        AllPositions = Kite.positions().get("net", [])
        Total = 0.0
        for P in AllPositions:
            if P.get("product") != "NRML":
                continue
            Realised = float(P.get("realised", 0))
            M2m = float(P.get("m2m", 0))
            Qty = P.get("quantity", 0)
            Symbol = P.get("tradingsymbol", "")
            Logger.debug("  Kite OFS653 %s: qty=%s realised=%.2f m2m=%.2f", Symbol, Qty, Realised, M2m)
            if Qty == 0 and Realised == 0 and M2m != 0:
                Logger.info("  Kite OFS653 %s: closed position, using m2m=%.2f instead of realised=0", Symbol, M2m)
                Total += M2m
            else:
                Total += Realised
        Result["OFS653"] = round(Total, 2)
        Logger.info("Kite OFS653 realized today: %.2f", Total)
    except _ExchangeNotOpen:
        Result["OFS653"] = 0.0
    except Exception as e:
        Logger.error("Kite OFS653 realized fetch failed: %s\n%s", e, traceback.format_exc())
        Result["OFS653"] = 0.0

    return Result


def _UpdateRealizedPnlAccumulator(DailyByAccount, DateStr, EodUnrealized):
    """Write today's realized P&L into the accumulator JSON.

    Idempotent: overwrites today's entry if called multiple times.
    The cumulative total is recomputed from all daily entries each time.
    """
    if REALIZED_PNL_PATH.exists():
        with open(REALIZED_PNL_PATH, "r") as f:
            Data = json.load(f)
    else:
        Data = {
            "fy_start": "2026-04-01",
            "cumulative_realized_pnl": 0.0,
            "eod_unrealized": 0.0,
            "last_updated": "",
            "daily_entries": {},
        }

    DailyTotal = round(sum(DailyByAccount.values()), 2)
    Entry = dict(DailyByAccount)
    Entry["total"] = DailyTotal
    Data["daily_entries"][DateStr] = Entry

    # Recompute cumulative from all daily entries (safe against drift)
    Data["cumulative_realized_pnl"] = round(
        sum(E["total"] for E in Data["daily_entries"].values()), 2
    )
    Data["eod_unrealized"] = round(EodUnrealized, 2)
    Data["last_updated"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Atomic write
    TmpPath = REALIZED_PNL_PATH.with_suffix(".tmp")
    with open(TmpPath, "w") as f:
        json.dump(Data, f, indent=2)
    TmpPath.replace(REALIZED_PNL_PATH)

    Logger.info("Realized P&L accumulator: date=%s daily=%.2f cumulative=%.2f unrealized=%.2f",
                DateStr, DailyTotal, Data["cumulative_realized_pnl"], EodUnrealized)


# ─── Fetch Today's Orders ────────────────────────────────────────

def _FetchTodayOrders(FullConfig):
    """Fetch today's completed orders from all broker accounts."""
    Instruments = FullConfig.get("instruments", {})
    FuturesOrders = []
    OptionsOrders = []

    # ── Kite orders — YD6016 + OFS653 ──
    KiteExchangeMap = {"YD6016": "MCX", "OFS653": "NFO"}
    for KiteUser in ["YD6016", "OFS653"]:
        ExKey = KiteExchangeMap.get(KiteUser, "MCX")
        if not _IsExchangeOpen(ExKey):
            continue
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

    # ── Angel orders (NCDEX opens 10:00) ──
    try:
        if not _IsExchangeOpen("NCDEX"):
            raise _ExchangeNotOpen("NCDEX")
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
    except _ExchangeNotOpen:
        pass
    except Exception as e:
        Logger.error("Angel orders failed: %s", e)

    return FuturesOrders, OptionsOrders


# ─── HTML Report ─────────────────────────────────────────────────

def _BuildReportHtml(D):
    """Build the full HTML email."""
    DateDisplay = datetime.strptime(D["date"], "%Y-%m-%d").strftime("%d %b %Y")
    DailyMtm = D["total_daily_mtm"]
    OpenSwing = D["open_swing"]
    RealizedToday = D["realized_today"]
    TotalPnl = D["total_pnl"]
    HeroColor = _PnlColor(DailyMtm)
    HeroBg = "#0d3320" if DailyMtm >= 0 else "#3b1119"

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
                letter-spacing:0.5px;">\u20b9{_FmtINR(DailyMtm)}</span>
        </div>
        <div style="font-size:11px;color:rgba(255,255,255,0.4);margin-top:6px;">
            TODAY'S DAILY MTM</div>
    </div>"""

    # ── Quick Stats ──
    UnrealizedColor = _PnlColor(TotalPnl)
    SwingColor = _PnlColor(OpenSwing)
    RealizedColor = _PnlColor(RealizedToday)
    StatsHtml = f"""
    <table style="width:100%;border-collapse:collapse;border-bottom:1px solid {BORDER};">
        <tr>
            <td style="width:25%;text-align:center;padding:14px 6px;">
                <div style="font-size:10px;color:{SLATE};text-transform:uppercase;">Open Swing</div>
                <div style="font-size:15px;font-weight:700;color:{SwingColor};">\u20b9{_FmtINR(OpenSwing)}</div>
            </td>
            <td style="width:25%;text-align:center;padding:14px 6px;border-left:1px solid {BORDER};">
                <div style="font-size:10px;color:{SLATE};text-transform:uppercase;">Realized</div>
                <div style="font-size:15px;font-weight:700;color:{RealizedColor};">\u20b9{_FmtINR(RealizedToday)}</div>
            </td>
            <td style="width:25%;text-align:center;padding:14px 6px;border-left:1px solid {BORDER};">
                <div style="font-size:10px;color:{SLATE};text-transform:uppercase;">Unrealized</div>
                <div style="font-size:15px;font-weight:700;color:{UnrealizedColor};">\u20b9{_FmtINR(TotalPnl)}</div>
            </td>
            <td style="width:25%;text-align:center;padding:14px 6px;border-left:1px solid {BORDER};">
                <div style="font-size:10px;color:{SLATE};text-transform:uppercase;">Trades</div>
                <div style="font-size:15px;font-weight:700;color:{NAVY};">{D["trade_count"]}</div>
            </td>
        </tr>
    </table>"""

    # ── Fetch Error Warning ──
    WarningHtml = ""
    if D.get("fetch_errors"):
        ErrorItems = "".join(f"<li>{_html.escape(E)}</li>" for E in D["fetch_errors"])
        WarningHtml = f"""
    <div style="background:#fef3c7;border:1px solid #f59e0b;border-radius:6px;
        padding:12px 16px;margin:12px 16px 0;">
        <div style="font-weight:700;color:#92400e;font-size:13px;">⚠ Broker Fetch Errors</div>
        <ul style="margin:6px 0 0;padding-left:18px;color:#78350f;font-size:12px;">
            {ErrorItems}
        </ul>
        <div style="font-size:11px;color:#a16207;margin-top:6px;">
            Positions from failed brokers are missing from this report.</div>
    </div>"""

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

        LOT_SIZES = {"NIFTY": 65, "BANKNIFTY": 30, "SENSEX": 20, "BANKEX": 15}

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
        {WarningHtml}
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

    # Skip holidays and weekends — stale prices would produce bogus swing numbers.
    # Use IsAnyExchangeOpen so the report still runs on equity-only holidays
    # when MCX evening session is open (positions may have moved).
    ReportDate = datetime.strptime(DateStr, "%Y-%m-%d").date()
    if not IsAnyExchangeOpen(ReportDate):
        Logger.info("Skipping report for %s — not a trading day for any exchange (holiday/weekend)", DateStr)
        return

    Logger.info("Generating report for %s (dry_run=%s)", DateStr, DryRun)

    with open(CONFIG_PATH) as f:
        FullConfig = json.load(f)

    # Fetch from brokers
    Positions, FetchErrors = _FetchOpenPositions(FullConfig)
    FuturesOrders, OptionsOrders = _FetchTodayOrders(FullConfig)

    # Total unrealized P&L = sum of (LTP - Entry) across all positions
    TotalPnl = sum(P["pnl"] for P in Positions)
    # Daily swing on open positions = sum of (LTP - Prev Close)
    OpenSwing = sum(P["daily_swing"] for P in Positions)

    # Accumulate realized P&L into JSON for capital tracking
    DailyRealized = _FetchDailyRealizedPnl()
    _UpdateRealizedPnlAccumulator(DailyRealized, DateStr, TotalPnl)

    # Total daily MTM = open position swing + realized from all exits today
    TotalRealizedToday = sum(DailyRealized.values())
    TotalDailyMtm = OpenSwing + TotalRealizedToday

    Logger.info("Total unrealized: %.2f | Open swing: %.2f | Realized today: %.2f | Daily MTM: %.2f",
                TotalPnl, OpenSwing, TotalRealizedToday, TotalDailyMtm)

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
        "total_daily_mtm": TotalDailyMtm,
        "open_swing": OpenSwing,
        "realized_today": TotalRealizedToday,
        "realized_by_account": DailyRealized,
        "position_count": len(Positions),
        "trade_count": len(FuturesOrders) + len(OptionsOrders),
        "futures_orders": FuturesOrders,
        "options_orders": OptionsOrders,
        "fetch_errors": FetchErrors,
    }

    Html = _BuildReportHtml(ReportData)
    DateDisplay = datetime.strptime(DateStr, "%Y-%m-%d").strftime("%d %b %Y")
    Subject = f"Daily P&L Report | {DateDisplay} | \u20b9{_FmtINR(TotalDailyMtm)}"

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
