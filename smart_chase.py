"""
Smart Chase Order Execution Module.

Replaces the legacy limit-then-sleep-then-market pattern with a dynamic
execution algorithm that:
  0. Pre-flight: market open delay, circuit limit detection, spread gate
  1. Assesses volatility (bid-ask spread + intraday range vs ATR)
  2. Selects execution mode: A (match), B (aggressive), C (passive)
  3. Places a limit order at the computed price
  4. Chase loop: polls every few seconds, widening the limit toward market
  5. Market fallback as last resort

Works with both Zerodha (Kite) and Angel (SmartAPI) brokers.
"""

import math
import time
import json
import logging
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from pathlib import Path

from Directories import workInputRoot

Logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────

EXCHANGE_OPEN_TIMES = {
    "MCX":   "09:00",
    "NFO":   "09:15",
    "NSE":   "09:15",
    "BFO":   "09:15",
    "NCDEX": "10:00",
}

EMAIL_CONFIG_PATH = Path(workInputRoot) / "email_config.json"


# ─── Main Entry Point ─────────────────────────────────────────────────

def SmartChaseExecute(BrokerSession, OrderDetails, ExecutionConfig, IsEntry, Broker, ATR):
    """
    Execute an order using the smart chase algorithm.

    Returns: (success: bool, order_id, fill_info: dict)
    """
    Config = ExecutionConfig
    TickSize = Config.get("tick_size", 0.05)
    Direction = 1 if OrderDetails["Tradetype"].lower() == "buy" else -1
    Exchange = OrderDetails["Exchange"]
    Instrument = OrderDetails.get("Tradingsymbol", "UNKNOWN")

    FillInfo = {
        "execution_mode": None, "initial_ltp": None,
        "initial_bid": None, "initial_ask": None,
        "initial_spread": None, "limit_price": None,
        "fill_price": None, "slippage": None,
        "chase_iterations": 0, "chase_duration_seconds": 0.0,
        "market_fallback": 0, "spread_ratio": None,
        "range_ratio": None, "settle_wait_seconds": 0.0,
    }

    try:
        # ── Step 0a: Market open delay ────────────────────────────
        _WaitForMarketOpen(Exchange, Config)

        # ── Step 0b: Fetch initial quote ──────────────────────────
        Quote = _FetchQuote(BrokerSession, OrderDetails, Broker)
        if Quote is None:
            Logger.error("%s: Failed to fetch quote", Instrument)
            return False, None, FillInfo

        # ── Step 0b: Circuit limit handling ───────────────────────
        SettleStart = time.time()
        Quote = _WaitForCircuitRelease(BrokerSession, OrderDetails, Quote,
                                       TickSize, Broker, Instrument)
        if Quote is None:
            Logger.error("%s: Circuit wait failed, could not get valid quote", Instrument)
            return False, None, FillInfo

        # ── Step 0c: Spread gate ──────────────────────────────────
        Quote = _WaitForSpreadToSettle(BrokerSession, OrderDetails, Quote,
                                       Config, TickSize, Broker, Instrument)
        FillInfo["settle_wait_seconds"] = round(time.time() - SettleStart, 1)

        # ── Step 1: Assess volatility ─────────────────────────────
        OHLC = _FetchOHLC(BrokerSession, OrderDetails, Broker)
        Mode, SpreadLevel, RangeLevel = _AssessVolatility(Quote, OHLC, ATR, Config)
        FillInfo["execution_mode"] = Mode
        FillInfo["spread_level"] = SpreadLevel
        FillInfo["range_level"] = RangeLevel

        Bid = Quote.get("best_bid", Quote.get("ltp", 0))
        Ask = Quote.get("best_ask", Quote.get("ltp", 0))
        LTP = Quote.get("ltp", 0)

        FillInfo["initial_ltp"] = LTP
        FillInfo["initial_bid"] = Bid
        FillInfo["initial_ask"] = Ask
        FillInfo["initial_spread"] = round(Ask - Bid, 4) if Ask and Bid else None
        FillInfo["spread_ratio"] = Quote.get("spread_ratio")
        FillInfo["range_ratio"] = Quote.get("range_ratio")
        FillInfo["atr"] = ATR
        FillInfo["ohlc"] = OHLC
        FillInfo["baseline_spread"] = Config.get("baseline_spread_ticks", 2) * TickSize
        FillInfo["depth"] = Quote.get("depth", {})

        Logger.info(
            "%s: Volatility assessment | spread=%.2f ratio=%.1f | range_ratio=%.2f → Mode %s",
            Instrument, Ask - Bid if Ask and Bid else 0,
            Quote.get("spread_ratio", 0), Quote.get("range_ratio", 0), Mode
        )

        # ── Step 2: Compute initial limit price ──────────────────
        Price = _ComputeInitialPrice(Mode, Quote, Config, Direction, TickSize)
        FillInfo["limit_price"] = Price

        Logger.info(
            "%s: Chase started | mode=%s | direction=%s | bid=%.2f ask=%.2f | limit=%.2f",
            Instrument, Mode, "BUY" if Direction > 0 else "SELL", Bid, Ask, Price
        )

        # ── Step 3: Place initial limit order ─────────────────────
        OrderDetails["Price"] = Price
        OrderDetails["Ordertype"] = "LIMIT"
        OrderId = _PlaceLimitOrder(BrokerSession, OrderDetails, Price, Broker)

        if not OrderId:
            Logger.error("%s: Failed to place initial limit order", Instrument)
            return False, None, FillInfo

        # ── Step 4: Chase loop ────────────────────────────────────
        PollInterval = Config.get("poll_interval_seconds", 4)
        MaxChaseSeconds = (Config.get("max_chase_seconds_entry", 50) if IsEntry
                          else Config.get("max_chase_seconds_exit", 35))
        ChaseStep = Config.get("chase_step_ticks", 1)
        MaxChaseTicks = Config.get("max_chase_ticks", 8)

        ChaseStart = time.time()
        CurrentOffset = 0
        CurrentPrice = Price
        Iterations = 0

        while (time.time() - ChaseStart) < MaxChaseSeconds:
            time.sleep(PollInterval)
            Iterations += 1

            Status, FilledQty, PendingQty, AvgPrice = _CheckOrderStatus(
                BrokerSession, OrderId, Broker
            )

            if Status == "COMPLETE":
                Elapsed = round(time.time() - ChaseStart, 1)
                FillInfo["fill_price"] = AvgPrice
                # Slippage: negative = favorable, positive = adverse
                # BUY: paying more than LTP is adverse (+), less is favorable (-)
                # SELL: receiving more than LTP is favorable (-), less is adverse (+)
                if AvgPrice:
                    RawSlip = AvgPrice - LTP
                    FillInfo["slippage"] = round(-RawSlip, 4) if Direction < 0 else round(RawSlip, 4)
                else:
                    FillInfo["slippage"] = None
                FillInfo["chase_iterations"] = Iterations
                FillInfo["chase_duration_seconds"] = Elapsed
                Logger.info(
                    "%s: Chase FILLED | price=%.2f | slippage=%.2f | iters=%d | elapsed=%.1fs",
                    Instrument, AvgPrice or 0,
                    (AvgPrice - LTP) if AvgPrice else 0, Iterations, Elapsed
                )
                _SendOrderEmail(OrderDetails, FillInfo, "FILLED")
                return True, OrderId, FillInfo

            if Status in ("REJECTED", "CANCELLED"):
                Logger.warning("%s: Order %s (status=%s)", Instrument, OrderId, Status)
                FillInfo["chase_iterations"] = Iterations
                FillInfo["chase_duration_seconds"] = round(time.time() - ChaseStart, 1)
                _SendOrderEmail(OrderDetails, FillInfo, Status)
                return False, OrderId, FillInfo

            # Still OPEN — chase the price
            if CurrentOffset < MaxChaseTicks:
                CurrentOffset = min(CurrentOffset + ChaseStep, MaxChaseTicks)

            FreshQuote = _FetchQuote(BrokerSession, OrderDetails, Broker)
            if FreshQuote:
                FreshRef = (FreshQuote.get("best_ask", FreshQuote["ltp"]) if Direction > 0
                            else FreshQuote.get("best_bid", FreshQuote["ltp"]))
                NewPrice = _RoundToTick(
                    FreshRef + Direction * CurrentOffset * TickSize,
                    TickSize, Direction
                )

                if NewPrice != CurrentPrice:
                    try:
                        _ModifyOrderPrice(BrokerSession, OrderDetails, OrderId,
                                          NewPrice, Broker)
                        CurrentPrice = NewPrice
                    except Exception as e:
                        Logger.warning("%s: Modify failed (iter %d): %s",
                                       Instrument, Iterations, e)

            Logger.info(
                "%s: Chase iter %d | status=%s | price=%.2f | elapsed=%.1fs",
                Instrument, Iterations, Status, CurrentPrice,
                time.time() - ChaseStart
            )

        # ── Step 5: Market fallback ───────────────────────────────
        Status, FilledQty, PendingQty, AvgPrice = _CheckOrderStatus(
            BrokerSession, OrderId, Broker
        )
        if Status == "COMPLETE":
            Elapsed = round(time.time() - ChaseStart, 1)
            FillInfo["fill_price"] = AvgPrice
            if AvgPrice:
                RawSlip = AvgPrice - LTP
                FillInfo["slippage"] = round(-RawSlip, 4) if Direction < 0 else round(RawSlip, 4)
            else:
                FillInfo["slippage"] = None
            FillInfo["chase_iterations"] = Iterations
            FillInfo["chase_duration_seconds"] = Elapsed
            _SendOrderEmail(OrderDetails, FillInfo, "FILLED")
            return True, OrderId, FillInfo

        Logger.info("%s: Chase budget exhausted, converting to MARKET", Instrument)
        FillInfo["market_fallback"] = 1

        try:
            _ConvertToMarket(BrokerSession, OrderDetails, OrderId, Broker)
        except Exception as e:
            Logger.error("%s: Market conversion failed: %s", Instrument, e)
            FillInfo["chase_iterations"] = Iterations
            FillInfo["chase_duration_seconds"] = round(time.time() - ChaseStart, 1)
            _SendOrderEmail(OrderDetails, FillInfo, "FAILED")
            return False, OrderId, FillInfo

        time.sleep(3)
        Status, FilledQty, PendingQty, AvgPrice = _CheckOrderStatus(
            BrokerSession, OrderId, Broker
        )
        Elapsed = round(time.time() - ChaseStart, 1)
        FillInfo["fill_price"] = AvgPrice
        if AvgPrice:
            RawSlip = AvgPrice - LTP
            FillInfo["slippage"] = round(-RawSlip, 4) if Direction < 0 else round(RawSlip, 4)
        else:
            FillInfo["slippage"] = None
        FillInfo["chase_iterations"] = Iterations
        FillInfo["chase_duration_seconds"] = Elapsed

        Success = (Status == "COMPLETE")
        _SendOrderEmail(OrderDetails, FillInfo, "FILLED (MARKET)" if Success else "FAILED")
        return Success, OrderId, FillInfo

    except Exception as e:
        Logger.exception("%s: SmartChaseExecute error: %s", Instrument, e)
        _SendOrderEmail(OrderDetails, FillInfo, f"ERROR: {e}")
        return False, None, FillInfo


# ─── Pre-flight Checks ────────────────────────────────────────────────

def _WaitForMarketOpen(Exchange, Config):
    """If within market_open_delay_seconds of exchange open, sleep the remainder."""
    OpenTimeStr = EXCHANGE_OPEN_TIMES.get(Exchange.upper())
    if not OpenTimeStr:
        return

    Delay = Config.get("market_open_delay_seconds", 10)
    Now = datetime.now()
    OpenTime = Now.replace(
        hour=int(OpenTimeStr.split(":")[0]),
        minute=int(OpenTimeStr.split(":")[1]),
        second=0, microsecond=0
    )
    Deadline = OpenTime + timedelta(seconds=Delay)

    if OpenTime <= Now < Deadline:
        WaitSecs = (Deadline - Now).total_seconds()
        Logger.info("Market open delay: waiting %.1fs for %s to settle", WaitSecs, Exchange)
        time.sleep(WaitSecs)


def _IsAtCircuit(Quote, TickSize):
    """Check if the market is at a circuit limit (any side)."""
    LTP = Quote.get("ltp", 0)
    Upper = Quote.get("upper_circuit_limit")
    Lower = Quote.get("lower_circuit_limit")

    AtUpper = Upper is not None and LTP >= Upper - TickSize
    AtLower = Lower is not None and LTP <= Lower + TickSize

    # Also check for one-sided book (no counterparty depth)
    Depth = Quote.get("depth", {})
    SellDepth = Depth.get("sell", [])
    BuyDepth = Depth.get("buy", [])
    NoSellers = all(level.get("quantity", 0) == 0 for level in SellDepth) if SellDepth else False
    NoBuyers = all(level.get("quantity", 0) == 0 for level in BuyDepth) if BuyDepth else False

    return AtUpper or AtLower or NoSellers or NoBuyers


def _WaitForCircuitRelease(BrokerSession, OrderDetails, Quote, TickSize,
                           Broker, Instrument):
    """Wait indefinitely until the market is no longer at a circuit limit.
    Returns the final quote once the circuit clears."""
    if not _IsAtCircuit(Quote, TickSize):
        return Quote

    Logger.warning("%s: Circuit limit detected (LTP=%.2f). Waiting for release...",
                   Instrument, Quote.get("ltp", 0))
    _SendCircuitAlert(Instrument, Quote)

    while True:
        time.sleep(15)
        FreshQuote = _FetchQuote(BrokerSession, OrderDetails, Broker)
        if FreshQuote is None:
            continue
        if not _IsAtCircuit(FreshQuote, TickSize):
            Logger.info("%s: Circuit released (LTP=%.2f)", Instrument, FreshQuote.get("ltp", 0))
            return FreshQuote


def _WaitForSpreadToSettle(BrokerSession, OrderDetails, Quote, Config,
                           TickSize, Broker, Instrument):
    """Wait until the spread comes below the extreme threshold or timeout."""
    BaselineSpread = Config.get("baseline_spread_ticks", 2) * TickSize
    ExtremeThreshold = 5 * BaselineSpread
    MaxWait = Config.get("max_settle_wait_seconds", 30)

    Bid = Quote.get("best_bid", 0)
    Ask = Quote.get("best_ask", 0)
    Spread = Ask - Bid if Ask and Bid else 0

    if Spread <= ExtremeThreshold:
        return Quote

    Logger.info("%s: Spread too wide (%.2f vs threshold %.2f), waiting...",
                Instrument, Spread, ExtremeThreshold)
    Start = time.time()
    FreshQuote = None

    while (time.time() - Start) < MaxWait:
        time.sleep(3)
        FreshQuote = _FetchQuote(BrokerSession, OrderDetails, Broker)
        if FreshQuote is None:
            continue

        # Re-check circuit on each refresh
        if _IsAtCircuit(FreshQuote, TickSize):
            Logger.info("%s: Circuit hit during spread gate, re-entering circuit wait",
                        Instrument)
            FreshQuote = _WaitForCircuitRelease(BrokerSession, OrderDetails,
                                                FreshQuote, TickSize, Broker, Instrument)
            if FreshQuote is None:
                continue

        Bid = FreshQuote.get("best_bid", 0)
        Ask = FreshQuote.get("best_ask", 0)
        Spread = Ask - Bid if Ask and Bid else 0

        if Spread <= ExtremeThreshold:
            Logger.info("%s: Spread settled (%.2f)", Instrument, Spread)
            return FreshQuote

    Logger.info("%s: Spread gate timeout (%.1fs), proceeding with Option B",
                Instrument, MaxWait)
    return FreshQuote if FreshQuote else Quote


# ─── Volatility Assessment ─────────────────────────────────────────────

def _AssessVolatility(Quote, OHLC, ATR, Config):
    """Assess market conditions and return execution mode: 'A', 'B', or 'C'."""
    TickSize = Config.get("tick_size", 0.05)
    BaselineSpread = Config.get("baseline_spread_ticks", 2) * TickSize

    Bid = Quote.get("best_bid", Quote.get("ltp", 0))
    Ask = Quote.get("best_ask", Quote.get("ltp", 0))
    Spread = Ask - Bid if Ask and Bid and Ask > Bid else 0

    # Signal 1: Spread ratio
    SpreadRatio = Spread / BaselineSpread if BaselineSpread > 0 else 1.0
    Quote["spread_ratio"] = round(SpreadRatio, 2)

    if SpreadRatio <= 1.5:
        SpreadLevel = "tight"
    elif SpreadRatio <= 3.0:
        SpreadLevel = "normal"
    else:
        SpreadLevel = "wide"

    # Signal 2: Intraday range vs ATR
    RangeRatio = 0.5  # default to "normal" if no data
    if OHLC and ATR and ATR > 0:
        High = OHLC.get("high", 0)
        Low = OHLC.get("low", 0)
        IntraRange = High - Low if High > Low else 0
        RangeRatio = IntraRange / ATR
    Quote["range_ratio"] = round(RangeRatio, 2)

    if RangeRatio <= 0.4:
        RangeLevel = "low"
    elif RangeRatio <= 0.8:
        RangeLevel = "normal"
    else:
        RangeLevel = "high"

    # Decision matrix
    Matrix = {
        ("low",    "tight"):  "C", ("low",    "normal"): "C", ("low",    "wide"): "C",
        ("normal", "tight"):  "C", ("normal", "normal"): "C", ("normal", "wide"): "A",
        ("high",   "tight"):  "A", ("high",   "normal"): "B", ("high",   "wide"): "B",
    }

    Mode = Matrix.get((RangeLevel, SpreadLevel), "A")
    return Mode, SpreadLevel, RangeLevel


# ─── Quote & OHLC Fetching ────────────────────────────────────────────

def _FetchQuote(BrokerSession, OrderDetails, Broker):
    """Fetch quote data (bid, ask, ltp, depth, circuit limits).
    Returns a normalized dict regardless of broker."""
    try:
        if Broker == "ZERODHA":
            ExSym = f"{OrderDetails['Exchange']}:{OrderDetails['Tradingsymbol']}"
            RawQuote = BrokerSession.quote([ExSym])
            Data = RawQuote.get(ExSym, {})

            Depth = Data.get("depth", {})
            BuyDepth = Depth.get("buy", [{}])
            SellDepth = Depth.get("sell", [{}])

            return {
                "ltp": Data.get("last_price", 0),
                "best_bid": BuyDepth[0].get("price", 0) if BuyDepth else 0,
                "best_ask": SellDepth[0].get("price", 0) if SellDepth else 0,
                "upper_circuit_limit": Data.get("upper_circuit_limit"),
                "lower_circuit_limit": Data.get("lower_circuit_limit"),
                "depth": Depth,
            }

        elif Broker == "ANGEL":
            LtpInfo = BrokerSession.ltpData(
                exchange=str(OrderDetails["Exchange"]),
                tradingsymbol=str(OrderDetails["Tradingsymbol"]),
                symboltoken=str(OrderDetails.get("Symboltoken", ""))
            )
            LTP = LtpInfo.get("data", {}).get("ltp", 0) if LtpInfo else 0
            # Angel doesn't provide depth — use LTP for both bid/ask
            return {
                "ltp": LTP,
                "best_bid": LTP,
                "best_ask": LTP,
                "upper_circuit_limit": None,
                "lower_circuit_limit": None,
                "depth": {},
            }

    except Exception as e:
        Logger.error("Failed to fetch quote: %s", e)
    return None


def _FetchOHLC(BrokerSession, OrderDetails, Broker):
    """Fetch today's OHLC. Returns dict with 'high', 'low' or None."""
    try:
        if Broker == "ZERODHA":
            ExSym = f"{OrderDetails['Exchange']}:{OrderDetails['Tradingsymbol']}"
            RawOHLC = BrokerSession.ohlc([ExSym])
            Data = RawOHLC.get(ExSym, {}).get("ohlc", {})
            return {
                "high": Data.get("high", 0),
                "low": Data.get("low", 0),
            }
        elif Broker == "ANGEL":
            # Angel doesn't have a simple ohlc endpoint — return None
            return None
    except Exception as e:
        Logger.error("Failed to fetch OHLC: %s", e)
    return None


# ─── Price Computation ─────────────────────────────────────────────────

def _ComputeInitialPrice(Mode, Quote, Config, Direction, TickSize):
    """Compute initial limit price based on execution mode."""
    Bid = Quote.get("best_bid", Quote.get("ltp", 0))
    Ask = Quote.get("best_ask", Quote.get("ltp", 0))
    BufferTicks = Config.get("buffer_ticks", 2)

    if Mode == "A":
        # Match counterparty: BUY at best_ask, SELL at best_bid
        Price = Ask if Direction > 0 else Bid
    elif Mode == "B":
        # Aggressive: BUY at best_ask + buffer, SELL at best_bid - buffer
        Buffer = BufferTicks * TickSize
        Price = (Ask + Buffer) if Direction > 0 else (Bid - Buffer)
    elif Mode == "C":
        # Passive: BUY at best_bid (join your side), SELL at best_ask
        Price = Bid if Direction > 0 else Ask
    else:
        Price = Ask if Direction > 0 else Bid

    return _RoundToTick(Price, TickSize, Direction)


def _RoundToTick(Price, TickSize, Direction):
    """Round price to valid tick size. BUY: ceil (round up). SELL: floor (round down)."""
    if TickSize <= 0:
        return Price
    # Determine decimal places from tick_size to avoid floating point drift
    Decimals = max(0, -math.floor(math.log10(TickSize))) if TickSize < 1 else 0
    # Round the division first to avoid floating point drift
    # e.g. 100.3/0.05 = 2005.9999... → round to 2006.0 before floor/ceil
    Ticks = round(Price / TickSize, 8)
    if Direction > 0:
        return round(math.ceil(Ticks) * TickSize, Decimals)
    else:
        return round(math.floor(Ticks) * TickSize, Decimals)


# ─── Order Operations (broker-agnostic) ────────────────────────────────

def _PlaceLimitOrder(BrokerSession, OrderDetails, Price, Broker):
    """Place a limit order. Returns order_id or None on failure."""
    try:
        if Broker == "ZERODHA":
            from Server_Order_Place import order
            OrderDetails["Price"] = Price
            OrderDetails["Ordertype"] = "LIMIT"
            OrderId = order(OrderDetails)
            return OrderId if OrderId and OrderId != 0 else None

        elif Broker == "ANGEL":
            OrderParams = {
                "variety": str(OrderDetails.get("Variety", "NORMAL")),
                "tradingsymbol": str(OrderDetails["Tradingsymbol"]).replace(" ", "").upper(),
                "symboltoken": str(OrderDetails.get("Symboltoken", "")),
                "transactiontype": str(OrderDetails["Tradetype"]).upper(),
                "exchange": str(OrderDetails["Exchange"]),
                "ordertype": "LIMIT",
                "producttype": str(OrderDetails.get("Product", "CARRYFORWARD")),
                "duration": str(OrderDetails.get("Validity", "DAY")),
                "price": str(Price),
                "squareoff": str(OrderDetails.get("Squareoff", "0")),
                "stoploss": str(OrderDetails.get("Stoploss", "0")),
                "quantity": str(OrderDetails["Quantity"]),
            }
            OrderId = BrokerSession.placeOrder(OrderParams)
            return OrderId if OrderId else None

    except Exception as e:
        Logger.error("Failed to place limit order: %s", e)
    return None


def _CheckOrderStatus(BrokerSession, OrderId, Broker):
    """Check order status. Returns (status, filled_qty, pending_qty, avg_price)."""
    try:
        if Broker == "ZERODHA":
            History = BrokerSession.order_history(order_id=OrderId)
            if History:
                Latest = History[-1]
                Status = Latest.get("status", "").upper()
                # Normalize Kite statuses
                if Status in ("COMPLETE", "COMPLETED"):
                    Status = "COMPLETE"
                return (
                    Status,
                    Latest.get("filled_quantity", 0),
                    Latest.get("pending_quantity", 0),
                    Latest.get("average_price", 0),
                )

        elif Broker == "ANGEL":
            OrderBook = BrokerSession.orderBook()
            if OrderBook and OrderBook.get("data"):
                for Order in OrderBook["data"]:
                    if str(Order.get("orderid")) == str(OrderId):
                        Status = str(Order.get("status", "")).lower()
                        if Status in ("complete", "completed"):
                            Status = "COMPLETE"
                        elif Status in ("rejected",):
                            Status = "REJECTED"
                        elif Status in ("cancelled",):
                            Status = "CANCELLED"
                        else:
                            Status = "OPEN"
                        return (
                            Status,
                            int(Order.get("filledshares", 0)),
                            int(Order.get("unfilledshares", 0)),
                            float(Order.get("averageprice", 0)),
                        )

    except Exception as e:
        Logger.error("Failed to check order status: %s", e)
    return ("UNKNOWN", 0, 0, 0)


def _ModifyOrderPrice(BrokerSession, OrderDetails, OrderId, NewPrice, Broker):
    """Modify an existing order to a new limit price."""
    if Broker == "ZERODHA":
        BrokerSession.modify_order(
            variety=BrokerSession.VARIETY_REGULAR,
            order_id=OrderId,
            price=NewPrice,
            order_type=BrokerSession.ORDER_TYPE_LIMIT,
        )

    elif Broker == "ANGEL":
        ModifyParams = {
            "variety": str(OrderDetails.get("Variety", "NORMAL")),
            "orderid": str(OrderId),
            "tradingsymbol": str(OrderDetails["Tradingsymbol"]).replace(" ", "").upper(),
            "symboltoken": str(OrderDetails.get("Symboltoken", "")),
            "transactiontype": str(OrderDetails["Tradetype"]).upper(),
            "exchange": str(OrderDetails["Exchange"]),
            "ordertype": "LIMIT",
            "producttype": str(OrderDetails.get("Product", "CARRYFORWARD")),
            "duration": str(OrderDetails.get("Validity", "DAY")),
            "quantity": str(OrderDetails["Quantity"]),
            "price": str(NewPrice),
        }
        BrokerSession.modifyOrder(ModifyParams)


def _ConvertToMarket(BrokerSession, OrderDetails, OrderId, Broker):
    """Convert an existing order to MARKET as last resort."""
    if Broker == "ZERODHA":
        BrokerSession.modify_order(
            variety=BrokerSession.VARIETY_REGULAR,
            order_id=OrderId,
            order_type=BrokerSession.ORDER_TYPE_MARKET,
        )

    elif Broker == "ANGEL":
        ModifyParams = {
            "variety": str(OrderDetails.get("Variety", "NORMAL")),
            "orderid": str(OrderId),
            "tradingsymbol": str(OrderDetails["Tradingsymbol"]).replace(" ", "").upper(),
            "symboltoken": str(OrderDetails.get("Symboltoken", "")),
            "transactiontype": str(OrderDetails["Tradetype"]).upper(),
            "exchange": str(OrderDetails["Exchange"]),
            "ordertype": "MARKET",
            "producttype": str(OrderDetails.get("Product", "CARRYFORWARD")),
            "duration": str(OrderDetails.get("Validity", "DAY")),
            "quantity": str(OrderDetails["Quantity"]),
            "price": "0",
        }
        BrokerSession.modifyOrder(ModifyParams)


# ─── Email Notifications ──────────────────────────────────────────────

def _SendOrderEmail(OrderDetails, FillInfo, Outcome):
    """Send email notification with full order details."""
    try:
        if not EMAIL_CONFIG_PATH.exists():
            return

        with open(EMAIL_CONFIG_PATH, "r") as f:
            EmailCfg = json.load(f)

        Instrument = OrderDetails.get("Tradingsymbol", "UNKNOWN")
        Action = OrderDetails.get("Tradetype", "").upper()
        Qty = OrderDetails.get("Quantity", "")
        FillPrice = FillInfo.get("fill_price", "N/A")
        Mode = FillInfo.get("execution_mode", "N/A")

        Subject = f"[Order {Outcome}] {Action} {Qty} {Instrument}"
        if FillPrice and FillPrice != "N/A":
            Subject += f" @ {FillPrice}"
        Subject += f" — {datetime.now().strftime('%Y-%m-%d %H:%M')}"

        # Build slippage display
        Slip = FillInfo.get('slippage', None)
        if Slip is not None:
            SlipLabel = f"{Slip:+.2f} ({'favorable' if Slip < 0 else 'adverse'})"
        else:
            SlipLabel = "N/A"

        # Execution mode descriptions
        ModeDesc = {
            "A": "Match (cross spread — hit counterparty's price)",
            "B": "Aggressive (cross spread + buffer ticks beyond)",
            "C": "Passive (join your side of book — wait for fill)",
        }
        ModeExplain = ModeDesc.get(Mode, Mode)

        # Matrix context
        SpreadLvl = FillInfo.get('spread_level', 'N/A')
        RangeLvl = FillInfo.get('range_level', 'N/A')

        # Build matrix display with marker
        def _MatrixCell(r, s, current_r, current_s):
            CellMap = {
                ("low","tight"):"C", ("low","normal"):"C", ("low","wide"):"C",
                ("normal","tight"):"C", ("normal","normal"):"C", ("normal","wide"):"A",
                ("high","tight"):"A", ("high","normal"):"B", ("high","wide"):"B",
            }
            Val = CellMap.get((r, s), "?")
            return f"[{Val}]" if r == current_r and s == current_s else f" {Val} "

        # Build volatility calculation breakdown
        OhlcData = FillInfo.get('ohlc') or {}
        OhlcHigh = OhlcData.get('high', 'N/A')
        OhlcLow = OhlcData.get('low', 'N/A')
        AtrVal = FillInfo.get('atr', 'N/A')
        BaselineSpread = FillInfo.get('baseline_spread', 'N/A')

        if OhlcHigh != 'N/A' and OhlcLow != 'N/A':
            IntraRange = OhlcHigh - OhlcLow
            RangeCalc = f"  Intraday High:  {OhlcHigh}\n  Intraday Low:   {OhlcLow}\n  Intraday Range: {IntraRange:.2f}\n  ATR:            {AtrVal}\n  Range/ATR:      {IntraRange:.2f} / {AtrVal} = {FillInfo.get('range_ratio', 'N/A')}"
        else:
            RangeCalc = f"  OHLC: N/A (defaulted to 0.5)\n  ATR:  {AtrVal}"

        ActualSpread = FillInfo.get('initial_spread', 'N/A')
        SpreadCalc = f"  Spread:         {ActualSpread}\n  Baseline:       {BaselineSpread}\n  Spread Ratio:   {ActualSpread} / {BaselineSpread} = {FillInfo.get('spread_ratio', 'N/A')}"

        # Build order book depth
        DepthData = FillInfo.get('depth', {})
        BuyDepth = DepthData.get('buy', [])
        SellDepth = DepthData.get('sell', [])
        DepthLines = "Order Book:\n  BID (Buy)                    ASK (Sell)\n  Price      Qty   Orders     Price      Qty   Orders\n"
        for i in range(min(5, max(len(BuyDepth), len(SellDepth)))):
            b = BuyDepth[i] if i < len(BuyDepth) else {}
            s = SellDepth[i] if i < len(SellDepth) else {}
            bp = f"{b.get('price', ''):>10}" if b.get('price') else "         -"
            bq = f"{b.get('quantity', ''):>5}" if b.get('quantity') else "    -"
            bo = f"{b.get('orders', ''):>5}" if b.get('orders') else "    -"
            sp = f"{s.get('price', ''):>10}" if s.get('price') else "         -"
            sq = f"{s.get('quantity', ''):>5}" if s.get('quantity') else "    -"
            so = f"{s.get('orders', ''):>5}" if s.get('orders') else "    -"
            DepthLines += f"  {bp} {bq} {bo}     {sp} {sq} {so}\n"

        MatrixStr = f"""
Decision Matrix (Range x Spread -> Mode):
  ┌──────────┬─────────┬─────────┬─────────┐
  │          │ Tight   │ Normal  │  Wide   │
  ├──────────┼─────────┼─────────┼─────────┤
  │ Low      │  {_MatrixCell('low','tight',RangeLvl,SpreadLvl)}    │  {_MatrixCell('low','normal',RangeLvl,SpreadLvl)}    │  {_MatrixCell('low','wide',RangeLvl,SpreadLvl)}    │
  │ Normal   │  {_MatrixCell('normal','tight',RangeLvl,SpreadLvl)}    │  {_MatrixCell('normal','normal',RangeLvl,SpreadLvl)}    │  {_MatrixCell('normal','wide',RangeLvl,SpreadLvl)}    │
  │ High     │  {_MatrixCell('high','tight',RangeLvl,SpreadLvl)}    │  {_MatrixCell('high','normal',RangeLvl,SpreadLvl)}    │  {_MatrixCell('high','wide',RangeLvl,SpreadLvl)}    │
  └──────────┴─────────┴─────────┴─────────┘
  Current: Range={RangeLvl}, Spread={SpreadLvl} -> Mode {Mode}

Thresholds:
  Range:  low <= 0.4 | normal <= 0.8 | high > 0.8
  Spread: tight <= 1.5 | normal <= 3.0 | wide > 3.0"""

        Body = f"""Instrument:     {Instrument}
Action:         {Action}
Quantity:       {Qty}
Fill Price:     {FillInfo.get('fill_price', 'N/A')}
Slippage:       {SlipLabel} vs LTP ({FillInfo.get('initial_ltp', 'N/A')})
Execution Mode: {Mode} — {ModeExplain}
Outcome:        {Outcome}

Market Context:
  Initial LTP:  {FillInfo.get('initial_ltp', 'N/A')}
  Best Bid:     {FillInfo.get('initial_bid', 'N/A')}
  Best Ask:     {FillInfo.get('initial_ask', 'N/A')}

Range Calculation:
{RangeCalc}

Spread Calculation:
{SpreadCalc}

{DepthLines}
Execution Details:
  Chase Iters:  {FillInfo.get('chase_iterations', 0)}
  Duration:     {FillInfo.get('chase_duration_seconds', 0)}s
  Market Fallback: {'Yes' if FillInfo.get('market_fallback') else 'No'}
  Settle Wait:  {FillInfo.get('settle_wait_seconds', 0)}s
{MatrixStr}

Broker Order ID: {OrderDetails.get('OrderId', 'N/A')}
"""

        import html as _html
        HtmlBody = f"""<html><body>
<pre style="font-family: Consolas, 'Courier New', monospace; font-size: 14px; line-height: 1.4; color: #222; background: #f9f9f9; padding: 16px; border-radius: 6px;">
{_html.escape(Body)}
</pre></body></html>"""
        Msg = MIMEText(HtmlBody, "html")
        Msg["Subject"] = Subject
        Msg["From"] = EmailCfg["sender"]
        Msg["To"] = EmailCfg["recipient"]

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as Server:
            Server.login(EmailCfg["sender"], EmailCfg["app_password"])
            Server.send_message(Msg)

        Logger.info("Order email sent: %s", Subject)

    except Exception as e:
        Logger.error("Failed to send order email: %s", e)


def _SendCircuitAlert(Instrument, Quote):
    """Send email alert when circuit limit is detected."""
    try:
        if not EMAIL_CONFIG_PATH.exists():
            return

        with open(EMAIL_CONFIG_PATH, "r") as f:
            EmailCfg = json.load(f)

        LTP = Quote.get("ltp", 0)
        Upper = Quote.get("upper_circuit_limit", "N/A")
        Lower = Quote.get("lower_circuit_limit", "N/A")

        Subject = f"[Circuit Alert] {Instrument} at circuit (LTP={LTP}) — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        Body = f"""Circuit limit detected on {Instrument}.

LTP:            {LTP}
Upper Circuit:  {Upper}
Lower Circuit:  {Lower}

Waiting for market to resume free trading before placing order.
"""

        Msg = MIMEText(Body)
        Msg["Subject"] = Subject
        Msg["From"] = EmailCfg["sender"]
        Msg["To"] = EmailCfg["recipient"]

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as Server:
            Server.login(EmailCfg["sender"], EmailCfg["app_password"])
            Server.send_message(Msg)

    except Exception as e:
        Logger.error("Failed to send circuit alert email: %s", e)
