"""
forecast_orchestrator.py — Carver-style forecast orchestrator.

Collects binary forecasts (+10/0/-10) from TradingView webhooks,
combines them with weights, computes one position per instrument,
rounds once, and executes via existing Kite/Angel order handlers.

Architecture:
  - One queue + one daemon worker thread per instrument (serial within, parallel across)
  - Flask returns 200 immediately; worker processes in background
  - Reconciliation runs every 15 minutes (alert-only, no auto-correction)
"""

import json
import math
import queue
import threading
import logging
import smtplib
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime

import forecast_db as db
from Kite_Server_Order_Handler import ControlOrderFlowKite
from Server_Order_Handler import ControlOrderFlowAngel
from Directories import workInputRoot

Logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

CONFIG_PATH = Path(__file__).parent / "instrument_config.json"

# Email config for reconciliation alerts (uses Directories.py for consistent path)
EMAIL_CONFIG_PATH = workInputRoot / "email_config.json"


class ForecastOrchestrator:
    """Central orchestrator that combines subsystem forecasts and executes trades."""

    def __init__(self, ConfigPath=None):
        self.ConfigPath = ConfigPath or CONFIG_PATH
        self._Started = False

        # Load instrument config
        with open(self.ConfigPath, "r") as f:
            FullConfig = json.load(f)

        self.Account = FullConfig["account"]
        self.Instruments = FullConfig["instruments"]

        # DryRun from config JSON: if present and true → dry run, otherwise live
        self.DryRun = self.Account.get("dry_run", False)

        # Build reverse lookup: webhook SystemName → config subsystem key
        # e.g. "S30A_GoldM" → ("GOLDM", "S30A"), "AUTO_S30A_GoldM" → ("GOLDM", "S30A")
        self._SystemNameMap = {}
        for InstName, InstCfg in self.Instruments.items():
            # Use explicit map if provided in config
            if "system_name_map" in InstCfg:
                for WebhookName, ConfigKey in InstCfg["system_name_map"].items():
                    self._SystemNameMap[WebhookName] = (InstName, ConfigKey)
            # Also register the config keys themselves as valid names
            for SysKey in InstCfg.get("subsystems", {}):
                self._SystemNameMap[SysKey] = (InstName, SysKey)

        # Initialize database
        db.InitDB()

        # One queue per instrument
        self.Queues = {}
        self._Workers = {}
        self._ReconTimer = None

        for InstName in self.Instruments:
            self.Queues[InstName] = queue.Queue()

    def Start(self):
        """Start all worker threads and the reconciliation timer.
        Safe to call multiple times — only starts once.
        """
        if self._Started:
            Logger.warning("Orchestrator already started, ignoring duplicate Start()")
            return
        self._Started = True

        for InstName in self.Instruments:
            t = threading.Thread(
                target=self._WorkerLoop,
                args=(InstName,),
                name=f"worker-{InstName}",
                daemon=True
            )
            t.start()
            self._Workers[InstName] = t
            Logger.info("Started worker thread for %s", InstName)

        # Start reconciliation timer
        self._ScheduleReconciliation()
        Logger.info("Forecast orchestrator started (DryRun=%s)", self.DryRun)

    # ─── System Name Resolution ─────────────────────────────────────

    def _ResolveSystemName(self, RawName, Instrument):
        """
        Resolve a webhook SystemName to the config subsystem key.

        Lookup order:
        1. Exact match in _SystemNameMap (covers explicit mappings + bare config keys)
        2. Strip known prefixes (AUTO_, AUTO2_, etc.) and instrument suffix, retry
        3. Fall back to RawName (will trigger a warning in HandleWebhook)

        Examples:
            "S30A"           → "S30A"  (exact match to config key)
            "S30A_GoldM"     → "S30A"  (strip suffix)
            "AUTO_S30A_GoldM"→ "S30A"  (strip prefix + suffix)
            "AUTO2_NIFTY_15A"→ "S15A"  (needs explicit system_name_map)
        """
        # 1. Exact match
        if RawName in self._SystemNameMap:
            MappedInst, ConfigKey = self._SystemNameMap[RawName]
            return ConfigKey

        # 2. Try stripping common prefixes and instrument suffix
        Name = RawName

        # Strip leading "AUTO_", "AUTO2_", etc.
        for Prefix in ("AUTO_", "AUTO2_", "AUTO3_"):
            if Name.startswith(Prefix):
                Name = Name[len(Prefix):]
                break

        # Strip trailing instrument name (e.g., "_GoldM", "_ZINC", "_NIFTY")
        Parts = Name.split("_")
        if len(Parts) >= 2:
            # Try first part as subsystem key
            Candidate = Parts[0]
            if Candidate in self._SystemNameMap:
                _, ConfigKey = self._SystemNameMap[Candidate]
                return ConfigKey

        # 3. Try the stripped name itself
        if Name in self._SystemNameMap:
            _, ConfigKey = self._SystemNameMap[Name]
            return ConfigKey

        # 4. Fall back to raw name (will log a warning upstream)
        return RawName

    # ─── Webhook Handler ──────────────────────────────────────────────

    def HandleWebhook(self, Payload):
        """
        Called by Flask on POST /forecast.
        Parses webhook, logs signal, upserts forecast, pushes to queue.
        Returns immediately so Flask can send 200 OK.

        Expected Payload:
        {
            "SystemName": "S30A_GoldM",
            "Instrument": "GOLDM",
            "Netposition": 1,       # 1, 0, or -1
            "ATR": 1200.5
        }
        """
        RawSystemName = Payload["SystemName"]
        Instrument = Payload["Instrument"]
        Netposition = int(Payload["Netposition"])
        ATR = float(Payload["ATR"])

        # Validate instrument is known
        if Instrument not in self.Instruments:
            Logger.warning("Unknown instrument in webhook: %s", Instrument)
            return {"status": "error", "message": f"Unknown instrument: {Instrument}"}

        # Normalize system name: map webhook name to config subsystem key
        SystemName = self._ResolveSystemName(RawSystemName, Instrument)

        # Validate subsystem is configured for this instrument
        Config = self.Instruments[Instrument]
        if SystemName not in Config.get("subsystems", {}):
            Logger.warning(
                "Unknown subsystem '%s' (raw: '%s') for instrument %s. "
                "Add it to system_name_map in instrument_config.json. Signal logged but not used.",
                SystemName, RawSystemName, Instrument
            )

        # Convert binary Netposition to forecast scale
        # +1 → +10, -1 → -10, 0 → 0
        if Netposition > 0:
            Forecast = 10.0
        elif Netposition < 0:
            Forecast = -10.0
        else:
            Forecast = 0.0

        # Log raw signal with original name (append-only, never overwritten)
        db.LogTVSignal(Instrument, RawSystemName, Netposition, ATR)

        # Upsert derived forecast using normalized config key
        db.UpsertForecast(Instrument, SystemName, Forecast, ATR)

        # Push to instrument queue (worker will process)
        self.Queues[Instrument].put(Instrument)

        Logger.info(
            "Webhook: %s | %s (raw: %s) | netpos=%d → forecast=%.0f | ATR=%.2f",
            Instrument, SystemName, RawSystemName, Netposition, Forecast, ATR
        )
        return {"status": "ok", "instrument": Instrument, "system": SystemName,
                "raw_system_name": RawSystemName}

    # ─── Worker Loop ──────────────────────────────────────────────────

    def _WorkerLoop(self, Instrument):
        """Per-instrument daemon thread. Blocks on queue, drains, computes."""
        Q = self.Queues[Instrument]
        while True:
            try:
                # Block until a signal arrives
                Q.get(block=True)

                # Drain any additional queued items (coalesce rapid signals)
                while not Q.empty():
                    try:
                        Q.get_nowait()
                    except queue.Empty:
                        break

                # Compute and (possibly) execute
                self._ComputeAndExecute(Instrument)

            except Exception as e:
                Logger.exception("Worker %s error: %s", Instrument, e)

    # ─── Core Algorithm ───────────────────────────────────────────────

    def _ComputeAndExecute(self, Instrument):
        """
        Core algorithm:
        1. Get all subsystem forecasts from DB
        2. Combine with weights × FDM
        3. Cap at ±20
        4. Compute vol_scalar from ATR (daily_vol_target has weights × IDM baked in)
        5. Compute position and round
        6. Check overrides
        7. Calculate delta vs confirmed_qty
        8. Apply position inertia
        9. Execute if needed
        """
        Config = self.Instruments[Instrument]

        # Skip disabled instruments (still logged signals, just don't compute)
        if not Config.get("enabled", True):
            Logger.debug("Instrument %s is disabled, skipping compute", Instrument)
            return

        # Step 1: Get all subsystem forecasts from DB
        ForecastsDB = db.GetForecastsForInstrument(Instrument)
        ForecastMap = {r["system_name"]: r for r in ForecastsDB}

        # Step 2: Weighted combination
        SubsystemWeights = Config["subsystems"]
        FDM = Config.get("FDM", 1.1)

        WeightedSum = 0.0
        for SysName, Weight in SubsystemWeights.items():
            FC = ForecastMap.get(SysName, {}).get("forecast", 0.0)
            WeightedSum += Weight * FC

        Combined = WeightedSum * FDM

        # Step 3: Cap at ±forecast_cap
        ForecastCap = Config.get("forecast_cap", 20)
        Combined = max(-ForecastCap, min(ForecastCap, Combined))

        # Step 4: Get ATR from the most recently updated subsystem
        if not ForecastsDB:
            Logger.warning("No forecasts in DB for %s, skipping", Instrument)
            return

        LatestRow = max(ForecastsDB, key=lambda r: r["updated_at"])
        ATR = LatestRow["atr"]
        if ATR <= 0:
            Logger.warning("%s: Latest ATR is <= 0 (%.2f), skipping", Instrument, ATR)
            return
        PointValue = Config["point_value"]
        ATRRupees = ATR * PointValue

        # Guard: ATR too small
        if ATRRupees < 1:
            Logger.warning(
                "%s: ATR in rupees too small (%.2f), skipping", Instrument, ATRRupees
            )
            return

        # Step 5: Vol scalar and position
        # daily_vol_target already has weights × IDM baked in from spreadsheet
        DailyVolTarget = Config["daily_vol_target"]
        VolScalar = DailyVolTarget / ATRRupees
        SubsystemPos = (Combined * VolScalar) / 10.0

        # Step 6: Round to get target (standard rounding, not banker's)
        # math.floor(x + 0.5) ensures 0.5 rounds up to 1, not down to 0
        if SubsystemPos >= 0:
            Target = math.floor(SubsystemPos + 0.5)
        else:
            Target = -math.floor(-SubsystemPos + 0.5)

        # Step 7: Check overrides
        Override = db.GetOverride(Instrument)
        if Override:
            OType = Override["override_type"]
            if OType == "FORCE_FLAT":
                Logger.info("%s: FORCE_FLAT override active, target → 0", Instrument)
                Target = 0
            elif OType == "SET_POSITION":
                Target = int(Override["value"])
                Logger.info("%s: SET_POSITION override active, target → %d", Instrument, Target)

        # Step 8: Get current confirmed position
        Pos = db.GetSystemPosition(Instrument)
        Current = Pos["confirmed_qty"]

        # Step 9: Calculate delta
        Delta = Target - Current

        # Step 10: Position inertia — skip if delta is too small relative to target
        InertiaPct = Config.get("position_inertia_pct", 0.10)
        if Target != 0 and abs(Delta) < InertiaPct * abs(Target):
            Logger.info(
                "%s: Inertia filter — delta=%d < %.0f%% of target=%d, skipping",
                Instrument, Delta, InertiaPct * 100, Target
            )
            # Still update target_qty so we track what we *would* want
            db.UpdateSystemPosition(Instrument, Target, Current)
            return

        # Skip if both target and current are 0
        if Target == 0 and Current == 0:
            Logger.debug("%s: target=0, current=0, nothing to do", Instrument)
            return

        # Skip if delta is 0
        if Delta == 0:
            Logger.debug("%s: delta=0, position already at target=%d", Instrument, Target)
            db.UpdateSystemPosition(Instrument, Target, Current)
            return

        Logger.info(
            "%s: combined=%.2f | vol_scalar=%.2f | pos=%.2f | "
            "target=%d | current=%d | delta=%d",
            Instrument, Combined, VolScalar, SubsystemPos,
            Target, Current, Delta
        )

        # Step 11: Execute
        if self.DryRun:
            Logger.info("[DRY RUN] %s: Would execute delta=%d (target=%d)", Instrument, Delta, Target)
            db.LogOrder(Instrument, "BUY" if Delta > 0 else "SELL", abs(Delta), "DRY_RUN",
                        Reason=f"target={Target}, current={Current}")
            db.UpdateSystemPosition(Instrument, Target, Target)
        else:
            self._ExecuteDelta(Instrument, Delta, Target)

    # ─── Order Execution ──────────────────────────────────────────────

    def _ExecuteDelta(self, Instrument, Delta, Target):
        """Build old-format order dict and route to existing Kite/Angel handler.

        The legacy handlers have inconsistent error handling:
        - PlaceOrderKiteAPI (Server_Order_Place.py:72) returns 0 on failure instead of raising
        - PlaceOrderAngelAPI (Server_Order_Handler.py:294) may return None on failure
        - ControlOrderFlowKite returns order_id (could be 0 on failure)
        - ControlOrderFlowAngel returns OrderIdDetails or OrderDetails dict

        We check the return value explicitly: falsy (0, None, empty) = failure.
        """
        Config = self.Instruments[Instrument]
        Pos = db.GetSystemPosition(Instrument)
        Current = Pos["confirmed_qty"]

        OrderDict = self._BuildOrderDict(Instrument, Delta, Target, Current)

        Broker = Config["broker"]
        Action = "BUY" if Delta > 0 else "SELL"

        Logger.info(
            "%s: Executing %s %d contracts via %s (target=%d)",
            Instrument, Action, abs(Delta), Broker, Target
        )

        try:
            Result = None
            if Broker == "ZERODHA":
                Result = ControlOrderFlowKite(OrderDict)
            elif Broker == "ANGEL":
                Result = ControlOrderFlowAngel(OrderDict)
            else:
                raise ValueError(f"Unknown broker: {Broker}")

            # Check for non-raising failures:
            # Kite returns order_id (int) — 0 means failure
            # Angel returns OrderIdDetails dict or order ID — None/empty means failure
            if Result is None or Result == 0 or Result == "":
                raise RuntimeError(
                    f"Broker handler returned falsy result: {Result!r}. "
                    f"Order likely not placed."
                )

            # Success: update confirmed_qty to target
            db.UpdateConfirmedQty(Instrument, Target)
            db.UpdateSystemPosition(Instrument, Target, Target)
            db.LogOrder(Instrument, Action, abs(Delta), "PLACED",
                        BrokerOrderId=str(Result) if Result else None,
                        Reason=f"target={Target}, prev={Current}")
            Logger.info("%s: Order placed successfully (id=%s), confirmed_qty → %d",
                        Instrument, Result, Target)

        except Exception as e:
            # Failure: confirmed_qty stays at current value (will retry on next signal)
            db.UpdateSystemPosition(Instrument, Target, Current)
            db.LogOrder(Instrument, Action, abs(Delta), "FAILED",
                        Reason=str(e))
            Logger.error("%s: Order FAILED: %s", Instrument, e)

    def _BuildOrderDict(self, Instrument, Delta, Target, Current):
        """
        Build the old-format order dict expected by ControlOrderFlowKite/Angel.

        Critical: Netposition mapping determines rollover and sleep behavior.
        - Exiting (target=0 or reducing): Netposition="0" → triggers rollover check + exit sleep
        - Entering/adding: Netposition=str(abs(delta)) → skip rollover + entry sleep
        """
        Config = self.Instruments[Instrument]
        Routing = Config["order_routing"]

        Tradetype = "buy" if Delta > 0 else "sell"

        # Netposition mapping for rollover logic compatibility
        NewTarget = Current + Delta
        if NewTarget == 0:
            Netposition = "0"               # full exit
        elif abs(NewTarget) < abs(Current):
            Netposition = "0"               # reducing position
        else:
            Netposition = str(abs(Delta))   # entering or adding

        # Quantity formatting
        QtyMultiplier = Routing.get("QuantityMultiplier")
        if QtyMultiplier and QtyMultiplier != 1:
            Quantity = f"{abs(Delta)}*{QtyMultiplier}"
        else:
            Quantity = str(abs(Delta))

        OrderDict = {
            "Tradetype": Tradetype,
            "Exchange": Config["exchange"],
            "Tradingsymbol": Instrument,
            "Quantity": Quantity,
            "Variety": Routing.get("Variety", "REGULAR"),
            "Ordertype": "LIMIT",
            "Product": Routing.get("Product", "NRML"),
            "Validity": Routing.get("Validity", "DAY"),
            "Price": "0",
            "Symboltoken": "",
            "Broker": Config["broker"],
            "Netposition": Netposition,
            "User": Config["user"],
            "UpdatedOrderRouting": "True",
            "ContractNameProvided": Routing.get("ContractNameProvided", "False"),
            "InstrumentType": Routing.get("InstrumentType", "FUT"),
            "DaysPostWhichSelectNextContract": Routing.get("DaysPostWhichSelectNextContract", "9"),
            "EntrySleepDuration": Routing.get("EntrySleepDuration", "60"),
            "ExitSleepDuration": Routing.get("ExitSleepDuration", "45"),
            "ConvertToMarketOrder": Routing.get("ConvertToMarketOrder", "True"),
        }

        # Angel-specific fields
        if Config["broker"] == "ANGEL":
            OrderDict["Squareoff"] = "0"
            OrderDict["Stoploss"] = "0"

        return OrderDict

    # ─── Override Management ──────────────────────────────────────────

    def ApplyOverride(self, Instrument, OverrideType, Value=None):
        """
        Apply a manual override to an instrument.
        OverrideType: FORCE_FLAT, SET_POSITION, CLEAR
        """
        if Instrument not in self.Instruments:
            return {"status": "error", "message": f"Unknown instrument: {Instrument}"}

        if OverrideType == "CLEAR":
            db.ClearOverride(Instrument)
            Logger.info("Override cleared for %s", Instrument)
            return {"status": "ok", "message": f"Override cleared for {Instrument}"}

        if OverrideType == "SET_POSITION" and Value is None:
            return {"status": "error", "message": "SET_POSITION requires a value"}

        db.SetOverride(Instrument, OverrideType, str(Value) if Value is not None else None)

        # Trigger recomputation
        self.Queues[Instrument].put(Instrument)

        return {"status": "ok", "message": f"Override {OverrideType} set for {Instrument}"}

    # ─── Reconciliation ───────────────────────────────────────────────

    def _ScheduleReconciliation(self):
        """Schedule the next reconciliation run (every 15 minutes)."""
        self._ReconTimer = threading.Timer(900.0, self._ReconWrapper)
        self._ReconTimer.daemon = True
        self._ReconTimer.start()

    def _ReconWrapper(self):
        """Wrapper to run reconciliation and reschedule."""
        try:
            self._RunReconciliation()
        except Exception as e:
            Logger.exception("Reconciliation error: %s", e)
        finally:
            self._ScheduleReconciliation()

    def _RunReconciliation(self):
        """
        Fetch broker positions, compare with system_positions, log and alert on mismatch.
        Groups by (broker, user) to minimize API connections.
        Alert-only: no automatic correction.
        """
        Logger.info("Running reconciliation check...")
        Mismatches = []

        # Group instruments by (broker, user)
        Groups = {}
        for InstName, Config in self.Instruments.items():
            if not Config.get("enabled", True):
                continue
            Key = (Config["broker"], Config["user"])
            Groups.setdefault(Key, []).append(InstName)

        for (Broker, User), InstList in Groups.items():
            try:
                BrokerPositions = self._FetchBrokerPositions(Broker, User)
            except Exception as e:
                Logger.error("Failed to fetch positions for %s/%s: %s", Broker, User, e)
                continue

            for InstName in InstList:
                Pos = db.GetSystemPosition(InstName)
                SystemQty = Pos["confirmed_qty"]

                # Look up broker position by matching root instrument name.
                # Broker returns full contract names like "GOLDM25APRFUT"
                # but our InstName is just "GOLDM". Match by startswith.
                BrokerQty = 0
                for BrokerSymbol, Qty in BrokerPositions.items():
                    if BrokerSymbol.startswith(InstName):
                        BrokerQty += Qty

                Match = (SystemQty == BrokerQty)
                db.LogReconciliation(InstName, SystemQty, BrokerQty, Match)

                if not Match:
                    Msg = (f"MISMATCH {InstName}: system={SystemQty}, broker={BrokerQty} "
                           f"({Broker}/{User})")
                    Logger.warning(Msg)
                    Mismatches.append(Msg)

        if Mismatches:
            self._SendReconAlert(Mismatches)
        else:
            Logger.info("Reconciliation complete — all positions match")

    def _FetchBrokerPositions(self, Broker, User):
        """
        Fetch current positions from broker API.
        Returns dict of {instrument: net_qty}.
        """
        Positions = {}

        try:
            if Broker == "ZERODHA":
                from kiteconnect import KiteConnect
                from Kite_Server_Order_Handler import EstablishConnectionKiteAPI

                Kite = EstablishConnectionKiteAPI({"User": User})
                if Kite:
                    NetPositions = Kite.positions().get("net", [])
                    for p in NetPositions:
                        Symbol = p.get("tradingsymbol", "")
                        Qty = p.get("quantity", 0)
                        if Qty != 0:
                            Positions[Symbol] = Qty

            elif Broker == "ANGEL":
                from Server_Order_Handler import EstablishConnectionAngelAPI

                SmartAPI = EstablishConnectionAngelAPI({"User": User})
                if SmartAPI:
                    PosData = SmartAPI.position()
                    if PosData and PosData.get("data"):
                        for p in PosData["data"]:
                            Symbol = p.get("tradingsymbol", "")
                            Qty = int(p.get("netqty", 0))
                            if Qty != 0:
                                Positions[Symbol] = Qty

        except Exception as e:
            Logger.error("Error fetching %s positions for %s: %s", Broker, User, e)
            raise

        return Positions

    def _SendReconAlert(self, Mismatches):
        """Send email alert for reconciliation mismatches."""
        try:
            if not EMAIL_CONFIG_PATH.exists():
                Logger.warning("No email config at %s, skipping alert", EMAIL_CONFIG_PATH)
                return

            with open(EMAIL_CONFIG_PATH, "r") as f:
                EmailCfg = json.load(f)

            Subject = f"[Trading Alert] Position Mismatch - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            Body = "Reconciliation found the following mismatches:\n\n"
            Body += "\n".join(f"  - {m}" for m in Mismatches)
            Body += "\n\nPlease review and take manual action if needed."

            Msg = MIMEText(Body)
            Msg["Subject"] = Subject
            Msg["From"] = EmailCfg["sender"]
            Msg["To"] = EmailCfg["recipient"]

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as Server:
                Server.login(EmailCfg["sender"], EmailCfg["app_password"])
                Server.send_message(Msg)

            Logger.info("Reconciliation alert email sent to %s", EmailCfg["recipient"])

        except Exception as e:
            Logger.error("Failed to send recon alert email: %s", e)

    # ─── Status ───────────────────────────────────────────────────────

    def GetStatus(self):
        """Return combined status dict for the /status endpoint."""
        return {
            "dry_run": self.DryRun,
            "instruments_enabled": {
                Name: Cfg.get("enabled", True)
                for Name, Cfg in self.Instruments.items()
            },
            "forecasts": db.GetAllForecasts(),
            "positions": db.GetAllPositions(),
            "overrides": db.GetAllOverrides(),
            "recent_orders": db.GetRecentOrders(20),
            "recent_signals": db.GetRecentTVSignals(limit=20),
            "recent_reconciliations": db.GetRecentReconciliations(10),
        }
