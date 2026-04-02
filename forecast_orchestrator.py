"""
forecast_orchestrator.py — Carver-style forecast orchestrator.

Collects binary forecasts (+10/0/-10) from TradingView webhooks,
combines them with weights, computes one position per instrument,
rounds once, and executes via existing Kite/Angel order handlers.

Architecture:
  - One queue + one daemon worker thread per instrument (serial within, parallel across)
  - Flask returns 200 immediately; worker processes in background
  - Reconciliation runs every 15 minutes (syncs confirmed_qty to broker and alerts on unexpected mismatches)
"""

import copy
import json
import math
import os
import time
import queue
import threading
import logging
import smtplib
import uuid
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime

import requests

import forecast_db as db
import Kite_Server_Order_Handler as KiteHandler
import Server_Order_Handler as AngelHandler
from Kite_Server_Order_Handler import ControlOrderFlowKite
from Server_Order_Handler import ControlOrderFlowAngel
from smart_chase import SmartChaseExecute, _CheckOrderStatus
from Directories import workInputRoot
from vol_target import compute_daily_vol_target

Logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

CONFIG_PATH = Path(__file__).parent / "instrument_config.json"
REALIZED_PNL_PATH = Path(workInputRoot) / "realized_pnl_accumulator.json"

# Email config for reconciliation alerts (uses Directories.py for consistent path)
EMAIL_CONFIG_PATH = workInputRoot / "email_config.json"


def _GetCumulativeRealizedPnlFromJson():
    """Read cumulative realized P&L + unrealized from the EOD accumulator file.

    Returns (cumulative_realized, eod_unrealized).
    Falls back to (DB realized, 0) if the JSON file doesn't exist yet.
    """
    try:
        with open(REALIZED_PNL_PATH, "r") as f:
            Data = json.load(f)
        Realized = float(Data.get("cumulative_realized_pnl", 0.0))
        Unrealized = float(Data.get("eod_unrealized", 0.0))
        Logger.info("Capital from JSON: realized=%.0f unrealized=%.0f (updated %s)",
                     Realized, Unrealized, Data.get("last_updated", "?"))
        return Realized, Unrealized
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        Logger.warning("Realized P&L JSON not found (%s), falling back to DB", e)
        return db.GetCumulativeRealizedPnl(), 0.0


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
        self.AngelRemoteExecutionUrl = (os.environ.get("ANGEL_REMOTE_EXECUTION_URL") or "").strip()
        self.AngelExecutorToken = (os.environ.get("ANGEL_EXECUTOR_TOKEN") or "").strip()
        try:
            self.AngelRemoteExecutionTimeoutSeconds = float(
                os.environ.get("ANGEL_REMOTE_EXECUTION_TIMEOUT_SECONDS", "60")
            )
        except ValueError:
            self.AngelRemoteExecutionTimeoutSeconds = 60.0
            Logger.warning(
                "Invalid ANGEL_REMOTE_EXECUTION_TIMEOUT_SECONDS value; defaulting to %.1f seconds",
                self.AngelRemoteExecutionTimeoutSeconds,
            )

        # Ensure the schema exists before any startup reads against optional newer tables.
        db.InitDB()

        # Compute effective capital = base + realized + unrealized (from EOD JSON)
        BaseCapital = self.Account["base_capital"]
        CumulativeRealized, EodUnrealized = _GetCumulativeRealizedPnlFromJson()
        EffectiveCapital = BaseCapital + CumulativeRealized + EodUnrealized
        Logger.info("Effective capital: base=%d + realized=%.0f + unrealized=%.0f = %.0f",
                     BaseCapital, CumulativeRealized, EodUnrealized, EffectiveCapital)

        # Compute daily vol targets from effective capital + allocation weights
        VolPct = self.Account["annual_vol_target_pct"]
        for InstName, Config in self.Instruments.items():
            VolWeights = Config.get("vol_weights")
            if VolWeights:
                Config["daily_vol_target"] = compute_daily_vol_target(
                    EffectiveCapital, VolPct, VolWeights
                )

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

        # One queue per instrument
        self.Queues = {}
        self._Workers = {}
        self._ReconTimer = None

        if self.AngelRemoteExecutionUrl:
            Logger.info(
                "Angel remote execution enabled | url=%s timeout_seconds=%.1f",
                self.AngelRemoteExecutionUrl,
                self.AngelRemoteExecutionTimeoutSeconds,
            )

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

    def _GetContractLookupName(self, Instrument):
        """Return the broker-facing root symbol used for contract lookup."""
        Config = self.Instruments[Instrument]
        Routing = Config.get("order_routing", {})
        return str(Routing.get("ContractLookupName") or Instrument)

    def _GetReconciliationPrefixes(self, Instrument):
        """Return broker symbol prefixes that identify this instrument's positions."""
        Config = self.Instruments[Instrument]
        Routing = Config.get("order_routing", {})
        Prefixes = Routing.get("ReconciliationPrefixes")

        if Prefixes is None:
            Prefixes = [self._GetContractLookupName(Instrument), Instrument]
        elif isinstance(Prefixes, str):
            Prefixes = [Prefixes]

        Normalized = []
        Seen = set()
        for Prefix in Prefixes:
            PrefixText = str(Prefix).strip().upper()
            if PrefixText and PrefixText not in Seen:
                Seen.add(PrefixText)
                Normalized.append(PrefixText)
        return Normalized

    def _CalculateBrokerQty(self, Instrument, BrokerPositions):
        """Aggregate broker net quantity for an instrument using configured prefixes."""
        Prefixes = self._GetReconciliationPrefixes(Instrument)
        BrokerQty = 0

        for BrokerSymbol, Qty in BrokerPositions.items():
            SymbolUpper = str(BrokerSymbol).strip().upper()
            for Prefix in Prefixes:
                if SymbolUpper.startswith(Prefix):
                    BrokerQty += Qty
                    break

        return BrokerQty

    def _SyncInstrumentWithBroker(self, Instrument, Config):
        """Refresh confirmed_qty from broker positions for a single instrument."""
        Pos = db.GetSystemPosition(Instrument)
        ConfirmedQty = Pos["confirmed_qty"]
        TargetQty = Pos["target_qty"]
        BrokerPositions = self._FetchBrokerPositions(Config["broker"], Config["user"])
        BrokerQty = self._CalculateBrokerQty(Instrument, BrokerPositions)

        if BrokerQty == ConfirmedQty:
            return {
                "changed": False,
                "confirmed_qty": ConfirmedQty,
                "target_qty": TargetQty,
                "broker_qty": BrokerQty,
            }

        db.UpdateSystemPosition(Instrument, TargetQty, BrokerQty)

        SyncedToTarget = (TargetQty != ConfirmedQty and BrokerQty == TargetQty)
        if SyncedToTarget:
            Logger.info(
                "%s: Broker position reached pending target; confirmed_qty synced %d -> %d",
                Instrument, ConfirmedQty, BrokerQty
            )
        else:
            Logger.warning(
                "%s: Broker position sync adjusted confirmed_qty %d -> %d (target=%d)",
                Instrument, ConfirmedQty, BrokerQty, TargetQty
            )

        return {
            "changed": True,
            "confirmed_qty": ConfirmedQty,
            "target_qty": TargetQty,
            "broker_qty": BrokerQty,
            "synced_to_target": SyncedToTarget,
        }

    def _ShouldUseRemoteAngelExecutor(self, OrderDict, Broker):
        """Return True when Angel NCDEX orders should be delegated to a remote worker."""
        if Broker != "ANGEL" or not self.AngelRemoteExecutionUrl:
            return False
        return str(OrderDict.get("Exchange", "")).strip().upper() == "NCDEX"

    def _ExecuteRemoteAngel(self, OrderDict):
        """Delegate an Angel order to the Windows browser worker and mirror its response."""
        RequestId = str(uuid.uuid4())
        Payload = {
            "request_id": RequestId,
            "source": "forecast_orchestrator",
            "instrument": str(OrderDict.get("Tradingsymbol") or ""),
            "order": copy.deepcopy(OrderDict),
        }
        Headers = {"Content-Type": "application/json"}
        if self.AngelExecutorToken:
            Headers["Authorization"] = f"Bearer {self.AngelExecutorToken}"

        Logger.info(
            "Forwarding Angel order to remote executor | request_id=%s url=%s order=%s",
            RequestId,
            self.AngelRemoteExecutionUrl,
            {
                "Exchange": OrderDict.get("Exchange"),
                "Tradingsymbol": OrderDict.get("Tradingsymbol"),
                "Tradetype": OrderDict.get("Tradetype"),
                "Quantity": OrderDict.get("Quantity"),
                "Ordertype": OrderDict.get("Ordertype"),
                "User": OrderDict.get("User"),
            },
        )

        try:
            Response = requests.post(
                self.AngelRemoteExecutionUrl,
                json=Payload,
                headers=Headers,
                timeout=self.AngelRemoteExecutionTimeoutSeconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Remote Angel execution request failed: {exc}") from exc

        try:
            ResponsePayload = Response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Remote Angel executor returned non-JSON response (status={Response.status_code})"
            ) from exc

        if not isinstance(ResponsePayload, dict):
            raise RuntimeError(
                f"Remote Angel executor returned invalid payload type: {type(ResponsePayload).__name__}"
            )

        RequestStatus = str(ResponsePayload.get("status") or "").strip().lower()
        if Response.status_code == 202:
            raise RuntimeError(
                ResponsePayload.get("message")
                or "Remote Angel execution request is already processing."
            )

        if ResponsePayload.get("execution_route") is not None:
            OrderDict["ExecutionRoute"] = ResponsePayload.get("execution_route")
        if ResponsePayload.get("order_id") is not None:
            OrderDict["OrderId"] = ResponsePayload.get("order_id")
        if ResponsePayload.get("warning"):
            OrderDict["LastOrderWarning"] = ResponsePayload.get("warning")
        if ResponsePayload.get("error"):
            OrderDict["LastOrderError"] = ResponsePayload.get("error")
        OrderDict["RemoteExecutionRequestId"] = RequestId

        if Response.status_code >= 400:
            raise RuntimeError(
                ResponsePayload.get("error")
                or ResponsePayload.get("message")
                or f"Remote Angel executor HTTP {Response.status_code}"
            )

        if RequestStatus == "error":
            raise RuntimeError(
                ResponsePayload.get("error")
                or ResponsePayload.get("message")
                or "Remote Angel executor reported an error."
            )

        Logger.info(
            "Remote Angel execution returned | request_id=%s status=%s execution_route=%s order_id=%s",
            RequestId,
            ResponsePayload.get("status"),
            ResponsePayload.get("execution_route"),
            ResponsePayload.get("order_id"),
        )
        return ResponsePayload

    @staticmethod
    def _FormatLegacyLimitPrice(Price):
        """Normalize price values for legacy order dicts."""
        if Price in (None, ""):
            return None
        PriceValue = float(Price)
        if PriceValue <= 0:
            return None
        return format(PriceValue, ".10f").rstrip("0").rstrip(".")

    def _PrimeAngelLegacyLimitPrice(self, Instrument, OrderDict):
        """Populate Angel LIMIT order price on Unix before legacy/browser execution."""
        if str(OrderDict.get("Broker", "")).strip().upper() != "ANGEL":
            return
        if str(OrderDict.get("Ordertype", "")).strip().upper() == "MARKET":
            return

        ExplicitPrice = self._FormatLegacyLimitPrice(OrderDict.get("Price"))
        if ExplicitPrice:
            OrderDict["Price"] = ExplicitPrice
            return

        WebhookLtp = db.GetLatestLTP(Instrument)
        WebhookPrice = self._FormatLegacyLimitPrice(WebhookLtp)
        if WebhookPrice:
            OrderDict["Price"] = WebhookPrice
            Logger.info(
                "%s: Using webhook LTP for Angel limit order | price=%s",
                Instrument,
                WebhookPrice,
            )
            return

        Logger.info(
            "%s: No webhook LTP supplied; fetching Angel LTP on Unix before execution",
            Instrument,
        )

        PreflightOrder = copy.deepcopy(OrderDict)
        Session = AngelHandler.EstablishConnectionAngelAPI(PreflightOrder)
        AngelHandler.ConfigureNetDirectionOfTrade(PreflightOrder)
        AngelHandler.Validate_Quantity(PreflightOrder)

        if PreflightOrder['ContractNameProvided'] == 'False':
            AngelHandler.PrepareInstrumentContractName(Session, PreflightOrder)
            if PreflightOrder.get("LastOrderError"):
                raise RuntimeError(PreflightOrder["LastOrderError"])

        PreparedOrder = AngelHandler.PrepareOrderAngel(Session, PreflightOrder)
        if PreparedOrder.get("LastOrderError"):
            raise RuntimeError(PreparedOrder["LastOrderError"])

        ResolvedPrice = self._FormatLegacyLimitPrice(PreparedOrder.get("Price"))
        if not ResolvedPrice:
            raise RuntimeError("Angel Unix LTP preflight did not produce a usable limit price.")

        for Key in ("Tradingsymbol", "Symboltoken", "Quantity", "Netposition"):
            if PreparedOrder.get(Key) not in (None, ""):
                OrderDict[Key] = PreparedOrder[Key]

        if PreparedOrder.get("Tradingsymbol") and PreparedOrder.get("Symboltoken"):
            OrderDict["ContractNameProvided"] = "True"

        OrderDict["Price"] = ResolvedPrice
        Logger.info(
            "%s: Retrieved Angel LTP on Unix | price=%s tradingsymbol=%s",
            Instrument,
            ResolvedPrice,
            OrderDict.get("Tradingsymbol"),
        )

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
            "ATR": 1200.5,
            "LTP": 72500.0          # optional, used for Angel limit-order routing
        }
        """
        RawSystemName = Payload["SystemName"]
        Instrument = Payload["Instrument"]
        Netposition = int(Payload["Netposition"])
        ATR = float(Payload["ATR"])
        LtpRaw = Payload.get("LTP", Payload.get("ltp"))
        Action = Payload.get("Action", "").lower().strip() or None  # "buy" or "sell" or None

        LTP = None
        if LtpRaw not in (None, ""):
            LTP = float(LtpRaw)
            if LTP <= 0:
                return {"status": "error", "message": f"Invalid LTP for {Instrument}: {LtpRaw}"}

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
        db.LogTVSignal(Instrument, RawSystemName, Netposition, ATR, Action, LTP)

        # Upsert derived forecast using normalized config key
        db.UpsertForecast(Instrument, SystemName, Forecast, ATR, Action)

        # Push to instrument queue (worker will process)
        self.Queues[Instrument].put(Instrument)

        Logger.info(
            "Webhook: %s | %s (raw: %s) | netpos=%d → forecast=%.0f | ATR=%.2f | LTP=%s | action=%s",
            Instrument, SystemName, RawSystemName, Netposition, Forecast, ATR, LTP, Action
        )
        return {"status": "ok", "instrument": Instrument, "system": SystemName,
                "raw_system_name": RawSystemName, "action": Action, "ltp": LTP}

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

        # Step 8: Refresh pending positions from broker before deciding whether to trade again.
        Pos = db.GetSystemPosition(Instrument)
        if Pos["target_qty"] != Pos["confirmed_qty"]:
            try:
                self._SyncInstrumentWithBroker(Instrument, Config)
                Pos = db.GetSystemPosition(Instrument)
            except Exception as e:
                Logger.warning(
                    "%s: Pending broker sync failed, keeping DB state as-is: %s",
                    Instrument, e
                )

        # Step 9: Get current confirmed position
        Current = Pos["confirmed_qty"]
        PendingTarget = Pos["target_qty"]

        # Step 10: Avoid resubmitting while a prior browser/LIMIT order is unresolved.
        if PendingTarget != Current:
            if Target == PendingTarget:
                Logger.info(
                    "%s: Existing pending target=%d with confirmed=%d; skipping duplicate execution",
                    Instrument, PendingTarget, Current
                )
                db.LogOrder(
                    Instrument, "BUY" if Target > Current else "SELL",
                    abs(Target - Current), "PENDING_SKIP",
                    Reason=f"pending_target={PendingTarget}, confirmed={Current}"
                )
                db.UpdateSystemPosition(Instrument, Target, Current)
                return

            PendingMsg = (
                f"{Instrument}: Pending target {PendingTarget} is unresolved with confirmed_qty "
                f"{Current}; new target {Target} will not be traded until broker sync."
            )
            Logger.warning(PendingMsg)
            db.LogOrder(
                Instrument, "BUY" if Target > Current else "SELL",
                abs(Target - Current), "PENDING_CONFLICT",
                Reason=PendingMsg
            )
            db.UpdateSystemPosition(Instrument, PendingTarget, Current)
            return

        # Step 11: Calculate delta
        Delta = Target - Current

        # Step 12: Position inertia — skip if delta is too small relative to target
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

        # Step 13: Direction cross-check — halt if orchestrator delta conflicts
        # with ALL subsystem actions from TradingView.
        # e.g., all subsystems say "sell" but orchestrator wants to BUY → DB out of sync
        SubsystemActions = [
            r.get("action") for r in ForecastsDB if r.get("action")
        ]
        if SubsystemActions:
            AllSell = all(a == "sell" for a in SubsystemActions)
            AllBuy = all(a == "buy" for a in SubsystemActions)

            if AllSell and Delta > 0:
                # All subsystems say SELL but we're about to BUY — conflict!
                ConflictMsg = (
                    f"DIRECTION CONFLICT {Instrument}: All subsystems say SELL "
                    f"but computed delta=+{Delta} (BUY). "
                    f"DB confirmed_qty={Current}, target={Target}. "
                    f"Likely DB out of sync with broker. TRADE HALTED."
                )
                Logger.error(ConflictMsg)
                db.LogOrder(Instrument, "BUY", abs(Delta), "HALTED_CONFLICT",
                            Reason=ConflictMsg)
                self._SendDirectionConflictAlert(Instrument, "SELL", "BUY", Delta, Current, Target, SubsystemActions)
                return

            if AllBuy and Delta < 0:
                # All subsystems say BUY but we're about to SELL — conflict!
                ConflictMsg = (
                    f"DIRECTION CONFLICT {Instrument}: All subsystems say BUY "
                    f"but computed delta={Delta} (SELL). "
                    f"DB confirmed_qty={Current}, target={Target}. "
                    f"Likely DB out of sync with broker. TRADE HALTED."
                )
                Logger.error(ConflictMsg)
                db.LogOrder(Instrument, "SELL", abs(Delta), "HALTED_CONFLICT",
                            Reason=ConflictMsg)
                self._SendDirectionConflictAlert(Instrument, "BUY", "SELL", Delta, Current, Target, SubsystemActions)
                return

        # Step 14: Execute
        if self.DryRun:
            Logger.info("[DRY RUN] %s: Would execute delta=%d (target=%d)", Instrument, Delta, Target)
            db.LogOrder(Instrument, "BUY" if Delta > 0 else "SELL", abs(Delta), "DRY_RUN",
                        Reason=f"target={Target}, current={Current}")
            db.UpdateSystemPosition(Instrument, Target, Target)
        else:
            self._ExecuteDelta(Instrument, Delta, Target)

    # ─── Order Execution ──────────────────────────────────────────────

    def _ExecuteDelta(self, Instrument, Delta, Target):
        """Build old-format order dict and route to smart chase or legacy handler.

        If the instrument has execution.use_smart_chase=true, runs the smart chase
        algorithm (pre-flight checks, volatility assessment, chase loop).
        Otherwise falls back to the legacy ControlOrderFlowKite/Angel path.
        """
        Config = self.Instruments[Instrument]
        Pos = db.GetSystemPosition(Instrument)
        Current = Pos["confirmed_qty"]

        OrderDict = self._BuildOrderDict(Instrument, Delta, Target, Current)

        Broker = Config["broker"]
        Action = "BUY" if Delta > 0 else "SELL"
        ExecConfig = Config.get("execution", {})

        Logger.info(
            "%s: Executing %s %d contracts via %s (target=%d, smart_chase=%s)",
            Instrument, Action, abs(Delta), Broker, Target,
            ExecConfig.get("use_smart_chase", False)
        )

        try:
            # Check ATR availability before committing to smart chase path.
            # Pre-steps modify OrderDict in-place, so we must decide the path
            # BEFORE running them (legacy handler does its own pre-steps).
            UseSmartChase = ExecConfig.get("use_smart_chase", False)
            if UseSmartChase:
                ATR = db.GetLatestATR(Instrument)
                if ATR is None or ATR <= 0:
                    Logger.warning("%s: No valid ATR for smart chase, falling back to legacy",
                                   Instrument)
                    UseSmartChase = False

            if UseSmartChase:
                # ── Smart Chase Path ──────────────────────────────────
                # Run pre-steps that the legacy handler normally does:
                # establish broker session, resolve contract name, validate qty
                if Broker == "ZERODHA":
                    Session = KiteHandler.EstablishConnectionKiteAPI(OrderDict)
                    KiteHandler.ConfigureNetDirectionOfTrade(OrderDict)
                    KiteHandler.Validate_Quantity(OrderDict)
                    if OrderDict['ContractNameProvided'] == 'False':
                        KiteHandler.PrepareInstrumentContractNameKite(Session, OrderDict)
                elif Broker == "ANGEL":
                    Session = AngelHandler.EstablishConnectionAngelAPI(OrderDict)
                    AngelHandler.ConfigureNetDirectionOfTrade(OrderDict)
                    AngelHandler.Validate_Quantity(OrderDict)
                    if OrderDict['ContractNameProvided'] == 'False':
                        AngelHandler.PrepareInstrumentContractName(Session, OrderDict)
                else:
                    raise ValueError(f"Unknown broker: {Broker}")

                IsEntry = abs(Target) > abs(Current)
                Success, OrderId, FillInfo = SmartChaseExecute(
                    Session, OrderDict, ExecConfig, IsEntry, Broker, ATR
                )

                if Success:
                    # P&L tracking BEFORE position update (needs old confirmed_qty for direction)
                    FillPrice = FillInfo.get("fill_price", 0)
                    PointValue = Config.get("point_value", 1)
                    if FillPrice > 0:
                        IsFlip = Current != 0 and Target != 0 and (Current > 0) != (Target > 0)
                        if IsFlip:
                            db.RealizePnl(Instrument, FillPrice, abs(Current), PointValue, "futures")
                            db.ResetCostBasis(Instrument)
                            db.UpdateCostBasis(Instrument, FillPrice, abs(Target), PointValue)
                        elif IsEntry:
                            db.UpdateCostBasis(Instrument, FillPrice, abs(Delta), PointValue)
                        else:
                            db.RealizePnl(Instrument, FillPrice, abs(Delta), PointValue, "futures")

                    db.UpdateConfirmedQty(Instrument, Target)
                    db.UpdateSystemPosition(Instrument, Target, Target)
                    db.LogSmartChaseOrder(
                        Instrument, Action, abs(Delta), "FILLED",
                        BrokerOrderId=str(OrderId) if OrderId else None,
                        FillInfo=FillInfo
                    )
                    Logger.info("%s: Smart chase FILLED (id=%s), confirmed_qty → %d",
                                Instrument, OrderId, Target)
                else:
                    # Log failure WITH full FillInfo (partial execution data)
                    db.LogSmartChaseOrder(
                        Instrument, Action, abs(Delta), "FAILED",
                        BrokerOrderId=str(OrderId) if OrderId else None,
                        Reason=f"mode={FillInfo.get('execution_mode')}",
                        FillInfo=FillInfo
                    )
                    raise RuntimeError(
                        f"Smart chase failed (order_id={OrderId}, "
                        f"mode={FillInfo.get('execution_mode')})"
                    )
                return

            # ── Legacy Path ───────────────────────────────────────────
            if Broker == "ANGEL":
                self._PrimeAngelLegacyLimitPrice(Instrument, OrderDict)

            Result = self._ExecuteLegacy(OrderDict, Broker)

            if isinstance(Result, dict):
                ResultStatus = str(Result.get("status", "")).strip().lower()
                if ResultStatus and ResultStatus not in {"submitted", "success", "accepted"}:
                    raise RuntimeError(
                        Result.get("error")
                        or Result.get("message")
                        or f"Broker handler returned status={ResultStatus}"
                    )

            # Check for non-raising failures:
            if Result is None or Result == 0 or Result == "":
                raise RuntimeError(
                    f"Broker handler returned falsy result: {Result!r}. "
                    f"Order likely not placed."
                )

            if (
                Broker == "ANGEL" and
                OrderDict.get("ExecutionRoute") == "ANGEL_WEB" and
                str(OrderDict.get("Ordertype", "")).upper() == "LIMIT"
            ):
                BrokerOrderId = OrderDict.get("OrderId") or "ANGEL_WEB_LIMIT_SUBMITTED"
                db.UpdateSystemPosition(Instrument, Target, Current)
                db.LogOrder(
                    Instrument, Action, abs(Delta), "SUBMITTED_PENDING",
                    BrokerOrderId=str(BrokerOrderId),
                    Reason=f"target={Target}, prev={Current}, route=ANGEL_WEB_LIMIT"
                )
                Logger.info(
                    "%s: Angel browser LIMIT order submitted (id=%s); awaiting broker fill confirmation",
                    Instrument, BrokerOrderId
                )
                return

            # Success: update confirmed_qty to target
            db.UpdateConfirmedQty(Instrument, Target)
            db.UpdateSystemPosition(Instrument, Target, Target)
            db.LogOrder(Instrument, Action, abs(Delta), "PLACED",
                        BrokerOrderId=str(Result) if Result else None,
                        Reason=f"target={Target}, prev={Current}")
            Logger.info("%s: Order placed successfully (id=%s), confirmed_qty → %d",
                        Instrument, Result, Target)

            # P&L tracking for legacy orders — fetch fill price after brief wait
            # Current/Target captured before position update, safe to use for direction
            PointValue = Config.get("point_value", 1)
            IsEntry = abs(Target) > abs(Current)
            WasLong = Current > 0
            try:
                time.sleep(3)
                if Broker == "ZERODHA":
                    LegacySession = KiteHandler.EstablishConnectionKiteAPI(OrderDict)
                elif Broker == "ANGEL":
                    LegacySession = AngelHandler.EstablishConnectionAngelAPI(OrderDict)
                else:
                    LegacySession = None

                if LegacySession and Result:
                    Status, FilledQty, _, AvgPrice = _CheckOrderStatus(LegacySession, str(Result), Broker)
                    if Status == "COMPLETE" and AvgPrice > 0:
                        IsFlip = Current != 0 and Target != 0 and (Current > 0) != (Target > 0)
                        if IsFlip:
                            db.RealizePnl(Instrument, AvgPrice, abs(Current), PointValue, "futures", WasLong=WasLong)
                            db.ResetCostBasis(Instrument)
                            db.UpdateCostBasis(Instrument, AvgPrice, abs(Target), PointValue)
                        elif IsEntry:
                            db.UpdateCostBasis(Instrument, AvgPrice, abs(Delta), PointValue,
                                               OldQty=abs(Current))
                        else:
                            db.RealizePnl(Instrument, AvgPrice, abs(Delta), PointValue, "futures", WasLong=WasLong)
                    else:
                        Logger.warning("%s: Legacy order status=%s avg_price=%.2f, skipping P&L",
                                       Instrument, Status, AvgPrice)
            except Exception as PnlErr:
                Logger.warning("%s: Failed to track P&L for legacy order: %s", Instrument, PnlErr)

        except Exception as e:
            # Failure: confirmed_qty stays at current value (will retry on next signal)
            db.UpdateSystemPosition(Instrument, Target, Current)
            db.LogOrder(Instrument, Action, abs(Delta), "FAILED",
                        Reason=str(e))
            Logger.error("%s: Order FAILED: %s", Instrument, e)

    def _ExecuteLegacy(self, OrderDict, Broker):
        """Route to the legacy ControlOrderFlow handler. Returns order_id."""
        if Broker == "ZERODHA":
            return ControlOrderFlowKite(OrderDict)
        elif Broker == "ANGEL":
            if self._ShouldUseRemoteAngelExecutor(OrderDict, Broker):
                return self._ExecuteRemoteAngel(OrderDict)
            return ControlOrderFlowAngel(OrderDict)
        else:
            raise ValueError(f"Unknown broker: {Broker}")

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
            "Tradingsymbol": self._GetContractLookupName(Instrument),
            "Quantity": Quantity,
            "UiQuantityLots": abs(Delta),
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
        Fetch broker positions, compare with system_positions, sync confirmed_qty,
        and alert on unexpected mismatches.
        Groups by (broker, user) to minimize API connections.
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
                TargetQty = Pos["target_qty"]
                BrokerQty = self._CalculateBrokerQty(InstName, BrokerPositions)

                SyncedToTarget = (TargetQty != SystemQty and BrokerQty == TargetQty)
                if BrokerQty != SystemQty:
                    db.UpdateSystemPosition(InstName, TargetQty, BrokerQty)

                Match = (SystemQty == BrokerQty) or SyncedToTarget
                db.LogReconciliation(InstName, SystemQty, BrokerQty, Match)

                if not Match:
                    Msg = (f"MISMATCH {InstName}: system={SystemQty}, broker={BrokerQty} "
                           f"({Broker}/{User})")
                    Logger.warning(Msg)
                    Mismatches.append(Msg)
                elif SyncedToTarget:
                    Logger.info(
                        "%s: Broker position reached target during reconciliation; confirmed_qty synced to %d",
                        InstName, BrokerQty
                    )

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
        """Send HTML email alert for reconciliation mismatches."""
        try:
            if not EMAIL_CONFIG_PATH.exists():
                Logger.warning("No email config at %s, skipping alert", EMAIL_CONFIG_PATH)
                return

            with open(EMAIL_CONFIG_PATH, "r") as f:
                EmailCfg = json.load(f)

            Timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            Subject = f"⚠️ Position Mismatch — {Timestamp}"

            # Parse mismatch details for the table
            Rows = ""
            for m in Mismatches:
                # Parse: "MISMATCH GOLDM: system=1, broker=0 (ZERODHA/YD6016)"
                try:
                    Parts = m.replace("MISMATCH ", "").split(": ")
                    Instrument = Parts[0]
                    Detail = Parts[1]
                    SystemQty = Detail.split("system=")[1].split(",")[0]
                    BrokerQty = Detail.split("broker=")[1].split(" ")[0]
                    BrokerInfo = Detail.split("(")[1].rstrip(")")
                    Delta = int(SystemQty) - int(BrokerQty)
                    DeltaSign = f"+{Delta}" if Delta > 0 else str(Delta)
                    DeltaColor = "#e74c3c" if abs(Delta) >= 2 else "#f39c12"
                except Exception:
                    Instrument = m
                    SystemQty = BrokerQty = DeltaSign = BrokerInfo = "?"
                    DeltaColor = "#e74c3c"

                Rows += f"""
                <tr>
                    <td style="padding: 10px 14px; border-bottom: 1px solid #eee; font-weight: 600;">{Instrument}</td>
                    <td style="padding: 10px 14px; border-bottom: 1px solid #eee; text-align: center;">{SystemQty}</td>
                    <td style="padding: 10px 14px; border-bottom: 1px solid #eee; text-align: center;">{BrokerQty}</td>
                    <td style="padding: 10px 14px; border-bottom: 1px solid #eee; text-align: center; color: {DeltaColor}; font-weight: 700;">{DeltaSign}</td>
                    <td style="padding: 10px 14px; border-bottom: 1px solid #eee; color: #888; font-size: 13px;">{BrokerInfo}</td>
                </tr>"""

            Html = f"""
            <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto;">
                <div style="background: linear-gradient(135deg, #e74c3c, #c0392b); padding: 20px 24px; border-radius: 10px 10px 0 0;">
                    <h2 style="color: white; margin: 0; font-size: 18px;">⚠️ Position Mismatch Detected</h2>
                    <p style="color: rgba(255,255,255,0.8); margin: 6px 0 0; font-size: 13px;">{Timestamp}</p>
                </div>

                <div style="background: #fff; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px; padding: 0;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                        <thead>
                            <tr style="background: #f8f9fa;">
                                <th style="padding: 12px 14px; text-align: left; color: #555; font-weight: 600; border-bottom: 2px solid #dee2e6;">Instrument</th>
                                <th style="padding: 12px 14px; text-align: center; color: #555; font-weight: 600; border-bottom: 2px solid #dee2e6;">System</th>
                                <th style="padding: 12px 14px; text-align: center; color: #555; font-weight: 600; border-bottom: 2px solid #dee2e6;">Broker</th>
                                <th style="padding: 12px 14px; text-align: center; color: #555; font-weight: 600; border-bottom: 2px solid #dee2e6;">Delta</th>
                                <th style="padding: 12px 14px; text-align: left; color: #555; font-weight: 600; border-bottom: 2px solid #dee2e6;">Account</th>
                            </tr>
                        </thead>
                        <tbody>{Rows}
                        </tbody>
                    </table>

                    <div style="padding: 16px 20px; background: #fff8e1; border-top: 1px solid #eee; border-radius: 0 0 10px 10px;">
                        <p style="margin: 0; font-size: 13px; color: #666;">
                            🔍 <strong>Action Required:</strong> Review positions and reconcile manually if needed.
                            Check <code>/status</code> endpoint for full details.
                        </p>
                    </div>
                </div>

                <p style="text-align: center; font-size: 11px; color: #aaa; margin-top: 16px;">
                    Forecast Orchestrator • Auto-generated alert • Do not reply
                </p>
            </div>"""

            Msg = MIMEText(Html, "html")
            Msg["Subject"] = Subject
            Msg["From"] = EmailCfg["sender"]
            Msg["To"] = EmailCfg["recipient"]
            Msg["X-Priority"] = "1"
            Msg["X-MSMail-Priority"] = "High"
            Msg["Importance"] = "High"

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as Server:
                Server.login(EmailCfg["sender"], EmailCfg["app_password"])
                Server.send_message(Msg)

            Logger.info("Reconciliation alert email sent to %s", EmailCfg["recipient"])

        except Exception as e:
            Logger.error("Failed to send recon alert email: %s", e)

    def _SendDirectionConflictAlert(self, Instrument, SubsystemDir, OrchestratorDir, Delta, Current, Target, Actions):
        """Send HTML email alert when direction cross-check fails. Trade is HALTED."""
        try:
            if not EMAIL_CONFIG_PATH.exists():
                Logger.warning("No email config at %s, skipping conflict alert", EMAIL_CONFIG_PATH)
                return

            with open(EMAIL_CONFIG_PATH, "r") as f:
                EmailCfg = json.load(f)

            Timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            Subject = f"🚨 TRADE HALTED — Direction Conflict on {Instrument} — {Timestamp}"

            Html = f"""
            <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto;">
                <div style="background: linear-gradient(135deg, #b71c1c, #880e0e); padding: 20px 24px; border-radius: 10px 10px 0 0;">
                    <h2 style="color: white; margin: 0; font-size: 18px;">🚨 TRADE HALTED — Direction Conflict</h2>
                    <p style="color: rgba(255,255,255,0.8); margin: 6px 0 0; font-size: 13px;">{Timestamp}</p>
                </div>

                <div style="background: #fff; border: 1px solid #e0e0e0; border-top: none; padding: 20px 24px;">
                    <p style="font-size: 15px; color: #333; margin: 0 0 16px;">
                        <strong>{Instrument}</strong> — All TradingView subsystems say
                        <span style="color: {'#e74c3c' if SubsystemDir == 'SELL' else '#27ae60'}; font-weight: 700;">{SubsystemDir}</span>
                        but the orchestrator computed a
                        <span style="color: {'#e74c3c' if OrchestratorDir == 'SELL' else '#27ae60'}; font-weight: 700;">{OrchestratorDir}</span>
                        delta of <strong>{Delta:+d}</strong>.
                    </p>

                    <table style="width: 100%; border-collapse: collapse; font-size: 14px; margin-bottom: 16px;">
                        <tr style="background: #f8f9fa;">
                            <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-weight: 600;">DB confirmed_qty</td>
                            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{Current}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-weight: 600;">Computed target</td>
                            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{Target}</td>
                        </tr>
                        <tr style="background: #f8f9fa;">
                            <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-weight: 600;">Computed delta</td>
                            <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #e74c3c; font-weight: 700;">{Delta:+d} ({OrchestratorDir})</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-weight: 600;">Subsystem actions</td>
                            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{', '.join(Actions)} → all {SubsystemDir}</td>
                        </tr>
                    </table>

                    <div style="background: #ffebee; border-left: 4px solid #e74c3c; padding: 12px 16px; border-radius: 4px;">
                        <p style="margin: 0; font-size: 13px; color: #c62828;">
                            <strong>Root cause:</strong> The system_positions DB is likely out of sync with the broker.
                            The orchestrator halted this trade to prevent placing an order in the wrong direction.
                        </p>
                    </div>

                    <div style="background: #fff8e1; border-left: 4px solid #f9a825; padding: 12px 16px; border-radius: 4px; margin-top: 12px;">
                        <p style="margin: 0; font-size: 13px; color: #666;">
                            <strong>To fix:</strong> Check your broker positions, then correct the DB:<br>
                            <code style="background: #f5f5f5; padding: 2px 6px; border-radius: 3px; font-size: 12px;">
                            sqlite3 forecast_store.db "UPDATE system_positions SET confirmed_qty=&lt;BROKER_QTY&gt;, target_qty=&lt;BROKER_QTY&gt; WHERE instrument='{Instrument}';"
                            </code>
                        </p>
                    </div>
                </div>

                <div style="background: #f5f5f5; padding: 12px 24px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
                    <p style="margin: 0; font-size: 11px; color: #999; text-align: center;">
                        Forecast Orchestrator • Direction Safety Check • Do not reply
                    </p>
                </div>
            </div>"""

            Msg = MIMEText(Html, "html")
            Msg["Subject"] = Subject
            Msg["From"] = EmailCfg["sender"]
            Msg["To"] = EmailCfg["recipient"]
            Msg["X-Priority"] = "1"
            Msg["X-MSMail-Priority"] = "High"
            Msg["Importance"] = "High"

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as Server:
                Server.login(EmailCfg["sender"], EmailCfg["app_password"])
                Server.send_message(Msg)

            Logger.info("Direction conflict alert sent for %s", Instrument)

        except Exception as e:
            Logger.error("Failed to send direction conflict alert: %s", e)

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
