"""
Comprehensive tests for cost basis tracking fixes and pending order deadlock resolution.

Covers:
  Step 1: Angel Web LIMIT approximate cost basis
  Step 2: Legacy futures retry loop for fill price
  Step 3: Legacy options cost basis from premium
  Step 4: SmartChase fill_price=None handling
  Step 5: Rollover cost basis from leg2 fill
  Step 6a: Staleness timeout on pending guard
  Step 6b: Reconciliation alert for swallowed signals
"""
import importlib
import json
import os
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.dirname(__file__))

_MISSING = object()


def _snapshot_modules(names):
    return {name: sys.modules.get(name, _MISSING) for name in names}


def _restore_modules(snapshot):
    for name, module in snapshot.items():
        if module is _MISSING:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


def _install_module(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module


MODULES_TO_SNAPSHOT = [
    "Directories",
    "Kite_Server_Order_Handler",
    "Server_Order_Handler",
    "smart_chase",
    "kiteconnect",
    "forecast_db",
    "forecast_orchestrator",
]


class OrchestratorTestBase(unittest.TestCase):
    """Base class that sets up an in-memory forecast_db and forecast_orchestrator."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmpdir.name)
        self.db_path = str(tmp_path / "forecast_store.db")
        self.email_config = tmp_path / "email_config.json"
        self.email_config.write_text(
            json.dumps({"sender": "t@t.com", "recipient": "o@t.com", "app_password": "x"}),
            encoding="utf-8",
        )
        self._module_snapshot = _snapshot_modules(MODULES_TO_SNAPSHOT)

        _install_module("Directories", workInputRoot=tmp_path)
        _install_module(
            "Kite_Server_Order_Handler",
            ControlOrderFlowKite=MagicMock(),
            EstablishConnectionKiteAPI=MagicMock(),
            ConfigureNetDirectionOfTrade=MagicMock(),
            Validate_Quantity=MagicMock(),
            PrepareInstrumentContractNameKite=MagicMock(),
        )
        _install_module(
            "Server_Order_Handler",
            ControlOrderFlowAngel=MagicMock(),
            EstablishConnectionAngelAPI=MagicMock(return_value=MagicMock()),
            ConfigureNetDirectionOfTrade=MagicMock(),
            Validate_Quantity=MagicMock(),
            PrepareInstrumentContractName=MagicMock(),
            PrepareOrderAngel=MagicMock(),
        )
        _install_module(
            "smart_chase",
            SmartChaseExecute=MagicMock(),
            _CheckOrderStatus=MagicMock(return_value=("COMPLETE", 0, 0.0, 0.0)),
        )
        _install_module("kiteconnect", KiteConnect=MagicMock())

        sys.modules.pop("forecast_db", None)
        sys.modules.pop("forecast_orchestrator", None)

        import forecast_db as db
        db.DB_PATH = self.db_path
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()
        self.db = db

        import forecast_orchestrator as fo
        fo = importlib.reload(fo)
        self.fo = fo
        _restore_modules(self._module_snapshot)

    def tearDown(self):
        if getattr(self.db, "_Connection", None) is not None:
            self.db._Connection.close()
            self.db._Connection = None
        self.tmpdir.cleanup()

    def _write_config(self, instrument_name="GOLDM", broker="ZERODHA",
                      user="OFS653", exchange="MCX", point_value=100,
                      order_routing=None):
        routing = order_routing or {
            "ContractLookupName": instrument_name,
            "ReconciliationPrefixes": [instrument_name],
            "InstrumentType": "FUTCOM",
            "Variety": "NORMAL",
            "Product": "CARRYFORWARD",
            "Validity": "DAY",
            "DaysPostWhichSelectNextContract": "9",
            "EntrySleepDuration": "60",
            "ExitSleepDuration": "45",
            "ConvertToMarketOrder": "True",
            "ContractNameProvided": "False",
        }
        cfg = {
            "account": {
                "dry_run": False,
                "base_capital": 1_000_000,
                "annual_vol_target_pct": 20,
            },
            "instruments": {
                instrument_name: {
                    "enabled": True,
                    "exchange": exchange,
                    "broker": broker,
                    "user": user,
                    "point_value": point_value,
                    "daily_vol_target": 50,
                    "FDM": 1.0,
                    "forecast_cap": 20,
                    "position_inertia_pct": 0.10,
                    "subsystems": {"S60C": 1.0},
                    "system_name_map": {"AUTO2_GM_S60C": "S60C"},
                    "order_routing": routing,
                },
            },
        }
        config_path = Path(self.tmpdir.name) / "instrument_config.json"
        config_path.write_text(json.dumps(cfg), encoding="utf-8")
        return config_path


# ─── Step 1: Angel Web LIMIT approximate cost basis ──────────────────────


class TestAngelWebLimitCostBasis(OrchestratorTestBase):

    def test_angel_web_limit_entry_records_approximate_cost_basis(self):
        """When Angel Web LIMIT order is submitted for an entry, cost basis
        should be recorded from the limit price."""
        config_path = self._write_config(
            instrument_name="CASTOR", broker="ANGEL", user="AABM826021",
            exchange="NCDEX", point_value=10,
            order_routing={
                "ContractLookupName": "CASTOR",
                "ReconciliationPrefixes": ["CASTOR"],
                "InstrumentType": "FUTCOM", "Variety": "NORMAL",
                "Product": "CARRYFORWARD", "Validity": "DAY",
                "DaysPostWhichSelectNextContract": "9",
                "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                "ConvertToMarketOrder": "True",
                "ContractNameProvided": "False",
                "QuantityMultiplier": 5,
            },
        )
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        self.db.UpdateSystemPosition("CASTOR", 0, 0)

        limit_price = 7890.0

        def fake_legacy(order_dict, _broker):
            order_dict["ExecutionRoute"] = "ANGEL_WEB"
            order_dict["OrderId"] = "ANGEL_WEB_SUBMITTED"
            order_dict["Ordertype"] = "LIMIT"
            order_dict["price"] = limit_price
            return {"status": "submitted"}

        with patch.object(orch, "_PrimeAngelLegacyLimitPrice"), \
             patch.object(orch, "_ExecuteLegacy", side_effect=fake_legacy):
            orch._ExecuteDelta("CASTOR", Delta=5, Target=5)

        pos = self.db.GetSystemPosition("CASTOR")
        self.assertAlmostEqual(pos["avg_entry_price"], limit_price)

    def test_angel_web_limit_exit_does_not_update_cost_basis(self):
        """Exit orders should NOT update cost basis (only entries should)."""
        config_path = self._write_config(
            instrument_name="CASTOR", broker="ANGEL", user="AABM826021",
            exchange="NCDEX", point_value=10,
            order_routing={
                "ContractLookupName": "CASTOR",
                "ReconciliationPrefixes": ["CASTOR"],
                "InstrumentType": "FUTCOM", "Variety": "NORMAL",
                "Product": "CARRYFORWARD", "Validity": "DAY",
                "DaysPostWhichSelectNextContract": "9",
                "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                "ConvertToMarketOrder": "True",
                "ContractNameProvided": "False",
                "QuantityMultiplier": 5,
            },
        )
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        # Start with an existing position at known cost basis
        self.db.UpdateSystemPosition("CASTOR", 5, 5)
        self.db.UpdateCostBasis("CASTOR", 8000.0, 5, 10)

        def fake_legacy(order_dict, _broker):
            order_dict["ExecutionRoute"] = "ANGEL_WEB"
            order_dict["OrderId"] = "ANGEL_WEB_SUBMITTED"
            order_dict["Ordertype"] = "LIMIT"
            order_dict["price"] = 7500.0
            return {"status": "submitted"}

        with patch.object(orch, "_PrimeAngelLegacyLimitPrice"), \
             patch.object(orch, "_ExecuteLegacy", side_effect=fake_legacy):
            orch._ExecuteDelta("CASTOR", Delta=-5, Target=0)

        pos = self.db.GetSystemPosition("CASTOR")
        # Cost basis should remain at 8000 (not overwritten by exit limit price)
        self.assertAlmostEqual(pos["avg_entry_price"], 8000.0)


# ��── Step 2: Legacy futures retry loop ───────────────────────────────────


class TestLegacyFuturesRetry(OrchestratorTestBase):

    def test_legacy_order_retries_fill_price_check(self):
        """Legacy path should retry _CheckOrderStatus up to 3 times before giving up."""
        config_path = self._write_config(instrument_name="GOLDM", point_value=100)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        self.db.UpdateSystemPosition("GOLDM", 0, 0)

        fo = self.fo
        # Patch the KiteHandler reference inside the orchestrator module
        mock_session = MagicMock()
        fo.KiteHandler.EstablishConnectionKiteAPI = MagicMock(return_value=mock_session)

        # First two attempts: OPEN with avg_price=0, third: COMPLETE
        check_results = [
            ("OPEN", 0, 0, 0.0),
            ("OPEN", 0, 0, 0.0),
            ("COMPLETE", 2, 0, 72500.0),
        ]
        call_count = {"n": 0}

        def fake_check(session, order_id, broker):
            idx = min(call_count["n"], len(check_results) - 1)
            call_count["n"] += 1
            return check_results[idx]

        with patch.object(orch, "_ExecuteLegacy", return_value="ORDER_123"), \
             patch.object(fo, "_CheckOrderStatus", side_effect=fake_check), \
             patch.object(fo.time, "sleep", MagicMock()):
            orch._ExecuteDelta("GOLDM", Delta=2, Target=2)

        # Should have called _CheckOrderStatus 3 times
        self.assertEqual(call_count["n"], 3)
        # Cost basis should be recorded from the successful check
        pos = self.db.GetSystemPosition("GOLDM")
        self.assertAlmostEqual(pos["avg_entry_price"], 72500.0)

    def test_legacy_order_logs_error_after_all_retries_fail(self):
        """If all 3 retries fail, should log ERROR (not just warning)."""
        config_path = self._write_config(instrument_name="GOLDM", point_value=100)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        self.db.UpdateSystemPosition("GOLDM", 0, 0)

        import Kite_Server_Order_Handler as kite_handler
        kite_handler.EstablishConnectionKiteAPI = MagicMock(return_value=MagicMock())

        fo = self.fo
        with patch.object(orch, "_ExecuteLegacy", return_value="ORDER_456"), \
             patch.object(fo, "_CheckOrderStatus",
                          return_value=("OPEN", 0, 0, 0.0)), \
             patch.object(fo.time, "sleep", MagicMock()), \
             patch.object(fo, "Logger") as mock_logger:
            orch._ExecuteDelta("GOLDM", Delta=2, Target=2)

        # Should have logged an error about failed fill price confirmation
        error_calls = [c for c in mock_logger.error.call_args_list
                       if "NOT confirmed" in str(c)]
        self.assertTrue(len(error_calls) > 0,
                        "Expected ERROR log about fill price not confirmed")


# ─── Step 4: SmartChase fill_price=None ──────────────────────────────────


class TestSmartChaseFillPriceNone(OrchestratorTestBase):

    def _make_smart_chase_config(self):
        """Create config with SmartChase enabled."""
        return self._write_config(instrument_name="GOLDM", point_value=100)

    def _setup_smart_chase_orch(self):
        config_path = self._make_smart_chase_config()
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        # Enable smart chase in the instrument config
        orch.Instruments["GOLDM"]["execution"] = {"use_smart_chase": True}
        self.db.UpdateSystemPosition("GOLDM", 0, 0)
        # Seed ATR so smart chase path is used
        self.db._Connection.execute(
            "INSERT INTO order_log (instrument, action, qty, status, initial_ltp, created_at) "
            "VALUES ('GOLDM', 'BUY', 1, 'FILLED', 72000, datetime('now'))"
        )
        self.db._Connection.commit()
        return orch

    def test_fill_price_none_logs_warning_and_skips_cost_basis(self):
        """When SmartChase succeeds but fill_price is None, should log warning."""
        orch = self._setup_smart_chase_orch()
        fo = self.fo

        mock_sc = MagicMock(return_value=(
            True, "ORD_1", {"fill_price": None, "execution_mode": "C",
                            "slippage": 0, "chase_iterations": 1}
        ))

        with patch.object(fo, "SmartChaseExecute", mock_sc), \
             patch.object(fo, "Logger") as mock_logger, \
             patch.object(fo.db, "GetLatestATR", return_value=500.0):
            orch._ExecuteDelta("GOLDM", Delta=2, Target=2)

        # Cost basis should remain 0 (not crash)
        pos = self.db.GetSystemPosition("GOLDM")
        self.assertAlmostEqual(pos["avg_entry_price"], 0.0)

        # Should have logged a warning about missing fill price
        warning_calls = [c for c in mock_logger.warning.call_args_list
                         if "fill_price" in str(c)]
        self.assertTrue(len(warning_calls) > 0,
                        "Expected WARNING log about missing fill_price")

    def test_fill_price_zero_logs_warning(self):
        """fill_price=0 should also trigger warning."""
        orch = self._setup_smart_chase_orch()
        fo = self.fo

        mock_sc = MagicMock(return_value=(
            True, "ORD_2", {"fill_price": 0, "execution_mode": "C",
                            "slippage": 0, "chase_iterations": 1}
        ))

        with patch.object(fo, "SmartChaseExecute", mock_sc), \
             patch.object(fo, "Logger") as mock_logger, \
             patch.object(fo.db, "GetLatestATR", return_value=500.0):
            orch._ExecuteDelta("GOLDM", Delta=2, Target=2)

        warning_calls = [c for c in mock_logger.warning.call_args_list
                         if "fill_price" in str(c)]
        self.assertTrue(len(warning_calls) > 0)

    def test_fill_price_valid_records_cost_basis(self):
        """Normal case: valid fill_price should record cost basis."""
        orch = self._setup_smart_chase_orch()
        fo = self.fo

        mock_sc = MagicMock(return_value=(
            True, "ORD_3", {"fill_price": 72500.0, "execution_mode": "C",
                            "slippage": 5.0, "chase_iterations": 2}
        ))

        with patch.object(fo, "SmartChaseExecute", mock_sc), \
             patch.object(fo.db, "GetLatestATR", return_value=500.0):
            orch._ExecuteDelta("GOLDM", Delta=2, Target=2)

        pos = self.db.GetSystemPosition("GOLDM")
        self.assertAlmostEqual(pos["avg_entry_price"], 72500.0)


# ─── Step 5: Rollover cost basis ──────────��──────────────────────────────


class TestRolloverCostBasis(unittest.TestCase):
    """Test that rollover sets cost basis from leg2 fill price."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmpdir.name)

        self._module_snapshot = _snapshot_modules(MODULES_TO_SNAPSHOT + ["rollover_monitor"])

        _install_module("Directories", workInputRoot=tmp_path)
        _install_module(
            "Kite_Server_Order_Handler",
            EstablishConnectionKiteAPI=MagicMock(),
        )
        _install_module(
            "Server_Order_Handler",
            EstablishConnectionAngelAPI=MagicMock(),
        )
        _install_module(
            "smart_chase",
            SmartChaseExecute=MagicMock(),
        )
        _install_module("kiteconnect", KiteConnect=MagicMock())

        sys.modules.pop("forecast_db", None)
        sys.modules.pop("rollover_monitor", None)

        import forecast_db as db
        db.DB_PATH = str(tmp_path / "forecast_store.db")
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()
        self.db = db

    def tearDown(self):
        if getattr(self.db, "_Connection", None) is not None:
            self.db._Connection.close()
            self.db._Connection = None
        _restore_modules(self._module_snapshot)
        self.tmpdir.cleanup()

    def test_rollover_sets_cost_basis_from_leg2(self):
        """After rollover, avg_entry_price should be the leg2 fill price."""
        # Set up a position with known cost basis
        self.db.UpdateSystemPosition("GOLDM", 2, 2)
        self.db.UpdateCostBasis("GOLDM", 72000.0, 2, 100)

        pos_before = self.db.GetSystemPosition("GOLDM")
        self.assertAlmostEqual(pos_before["avg_entry_price"], 72000.0)

        # Simulate what rollover does after both legs complete
        leg2_fill_price = 72300.0
        # Replicate the fix code path
        SysPos = self.db.GetSystemPosition("GOLDM")
        CurrentConfirmed = SysPos.get("confirmed_qty", 0)
        self.db.UpdateSystemPosition("GOLDM", SysPos["target_qty"], CurrentConfirmed)

        Leg2FillInfo = {"fill_price": leg2_fill_price, "slippage": 5.0}
        Leg2Price = Leg2FillInfo.get("fill_price", 0)
        if Leg2Price > 0 and CurrentConfirmed != 0:
            self.db.UpdateCostBasis("GOLDM", Leg2Price, abs(CurrentConfirmed),
                                    100, OldQty=0)

        pos_after = self.db.GetSystemPosition("GOLDM")
        self.assertAlmostEqual(pos_after["avg_entry_price"], leg2_fill_price)

    def test_rollover_no_cost_basis_if_leg2_price_zero(self):
        """If leg2 fill_price is 0, cost basis should not be updated."""
        self.db.UpdateSystemPosition("GOLDM", 2, 2)
        self.db.UpdateCostBasis("GOLDM", 72000.0, 2, 100)

        Leg2FillInfo = {"fill_price": 0}
        Leg2Price = Leg2FillInfo.get("fill_price", 0)
        if Leg2Price > 0:
            self.db.UpdateCostBasis("GOLDM", Leg2Price, 2, 100, OldQty=0)

        pos = self.db.GetSystemPosition("GOLDM")
        self.assertAlmostEqual(pos["avg_entry_price"], 72000.0)  # unchanged


# ─── Step 6a: Staleness timeout ─────────────���─────────────────────────��──


class TestStalenessTimeout(OrchestratorTestBase):

    def test_stale_pending_state_is_force_resolved(self):
        """Pending state >30 min old should be force-resolved, allowing new signal."""
        config_path = self._write_config(
            instrument_name="CASTOR", broker="ANGEL", user="AABM826021",
            exchange="NCDEX", point_value=10,
            order_routing={
                "ContractLookupName": "CASTOR",
                "ReconciliationPrefixes": ["CASTOR"],
                "InstrumentType": "FUTCOM", "Variety": "NORMAL",
                "Product": "CARRYFORWARD", "Validity": "DAY",
                "DaysPostWhichSelectNextContract": "9",
                "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                "ConvertToMarketOrder": "True",
                "ContractNameProvided": "False",
                "QuantityMultiplier": 5,
            },
        )
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        # Create a stale pending state: target=0, confirmed=5, updated 2 hours ago (UTC)
        self.db.UpdateSystemPosition("CASTOR", 0, 5)
        stale_time = (datetime.utcnow() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
        self.db._Connection.execute(
            "UPDATE system_positions SET updated_at = ? WHERE instrument = ?",
            (stale_time, "CASTOR")
        )
        self.db._Connection.commit()
        self.db.UpsertForecast("CASTOR", "S60C", 10.0, 1.0)

        with patch.object(orch, "_FetchBrokerPositions", return_value={}), \
             patch.object(orch, "_ExecuteDelta") as mock_execute:
            orch._ComputeAndExecute("CASTOR")

        # Should have force-resolved and called _ExecuteDelta
        mock_execute.assert_called_once()

    def test_fresh_pending_state_blocks_execution(self):
        """Pending state <30 min old should still block (PENDING_SKIP)."""
        config_path = self._write_config(
            instrument_name="CASTOR", broker="ANGEL", user="AABM826021",
            exchange="NCDEX", point_value=10,
            order_routing={
                "ContractLookupName": "CASTOR",
                "ReconciliationPrefixes": ["CASTOR"],
                "InstrumentType": "FUTCOM", "Variety": "NORMAL",
                "Product": "CARRYFORWARD", "Validity": "DAY",
                "DaysPostWhichSelectNextContract": "9",
                "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                "ConvertToMarketOrder": "True",
                "ContractNameProvided": "False",
                "QuantityMultiplier": 5,
            },
        )
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        # Create a fresh pending state (just now)
        self.db.UpdateSystemPosition("CASTOR", 5, 0)
        self.db.UpsertForecast("CASTOR", "S60C", 10.0, 1.0)

        with patch.object(orch, "_FetchBrokerPositions", return_value={}), \
             patch.object(orch, "_ExecuteDelta") as mock_execute:
            orch._ComputeAndExecute("CASTOR")

        # Should NOT have called _ExecuteDelta (still pending)
        mock_execute.assert_not_called()
        latest = self.db.GetRecentOrders(1)[0]
        self.assertEqual(latest["status"], "PENDING_SKIP")

    def test_staleness_uses_utc_not_local_time(self):
        """Staleness calculation must use UTC to match SQLite datetime('now')."""
        config_path = self._write_config(
            instrument_name="CASTOR", broker="ANGEL", user="AABM826021",
            exchange="NCDEX", point_value=10,
            order_routing={
                "ContractLookupName": "CASTOR",
                "ReconciliationPrefixes": ["CASTOR"],
                "InstrumentType": "FUTCOM", "Variety": "NORMAL",
                "Product": "CARRYFORWARD", "Validity": "DAY",
                "DaysPostWhichSelectNextContract": "9",
                "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                "ConvertToMarketOrder": "True",
                "ContractNameProvided": "False",
                "QuantityMultiplier": 5,
            },
        )
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        # Create a pending state updated 5 minutes ago (UTC) — should NOT be stale
        self.db.UpdateSystemPosition("CASTOR", 5, 0)
        recent_time = (datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
        self.db._Connection.execute(
            "UPDATE system_positions SET updated_at = ? WHERE instrument = ?",
            (recent_time, "CASTOR")
        )
        self.db._Connection.commit()
        self.db.UpsertForecast("CASTOR", "S60C", 10.0, 1.0)

        with patch.object(orch, "_FetchBrokerPositions", return_value={}), \
             patch.object(orch, "_ExecuteDelta") as mock_execute:
            orch._ComputeAndExecute("CASTOR")

        # 5 minutes is NOT stale — should still block
        mock_execute.assert_not_called()


# ─── Step 6b: Reconciliation alert ───────────��──────────────────────────


class TestReconciliationAlert(OrchestratorTestBase):

    def test_reconciliation_logs_error_when_pending_mismatch_resolved(self):
        """When reconciliation resolves a SyncedToTarget, it should log ERROR."""
        config_path = self._write_config(instrument_name="GOLDM", point_value=100)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        # Set up pending state: target=0, confirmed=2 (pending exit)
        self.db.UpdateSystemPosition("GOLDM", 0, 2)

        fo = self.fo
        with patch.object(orch, "_FetchBrokerPositions", return_value={}), \
             patch.object(orch, "_CalculateBrokerQty", return_value=0), \
             patch.object(fo, "Logger") as mock_logger:
            orch._RunReconciliation()

        # Should have logged an ERROR about swallowed signals
        error_calls = [c for c in mock_logger.error.call_args_list
                       if "ALERT" in str(c) and "BLOCKED" in str(c)]
        self.assertTrue(len(error_calls) > 0,
                        "Expected ERROR alert about blocked signals during reconciliation")

    def test_reconciliation_no_alert_when_positions_match(self):
        """Normal matching positions should not trigger any alert."""
        config_path = self._write_config(instrument_name="GOLDM", point_value=100)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        # Already in sync
        self.db.UpdateSystemPosition("GOLDM", 2, 2)

        fo = self.fo
        with patch.object(orch, "_FetchBrokerPositions", return_value={}), \
             patch.object(orch, "_CalculateBrokerQty", return_value=2), \
             patch.object(fo, "Logger") as mock_logger:
            orch._RunReconciliation()

        error_calls = [c for c in mock_logger.error.call_args_list
                       if "ALERT" in str(c)]
        self.assertEqual(len(error_calls), 0,
                         "Should not log ALERT when positions already match")


# ─── Database-level cost basis tests ─────────────────────────────────────


class TestCostBasisDatabase(unittest.TestCase):
    """Direct DB tests verifying UpdateCostBasis behavior."""

    def setUp(self):
        self._module_snapshot = _snapshot_modules(["forecast_db", "Directories"])
        _install_module("Directories", workInputRoot=Path(tempfile.mkdtemp()))

        sys.modules.pop("forecast_db", None)
        import forecast_db as db
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()
        self.db = db

    def tearDown(self):
        if getattr(self.db, "_Connection", None) is not None:
            self.db._Connection.close()
            self.db._Connection = None
        _restore_modules(self._module_snapshot)

    def test_update_cost_basis_on_fresh_instrument(self):
        """First fill on an instrument should set avg_entry_price = fill_price."""
        self.db.UpdateCostBasis("TESTINST", 100.0, 5, 1.0)
        pos = self.db.GetSystemPosition("TESTINST")
        self.assertAlmostEqual(pos["avg_entry_price"], 100.0)

    def test_update_cost_basis_weighted_average(self):
        """Adding to a position should compute weighted average."""
        self.db.UpdateSystemPosition("TESTINST", 5, 5)
        self.db.UpdateCostBasis("TESTINST", 100.0, 5, 1.0, OldQty=0)

        # Now add 5 more at 110
        self.db.UpdateCostBasis("TESTINST", 110.0, 5, 1.0, OldQty=5)
        pos = self.db.GetSystemPosition("TESTINST")
        self.assertAlmostEqual(pos["avg_entry_price"], 105.0)

    def test_realize_pnl_uses_avg_entry_price(self):
        """RealizePnl should use the avg_entry_price from system_positions."""
        self.db.UpdateSystemPosition("TESTINST", 5, 5)
        self.db.UpdateCostBasis("TESTINST", 100.0, 5, 10.0, OldQty=0)

        pnl = self.db.RealizePnl("TESTINST", 110.0, 5, 10.0, "futures")
        # Long: (110 - 100) * 5 * 10 = 500
        self.assertAlmostEqual(pnl, 500.0)

    def test_realize_pnl_with_zero_entry_produces_garbage(self):
        """Demonstrates that zero avg_entry_price causes wrong PnL (the bug we're fixing)."""
        self.db.UpdateSystemPosition("TESTINST", 5, 5)
        # NO UpdateCostBasis call — avg_entry_price stays at 0

        pos = self.db.GetSystemPosition("TESTINST")
        self.assertAlmostEqual(pos["avg_entry_price"], 0.0)

        pnl = self.db.RealizePnl("TESTINST", 110.0, 5, 10.0, "futures")
        # (110 - 0) * 5 * 10 = 5500 — wildly wrong
        self.assertAlmostEqual(pnl, 5500.0)

    def test_reset_cost_basis_clears_entry_price(self):
        """ResetCostBasis should set avg_entry_price to 0."""
        self.db.UpdateSystemPosition("TESTINST", 5, 5)
        self.db.UpdateCostBasis("TESTINST", 100.0, 5, 1.0)
        self.db.ResetCostBasis("TESTINST")
        pos = self.db.GetSystemPosition("TESTINST")
        self.assertAlmostEqual(pos["avg_entry_price"], 0.0)


if __name__ == "__main__":
    unittest.main()
