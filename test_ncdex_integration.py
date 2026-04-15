import importlib
import json
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd


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


class ForecastOrchestratorNcDexTests(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmpdir.name)
        self.db_path = str(tmp_path / "forecast_store.db")
        self.email_config = tmp_path / "email_config.json"
        self.email_config.write_text(
            json.dumps({"sender": "test@example.com", "recipient": "ops@example.com", "app_password": "x"}),
            encoding="utf-8",
        )
        self._module_snapshot = _snapshot_modules(
            [
                "Directories",
                "Kite_Server_Order_Handler",
                "Server_Order_Handler",
                "smart_chase",
                "kiteconnect",
                "forecast_db",
                "forecast_orchestrator",
            ]
        )

        _install_module(
            "Directories",
            workInputRoot=tmp_path,
        )
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
            EstablishConnectionAngelAPI=MagicMock(),
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

    def _write_config(self, instrument_name="TURMERIC", order_routing=None):
        cfg = {
            "account": {
                "dry_run": False,
                "base_capital": 1_000_000,
                "annual_vol_target_pct": 20,
            },
            "instruments": {
                instrument_name: {
                    "enabled": True,
                    "exchange": "NCDEX",
                    "broker": "ANGEL",
                    "user": "AABM826021",
                    "point_value": 10,
                    "daily_vol_target": 50,
                    "FDM": 1.0,
                    "forecast_cap": 20,
                    "position_inertia_pct": 0.10,
                    "subsystems": {"S60C": 1.0},
                    "system_name_map": {"AUTO2_TMC_S60C": "S60C"},
                    "order_routing": {
                        "ContractLookupName": "TMCFGRNZM",
                        "ReconciliationPrefixes": ["TMCFGRNZM"],
                        "InstrumentType": "FUTCOM",
                        "Variety": "NORMAL",
                        "Product": "CARRYFORWARD",
                        "Validity": "DAY",
                        "DaysPostWhichSelectNextContract": "9",
                        "EntrySleepDuration": "60",
                        "ExitSleepDuration": "45",
                        "ConvertToMarketOrder": "True",
                        "ContractNameProvided": "False",
                        "QuantityMultiplier": 5,
                    },
                }
            },
        }
        if order_routing:
            cfg["instruments"][instrument_name]["order_routing"].update(order_routing)

        config_path = Path(self.tmpdir.name) / "instrument_config.json"
        config_path.write_text(json.dumps(cfg), encoding="utf-8")
        return config_path

    def test_build_order_dict_uses_contract_lookup_name(self):
        config_path = self._write_config()
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        order_dict = orch._BuildOrderDict("TURMERIC", Delta=1, Target=1, Current=0)

        self.assertEqual(order_dict["Tradingsymbol"], "TMCFGRNZM")
        self.assertEqual(order_dict["UiQuantityLots"], 1)

    def test_reconciliation_syncs_confirmed_qty_when_broker_reaches_pending_target(self):
        config_path = self._write_config()
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        orch._SendReconAlert = MagicMock()
        self.db.UpdateSystemPosition("TURMERIC", 5, 0)

        # Broker returns 25 raw units = 5 lots (QuantityMultiplier=5)
        with patch.object(
            orch,
            "_FetchBrokerPositions",
            return_value={"TMCFGRNZM20APR2026": 25},
        ):
            orch._RunReconciliation()

        pos = self.db.GetSystemPosition("TURMERIC")
        self.assertEqual(pos["target_qty"], 5)
        self.assertEqual(pos["confirmed_qty"], 5)
        orch._SendReconAlert.assert_not_called()

    def test_compute_skips_duplicate_when_same_target_is_already_pending(self):
        config_path = self._write_config(instrument_name="CASTOR", order_routing={
            "ContractLookupName": "CASTOR",
            "ReconciliationPrefixes": ["CASTOR"],
        })
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        self.db.UpsertForecast("CASTOR", "S60C", 10.0, 1.0)
        self.db.UpdateSystemPosition("CASTOR", 5, 0)

        with patch.object(orch, "_FetchBrokerPositions", return_value={}), \
             patch.object(orch, "_ExecuteDelta") as execute_delta:
            orch._ComputeAndExecute("CASTOR")

        execute_delta.assert_not_called()
        latest = self.db.GetRecentOrders(1)[0]
        self.assertEqual(latest["status"], "PENDING_SKIP")

    def test_limit_browser_submission_stays_pending_until_broker_sync(self):
        config_path = self._write_config(instrument_name="CASTOR", order_routing={
            "ContractLookupName": "CASTOR",
            "ReconciliationPrefixes": ["CASTOR"],
        })
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        self.db.UpdateSystemPosition("CASTOR", 0, 0)

        def fake_legacy(order_dict, _broker):
            order_dict["ExecutionRoute"] = "ANGEL_WEB"
            order_dict["OrderId"] = "ANGEL_WEB_SUBMITTED"
            return {"status": "submitted", "placements": [{"status": "submitted"}]}

        with patch.object(orch, "_PrimeAngelLegacyLimitPrice") as price_prime, \
             patch.object(orch, "_ExecuteLegacy", side_effect=fake_legacy):
            orch._ExecuteDelta("CASTOR", Delta=5, Target=5)

        pos = self.db.GetSystemPosition("CASTOR")
        self.assertEqual(pos["target_qty"], 5)
        self.assertEqual(pos["confirmed_qty"], 0)
        latest = self.db.GetRecentOrders(1)[0]
        self.assertEqual(latest["status"], "SUBMITTED_PENDING")
        price_prime.assert_called_once()

    def test_execute_delta_uses_webhook_ltp_for_remote_angel_limit_order(self):
        config_path = self._write_config(instrument_name="CASTOR", order_routing={
            "ContractLookupName": "CASTOR",
            "ReconciliationPrefixes": ["CASTOR"],
        })
        self.db.LogTVSignal("CASTOR", "S30A", 1, 100.0, Ltp=315.95)
        response_payload = {
            "request_id": "remote-123",
            "flow": "angel_internal",
            "status": "success",
            "execution_route": "ANGEL_WEB",
            "order_id": "ANGEL_WEB_SUBMITTED",
            "warning": "Remote worker submitted the initial limit order.",
            "result": {"status": "submitted", "placements": [{"status": "submitted"}]},
        }
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = response_payload

        with patch.dict(os.environ, {
            "ANGEL_REMOTE_EXECUTION_URL": "http://angel-worker/internal/angel-execute",
            "ANGEL_EXECUTOR_TOKEN": "shared-secret",
            "ANGEL_REMOTE_EXECUTION_TIMEOUT_SECONDS": "15",
        }, clear=False):
            orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        with patch.object(self.fo.requests, "post", return_value=response) as post_mock:
            orch._ExecuteDelta("CASTOR", Delta=5, Target=5)

        pos = self.db.GetSystemPosition("CASTOR")
        self.assertEqual(pos["target_qty"], 5)
        self.assertEqual(pos["confirmed_qty"], 0)
        latest = self.db.GetRecentOrders(1)[0]
        self.assertEqual(latest["status"], "SUBMITTED_PENDING")

        post_mock.assert_called_once()
        self.assertEqual(post_mock.call_args.kwargs["timeout"], 15.0)
        self.assertEqual(
            post_mock.call_args.kwargs["headers"]["Authorization"],
            "Bearer shared-secret",
        )
        forwarded_order = post_mock.call_args.kwargs["json"]["order"]
        self.assertEqual(forwarded_order["Exchange"], "NCDEX")
        self.assertEqual(forwarded_order["Tradingsymbol"], "CASTOR")
        self.assertEqual(forwarded_order["Quantity"], "5*5")
        self.assertEqual(forwarded_order["Price"], "315.95")

    def test_execute_delta_fetches_angel_ltp_on_unix_when_webhook_ltp_missing(self):
        config_path = self._write_config(instrument_name="CASTOR", order_routing={
            "ContractLookupName": "CASTOR",
            "ReconciliationPrefixes": ["CASTOR"],
        })
        self.db.LogTVSignal("CASTOR", "S30A", 1, 100.0)

        response_payload = {
            "request_id": "remote-456",
            "flow": "angel_internal",
            "status": "success",
            "execution_route": "ANGEL_WEB",
            "order_id": "ANGEL_WEB_SUBMITTED",
            "result": {"status": "submitted", "placements": [{"status": "submitted"}]},
        }
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = response_payload

        with patch.dict(os.environ, {
            "ANGEL_REMOTE_EXECUTION_URL": "http://angel-worker/internal/angel-execute",
            "ANGEL_EXECUTOR_TOKEN": "shared-secret",
            "ANGEL_REMOTE_EXECUTION_TIMEOUT_SECONDS": "15",
        }, clear=False):
            orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        session = MagicMock()

        def validate_qty(order_dict):
            order_dict["Quantity"] = 25
            order_dict["Netposition"] = 25

        def resolve_contract(_session, order_dict):
            order_dict["Tradingsymbol"] = "CASTOR20APR2026"
            order_dict["Symboltoken"] = "12345"

        def prepare_order(_session, order_dict):
            order_dict["Price"] = 318.4
            return order_dict

        with patch.object(self.fo.AngelHandler, "EstablishConnectionAngelAPI", return_value=session) as connect_mock, \
             patch.object(self.fo.AngelHandler, "ConfigureNetDirectionOfTrade") as direction_mock, \
             patch.object(self.fo.AngelHandler, "Validate_Quantity", side_effect=validate_qty) as validate_mock, \
             patch.object(self.fo.AngelHandler, "PrepareInstrumentContractName", side_effect=resolve_contract) as resolve_mock, \
             patch.object(self.fo.AngelHandler, "PrepareOrderAngel", side_effect=prepare_order) as prepare_mock, \
             patch.object(self.fo.requests, "post", return_value=response) as post_mock:
            orch._ExecuteDelta("CASTOR", Delta=5, Target=5)

        connect_mock.assert_called_once()
        direction_mock.assert_called_once()
        validate_mock.assert_called_once()
        resolve_mock.assert_called_once()
        prepare_mock.assert_called_once()
        forwarded_order = post_mock.call_args.kwargs["json"]["order"]
        self.assertEqual(forwarded_order["Tradingsymbol"], "CASTOR20APR2026")
        self.assertEqual(forwarded_order["Symboltoken"], "12345")
        self.assertEqual(forwarded_order["Quantity"], 25)
        self.assertEqual(forwarded_order["Netposition"], 25)
        self.assertEqual(forwarded_order["ContractNameProvided"], "True")
        self.assertEqual(forwarded_order["Price"], "318.4")


class RolloverMonitorNcDexTests(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmpdir.name)
        angel_csv = tmp_path / "angel.csv"
        angel_csv.write_text(
            "token,symbol,name,expiry,instrumenttype,exch_seg\n"
            "1,TMCFGRNZM20APR2026,TMCFGRNZM,20APR2026,FUTCOM,NCDEX\n",
            encoding="utf-8",
        )
        self._module_snapshot = _snapshot_modules(
            [
                "Directories",
                "Holidays",
                "smart_chase",
                "kiteconnect",
                "forecast_db",
                "rollover_monitor",
            ]
        )

        _install_module(
            "Directories",
            workInputRoot=tmp_path,
            ZerodhaInstrumentDirectory=str(tmp_path / "zerodha.csv"),
            AngelInstrumentDirectory=str(angel_csv),
            KiteEkanshLogin="unused",
            KiteEkanshLoginAccessToken="unused",
            KiteRashmiLogin="unused",
            KiteRashmiLoginAccessToken="unused",
            KiteEshitaLogin="unused",
            KiteEshitaLoginAccessToken="unused",
            AngelEkanshLoginCred="unused",
            AngelNararushLoginCred="unused",
            AngelEshitaLoginCred="unused",
        )
        _install_module("Holidays", CheckForDateHoliday=lambda d, exchange=None: False,
                        MCX_FULL_HOLIDAYS=set(), COMMODITY_EXCHANGES={'MCX', 'NCDEX'})
        _install_module(
            "smart_chase",
            SmartChaseExecute=MagicMock(),
            EXCHANGE_OPEN_TIMES={},
        )
        _install_module("kiteconnect", KiteConnect=MagicMock())

        sys.modules.pop("forecast_db", None)
        sys.modules.pop("rollover_monitor", None)
        import forecast_db as db
        db.DB_PATH = str(tmp_path / "forecast_store.db")
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()

        import rollover_monitor as rm
        self.rm = importlib.reload(rm)
        self.db = db
        _restore_modules(self._module_snapshot)

    def tearDown(self):
        if getattr(self.db, "_Connection", None) is not None:
            self.db._Connection.close()
            self.db._Connection = None
        self.tmpdir.cleanup()

    def test_match_position_uses_contract_lookup_name(self):
        instrument_config = {
            "TURMERIC": {
                "enabled": True,
                "exchange": "NCDEX",
                "broker": "ANGEL",
                "user": "AABM826021",
                "order_routing": {
                    "ContractLookupName": "TMCFGRNZM",
                    "ReconciliationPrefixes": ["TMCFGRNZM"],
                    "InstrumentType": "FUTCOM",
                },
                "rollover": {"enabled": True},
            }
        }
        position = {
            "tradingsymbol": "TMCFGRNZM20APR2026",
            "exchange": "NCDEX",
            "broker": "ANGEL",
        }

        inst_name, cfg = self.rm.MatchPositionToInstrument(position, instrument_config)

        self.assertEqual(inst_name, "TURMERIC")
        self.assertEqual(cfg["user"], "AABM826021")

    def test_ncdex_angel_rollover_stays_alert_only(self):
        inst_cfg = {
            "exchange": "NCDEX",
            "broker": "ANGEL",
            "rollover": {
                "enabled": True,
                "alert_days_before_expiry": 5,
                "execute_days_before_expiry": 3,
            },
        }
        expiry_info = {"current_expiry": datetime.now() + timedelta(days=1)}
        position = {"broker": "ANGEL"}

        with patch.object(self.rm.db, "GetPendingRollovers", return_value=[]):
            decision = self.rm.EvaluateRolloverNeed("TURMERIC", inst_cfg, expiry_info, position)

        self.assertEqual(decision, "ALERT_ONLY")


if __name__ == "__main__":
    unittest.main()
