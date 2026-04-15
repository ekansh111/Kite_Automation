"""
Tests for:
1. Daily rollover summary email sent every day with ALL positions
2. NCDEX broker quantity divided by QuantityMultiplier in reconciliation
"""

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

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


# ─── NCDEX Lot Fix Tests (forecast_orchestrator) ───────────────────


class BrokerQtyNcdexLotConversionTests(unittest.TestCase):
    """Test that _CalculateBrokerQty divides by QuantityMultiplier for NCDEX."""

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

    def _write_config(self, instruments):
        cfg = {
            "account": {
                "dry_run": False,
                "base_capital": 1_000_000,
                "annual_vol_target_pct": 20,
            },
            "instruments": instruments,
        }
        config_path = Path(self.tmpdir.name) / "instrument_config.json"
        config_path.write_text(json.dumps(cfg), encoding="utf-8")
        return config_path

    def _make_ncdex_instrument(self, name, lookup_name, multiplier):
        return {
            "enabled": True,
            "exchange": "NCDEX",
            "broker": "ANGEL",
            "user": "AABM826021",
            "point_value": multiplier,
            "daily_vol_target": 50,
            "FDM": 1.0,
            "forecast_cap": 20,
            "position_inertia_pct": 0.10,
            "subsystems": {"S60C": 1.0},
            "system_name_map": {f"AUTO2_{name}_S60C": "S60C"},
            "order_routing": {
                "ContractLookupName": lookup_name,
                "ReconciliationPrefixes": [lookup_name],
                "InstrumentType": "FUTCOM",
                "Variety": "NORMAL",
                "Product": "CARRYFORWARD",
                "Validity": "DAY",
                "QuantityMultiplier": multiplier,
            },
        }

    def _make_mcx_instrument(self, name, lookup_name):
        return {
            "enabled": True,
            "exchange": "MCX",
            "broker": "ZERODHA",
            "user": "OFS653",
            "point_value": 1,
            "daily_vol_target": 50,
            "FDM": 1.0,
            "forecast_cap": 20,
            "position_inertia_pct": 0.10,
            "subsystems": {"S60C": 1.0},
            "system_name_map": {f"AUTO2_{name}_S60C": "S60C"},
            "order_routing": {
                "ContractLookupName": lookup_name,
                "ReconciliationPrefixes": [lookup_name],
                "InstrumentType": "FUT",
                "QuantityMultiplier": 1,
            },
        }

    # ── Core lot conversion tests ──

    def test_ncdex_broker_qty_divided_by_multiplier_5(self):
        """TURMERIC with multiplier=5: broker returns 25 raw units -> 5 lots."""
        instruments = {"TURMERIC": self._make_ncdex_instrument("TURMERIC", "TMCFGRNZM", 5)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        broker_positions = {"TMCFGRNZM20APR2026": 25}
        result = orch._CalculateBrokerQty("TURMERIC", broker_positions)

        self.assertEqual(result, 5)

    def test_ncdex_broker_qty_divided_by_multiplier_10(self):
        """COCUDAKL with multiplier=10: broker returns 10 raw units -> 1 lot."""
        instruments = {"COCUDAKL": self._make_ncdex_instrument("COCUDAKL", "COCUDAKL", 10)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        broker_positions = {"COCUDAKL20APR2026": 10}
        result = orch._CalculateBrokerQty("COCUDAKL", broker_positions)

        self.assertEqual(result, 1)

    def test_ncdex_broker_qty_divided_by_multiplier_50(self):
        """Large multiplier (50): broker returns 150 raw -> 3 lots."""
        instruments = {"COTTONGUCCI": self._make_ncdex_instrument("COTTONGUCCI", "COTTONGUCCI", 50)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        broker_positions = {"COTTONGUCCI20APR2026": 150}
        result = orch._CalculateBrokerQty("COTTONGUCCI", broker_positions)

        self.assertEqual(result, 3)

    def test_mcx_instrument_multiplier_1_unchanged(self):
        """MCX with multiplier=1: broker returns 5 -> stays 5 (no division)."""
        instruments = {"GOLDM": self._make_mcx_instrument("GOLDM", "GOLDM")}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        broker_positions = {"GOLDM26APR2026": 5}
        result = orch._CalculateBrokerQty("GOLDM", broker_positions)

        self.assertEqual(result, 5)

    def test_zero_broker_qty_stays_zero(self):
        """Zero quantity should remain zero after division."""
        instruments = {"TURMERIC": self._make_ncdex_instrument("TURMERIC", "TMCFGRNZM", 5)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        broker_positions = {}
        result = orch._CalculateBrokerQty("TURMERIC", broker_positions)

        self.assertEqual(result, 0)

    def test_negative_broker_qty_divided_correctly(self):
        """Short position: broker returns -25 raw -> -5 lots."""
        instruments = {"TURMERIC": self._make_ncdex_instrument("TURMERIC", "TMCFGRNZM", 5)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        broker_positions = {"TMCFGRNZM20APR2026": -25}
        result = orch._CalculateBrokerQty("TURMERIC", broker_positions)

        self.assertEqual(result, -5)

    def test_non_exact_multiple_truncates(self):
        """If broker qty isn't exact multiple, integer division truncates (signals real mismatch)."""
        instruments = {"TURMERIC": self._make_ncdex_instrument("TURMERIC", "TMCFGRNZM", 5)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        broker_positions = {"TMCFGRNZM20APR2026": 12}  # 12 / 5 = 2 (truncated)
        result = orch._CalculateBrokerQty("TURMERIC", broker_positions)

        self.assertEqual(result, 2)

    def test_no_matching_broker_position_returns_zero(self):
        """When no broker position matches the instrument prefix, result is 0."""
        instruments = {"TURMERIC": self._make_ncdex_instrument("TURMERIC", "TMCFGRNZM", 5)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        # Broker has positions but none match TURMERIC's prefix
        broker_positions = {"GOLDM26APR2026": 10, "SILVERM26APR2026": 5}
        result = orch._CalculateBrokerQty("TURMERIC", broker_positions)

        self.assertEqual(result, 0)

    # ── Reconciliation integration tests ──

    def test_reconciliation_ncdex_no_false_mismatch(self):
        """Reconciliation should NOT flag mismatch when system=1 lot and broker=10 raw (multiplier=10)."""
        instruments = {"COCUDAKL": self._make_ncdex_instrument("COCUDAKL", "COCUDAKL", 10)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        orch._SendReconAlert = MagicMock()
        self.db.UpdateSystemPosition("COCUDAKL", 1, 1)  # system has 1 lot

        with patch.object(
            orch, "_FetchBrokerPositions",
            return_value={"COCUDAKL20APR2026": 10},  # broker returns 10 raw units
        ):
            orch._RunReconciliation()

        pos = self.db.GetSystemPosition("COCUDAKL")
        self.assertEqual(pos["confirmed_qty"], 1)
        orch._SendReconAlert.assert_not_called()

    def test_reconciliation_ncdex_detects_real_mismatch(self):
        """Reconciliation SHOULD flag mismatch when system=2 lots but broker=10 raw (=1 lot, multiplier=10)."""
        instruments = {"COCUDAKL": self._make_ncdex_instrument("COCUDAKL", "COCUDAKL", 10)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        orch._SendReconAlert = MagicMock()
        self.db.UpdateSystemPosition("COCUDAKL", 2, 2)  # system has 2 lots

        with patch.object(
            orch, "_FetchBrokerPositions",
            return_value={"COCUDAKL20APR2026": 10},  # broker = 1 lot
        ):
            orch._RunReconciliation()

        pos = self.db.GetSystemPosition("COCUDAKL")
        self.assertEqual(pos["confirmed_qty"], 1)  # synced to broker
        orch._SendReconAlert.assert_called_once()

    def test_reconciliation_mcx_unaffected(self):
        """MCX instruments (multiplier=1) should work exactly as before."""
        instruments = {"GOLDM": self._make_mcx_instrument("GOLDM", "GOLDM")}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        orch._SendReconAlert = MagicMock()
        self.db.UpdateSystemPosition("GOLDM", 3, 3)

        with patch.object(
            orch, "_FetchBrokerPositions",
            return_value={"GOLDM26APR2026": 3},
        ):
            orch._RunReconciliation()

        pos = self.db.GetSystemPosition("GOLDM")
        self.assertEqual(pos["confirmed_qty"], 3)
        orch._SendReconAlert.assert_not_called()

    def test_reconciliation_ncdex_syncs_to_target_via_lots(self):
        """Broker reaches pending target (in lots) — confirmed_qty synced correctly."""
        instruments = {"TURMERIC": self._make_ncdex_instrument("TURMERIC", "TMCFGRNZM", 5)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        orch._SendReconAlert = MagicMock()
        self.db.UpdateSystemPosition("TURMERIC", 5, 0)  # target=5, confirmed=0

        with patch.object(
            orch, "_FetchBrokerPositions",
            return_value={"TMCFGRNZM20APR2026": 25},  # 25 raw = 5 lots = target
        ):
            orch._RunReconciliation()

        pos = self.db.GetSystemPosition("TURMERIC")
        self.assertEqual(pos["target_qty"], 5)
        self.assertEqual(pos["confirmed_qty"], 5)
        orch._SendReconAlert.assert_not_called()

    def test_sync_instrument_with_broker_uses_lot_conversion(self):
        """_SyncInstrumentWithBroker also goes through _CalculateBrokerQty."""
        instruments = {"COCUDAKL": self._make_ncdex_instrument("COCUDAKL", "COCUDAKL", 10)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))
        self.db.UpdateSystemPosition("COCUDAKL", 2, 1)  # target=2, confirmed=1

        with patch.object(
            orch, "_FetchBrokerPositions",
            return_value={"COCUDAKL20APR2026": 20},  # 20 raw = 2 lots
        ):
            result = orch._SyncInstrumentWithBroker("COCUDAKL", instruments["COCUDAKL"])

        self.assertTrue(result["changed"])
        self.assertEqual(result["broker_qty"], 2)
        pos = self.db.GetSystemPosition("COCUDAKL")
        self.assertEqual(pos["confirmed_qty"], 2)

    def test_multiple_ncdex_positions_aggregated_then_divided(self):
        """Multiple broker positions for same prefix sum then divide."""
        instruments = {"TURMERIC": self._make_ncdex_instrument("TURMERIC", "TMCFGRNZM", 5)}
        config_path = self._write_config(instruments)
        orch = self.fo.ForecastOrchestrator(ConfigPath=str(config_path))

        # Two contracts for same instrument (shouldn't normally happen, but edge case)
        broker_positions = {
            "TMCFGRNZM20APR2026": 10,
            "TMCFGRNZM20MAY2026": 15,
        }
        result = orch._CalculateBrokerQty("TURMERIC", broker_positions)

        self.assertEqual(result, 5)  # (10 + 15) // 5 = 5


# ─── Daily Rollover Email Tests (rollover_monitor) ─────────────────


class DailyRolloverEmailTests(unittest.TestCase):
    """Test that daily summary email is always sent with all positions."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmpdir.name)
        angel_csv = tmp_path / "angel.csv"
        angel_csv.write_text(
            "token,symbol,name,expiry,instrumenttype,exch_seg\n"
            "1,TMCFGRNZM20APR2026,TMCFGRNZM,20APR2026,FUTCOM,NCDEX\n"
            "2,TMCFGRNZM20MAY2026,TMCFGRNZM,20MAY2026,FUTCOM,NCDEX\n"
            "3,COCUDAKL20APR2026,COCUDAKL,20APR2026,FUTCOM,NCDEX\n",
            encoding="utf-8",
        )
        self.email_config = tmp_path / "email_config.json"
        self.email_config.write_text(
            json.dumps({"sender": "test@example.com", "recipient": "ops@example.com", "app_password": "x"}),
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
                        MCX_FULL_HOLIDAYS=set(), MCX_EXCHANGES={'MCX'})
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

    # ── SendDailySummaryEmail tests ──

    def test_summary_email_includes_all_positions(self):
        """All positions should appear in the email, not just those near expiry."""
        upcoming = [
            {"instrument": "GOLDM", "expiry": "2026-04-25", "days_left": 15, "alert_days": 4},
            {"instrument": "TURMERIC", "expiry": "2026-04-20", "days_left": 3, "alert_days": 5},
            {"instrument": "SILVERM", "expiry": "2026-05-28", "days_left": 38, "alert_days": 4},
        ]

        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        mock_send.assert_called_once()
        subject, html_body = mock_send.call_args[0][0], mock_send.call_args[0][1]
        # All three instruments should be present
        self.assertIn("GOLDM", html_body)
        self.assertIn("TURMERIC", html_body)
        self.assertIn("SILVERM", html_body)
        # Should have the new card title
        self.assertIn("All Position Expiry Status", html_body)
        self.assertNotIn("next 7 days", html_body)

    def test_summary_email_sorted_by_days_left(self):
        """Positions should be sorted by days_left ascending (most urgent first)."""
        upcoming = [
            {"instrument": "SILVERM", "expiry": "2026-05-28", "days_left": 38, "alert_days": 4},
            {"instrument": "TURMERIC", "expiry": "2026-04-20", "days_left": 3, "alert_days": 5},
            {"instrument": "GOLDM", "expiry": "2026-04-25", "days_left": 15, "alert_days": 4},
        ]

        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        # TURMERIC (3 days) should appear before GOLDM (15 days) before SILVERM (38 days)
        turmeric_pos = html_body.index("TURMERIC")
        goldm_pos = html_body.index("GOLDM")
        silverm_pos = html_body.index("SILVERM")
        self.assertLess(turmeric_pos, goldm_pos)
        self.assertLess(goldm_pos, silverm_pos)

    def test_summary_email_red_within_alert_window(self):
        """Positions within alert_days should use red (#ff1744)."""
        upcoming = [
            {"instrument": "TURMERIC", "expiry": "2026-04-20", "days_left": 3, "alert_days": 5},
        ]

        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        self.assertIn("#ff1744", html_body)

    def test_summary_email_amber_approaching_alert(self):
        """Positions within alert_days+3 but outside alert_days should use amber (#ff9100)."""
        upcoming = [
            {"instrument": "GOLDM", "expiry": "2026-04-25", "days_left": 6, "alert_days": 4},
        ]

        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        self.assertIn("#ff9100", html_body)

    def test_summary_email_no_color_far_from_expiry(self):
        """Positions far from expiry should have no special color."""
        upcoming = [
            {"instrument": "SILVERM", "expiry": "2026-05-28", "days_left": 38, "alert_days": 4},
        ]

        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        # Neither red nor amber should appear for the position row
        self.assertNotIn("#ff1744", html_body)
        self.assertNotIn("#ff9100", html_body)

    def test_summary_email_empty_positions_shows_fallback(self):
        """When no positions exist, should show 'No open positions found'."""
        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], [])

        html_body = mock_send.call_args[0][1]
        self.assertIn("No open positions found", html_body)

    def test_summary_email_with_results_and_positions(self):
        """Both rollovers executed and all positions should appear."""
        results = [
            {
                "instrument": "TURMERIC",
                "old_contract": "TMCFGRNZM20APR2026",
                "new_contract": "TMCFGRNZM20MAY2026",
                "status": "COMPLETE",
                "success": True,
            }
        ]
        upcoming = [
            {"instrument": "GOLDM", "expiry": "2026-05-25", "days_left": 35, "alert_days": 4},
        ]

        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail(results, upcoming)

        html_body = mock_send.call_args[0][1]
        self.assertIn("TMCFGRNZM20APR2026", html_body)
        self.assertIn("GOLDM", html_body)
        self.assertIn("Today&#x27;s Rollovers", html_body)
        self.assertIn("All Position Expiry Status", html_body)

    def test_summary_email_days_left_text(self):
        """Each position should show days-to-expiry text."""
        upcoming = [
            {"instrument": "GOLDM", "expiry": "2026-04-25", "days_left": 15, "alert_days": 4},
        ]

        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        self.assertIn("15 trading days left", html_body)

    def test_summary_email_subject_format(self):
        """Subject should include date and rollover count."""
        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], [])

        subject = mock_send.call_args[0][0]
        today = date.today().strftime('%Y-%m-%d')
        self.assertIn(today, subject)
        self.assertIn("0 rollover(s)", subject)

    def test_summary_email_alert_days_fallback(self):
        """Missing alert_days key defaults to 4 for color calculation."""
        upcoming = [
            {"instrument": "GOLDM", "expiry": "2026-04-20", "days_left": 3},  # no alert_days key
        ]

        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        # days_left=3 <= alert_days default 4, so red
        self.assertIn("#ff1744", html_body)

    # ── main() integration tests ──

    def _patch_main_logging(self):
        """Create logs dir so main() doesn't fail on FileHandler."""
        logs_dir = Path(self.rm.__file__).parent / "logs"
        logs_dir.mkdir(exist_ok=True)

    def test_main_sends_email_when_no_rollovers_needed(self):
        """Daily summary email should be sent even when all positions are far from expiry."""
        self._patch_main_logging()
        far_expiry = datetime.now() + timedelta(days=45)
        instrument_config = {
            "TURMERIC": {
                "enabled": True,
                "exchange": "NCDEX",
                "broker": "ANGEL",
                "user": "AABM826021",
                "order_routing": {
                    "ContractLookupName": "TMCFGRNZM",
                    "ReconciliationPrefixes": ["TMCFGRNZM"],
                    "QuantityMultiplier": 5,
                },
                "rollover": {
                    "enabled": True,
                    "alert_days_before_expiry": 5,
                    "execute_days_before_expiry": 3,
                },
            }
        }
        position = {
            "tradingsymbol": "TMCFGRNZM20APR2026",
            "exchange": "NCDEX",
            "quantity": 5,
            "last_price": 100.0,
            "symboltoken": "1",
            "product": "CARRYFORWARD",
            "broker": "ANGEL",
            "user": "AABM826021",
            "_session": MagicMock(),
        }
        expiry_info = {
            "current_expiry": far_expiry,
            "current_symbol": "TMCFGRNZM20MAY2026",
            "current_token": "2",
            "next_symbol": "TMCFGRNZM20JUN2026",
            "next_token": "3",
            "next_expiry": far_expiry + timedelta(days=30),
        }

        with patch.object(self.rm, "ScanAllPositions", return_value=[position]), \
             patch.object(self.rm, "MatchPositionToInstrument",
                         return_value=("TURMERIC", instrument_config["TURMERIC"])), \
             patch.object(self.rm, "ResolveExpiryInfo", return_value=expiry_info), \
             patch.object(self.rm.db, "GetIncompleteRollovers", return_value=[]), \
             patch.object(self.rm, "IsTradingDay", return_value=True), \
             patch.object(self.rm, "SendDailySummaryEmail") as mock_summary, \
             patch.object(self.rm, "SendAlertEmail") as mock_alert, \
             patch("argparse.ArgumentParser.parse_args",
                   return_value=MagicMock(dry_run=False, instrument=None, force=False, status=False)):
            self.rm.main()

        # Alert email should NOT be sent (too far from expiry)
        mock_alert.assert_not_called()
        # Daily summary SHOULD be sent
        mock_summary.assert_called_once()
        # Check that upcoming rollovers list contains our position
        args = mock_summary.call_args
        upcoming = args[0][1]
        self.assertEqual(len(upcoming), 1)
        self.assertEqual(upcoming[0]["instrument"], "TURMERIC")

    def test_main_upcoming_rollovers_includes_all_not_just_near_expiry(self):
        """Positions far from expiry (30+ days) should still be in UpcomingRollovers."""
        self._patch_main_logging()
        far_expiry = datetime.now() + timedelta(days=60)
        instrument_config = {
            "GOLDM": {
                "enabled": True,
                "exchange": "MCX",
                "broker": "ZERODHA",
                "user": "OFS653",
                "order_routing": {
                    "ContractLookupName": "GOLDM",
                    "ReconciliationPrefixes": ["GOLDM"],
                    "QuantityMultiplier": 1,
                },
                "rollover": {
                    "enabled": True,
                    "alert_days_before_expiry": 4,
                    "execute_days_before_expiry": 3,
                },
            }
        }
        position = {
            "tradingsymbol": "GOLDM26JUN2026",
            "exchange": "MCX",
            "quantity": 1,
            "last_price": 5000.0,
            "instrument_token": "12345",
            "product": "NRML",
            "broker": "ZERODHA",
            "user": "OFS653",
            "_session": MagicMock(),
        }
        expiry_info = {
            "current_expiry": far_expiry,
            "current_symbol": "GOLDM26JUN2026",
            "current_token": "12345",
            "next_symbol": "GOLDM26JUL2026",
            "next_token": "12346",
            "next_expiry": far_expiry + timedelta(days=30),
        }

        with patch.object(self.rm, "ScanAllPositions", return_value=[position]), \
             patch.object(self.rm, "MatchPositionToInstrument",
                         return_value=("GOLDM", instrument_config["GOLDM"])), \
             patch.object(self.rm, "ResolveExpiryInfo", return_value=expiry_info), \
             patch.object(self.rm.db, "GetIncompleteRollovers", return_value=[]), \
             patch.object(self.rm, "IsTradingDay", return_value=True), \
             patch.object(self.rm, "SendDailySummaryEmail") as mock_summary, \
             patch("argparse.ArgumentParser.parse_args",
                   return_value=MagicMock(dry_run=False, instrument=None, force=False, status=False)):
            self.rm.main()

        mock_summary.assert_called_once()
        upcoming = mock_summary.call_args[0][1]
        self.assertEqual(len(upcoming), 1)
        self.assertEqual(upcoming[0]["instrument"], "GOLDM")
        self.assertGreater(upcoming[0]["days_left"], 7)
        self.assertIn("alert_days", upcoming[0])

    def test_main_sends_summary_even_with_zero_positions(self):
        """When no positions match config, summary email is still sent (with empty lists)."""
        self._patch_main_logging()
        position = {
            "tradingsymbol": "UNKNOWNFUT",
            "exchange": "MCX",
            "quantity": 1,
            "last_price": 100.0,
            "instrument_token": "99999",
            "product": "NRML",
            "broker": "ZERODHA",
            "user": "OFS653",
            "_session": MagicMock(),
        }

        with patch.object(self.rm, "ScanAllPositions", return_value=[position]), \
             patch.object(self.rm, "MatchPositionToInstrument", return_value=(None, None)), \
             patch.object(self.rm.db, "GetIncompleteRollovers", return_value=[]), \
             patch.object(self.rm, "IsTradingDay", return_value=True), \
             patch.object(self.rm, "SendDailySummaryEmail") as mock_summary, \
             patch("argparse.ArgumentParser.parse_args",
                   return_value=MagicMock(dry_run=False, instrument=None, force=False, status=False)):
            self.rm.main()

        mock_summary.assert_called_once()
        results, upcoming = mock_summary.call_args[0]
        self.assertEqual(len(results), 0)
        self.assertEqual(len(upcoming), 0)

    def test_upcoming_includes_alert_days_field(self):
        """Each entry in UpcomingRollovers should have alert_days for color coding."""
        self._patch_main_logging()
        near_expiry = datetime.now() + timedelta(days=2)
        instrument_config = {
            "TURMERIC": {
                "enabled": True,
                "exchange": "NCDEX",
                "broker": "ANGEL",
                "user": "AABM826021",
                "order_routing": {
                    "ContractLookupName": "TMCFGRNZM",
                    "ReconciliationPrefixes": ["TMCFGRNZM"],
                    "QuantityMultiplier": 5,
                },
                "rollover": {
                    "enabled": True,
                    "alert_days_before_expiry": 7,
                    "execute_days_before_expiry": 3,
                },
            }
        }
        position = {
            "tradingsymbol": "TMCFGRNZM20APR2026",
            "exchange": "NCDEX",
            "quantity": 5,
            "last_price": 100.0,
            "symboltoken": "1",
            "product": "CARRYFORWARD",
            "broker": "ANGEL",
            "user": "AABM826021",
            "_session": MagicMock(),
        }
        expiry_info = {
            "current_expiry": near_expiry,
            "current_symbol": "TMCFGRNZM20APR2026",
            "current_token": "1",
            "next_symbol": "TMCFGRNZM20MAY2026",
            "next_token": "2",
            "next_expiry": near_expiry + timedelta(days=30),
        }

        with patch.object(self.rm, "ScanAllPositions", return_value=[position]), \
             patch.object(self.rm, "MatchPositionToInstrument",
                         return_value=("TURMERIC", instrument_config["TURMERIC"])), \
             patch.object(self.rm, "ResolveExpiryInfo", return_value=expiry_info), \
             patch.object(self.rm.db, "GetPendingRollovers", return_value=[]), \
             patch.object(self.rm.db, "GetIncompleteRollovers", return_value=[]), \
             patch.object(self.rm, "IsTradingDay", return_value=True), \
             patch.object(self.rm, "SendDailySummaryEmail") as mock_summary, \
             patch.object(self.rm, "SendAlertEmail"), \
             patch("argparse.ArgumentParser.parse_args",
                   return_value=MagicMock(dry_run=False, instrument=None, force=False, status=False)):
            self.rm.main()

        upcoming = mock_summary.call_args[0][1]
        self.assertEqual(len(upcoming), 1)
        self.assertEqual(upcoming[0]["alert_days"], 7)

    # ── Color tier edge case tests ──

    def test_color_exactly_at_alert_days_boundary(self):
        """days_left == alert_days should be red (within alert window)."""
        upcoming = [
            {"instrument": "X", "expiry": "2026-04-20", "days_left": 4, "alert_days": 4},
        ]
        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        self.assertIn("#ff1744", html_body)

    def test_color_exactly_at_alert_plus_3_boundary(self):
        """days_left == alert_days + 3 should be amber."""
        upcoming = [
            {"instrument": "X", "expiry": "2026-04-20", "days_left": 7, "alert_days": 4},
        ]
        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        self.assertIn("#ff9100", html_body)

    def test_color_one_past_alert_plus_3_is_no_color(self):
        """days_left == alert_days + 4 should have no special color."""
        upcoming = [
            {"instrument": "X", "expiry": "2026-04-20", "days_left": 8, "alert_days": 4},
        ]
        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        self.assertNotIn("#ff1744", html_body)
        self.assertNotIn("#ff9100", html_body)

    def test_mixed_colors_all_present(self):
        """Multiple positions with different urgencies should show correct colors."""
        upcoming = [
            {"instrument": "URGENT", "expiry": "2026-04-20", "days_left": 2, "alert_days": 4},
            {"instrument": "APPROACHING", "expiry": "2026-04-25", "days_left": 6, "alert_days": 4},
            {"instrument": "SAFE", "expiry": "2026-06-20", "days_left": 50, "alert_days": 4},
        ]
        with patch.object(self.rm, "_SendEmail") as mock_send:
            self.rm.SendDailySummaryEmail([], upcoming)

        html_body = mock_send.call_args[0][1]
        # Both red and amber should be present (for URGENT and APPROACHING)
        self.assertIn("#ff1744", html_body)
        self.assertIn("#ff9100", html_body)


    # ── Expiry fallback tests (MCX with stale CSV) ──

    def test_parse_expiry_from_position_iso_string(self):
        """Kite returns expiry as ISO date string."""
        pos = {"expiry": "2026-04-20"}
        result = self.rm._ParseExpiryFromPosition(pos)
        self.assertEqual(result, datetime(2026, 4, 20))

    def test_parse_expiry_from_position_datetime_object(self):
        """Kite may return expiry as datetime object."""
        dt = datetime(2026, 4, 20, 0, 0)
        pos = {"expiry": dt}
        result = self.rm._ParseExpiryFromPosition(pos)
        self.assertEqual(result, dt)

    def test_parse_expiry_from_position_date_object(self):
        """Date object converted to datetime."""
        d = date(2026, 4, 20)
        pos = {"expiry": d}
        result = self.rm._ParseExpiryFromPosition(pos)
        self.assertEqual(result, datetime(2026, 4, 20))

    def test_parse_expiry_from_position_angel_format(self):
        """Angel returns expiry like '20APR2026'."""
        pos = {"expiry": "20APR2026"}
        result = self.rm._ParseExpiryFromPosition(pos)
        self.assertEqual(result, datetime(2026, 4, 20))

    def test_parse_expiry_from_position_empty(self):
        """Missing expiry returns None."""
        pos = {"expiry": ""}
        result = self.rm._ParseExpiryFromPosition(pos)
        self.assertIsNone(result)

    def test_parse_expiry_from_position_no_key(self):
        """No expiry key returns None."""
        pos = {}
        result = self.rm._ParseExpiryFromPosition(pos)
        self.assertIsNone(result)

    def test_mcx_position_shown_when_csv_stale(self):
        """MCX position should appear in daily summary even when ResolveExpiryInfo fails."""
        self._patch_main_logging()
        mcx_expiry = datetime(2026, 4, 20)
        instrument_config = {
            "GOLDM": {
                "enabled": True,
                "exchange": "MCX",
                "broker": "ZERODHA",
                "user": "YD6016",
                "rollover": {
                    "enabled": True,
                    "alert_days_before_expiry": 4,
                    "execute_days_before_expiry": 3,
                },
            }
        }
        position = {
            "tradingsymbol": "GOLDM26APRFUT",
            "exchange": "MCX",
            "quantity": 1,
            "last_price": 5000.0,
            "instrument_token": "12345",
            "expiry": "2026-04-20",  # Broker provides expiry
            "product": "NRML",
            "broker": "ZERODHA",
            "user": "YD6016",
            "_session": MagicMock(),
        }

        with patch.object(self.rm, "ScanAllPositions", return_value=[position]), \
             patch.object(self.rm, "MatchPositionToInstrument",
                         return_value=("GOLDM", instrument_config["GOLDM"])), \
             patch.object(self.rm, "ResolveExpiryInfo", return_value=None), \
             patch.object(self.rm.db, "GetIncompleteRollovers", return_value=[]), \
             patch.object(self.rm, "IsTradingDay", return_value=True), \
             patch.object(self.rm, "SendDailySummaryEmail") as mock_summary, \
             patch("argparse.ArgumentParser.parse_args",
                   return_value=MagicMock(dry_run=False, instrument=None, force=False, status=False)):
            self.rm.main()

        mock_summary.assert_called_once()
        upcoming = mock_summary.call_args[0][1]
        # GOLDM should still appear despite ResolveExpiryInfo returning None
        self.assertEqual(len(upcoming), 1)
        self.assertEqual(upcoming[0]["instrument"], "GOLDM")
        self.assertEqual(upcoming[0]["expiry"], "2026-04-20")

    def test_mixed_positions_csv_ok_and_csv_stale(self):
        """Mix of positions: some with working CSV, some needing fallback."""
        self._patch_main_logging()
        ncdex_expiry = datetime.now() + timedelta(days=15)
        mcx_expiry = datetime(2026, 4, 20)

        ncdex_config = {
            "enabled": True, "exchange": "NCDEX", "broker": "ANGEL",
            "user": "AABM826021",
            "order_routing": {"ContractLookupName": "TMCFGRNZM", "QuantityMultiplier": 5},
            "rollover": {"enabled": True, "alert_days_before_expiry": 5},
        }
        mcx_config = {
            "enabled": True, "exchange": "MCX", "broker": "ZERODHA",
            "user": "YD6016",
            "rollover": {"enabled": True, "alert_days_before_expiry": 4},
        }

        positions = [
            {"tradingsymbol": "TMCFGRNZM20APR2026", "exchange": "NCDEX",
             "quantity": 5, "last_price": 100, "symboltoken": "1",
             "expiry": "", "product": "CARRYFORWARD", "broker": "ANGEL",
             "user": "AABM826021", "_session": MagicMock()},
            {"tradingsymbol": "GOLDM26APRFUT", "exchange": "MCX",
             "quantity": 1, "last_price": 5000, "instrument_token": "12345",
             "expiry": "2026-04-20", "product": "NRML", "broker": "ZERODHA",
             "user": "YD6016", "_session": MagicMock()},
        ]

        ncdex_expiry_info = {
            "current_expiry": ncdex_expiry,
            "current_symbol": "TMCFGRNZM20APR2026", "current_token": "1",
            "next_symbol": "TMCFGRNZM20MAY2026", "next_token": "2",
            "next_expiry": ncdex_expiry + timedelta(days=30),
        }

        def mock_match(pos, cfg):
            sym = pos["tradingsymbol"]
            if "TMCFGRNZM" in sym:
                return ("TURMERIC", ncdex_config)
            if "GOLDM" in sym:
                return ("GOLDM", mcx_config)
            return (None, None)

        def mock_resolve(name, cfg, pos):
            if name == "TURMERIC":
                return ncdex_expiry_info
            return None  # GOLDM CSV stale

        with patch.object(self.rm, "ScanAllPositions", return_value=positions), \
             patch.object(self.rm, "MatchPositionToInstrument", side_effect=mock_match), \
             patch.object(self.rm, "ResolveExpiryInfo", side_effect=mock_resolve), \
             patch.object(self.rm.db, "GetIncompleteRollovers", return_value=[]), \
             patch.object(self.rm, "IsTradingDay", return_value=True), \
             patch.object(self.rm, "SendDailySummaryEmail") as mock_summary, \
             patch.object(self.rm, "SendAlertEmail"), \
             patch("argparse.ArgumentParser.parse_args",
                   return_value=MagicMock(dry_run=False, instrument=None, force=False, status=False)):
            self.rm.main()

        upcoming = mock_summary.call_args[0][1]
        instruments = {u["instrument"] for u in upcoming}
        # Both TURMERIC (CSV ok) and GOLDM (CSV stale, fallback) should appear
        self.assertIn("TURMERIC", instruments)
        self.assertIn("GOLDM", instruments)
        self.assertEqual(len(upcoming), 2)


if __name__ == "__main__":
    unittest.main()
