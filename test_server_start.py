import importlib
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


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


_MODULE_SNAPSHOT = _snapshot_modules(
    [
        "flask_ngrok",
        "Server_Order_Place",
        "Login_Auto3_Angel",
        "PlaceFNOTradesKite",
        "PlaceMonthlyContrctFNOtrades",
        "Server_Order_Handler",
        "Kite_Server_Order_Handler",
        "forecast_orchestrator",
        "Server_Start",
    ]
)
_install_module("flask_ngrok", run_with_ngrok=lambda app, subdomain=None: None)
_install_module("Server_Order_Place", order=MagicMock())
_install_module("Login_Auto3_Angel", Login_Angel_Api=MagicMock())
_install_module("PlaceFNOTradesKite", LoopHashOrderRequest=MagicMock())
_install_module("PlaceMonthlyContrctFNOtrades", set_week_based_sl=MagicMock())
_install_module(
    "Server_Order_Handler",
    ControlOrderFlowAngel=MagicMock(),
    ControlOrderFlowKite=MagicMock(),
)
_install_module("Kite_Server_Order_Handler", ControlOrderFlowKite=MagicMock())


class _DummyForecastOrchestrator:
    def Start(self):
        return None

    def HandleWebhook(self, payload):
        return {"status": "ok", "payload": payload}

    def ApplyOverride(self, instrument, override_type, value):
        return {"status": "ok", "instrument": instrument, "override_type": override_type, "value": value}

    def GetStatus(self):
        return {"status": "ok"}


_install_module("forecast_orchestrator", ForecastOrchestrator=_DummyForecastOrchestrator)

sys.modules.pop("Server_Start", None)
server_start = importlib.import_module("Server_Start")
_restore_modules(_MODULE_SNAPSHOT)


def _make_payload():
    return {
        "User": "AABM826021",
        "Broker": "ANGEL",
        "Exchange": "NCDEX",
        "Tradetype": "BUY",
        "Tradingsymbol": "CASTOR",
        "Quantity": "1*5",
        "Variety": "NORMAL",
        "Ordertype": "LIMIT",
        "Product": "CARRYFORWARD",
        "Validity": "DAY",
        "Price": "0",
        "Symboltoken": "",
        "Netposition": "0",
        "UpdatedOrderRouting": "True",
        "ContractNameProvided": "False",
        "InstrumentType": "FUTCOM",
    }


class TestServerStartWebhookResponses(unittest.TestCase):

    def setUp(self):
        self.client = server_start.app.test_client()
        server_start._ANGEL_EXECUTION_REQUESTS.clear()

    def test_angel_failure_returns_structured_error_payload(self):
        payload = _make_payload()

        def fake_control(order_details):
            order_details["LastOrderError"] = "Insufficient margin"
            return None

        with patch.object(server_start, "ControlOrderFlowAngel", side_effect=fake_control):
            response = self.client.post(server_start.ANGEL_ORDER_WEBHOOK_PATH, json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["flow"], "angel")
        self.assertEqual(body["error"], "Insufficient margin")
        self.assertEqual(body["order"]["Tradingsymbol"], "CASTOR")

    def test_angel_success_returns_structured_success_payload(self):
        payload = _make_payload()

        def fake_control(order_details):
            order_details["ExecutionRoute"] = "ANGEL_WEB"
            order_details["OrderId"] = "ANGEL_WEB_SUBMITTED"
            return {"status": "submitted", "placements": [{"status": "submitted"}]}

        with patch.object(server_start, "ControlOrderFlowAngel", side_effect=fake_control):
            response = self.client.post(server_start.ANGEL_ORDER_WEBHOOK_PATH, json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["status"], "success")
        self.assertEqual(body["flow"], "angel")
        self.assertEqual(body["execution_route"], "ANGEL_WEB")
        self.assertEqual(body["order_id"], "ANGEL_WEB_SUBMITTED")
        self.assertEqual(body["result"]["status"], "submitted")

    def test_internal_angel_execute_requires_bearer_token_when_configured(self):
        payload = {"request_id": "abc123", "order": _make_payload()}

        with patch.object(server_start, "ANGEL_EXECUTOR_TOKEN", "secret-token"):
            response = self.client.post(server_start.ANGEL_EXECUTOR_PATH, json=payload)

        self.assertEqual(response.status_code, 401)
        body = response.get_json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["flow"], "angel_internal")

    def test_internal_angel_execute_returns_cached_response_for_duplicate_request_id(self):
        payload = {"request_id": "abc123", "order": _make_payload()}

        def fake_control(order_details):
            order_details["ExecutionRoute"] = "ANGEL_WEB"
            order_details["OrderId"] = "ANGEL_WEB_SUBMITTED"
            return {"status": "submitted", "placements": [{"status": "submitted"}]}

        with patch.object(server_start, "ControlOrderFlowAngel", side_effect=fake_control) as control_mock, \
             patch.object(server_start, "ANGEL_EXECUTOR_TOKEN", "secret-token"):
            headers = {"Authorization": "Bearer secret-token"}
            first = self.client.post(server_start.ANGEL_EXECUTOR_PATH, json=payload, headers=headers)
            second = self.client.post(server_start.ANGEL_EXECUTOR_PATH, json=payload, headers=headers)

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(control_mock.call_count, 1)
        self.assertEqual(first.get_json(), second.get_json())
        self.assertEqual(first.get_json()["flow"], "angel_internal")
        self.assertEqual(first.get_json()["execution_route"], "ANGEL_WEB")

    def test_worker_only_mode_rejects_public_routes(self):
        with patch.object(server_start, "ANGEL_EXECUTOR_ONLY", True):
            forecast_response = self.client.post("/forecast", json={"SystemName": "S30A", "Instrument": "CASTOR", "Netposition": 1, "ATR": 100})
            angel_response = self.client.post(server_start.ANGEL_ORDER_WEBHOOK_PATH, json=_make_payload())
            status_response = self.client.get("/status")

        self.assertEqual(forecast_response.status_code, 404)
        self.assertEqual(angel_response.status_code, 404)
        self.assertEqual(status_response.status_code, 404)
        self.assertEqual(forecast_response.get_json()["message"], "This server is running in ANGEL_EXECUTOR_ONLY mode. Use the internal executor endpoint only.")
        self.assertEqual(angel_response.get_json()["flow"], "worker_only")
        self.assertEqual(status_response.get_json()["flow"], "status")

    def test_worker_only_mode_still_allows_internal_executor(self):
        payload = {"request_id": "worker-1", "order": _make_payload()}

        def fake_control(order_details):
            order_details["ExecutionRoute"] = "ANGEL_WEB"
            order_details["OrderId"] = "ANGEL_WEB_SUBMITTED"
            return {"status": "submitted", "placements": [{"status": "submitted"}]}

        with patch.object(server_start, "ControlOrderFlowAngel", side_effect=fake_control), \
             patch.object(server_start, "ANGEL_EXECUTOR_ONLY", True), \
             patch.object(server_start, "ANGEL_EXECUTOR_TOKEN", "secret-token"):
            headers = {"Authorization": "Bearer secret-token"}
            response = self.client.post(server_start.ANGEL_EXECUTOR_PATH, json=payload, headers=headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["flow"], "angel_internal")
        self.assertEqual(response.get_json()["execution_route"], "ANGEL_WEB")


if __name__ == "__main__":
    unittest.main()
