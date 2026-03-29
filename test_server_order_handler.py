import pathlib
import os
import sys
import threading
import time
import types
import unittest
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch


sys.path.insert(0, os.path.dirname(__file__))


_smartapi = types.ModuleType("SmartApi")
_smartapi.SmartConnect = MagicMock()
sys.modules["SmartApi"] = _smartapi
sys.modules["pyotp"] = MagicMock()

_dirs = types.ModuleType("Directories")
_dirs.AngelInstrumentDirectory = "AngelInstrumentDetails.csv"
_dirs.AngelNararushLoginCred = "unused_nararush.txt"
_dirs.AngelEkanshLoginCred = "unused_ekansh.txt"
_dirs.AngelEshitaLoginCred = "unused_eshita.txt"
sys.modules["Directories"] = _dirs


import Server_Order_Handler as handler


def _make_order_details(**overrides):
    order = {
        "Tradetype": "BUY",
        "Exchange": "NCDEX",
        "Tradingsymbol": "DHANIYA20APR2099",
        "Quantity": "5",
        "Variety": "NORMAL",
        "Ordertype": "LIMIT",
        "Product": "CARRYFORWARD",
        "Validity": "DAY",
        "Price": "0",
        "Symboltoken": "DHANIYA20APR2099",
        "Squareoff": "",
        "Stoploss": "",
        "Broker": "ANGEL",
        "Netposition": "0",
        "User": "AABM826021",
        "UpdatedOrderRouting": "True",
        "ContractNameProvided": "False",
        "InstrumentType": "FUTCOM",
        "DaysPostWhichSelectNextContract": "12",
        "EntrySleepDuration": "90",
        "ExitSleepDuration": "60",
        "ConvertToMarketOrder": "True",
    }
    order.update(overrides)
    return order


class TestPlaceOrderAngelAPI(unittest.TestCase):

    def test_returns_none_and_sets_error_on_api_failure(self):
        session = MagicMock()
        session._postRequest.return_value = {
            "success": False,
            "message": "Access denied: Unregistered IP address. Register your IP before retrying.",
            "errorCode": "AG7002",
            "data": "",
        }
        order = _make_order_details()

        result = handler.PlaceOrderAngelAPI(session, order)

        self.assertIsNone(result)
        self.assertIn("AG7002", order["LastOrderError"])

    def test_returns_order_id_from_raw_post_response(self):
        session = MagicMock()
        session._postRequest.return_value = {
            "status": True,
            "data": {"orderid": "ANG12345"},
        }
        order = _make_order_details()

        result = handler.PlaceOrderAngelAPI(session, order)

        self.assertEqual(result, "ANG12345")
        self.assertNotIn("LastOrderError", order)


class TestPrepareAngelInstrumentContractName(unittest.TestCase):

    def test_exact_symbol_match_is_used_for_full_contract_symbol(self):
        csv_content = (
            ",token,symbol,name,expiry,strike,lotsize,instrumenttype,exch_seg,tick_size\n"
            "0,DHANIYA20APR2099,DHANIYA20APR2099,DHANIYA,20APR2099,-1,5,FUTCOM,NCDEX,200\n"
        )

        with NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
            tmp.write(csv_content)
            csv_path = tmp.name

        order = _make_order_details()

        try:
            with patch.object(handler, "AngelInstrumentDirectory", csv_path):
                filtered = handler.PrepareAngelInstrumentContractName(MagicMock(), order)
        finally:
            os.unlink(csv_path)

        self.assertFalse(filtered.empty)
        self.assertEqual(filtered["symbol"].iloc[0], "DHANIYA20APR2099")
        self.assertEqual(filtered["token"].iloc[0], "DHANIYA20APR2099")


class TestControlOrderFlowAngel(unittest.TestCase):

    def test_ncdex_route_enters_fifo_before_api_session(self):
        order = _make_order_details(ContractNameProvided="True", ConvertToMarketOrder="False")
        events = []

        @contextmanager
        def fake_fifo(*_args, **_kwargs):
            events.append("fifo_enter")
            yield
            events.append("fifo_exit")

        def fake_connect(_details):
            events.append("connect")
            return MagicMock()

        def fake_browser_order(_details):
            events.append("browser")
            return {"status": "submitted"}

        with patch.object(handler, "_AcquireAngelBrowserFifoTurn", fake_fifo), \
             patch.object(handler, "EstablishConnectionAngelAPI", side_effect=fake_connect), \
             patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "PrepareOrderAngel", side_effect=lambda session, details: details), \
             patch.object(handler, "PlaceOrderAngelBrowser", side_effect=fake_browser_order):

            result = handler.ControlOrderFlowAngel(order)

        self.assertEqual(result["status"], "submitted")
        self.assertEqual(events, ["fifo_enter", "connect", "browser", "fifo_exit"])

    def test_stops_cleanly_when_browser_order_placement_fails(self):
        order = _make_order_details(ContractNameProvided="True")

        with patch.object(handler, "EstablishConnectionAngelAPI", return_value=MagicMock()), \
             patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "PrepareOrderAngel", side_effect=lambda session, details: details), \
             patch.object(handler, "PlaceOrderAngelBrowser", return_value=None), \
             patch.object(handler, "PlaceOrderAngelAPI") as api_order, \
             patch.object(handler, "ModifyAngeOrder") as modify_order:

            result = handler.ControlOrderFlowAngel(order)

        self.assertIsNone(result)
        api_order.assert_not_called()
        modify_order.assert_not_called()

    def test_routes_to_browser_execution_after_contract_processing(self):
        order = _make_order_details(ContractNameProvided="True", ConvertToMarketOrder="False")

        with patch.object(handler, "EstablishConnectionAngelAPI", return_value=MagicMock()), \
             patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "PrepareOrderAngel", side_effect=lambda session, details: details), \
             patch.object(handler, "PlaceOrderAngelBrowser", return_value={"status": "submitted"}) as browser_order, \
             patch.object(handler, "PlaceOrderAngelAPI") as api_order:

            result = handler.ControlOrderFlowAngel(order)

        self.assertEqual(result["status"], "submitted")
        self.assertEqual(len(result["placements"]), 1)
        browser_order.assert_called_once()
        api_order.assert_not_called()

    def test_non_ncdex_orders_stay_on_smartapi_route(self):
        order = _make_order_details(
            Exchange="MCX",
            Tradingsymbol="CRUDEOILM20APR2099",
            Symboltoken="CRUDEOILM20APR2099",
            ContractNameProvided="True",
            ConvertToMarketOrder="False",
        )

        with patch.object(handler, "EstablishConnectionAngelAPI", return_value=MagicMock()), \
             patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "PrepareOrderAngel", side_effect=lambda session, details: details), \
             patch.object(handler, "PlaceOrderAngelBrowser") as browser_order, \
             patch.object(handler, "PlaceOrderAngelAPI", return_value="ANG123") as api_order:

            result = handler.ControlOrderFlowAngel(order)

        self.assertEqual(result, "ANG123")
        browser_order.assert_not_called()
        api_order.assert_called_once()

    def test_sets_warning_when_limit_to_market_conversion_is_skipped(self):
        order = _make_order_details(ContractNameProvided="True", ConvertToMarketOrder="True")

        with patch.object(handler, "EstablishConnectionAngelAPI", return_value=MagicMock()), \
             patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "PrepareOrderAngel", side_effect=lambda session, details: details), \
             patch.object(handler, "PlaceOrderAngelBrowser", return_value={"status": "submitted"}):

            result = handler.ControlOrderFlowAngel(order)

        self.assertIn("warning", result)
        self.assertIn("limit-to-market conversion", result["warning"])


class TestPlaceOrderAngelBrowser(unittest.TestCase):

    def _browser_config(self):
        return {
            "selectors_path": pathlib.Path("/tmp/selectors.json"),
            "profile_dir": pathlib.Path("/tmp/profile"),
            "log_dir": pathlib.Path("/tmp/logs"),
            "instrument_file": pathlib.Path("/tmp/instruments.csv"),
            "login_credentials_file": pathlib.Path("/tmp/angel_web_login_credentials.txt"),
            "otp_file": pathlib.Path("/tmp/angel_web_login_otp.txt"),
            "otp_timeout_seconds": 120,
            "otp_poll_interval": 1.0,
            "debugger_address": "127.0.0.1:9222",
            "url": "https://example.com",
            "watchlist_index": 4,
            "lock_path": pathlib.Path("/tmp/angel_browser.lock"),
            "lock_timeout_seconds": 5.0,
        }

    def test_returns_none_when_browser_lock_times_out(self):
        order = _make_order_details(ContractNameProvided="True")

        with patch.object(handler, "_GetAngelWebExecutionConfig", return_value=self._browser_config()), \
             patch.object(
                 handler,
                 "_AcquireAngelBrowserExecutionLock",
                 side_effect=TimeoutError("Angel browser execution is busy"),
             ):

            result = handler.PlaceOrderAngelBrowser(order)

        self.assertIsNone(result)
        self.assertIn("busy", order["LastOrderError"])

    def test_sets_scheduled_order_id_for_scheduled_modal_result(self):
        order = _make_order_details(ContractNameProvided="True")
        bot_instance = MagicMock()
        bot_instance.place_order.return_value = {
            "status": "submitted",
            "message": "Order Scheduled Your order has been scheduled for 10:00 in the next trading window.",
            "artifacts": {},
        }
        bot_instance.__enter__.return_value = bot_instance
        bot_instance.__exit__.return_value = None

        order_request = MagicMock()
        order_request.symbol = "CASTOR20APR2026"
        order_request.exchange = "NCDEX"

        @contextmanager
        def fake_lock(*_args, **_kwargs):
            yield

        with patch.object(handler, "_GetAngelWebExecutionConfig", return_value=self._browser_config()), \
             patch.object(handler, "_AcquireAngelBrowserExecutionLock", fake_lock), \
             patch.object(handler, "LoadAngelWebJson", return_value={}), \
             patch.object(handler, "InspectAngelBrowserSession", return_value={"status": "READY"}), \
             patch.object(handler, "NormalizeAngelWebOrderPayload", return_value=order_request), \
             patch.object(handler, "ResolveAngelWatchlistCandidate", return_value=MagicMock()), \
             patch.object(handler, "AngelWebOrderBot", return_value=bot_instance):

            result = handler.PlaceOrderAngelBrowser(order)

        self.assertEqual(result["status"], "submitted")
        self.assertEqual(order["OrderId"], "ANGEL_WEB_SCHEDULED")
        self.assertEqual(order["ExecutionRoute"], "ANGEL_WEB")

    def test_proceeds_when_guard_reports_login_required(self):
        order = _make_order_details(ContractNameProvided="True")
        bot_instance = MagicMock()
        bot_instance.place_order.return_value = {
            "status": "submitted",
            "message": "submitted",
            "artifacts": {},
        }
        bot_instance.__enter__.return_value = bot_instance
        bot_instance.__exit__.return_value = None

        order_request = MagicMock()
        order_request.symbol = "CASTOR20APR2026"
        order_request.exchange = "NCDEX"

        @contextmanager
        def fake_lock(*_args, **_kwargs):
            yield

        with patch.object(handler, "_GetAngelWebExecutionConfig", return_value=self._browser_config()), \
             patch.object(handler, "_AcquireAngelBrowserExecutionLock", fake_lock), \
             patch.object(handler, "LoadAngelWebJson", return_value={}), \
             patch.object(handler, "InspectAngelBrowserSession", return_value={"status": "LOGIN_REQUIRED"}), \
             patch.object(handler, "NormalizeAngelWebOrderPayload", return_value=order_request), \
             patch.object(handler, "ResolveAngelWatchlistCandidate", return_value=MagicMock()), \
             patch.object(handler, "AngelWebOrderBot", return_value=bot_instance):

            result = handler.PlaceOrderAngelBrowser(order)

        self.assertEqual(result["status"], "submitted")
        self.assertNotIn("LastOrderError", order)


class TestAngelBrowserFifoTurn(unittest.TestCase):

    def test_fifo_turn_serves_requests_in_arrival_order(self):
        original_next = handler._ANGEL_BROWSER_FIFO_NEXT_TICKET
        original_serving = handler._ANGEL_BROWSER_FIFO_SERVING_TICKET
        handler._ANGEL_BROWSER_FIFO_NEXT_TICKET = 0
        handler._ANGEL_BROWSER_FIFO_SERVING_TICKET = 0

        entry_order = []
        first_entered = threading.Event()

        def first_worker():
            with handler._AcquireAngelBrowserFifoTurn(_make_order_details(User="FIRST")):
                entry_order.append("first")
                first_entered.set()
                time.sleep(0.1)

        def second_worker():
            first_entered.wait(timeout=1)
            with handler._AcquireAngelBrowserFifoTurn(_make_order_details(User="SECOND")):
                entry_order.append("second")

        thread_one = threading.Thread(target=first_worker)
        thread_two = threading.Thread(target=second_worker)

        try:
            thread_one.start()
            thread_two.start()
            thread_one.join(timeout=2)
            thread_two.join(timeout=2)
        finally:
            handler._ANGEL_BROWSER_FIFO_NEXT_TICKET = original_next
            handler._ANGEL_BROWSER_FIFO_SERVING_TICKET = original_serving

        self.assertFalse(thread_one.is_alive())
        self.assertFalse(thread_two.is_alive())
        self.assertEqual(entry_order, ["first", "second"])


class TestEstablishConnectionAngelAPI(unittest.TestCase):

    def test_maps_aabm_user_to_ekansh_credentials(self):
        mock_session = MagicMock()

        with patch.object(handler, "SmartConnect", return_value=mock_session) as smart_connect_cls, \
             patch("builtins.open", unittest.mock.mock_open(read_data="api\nclient\npwd\ntotp\n")), \
             patch.object(handler.pyotp, "TOTP") as totp_cls:

            totp_cls.return_value.now.return_value = "123456"
            mock_session.generateSession.return_value = {
                "data": {"jwtToken": "jwt", "refreshToken": "refresh"}
            }
            mock_session.getfeedToken.return_value = "feed"
            mock_session.getProfile.return_value = {"data": {"exchanges": []}}

            result = handler.EstablishConnectionAngelAPI({"User": "AABM826021"})

        self.assertIs(result, mock_session)
        smart_connect_cls.assert_called_with("api")

    def test_raises_clear_error_for_unknown_user(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Angel user"):
            handler.EstablishConnectionAngelAPI({"User": "UNKNOWN"})


if __name__ == "__main__":
    unittest.main()
