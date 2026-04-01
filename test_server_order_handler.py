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
_MISSING = object()


def _snapshot_modules(names):
    return {name: sys.modules.get(name, _MISSING) for name in names}


def _restore_modules(snapshot):
    for name, module in snapshot.items():
        if module is _MISSING:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


_MODULE_SNAPSHOT = _snapshot_modules(["SmartApi", "pyotp", "Directories", "Server_Order_Handler"])
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
_restore_modules(_MODULE_SNAPSHOT)


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


class TestAngelInstrumentMasterCache(unittest.TestCase):

    def test_reuses_cached_master_when_file_is_unchanged(self):
        csv_content = (
            ",token,symbol,name,expiry,strike,lotsize,instrumenttype,exch_seg,tick_size\n"
            "0,DHANIYA20APR2099,DHANIYA20APR2099,DHANIYA,20APR2099,-1,5,FUTCOM,NCDEX,200\n"
        )

        with NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
            tmp.write(csv_content)
            csv_path = tmp.name

        read_calls = []
        real_read_csv = handler.pd.read_csv

        def tracking_read_csv(*args, **kwargs):
            read_calls.append((args, kwargs))
            return real_read_csv(*args, **kwargs)

        handler._ANGEL_INSTRUMENT_MASTER_CACHE.clear()
        try:
            with patch.object(handler.pd, "read_csv", side_effect=tracking_read_csv):
                first_df, first_hit, first_path = handler._LoadAngelInstrumentMaster(csv_path)
                second_df, second_hit, second_path = handler._LoadAngelInstrumentMaster(csv_path)
        finally:
            handler._ANGEL_INSTRUMENT_MASTER_CACHE.clear()
            os.unlink(csv_path)

        self.assertFalse(first_hit)
        self.assertTrue(second_hit)
        self.assertEqual(first_path, second_path)
        self.assertEqual(len(read_calls), 1)
        self.assertIn("serialnumber", first_df.columns)
        self.assertEqual(str(second_df["expiry"].iloc[0].date()), "2099-04-20")


class TestControlOrderFlowAngel(unittest.TestCase):

    def test_ncdex_route_uses_browser_only_preflight_for_simple_orders(self):
        order = _make_order_details(
            ContractNameProvided="True",
            ConvertToMarketOrder="False",
            Price="11948",
        )
        events = []

        @contextmanager
        def fake_fifo(*_args, **_kwargs):
            events.append("fifo_enter")
            yield
            events.append("fifo_exit")

        def fake_browser_order(_details):
            events.append("browser")
            return {"status": "submitted"}

        with patch.object(handler, "_AcquireAngelBrowserFifoTurn", fake_fifo), \
             patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "EstablishConnectionAngelAPI") as connect_api, \
             patch.object(handler, "PlaceOrderAngelBrowser", side_effect=fake_browser_order):

            result = handler.ControlOrderFlowAngel(order)

        self.assertEqual(result["status"], "submitted")
        self.assertEqual(events, ["fifo_enter", "browser", "fifo_exit"])
        connect_api.assert_not_called()

    def test_stops_cleanly_when_browser_order_placement_fails(self):
        order = _make_order_details(ContractNameProvided="True", Price="11948")

        with patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "PlaceOrderAngelBrowser", return_value=None), \
             patch.object(handler, "EstablishConnectionAngelAPI") as connect_api, \
             patch.object(handler, "PlaceOrderAngelAPI") as api_order, \
             patch.object(handler, "ModifyAngeOrder") as modify_order:

            result = handler.ControlOrderFlowAngel(order)

        self.assertIsNone(result)
        connect_api.assert_not_called()
        api_order.assert_not_called()
        modify_order.assert_not_called()

    def test_routes_to_browser_execution_without_smartapi_for_simple_ncdex_orders(self):
        order = _make_order_details(
            ContractNameProvided="True",
            ConvertToMarketOrder="False",
            Price="11948",
        )

        with patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "EstablishConnectionAngelAPI") as connect_api, \
             patch.object(handler, "PlaceOrderAngelBrowser", return_value={"status": "submitted"}) as browser_order, \
             patch.object(handler, "PlaceOrderAngelAPI") as api_order:

            result = handler.ControlOrderFlowAngel(order)

        self.assertEqual(result["status"], "submitted")
        self.assertEqual(len(result["placements"]), 1)
        connect_api.assert_not_called()
        browser_order.assert_called_once()
        api_order.assert_not_called()

    def test_browser_route_resolves_contract_without_smartapi_for_simple_ncdex_entries(self):
        order = _make_order_details(
            Tradingsymbol="CASTOR",
            Symboltoken="",
            ContractNameProvided="False",
            ConvertToMarketOrder="False",
            Netposition="3",
            Quantity="3*5",
            Price="11948",
        )

        with patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity", side_effect=lambda details: details.update({"Quantity": 15, "Netposition": 15})), \
             patch.object(handler, "PrepareInstrumentContractName", side_effect=lambda _session, details: details.update({"Tradingsymbol": "CASTOR20APR2026", "Symboltoken": "12345"})), \
             patch.object(handler, "EstablishConnectionAngelAPI") as connect_api, \
             patch.object(handler, "PlaceOrderAngelBrowser", return_value={"status": "submitted"}) as browser_order:

            result = handler.ControlOrderFlowAngel(order)

        self.assertEqual(result["status"], "submitted")
        self.assertEqual(order["Tradingsymbol"], "CASTOR20APR2026")
        self.assertEqual(order["Symboltoken"], "12345")
        connect_api.assert_not_called()
        browser_order.assert_called_once()

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
        order = _make_order_details(
            ContractNameProvided="True",
            ConvertToMarketOrder="True",
            Price="11948",
        )

        with patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "EstablishConnectionAngelAPI") as connect_api, \
             patch.object(handler, "PlaceOrderAngelBrowser", return_value={"status": "submitted"}):

            result = handler.ControlOrderFlowAngel(order)

        connect_api.assert_not_called()
        self.assertIn("warning", result)
        self.assertIn("limit-to-market conversion", result["warning"])

    def test_ncdex_limit_order_without_price_still_uses_smartapi_preflight(self):
        order = _make_order_details(
            ContractNameProvided="True",
            ConvertToMarketOrder="False",
            Price="0",
        )

        with patch.object(handler, "ConfigureNetDirectionOfTrade"), \
             patch.object(handler, "Validate_Quantity"), \
             patch.object(handler, "EstablishConnectionAngelAPI", return_value=MagicMock()) as connect_api, \
             patch.object(handler, "PrepareOrderAngel", side_effect=lambda session, details: details), \
             patch.object(handler, "PlaceOrderAngelBrowser", return_value={"status": "submitted"}) as browser_order:

            result = handler.ControlOrderFlowAngel(order)

        self.assertEqual(result["status"], "submitted")
        connect_api.assert_called_once()
        browser_order.assert_called_once()


class TestValidateQuantity(unittest.TestCase):

    def test_preserves_lot_count_for_browser_routes_when_multiplier_is_expanded(self):
        order = _make_order_details(
            Quantity="3*5",
            Netposition="3",
        )

        handler.Validate_Quantity(order)

        self.assertEqual(order["Quantity"], 15)
        self.assertEqual(order["Netposition"], 15)
        self.assertEqual(order["UiQuantityLots"], 3)


class TestPrepareOrderAngel(unittest.TestCase):

    def test_preserves_explicit_limit_price_when_provided(self):
        session = MagicMock()
        session.ltpData.return_value = {
            "data": {"ltp": 12345.0},
        }
        order = _make_order_details(Price="11948", Ordertype="LIMIT")

        result = handler.PrepareOrderAngel(session, order)

        self.assertEqual(result["Price"], "11948")


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
            "chrome_binary": None,
            "headless": False,
            "attach_only": False,
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

    def test_uses_browser_config_for_auto_launch_capable_guard_and_bot(self):
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

        config = self._browser_config()
        config["chrome_binary"] = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        config["attach_only"] = False

        with patch.object(handler, "_GetAngelWebExecutionConfig", return_value=config), \
             patch.object(handler, "_AcquireAngelBrowserExecutionLock", fake_lock), \
             patch.object(handler, "LoadAngelWebJson", return_value={}), \
             patch.object(handler, "InspectAngelBrowserSession", return_value={"status": "READY"}) as inspect_mock, \
             patch.object(handler, "NormalizeAngelWebOrderPayload", return_value=order_request), \
             patch.object(handler, "ResolveAngelWatchlistCandidate", return_value=MagicMock()), \
             patch.object(handler, "AngelWebOrderBot", return_value=bot_instance) as bot_cls:

            result = handler.PlaceOrderAngelBrowser(order)

        self.assertEqual(result["status"], "submitted")
        self.assertEqual(inspect_mock.call_args.kwargs["attach_only"], False)
        self.assertEqual(inspect_mock.call_args.kwargs["chrome_binary"], config["chrome_binary"])
        self.assertEqual(bot_cls.call_args.kwargs["attach_only"], False)
        self.assertEqual(bot_cls.call_args.kwargs["chrome_binary"], config["chrome_binary"])

    def test_browser_payload_uses_ui_quantity_lots_instead_of_expanded_quantity(self):
        order = _make_order_details(
            Tradingsymbol="CASTOR20APR2026",
            Symboltoken="CASTOR20APR2026",
            Quantity=15,
            UiQuantityLots=3,
            Price="6557",
        )

        payload = handler._BuildAngelWebOrderPayload(order)

        self.assertEqual(payload["quantity"], 3)
        self.assertEqual(payload["symbol"], "CASTOR20APR2026")
        self.assertEqual(payload["price"], 6557.0)

    def test_browser_payload_falls_back_to_lot_count_from_multiplier_string(self):
        order = _make_order_details(
            Tradingsymbol="CASTOR20APR2026",
            Symboltoken="CASTOR20APR2026",
            Quantity="3*5",
            Price="6557",
        )

        payload = handler._BuildAngelWebOrderPayload(order)

        self.assertEqual(payload["quantity"], 3)


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

    def test_maps_aabm_user_to_eshita_credentials(self):
        mock_session = MagicMock()

        open_mock = unittest.mock.mock_open(read_data="api\nclient\npwd\ntotp\n")

        with patch.object(handler, "SmartConnect", return_value=mock_session) as smart_connect_cls, \
             patch("builtins.open", open_mock), \
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
        open_mock.assert_called_once_with("unused_eshita.txt", 'r')

    def test_raises_clear_error_for_unknown_user(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Angel user"):
            handler.EstablishConnectionAngelAPI({"User": "UNKNOWN"})

    def test_e513_user_is_rejected_as_unsupported(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Angel user"):
            handler.EstablishConnectionAngelAPI({"User": "E51339915"})


if __name__ == "__main__":
    unittest.main()
