import unittest
from datetime import datetime
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
import time
from unittest.mock import call, patch

import angel_web_order_bot as bot


class TestNormalizeOrderPayload(unittest.TestCase):
    def test_limit_order_requires_price(self):
        with self.assertRaises(ValueError):
            bot.normalize_order_payload(
                {
                    "exchange": "NCDEX",
                    "symbol": "DHANIYA20APR2026",
                    "side": "BUY",
                    "quantity": 5,
                    "product": "CARRYFORWARD",
                    "order_type": "LIMIT",
                }
            )

    def test_market_order_clears_price(self):
        order = bot.normalize_order_payload(
            {
                "exchange": "ncdex",
                "symbol": "dhaniya20apr2026",
                "side": "buy",
                "quantity": 5,
                "product": "carryforward",
                "order_type": "market",
                "price": 123.4,
            }
        )
        self.assertEqual(order.exchange, "NCDEX")
        self.assertEqual(order.symbol, "DHANIYA20APR2026")
        self.assertEqual(order.price, None)

    def test_cli_gate_requires_payload_opt_in(self):
        order = bot.normalize_order_payload(
            {
                "exchange": "NCDEX",
                "symbol": "DHANIYA20APR2026",
                "side": "BUY",
                "quantity": 5,
                "product": "CARRYFORWARD",
                "order_type": "LIMIT",
                "price": 11948.0,
                "submit_live": False,
            },
            submit_live_override=True,
        )
        self.assertFalse(order.submit_live)

    def test_cli_gate_and_payload_together_enable_live_submit(self):
        order = bot.normalize_order_payload(
            {
                "exchange": "NCDEX",
                "symbol": "DHANIYA20APR2026",
                "side": "BUY",
                "quantity": 5,
                "product": "CARRYFORWARD",
                "order_type": "LIMIT",
                "price": 11948.0,
                "submit_live": True,
            },
            submit_live_override=True,
        )
        self.assertTrue(order.submit_live)


class TestNormalizeSelectorConfig(unittest.TestCase):
    def test_accepts_single_selector_object(self):
        selectors = bot.normalize_selector_config(
            {
                "search_input": {
                    "by": "xpath",
                    "value": "//input"
                }
            }
        )
        self.assertEqual(len(selectors["search_input"]), 1)

    def test_rejects_unknown_locator_strategy(self):
        with self.assertRaises(ValueError):
            bot.normalize_selector_config(
                {
                    "bad": [
                        {
                            "by": "shadow",
                            "value": "//input"
                        }
                    ]
                }
            )


class TestDefaultSelectorConfig(unittest.TestCase):
    def test_includes_review_and_inline_error_selectors(self):
        selectors = json.loads(bot.DEFAULT_SELECTORS_PATH.read_text(encoding="utf-8"))
        self.assertIn("review_edit_button", selectors)
        self.assertIn("inline_order_pad_message", selectors)
        self.assertIn("scheduled_order_title", selectors)
        self.assertIn("scheduled_order_message", selectors)
        self.assertIn("scheduled_order_ok_button", selectors)
        self.assertIn("login_identifier_input", selectors)
        self.assertIn("login_client_id_tab", selectors)
        self.assertIn("login_mobile_number_tab", selectors)
        self.assertIn("mpin_input", selectors)
        self.assertIn("otp_input", selectors)


class TestAngelLoginFiles(unittest.TestCase):
    def test_load_login_credentials_file_accepts_key_value_format(self):
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("login_id=E12345678\nmpin=1234\nlogin_mode=client_id\n")
            temp_path = Path(handle.name)

        try:
            credentials = bot.load_login_credentials_file(temp_path)
        finally:
            temp_path.unlink(missing_ok=True)

        self.assertEqual(
            credentials,
            {
                "login_id": "E12345678",
                "client_id": "E12345678",
                "mobile_number": "",
                "mpin": "1234",
                "login_mode": "client_id",
            },
        )

    def test_load_login_credentials_file_accepts_two_line_format(self):
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("9999999999\n4321\n")
            temp_path = Path(handle.name)

        try:
            credentials = bot.load_login_credentials_file(temp_path)
        finally:
            temp_path.unlink(missing_ok=True)

        self.assertEqual(credentials["login_id"], "9999999999")
        self.assertEqual(credentials["mobile_number"], "9999999999")
        self.assertEqual(credentials["mpin"], "4321")

    def test_load_login_credentials_file_accepts_explicit_client_id_and_mobile(self):
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("client_id=E51339915\nmobile_number=9825944354\nmpin=1812\nlogin_mode=client_id\n")
            temp_path = Path(handle.name)

        try:
            credentials = bot.load_login_credentials_file(temp_path)
        finally:
            temp_path.unlink(missing_ok=True)

        self.assertEqual(credentials["login_id"], "E51339915")
        self.assertEqual(credentials["client_id"], "E51339915")
        self.assertEqual(credentials["mobile_number"], "9825944354")
        self.assertEqual(credentials["login_mode"], "client_id")

    def test_read_otp_from_file_requires_fresh_update(self):
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("111111\n")
            temp_path = Path(handle.name)

        try:
            stale_time = time.time() - 30
            os.utime(temp_path, (stale_time, stale_time))
            wait_started_at = time.time()
            temp_path.write_text("654321\n", encoding="utf-8")

            bot_instance = bot.AngelWebOrderBot(
                {"search_input": {"by": "xpath", "value": "//input"}},
                otp_file=temp_path,
                otp_timeout_seconds=1,
                otp_poll_interval=0.01,
            )
            code = bot_instance._read_otp_from_file(wait_started_at)
        finally:
            temp_path.unlink(missing_ok=True)

        self.assertEqual(code, "654321")


class TestEnsureReadyLoginFlow(unittest.TestCase):
    def test_ensure_ready_attempts_file_login_before_raising(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        page_ready_checks = [bot.TimeoutException("not ready"), object()]
        attempted = []

        def fake_find(selector_key, **_kwargs):
            if selector_key != "page_ready":
                raise AssertionError(selector_key)
            result = page_ready_checks.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        bot_instance._find_first = fake_find  # type: ignore[assignment]
        bot_instance._attempt_login_from_files = lambda: attempted.append(True) or True  # type: ignore[assignment]

        bot_instance.ensure_ready()

        self.assertEqual(attempted, [True])


class TestAuthChallengeDetection(unittest.TestCase):
    def test_infer_login_identifier_mode_distinguishes_mobile_and_client_id(self):
        self.assertEqual(bot.infer_login_identifier_mode("9825944354"), "mobile")
        self.assertEqual(bot.infer_login_identifier_mode("E51339915"), "client_id")

    def test_extract_login_blocker_message_detects_device_authentication_error(self):
        message = bot.extract_login_blocker_message(
            "Unable to authenticate you with this device, please try again"
        )
        self.assertEqual(message, "unable to authenticate you with this device")

    def test_detect_auth_challenge_prefers_otp_when_page_text_mentions_otp(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance._read_visible_page_text = lambda: "please enter otp to continue"  # type: ignore[assignment]
        bot_instance._find_generic_auth_inputs = lambda: [object()]  # type: ignore[assignment]

        def fake_find_all(selector_key, **_kwargs):
            if selector_key in {"otp_input", "mpin_input"}:
                return []
            raise AssertionError(selector_key)

        bot_instance._find_all = fake_find_all  # type: ignore[assignment]

        self.assertEqual(bot_instance._detect_auth_challenge(), "otp")

    def test_detect_auth_challenge_uses_generic_inputs_for_mpin_when_page_mentions_mpin(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance._read_visible_page_text = lambda: "enter your mpin"  # type: ignore[assignment]
        bot_instance._find_generic_auth_inputs = lambda: [object()]  # type: ignore[assignment]

        def fake_find_all(selector_key, **_kwargs):
            if selector_key in {"otp_input", "mpin_input"}:
                return []
            raise AssertionError(selector_key)

        bot_instance._find_all = fake_find_all  # type: ignore[assignment]

        self.assertEqual(bot_instance._detect_auth_challenge(), "mpin")


class TestWatchlistCandidateLoading(unittest.TestCase):
    def test_load_watchlist_candidates_filters_and_dedupes(self):
        csv_text = """token,symbol,name,expiry,strike,lotsize,instrumenttype,exch_seg,tick_size
1,DHANIYA20APR2026,DHANIYA,20APR2026,,1,FUTCOM,NCDEX,1
2,DHANIYA20APR2026_DUP,DHANIYA,20APR2026,,1,FUTCOM,NCDEX,1
3,DHANIYA20MAY2026,DHANIYA,20MAY2026,,1,FUTCOM,NCDEX,1
4,DHANIYA01APR2026,DHANIYA,01APR2026,,1,FUTCOM,NCDEX,1
5,JEERA20APR2026,JEERAUNJHA,20APR2026,,1,FUTCOM,NCDEX,1
6,JEERA20APR2026_OPT,JEERAUNJHA,20APR2026,,1,OPTFUT,NCDEX,1
7,MCXTEST20APR2026,MCXTEST,20APR2026,,1,FUTCOM,MCX,1
"""
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write(csv_text)
            temp_path = Path(handle.name)

        try:
            candidates = bot.load_watchlist_candidates(
                temp_path,
                min_days_to_expiry=6,
                as_of=datetime(2026, 3, 28),
            )
        finally:
            temp_path.unlink(missing_ok=True)

        self.assertEqual(
            [(candidate.name, candidate.expiry_label) for candidate in candidates],
            [
                ("DHANIYA", "20 Apr 2026"),
                ("JEERAUNJHA", "20 Apr 2026"),
                ("DHANIYA", "20 May 2026"),
            ],
        )

    def test_resolve_watchlist_candidate_matches_exact_symbol(self):
        csv_text = """token,symbol,name,expiry,strike,lotsize,instrumenttype,exch_seg,tick_size
1,DHANIYA20APR2026,DHANIYA,20APR2026,,1,FUTCOM,NCDEX,1
2,DHANIYA20MAY2026,DHANIYA,20MAY2026,,1,FUTCOM,NCDEX,1
3,DHANIYA20APR2026_OPT,DHANIYA,20APR2026,,1,OPTFUT,NCDEX,1
"""
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write(csv_text)
            temp_path = Path(handle.name)

        try:
            candidate = bot.resolve_watchlist_candidate(
                temp_path,
                "dhaniya20apr2026",
            )
        finally:
            temp_path.unlink(missing_ok=True)

        self.assertEqual(candidate.symbol, "DHANIYA20APR2026")
        self.assertEqual(candidate.name, "DHANIYA")
        self.assertEqual(candidate.expiry_label, "20 Apr 2026")


class TestWatchlistRowParsing(unittest.TestCase):
    def test_extract_watchlist_row_key_handles_search_row_shape(self):
        class DummyElement:
            text = "CMD\nBAJRA\nNCDEX\n20 Apr 2026\nB\nS"

        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        self.assertEqual(
            bot_instance._extract_watchlist_row_key(DummyElement()),
            ("BAJRA", "NCDEX", "20 APR 2026"),
        )


class TestSubmitTransitionState(unittest.TestCase):
    def test_wait_for_post_submit_state_detects_scheduled_modal(self):
        class DummyElement:
            def is_displayed(self):
                return True

        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance.driver = object()

        def fake_find_first(selector_key, **_kwargs):
            if selector_key == "scheduled_order_title":
                return DummyElement()
            raise bot.TimeoutException()

        bot_instance._find_first = fake_find_first  # type: ignore[assignment]

        self.assertEqual(bot_instance._wait_for_post_submit_state(), "scheduled_modal")

    def test_wait_for_post_submit_state_detects_review_screen(self):
        class DummyElement:
            def is_displayed(self):
                return True

        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance.driver = object()

        def fake_find_first(selector_key, **_kwargs):
            if selector_key == "review_edit_button":
                return DummyElement()
            raise bot.TimeoutException()

        bot_instance._find_first = fake_find_first  # type: ignore[assignment]

        self.assertEqual(bot_instance._wait_for_post_submit_state(), "confirm_required")

    def test_wait_for_post_submit_state_detects_inline_rejection(self):
        class DummyElement:
            def is_displayed(self):
                return True

        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance.driver = object()

        def fake_find_first(selector_key, **_kwargs):
            if selector_key == "inline_order_pad_message":
                return DummyElement()
            if selector_key == "submit_button":
                return DummyElement()
            raise bot.TimeoutException()

        bot_instance._find_first = fake_find_first  # type: ignore[assignment]

        self.assertEqual(bot_instance._wait_for_post_submit_state(), "inline_message")


class TestInputSetter(unittest.TestCase):
    def test_normalize_input_value_strips_commas(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        self.assertEqual(bot_instance._normalize_input_value("6,565.00"), "6565.00")

    def test_set_input_value_with_retry_uses_js_fallback(self):
        class DummyElement:
            def __init__(self):
                self.value = ""

            def get_attribute(self, name):
                if name == "value":
                    return self.value
                return None

        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance.driver = object()
        element = DummyElement()

        def broken_type(*_args, **_kwargs):
            element.value = ""

        def js_set(_element, value):
            element.value = value
            return value

        bot_instance._clear_and_type_element = broken_type  # type: ignore[assignment]
        bot_instance._set_input_value_element = js_set  # type: ignore[assignment]

        bot_instance._set_input_value_with_retry(element, "5")
        self.assertEqual(element.value, "5")

    def test_fill_order_pad_fields_pauses_before_price_fill(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        order = bot.OrderRequest(
            exchange="NCDEX",
            symbol="CASTOR20APR2026",
            side="BUY",
            quantity=5,
            product="CARRYFORWARD",
            order_type="LIMIT",
            price=6565.0,
        )
        elements = {
            "quantityOrderPad": object(),
            "priceOrderPad": object(),
        }
        calls = []

        def fake_find(element_id, **_kwargs):
            calls.append(("find", element_id))
            return elements[element_id]

        def fake_set_input(element, value):
            label = "quantity" if element is elements["quantityOrderPad"] else "price"
            calls.append(("set", label, value))

        bot_instance._find_element_by_id = fake_find  # type: ignore[assignment]
        bot_instance._set_input_value_with_retry = fake_set_input  # type: ignore[assignment]
        bot_instance._select_product_button = lambda product: calls.append(("product", product))  # type: ignore[assignment]
        bot_instance._select_order_type_button = lambda order_type: calls.append(("order_type", order_type))  # type: ignore[assignment]

        with patch("angel_web_order_bot.time.sleep") as sleep_mock:
            bot_instance._fill_order_pad_fields(order)

        self.assertIn(("set", "quantity", "5"), calls)
        self.assertIn(("set", "price", "6565.0"), calls)
        sleep_mock.assert_called_once_with(3)
        self.assertLess(calls.index(("set", "quantity", "5")), calls.index(("set", "price", "6565.0")))


class TestUiMessageSummary(unittest.TestCase):
    def test_summarize_ui_message_extracts_scheduled_text(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        message = bot_instance._summarize_ui_message(
            "Random page text Order Scheduled Your order has been scheduled for 10:00 in the next trading window. "
            "Please ensure sufficient funds in your trading balance to execute the order random footer"
        )
        self.assertEqual(
            message,
            "Order Scheduled Your order has been scheduled for 10:00 in the next trading window. "
            "Please ensure sufficient funds in your trading balance to execute the order",
        )

    def test_summarize_ui_message_extracts_margin_text(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        message = bot_instance._summarize_ui_message(
            "Insufficient margin! To buy 5 Lots, please add ₹1,48,317.12 ADD FUNDS"
        )
        self.assertEqual(
            message,
            "Insufficient margin! To buy 5 Lots, please add ₹1,48,317.12",
        )


class TestPlaceOrderDelays(unittest.TestCase):
    def test_place_order_waits_before_submit_and_confirm_clicks(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance.driver = object()
        order = bot.OrderRequest(
            exchange="NCDEX",
            symbol="CASTOR20APR2026",
            side="BUY",
            quantity=5,
            product="CARRYFORWARD",
            order_type="LIMIT",
            price=6565.0,
            submit_live=True,
        )
        candidate = bot.WatchlistCandidate(
            symbol="CASTOR20APR2026",
            name="CASTOR",
            exchange="NCDEX",
            expiry_label="20 Apr 2026",
            expiry_date=datetime(2026, 4, 20),
        )
        submit_button = object()
        confirm_button = object()
        clicked = []

        bot_instance.open_target_page = lambda: None  # type: ignore[assignment]
        bot_instance.ensure_ready = lambda allow_manual_login=False: None  # type: ignore[assignment]
        bot_instance._reset_order_entry_state = lambda: None  # type: ignore[assignment]
        bot_instance._select_watchlist = lambda index: None  # type: ignore[assignment]
        bot_instance._click_watchlist_action = lambda candidate_arg, side: None  # type: ignore[assignment]
        bot_instance._fill_order_pad_fields = lambda order_arg: None  # type: ignore[assignment]
        bot_instance._capture_artifacts = lambda prefix: {}  # type: ignore[assignment]
        bot_instance._prepare_submit_click = lambda: None  # type: ignore[assignment]
        bot_instance._wait_for_submit_ready = lambda *args, **kwargs: submit_button  # type: ignore[assignment]
        bot_instance._wait_for_post_submit_state = lambda: "confirm_required" if len(clicked) < 2 else "scheduled_modal"  # type: ignore[assignment]
        bot_instance._find_first_enabled = lambda selector_key, timeout_seconds=3: confirm_button  # type: ignore[assignment]
        bot_instance._click = lambda selector_key, timeout_seconds=3: None  # type: ignore[assignment]
        bot_instance._read_optional_message = lambda selector_key, timeout_seconds=1: "Order Scheduled" if selector_key == "scheduled_order_message" else None  # type: ignore[assignment]
        bot_instance._summarize_ui_message = lambda message: message  # type: ignore[assignment]

        def fake_click(element):
            clicked.append(element)

        bot_instance._click_element = fake_click  # type: ignore[assignment]

        with patch("angel_web_order_bot.time.sleep") as sleep_mock:
            result = bot_instance.place_order(order, candidate=candidate, watchlist_index=4)

        self.assertEqual(clicked, [submit_button, confirm_button])
        self.assertEqual(sleep_mock.call_args_list, [call(1), call(1)])
        self.assertEqual(result["status"], "submitted")
        self.assertEqual(result["transition_state"], "scheduled_modal")


if __name__ == "__main__":
    unittest.main()
