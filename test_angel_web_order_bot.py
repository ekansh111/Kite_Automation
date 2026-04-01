import unittest
from datetime import datetime
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
import time
from types import SimpleNamespace
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
        self.assertIn("post_login_got_it_button", selectors)


class TestChromeLaunchConfig(unittest.TestCase):
    def test_configure_chrome_profile_preferences_disables_password_manager(self):
        with TemporaryDirectory() as temp_dir:
            profile_dir = Path(temp_dir)
            preferences_path = bot.configure_chrome_profile_preferences(profile_dir)

            self.assertTrue(preferences_path.exists())
            preferences = json.loads(preferences_path.read_text(encoding="utf-8"))

        self.assertFalse(preferences["credentials_enable_service"])
        self.assertFalse(preferences["profile"]["password_manager_enabled"])
        self.assertFalse(preferences["profile"]["password_manager_leak_detection"])
        self.assertFalse(preferences["autofill"]["credit_card_enabled"])
        self.assertFalse(preferences["autofill"]["profile_enabled"])

    @patch("angel_web_order_bot.subprocess.Popen")
    @patch("angel_web_order_bot.resolve_chrome_binary", return_value="chrome.exe")
    def test_run_launch_command_includes_password_manager_suppression_flags(self, _resolve_mock, popen_mock):
        with TemporaryDirectory() as temp_dir:
            args = SimpleNamespace(
                profile_dir=temp_dir,
                chrome_binary=None,
                debug_port=9222,
                url=bot.APP_URL,
            )

            result = bot.run_launch_command(args)

        self.assertEqual(result, 0)
        popen_mock.assert_called_once()
        command = popen_mock.call_args.args[0]
        self.assertIn("--remote-debugging-address=127.0.0.1", command)
        self.assertIn("--remote-debugging-port=9222", command)
        self.assertIn("--no-first-run", command)
        self.assertIn("--no-default-browser-check", command)
        self.assertIn("--disable-notifications", command)
        self.assertIn("--disable-popup-blocking", command)
        self.assertTrue(any(arg.startswith("--disable-features=PasswordManagerOnboarding") for arg in command))

    @patch("angel_web_order_bot.subprocess.Popen")
    def test_launch_debugger_chrome_session_writes_profile_and_debugger_flags(self, popen_mock):
        with TemporaryDirectory() as temp_dir:
            profile_dir = Path(temp_dir)
            resolved_profile_dir = profile_dir.resolve()
            bot.launch_debugger_chrome_session(
                chrome_binary="chrome.exe",
                profile_dir=profile_dir,
                debugger_address="127.0.0.1:9333",
                url="https://example.com/trade",
            )

            command = popen_mock.call_args.args[0]
            preferences_path = resolved_profile_dir / "Default" / "Preferences"
            self.assertTrue(preferences_path.exists())

        self.assertIn("--remote-debugging-address=127.0.0.1", command)
        self.assertIn("--remote-debugging-port=9333", command)
        self.assertIn(f"--user-data-dir={resolved_profile_dir}", command)
        self.assertIn("--new-window", command)
        self.assertEqual(command[-1], "https://example.com/trade")

    @patch("angel_web_order_bot.launch_debugger_chrome_session")
    @patch("angel_web_order_bot.resolve_chrome_binary", return_value="chrome.exe")
    @patch("angel_web_order_bot.wait_for_debugger_address", side_effect=[False, True])
    def test_build_driver_auto_launches_debugger_browser_when_debugger_is_initially_unreachable(
        self,
        wait_mock,
        resolve_mock,
        launch_mock,
    ):
        driver = object()
        bot_instance = bot.AngelWebOrderBot(
            {"search_input": {"by": "xpath", "value": "//input"}},
            debugger_address="127.0.0.1:9222",
            chrome_binary=None,
        )

        with patch.object(
            bot.AngelWebOrderBot,
            "_attach_to_debugger_session",
            return_value=driver,
        ) as attach_mock:
            result = bot_instance._build_driver()

        self.assertIs(result, driver)
        self.assertEqual(attach_mock.call_count, 1)
        self.assertEqual(wait_mock.call_count, 2)
        resolve_mock.assert_called_once_with(None)
        launch_mock.assert_called_once_with(
            chrome_binary="chrome.exe",
            profile_dir=bot_instance.profile_dir,
            debugger_address="127.0.0.1:9222",
            url=bot_instance.url,
        )

    @patch("angel_web_order_bot.launch_debugger_chrome_session")
    @patch("angel_web_order_bot.resolve_chrome_binary")
    @patch("angel_web_order_bot.wait_for_debugger_address", return_value=True)
    def test_build_driver_falls_back_without_relaunch_when_debugger_is_reachable_but_attach_fails(
        self,
        wait_mock,
        resolve_mock,
        launch_mock,
    ):
        bot_instance = bot.AngelWebOrderBot(
            {"search_input": {"by": "xpath", "value": "//input"}},
            debugger_address="127.0.0.1:9222",
            chrome_binary=None,
        )

        with patch.object(
            bot.AngelWebOrderBot,
            "_attach_to_debugger_session",
            side_effect=RuntimeError("attach failed"),
        ):
            with patch("angel_web_order_bot.uc", None):
                with self.assertRaisesRegex(RuntimeError, "undetected_chromedriver is not available"):
                    bot_instance._build_driver()

        wait_mock.assert_called_once_with(
            "127.0.0.1:9222",
            timeout_seconds=1.0,
            poll_interval_seconds=0.25,
        )
        resolve_mock.assert_not_called()
        launch_mock.assert_not_called()


class TestAngelLoginFiles(unittest.TestCase):
    def test_login_fill_delay_constants_match_requested_values(self):
        self.assertEqual(bot.LOGIN_IDENTIFIER_FILL_DELAY_SECONDS, 1)
        self.assertEqual(bot.MPIN_FILL_DELAY_SECONDS, 1)
        self.assertEqual(bot.OTP_FILL_DELAY_SECONDS, 2)
        self.assertEqual(bot.LOGIN_IDENTIFIER_TYPING_DELAY_SECONDS, 0.15)
        self.assertEqual(bot.MPIN_TYPING_DELAY_SECONDS, 0.15)
        self.assertEqual(bot.OTP_TYPING_DELAY_SECONDS, 0.15)

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
            handle.write("client_id=AABM826021\nmobile_number=9825944354\nmpin=1812\nlogin_mode=client_id\n")
            temp_path = Path(handle.name)

        try:
            credentials = bot.load_login_credentials_file(temp_path)
        finally:
            temp_path.unlink(missing_ok=True)

        self.assertEqual(credentials["login_id"], "AABM826021")
        self.assertEqual(credentials["client_id"], "AABM826021")
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

    def test_read_otp_from_file_starts_fetcher_when_code_is_missing(self):
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("")
            temp_path = Path(handle.name)

        try:
            wait_started_at = time.time()
            started = []

            bot_instance = bot.AngelWebOrderBot(
                {"search_input": {"by": "xpath", "value": "//input"}},
                otp_file=temp_path,
                otp_timeout_seconds=1,
                otp_poll_interval=0.01,
            )

            def fake_start():
                started.append(True)
                temp_path.write_text("777777\n", encoding="utf-8")
                current_time = time.time()
                os.utime(temp_path, (current_time, current_time))
                return True

            bot_instance._start_otp_fetcher = fake_start  # type: ignore[assignment]
            code = bot_instance._read_otp_from_file(wait_started_at)
        finally:
            temp_path.unlink(missing_ok=True)

        self.assertEqual(started, [True])
        self.assertEqual(code, "777777")

    @patch("angel_web_order_bot.subprocess.Popen")
    def test_start_otp_fetcher_uses_repo_script_and_current_python(self, popen_mock):
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as otp_handle:
            otp_path = Path(otp_handle.name)
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as script_handle:
            script_path = Path(script_handle.name)

        try:
            process = popen_mock.return_value
            process.poll.return_value = None
            bot_instance = bot.AngelWebOrderBot(
                {"search_input": {"by": "xpath", "value": "//input"}},
                otp_file=otp_path,
            )
            bot_instance.otp_fetch_script = script_path
            bot_instance.otp_fetch_wait_timeout_seconds = 30.0
            bot_instance.otp_fetch_poll_interval_seconds = 2.0

            started = bot_instance._start_otp_fetcher()
        finally:
            otp_path.unlink(missing_ok=True)
            script_path.unlink(missing_ok=True)

        self.assertTrue(started)
        popen_mock.assert_called_once()
        command = popen_mock.call_args.args[0]
        self.assertEqual(command[0], bot.sys.executable)
        self.assertEqual(command[1], str(script_path))
        self.assertIn(str(otp_path), command)
        self.assertIn("30.0", command)
        self.assertIn("2.0", command)


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
        bot_instance._dismiss_post_login_dialogs = lambda **_kwargs: False  # type: ignore[assignment]

        bot_instance.ensure_ready()

        self.assertEqual(attempted, [True])

    def test_ensure_ready_dismisses_post_login_dialogs_after_page_ready(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        dismissed = []

        bot_instance._find_first = lambda selector_key, **_kwargs: object()  # type: ignore[assignment]
        bot_instance._dismiss_post_login_dialogs = lambda **_kwargs: dismissed.append(True) or True  # type: ignore[assignment]

        bot_instance.ensure_ready()

        self.assertEqual(dismissed, [True])

    def test_dismiss_post_login_dialogs_clicks_got_it_when_visible(self):
        class DummyButton:
            pass

        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance.driver = object()
        button = DummyButton()
        optional_results = iter([button, None])
        clicked = []

        bot_instance._find_optional = lambda selector_key, **_kwargs: next(optional_results) if selector_key == "post_login_got_it_button" else None  # type: ignore[assignment]
        bot_instance._click_element = lambda element: clicked.append(element)  # type: ignore[assignment]

        with patch("angel_web_order_bot.time.sleep", return_value=None):
            self.assertTrue(bot_instance._dismiss_post_login_dialogs())

        self.assertEqual(clicked, [button])

    def test_get_session_snapshot_dismisses_post_login_dialogs_when_ready(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance.driver = SimpleNamespace(
            current_url="https://www.angelone.in/trade/watchlist/chart",
            title="Angel One",
        )
        dismissed = []

        bot_instance._find_first = lambda selector_key, **_kwargs: object()  # type: ignore[assignment]
        bot_instance._dismiss_post_login_dialogs = lambda **_kwargs: dismissed.append(True) or True  # type: ignore[assignment]
        bot_instance._read_cookie_expiry_iso = lambda name: "2026-03-29T12:00:00+00:00" if name == "prod_trade_access_token" else None  # type: ignore[assignment]

        snapshot = bot_instance.get_session_snapshot(open_page=False)

        self.assertTrue(snapshot.page_ready)
        self.assertEqual(snapshot.status, "READY")
        self.assertEqual(dismissed, [True])

    def test_attempt_login_prioritizes_mpin_challenge_over_identifier_fallback(self):
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("login_id=E12345678\nmpin=1234\nlogin_mode=client_id\n")
            credentials_path = Path(handle.name)

        try:
            bot_instance = bot.AngelWebOrderBot(
                {"search_input": {"by": "xpath", "value": "//input"}},
                login_credentials_file=credentials_path,
            )
            bot_instance.driver = SimpleNamespace(
                current_url="https://www.angelone.in/login/",
                title="Angel One Login",
            )

            state = {"page_ready": False}
            fake_identifier_input = object()
            fake_mpin_input = object()
            filled_codes = []
            submitted_steps = []
            identifier_sets = []
            switched_modes = []

            def fake_selector_exists(selector_key, **_kwargs):
                if selector_key == "page_ready":
                    return state["page_ready"]
                return False

            def fake_switch(mode):
                switched_modes.append(mode)
                return True

            bot_instance._selector_exists = fake_selector_exists  # type: ignore[assignment]
            bot_instance._switch_login_identifier_mode = fake_switch  # type: ignore[assignment]
            bot_instance._detect_auth_challenge = lambda: "mpin" if not state["page_ready"] else None  # type: ignore[assignment]
            bot_instance._resolve_auth_inputs = lambda challenge: [fake_mpin_input]  # type: ignore[assignment]
            bot_instance._find_optional = lambda selector_key, **_kwargs: fake_identifier_input if selector_key == "login_identifier_input" else None  # type: ignore[assignment]
            bot_instance._fill_code_elements = lambda inputs, code, **kwargs: filled_codes.append((list(inputs), code, kwargs))  # type: ignore[assignment]
            bot_instance._set_input_value_with_retry = lambda element, value, **kwargs: identifier_sets.append((element, value, kwargs))  # type: ignore[assignment]

            def fake_submit(selector_key, *, source_element=None):
                submitted_steps.append((selector_key, source_element))
                state["page_ready"] = True

            bot_instance._submit_auth_step = fake_submit  # type: ignore[assignment]

            with patch("angel_web_order_bot.time.sleep", return_value=None):
                self.assertTrue(bot_instance._attempt_login_from_files())
        finally:
            credentials_path.unlink(missing_ok=True)

        self.assertEqual(switched_modes, ["client_id"])
        self.assertEqual(
            filled_codes,
            [([fake_mpin_input], "1234", {"typing_delay_seconds": bot.MPIN_TYPING_DELAY_SECONDS})],
        )
        self.assertEqual(submitted_steps, [("mpin_submit_button", fake_mpin_input)])
        self.assertEqual(identifier_sets, [])

    def test_attempt_login_reuses_identifier_like_input_for_mpin_after_otp_when_tabs_absent(self):
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("login_id=E12345678\nmpin=1234\nlogin_mode=client_id\n")
            credentials_path = Path(handle.name)

        try:
            bot_instance = bot.AngelWebOrderBot(
                {"search_input": {"by": "xpath", "value": "//input"}},
                login_credentials_file=credentials_path,
            )
            bot_instance.driver = SimpleNamespace(
                current_url="https://www.angelone.in/login/",
                title="Angel One Login",
            )

            state = {"page_ready": False}
            challenge_sequence = iter(["otp", None, None])
            fake_otp_input = object()
            fake_reused_input = object()
            filled_codes = []
            submitted_steps = []
            identifier_sets = []
            switched_modes = []

            def fake_selector_exists(selector_key, **_kwargs):
                if selector_key == "page_ready":
                    return state["page_ready"]
                return False

            def fake_switch(mode):
                switched_modes.append(mode)
                return True

            def fake_find_optional(selector_key, **_kwargs):
                if selector_key in {"login_client_id_tab", "login_mobile_number_tab"}:
                    return None
                return None

            bot_instance._selector_exists = fake_selector_exists  # type: ignore[assignment]
            bot_instance._switch_login_identifier_mode = fake_switch  # type: ignore[assignment]
            bot_instance._detect_auth_challenge = lambda: next(challenge_sequence) if not state["page_ready"] else None  # type: ignore[assignment]
            bot_instance._read_otp_from_file = lambda _started_at: "654321"  # type: ignore[assignment]
            bot_instance._resolve_auth_inputs = lambda challenge: [fake_otp_input] if challenge == "otp" else []  # type: ignore[assignment]
            bot_instance._find_login_identifier_input = lambda **_kwargs: fake_reused_input if not state["page_ready"] else None  # type: ignore[assignment]
            bot_instance._find_optional = fake_find_optional  # type: ignore[assignment]
            bot_instance._fill_code_elements = lambda inputs, code, **kwargs: filled_codes.append((list(inputs), code, kwargs))  # type: ignore[assignment]
            bot_instance._set_input_value_with_retry = lambda element, value, **kwargs: identifier_sets.append((element, value, kwargs))  # type: ignore[assignment]
            bot_instance._page_mentions_pin_prompt = lambda page_text=None: False  # type: ignore[assignment]

            def fake_submit(selector_key, *, source_element=None):
                submitted_steps.append((selector_key, source_element))
                if selector_key == "mpin_submit_button":
                    state["page_ready"] = True

            bot_instance._submit_auth_step = fake_submit  # type: ignore[assignment]

            with patch("angel_web_order_bot.time.sleep", return_value=None):
                self.assertTrue(bot_instance._attempt_login_from_files())
        finally:
            credentials_path.unlink(missing_ok=True)

        self.assertEqual(switched_modes, ["client_id"])
        self.assertEqual(
            filled_codes,
            [([fake_otp_input], "654321", {"typing_delay_seconds": bot.OTP_TYPING_DELAY_SECONDS})],
        )
        self.assertEqual(
            identifier_sets,
            [(fake_reused_input, "1234", {"typing_delay_seconds": bot.MPIN_TYPING_DELAY_SECONDS})],
        )
        self.assertEqual(
            submitted_steps,
            [("otp_submit_button", fake_otp_input), ("mpin_submit_button", fake_reused_input)],
        )

    def test_find_login_identifier_input_skips_form_fallback_when_disabled(self):
        class FakeElement:
            def is_displayed(self):
                return True

        class FakeDriver:
            def find_elements(self, by, value):
                if value == bot.LOGIN_IDENTIFIER_FORM_FALLBACK_XPATH:
                    return [FakeElement()]
                return []

        bot_instance = bot.AngelWebOrderBot(
            {
                "search_input": {"by": "xpath", "value": "//input"},
                "login_identifier_input": [
                    {"by": "xpath", "value": "//input[@name='client_id']"},
                    {"by": "xpath", "value": bot.LOGIN_IDENTIFIER_FORM_FALLBACK_XPATH},
                ],
            }
        )
        bot_instance.driver = FakeDriver()

        with patch("angel_web_order_bot.time.sleep", return_value=None):
            self.assertIsNone(
                bot_instance._find_login_identifier_input(
                    timeout_seconds=0.3,
                    allow_form_fallback=False,
                )
            )
            self.assertIsNotNone(
                bot_instance._find_login_identifier_input(
                    timeout_seconds=0.3,
                    allow_form_fallback=True,
                )
            )


class TestAuthChallengeDetection(unittest.TestCase):
    def test_infer_login_identifier_mode_distinguishes_mobile_and_client_id(self):
        self.assertEqual(bot.infer_login_identifier_mode("9825944354"), "mobile")
        self.assertEqual(bot.infer_login_identifier_mode("AABM826021"), "client_id")

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

    def test_detect_auth_challenge_uses_generic_inputs_for_four_digit_pin_text(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance._read_visible_page_text = lambda: "welcome back enter your 4-digit pin forgot pin"  # type: ignore[assignment]
        bot_instance._find_generic_auth_inputs = lambda: [object()]  # type: ignore[assignment]

        def fake_find_all(selector_key, **_kwargs):
            if selector_key in {"otp_input", "mpin_input"}:
                return []
            raise AssertionError(selector_key)

        bot_instance._find_all = fake_find_all  # type: ignore[assignment]

        self.assertEqual(bot_instance._detect_auth_challenge(), "mpin")

    def test_detect_auth_challenge_prefers_pin_prompt_over_stale_otp_attributes(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance._read_visible_page_text = lambda: "welcome back enter your 4-digit pin forgot pin"  # type: ignore[assignment]
        bot_instance._find_generic_auth_inputs = lambda: [object()]  # type: ignore[assignment]

        def fake_find_all(selector_key, **_kwargs):
            if selector_key == "otp_input":
                return [object()]
            if selector_key == "mpin_input":
                return []
            raise AssertionError(selector_key)

        bot_instance._find_all = fake_find_all  # type: ignore[assignment]

        self.assertEqual(bot_instance._detect_auth_challenge(), "mpin")

    def test_page_mentions_pin_prompt_detects_angel_pin_copy(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})

        self.assertTrue(bot_instance._page_mentions_pin_prompt("welcome back enter your 4-digit pin forgot pin"))
        self.assertFalse(bot_instance._page_mentions_pin_prompt("enter otp to continue"))

    def test_identifier_screen_requires_mode_reset_after_otp(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})

        self.assertTrue(
            bot_instance._identifier_screen_requires_mode_reset(
                object(),
                "otp",
                "client_id",
                "client_id",
            )
        )
        self.assertFalse(
            bot_instance._identifier_screen_requires_mode_reset(
                object(),
                "identifier",
                "client_id",
                "client_id",
            )
        )


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

    def test_clear_and_type_element_types_character_by_character_when_delay_enabled(self):
        class DummyDriver:
            def execute_script(self, *_args):
                return None

        class DummyElement:
            def __init__(self):
                self.calls = []

            def click(self):
                return None

            def send_keys(self, *args):
                self.calls.append(args)

        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance.driver = DummyDriver()
        element = DummyElement()

        with patch("angel_web_order_bot.time.sleep", return_value=None):
            bot_instance._clear_and_type_element(element, "123", typing_delay_seconds=0.15)

        self.assertEqual(element.calls[-3:], [("1",), ("2",), ("3",)])

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

    def test_fill_code_elements_forwards_typing_delay_for_single_input(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        forwarded = []

        bot_instance._set_input_value_with_retry = lambda element, value, **kwargs: forwarded.append((element, value, kwargs))  # type: ignore[assignment]

        element = object()
        bot_instance._fill_code_elements([element], "1234", typing_delay_seconds=0.15)

        self.assertEqual(
            forwarded,
            [(element, "1234", {"typing_delay_seconds": 0.15})],
        )

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

        def fake_set_input(element, value, **kwargs):
            label = "quantity" if element is elements["quantityOrderPad"] else "price"
            calls.append(("set", label, value, kwargs))

        bot_instance._find_element_by_id = fake_find  # type: ignore[assignment]
        bot_instance._set_input_value_with_retry = fake_set_input  # type: ignore[assignment]
        bot_instance._select_product_button = lambda product: calls.append(("product", product))  # type: ignore[assignment]
        bot_instance._select_order_type_button = lambda order_type: calls.append(("order_type", order_type))  # type: ignore[assignment]

        with patch("angel_web_order_bot.time.sleep") as sleep_mock:
            bot_instance._fill_order_pad_fields(order)

        self.assertIn(("set", "quantity", "5", {}), calls)
        self.assertIn(
            (
                "set",
                "price",
                "6565.0",
                {"typing_delay_seconds": bot.ORDER_PRICE_TYPING_DELAY_SECONDS},
            ),
            calls,
        )
        sleep_mock.assert_called_once_with(3)
        self.assertLess(
            calls.index(("set", "quantity", "5", {})),
            calls.index(
                (
                    "set",
                    "price",
                    "6565.0",
                    {"typing_delay_seconds": bot.ORDER_PRICE_TYPING_DELAY_SECONDS},
                )
            ),
        )


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


class TestFlowGuardrails(unittest.TestCase):
    def test_remaining_timeout_raises_when_deadline_has_expired(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})

        with self.assertRaises(bot.TimeoutException):
            bot_instance._remaining_timeout(
                100.0,
                fallback_seconds=5,
                flow_name="Angel order placement for CASTOR20APR2026",
            )

    def test_attempt_login_stops_after_max_identifier_submits(self):
        with NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("login_id=AABM826021\nmpin=1234\nlogin_mode=client_id\n")
            credentials_path = Path(handle.name)

        try:
            bot_instance = bot.AngelWebOrderBot(
                {"search_input": {"by": "xpath", "value": "//input"}},
                login_credentials_file=credentials_path,
            )
            bot_instance.driver = SimpleNamespace(
                current_url="https://www.angelone.in/login/",
                title="Angel One Login",
            )
            fake_identifier_input = object()
            submit_calls = []

            bot_instance._selector_exists = lambda selector_key, **_kwargs: False  # type: ignore[assignment]
            bot_instance._switch_login_identifier_mode = lambda mode: True  # type: ignore[assignment]
            bot_instance._detect_auth_challenge = lambda: None  # type: ignore[assignment]
            bot_instance._find_login_identifier_input = lambda **_kwargs: fake_identifier_input  # type: ignore[assignment]
            bot_instance._identifier_screen_requires_mode_reset = lambda *args, **kwargs: False  # type: ignore[assignment]
            bot_instance._detect_login_blocker = lambda: None  # type: ignore[assignment]
            bot_instance._set_input_value_with_retry = lambda *args, **kwargs: None  # type: ignore[assignment]
            bot_instance._submit_auth_step = lambda selector_key, *, source_element=None: submit_calls.append((selector_key, source_element))  # type: ignore[assignment]

            fake_now = {"value": 1000.0}

            def advance_time():
                fake_now["value"] += 3.0
                return fake_now["value"]

            with patch("angel_web_order_bot.time.sleep", return_value=None), patch(
                "angel_web_order_bot.time.time",
                side_effect=advance_time,
            ):
                with self.assertRaises(bot.TimeoutException):
                    bot_instance._attempt_login_from_files()
        finally:
            credentials_path.unlink(missing_ok=True)

        self.assertEqual(len(submit_calls), bot.MAX_LOGIN_IDENTIFIER_SUBMIT_ATTEMPTS)

    def test_wait_for_manual_login_times_out_cleanly(self):
        bot_instance = bot.AngelWebOrderBot({"search_input": {"by": "xpath", "value": "//input"}})
        bot_instance._selector_exists = lambda selector_key, **_kwargs: False  # type: ignore[assignment]

        fake_now = {"value": 1000.0}

        def advance_time():
            fake_now["value"] += 2.0
            return fake_now["value"]

        with patch("angel_web_order_bot.time.sleep", return_value=None), patch(
            "angel_web_order_bot.time.time",
            side_effect=advance_time,
        ):
            with self.assertRaises(bot.TimeoutException):
                bot_instance.wait_for_manual_login(timeout_seconds=5)


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
        bot_instance._select_watchlist = lambda index, **kwargs: None  # type: ignore[assignment]
        bot_instance._ensure_watchlist_candidate_present = lambda candidate_arg, watchlist_index, **kwargs: None  # type: ignore[assignment]
        bot_instance._click_watchlist_action = lambda candidate_arg, side, **kwargs: None  # type: ignore[assignment]
        bot_instance._fill_order_pad_fields = lambda order_arg: None  # type: ignore[assignment]
        bot_instance._capture_artifacts = lambda prefix: {}  # type: ignore[assignment]
        bot_instance._prepare_submit_click = lambda: None  # type: ignore[assignment]
        bot_instance._wait_for_submit_ready = lambda *args, **kwargs: submit_button  # type: ignore[assignment]
        bot_instance._wait_for_post_submit_state = lambda **kwargs: "confirm_required" if len(clicked) < 2 else "scheduled_modal"  # type: ignore[assignment]
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

    def test_place_order_auto_adds_missing_watchlist_candidate_before_click(self):
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

        bot_instance.open_target_page = lambda: None  # type: ignore[assignment]
        bot_instance.ensure_ready = lambda allow_manual_login=False: None  # type: ignore[assignment]
        bot_instance._reset_order_entry_state = lambda: None  # type: ignore[assignment]
        bot_instance._select_watchlist = lambda index, **kwargs: None  # type: ignore[assignment]
        bot_instance._fill_order_pad_fields = lambda order_arg: None  # type: ignore[assignment]
        bot_instance._capture_artifacts = lambda prefix: {}  # type: ignore[assignment]
        bot_instance._prepare_submit_click = lambda: None  # type: ignore[assignment]
        bot_instance._wait_for_submit_ready = lambda *args, **kwargs: object()  # type: ignore[assignment]
        bot_instance._wait_for_post_submit_state = lambda **kwargs: "scheduled_modal"  # type: ignore[assignment]
        bot_instance._click = lambda selector_key, timeout_seconds=3: None  # type: ignore[assignment]
        bot_instance._read_optional_message = lambda selector_key, timeout_seconds=1: None  # type: ignore[assignment]
        bot_instance._summarize_ui_message = lambda message: message  # type: ignore[assignment]
        bot_instance._click_element = lambda element: None  # type: ignore[assignment]

        steps = []

        def fake_find_watchlist_row(candidate_arg, **kwargs):
            steps.append(("find", candidate_arg.symbol))
            if steps.count(("find", candidate_arg.symbol)) == 1:
                raise bot.TimeoutException("Visible rows=0")
            return object()

        def fake_add_candidate(candidate_arg, **kwargs):
            steps.append(("add", candidate_arg.symbol))
            return {"status": "added"}

        def fake_close_search():
            steps.append(("close_search", None))

        def fake_click_watchlist_action(candidate_arg, side, **kwargs):
            steps.append(("click", candidate_arg.symbol, side))

        bot_instance._find_watchlist_row = fake_find_watchlist_row  # type: ignore[assignment]
        bot_instance.add_candidate_to_watchlist = fake_add_candidate  # type: ignore[assignment]
        bot_instance._close_watchlist_search = fake_close_search  # type: ignore[assignment]
        bot_instance._click_watchlist_action = fake_click_watchlist_action  # type: ignore[assignment]

        with patch("angel_web_order_bot.time.sleep", return_value=None):
            result = bot_instance.place_order(order, candidate=candidate, watchlist_index=4)

        self.assertEqual(result["status"], "submitted")
        self.assertEqual(
            steps,
            [
                ("find", "CASTOR20APR2026"),
                ("add", "CASTOR20APR2026"),
                ("close_search", None),
                ("find", "CASTOR20APR2026"),
                ("click", "CASTOR20APR2026", "BUY"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
