#!/usr/bin/env python3
"""
Session guard for the standalone Angel browser-trading flow.

This script inspects the existing browser session, reports readiness, and can
refresh the dedicated watchlist. When explicitly requested, it can also drive
the same file-based Angel login flow used by the main browser bot.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import time
from typing import Any, Dict, Optional, Sequence

from angel_web_order_bot import (
    APP_URL,
    DEFAULT_DEBUGGER_ADDRESS,
    DEFAULT_INSTRUMENT_FILE,
    DEFAULT_LOGIN_CREDENTIALS_FILE,
    DEFAULT_LOGIN_OTP_FILE,
    DEFAULT_LOG_DIR,
    DEFAULT_PROFILE_DIR,
    DEFAULT_SELECTORS_PATH,
    AngelWebOrderBot,
    configure_logging,
    load_json_file,
    load_watchlist_candidates,
)


READY_STATUSES = {"READY", "READY_SEEDED"}


def inspect_browser_session(
    *,
    selector_config: Dict[str, Any],
    debugger_address: Optional[str],
    profile_dir: pathlib.Path,
    log_dir: pathlib.Path,
    url: str,
    chrome_binary: Optional[str],
    headless: bool,
    attach_only: bool,
    seed_watchlist: bool,
    attempt_login: bool,
    login_credentials_file: pathlib.Path,
    otp_file: pathlib.Path,
    otp_timeout_seconds: int,
    otp_poll_interval: float,
    instrument_file: pathlib.Path,
    watchlist_index: int,
    min_days_to_expiry: int,
    max_items: int,
) -> Dict[str, Any]:
    bot = AngelWebOrderBot(
        selector_config,
        url=url,
        profile_dir=profile_dir,
        log_dir=log_dir,
        debugger_address=debugger_address,
        headless=headless,
        chrome_binary=chrome_binary,
        attach_only=attach_only,
        keep_open=True,
        login_credentials_file=login_credentials_file,
        otp_file=otp_file,
        otp_timeout_seconds=otp_timeout_seconds,
        otp_poll_interval=otp_poll_interval,
    )

    try:
        with bot:
            snapshot = bot.get_session_snapshot(open_page=True)
            if snapshot.status != "READY" and attempt_login:
                try:
                    bot.ensure_ready(allow_manual_login=False)
                    snapshot = bot.get_session_snapshot(open_page=False)
                except Exception as exc:
                    return {
                        "status": "BROWSER_UNAVAILABLE",
                        "error": {
                            "message": str(exc),
                            "type": exc.__class__.__name__,
                        },
                    }
            result: Dict[str, Any] = {
                "status": snapshot.status,
                "session": snapshot.to_dict(),
            }
            if snapshot.status != "READY":
                return result

            if seed_watchlist:
                candidates = load_watchlist_candidates(
                    instrument_file,
                    min_days_to_expiry=min_days_to_expiry,
                )
                seed_result = bot.seed_watchlist(
                    candidates,
                    watchlist_index=watchlist_index,
                    max_items=max_items,
                    allow_manual_login=False,
                )
                result["status"] = "READY_SEEDED"
                result["watchlist"] = seed_result
            return result
    except Exception as exc:
        return {
            "status": "BROWSER_UNAVAILABLE",
            "error": {
                "message": str(exc),
                "type": exc.__class__.__name__,
            },
        }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect the Angel browser session and refresh the watchlist.")
    parser.add_argument("--selectors", default=str(DEFAULT_SELECTORS_PATH), help="Path to the selector JSON file.")
    parser.add_argument("--profile-dir", default=str(DEFAULT_PROFILE_DIR), help="Persistent Chrome profile directory.")
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR), help="Directory for guard logs and artifacts.")
    parser.add_argument("--url", default=APP_URL, help="Angel trading URL.")
    parser.add_argument("--chrome-binary", default=None, help="Explicit path to Chrome/Chromium.")
    parser.add_argument("--debugger-address", default=DEFAULT_DEBUGGER_ADDRESS, help="Chrome debugger address.")
    parser.add_argument("--headless", action="store_true", help="Only relevant if the bot needs to self-launch Chrome.")
    parser.add_argument("--attach-only", action="store_true", help="Fail instead of launching a new browser session.")
    parser.add_argument("--login-credentials-file", default=str(DEFAULT_LOGIN_CREDENTIALS_FILE), help="Path to the Angel web login credentials file.")
    parser.add_argument("--otp-file", default=str(DEFAULT_LOGIN_OTP_FILE), help="Path to the Angel web OTP file.")
    parser.add_argument("--otp-timeout-seconds", type=int, default=120, help="Seconds to wait for a fresh Angel OTP file update.")
    parser.add_argument("--otp-poll-interval", type=float, default=1.0, help="Seconds between Angel OTP file polls.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose guard logging.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status", help="Return the current browser/session status once.")
    status_parser.add_argument("--attempt-login", action="store_true", help="If login is required, try the configured file-based Angel login flow once.")
    status_parser.add_argument("--seed-watchlist", action="store_true", help="Refresh the dedicated Angel watchlist if the session is ready.")
    status_parser.add_argument("--instrument-file", default=str(DEFAULT_INSTRUMENT_FILE), help="Path to Angel instrument master CSV.")
    status_parser.add_argument("--watchlist-index", type=int, default=4, help="Watchlist tab reserved for bot execution.")
    status_parser.add_argument("--min-days-to-expiry", type=int, default=6, help="Minimum days to expiry for seeded contracts.")
    status_parser.add_argument("--max-items", type=int, default=50, help="Watchlist item ceiling.")

    wait_parser = subparsers.add_parser("wait-ready", help="Poll until the browser session is ready or timeout.")
    wait_parser.add_argument("--attempt-login", action="store_true", help="If login is required, try the configured file-based Angel login flow while polling.")
    wait_parser.add_argument("--seed-watchlist", action="store_true", help="Refresh the watchlist once the session is ready.")
    wait_parser.add_argument("--instrument-file", default=str(DEFAULT_INSTRUMENT_FILE), help="Path to Angel instrument master CSV.")
    wait_parser.add_argument("--watchlist-index", type=int, default=4, help="Watchlist tab reserved for bot execution.")
    wait_parser.add_argument("--min-days-to-expiry", type=int, default=6, help="Minimum days to expiry for seeded contracts.")
    wait_parser.add_argument("--max-items", type=int, default=50, help="Watchlist item ceiling.")
    wait_parser.add_argument("--poll-interval", type=int, default=20, help="Seconds between readiness checks.")
    wait_parser.add_argument("--timeout-seconds", type=int, default=900, help="Maximum wait time before timing out.")
    return parser


def _run_status(args: argparse.Namespace) -> int:
    selector_config = load_json_file(pathlib.Path(args.selectors).expanduser().resolve())
    result = inspect_browser_session(
        selector_config=selector_config,
        debugger_address=(args.debugger_address or "").strip() or None,
        profile_dir=pathlib.Path(args.profile_dir).expanduser().resolve(),
        log_dir=pathlib.Path(args.log_dir).expanduser().resolve(),
        url=args.url,
        chrome_binary=args.chrome_binary,
        headless=args.headless,
        attach_only=args.attach_only,
        seed_watchlist=args.seed_watchlist,
        attempt_login=args.attempt_login,
        login_credentials_file=pathlib.Path(args.login_credentials_file).expanduser().resolve(),
        otp_file=pathlib.Path(args.otp_file).expanduser().resolve(),
        otp_timeout_seconds=args.otp_timeout_seconds,
        otp_poll_interval=args.otp_poll_interval,
        instrument_file=pathlib.Path(args.instrument_file).expanduser().resolve(),
        watchlist_index=args.watchlist_index,
        min_days_to_expiry=args.min_days_to_expiry,
        max_items=args.max_items,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] in READY_STATUSES.union({"LOGIN_REQUIRED"}) else 1


def _run_wait_ready(args: argparse.Namespace) -> int:
    start = time.time()
    while True:
        result = inspect_browser_session(
            selector_config=load_json_file(pathlib.Path(args.selectors).expanduser().resolve()),
            debugger_address=(args.debugger_address or "").strip() or None,
            profile_dir=pathlib.Path(args.profile_dir).expanduser().resolve(),
            log_dir=pathlib.Path(args.log_dir).expanduser().resolve(),
            url=args.url,
            chrome_binary=args.chrome_binary,
            headless=args.headless,
            attach_only=args.attach_only,
            seed_watchlist=args.seed_watchlist,
            attempt_login=args.attempt_login,
            login_credentials_file=pathlib.Path(args.login_credentials_file).expanduser().resolve(),
            otp_file=pathlib.Path(args.otp_file).expanduser().resolve(),
            otp_timeout_seconds=args.otp_timeout_seconds,
            otp_poll_interval=args.otp_poll_interval,
            instrument_file=pathlib.Path(args.instrument_file).expanduser().resolve(),
            watchlist_index=args.watchlist_index,
            min_days_to_expiry=args.min_days_to_expiry,
            max_items=args.max_items,
        )
        if result["status"] in READY_STATUSES:
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0

        if time.time() - start >= args.timeout_seconds:
            result["status"] = "TIMEOUT"
            print(json.dumps(result, indent=2, sort_keys=True))
            return 1

        time.sleep(args.poll_interval)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    configure_logging(pathlib.Path(args.log_dir).expanduser().resolve(), verbose=args.verbose)
    if args.command == "status":
        return _run_status(args)
    if args.command == "wait-ready":
        return _run_wait_ready(args)
    parser.error(f"Unknown command '{args.command}'.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
