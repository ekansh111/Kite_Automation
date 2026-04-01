#!/usr/bin/env python3
"""
Standalone Selenium bot for placing Angel One web orders.

This script is intentionally isolated from the existing webhook/API order
handlers. It supports two workflows:

1. Launch a dedicated Chrome window with remote debugging enabled, log in
   manually, and keep that browser open during market hours.
2. Attach to the same browser later and place orders from a JSON payload.

The page structure on Angel's web app is dynamic and may change. To avoid
hardcoding fragile DOM assumptions into the script, UI selectors live in a
sidecar JSON file that can be tuned without touching the Python flow.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import logging
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from selenium.webdriver import ActionChains
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    SELENIUM_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover - environment dependent
    webdriver = None  # type: ignore[assignment]
    ActionChains = None  # type: ignore[assignment]
    ChromeOptions = None  # type: ignore[assignment]
    Keys = None  # type: ignore[assignment]
    SELENIUM_IMPORT_ERROR = exc

    class TimeoutException(Exception):
        pass

    class WebDriverException(Exception):
        pass

    class _ByFallback:
        CSS_SELECTOR = "css selector"
        XPATH = "xpath"
        ID = "id"
        NAME = "name"
        CLASS_NAME = "class name"
        TAG_NAME = "tag name"
        LINK_TEXT = "link text"
        PARTIAL_LINK_TEXT = "partial link text"

    By = _ByFallback()  # type: ignore[assignment]

try:
    import undetected_chromedriver as uc
except Exception:  # pragma: no cover - dependency may be missing in some envs
    uc = None


APP_URL = "https://www.angelone.in/trade/watchlist/chart"
ROOT_DIR = pathlib.Path(__file__).resolve().parent
DEFAULT_SELECTORS_PATH = ROOT_DIR / "angel_web_order_bot_selectors.json"
DEFAULT_ORDER_PATH = ROOT_DIR / "angel_web_order_bot_order.example.json"
DEFAULT_INSTRUMENT_FILE = ROOT_DIR / "AngelInstrumentDetails.csv"
DEFAULT_PROFILE_DIR = ROOT_DIR / "angel_web_bot_profile"
DEFAULT_LOG_DIR = ROOT_DIR / "logs" / "angel_web_bot"
DEFAULT_DEBUGGER_ADDRESS = "127.0.0.1:9222"
DEFAULT_LOGIN_CREDENTIALS_FILE = ROOT_DIR / "angel_web_login_credentials.txt"
DEFAULT_LOGIN_OTP_FILE = ROOT_DIR / "angel_web_login_otp.txt"
DEFAULT_LOGIN_OTP_FETCH_SCRIPT = ROOT_DIR / "fetch_broker_email_otp.py"
DEFAULT_LOGIN_OTP_FETCH_WAIT_TIMEOUT_SECONDS = 30.0
DEFAULT_LOGIN_OTP_FETCH_POLL_INTERVAL_SECONDS = 2.0

BY_MAP = {
    "css": By.CSS_SELECTOR,
    "css selector": By.CSS_SELECTOR,
    "xpath": By.XPATH,
    "id": By.ID,
    "name": By.NAME,
    "class": By.CLASS_NAME,
    "class name": By.CLASS_NAME,
    "tag": By.TAG_NAME,
    "tag name": By.TAG_NAME,
    "link text": By.LINK_TEXT,
    "partial link text": By.PARTIAL_LINK_TEXT,
}

CHROME_CANDIDATES = (
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    "google-chrome",
    "google-chrome-stable",
    "chromium",
    "chromium-browser",
    "chrome",
)

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOGGER = logging.getLogger("angel_web_order_bot")
LOGIN_IDENTIFIER_FILL_DELAY_SECONDS = 1
OTP_FILL_DELAY_SECONDS = 2
MPIN_FILL_DELAY_SECONDS = 1
LOGIN_IDENTIFIER_TYPING_DELAY_SECONDS = 0.15
OTP_TYPING_DELAY_SECONDS = 0.15
MPIN_TYPING_DELAY_SECONDS = 0.15
AUTH_SUBMIT_BUTTON_DELAY_SECONDS = 1
ORDER_PRICE_SETTLE_DELAY_SECONDS = 3
ORDER_PRICE_TYPING_DELAY_SECONDS = 0.35
ORDER_SUBMIT_BUTTON_DELAY_SECONDS = 1
ORDER_CONFIRM_BUTTON_DELAY_SECONDS = 1
FILE_LOGIN_HARD_TIMEOUT_SECONDS = 180
WATCHLIST_ADD_HARD_TIMEOUT_SECONDS = 45
ORDER_PLACEMENT_HARD_TIMEOUT_SECONDS = 150
MANUAL_LOGIN_HARD_TIMEOUT_SECONDS = 300
MAX_LOGIN_IDENTIFIER_SUBMIT_ATTEMPTS = 3
LOGIN_DEVICE_BLOCKER_PATTERNS = (
    "unable to authenticate you with this device",
)
LOGIN_IDENTIFIER_FORM_FALLBACK_XPATH = "//form//input[not(@type='hidden')][1]"
PIN_PROMPT_KEYWORDS = (
    "mpin",
    "m-pin",
    "m pin",
    "4-digit pin",
    "4 digit pin",
    "enter your pin",
    "enter your 4-digit pin",
    "forgot pin",
)
CHROME_COMMON_ARGS = (
    "--start-maximized",
    "--disable-notifications",
    "--disable-popup-blocking",
    "--disable-features=PasswordManagerOnboarding,PasswordCheck,AutofillServerCommunication,PasswordsImport,PasswordManagerRedesign,FillOnAccountSelect",
)
CHROME_PROFILE_PREFERENCES = {
    "credentials_enable_service": False,
    "autofill": {
        "credit_card_enabled": False,
        "profile_enabled": False,
    },
    "profile": {
        "password_manager_enabled": False,
        "password_manager_leak_detection": False,
    },
}

PRODUCT_BUTTON_IDS = {
    "INTRADAY": "productType1",
    "INT": "productType1",
    "MIS": "productType1",
    "CARRYFORWARD": "productType2",
    "CF": "productType2",
    "NRML": "productType2",
}

ORDER_TYPE_BUTTON_IDS = {
    "LIMIT": "limtToggleButton",
    "MARKET": "marketToggleButton",
}


@dataclass(frozen=True)
class OrderRequest:
    exchange: str
    symbol: str
    side: str
    quantity: int
    product: str
    order_type: str
    validity: str = "DAY"
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    submit_live: bool = False
    allow_manual_login: bool = False

    def to_log_dict(self) -> Dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "product": self.product,
            "order_type": self.order_type,
            "validity": self.validity,
            "price": self.price,
            "trigger_price": self.trigger_price,
            "submit_live": self.submit_live,
            "allow_manual_login": self.allow_manual_login,
        }


@dataclass(frozen=True)
class WatchlistCandidate:
    symbol: str
    name: str
    exchange: str
    expiry_label: str
    expiry_date: datetime

    def key(self) -> Tuple[str, str, str]:
        return (self.name.upper(), self.exchange.upper(), self.expiry_label.upper())

    def to_log_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "exchange": self.exchange,
            "expiry_label": self.expiry_label,
            "expiry_date": self.expiry_date.strftime("%Y-%m-%d"),
        }


@dataclass(frozen=True)
class SessionSnapshot:
    status: str
    current_url: str
    title: str
    page_ready: bool
    login_required: bool
    trade_access_token_expiry: Optional[str]
    non_trade_access_token_expiry: Optional[str]
    checked_at: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "current_url": self.current_url,
            "title": self.title,
            "page_ready": self.page_ready,
            "login_required": self.login_required,
            "trade_access_token_expiry": self.trade_access_token_expiry,
            "non_trade_access_token_expiry": self.non_trade_access_token_expiry,
            "checked_at": self.checked_at,
        }


def configure_logging(log_dir: pathlib.Path, verbose: bool = False) -> pathlib.Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    handlers: List[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_path),
    ]
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=LOG_FORMAT,
        handlers=handlers,
        force=True,
    )
    return log_path


def _iter_chrome_binaries() -> Iterable[str]:
    for candidate in CHROME_CANDIDATES:
        expanded_candidate = os.path.expandvars(candidate)
        if os.path.isabs(expanded_candidate):
            if os.path.exists(expanded_candidate):
                yield expanded_candidate
            continue
        if candidate.startswith("/"):
            yield candidate
            continue
        resolved = shutil.which(candidate)
        if resolved:
            yield resolved


def resolve_chrome_binary(explicit_binary: Optional[str] = None) -> str:
    if explicit_binary:
        return explicit_binary

    for binary in _iter_chrome_binaries():
        return binary

    raise FileNotFoundError(
        "Unable to locate a Chrome/Chromium binary. Pass --chrome-binary explicitly."
    )


def detect_chrome_major_version(explicit_binary: Optional[str] = None) -> Optional[int]:
    binary = resolve_chrome_binary(explicit_binary)
    try:
        result = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.SubprocessError, OSError):
        return None

    version_output = (result.stdout or result.stderr or "").strip()
    chunks = version_output.split()
    for token in chunks:
        if token and token[0].isdigit() and "." in token:
            try:
                return int(token.split(".", 1)[0])
            except ValueError:
                continue
    return None


def _deep_merge_mapping(base: Dict[str, Any], updates: Mapping[str, Any]) -> Dict[str, Any]:
    for key, value in updates.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            _deep_merge_mapping(base[key], value)
        else:
            base[key] = copy.deepcopy(value)
    return base


def configure_chrome_profile_preferences(profile_dir: pathlib.Path) -> pathlib.Path:
    profile_dir = pathlib.Path(profile_dir).expanduser().resolve()
    default_profile_dir = profile_dir / "Default"
    default_profile_dir.mkdir(parents=True, exist_ok=True)
    preferences_path = default_profile_dir / "Preferences"

    existing_preferences: Dict[str, Any] = {}
    if preferences_path.exists():
        try:
            loaded = json.loads(preferences_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                existing_preferences = loaded
        except (OSError, json.JSONDecodeError):
            LOGGER.warning(
                "Could not parse Chrome profile preferences; rewriting managed preferences | path=%s",
                preferences_path,
            )

    merged_preferences = _deep_merge_mapping(existing_preferences, CHROME_PROFILE_PREFERENCES)
    preferences_path.write_text(
        json.dumps(merged_preferences, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return preferences_path


def parse_debugger_address(debugger_address: str) -> Tuple[str, int]:
    raw_value = str(debugger_address or "").strip()
    host, separator, port_text = raw_value.rpartition(":")
    if not separator or not host or not port_text:
        raise ValueError(
            f"Invalid debugger address '{debugger_address}'. Expected format host:port."
        )

    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(
            f"Invalid debugger port in '{debugger_address}'. Expected an integer port."
        ) from exc

    if port <= 0 or port > 65535:
        raise ValueError(
            f"Invalid debugger port in '{debugger_address}'. Expected 1-65535."
        )

    return host, port


def wait_for_debugger_address(
    debugger_address: str,
    *,
    timeout_seconds: float = 15.0,
    poll_interval_seconds: float = 0.25,
) -> bool:
    host, port = parse_debugger_address(debugger_address)
    deadline = time.time() + max(timeout_seconds, 0.0)
    while time.time() <= deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(max(poll_interval_seconds, 0.05))
    return False


def launch_debugger_chrome_session(
    *,
    chrome_binary: str,
    profile_dir: pathlib.Path,
    debugger_address: str,
    url: str = APP_URL,
) -> subprocess.Popen:
    profile_dir = pathlib.Path(profile_dir).expanduser().resolve()
    profile_dir.mkdir(parents=True, exist_ok=True)
    configure_chrome_profile_preferences(profile_dir)
    host, port = parse_debugger_address(debugger_address)

    command = [
        chrome_binary,
        f"--remote-debugging-address={host}",
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    command.extend(CHROME_COMMON_ARGS)
    command.extend([
        "--new-window",
        url,
    ])

    LOGGER.info(
        "Launching dedicated Angel browser | chrome_binary=%s profile_dir=%s debugger_address=%s url=%s",
        chrome_binary,
        profile_dir,
        debugger_address,
        url,
    )
    return subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def load_json_file(path: pathlib.Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _normalize_login_credential_key(key: str) -> str:
    normalized = str(key or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "clientid": "client_id",
        "client_id": "client_id",
        "userid": "client_id",
        "user_id": "client_id",
        "loginid": "login_id",
        "login_id": "login_id",
        "email": "login_id",
        "mobile": "mobile_number",
        "mobile_number": "mobile_number",
        "phone": "mobile_number",
        "phone_number": "mobile_number",
        "number": "mobile_number",
        "identifier": "login_id",
        "loginmode": "login_mode",
        "login_mode": "login_mode",
        "mode": "login_mode",
        "mpin": "mpin",
        "pin": "mpin",
        "password": "mpin",
    }
    return aliases.get(normalized, normalized)


def _extract_login_code(raw_text: str) -> Optional[str]:
    text = str(raw_text or "").strip()
    if not text:
        return None

    digits = re.sub(r"\D", "", text)
    if 4 <= len(digits) <= 8:
        return digits

    match = re.search(r"\b(\d{4,8})\b", text)
    if match:
        return match.group(1)

    return None


def normalize_login_mode(raw_value: Optional[str]) -> Optional[str]:
    normalized = str(raw_value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "client": "client_id",
        "clientid": "client_id",
        "client_id": "client_id",
        "userid": "client_id",
        "user_id": "client_id",
        "mobile": "mobile",
        "mobile_number": "mobile",
        "phone": "mobile",
        "phone_number": "mobile",
    }
    resolved = aliases.get(normalized, normalized)
    return resolved if resolved in {"client_id", "mobile"} else None


def infer_login_identifier_mode(identifier: str) -> str:
    text = str(identifier or "").strip()
    digits = re.sub(r"\D", "", text)
    if digits == text and len(digits) == 10:
        return "mobile"
    return "client_id"


def extract_login_blocker_message(page_text: str) -> Optional[str]:
    normalized = " ".join(str(page_text or "").split()).lower()
    for pattern in LOGIN_DEVICE_BLOCKER_PATTERNS:
        if pattern in normalized:
            return pattern
    return None


def load_login_credentials_file(path: pathlib.Path) -> Dict[str, str]:
    raw_text = path.read_text(encoding="utf-8")
    cleaned = raw_text.strip()
    if not cleaned:
        raise ValueError(f"Angel login credentials file is empty: {path}")

    values: Dict[str, str] = {}

    if cleaned.startswith("{"):
        raw = json.loads(cleaned)
        if not isinstance(raw, Mapping):
            raise ValueError(f"Angel login credentials JSON must be an object: {path}")
        for key, value in raw.items():
            normalized_key = _normalize_login_credential_key(str(key))
            if value in (None, ""):
                continue
            values[normalized_key] = str(value).strip()
    else:
        positional: List[str] = []
        for line in raw_text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" in stripped:
                key, value = stripped.split("=", 1)
                normalized_key = _normalize_login_credential_key(key)
                values[normalized_key] = value.strip()
            else:
                positional.append(stripped)

        if positional:
            values.setdefault("login_id", positional[0])
        if len(positional) > 1:
            values.setdefault("mpin", positional[1])

    login_id = str(values.get("login_id", "")).strip()
    client_id = str(values.get("client_id", "")).strip()
    mobile_number = str(values.get("mobile_number", "")).strip()
    mpin = str(values.get("mpin", "")).strip()
    login_mode = normalize_login_mode(values.get("login_mode"))

    if not login_id:
        login_id = client_id or mobile_number

    if not login_id:
        raise ValueError(
            f"Angel login credentials file must include a login identifier: {path}"
        )

    if not client_id and infer_login_identifier_mode(login_id) == "client_id":
        client_id = login_id
    if not mobile_number and infer_login_identifier_mode(login_id) == "mobile":
        mobile_number = login_id

    return {
        "login_id": login_id,
        "client_id": client_id,
        "mobile_number": mobile_number,
        "mpin": mpin,
        "login_mode": login_mode or "",
    }


def normalize_selector_config(raw: Mapping[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    normalized: Dict[str, List[Dict[str, str]]] = {}
    for key, value in raw.items():
        if isinstance(value, Mapping):
            candidates = [dict(value)]
        elif isinstance(value, list):
            candidates = [dict(item) for item in value]
        else:
            raise ValueError(f"Selector group '{key}' must be an object or list.")

        normalized[key] = []
        for candidate in candidates:
            by_value = str(candidate.get("by", "")).strip().lower()
            locator_value = str(candidate.get("value", "")).strip()
            if by_value not in BY_MAP:
                raise ValueError(f"Unsupported locator strategy '{by_value}' in '{key}'.")
            if not locator_value:
                raise ValueError(f"Selector '{key}' contains an empty locator value.")
            normalized[key].append({"by": by_value, "value": locator_value})

    return normalized


def normalize_order_payload(raw: Mapping[str, Any], submit_live_override: Optional[bool] = None) -> OrderRequest:
    def _read_text(key: str, default: Optional[str] = None, *, required: bool = False) -> str:
        value = raw.get(key, default)
        if value is None:
            if required:
                raise ValueError(f"Missing required field '{key}'.")
            return ""
        return str(value).strip()

    def _read_number(key: str) -> Optional[float]:
        value = raw.get(key)
        if value in (None, "", "null"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Field '{key}' must be numeric.") from exc

    exchange = _read_text("exchange", required=True).upper()
    symbol = _read_text("symbol", required=True).upper().replace(" ", "")
    side = _read_text("side", required=True).upper()
    product = _read_text("product", required=True).upper()
    order_type = _read_text("order_type", required=True).upper()
    validity = _read_text("validity", "DAY").upper()

    quantity_raw = raw.get("quantity")
    try:
        quantity = int(quantity_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("Field 'quantity' must be an integer.") from exc

    if quantity <= 0:
        raise ValueError("Field 'quantity' must be greater than zero.")
    if side not in {"BUY", "SELL"}:
        raise ValueError("Field 'side' must be BUY or SELL.")
    if order_type not in {"MARKET", "LIMIT", "SL", "SL-M"}:
        raise ValueError("Field 'order_type' must be MARKET, LIMIT, SL, or SL-M.")

    price = _read_number("price")
    trigger_price = _read_number("trigger_price")

    if order_type == "LIMIT" and price is None:
        raise ValueError("LIMIT orders require 'price'.")
    if order_type in {"SL", "SL-M"} and trigger_price is None:
        raise ValueError(f"{order_type} orders require 'trigger_price'.")
    if order_type == "SL" and price is None:
        raise ValueError("SL orders require both 'price' and 'trigger_price'.")
    if order_type == "MARKET":
        price = None

    raw_submit_live = bool(raw.get("submit_live", False))
    if submit_live_override is None:
        submit_live = raw_submit_live
    else:
        # Live submission requires both an explicit CLI gate and a payload that
        # was intentionally marked for submission.
        submit_live = raw_submit_live and bool(submit_live_override)
    allow_manual_login = bool(raw.get("allow_manual_login", False))

    return OrderRequest(
        exchange=exchange,
        symbol=symbol,
        side=side,
        quantity=quantity,
        product=product,
        order_type=order_type,
        validity=validity,
        price=price,
        trigger_price=trigger_price,
        submit_live=submit_live,
        allow_manual_login=allow_manual_login,
    )


def _parse_expiry_label(expiry_text: str) -> Optional[datetime]:
    clean = (expiry_text or "").strip()
    if not clean or clean.lower() == "nan":
        return None

    for fmt in ("%d%b%Y", "%d %b %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(clean.title(), fmt)
        except ValueError:
            continue
    return None


def load_watchlist_candidates(
    instrument_file: pathlib.Path,
    *,
    exchange: str = "NCDEX",
    instrument_type: str = "FUTCOM",
    min_days_to_expiry: int = 6,
    as_of: Optional[datetime] = None,
) -> List[WatchlistCandidate]:
    cutoff = (as_of or datetime.now()).date().toordinal() + int(min_days_to_expiry)
    candidates: Dict[Tuple[str, str], WatchlistCandidate] = {}

    with instrument_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if str(row.get("exch_seg", "")).strip().upper() != exchange.upper():
                continue
            if str(row.get("instrumenttype", "")).strip().upper() != instrument_type.upper():
                continue

            expiry_dt = _parse_expiry_label(str(row.get("expiry", "")))
            if expiry_dt is None:
                continue
            if expiry_dt.date().toordinal() < cutoff:
                continue

            symbol = str(row.get("symbol", "")).strip().upper().replace(" ", "")
            name = str(row.get("name", "")).strip().upper()
            expiry_label = expiry_dt.strftime("%d %b %Y")
            dedupe_key = (name, expiry_label)

            if not symbol or not name:
                continue
            if dedupe_key in candidates:
                continue

            candidates[dedupe_key] = WatchlistCandidate(
                symbol=symbol,
                name=name,
                exchange=exchange.upper(),
                expiry_label=expiry_label,
                expiry_date=expiry_dt,
            )

    return sorted(
        candidates.values(),
        key=lambda item: (item.expiry_date, item.name, item.symbol),
    )


def resolve_watchlist_candidate(
    instrument_file: pathlib.Path,
    symbol: str,
    *,
    exchange: str = "NCDEX",
    instrument_type: str = "FUTCOM",
) -> WatchlistCandidate:
    target_symbol = str(symbol).strip().upper().replace(" ", "")
    if not target_symbol:
        raise ValueError("Symbol is required to resolve a watchlist candidate.")

    with instrument_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if str(row.get("exch_seg", "")).strip().upper() != exchange.upper():
                continue
            if str(row.get("instrumenttype", "")).strip().upper() != instrument_type.upper():
                continue

            row_symbol = str(row.get("symbol", "")).strip().upper().replace(" ", "")
            if row_symbol != target_symbol:
                continue

            expiry_dt = _parse_expiry_label(str(row.get("expiry", "")))
            name = str(row.get("name", "")).strip().upper()
            if expiry_dt is None or not name:
                break

            return WatchlistCandidate(
                symbol=row_symbol,
                name=name,
                exchange=exchange.upper(),
                expiry_label=expiry_dt.strftime("%d %b %Y"),
                expiry_date=expiry_dt,
            )

    raise ValueError(
        f"Could not resolve symbol '{target_symbol}' in {instrument_file} "
        f"for {exchange}/{instrument_type}."
    )


class AngelWebOrderBot:
    def __init__(
        self,
        selector_config: Mapping[str, Sequence[Mapping[str, str]]],
        *,
        url: str = APP_URL,
        profile_dir: pathlib.Path = DEFAULT_PROFILE_DIR,
        log_dir: pathlib.Path = DEFAULT_LOG_DIR,
        debugger_address: Optional[str] = None,
        headless: bool = False,
        chrome_binary: Optional[str] = None,
        attach_only: bool = False,
        keep_open: bool = False,
        timeout_seconds: int = 20,
        login_credentials_file: Optional[pathlib.Path] = None,
        otp_file: Optional[pathlib.Path] = None,
        otp_timeout_seconds: int = 120,
        otp_poll_interval: float = 1.0,
    ) -> None:
        self.url = url
        self.selector_config = normalize_selector_config(selector_config)
        self.profile_dir = profile_dir
        self.log_dir = log_dir
        self.debugger_address = debugger_address
        self.headless = headless
        self.chrome_binary = chrome_binary
        self.attach_only = attach_only
        self.keep_open = keep_open
        self.timeout_seconds = timeout_seconds
        self.login_credentials_file = (
            login_credentials_file
            if login_credentials_file is not None
            else pathlib.Path(
                os.environ.get("ANGEL_WEB_LOGIN_CREDENTIALS_PATH", str(DEFAULT_LOGIN_CREDENTIALS_FILE))
            ).expanduser().resolve()
        )
        self.otp_file = (
            otp_file
            if otp_file is not None
            else pathlib.Path(
                os.environ.get("ANGEL_WEB_LOGIN_OTP_PATH", str(DEFAULT_LOGIN_OTP_FILE))
            ).expanduser().resolve()
        )
        self.otp_timeout_seconds = max(int(otp_timeout_seconds), 1)
        self.otp_poll_interval = max(float(otp_poll_interval), 0.25)
        otp_fetch_script_raw = os.environ.get(
            "ANGEL_WEB_OTP_FETCH_SCRIPT_PATH",
            str(DEFAULT_LOGIN_OTP_FETCH_SCRIPT),
        )
        self.otp_fetch_script = pathlib.Path(otp_fetch_script_raw).expanduser().resolve()
        self.otp_fetch_wait_timeout_seconds = max(
            float(
                os.environ.get(
                    "ANGEL_WEB_OTP_FETCH_WAIT_TIMEOUT_SECONDS",
                    str(DEFAULT_LOGIN_OTP_FETCH_WAIT_TIMEOUT_SECONDS),
                )
            ),
            0.0,
        )
        self.otp_fetch_poll_interval_seconds = max(
            float(
                os.environ.get(
                    "ANGEL_WEB_OTP_FETCH_POLL_INTERVAL_SECONDS",
                    str(DEFAULT_LOGIN_OTP_FETCH_POLL_INTERVAL_SECONDS),
                )
            ),
            0.25,
        )
        self._otp_fetch_process = None
        self.driver: Optional[webdriver.Chrome] = None
        self.artifact_dir = self.log_dir / datetime.now().strftime("%Y%m%d")
        self.artifact_dir.mkdir(parents=True, exist_ok=True)

    def _build_debugger_attach_options(self) -> ChromeOptions:
        if ChromeOptions is None:
            raise RuntimeError(
                "selenium is not installed in this environment."
            ) from SELENIUM_IMPORT_ERROR

        options = ChromeOptions()
        options.add_experimental_option("debuggerAddress", self.debugger_address)
        if self.chrome_binary:
            options.binary_location = resolve_chrome_binary(self.chrome_binary)
        return options

    def _attach_to_debugger_session(self) -> webdriver.Chrome:
        if webdriver is None:
            raise RuntimeError(
                "selenium is not installed in this environment."
            ) from SELENIUM_IMPORT_ERROR
        return webdriver.Chrome(options=self._build_debugger_attach_options())

    def _build_driver(self) -> webdriver.Chrome:
        if webdriver is None or ChromeOptions is None:
            raise RuntimeError(
                "selenium is not installed in this environment."
            ) from SELENIUM_IMPORT_ERROR

        if self.debugger_address:
            LOGGER.info("Attaching to existing Chrome session at %s", self.debugger_address)
            try:
                debugger_ready = wait_for_debugger_address(
                    self.debugger_address,
                    timeout_seconds=1.0,
                    poll_interval_seconds=0.25,
                )
            except ValueError:
                raise

            if not debugger_ready:
                if self.attach_only:
                    raise RuntimeError(
                        f"Debugger session {self.debugger_address} is unavailable."
                    )
                chrome_binary = resolve_chrome_binary(self.chrome_binary)
                LOGGER.info(
                    "Debugger session %s is not reachable yet. Launching dedicated Chrome for that debugger address.",
                    self.debugger_address,
                )
                launch_debugger_chrome_session(
                    chrome_binary=chrome_binary,
                    profile_dir=self.profile_dir,
                    debugger_address=self.debugger_address,
                    url=self.url,
                )
                if wait_for_debugger_address(self.debugger_address, timeout_seconds=15.0):
                    try:
                        return self._attach_to_debugger_session()
                    except Exception:
                        LOGGER.warning(
                            "Auto-launched debugger browser at %s did not become attachable. Falling back to a dedicated browser.",
                            self.debugger_address,
                            exc_info=True,
                        )
                else:
                    LOGGER.warning(
                        "Auto-launched debugger browser at %s did not open a reachable debugger endpoint. Falling back to a dedicated browser.",
                        self.debugger_address,
                    )

            try:
                return self._attach_to_debugger_session()
            except Exception:
                if self.attach_only:
                    raise
                LOGGER.warning(
                    "Debugger session %s is reachable but Selenium could not attach. Falling back to a dedicated browser.",
                    self.debugger_address,
                    exc_info=True,
                )

        if self.attach_only:
            raise RuntimeError(
                "Attach-only mode requested without a debugger address."
            )

        if uc is None:
            raise RuntimeError(
                "undetected_chromedriver is not available. Install it or use --debugger-address."
            )

        try:
            import distutils_compat  # noqa: F401
        except Exception:
            LOGGER.info("distutils compatibility shim could not be loaded; continuing without it.")

        version_main = detect_chrome_major_version(self.chrome_binary)
        options = uc.ChromeOptions()
        configure_chrome_profile_preferences(self.profile_dir)
        options.add_argument(f"--user-data-dir={self.profile_dir}")
        for arg in CHROME_COMMON_ARGS:
            options.add_argument(arg)
        options.add_experimental_option("prefs", CHROME_PROFILE_PREFERENCES)
        if self.headless:
            options.headless = True

        if self.chrome_binary:
            options.binary_location = self.chrome_binary

        LOGGER.info(
            "Launching dedicated Chrome session | profile_dir=%s headless=%s version_main=%s",
            self.profile_dir,
            self.headless,
            version_main,
        )
        if version_main is not None:
            return uc.Chrome(options=options, version_main=version_main)
        return uc.Chrome(options=options)

    def __enter__(self) -> "AngelWebOrderBot":
        self.driver = self._build_driver()
        self.driver.implicitly_wait(0)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.keep_open:
            LOGGER.info("Keeping browser open by request.")
            return
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                LOGGER.exception("Error while closing browser")

    def _build_flow_deadline(self, timeout_seconds: float) -> float:
        return time.time() + max(float(timeout_seconds), 0.0)

    def _remaining_timeout(
        self,
        deadline: Optional[float],
        *,
        fallback_seconds: float,
        flow_name: str,
        minimum_seconds: float = 0.25,
    ) -> float:
        fallback = max(float(fallback_seconds), minimum_seconds)
        if deadline is None:
            return fallback

        remaining = deadline - time.time()
        if remaining <= 0:
            raise TimeoutException(f"{flow_name} exceeded hard timeout.")

        return max(minimum_seconds, min(fallback, remaining))

    def _render_locator(self, locator: Mapping[str, str], context: Optional[Mapping[str, Any]] = None) -> Tuple[str, str]:
        context = context or {}
        by = BY_MAP[locator["by"]]
        value = locator["value"].format(**context)
        return by, value

    def _find_first(
        self,
        selector_key: str,
        *,
        context: Optional[Mapping[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
        displayed_only: bool = True,
    ):
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        timeout = timeout_seconds or self.timeout_seconds
        deadline = time.time() + timeout
        candidates = self.selector_config.get(selector_key, [])
        last_error: Optional[Exception] = None

        if not candidates:
            raise KeyError(f"No selectors configured for '{selector_key}'.")

        while time.time() < deadline:
            for locator in candidates:
                try:
                    by, value = self._render_locator(locator, context)
                    elements = self.driver.find_elements(by, value)
                    for element in elements:
                        if not displayed_only or element.is_displayed():
                            return element
                except WebDriverException as exc:
                    last_error = exc
            time.sleep(0.25)

        raise TimeoutException(
            f"Timed out waiting for selector '{selector_key}'. Last error: {last_error}"
        )

    def _find_all(
        self,
        selector_key: str,
        *,
        context: Optional[Mapping[str, Any]] = None,
        displayed_only: bool = True,
    ) -> List[Any]:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        candidates = self.selector_config.get(selector_key, [])
        if not candidates:
            raise KeyError(f"No selectors configured for '{selector_key}'.")

        elements: List[Any] = []
        seen_ids = set()
        for locator in candidates:
            try:
                by, value = self._render_locator(locator, context)
                for element in self.driver.find_elements(by, value):
                    if displayed_only and not element.is_displayed():
                        continue
                    element_id = getattr(element, "id", None) or id(element)
                    if element_id in seen_ids:
                        continue
                    seen_ids.add(element_id)
                    elements.append(element)
            except WebDriverException:
                continue

        return elements

    def _find_login_identifier_input(
        self,
        *,
        context: Optional[Mapping[str, Any]] = None,
        timeout_seconds: int = 1,
        displayed_only: bool = True,
        allow_form_fallback: bool = True,
    ):
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        timeout = timeout_seconds or self.timeout_seconds
        deadline = time.time() + timeout
        candidates = list(self.selector_config.get("login_identifier_input", []))
        last_error: Optional[Exception] = None

        if not allow_form_fallback:
            candidates = [
                locator
                for locator in candidates
                if locator.get("value") != LOGIN_IDENTIFIER_FORM_FALLBACK_XPATH
            ]

        if not candidates:
            return None

        while time.time() < deadline:
            for locator in candidates:
                try:
                    by, value = self._render_locator(locator, context)
                    elements = self.driver.find_elements(by, value)
                    for element in elements:
                        if not displayed_only or element.is_displayed():
                            return element
                except WebDriverException as exc:
                    last_error = exc
            time.sleep(0.25)

        if last_error is not None:
            LOGGER.debug("Identifier input lookup did not succeed before timeout | error=%s", last_error)
        return None

    def _find_optional(
        self,
        selector_key: str,
        *,
        context: Optional[Mapping[str, Any]] = None,
        timeout_seconds: int = 1,
        displayed_only: bool = True,
    ):
        try:
            return self._find_first(
                selector_key,
                context=context,
                timeout_seconds=timeout_seconds,
                displayed_only=displayed_only,
            )
        except (TimeoutException, KeyError):
            return None

    def _selector_exists(
        self,
        selector_key: str,
        *,
        context: Optional[Mapping[str, Any]] = None,
        timeout_seconds: int = 1,
        displayed_only: bool = True,
    ) -> bool:
        return self._find_optional(
            selector_key,
            context=context,
            timeout_seconds=timeout_seconds,
            displayed_only=displayed_only,
        ) is not None

    def _read_otp_from_file(self, wait_started_at: float) -> str:
        deadline = time.time() + self.otp_timeout_seconds
        saw_otp_file = False
        saw_candidate_code = False
        fetcher_attempted = False

        while time.time() < deadline:
            if self.otp_file.exists():
                saw_otp_file = True
                raw_text = self.otp_file.read_text(encoding="utf-8").strip()
                code = _extract_login_code(raw_text)
                if code:
                    saw_candidate_code = True
                    modified_at = self.otp_file.stat().st_mtime
                    if modified_at >= wait_started_at:
                        try:
                            self.otp_file.write_text("", encoding="utf-8")
                        except OSError:
                            LOGGER.warning("Could not clear Angel OTP file after reading | path=%s", self.otp_file)
                        LOGGER.info("Read Angel OTP from file | path=%s", self.otp_file)
                        return code
            if not fetcher_attempted:
                self._start_otp_fetcher()
                fetcher_attempted = True
            time.sleep(self.otp_poll_interval)

        raise TimeoutException(
            f"Timed out waiting for a fresh Angel OTP in {self.otp_file}. "
            f"otp_file_present={saw_otp_file} candidate_code_seen={saw_candidate_code}"
        )

    def _start_otp_fetcher(self) -> bool:
        if not self.otp_fetch_script.exists():
            LOGGER.info(
                "Angel OTP fetcher script was not found; continuing without auto-fetch | script=%s",
                self.otp_fetch_script,
            )
            return False

        if self._otp_fetch_process is not None and self._otp_fetch_process.poll() is None:
            LOGGER.info("Angel OTP fetcher is already running.")
            return True

        command = [
            sys.executable,
            str(self.otp_fetch_script),
            "--output-file",
            str(self.otp_file),
            "--wait-timeout-seconds",
            str(self.otp_fetch_wait_timeout_seconds),
            "--poll-interval-seconds",
            str(self.otp_fetch_poll_interval_seconds),
        ]

        LOGGER.info(
            "Starting Angel OTP fetcher | script=%s output_file=%s wait_timeout_seconds=%s poll_interval_seconds=%s",
            self.otp_fetch_script,
            self.otp_file,
            self.otp_fetch_wait_timeout_seconds,
            self.otp_fetch_poll_interval_seconds,
        )

        try:
            self._otp_fetch_process = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            return True
        except (FileNotFoundError, OSError, subprocess.SubprocessError):
            LOGGER.warning("Could not start Angel OTP fetcher.", exc_info=True)
            self._otp_fetch_process = None
            return False

    def _fill_code_elements(
        self,
        inputs: Sequence[Any],
        code: str,
        *,
        typing_delay_seconds: float = 0.0,
    ) -> None:
        code_text = str(code).strip()
        if not code_text:
            raise ValueError("Authentication code must not be empty.")

        if not inputs:
            raise TimeoutException("No authentication inputs were available.")

        if len(inputs) == 1:
            self._set_input_value_with_retry(inputs[0], code_text, typing_delay_seconds=typing_delay_seconds)
            return

        if len(inputs) < len(code_text):
            self._set_input_value_with_retry(inputs[0], code_text, typing_delay_seconds=typing_delay_seconds)
            return

        for element, digit in zip(inputs, code_text):
            self._clear_and_type_element(element, digit, typing_delay_seconds=typing_delay_seconds)
            if typing_delay_seconds > 0:
                time.sleep(typing_delay_seconds)

    def _fill_code_inputs(self, selector_key: str, code: str, *, typing_delay_seconds: float = 0.0) -> None:
        inputs = self._find_all(selector_key)
        if not inputs:
            raise TimeoutException(f"Timed out waiting for selector '{selector_key}'.")
        self._fill_code_elements(inputs, code, typing_delay_seconds=typing_delay_seconds)

    def _read_visible_page_text(self) -> str:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            return " ".join((body.text or "").lower().split())
        except Exception:
            return ""

    def _find_generic_auth_inputs(self) -> List[Any]:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        xpath = (
            "//input[not(@type='hidden') and "
            "(not(@type) or normalize-space(@type)='' or @type='password' or @type='tel' or @type='number' or @type='text' or @type='email' "
            "or @inputmode='numeric' or @autocomplete='one-time-code')]"
        )
        try:
            elements = self.driver.find_elements(By.XPATH, xpath)
        except Exception:
            return []

        return [element for element in elements if element.is_displayed()]

    def _resolve_auth_inputs(self, challenge: str) -> List[Any]:
        selector_key = "otp_input" if challenge == "otp" else "mpin_input"
        inputs = self._find_all(selector_key)
        if inputs:
            return inputs
        return self._find_generic_auth_inputs()

    def _page_mentions_pin_prompt(self, page_text: Optional[str] = None) -> bool:
        if page_text is None:
            page_text = self._read_visible_page_text()
        normalized_page_text = " ".join(str(page_text or "").lower().split())
        return any(keyword in normalized_page_text for keyword in PIN_PROMPT_KEYWORDS)

    def _login_mode_tabs_visible(self) -> bool:
        return (
            self._find_optional("login_client_id_tab", timeout_seconds=1) is not None
            or self._find_optional("login_mobile_number_tab", timeout_seconds=1) is not None
        )

    def _detect_auth_challenge(self) -> Optional[str]:
        page_text = self._read_visible_page_text()
        strict_otp_inputs = self._find_all("otp_input")
        strict_mpin_inputs = self._find_all("mpin_input")
        generic_inputs = self._find_generic_auth_inputs()

        otp_keywords = (" otp", "otp ", "one time password", "verification code", "verification otp", "enter otp")
        pin_prompt_visible = self._page_mentions_pin_prompt(page_text)

        # Angel can reuse the OTP input for the next PIN step, so visible PIN copy
        # must override stale OTP-oriented attributes on the same input.
        if pin_prompt_visible and (strict_mpin_inputs or generic_inputs):
            return "mpin"

        if strict_otp_inputs:
            return "otp"
        if any(keyword in page_text for keyword in otp_keywords) and generic_inputs:
            return "otp"

        if strict_mpin_inputs:
            return "mpin"
        if pin_prompt_visible and generic_inputs:
            return "mpin"

        return None

    def _detect_login_blocker(self) -> Optional[str]:
        return extract_login_blocker_message(self._read_visible_page_text())

    def _switch_login_identifier_mode(self, mode: str) -> bool:
        selector_key = "login_client_id_tab" if mode == "client_id" else "login_mobile_number_tab"
        tab = self._find_optional(selector_key, timeout_seconds=2)
        if tab is None:
            LOGGER.info(
                "Angel login mode switch target was not visible | mode=%s selector=%s",
                mode,
                selector_key,
            )
            return False
        LOGGER.info("Switching Angel login mode | mode=%s", mode)
        self._click_element(tab)
        time.sleep(1)
        return True

    def _identifier_screen_requires_mode_reset(
        self,
        identifier_input: Optional[Any],
        last_submitted_challenge: Optional[str],
        selected_mode: Optional[str],
        current_mode: str,
    ) -> bool:
        return (
            identifier_input is not None
            and last_submitted_challenge not in (None, "identifier")
            and selected_mode == current_mode
        )

    def _find_related_submit_button(self, source_element: Any):
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        script = """
            const source = arguments[0];
            const matchers = [
                'button[type="submit"]',
                '[role="button"][type="submit"]',
                'button',
                '[role="button"]',
                'input[type="submit"]'
            ];

            function buttonLooksRight(element) {
                const text = ((element.innerText || element.value || '') + '').trim().toLowerCase();
                return (
                    text.includes('continue')
                    || text.includes('next')
                    || text.includes('login')
                    || text.includes('submit')
                    || text.includes('verify')
                    || text.includes('otp')
                    || text.includes('generate')
                    || text.includes('proceed')
                );
            }

            let node = source;
            while (node) {
                for (const selector of matchers) {
                    const candidates = Array.from(node.querySelectorAll(selector));
                    for (const candidate of candidates) {
                        const style = window.getComputedStyle(candidate);
                        if (style && style.display === 'none') {
                            continue;
                        }
                        if (candidate.disabled || candidate.getAttribute('aria-disabled') === 'true') {
                            continue;
                        }
                        if (buttonLooksRight(candidate)) {
                            return candidate;
                        }
                    }
                }
                node = node.parentElement;
            }
            return null;
        """
        try:
            return self.driver.execute_script(script, source_element)
        except Exception:
            return None

    def _submit_auth_step(
        self,
        preferred_button_selector: Optional[str] = None,
        *,
        source_element: Optional[Any] = None,
    ) -> None:
        if source_element is not None:
            related_button = self._find_related_submit_button(source_element)
            if related_button is not None:
                LOGGER.info("Submitting Angel login step via related button.")
                self._click_element(related_button)
                return

            try:
                LOGGER.info("Submitting Angel login step via Enter on the active field.")
                source_element.send_keys(Keys.ENTER)
                return
            except Exception:
                pass

        submit_selectors = []
        if preferred_button_selector:
            submit_selectors.append(preferred_button_selector)
        submit_selectors.append("auth_submit_button")

        for selector_key in submit_selectors:
            try:
                button = self._find_optional(selector_key, timeout_seconds=2)
            except KeyError:
                button = None
            if button is not None:
                LOGGER.info("Submitting Angel login step via selector '%s'.", selector_key)
                self._click_element(button)
                return

        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")
        try:
            active_element = self.driver.switch_to.active_element
            active_element.send_keys(Keys.ENTER)
            return
        except Exception as exc:
            raise TimeoutException("Could not find a submit action for Angel login.") from exc

    def _click(self, selector_key: str, *, context: Optional[Mapping[str, Any]] = None, timeout_seconds: Optional[int] = None) -> None:
        element = self._find_first(
            selector_key,
            context=context,
            timeout_seconds=timeout_seconds,
        )
        self._click_element(element)

    def _click_element(self, element: Any) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        try:
            element.click()
        except WebDriverException:
            self.driver.execute_script("arguments[0].click();", element)

    def _clear_and_type(
        self,
        selector_key: str,
        value: str,
        *,
        context: Optional[Mapping[str, Any]] = None,
        press_enter: bool = False,
        typing_delay_seconds: float = 0.0,
    ) -> None:
        element = self._find_first(selector_key, context=context)
        self._clear_and_type_element(
            element,
            value,
            press_enter=press_enter,
            typing_delay_seconds=typing_delay_seconds,
        )

    def _clear_and_type_element(
        self,
        element: Any,
        value: str,
        *,
        press_enter: bool = False,
        typing_delay_seconds: float = 0.0,
    ) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        modifier = Keys.COMMAND if sys.platform == "darwin" else Keys.CONTROL
        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        try:
            element.click()
        except WebDriverException:
            self.driver.execute_script("arguments[0].click();", element)
        element.send_keys(modifier, "a")
        element.send_keys(Keys.DELETE)
        text_value = str(value)
        if typing_delay_seconds > 0:
            for character in text_value:
                element.send_keys(character)
                time.sleep(typing_delay_seconds)
        else:
            element.send_keys(text_value)
        if press_enter:
            element.send_keys(Keys.ENTER)

    def _set_input_value_element(self, element: Any, value: str) -> str:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        script = """
            const element = arguments[0];
            const nextValue = arguments[1];
            const prototype = Object.getPrototypeOf(element);
            const descriptor = Object.getOwnPropertyDescriptor(prototype, 'value')
                || Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')
                || Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value');
            if (descriptor && descriptor.set) {
                descriptor.set.call(element, nextValue);
            } else {
                element.value = nextValue;
            }
            element.dispatchEvent(new Event('input', { bubbles: true }));
            element.dispatchEvent(new Event('change', { bubbles: true }));
            element.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'Tab' }));
            element.blur();
            return element.value || '';
        """
        rendered_value = self.driver.execute_script(script, element, str(value))
        return str(rendered_value or "")

    def _normalize_input_value(self, value: str) -> str:
        return str(value).replace(",", "").strip()

    def _dismiss_post_login_dialogs(self, *, timeout_seconds: float = 3.0) -> bool:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        dismissed = False
        deadline = time.time() + max(timeout_seconds, 0.0)
        while time.time() < deadline:
            button = self._find_optional("post_login_got_it_button", timeout_seconds=1)
            if button is None:
                return dismissed

            LOGGER.info("Dismissing Angel post-login disclosure modal.")
            self._click_element(button)
            dismissed = True
            time.sleep(1)

        return dismissed

    def _set_input_value_with_retry(
        self,
        element: Any,
        value: str,
        *,
        press_enter: bool = False,
        timeout_seconds: int = 5,
        typing_delay_seconds: float = 0.0,
    ) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        expected_value = self._normalize_input_value(value)
        deadline = time.time() + timeout_seconds
        last_seen = ""
        last_error: Optional[Exception] = None

        while time.time() < deadline:
            try:
                self._clear_and_type_element(
                    element,
                    value,
                    press_enter=press_enter,
                    typing_delay_seconds=typing_delay_seconds,
                )
                last_seen = self._normalize_input_value(element.get_attribute("value") or "")
                if last_seen == expected_value:
                    return
            except Exception as exc:
                last_error = exc

            try:
                last_seen = self._normalize_input_value(self._set_input_value_element(element, value))
                if last_seen == expected_value:
                    return
            except Exception as exc:
                last_error = exc

            time.sleep(0.25)

        raise TimeoutException(
            f"Timed out setting input value to {value!r}. Last seen value={last_seen!r}. "
            f"Last error: {last_error}"
        )

    def _hover(self, selector_key: str, *, context: Optional[Mapping[str, Any]] = None) -> None:
        if ActionChains is None:
            raise RuntimeError(
                "selenium ActionChains is unavailable because selenium is not installed."
            ) from SELENIUM_IMPORT_ERROR
        element = self._find_first(selector_key, context=context)
        ActionChains(self.driver).move_to_element(element).perform()

    def _select_dropdown_option(self, dropdown_key: str, option: Optional[str]) -> None:
        if not option:
            return

        if dropdown_key not in self.selector_config:
            LOGGER.info("Skipping optional dropdown '%s' because no selectors were configured.", dropdown_key)
            return

        self._click(dropdown_key)
        self._click(
            "dropdown_option",
            context={"option": option, "option_upper": option.upper()},
        )

    def _find_watchlist_rows(self, search_mode: bool) -> List[Any]:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        css = '#wlSearch > div[id^="watchlist-DEFAULT-"]' if search_mode else '#wlContainer > li[id^="watchlist-row-"]'
        return self.driver.find_elements(By.CSS_SELECTOR, css)

    def _normalize_element_text(self, value: str) -> str:
        return " ".join((value or "").upper().split())

    def _extract_watchlist_row_key(self, element: Any) -> Optional[Tuple[str, str, str]]:
        try:
            lines = [line.strip() for line in element.text.splitlines() if line.strip()]
            if not lines:
                return None

            exchange_index = next((idx for idx, line in enumerate(lines) if line.upper() == "NCDEX"), None)
            if exchange_index is None or exchange_index == 0:
                return None

            name = lines[exchange_index - 1].upper()
            exchange = lines[exchange_index].upper()

            expiry = None
            for line in lines[exchange_index + 1:]:
                parsed = _parse_expiry_label(line)
                if parsed is not None:
                    expiry = parsed.strftime("%d %b %Y")
                    break

            if not expiry:
                return None

            return (name, exchange, expiry.upper())
        except Exception:
            return None

    def _get_current_watchlist_keys(self) -> List[Tuple[str, str, str]]:
        keys: List[Tuple[str, str, str]] = []
        for row in self._find_watchlist_rows(search_mode=False):
            key = self._extract_watchlist_row_key(row)
            if key:
                keys.append(key)
        return keys

    def _select_watchlist(self, watchlist_index: int, *, deadline: Optional[float] = None) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        local_deadline = time.time() + self._remaining_timeout(
            deadline,
            fallback_seconds=self.timeout_seconds,
            flow_name=f"Watchlist selection for tab {watchlist_index}",
        )
        while time.time() < local_deadline:
            for button in self.driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]'):
                if button.text.strip() == str(watchlist_index):
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                    button.click()
                    time.sleep(1)
                    return
            time.sleep(0.25)
        raise TimeoutException(f"Watchlist tab '{watchlist_index}' was not found.")

    def _set_watchlist_search(self, query: str) -> None:
        self._clear_and_type("search_input", query)
        time.sleep(2)

    def _filter_watchlist_search_to_commodity(self) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        try:
            commodity_tab = self.driver.find_element(By.ID, "watchlist-search-tabs-Commodity")
            if commodity_tab.get_attribute("aria-selected") != "true":
                commodity_tab.click()
                time.sleep(1)
        except Exception:
            LOGGER.info("Commodity search tab was not available. Continuing with default search scope.")

    def _close_watchlist_search(self) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        try:
            search = self.driver.find_element(By.ID, "watchlist-search")
        except Exception:
            return

        search.click()
        modifier = Keys.COMMAND if sys.platform == "darwin" else Keys.CONTROL
        search.send_keys(modifier, "a")
        search.send_keys(Keys.DELETE)
        search.send_keys(Keys.ESCAPE)
        time.sleep(1)

    def _find_search_result_row(self, candidate: WatchlistCandidate, *, deadline: Optional[float] = None) -> Any:
        local_deadline = time.time() + self._remaining_timeout(
            deadline,
            fallback_seconds=self.timeout_seconds,
            flow_name=f"Watchlist search for {candidate.symbol}",
        )
        target = candidate.key()
        while time.time() < local_deadline:
            rows = self._find_watchlist_rows(search_mode=True)
            for row in rows:
                row_key = self._extract_watchlist_row_key(row)
                if row_key == target:
                    return row
            time.sleep(0.25)
        raise TimeoutException(f"Could not find watchlist search row for {candidate.to_log_dict()}.")

    def _find_watchlist_row(self, candidate: WatchlistCandidate, *, deadline: Optional[float] = None) -> Any:
        local_deadline = time.time() + self._remaining_timeout(
            deadline,
            fallback_seconds=self.timeout_seconds,
            flow_name=f"Watchlist row lookup for {candidate.symbol}",
        )
        target = candidate.key()
        last_seen = 0
        while time.time() < local_deadline:
            rows = self._find_watchlist_rows(search_mode=False)
            last_seen = len(rows)
            for row in rows:
                row_key = self._extract_watchlist_row_key(row)
                if row_key == target:
                    return row
            time.sleep(0.25)
        raise TimeoutException(
            f"Could not find watchlist row for {candidate.to_log_dict()} in current watchlist. "
            f"Visible rows={last_seen}."
        )

    def _click_watchlist_action(
        self,
        candidate: WatchlistCandidate,
        side: str,
        *,
        deadline: Optional[float] = None,
    ) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")
        if ActionChains is None:
            raise RuntimeError(
                "selenium ActionChains is unavailable because selenium is not installed."
            ) from SELENIUM_IMPORT_ERROR

        button_id = side.lower()
        local_deadline = time.time() + self._remaining_timeout(
            deadline,
            fallback_seconds=self.timeout_seconds,
            flow_name=f"Watchlist action {side} for {candidate.symbol}",
        )
        last_error: Optional[Exception] = None
        while time.time() < local_deadline:
            try:
                row = self._find_watchlist_row(candidate, deadline=deadline)
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", row)
                ActionChains(self.driver).move_to_element(row).perform()
                time.sleep(0.4)
                button = row.find_element(By.ID, button_id)
                self._click_element(button)
                return
            except Exception as exc:
                last_error = exc
                time.sleep(0.25)

        raise TimeoutException(
            f"Could not click '{side}' for watchlist row {candidate.to_log_dict()}. "
            f"Last error: {last_error}"
        )

    def _ensure_watchlist_candidate_present(
        self,
        candidate: WatchlistCandidate,
        *,
        watchlist_index: int,
        deadline: Optional[float] = None,
    ) -> None:
        try:
            self._find_watchlist_row(candidate, deadline=deadline)
            return
        except TimeoutException:
            LOGGER.warning(
                "Watchlist candidate was not present; attempting automatic add | watchlist_index=%s candidate=%s",
                watchlist_index,
                json.dumps(candidate.to_log_dict(), sort_keys=True),
            )

        self.add_candidate_to_watchlist(candidate, deadline=deadline)
        self._close_watchlist_search()
        self._select_watchlist(watchlist_index, deadline=deadline)
        self._find_watchlist_row(candidate, deadline=deadline)

    def _find_element_by_id(self, element_id: str, *, timeout_seconds: Optional[int] = None):
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        timeout = timeout_seconds or self.timeout_seconds
        deadline = time.time() + timeout
        last_error: Optional[Exception] = None
        while time.time() < deadline:
            try:
                element = self.driver.find_element(By.ID, element_id)
                if element.is_displayed():
                    return element
                return element
            except Exception as exc:
                last_error = exc
            time.sleep(0.25)

        raise TimeoutException(f"Timed out waiting for element id '{element_id}'. Last error: {last_error}")

    def _is_element_displayed_by_id(self, element_id: str) -> bool:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        try:
            elements = self.driver.find_elements(By.ID, element_id)
        except Exception:
            return False

        return any(element.is_displayed() for element in elements)

    def _wait_for_element_hidden_by_id(self, element_id: str, *, timeout_seconds: int = 5) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if not self._is_element_displayed_by_id(element_id):
                return
            time.sleep(0.25)

        raise TimeoutException(f"Timed out waiting for element id '{element_id}' to hide.")

    def _reset_order_entry_state(self) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        try:
            if self._read_optional_message("scheduled_order_title", timeout_seconds=1):
                self._click("scheduled_order_ok_button", timeout_seconds=2)
                time.sleep(0.5)
        except (TimeoutException, KeyError):
            pass

        try:
            if self._is_element_displayed_by_id("close-orderpad-icon"):
                close_button = self._find_element_by_id("close-orderpad-icon", timeout_seconds=1)
                self._click_element(close_button)
                self._wait_for_element_hidden_by_id("order_pad_container", timeout_seconds=5)
        except (TimeoutException, KeyError):
            pass

    def _read_order_pad_state(self) -> Dict[str, Any]:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        state: Dict[str, Any] = {
            "quantity": None,
            "price": None,
            "product": None,
            "order_type": None,
            "submit_enabled": None,
        }

        try:
            quantity_input = self._find_element_by_id("quantityOrderPad", timeout_seconds=1)
            state["quantity"] = self._normalize_input_value(quantity_input.get_attribute("value") or "")
        except TimeoutException:
            pass

        try:
            price_input = self._find_element_by_id("priceOrderPad", timeout_seconds=1)
            state["price"] = self._normalize_input_value(price_input.get_attribute("value") or "")
        except TimeoutException:
            pass

        for product_key, button_id in PRODUCT_BUTTON_IDS.items():
            try:
                button = self._find_element_by_id(button_id, timeout_seconds=1)
                if button.get_attribute("aria-selected") == "true":
                    state["product"] = product_key
                    break
            except TimeoutException:
                continue

        for order_type_key, button_id in ORDER_TYPE_BUTTON_IDS.items():
            try:
                button = self._find_element_by_id(button_id, timeout_seconds=1)
                classes = (button.get_attribute("class") or "").lower()
                if "text-success-default" in classes:
                    state["order_type"] = order_type_key
                    break
            except TimeoutException:
                continue

        try:
            submit_button = self._find_first("submit_button", timeout_seconds=1)
            disabled_attr = submit_button.get_attribute("disabled")
            aria_disabled = (submit_button.get_attribute("aria-disabled") or "").strip().lower()
            state["submit_enabled"] = (
                submit_button.is_enabled()
                and disabled_attr in (None, "", "false")
                and aria_disabled != "true"
            )
        except (TimeoutException, KeyError):
            state["submit_enabled"] = False

        return state

    def _wait_for_submit_ready(
        self,
        order: OrderRequest,
        *,
        timeout_seconds: int = 6,
        refill_attempts: int = 1,
    ):
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        expected_quantity = self._normalize_input_value(str(order.quantity))
        expected_price = self._normalize_input_value(str(order.price)) if order.price is not None else None
        refill_count = 0
        deadline = time.time() + timeout_seconds
        last_state: Optional[Dict[str, Any]] = None

        while time.time() < deadline:
            last_state = self._read_order_pad_state()
            quantity_ok = last_state.get("quantity") == expected_quantity
            price_ok = expected_price is None or last_state.get("price") == expected_price
            product_ok = last_state.get("product") in {order.product, PRODUCT_BUTTON_IDS.get(order.product, "")}
            order_type_ok = last_state.get("order_type") == order.order_type
            submit_ok = bool(last_state.get("submit_enabled"))

            if quantity_ok and price_ok and product_ok and order_type_ok and submit_ok:
                return self._find_first_enabled("submit_button", timeout_seconds=1)

            if refill_count < refill_attempts:
                LOGGER.warning(
                    "Order pad state drift detected; refilling fields | state=%s expected_quantity=%s expected_price=%s",
                    json.dumps(last_state, sort_keys=True),
                    expected_quantity,
                    expected_price,
                )
                self._fill_order_pad_fields(order)
                refill_count += 1

            time.sleep(0.5)

        raise TimeoutException(
            "Timed out waiting for order pad to be ready for submit. "
            f"Last state={json.dumps(last_state or {}, sort_keys=True)}"
        )

    def _find_first_enabled(
        self,
        selector_key: str,
        *,
        context: Optional[Mapping[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ):
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        timeout = timeout_seconds or self.timeout_seconds
        deadline = time.time() + timeout
        last_element = None
        while time.time() < deadline:
            element = self._find_first(
                selector_key,
                context=context,
                timeout_seconds=1,
            )
            last_element = element
            disabled_attr = element.get_attribute("disabled")
            aria_disabled = (element.get_attribute("aria-disabled") or "").strip().lower()
            if element.is_enabled() and disabled_attr in (None, "", "false") and aria_disabled != "true":
                return element
            time.sleep(0.25)

        raise TimeoutException(
            f"Timed out waiting for enabled selector '{selector_key}'. "
            f"Last element text={getattr(last_element, 'text', None)!r}"
        )

    def _prepare_submit_click(self) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        # Let Angel commit any inline field validation before clicking the CTA.
        self.driver.execute_script(
            """
            if (document.activeElement) {
                document.activeElement.blur();
            }
            """
        )
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            body.click()
        except Exception:
            pass
        time.sleep(0.75)

    def _read_optional_message(
        self,
        selector_key: str,
        *,
        timeout_seconds: int = 1,
    ) -> Optional[str]:
        try:
            element = self._find_first(selector_key, timeout_seconds=timeout_seconds)
        except (TimeoutException, KeyError):
            return None

        message = element.text.strip()
        return message or None

    def _summarize_ui_message(self, message: Optional[str]) -> Optional[str]:
        if not message:
            return None

        normalized = " ".join(message.split())
        lowered = normalized.lower()

        scheduled_match = re.search(
            r"(Order Scheduled.*?next trading window.*?execute the order)",
            normalized,
            flags=re.IGNORECASE,
        )
        if scheduled_match:
            return scheduled_match.group(1)

        if "order scheduled" in lowered and "next trading window" in lowered:
            return (
                "Order Scheduled: Your order has been scheduled for the next trading window. "
                "Please ensure sufficient funds in your trading balance to execute the order."
            )

        margin_match = re.search(
            r"(Insufficient margin!? .*?₹[\d,]+(?:\.\d{2})?)",
            normalized,
            flags=re.IGNORECASE,
        )
        if margin_match:
            return margin_match.group(1)

        if len(normalized) > 240:
            return normalized[:237] + "..."

        return normalized

    def _wait_for_post_submit_state(self, *, timeout_seconds: float = 12) -> Optional[str]:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                scheduled = self._find_first("scheduled_order_title", timeout_seconds=1)
                if scheduled:
                    return "scheduled_modal"
            except (TimeoutException, KeyError):
                pass

            try:
                confirm = self._find_first("confirm_button", timeout_seconds=1)
                if confirm:
                    return "confirm_required"
            except (TimeoutException, KeyError):
                pass

            try:
                review = self._find_first("review_edit_button", timeout_seconds=1)
                if review:
                    return "confirm_required"
            except (TimeoutException, KeyError):
                pass

            try:
                message = self._find_first("inline_order_pad_message", timeout_seconds=1)
                if message:
                    return "inline_message"
            except (TimeoutException, KeyError):
                pass

            try:
                message = self._find_first("post_submit_message", timeout_seconds=1)
                if message:
                    return "message"
            except (TimeoutException, KeyError):
                pass

            try:
                submit_button = self._find_first("submit_button", timeout_seconds=1)
                if not submit_button.is_displayed():
                    return "transitioned"
            except (TimeoutException, KeyError):
                return "transitioned"

            time.sleep(0.25)

        return "no_transition"

    def _select_product_button(self, product: str) -> None:
        product_key = str(product).strip().upper()
        button_id = PRODUCT_BUTTON_IDS.get(product_key)
        if not button_id:
            raise ValueError(
                f"Unsupported product '{product}'. Supported values: {sorted(PRODUCT_BUTTON_IDS)}"
            )
        self._click_element(self._find_element_by_id(button_id))

    def _select_order_type_button(self, order_type: str) -> None:
        order_type_key = str(order_type).strip().upper()
        button_id = ORDER_TYPE_BUTTON_IDS.get(order_type_key)
        if not button_id:
            raise ValueError(
                f"Unsupported order type '{order_type}' in Angel web bot. "
                f"Supported values: {sorted(ORDER_TYPE_BUTTON_IDS)}"
            )
        self._click_element(self._find_element_by_id(button_id))

    def _fill_order_pad_fields(self, order: OrderRequest) -> None:
        if order.validity != "DAY":
            raise ValueError("Angel web bot currently supports only DAY validity.")
        if order.order_type in {"SL", "SL-M"}:
            raise ValueError("Angel web bot currently supports LIMIT and MARKET orders only.")

        quantity_input = self._find_element_by_id("quantityOrderPad", timeout_seconds=20)
        self._set_input_value_with_retry(quantity_input, str(order.quantity))
        self._select_product_button(order.product)
        self._select_order_type_button(order.order_type)

        if order.price is not None:
            # Angel occasionally ignores the first price update unless the pad settles first.
            LOGGER.info(
                "Pausing before filling Angel limit price | seconds=%s symbol=%s",
                ORDER_PRICE_SETTLE_DELAY_SECONDS,
                order.symbol,
            )
            time.sleep(ORDER_PRICE_SETTLE_DELAY_SECONDS)
            price_input = self._find_element_by_id("priceOrderPad")
            LOGGER.info(
                "Typing Angel limit price with inter-key delay | seconds=%s symbol=%s",
                ORDER_PRICE_TYPING_DELAY_SECONDS,
                order.symbol,
            )
            self._set_input_value_with_retry(
                price_input,
                str(order.price),
                typing_delay_seconds=ORDER_PRICE_TYPING_DELAY_SECONDS,
            )

    def add_candidate_to_watchlist(
        self,
        candidate: WatchlistCandidate,
        *,
        deadline: Optional[float] = None,
    ) -> Dict[str, Any]:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        flow_deadline = deadline or self._build_flow_deadline(WATCHLIST_ADD_HARD_TIMEOUT_SECONDS)
        LOGGER.info("Adding watchlist candidate | candidate=%s", json.dumps(candidate.to_log_dict(), sort_keys=True))
        self._set_watchlist_search(candidate.name)
        self._filter_watchlist_search_to_commodity()
        row = self._find_search_result_row(candidate, deadline=flow_deadline)
        buttons = row.find_elements(By.TAG_NAME, "button")
        if not buttons:
            raise RuntimeError(f"Search row for {candidate.symbol} did not expose any actions.")

        add_button = buttons[-1]
        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", add_button)
        add_button.click()
        time.sleep(1)

        result = {
            "candidate": candidate.to_log_dict(),
            "status": "added",
        }

        try:
            toast = self._find_first("post_submit_message", timeout_seconds=3, displayed_only=False)
            result["message"] = toast.text.strip()
        except Exception:
            pass

        return result

    def seed_watchlist(
        self,
        candidates: Sequence[WatchlistCandidate],
        *,
        watchlist_index: int,
        max_items: int = 50,
        allow_manual_login: bool = False,
    ) -> Dict[str, Any]:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        self.open_target_page()
        self.ensure_ready(allow_manual_login=allow_manual_login)
        self._select_watchlist(watchlist_index)

        existing_keys = set(self._get_current_watchlist_keys())
        LOGGER.info("Current watchlist state | watchlist_index=%s existing_items=%s", watchlist_index, len(existing_keys))

        pending = [candidate for candidate in candidates if candidate.key() not in existing_keys]
        if len(existing_keys) + len(pending) > max_items:
            raise RuntimeError(
                f"Watchlist {watchlist_index} would exceed {max_items} items "
                f"({len(existing_keys)} existing + {len(pending)} pending)."
            )

        results = []
        for candidate in pending:
            results.append(self.add_candidate_to_watchlist(candidate))
            existing_keys.add(candidate.key())

        self._close_watchlist_search()
        artifacts = self._capture_artifacts(f"watchlist_{watchlist_index}_seeded")

        return {
            "status": "completed",
            "watchlist_index": watchlist_index,
            "existing_count": len(existing_keys) - len(results),
            "added_count": len(results),
            "total_after": len(existing_keys),
            "results": results,
            "artifacts": artifacts,
        }

    def _capture_artifacts(self, prefix: str) -> Dict[str, str]:
        if self.driver is None:
            return {}

        timestamp = datetime.now().strftime("%H%M%S")
        safe_prefix = prefix.replace(" ", "_").replace("/", "_")
        screenshot_path = self.artifact_dir / f"{timestamp}_{safe_prefix}.png"
        html_path = self.artifact_dir / f"{timestamp}_{safe_prefix}.html"
        self.driver.save_screenshot(str(screenshot_path))
        html_path.write_text(self.driver.page_source, encoding="utf-8")
        return {
            "screenshot": str(screenshot_path),
            "html": str(html_path),
        }

    def _read_cookie_expiry_iso(self, cookie_name: str) -> Optional[str]:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        try:
            cookie = self.driver.get_cookie(cookie_name)
        except Exception:
            return None

        if not cookie:
            return None

        expiry = cookie.get("expiry")
        if not expiry:
            return None

        try:
            return datetime.fromtimestamp(float(expiry), tz=timezone.utc).isoformat()
        except Exception:
            return None

    def get_session_snapshot(self, *, open_page: bool = True) -> SessionSnapshot:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        if open_page:
            self.open_target_page()

        page_ready = False
        try:
            self._find_first("page_ready", timeout_seconds=3)
            page_ready = True
            self._dismiss_post_login_dialogs(timeout_seconds=1.5)
        except TimeoutException:
            page_ready = False

        current_url = self.driver.current_url
        title = self.driver.title
        trade_access_token_expiry = self._read_cookie_expiry_iso("prod_trade_access_token")
        non_trade_access_token_expiry = self._read_cookie_expiry_iso("prod_non_trade_access_token")
        login_required = not page_ready or "/login" in current_url.lower()
        status = "READY" if page_ready and trade_access_token_expiry else "LOGIN_REQUIRED"

        return SessionSnapshot(
            status=status,
            current_url=current_url,
            title=title,
            page_ready=page_ready,
            login_required=login_required,
            trade_access_token_expiry=trade_access_token_expiry,
            non_trade_access_token_expiry=non_trade_access_token_expiry,
            checked_at=datetime.now(timezone.utc).isoformat(),
        )

    def open_target_page(self) -> None:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        LOGGER.info("Opening Angel web page | url=%s", self.url)
        self.driver.get(self.url)
        try:
            self._find_first("page_ready", timeout_seconds=5)
            self._dismiss_post_login_dialogs(timeout_seconds=1.5)
        except TimeoutException:
            LOGGER.info("Page-ready markers were not visible immediately after navigation.")

    def _attempt_login_from_files(self) -> bool:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        current_url = (self.driver.current_url or "").lower()
        current_title = (self.driver.title or "").lower()
        if (
            "/login" not in current_url
            and "login" not in current_title
            and not self._selector_exists("otp_input", timeout_seconds=1)
            and not self._selector_exists("mpin_input", timeout_seconds=1)
        ):
            LOGGER.info(
                "Angel page does not appear to be on the login flow; skipping file-based login | url=%s title=%s",
                self.driver.current_url,
                self.driver.title,
            )
            return False

        if not self.login_credentials_file.exists():
            LOGGER.info(
                "Angel login credentials file was not found; auto-login is unavailable | path=%s",
                self.login_credentials_file,
            )
            return False

        credentials = load_login_credentials_file(self.login_credentials_file)
        login_id = credentials["login_id"]
        client_id = credentials.get("client_id", "").strip()
        mobile_number = credentials.get("mobile_number", "").strip()
        mpin = credentials.get("mpin", "")
        login_mode = normalize_login_mode(credentials.get("login_mode")) or infer_login_identifier_mode(login_id)

        if not client_id and infer_login_identifier_mode(login_id) == "client_id":
            client_id = login_id
        if not mobile_number and infer_login_identifier_mode(login_id) == "mobile":
            mobile_number = login_id

        current_mode = login_mode
        current_identifier = client_id if current_mode == "client_id" else mobile_number or login_id
        if not current_identifier:
            raise RuntimeError(
                f"Angel login mode '{current_mode}' does not have a usable identifier in {self.login_credentials_file}"
            )

        LOGGER.info(
            "Attempting Angel web login from files | credentials_path=%s otp_path=%s mode=%s",
            self.login_credentials_file,
            self.otp_file,
            current_mode,
        )

        identifier_submitted = False
        identifier_submit_attempts = 0
        last_identifier_submit_at = 0.0
        otp_wait_started_at: Optional[float] = None
        deadline = self._build_flow_deadline(
            min(max(self.otp_timeout_seconds + 30, 60), FILE_LOGIN_HARD_TIMEOUT_SECONDS)
        )
        last_wait_log_at = 0.0
        last_submitted_challenge: Optional[str] = None
        selected_mode: Optional[str] = None

        while time.time() < deadline:
            if self._selector_exists("page_ready", timeout_seconds=1):
                LOGGER.info("Angel page became ready after file-based login.")
                return True

            if selected_mode != current_mode:
                switched = self._switch_login_identifier_mode(current_mode)
                if switched or current_mode == "mobile":
                    selected_mode = current_mode

            challenge = self._detect_auth_challenge()

            if challenge == "otp":
                if otp_wait_started_at is None:
                    otp_wait_started_at = time.time()
                    LOGGER.info(
                        "Angel OTP prompt detected; waiting for OTP file update | path=%s timeout_seconds=%s",
                        self.otp_file,
                        self.otp_timeout_seconds,
                    )
                if last_submitted_challenge != "otp":
                    otp_code = self._read_otp_from_file(otp_wait_started_at)
                    LOGGER.info("Submitting Angel OTP from file.")
                    time.sleep(OTP_FILL_DELAY_SECONDS)
                    otp_inputs = self._resolve_auth_inputs("otp")
                    self._fill_code_elements(
                        otp_inputs,
                        otp_code,
                        typing_delay_seconds=OTP_TYPING_DELAY_SECONDS,
                    )
                    self._submit_auth_step("otp_submit_button", source_element=otp_inputs[0] if otp_inputs else None)
                    last_submitted_challenge = "otp"
                time.sleep(2)
                continue

            if challenge == "mpin":
                if not mpin:
                    raise RuntimeError(
                        f"Angel login requires MPIN but the credentials file does not include one: {self.login_credentials_file}"
                    )
                if last_submitted_challenge != "mpin":
                    LOGGER.info("Submitting Angel MPIN from file.")
                    time.sleep(MPIN_FILL_DELAY_SECONDS)
                    mpin_inputs = self._resolve_auth_inputs("mpin")
                    self._fill_code_elements(
                        mpin_inputs,
                        mpin,
                        typing_delay_seconds=MPIN_TYPING_DELAY_SECONDS,
                    )
                    time.sleep(AUTH_SUBMIT_BUTTON_DELAY_SECONDS)
                    self._submit_auth_step("mpin_submit_button", source_element=mpin_inputs[0] if mpin_inputs else None)
                    last_submitted_challenge = "mpin"
                time.sleep(2)
                continue

            if last_submitted_challenge == "otp" and self._page_mentions_pin_prompt():
                LOGGER.info("Angel PIN prompt text detected after OTP; waiting for MPIN challenge instead of retrying identifier.")
                time.sleep(1)
                continue

            identifier_input = self._find_login_identifier_input(
                timeout_seconds=1,
                allow_form_fallback=last_submitted_challenge in (None, "identifier"),
            )
            if (
                identifier_input is not None
                and last_submitted_challenge == "otp"
                and mpin
                and not self._login_mode_tabs_visible()
            ):
                LOGGER.info("Angel reused the login input for the PIN step after OTP; submitting MPIN through that field.")
                time.sleep(MPIN_FILL_DELAY_SECONDS)
                self._set_input_value_with_retry(
                    identifier_input,
                    mpin,
                    typing_delay_seconds=MPIN_TYPING_DELAY_SECONDS,
                )
                time.sleep(AUTH_SUBMIT_BUTTON_DELAY_SECONDS)
                self._submit_auth_step("mpin_submit_button", source_element=identifier_input)
                last_submitted_challenge = "mpin"
                time.sleep(2)
                continue
            if self._identifier_screen_requires_mode_reset(
                identifier_input,
                last_submitted_challenge,
                selected_mode,
                current_mode,
            ):
                LOGGER.info(
                    "Angel login identifier screen reappeared after %s; reapplying login mode %s.",
                    last_submitted_challenge,
                    current_mode,
                )
                selected_mode = None
                time.sleep(0.5)
                continue
            if identifier_input is not None and (
                not identifier_submitted or (time.time() - last_identifier_submit_at) >= 8
            ):
                if identifier_submit_attempts >= MAX_LOGIN_IDENTIFIER_SUBMIT_ATTEMPTS:
                    raise TimeoutException(
                        "Angel login exceeded the maximum identifier submit attempts. "
                        "Stopping to avoid looping on the login screen."
                    )
                attempt_number = identifier_submit_attempts + 1
                LOGGER.info(
                    "Submitting Angel login identifier from file | attempt=%s mode=%s",
                    attempt_number,
                    current_mode,
                )
                time.sleep(LOGIN_IDENTIFIER_FILL_DELAY_SECONDS)
                self._set_input_value_with_retry(
                    identifier_input,
                    current_identifier,
                    typing_delay_seconds=LOGIN_IDENTIFIER_TYPING_DELAY_SECONDS,
                )
                self._submit_auth_step("login_continue_button", source_element=identifier_input)
                identifier_submitted = True
                identifier_submit_attempts += 1
                last_identifier_submit_at = time.time()
                last_submitted_challenge = "identifier"
                LOGGER.info("Angel login identifier submitted. Waiting for MPIN/OTP challenge.")
                time.sleep(2)
                continue

            blocker = self._detect_login_blocker()
            if blocker:
                if current_mode == "mobile" and client_id:
                    LOGGER.warning(
                        "Angel rejected mobile-number login on this device; switching to client ID login | blocker=%s",
                        blocker,
                    )
                    current_mode = "client_id"
                    current_identifier = client_id
                    selected_mode = None
                    identifier_submitted = False
                    last_identifier_submit_at = 0.0
                    last_submitted_challenge = None
                    otp_wait_started_at = None
                    time.sleep(1)
                    continue

                raise RuntimeError(
                    "Angel rejected the current login method on this device: "
                    f"{blocker}. "
                    + (
                        "Provide a client_id in the credentials file or change login_mode=client_id."
                        if current_mode == "mobile"
                        else "Manual device authentication is required before this browser session can continue."
                    )
                )

            if last_submitted_challenge is not None and (time.time() - last_wait_log_at) >= 10:
                last_wait_log_at = time.time()
                LOGGER.info(
                    "Angel login is still waiting after %s submit | mode=%s url=%s title=%s",
                    last_submitted_challenge,
                    current_mode,
                    self.driver.current_url,
                    self.driver.title,
                )

            time.sleep(1)

        LOGGER.warning(
            "Angel file-based login did not reach a ready state before timeout | credentials_path=%s otp_path=%s",
            self.login_credentials_file,
            self.otp_file,
        )
        return False

    def wait_for_manual_login(self, *, timeout_seconds: float = MANUAL_LOGIN_HARD_TIMEOUT_SECONDS) -> None:
        print(
            textwrap.dedent(
                f"""
                Browser launched.
                1. Log in to Angel One manually in that browser.
                2. Navigate until the watchlist/chart page is visible.
                3. The bot will wait up to {int(timeout_seconds)} seconds for the page to become ready.
                """
            ).strip()
        )
        deadline = self._build_flow_deadline(timeout_seconds)
        while time.time() < deadline:
            if self._selector_exists("page_ready", timeout_seconds=1):
                LOGGER.info("Angel page became ready after manual login.")
                return
            time.sleep(1)

        raise TimeoutException("Manual Angel login did not reach a ready state before timeout.")

    def ensure_ready(self, allow_manual_login: bool = False) -> None:
        try:
            self._find_first("page_ready", timeout_seconds=10)
            self._dismiss_post_login_dialogs(timeout_seconds=2.0)
        except TimeoutException:
            LOGGER.warning("Could not confirm page-ready state immediately.")
            if self._attempt_login_from_files():
                self._find_first("page_ready", timeout_seconds=10)
                self._dismiss_post_login_dialogs(timeout_seconds=2.0)
                return
            if allow_manual_login:
                LOGGER.info("Manual login is allowed for this run. Waiting for user.")
                self.wait_for_manual_login()
                self._dismiss_post_login_dialogs(timeout_seconds=2.0)
            else:
                raise

    def place_order(
        self,
        order: OrderRequest,
        *,
        candidate: Optional[WatchlistCandidate] = None,
        watchlist_index: Optional[int] = None,
    ) -> Dict[str, Any]:
        if self.driver is None:
            raise RuntimeError("Browser is not initialized.")

        LOGGER.info("Starting Angel web order flow | order=%s", json.dumps(order.to_log_dict(), sort_keys=True))
        flow_deadline = self._build_flow_deadline(ORDER_PLACEMENT_HARD_TIMEOUT_SECONDS)
        self.open_target_page()
        self.ensure_ready(allow_manual_login=order.allow_manual_login)
        self._reset_order_entry_state()

        context = {
            "symbol": order.symbol,
            "exchange": order.exchange,
            "side": order.side,
        }

        last_open_error: Optional[Exception] = None
        for attempt in range(2):
            try:
                if candidate is not None and watchlist_index is not None:
                    LOGGER.info(
                        "Using watchlist row for order placement | watchlist_index=%s candidate=%s attempt=%s",
                        watchlist_index,
                        json.dumps(candidate.to_log_dict(), sort_keys=True),
                        attempt + 1,
                    )
                    self._select_watchlist(watchlist_index, deadline=flow_deadline)
                    self._ensure_watchlist_candidate_present(
                        candidate,
                        watchlist_index=watchlist_index,
                        deadline=flow_deadline,
                    )
                    self._click_watchlist_action(candidate, order.side, deadline=flow_deadline)
                    self._fill_order_pad_fields(order)
                else:
                    self._clear_and_type("search_input", order.symbol, press_enter=False)
                    time.sleep(1)

                    try:
                        self._hover("search_result_exact_exchange", context=context)
                        self._click(f"row_{order.side.lower()}_button", context=context)
                    except TimeoutException:
                        LOGGER.info("Exchange-specific search result locator did not match; falling back to symbol-only result.")
                        self._hover("search_result_exact_symbol", context=context)
                        self._click(f"row_{order.side.lower()}_button", context=context)

                    self._find_first("order_pad_ready", timeout_seconds=20)
                    self._clear_and_type("quantity_input", str(order.quantity))
                    self._select_dropdown_option("product_dropdown", order.product)
                    self._select_dropdown_option("order_type_dropdown", order.order_type)
                    self._select_dropdown_option("validity_dropdown", order.validity)

                    if order.price is not None and "price_input" in self.selector_config:
                        self._clear_and_type("price_input", str(order.price))
                    if order.trigger_price is not None and "trigger_price_input" in self.selector_config:
                        self._clear_and_type("trigger_price_input", str(order.trigger_price))

                break
            except Exception as exc:
                last_open_error = exc
                if attempt == 1:
                    raise
                LOGGER.warning("Order pad open/fill attempt failed; retrying once | error=%s", exc)
                self._reset_order_entry_state()
                time.sleep(0.75)

        if last_open_error and candidate is None and watchlist_index is None:
            LOGGER.info("Search-mode order pad recovered after retry | last_error=%s", last_open_error)

        artifacts = self._capture_artifacts("order_pad_filled")
        if not order.submit_live:
            LOGGER.info("Dry run only. Order pad has been filled but not submitted.")
            return {
                "status": "dry_run",
                "order": order.to_log_dict(),
                "artifacts": artifacts,
            }

        self._prepare_submit_click()
        submit_button = self._wait_for_submit_ready(
            order,
            timeout_seconds=self._remaining_timeout(
                flow_deadline,
                fallback_seconds=8,
                flow_name=f"Angel order placement for {order.symbol}",
            ),
            refill_attempts=1,
        )
        LOGGER.info(
            "Pausing before Angel submit click | seconds=%s symbol=%s",
            ORDER_SUBMIT_BUTTON_DELAY_SECONDS,
            order.symbol,
        )
        time.sleep(ORDER_SUBMIT_BUTTON_DELAY_SECONDS)
        self._click_element(submit_button)
        transition_state = self._wait_for_post_submit_state(
            timeout_seconds=self._remaining_timeout(
                flow_deadline,
                fallback_seconds=12,
                flow_name=f"Angel order placement for {order.symbol}",
            )
        )
        if transition_state == "confirm_required":
            try:
                confirm_button = self._find_first_enabled("confirm_button", timeout_seconds=3)
                LOGGER.info(
                    "Pausing before Angel confirm click | seconds=%s symbol=%s",
                    ORDER_CONFIRM_BUTTON_DELAY_SECONDS,
                    order.symbol,
                )
                time.sleep(ORDER_CONFIRM_BUTTON_DELAY_SECONDS)
                self._click_element(confirm_button)
            except (TimeoutException, KeyError):
                LOGGER.info("Confirm button was not enabled; waiting for downstream transition.")

            follow_up_state = self._wait_for_post_submit_state(
                timeout_seconds=self._remaining_timeout(
                    flow_deadline,
                    fallback_seconds=12,
                    flow_name=f"Angel order placement for {order.symbol}",
                )
            )
            if follow_up_state not in (None, "confirm_required", "no_transition"):
                transition_state = follow_up_state

        try:
            if transition_state == "scheduled_modal":
                self._click("scheduled_order_ok_button", timeout_seconds=3)
        except (TimeoutException, KeyError):
            LOGGER.info("Scheduled-order modal did not expose an OK button.")

        post_submit_artifacts = self._capture_artifacts("order_submitted")
        message = self._read_optional_message("scheduled_order_message", timeout_seconds=3)
        if not message:
            message = self._read_optional_message("post_submit_message", timeout_seconds=5)
        if not message:
            message = self._read_optional_message("inline_order_pad_message", timeout_seconds=2)
        message = self._summarize_ui_message(message)
        if not message:
            LOGGER.info("Could not read a post-submit message from the UI.")

        if transition_state == "confirm_required" and message and "order scheduled" in message.lower():
            transition_state = "scheduled_modal"

        status = "submitted"
        if transition_state in {"inline_message", "no_transition"} and not (
            message and "order scheduled" in message.lower()
        ):
            status = "rejected"

        return {
            "status": status,
            "order": order.to_log_dict(),
            "artifacts": {**artifacts, **post_submit_artifacts},
            "message": message,
            "transition_state": transition_state,
        }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Standalone Angel One Selenium order bot.",
    )
    parser.add_argument(
        "--selectors",
        default=str(DEFAULT_SELECTORS_PATH),
        help="Path to the selector JSON file.",
    )
    parser.add_argument(
        "--profile-dir",
        default=str(DEFAULT_PROFILE_DIR),
        help="Persistent Chrome user-data directory for Angel web sessions.",
    )
    parser.add_argument(
        "--log-dir",
        default=str(DEFAULT_LOG_DIR),
        help="Directory where logs, screenshots, and HTML artifacts are stored.",
    )
    parser.add_argument(
        "--url",
        default=APP_URL,
        help="Angel web page to open before placing orders.",
    )
    parser.add_argument(
        "--chrome-binary",
        default=None,
        help="Explicit path to the Chrome/Chromium binary.",
    )
    parser.add_argument(
        "--login-credentials-file",
        default=str(DEFAULT_LOGIN_CREDENTIALS_FILE),
        help="Path to the Angel web login credentials file.",
    )
    parser.add_argument(
        "--otp-file",
        default=str(DEFAULT_LOGIN_OTP_FILE),
        help="Path to the Angel web OTP file polled only when OTP is requested.",
    )
    parser.add_argument(
        "--otp-timeout-seconds",
        type=int,
        default=120,
        help="Seconds to wait for a fresh OTP file update once the OTP screen is visible.",
    )
    parser.add_argument(
        "--otp-poll-interval",
        type=float,
        default=1.0,
        help="Seconds between OTP file polls while waiting on the Angel OTP screen.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    launch_parser = subparsers.add_parser(
        "launch",
        help="Launch a dedicated Chrome window for manual Angel login.",
    )
    launch_parser.add_argument(
        "--debug-port",
        type=int,
        default=9222,
        help="Remote debugging port for the launched Chrome instance.",
    )

    place_parser = subparsers.add_parser(
        "place",
        help="Attach to an existing browser or launch a dedicated one and place an order.",
    )
    place_parser.add_argument(
        "--order-file",
        default=str(DEFAULT_ORDER_PATH),
        help="Path to the JSON order request.",
    )
    place_parser.add_argument(
        "--instrument-file",
        default=str(DEFAULT_INSTRUMENT_FILE),
        help="Path to Angel's instrument master CSV for watchlist row resolution.",
    )
    place_parser.add_argument(
        "--watchlist-index",
        type=int,
        default=4,
        help="Numeric Angel watchlist tab that already contains the target contract.",
    )
    place_parser.add_argument(
        "--submit-live",
        action="store_true",
        help="Allow live submission, but only if the order JSON also contains submit_live=true.",
    )
    place_parser.add_argument(
        "--debugger-address",
        default=DEFAULT_DEBUGGER_ADDRESS,
        help="Attach to an existing Chrome session via debugger address. Pass empty string to disable attach mode.",
    )
    place_parser.add_argument(
        "--headless",
        action="store_true",
        help="Run a self-launched browser in headless mode. Ignored when attaching.",
    )
    place_parser.add_argument(
        "--attach-only",
        action="store_true",
        help="Fail instead of launching a new browser if the debugger session is unavailable.",
    )
    place_parser.add_argument(
        "--keep-open",
        action="store_true",
        help="Leave the browser open after the order flow finishes.",
    )

    seed_parser = subparsers.add_parser(
        "seed-watchlist",
        help="Populate a watchlist with NCDEX futures from the local Angel instrument master.",
    )
    seed_parser.add_argument(
        "--instrument-file",
        default=str(ROOT_DIR / "AngelInstrumentDetails.csv"),
        help="Path to Angel's instrument master CSV.",
    )
    seed_parser.add_argument(
        "--watchlist-index",
        type=int,
        default=4,
        help="Numeric Angel watchlist tab to populate.",
    )
    seed_parser.add_argument(
        "--min-days-to-expiry",
        type=int,
        default=6,
        help="Only include contracts whose expiry is at least this many days away.",
    )
    seed_parser.add_argument(
        "--debugger-address",
        default=DEFAULT_DEBUGGER_ADDRESS,
        help="Attach to an existing Chrome session via debugger address. Pass empty string to disable attach mode.",
    )
    seed_parser.add_argument(
        "--headless",
        action="store_true",
        help="Run a self-launched browser in headless mode. Ignored when attaching.",
    )
    seed_parser.add_argument(
        "--attach-only",
        action="store_true",
        help="Fail instead of launching a new browser if the debugger session is unavailable.",
    )
    seed_parser.add_argument(
        "--keep-open",
        action="store_true",
        help="Leave the browser open after seeding the watchlist.",
    )
    seed_parser.add_argument(
        "--allow-manual-login",
        action="store_true",
        help="If the page is not ready, wait for a manual Angel login.",
    )
    seed_parser.add_argument(
        "--max-items",
        type=int,
        default=50,
        help="Hard ceiling for watchlist size.",
    )

    return parser


def run_launch_command(args: argparse.Namespace) -> int:
    profile_dir = pathlib.Path(args.profile_dir).expanduser().resolve()
    chrome_binary = resolve_chrome_binary(args.chrome_binary)
    debugger_address = f"127.0.0.1:{args.debug_port}"
    launch_debugger_chrome_session(
        chrome_binary=chrome_binary,
        profile_dir=profile_dir,
        debugger_address=debugger_address,
        url=args.url,
    )

    print(
        textwrap.dedent(
            f"""
            Chrome launched for Angel web trading.

            Remote debugger: {debugger_address}
            Profile dir: {profile_dir}

            Leave that browser open after logging in.
            Later, place an order with:
              python3 angel_web_order_bot.py place --debugger-address {debugger_address} --order-file {DEFAULT_ORDER_PATH}
            """
        ).strip()
    )
    return 0


def run_place_command(args: argparse.Namespace) -> int:
    selectors_path = pathlib.Path(args.selectors).expanduser().resolve()
    order_path = pathlib.Path(args.order_file).expanduser().resolve()
    instrument_file = pathlib.Path(args.instrument_file).expanduser().resolve()
    log_dir = pathlib.Path(args.log_dir).expanduser().resolve()
    profile_dir = pathlib.Path(args.profile_dir).expanduser().resolve()

    selector_config = load_json_file(selectors_path)
    raw_order = load_json_file(order_path)
    order = normalize_order_payload(raw_order, submit_live_override=args.submit_live)
    candidate = resolve_watchlist_candidate(
        instrument_file,
        order.symbol,
        exchange=order.exchange,
    )
    LOGGER.info(
        "Resolved order symbol into watchlist candidate | instrument_file=%s candidate=%s",
        instrument_file,
        json.dumps(candidate.to_log_dict(), sort_keys=True),
    )

    debugger_address = (args.debugger_address or "").strip() or None
    bot = AngelWebOrderBot(
        selector_config,
        url=args.url,
        profile_dir=profile_dir,
        log_dir=log_dir,
        debugger_address=debugger_address,
        headless=args.headless,
        chrome_binary=args.chrome_binary,
        attach_only=args.attach_only,
        keep_open=args.keep_open,
        login_credentials_file=pathlib.Path(args.login_credentials_file).expanduser().resolve(),
        otp_file=pathlib.Path(args.otp_file).expanduser().resolve(),
        otp_timeout_seconds=args.otp_timeout_seconds,
        otp_poll_interval=args.otp_poll_interval,
    )

    try:
        with bot:
            result = bot.place_order(
                order,
                candidate=candidate,
                watchlist_index=args.watchlist_index,
            )
    except Exception:
        LOGGER.exception("Angel web order flow failed")
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def run_seed_watchlist_command(args: argparse.Namespace) -> int:
    selectors_path = pathlib.Path(args.selectors).expanduser().resolve()
    log_dir = pathlib.Path(args.log_dir).expanduser().resolve()
    profile_dir = pathlib.Path(args.profile_dir).expanduser().resolve()
    instrument_file = pathlib.Path(args.instrument_file).expanduser().resolve()

    selector_config = load_json_file(selectors_path)
    candidates = load_watchlist_candidates(
        instrument_file,
        min_days_to_expiry=args.min_days_to_expiry,
    )
    LOGGER.info(
        "Loaded watchlist candidates | instrument_file=%s count=%s min_days_to_expiry=%s",
        instrument_file,
        len(candidates),
        args.min_days_to_expiry,
    )

    debugger_address = (args.debugger_address or "").strip() or None
    bot = AngelWebOrderBot(
        selector_config,
        url=args.url,
        profile_dir=profile_dir,
        log_dir=log_dir,
        debugger_address=debugger_address,
        headless=args.headless,
        chrome_binary=args.chrome_binary,
        attach_only=args.attach_only,
        keep_open=args.keep_open,
        login_credentials_file=pathlib.Path(args.login_credentials_file).expanduser().resolve(),
        otp_file=pathlib.Path(args.otp_file).expanduser().resolve(),
        otp_timeout_seconds=args.otp_timeout_seconds,
        otp_poll_interval=args.otp_poll_interval,
    )

    try:
        with bot:
            result = bot.seed_watchlist(
                candidates,
                watchlist_index=args.watchlist_index,
                max_items=args.max_items,
                allow_manual_login=args.allow_manual_login,
            )
    except Exception:
        LOGGER.exception("Angel watchlist seeding failed")
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    log_path = configure_logging(
        pathlib.Path(args.log_dir).expanduser().resolve(),
        verbose=args.verbose,
    )
    LOGGER.info("Angel web bot started | command=%s log_path=%s", args.command, log_path)

    if args.command == "launch":
        return run_launch_command(args)
    if args.command == "place":
        return run_place_command(args)
    if args.command == "seed-watchlist":
        return run_seed_watchlist_command(args)

    parser.error(f"Unknown command '{args.command}'.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
