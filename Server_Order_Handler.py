"""
This script demonstrates how to place orders with the Angel One (SmartAPI) using Python.
It includes functionality for:
- Reading an instrument details file (CSV) and filtering based on certain criteria (like expiry date).
- Establishing a connection/session to the Angel One API using user credentials.
- Validating and preparing order details (e.g., setting limit price to LTP if Ordertype != MARKET).
- Placing limit or market orders, with a potential fallback to convert unfilled limit orders to market orders.
- Handling contract rollover logic for futures based on a specified RolloverDate.
"""
# package import statement
from SmartApi import SmartConnect
import SmartApi
from contextlib import contextmanager
import pyotp
import time
from datetime import date, datetime
import calendar
import pytz
from Directories import *
import pandas as pd
from Directories import *
from datetime import datetime,timedelta
import json
import logging
import os
import pathlib
import threading
try:
    import fcntl
except ImportError:  # pragma: no cover - non-Unix fallback
    fcntl = None
try:
    import msvcrt
except ImportError:  # pragma: no cover - non-Windows fallback
    msvcrt = None
from angel_browser_guard import inspect_browser_session as InspectAngelBrowserSession
from angel_web_order_bot import (
    APP_URL as ANGEL_WEB_APP_URL,
    DEFAULT_DEBUGGER_ADDRESS as ANGEL_WEB_DEFAULT_DEBUGGER_ADDRESS,
    DEFAULT_INSTRUMENT_FILE as ANGEL_WEB_DEFAULT_INSTRUMENT_FILE,
    DEFAULT_LOGIN_CREDENTIALS_FILE as ANGEL_WEB_DEFAULT_LOGIN_CREDENTIALS_FILE,
    DEFAULT_LOGIN_OTP_FILE as ANGEL_WEB_DEFAULT_LOGIN_OTP_FILE,
    DEFAULT_LOG_DIR as ANGEL_WEB_DEFAULT_LOG_DIR,
    DEFAULT_PROFILE_DIR as ANGEL_WEB_DEFAULT_PROFILE_DIR,
    DEFAULT_SELECTORS_PATH as ANGEL_WEB_DEFAULT_SELECTORS_PATH,
    AngelWebOrderBot,
    load_json_file as LoadAngelWebJson,
    normalize_order_payload as NormalizeAngelWebOrderPayload,
    resolve_watchlist_candidate as ResolveAngelWatchlistCandidate,
)

#Types of Orders
LimitOrder = 'LIMIT'
MarketOrder = 'MARKET'
Logger = logging.getLogger(__name__)
_ANGEL_BROWSER_FIFO_CONDITION = threading.Condition()
_ANGEL_BROWSER_FIFO_NEXT_TICKET = 0
_ANGEL_BROWSER_FIFO_SERVING_TICKET = 0
_ANGEL_INSTRUMENT_MASTER_CACHE = {}
_ANGEL_INSTRUMENT_MASTER_CACHE_LOCK = threading.Lock()


def _SupportsOsFileLock():
    return fcntl is not None or msvcrt is not None


def _TryAcquireOsFileLock(LockFile):
    if fcntl is not None:
        fcntl.flock(LockFile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return

    if msvcrt is not None:
        LockFile.seek(0, os.SEEK_END)
        if LockFile.tell() == 0:
            LockFile.write('\0')
            LockFile.flush()
        LockFile.seek(0)
        msvcrt.locking(LockFile.fileno(), msvcrt.LK_NBLCK, 1)
        return

    raise RuntimeError('Angel browser execution lock is not supported on this platform.')


def _ReleaseOsFileLock(LockFile):
    if fcntl is not None:
        fcntl.flock(LockFile.fileno(), fcntl.LOCK_UN)
        return

    if msvcrt is not None:
        LockFile.seek(0)
        msvcrt.locking(LockFile.fileno(), msvcrt.LK_UNLCK, 1)
        return

    raise RuntimeError('Angel browser execution lock is not supported on this platform.')


def _WriteAngelBrowserLockMetadata(LockFile, Payload):
    Serialized = json.dumps(Payload, default=str, sort_keys=True)
    if msvcrt is not None and fcntl is None:
        LockFile.seek(1)
        LockFile.truncate()
        LockFile.write('\n' + Serialized)
    else:
        LockFile.seek(0)
        LockFile.truncate()
        LockFile.write(Serialized)
    LockFile.flush()


def _OrderLogContext(OrderDetails):
    """Return a compact, non-sensitive snapshot of the order flow state."""
    Keys = [
        'User', 'Broker', 'Exchange', 'Tradingsymbol', 'Symboltoken',
        'Tradetype', 'Ordertype', 'Variety', 'Product', 'Validity',
        'Quantity', 'Price', 'Netposition', 'ContractNameProvided',
        'InstrumentType', 'UpdatedOrderRouting', 'ReEnterOrderLoop',
        'OrderId', 'LastOrderError', 'LastOrderWarning', 'ExecutionRoute',
    ]
    return {Key: OrderDetails.get(Key) for Key in Keys if Key in OrderDetails}


def _LogAngelStep(Message, OrderDetails=None, Level='info', **Extra):
    """Emit structured Angel flow logs without leaking secrets."""
    Payload = {}
    if OrderDetails is not None:
        Payload['order'] = _OrderLogContext(OrderDetails)
    if Extra:
        Payload.update(Extra)

    LogFn = getattr(Logger, Level, Logger.info)
    if Payload:
        LogFn("%s | %s", Message, json.dumps(Payload, default=str, sort_keys=True))
    else:
        LogFn("%s", Message)


def _LoadAngelInstrumentMaster(instrument_file):
    instrument_path = pathlib.Path(instrument_file).expanduser().resolve()
    cache_key = str(instrument_path)
    mtime_ns = instrument_path.stat().st_mtime_ns

    with _ANGEL_INSTRUMENT_MASTER_CACHE_LOCK:
        cached_entry = _ANGEL_INSTRUMENT_MASTER_CACHE.get(cache_key)
        if cached_entry and cached_entry['mtime_ns'] == mtime_ns:
            return cached_entry['dataframe'], True, cache_key

    instrument_master = pd.read_csv(
        instrument_path,
        delimiter=',',
        low_memory=False,
        dtype={'token': 'string'},
    )
    instrument_master.rename(columns={'Unnamed: 0': 'serialnumber'}, inplace=True)
    if 'expiry' in instrument_master.columns:
        instrument_master['expiry'] = pd.to_datetime(
            instrument_master['expiry'].astype('string').str.title(),
            format='%d%b%Y',
            errors='coerce',
        )

    with _ANGEL_INSTRUMENT_MASTER_CACHE_LOCK:
        _ANGEL_INSTRUMENT_MASTER_CACHE[cache_key] = {
            'mtime_ns': mtime_ns,
            'dataframe': instrument_master,
        }

    return instrument_master, False, cache_key


def _FormatAngelApiError(Response, DefaultMessage='Angel API request failed'):
    """Normalize Angel SDK/API errors into a single readable string."""
    if isinstance(Response, dict):
        Message = (
            Response.get('message')
            or Response.get('Message')
            or Response.get('error_message')
            or Response.get('error')
        )
        ErrorCode = Response.get('errorCode') or Response.get('errorcode')

        Data = Response.get('data')
        if isinstance(Data, dict):
            Message = Message or Data.get('message') or Data.get('error_message')
            ErrorCode = ErrorCode or Data.get('errorCode') or Data.get('errorcode')

        if ErrorCode and Message:
            return f'{ErrorCode}: {Message}'
        if Message:
            return str(Message)

    if Response not in (None, ''):
        return str(Response)

    return DefaultMessage


def _GetAngelWebExecutionConfig():
    DefaultLockPath = ANGEL_WEB_DEFAULT_LOG_DIR / 'angel_web_order.lock'
    return {
        'selectors_path': pathlib.Path(
            os.environ.get('ANGEL_WEB_SELECTORS_PATH', str(ANGEL_WEB_DEFAULT_SELECTORS_PATH))
        ).expanduser().resolve(),
        'profile_dir': pathlib.Path(
            os.environ.get('ANGEL_WEB_PROFILE_DIR', str(ANGEL_WEB_DEFAULT_PROFILE_DIR))
        ).expanduser().resolve(),
        'log_dir': pathlib.Path(
            os.environ.get('ANGEL_WEB_LOG_DIR', str(ANGEL_WEB_DEFAULT_LOG_DIR))
        ).expanduser().resolve(),
        'instrument_file': pathlib.Path(
            os.environ.get('ANGEL_WEB_INSTRUMENT_FILE', str(ANGEL_WEB_DEFAULT_INSTRUMENT_FILE))
        ).expanduser().resolve(),
        'login_credentials_file': pathlib.Path(
            os.environ.get('ANGEL_WEB_LOGIN_CREDENTIALS_PATH', str(ANGEL_WEB_DEFAULT_LOGIN_CREDENTIALS_FILE))
        ).expanduser().resolve(),
        'otp_file': pathlib.Path(
            os.environ.get('ANGEL_WEB_LOGIN_OTP_PATH', str(ANGEL_WEB_DEFAULT_LOGIN_OTP_FILE))
        ).expanduser().resolve(),
        'otp_timeout_seconds': int(os.environ.get('ANGEL_WEB_OTP_TIMEOUT_SECONDS', '120')),
        'otp_poll_interval': float(os.environ.get('ANGEL_WEB_OTP_POLL_INTERVAL', '1.0')),
        'debugger_address': (os.environ.get('ANGEL_WEB_DEBUGGER_ADDRESS', ANGEL_WEB_DEFAULT_DEBUGGER_ADDRESS) or '').strip() or None,
        'chrome_binary': (os.environ.get('ANGEL_WEB_CHROME_BINARY', '') or '').strip() or None,
        'headless': str(os.environ.get('ANGEL_WEB_HEADLESS', 'false')).strip().lower() in {'1', 'true', 'yes', 'y'},
        'attach_only': str(os.environ.get('ANGEL_WEB_ATTACH_ONLY', 'false')).strip().lower() in {'1', 'true', 'yes', 'y'},
        'url': os.environ.get('ANGEL_WEB_URL', ANGEL_WEB_APP_URL),
        'watchlist_index': int(os.environ.get('ANGEL_WEB_WATCHLIST_INDEX', '4')),
        'lock_path': pathlib.Path(
            os.environ.get('ANGEL_WEB_LOCK_PATH', str(DefaultLockPath))
        ).expanduser().resolve(),
        'lock_timeout_seconds': float(os.environ.get('ANGEL_WEB_LOCK_TIMEOUT_SECONDS', '30')),
    }


@contextmanager
def _AcquireAngelBrowserExecutionLock(OrderDetails, Config):
    LockPath = pathlib.Path(Config['lock_path'])
    TimeoutSeconds = max(float(Config.get('lock_timeout_seconds', 0) or 0), 0.0)

    if not _SupportsOsFileLock():
        raise RuntimeError('Angel browser execution lock is not supported on this platform.')

    LockPath.parent.mkdir(parents=True, exist_ok=True)
    LockFile = LockPath.open('a+', encoding='utf-8')
    StartTime = time.time()
    Acquired = False

    try:
        while True:
            try:
                _TryAcquireOsFileLock(LockFile)
                Acquired = True
                _WriteAngelBrowserLockMetadata(
                    LockFile,
                    {
                        'pid': os.getpid(),
                        'user': OrderDetails.get('User'),
                        'symbol': OrderDetails.get('Tradingsymbol'),
                        'exchange': OrderDetails.get('Exchange'),
                        'acquired_at': datetime.now().isoformat(),
                    },
                )
                _LogAngelStep(
                    "Acquired Angel browser execution lock",
                    OrderDetails,
                    lock_path=str(LockPath),
                    timeout_seconds=TimeoutSeconds,
                )
                break
            except (BlockingIOError, OSError):
                if (time.time() - StartTime) >= TimeoutSeconds:
                    raise TimeoutError(
                        f"Angel browser execution is busy. lock_path={LockPath} timeout={TimeoutSeconds:.1f}s"
                    )
                time.sleep(0.25)

        yield
    finally:
        if Acquired:
            try:
                if msvcrt is not None and fcntl is None:
                    LockFile.seek(1)
                    LockFile.truncate()
                else:
                    LockFile.seek(0)
                    LockFile.truncate()
                _ReleaseOsFileLock(LockFile)
                _LogAngelStep("Released Angel browser execution lock", OrderDetails, lock_path=str(LockPath))
            except Exception:
                Logger.exception("Failed to release Angel browser execution lock")
        LockFile.close()


def _BuildAngelWebOrderPayload(OrderDetails):
    QuantityForUi = OrderDetails.get('UiQuantityLots', OrderDetails.get('Quantity'))
    QuantityText = str(QuantityForUi).strip()
    if '*' in QuantityText:
        QuantityText = QuantityText.split('*', 1)[0].strip()

    Payload = {
        'exchange': str(OrderDetails['Exchange']).upper(),
        'symbol': str(OrderDetails['Tradingsymbol']).replace(' ', '').upper(),
        'side': str(OrderDetails['Tradetype']).upper(),
        'quantity': int(QuantityText),
        'product': str(OrderDetails['Product']).upper(),
        'order_type': str(OrderDetails['Ordertype']).upper(),
        'validity': str(OrderDetails.get('Validity', 'DAY')).upper(),
        'price': None,
        'trigger_price': None,
        'submit_live': True,
        'allow_manual_login': False,
    }

    if Payload['order_type'] != 'MARKET':
        Price = OrderDetails.get('Price')
        if Price not in (None, '', '0'):
            Payload['price'] = float(Price)

    return Payload


def _ShouldUseAngelBrowserRoute(OrderDetails):
    return str(OrderDetails.get('Exchange', '')).strip().upper() == 'NCDEX'


def _GetAngelBrowserSmartApiPreflightReason(OrderDetails):
    if not _ShouldUseAngelBrowserRoute(OrderDetails):
        return "not_browser_route"

    if str(OrderDetails.get('ReEnterOrderLoop', '')).strip().upper() == 'TRUE':
        return "reentry_requires_smartapi"

    OrderType = str(OrderDetails.get('Ordertype', '')).strip().upper()
    Price = OrderDetails.get('Price')
    if OrderType != 'MARKET' and Price in (None, '', '0', 0, 0.0):
        return "missing_limit_price"

    ContractProvided = str(OrderDetails.get('ContractNameProvided', '')).strip().upper() == 'TRUE'
    if ContractProvided:
        return None

    try:
        if int(OrderDetails.get('Netposition', 0)) != int(OrderDetails.get('Quantity', 0)):
            return "contract_lookup_requires_positions"
    except Exception:
        return "quantity_parse_failed"

    return None


def _CanUseAngelBrowserRouteWithoutSmartApi(OrderDetails):
    """Return True when NCDEX browser flow can skip SmartAPI preflight safely."""
    return _GetAngelBrowserSmartApiPreflightReason(OrderDetails) is None


@contextmanager
def _AcquireAngelBrowserFifoTurn(OrderDetails):
    global _ANGEL_BROWSER_FIFO_NEXT_TICKET
    global _ANGEL_BROWSER_FIFO_SERVING_TICKET

    with _ANGEL_BROWSER_FIFO_CONDITION:
        Ticket = _ANGEL_BROWSER_FIFO_NEXT_TICKET
        _ANGEL_BROWSER_FIFO_NEXT_TICKET += 1
        PendingAhead = max(Ticket - _ANGEL_BROWSER_FIFO_SERVING_TICKET, 0)
        _LogAngelStep(
            "Queued Angel browser request for FIFO execution",
            OrderDetails,
            fifo_ticket=Ticket,
            pending_ahead=PendingAhead,
        )

        while Ticket != _ANGEL_BROWSER_FIFO_SERVING_TICKET:
            _ANGEL_BROWSER_FIFO_CONDITION.wait()

        _LogAngelStep("Starting Angel browser FIFO turn", OrderDetails, fifo_ticket=Ticket)

    try:
        yield
    finally:
        with _ANGEL_BROWSER_FIFO_CONDITION:
            if Ticket == _ANGEL_BROWSER_FIFO_SERVING_TICKET:
                _ANGEL_BROWSER_FIFO_SERVING_TICKET += 1
            _ANGEL_BROWSER_FIFO_CONDITION.notify_all()
            _LogAngelStep(
                "Completed Angel browser FIFO turn",
                OrderDetails,
                fifo_ticket=Ticket,
                next_ticket=_ANGEL_BROWSER_FIFO_SERVING_TICKET,
            )


def PlaceOrderAngelBrowser(OrderDetails):
    Config = _GetAngelWebExecutionConfig()
    _LogAngelStep(
        "Routing Angel order to browser execution",
        OrderDetails,
        debugger_address=Config['debugger_address'],
        chrome_binary=Config.get('chrome_binary'),
        headless=Config.get('headless', False),
        attach_only=Config.get('attach_only', False),
        lock_path=str(Config['lock_path']),
        lock_timeout_seconds=Config['lock_timeout_seconds'],
        watchlist_index=Config['watchlist_index'],
        selectors_path=str(Config['selectors_path']),
    )

    try:
        with _AcquireAngelBrowserExecutionLock(OrderDetails, Config):
            SelectorConfig = LoadAngelWebJson(Config['selectors_path'])
            GuardResult = InspectAngelBrowserSession(
                selector_config=SelectorConfig,
                debugger_address=Config['debugger_address'],
                profile_dir=Config['profile_dir'],
                log_dir=Config['log_dir'],
                url=Config['url'],
                chrome_binary=Config.get('chrome_binary'),
                headless=Config.get('headless', False),
                attach_only=Config.get('attach_only', False),
                seed_watchlist=False,
                attempt_login=True,
                login_credentials_file=Config['login_credentials_file'],
                otp_file=Config['otp_file'],
                otp_timeout_seconds=Config['otp_timeout_seconds'],
                otp_poll_interval=Config['otp_poll_interval'],
                instrument_file=Config['instrument_file'],
                watchlist_index=Config['watchlist_index'],
                min_days_to_expiry=6,
                max_items=50,
            )
            if GuardResult.get('status') == 'BROWSER_UNAVAILABLE':
                OrderDetails['LastOrderError'] = (
                    f"Angel browser session is not ready: {GuardResult.get('status')}"
                )
                _LogAngelStep(
                    "Angel browser session unavailable",
                    OrderDetails,
                    Level='error',
                    guard=GuardResult,
                )
                return None

            Payload = _BuildAngelWebOrderPayload(OrderDetails)
            OrderRequest = NormalizeAngelWebOrderPayload(Payload, submit_live_override=True)
            Candidate = ResolveAngelWatchlistCandidate(
                Config['instrument_file'],
                OrderRequest.symbol,
                exchange=OrderRequest.exchange,
            )

            Bot = AngelWebOrderBot(
                SelectorConfig,
                url=Config['url'],
                profile_dir=Config['profile_dir'],
                log_dir=Config['log_dir'],
                debugger_address=Config['debugger_address'],
                headless=Config.get('headless', False),
                chrome_binary=Config.get('chrome_binary'),
                attach_only=Config.get('attach_only', False),
                keep_open=True,
                login_credentials_file=Config['login_credentials_file'],
                otp_file=Config['otp_file'],
                otp_timeout_seconds=Config['otp_timeout_seconds'],
                otp_poll_interval=Config['otp_poll_interval'],
            )
            with Bot:
                Result = Bot.place_order(
                    OrderRequest,
                    candidate=Candidate,
                    watchlist_index=Config['watchlist_index'],
                )

        if Result.get('status') != 'submitted':
            FailureMessage = Result.get('message') or f"Status={Result.get('status')}"
            OrderDetails['LastOrderError'] = (
                f"Angel browser order was not submitted. {FailureMessage}"
            )
            _LogAngelStep(
                "Angel browser order did not submit",
                OrderDetails,
                Level='error',
                result=Result,
            )
            return None

        OrderDetails['ExecutionRoute'] = 'ANGEL_WEB'
        ResultMessage = Result.get('message') or ''
        if 'order scheduled' in ResultMessage.lower():
            OrderDetails['OrderId'] = 'ANGEL_WEB_SCHEDULED'
        else:
            OrderDetails['OrderId'] = 'ANGEL_WEB_SUBMITTED'
        OrderDetails['BrowserOrderArtifacts'] = Result.get('artifacts')
        _LogAngelStep("Angel browser order submitted", OrderDetails, browser_result=Result)
        return Result
    except TimeoutError as Exc:
        OrderDetails['LastOrderError'] = str(Exc)
        _LogAngelStep(
            "Angel browser execution lock unavailable",
            OrderDetails,
            Level='error',
            lock_path=str(Config['lock_path']),
            lock_timeout_seconds=Config['lock_timeout_seconds'],
        )
        return None
    except Exception as Exc:
        OrderDetails['LastOrderError'] = str(Exc)
        Logger.exception("Unhandled exception during Angel browser order placement")
        return None

def ConfigureNetDirectionOfTrade(OrderDetails):
    if OrderDetails['Tradetype'].strip().upper() == 'BUY':
        OrderDetails['NetDirection'] = 1
    elif OrderDetails['Tradetype'].strip().upper() == 'SELL':
        OrderDetails['NetDirection'] = -1
    _LogAngelStep("Configured Angel net direction", OrderDetails)
    return OrderDetails

def PrepareInstrumentContractName(smartAPI, OrderDetails):
    """
    This function determines the broker from the OrderDetails and
    calls the respective instrument contract preparation function.
    """
    
    # Check broker type in the order details
    if OrderDetails['Broker'] == 'ANGEL':
        # If broker is Angel, prepare instrument contract for Angel
        _LogAngelStep("Preparing Angel contract details", OrderDetails)
        AngelInstrument_filtered = PrepareAngelInstrumentContractName(smartAPI,OrderDetails)    

        if AngelInstrument_filtered.empty:
            OrderDetails['LastOrderError'] = (
                f"Unable to resolve Angel contract details for "
                f"{OrderDetails.get('Tradingsymbol')} on {OrderDetails.get('Exchange')}."
            )
            print(OrderDetails['LastOrderError'])
            _LogAngelStep("Angel contract resolution failed", OrderDetails, Level='error')
            return OrderDetails

        UpdateRequestContractDetailsAngel(OrderDetails, AngelInstrument_filtered)
        _LogAngelStep("Angel contract details prepared", OrderDetails)

        return OrderDetails


def _ComputeTradingDaysRolloverDateAngel(Today, N, Exchange):
    """Return the calendar date that is N trading days after Today for Angel.

    Mirrors the helper used by the Kite handler; keeping the picker
    calendar aligned with the rollover monitor prevents already-rolled
    front-month contracts from being reselected across weekends/holidays.
    """
    # Deferred import for the same reason as the Kite helper — avoids pulling
    # Holidays into sys.modules at module load, which would defeat test
    # harnesses that stub it conditionally.
    from Holidays import CheckForDateHoliday

    try:
        N = int(N)
    except (TypeError, ValueError):
        N = 0
    if isinstance(Today, datetime):
        Current = Today
    else:
        Current = datetime.combine(Today, datetime.min.time())
    Remaining = N
    while Remaining > 0:
        Current = Current + timedelta(days=1)
        if Current.weekday() >= 5:
            continue
        if CheckForDateHoliday(Current.date(), exchange=Exchange):
            continue
        Remaining -= 1
    return Current


def _FindPinnedRolloverContractAngel(OrderDetails, AngelInstrumentDetails, Today):
    """If a rollover has already completed for this instrument, return the CSV
    row for the new_contract.  Empty DataFrame when no pin applies.
    """
    try:
        import forecast_db as _db  # deferred to keep import graph light
        Rows = _db.GetRecentCompletedRollovers(limit=30, Broker='ANGEL')
    except Exception as Exc:
        Logger.warning("Angel rollover DB lookup failed: %s", Exc)
        return pd.DataFrame()

    if not Rows:
        return pd.DataFrame()

    TargetName = OrderDetails.get('Tradingsymbol')
    TargetExchange = OrderDetails.get('Exchange')
    TargetInstType = OrderDetails.get('InstrumentType')

    for Row in Rows:
        NewContract = Row.get('new_contract')
        if not NewContract:
            continue
        Match = AngelInstrumentDetails[
            (AngelInstrumentDetails['symbol'] == NewContract) &
            (AngelInstrumentDetails['name'] == TargetName) &
            (AngelInstrumentDetails['exch_seg'] == TargetExchange) &
            (AngelInstrumentDetails['instrumenttype'] == TargetInstType) &
            (AngelInstrumentDetails['expiry'] > Today)
        ]
        if not Match.empty:
            Logger.info(
                "Pinning Angel order for %s to rolled-over contract %s (DB row id=%s)",
                TargetName, NewContract, Row.get('id')
            )
            return Match.sort_values(by='expiry', ascending=True).head(1)
    return pd.DataFrame()


def PrepareAngelInstrumentContractName(smartAPI,OrderDetails):
    """
    Reads the instrument details from AngelInstrumentDirectory CSV,
    applies filtering logic based on OrderDetails, and returns
    the filtered DataFrame.
    """

    AngelInstrumentDetails, CacheHit, CachePath = _LoadAngelInstrumentMaster(AngelInstrumentDirectory)
    _LogAngelStep(
        "Loaded Angel instrument master",
        OrderDetails,
        path=CachePath,
        rows=len(AngelInstrumentDetails),
        cached=CacheHit,
    )

    # Current datetime for reference
    today = datetime.now()

    # Compute the rollover date using *trading* days so the picker aligns with
    # the rollover_monitor's calendar and we don't re-select the front-month
    # contract after it has already been rolled out.
    RolloverDate = _ComputeTradingDaysRolloverDateAngel(
        today,
        OrderDetails['DaysPostWhichSelectNextContract'],
        OrderDetails.get('Exchange'),
    )

    # If rollover_monitor has already completed a rollover for this instrument,
    # pin the order to the new_contract.  DB is authoritative once status is
    # COMPLETE.
    PinnedMatch = _FindPinnedRolloverContractAngel(
        OrderDetails, AngelInstrumentDetails, today
    )
    if not PinnedMatch.empty:
        _LogAngelStep(
            "Pinned Angel order to completed-rollover contract",
            OrderDetails,
            pinned_symbol=PinnedMatch.iloc[0].get('symbol'),
            pinned_token=PinnedMatch.iloc[0].get('token'),
        )
        return PinnedMatch

    AngelInstrumentDetails_filtered = pd.DataFrame()

    # If Netposition == '0', filter by expiry > today and pick the nearest expiry 
    if ((int(OrderDetails['Netposition']) != int(OrderDetails['Quantity'])) or (OrderDetails.get('ReEnterOrderLoop') == 'True')):

        if int(OrderDetails['Netposition']) == 0:
            AngelInstrumentDetails_filtered = CheckIfExistingOldContractSqOffReqAngel(smartAPI,AngelInstrumentDetails,OrderDetails,today,RolloverDate)

        else:
            if OrderDetails.get('ReEnterOrderLoop') == 'True':
                OrderDetails['Quantity'] = OrderDetails['QuantityToBePlacedInNextRound']
                OrderDetails['ReEnterOrderLoop'] == 'False'
                OrderDetails['Tradingsymbol'] = OrderDetails['InitialTradingsymbol']
                
            
            else:
                OrderDetails['InitialTradingsymbol'] = OrderDetails['Tradingsymbol']

                AngelInstrumentDetails_filtered = CheckIfExistingOldContractSqOffReqAngel(smartAPI,AngelInstrumentDetails,OrderDetails,today,RolloverDate)
                if not AngelInstrumentDetails_filtered.empty:
                    OrderDetails['ReEnterOrderLoop'] = 'True'

                    NoOfContractsInOldMonthFormat = int(AngelInstrumentDetails_filtered['netqty'].iloc[0])
                    NoOfContractsInNewMonthFormatToPlaceOrders = int(OrderDetails['Quantity']) 

                    if NoOfContractsInNewMonthFormatToPlaceOrders > NoOfContractsInOldMonthFormat:
                        InitialOrderQuantity = NoOfContractsInOldMonthFormat#NoOfContractsInNewMonthFormatToPlaceOrders
                        NetQuantityOrdersToBePlaced = NoOfContractsInNewMonthFormatToPlaceOrders - abs(NoOfContractsInOldMonthFormat)

                    else:
                        InitialOrderQuantity = NoOfContractsInNewMonthFormatToPlaceOrders
                        NetQuantityOrdersToBePlaced = NoOfContractsInOldMonthFormat - abs(NoOfContractsInNewMonthFormatToPlaceOrders)

                    if InitialOrderQuantity < 0:
                        InitialOrderQuantity = InitialOrderQuantity * -1
                        
                    OrderDetails['Quantity'] = InitialOrderQuantity
                    OrderDetails['QuantityToBePlacedInNextRound'] = NetQuantityOrdersToBePlaced




    if AngelInstrumentDetails_filtered.empty:
        RequestedTradingsymbol = str(OrderDetails['Tradingsymbol']).replace(" ","").upper()

        AngelInstrumentDetails_filtered = AngelInstrumentDetails[
            (AngelInstrumentDetails['symbol'] == RequestedTradingsymbol) &
            (AngelInstrumentDetails['exch_seg'] == OrderDetails['Exchange']) &
            (AngelInstrumentDetails['instrumenttype'] == OrderDetails['InstrumentType']) &
            (
                AngelInstrumentDetails['expiry'].isna() |
                (AngelInstrumentDetails['expiry'] >= today)
            )
        ].sort_values(by='expiry', ascending=True).head(1)
        _LogAngelStep(
            "Checked exact symbol match in Angel instrument master",
            OrderDetails,
            requested_symbol=RequestedTradingsymbol,
            exact_match_rows=len(AngelInstrumentDetails_filtered),
        )

    if AngelInstrumentDetails_filtered.empty:
        AngelInstrumentDetails_filtered = AngelInstrumentDetails[
            (AngelInstrumentDetails['name'] == OrderDetails['Tradingsymbol']) &
            (AngelInstrumentDetails['exch_seg'] == OrderDetails['Exchange']) &
            (AngelInstrumentDetails['instrumenttype'] == OrderDetails['InstrumentType']) &
            (AngelInstrumentDetails['expiry'] > RolloverDate)
        ].sort_values(by='expiry', ascending=True).head(1)

    if not AngelInstrumentDetails_filtered.empty:
        Selected = AngelInstrumentDetails_filtered.iloc[0].to_dict()
        _LogAngelStep(
            "Selected Angel instrument contract",
            OrderDetails,
            selected_symbol=Selected.get('symbol'),
            selected_token=Selected.get('token'),
            selected_name=Selected.get('name'),
            selected_expiry=Selected.get('expiry'),
        )
    else:
        _LogAngelStep(
            "No Angel instrument contract matched request",
            OrderDetails,
            today=today,
            rollover_date=RolloverDate,
            Level='warning',
        )
    
    return AngelInstrumentDetails_filtered

def CheckIfExistingOldContractSqOffReqAngel(smartAPI, AngelInstrumentDetails, OrderDetails, today, RolloverDate):
    """
    Checks if there's an old contract that requires square-off in the specified date range.
    Filters the instrument details based on the OrderDetails, then compares it against
    existing Angel positions to see if there's a matching position to square off.
    
    :param smartAPI:      The authenticated Angel One (SmartAPI) session object.
    :param AngelInstrumentDetails: A DataFrame containing instrument details (symbol, token, expiry, etc.).
    :param OrderDetails:  A dictionary with order-related details (Tradingsymbol, Exchange, InstrumentType, etc.).
    :param today:         The current date/time (datetime object).
    :param RolloverDate:  The rollover deadline date/time (datetime object).
    :return:              A filtered DataFrame of positions matching the old contract criteria. 
                          Returns an empty DataFrame if none match.
    """
    
    # Step 1: Filter the contracts based on the given criteria
    # Match the symbol, exchange, and instrument type, and filter by expiry date range.
    AngelInstrumentDetails_filtered = AngelInstrumentDetails[
        (AngelInstrumentDetails['name'] == OrderDetails['Tradingsymbol']) &  # Match the trading symbol
        (AngelInstrumentDetails['exch_seg'] == OrderDetails['Exchange']) &  # Match the exchange segment
        (AngelInstrumentDetails['instrumenttype'] == OrderDetails['InstrumentType']) &  # Match the instrument type
        (AngelInstrumentDetails['expiry'] >= today) &  # Ensure the contract has not expired
        (AngelInstrumentDetails['expiry'] <= RolloverDate)  # Ensure the contract is within the rollover period
    ].sort_values(by='expiry', ascending=True).head(1)  # Sort by expiry and pick the earliest

    # Step 2: Check if any matching contract exists
    if not AngelInstrumentDetails_filtered.empty:
        # Fetch existing positions from Angel for the given order details
        AngelPositionsDetails = FetchExistingAngelPositions(smartAPI, OrderDetails)
        AngelPositions = pd.DataFrame(AngelPositionsDetails)

        AngelPositionsData = pd.DataFrame(AngelPositions['data'].tolist())

        AngelPositionsData['netqty'] = pd.to_numeric(AngelPositionsData['netqty'], errors='coerce')

        # 1. Determine the comparison condition based on Tradetype
        if str(OrderDetails['Tradetype']).upper() == 'BUY':
            comparison_condition = (AngelPositionsData['netqty'] < OrderDetails['NetDirection'])
        else:
            comparison_condition = (AngelPositionsData['netqty'] > OrderDetails['NetDirection'])

        # 2. Apply the condition in the DataFrame filter
        AngelPositionsFiltered = AngelPositionsData[
            (AngelPositionsData['symboltoken'] == AngelInstrumentDetails_filtered['token'].iloc[0]) &
            (AngelPositionsData['netqty'] != 0) &
            comparison_condition
        ].copy()
        _LogAngelStep(
            "Checked Angel old-contract square-off requirement",
            OrderDetails,
            matched_contract_rows=len(AngelInstrumentDetails_filtered),
            fetched_position_rows=len(AngelPositionsData),
            squareoff_match_rows=len(AngelPositionsFiltered),
        )


        # Step 3: If there are matching positions, return the filtered positions
        if not AngelPositionsFiltered.empty:
            # Rename columns to standardize naming for further processing
            AngelPositionsFiltered.rename(columns={'symbol': 'instrument_name', 'tradingsymbol': 'symbol', 'instrument_token': 'token', 'symboltoken': 'token'}, inplace=True)
            # Return the filtered positions DataFrame
            return AngelPositionsFiltered
        else:
            return pd.DataFrame()     
    else:
        # If no matching contract is found, return an empty DataFrame
        return pd.DataFrame()  # Ensure an empty DataFrame is returned for consistency


def FetchExistingAngelPositions(smartAPI, OrderDetails):
    """
    Fetches the user's existing positions from the Angel One (SmartAPI).
    
    :param smartAPI:     The authenticated Angel One (SmartAPI) session object.
    :param OrderDetails: A dictionary containing order-related details (not used in this function directly).
    :return:             A pandas DataFrame containing all current positions.
    """
    # The 'position()' method returns a list/dict of positions. We convert to a DataFrame for easier handling
    positions = smartAPI.position()
    AngelInstrument_positions = pd.DataFrame(positions)
    PositionCount = 0
    if isinstance(positions, dict) and isinstance(positions.get('data'), list):
        PositionCount = len(positions.get('data', []))
    _LogAngelStep("Fetched Angel positions", OrderDetails, position_rows=PositionCount)

    return AngelInstrument_positions


def UpdateRequestContractDetailsAngel(OrderDetails, AngelInstrument_filtered):
    """
    Updates the OrderDetails dictionary with the new contract
    (symbol and token) from the filtered DataFrame.
    """
    
    # Retrieve the first row's symbol and token values
    OrderDetails['Tradingsymbol'] = AngelInstrument_filtered['symbol'].iloc[0]
    OrderDetails['Symboltoken'] = AngelInstrument_filtered['token'].iloc[0]
    _LogAngelStep(
        "Updated order with resolved Angel contract",
        OrderDetails,
        resolved_symbol=OrderDetails['Tradingsymbol'],
        resolved_token=OrderDetails['Symboltoken'],
    )

    return OrderDetails


#Function to establish a connection with the API
def EstablishConnectionAngelAPI(OrderDetails):
    # This function reads credentials from the specified file and generates a session for Angel API

    UserCode = str(OrderDetails.get('User', '')).strip()
    _LogAngelStep("Establishing Angel API session", OrderDetails, user_code=UserCode)

    CredentialDirectoryByUser = {
        'R71302': AngelNararushLoginCred,
        'AABM826021': AngelEshitaLoginCred,
    }

    Directory = CredentialDirectoryByUser.get(UserCode)
    if Directory is None:
        raise ValueError(
            f"Unsupported Angel user '{UserCode}'. "
            f"Expected one of: {', '.join(sorted(CredentialDirectoryByUser))}"
        )
    _LogAngelStep("Resolved Angel credential file", OrderDetails, credential_file=str(Directory))
    
    # Open the credentials file and read all lines
    with open(Directory,'r') as a:
        content = a.readlines()
        a.close() 
    api_key = content[0].strip('\n')
    clientId = content[1].strip('\n')
    pwd = content[2].strip('\n')
    smartApi = SmartConnect(api_key)
    token = content[3].strip('\n')
    totp=pyotp.TOTP(token).now()
    _LogAngelStep(
        "Generated Angel TOTP and SmartConnect client",
        OrderDetails,
        client_id_masked=(clientId[:3] + "***" + clientId[-3:]) if len(clientId) >= 6 else "***",
    )

    # login api call
    data = smartApi.generateSession(clientId, pwd, totp)
    _LogAngelStep(
        "Angel session generated",
        OrderDetails,
        login_status=data.get('status') if isinstance(data, dict) else None,
        login_message=data.get('message') if isinstance(data, dict) else None,
    )

    SessionData = data.get('data') if isinstance(data, dict) else None
    if not isinstance(SessionData, dict):
        ErrorMessage = _FormatAngelApiError(data, 'Angel login failed.')
        OrderDetails['LastOrderError'] = f'Angel login failed: {ErrorMessage}'
        _LogAngelStep("Angel login failed", OrderDetails, Level='error', login_response=data)
        raise RuntimeError(OrderDetails['LastOrderError'])

    authToken = SessionData.get('jwtToken')
    refreshToken = SessionData.get('refreshToken')
    if not authToken or not refreshToken:
        ErrorMessage = _FormatAngelApiError(data, 'Angel login response missing session tokens.')
        OrderDetails['LastOrderError'] = f'Angel login failed: {ErrorMessage}'
        _LogAngelStep("Angel login missing tokens", OrderDetails, Level='error', login_response=data)
        raise RuntimeError(OrderDetails['LastOrderError'])

    # fetch the feedtoken
    feedToken = smartApi.getfeedToken()

    # fetch User Profile
    res = smartApi.getProfile(refreshToken)
    smartApi.generateToken(refreshToken)
    ProfileData = res.get('data') if isinstance(res, dict) else None
    if not isinstance(ProfileData, dict):
        ErrorMessage = _FormatAngelApiError(res, 'Angel profile fetch failed.')
        OrderDetails['LastOrderError'] = f'Angel profile fetch failed: {ErrorMessage}'
        _LogAngelStep("Angel profile fetch failed", OrderDetails, Level='error', profile_response=res)
        raise RuntimeError(OrderDetails['LastOrderError'])

    res = ProfileData.get('exchanges')
    _LogAngelStep("Angel profile fetched", OrderDetails, exchanges=res)

    return smartApi

#Function to handle disreparency in quantity and lotsizes for order to be placed
def Validate_Quantity(OrderDetails):
    # This function adjusts the quantity if it's given in a multiplier format like "2*50"
    
    Quantitysplit = str(OrderDetails['Quantity']).split('*')

    #If there is any disreparency between the total quantity and lotsize then correct it
    if len(Quantitysplit)>1:
        if OrderDetails.get('UiQuantityLots') in (None, '', 0, '0'):
            OrderDetails['UiQuantityLots'] = int(Quantitysplit[0])
        UpdatedQuantity = int(Quantitysplit[0]) * int(Quantitysplit[1])
        UpdatedNetQuantity = int(OrderDetails['Netposition']) * int(Quantitysplit[1])
        
        OrderDetails['Quantity'] = UpdatedQuantity 
        OrderDetails['Netposition'] = UpdatedNetQuantity 
        _LogAngelStep("Expanded Angel quantity multiplier", OrderDetails)
        
    
    return OrderDetails

#Function to place order on Angel Broking account
def PlaceOrderAngelAPI(smartApi,OrderDetails):
    print('Order details in place order')
    print(OrderDetails)
    _LogAngelStep("Entering Angel place order", OrderDetails)
    #place order
    try:
        # Prepare the request parameters for placing the order through the Angel API
        orderparams = {
            "variety":str(OrderDetails['Variety']),#Kind of order AMO/NORMAL ...
            "tradingsymbol":str(OrderDetails['Tradingsymbol']).replace(" ","").upper(),#The intrument name
            "symboltoken":str(OrderDetails['Symboltoken']),#Symbol token
            "transactiontype":str(OrderDetails['Tradetype']).upper(),#Buy/Sell
            "exchange":str(OrderDetails['Exchange']),#Exchange to place the order on
            "ordertype":str(OrderDetails['Ordertype']),#LIMIT/MARKET.. Order
            "producttype":str(OrderDetails['Product']),#CARRYFORWARD for futures
            "duration":str(OrderDetails['Validity']),#DAY
            "price":str(OrderDetails['Price']) or "0",
            "squareoff":str(OrderDetails['Squareoff']) or "0",
            "stoploss":str(OrderDetails['Stoploss']) or "0",
            "quantity":str(OrderDetails['Quantity'])#Quantity according to angel one multiplier set
            }
        _LogAngelStep("Prepared Angel order params", OrderDetails, orderparams=orderparams)

        RawPostRequest = getattr(smartApi, '_postRequest', None)
        OrderResponse = None

        if callable(RawPostRequest):
            OrderResponse = RawPostRequest("api.order.place", dict(orderparams))
            _LogAngelStep("Received Angel raw place-order response", OrderDetails, raw_response=OrderResponse)
        else:
            PlaceOrderFullResponse = getattr(smartApi, 'placeOrderFullResponse', None)
            if callable(PlaceOrderFullResponse):
                OrderResponse = PlaceOrderFullResponse(dict(orderparams))
                _LogAngelStep("Received Angel full place-order response", OrderDetails, raw_response=OrderResponse)
            else:
                OrderIdDetails = smartApi.placeOrder(orderparams)
                if OrderIdDetails:
                    _LogAngelStep("Angel placeOrder returned order id", OrderDetails, order_id=OrderIdDetails)
                    return OrderIdDetails
                OrderDetails['LastOrderError'] = 'Angel placeOrder returned no order id.'
                print("Order placement failed: {}".format(OrderDetails['LastOrderError']))
                _LogAngelStep("Angel placeOrder returned no order id", OrderDetails, Level='error')
                return None

        if isinstance(OrderResponse, dict):
            OrderStatus = OrderResponse.get('status')
            if OrderStatus is None:
                OrderStatus = OrderResponse.get('success')

            if OrderStatus:
                OrderData = OrderResponse.get('data')
                if isinstance(OrderData, dict) and OrderData.get('orderid'):
                    _LogAngelStep("Angel order accepted", OrderDetails, order_id=OrderData['orderid'])
                    return OrderData['orderid']

                OrderDetails['LastOrderError'] = _FormatAngelApiError(
                    OrderResponse,
                    'Angel order response was successful but did not include an order id.'
                )
            else:
                OrderDetails['LastOrderError'] = _FormatAngelApiError(OrderResponse)
        elif OrderResponse:
            return OrderResponse
        else:
            OrderDetails['LastOrderError'] = 'Angel API returned an empty order response.'

        print("Order placement failed: {}".format(OrderDetails['LastOrderError']))
        _LogAngelStep("Angel order placement failed", OrderDetails, raw_response=OrderResponse, Level='error')
    except Exception as e:
        OrderDetails['LastOrderError'] = str(e)
        print("Order placement failed: {}".format(str(e)))
        Logger.exception("Unhandled exception during Angel order placement")

    return None

#Function to place market order if the limit order failed
def ConvertToMarketOrder(smartApi,OrderDetails):
    # Converts the existing order details to a market order by setting price=0 and ordertype=MARKET
    
    OrderDetails['Price'] = '0'
    OrderDetails['Ordertype'] = MarketOrder

    PlaceOrderAngelAPI(smartApi,OrderDetails)


def SleepForRequiredTime(SleepTime):
    # Simple utility function to pause execution for a specified time in seconds
    time.sleep(SleepTime)
    return True

#Function to place Limit order first then if not filled , re-place Market Order
def PrepareOrderAngel(smartApi,OrderDetails):
    # This function checks the current LTP and uses it to set the limit order price if needed
    
    exchange = str(OrderDetails['Exchange'])
    tradingsymbol = str(OrderDetails['Tradingsymbol'])
    symboltoken = str(OrderDetails['Symboltoken'])
    _LogAngelStep(
        "Fetching Angel LTP before order placement",
        OrderDetails,
        ltp_request={
            'exchange': exchange,
            'tradingsymbol': tradingsymbol,
            'symboltoken': symboltoken,
        },
    )

    LtpInfo = smartApi.ltpData(exchange=exchange,tradingsymbol=tradingsymbol,symboltoken=symboltoken)

    Instrumentdata = LtpInfo.get('data') if isinstance(LtpInfo, dict) else None
    if not isinstance(Instrumentdata, dict) or Instrumentdata.get('ltp') in (None, ''):
        OrderDetails['LastOrderError'] = _FormatAngelApiError(
            LtpInfo,
            f'Unable to fetch LTP for {tradingsymbol} on {exchange}.'
        )
        print('LTP fetch failed')
        print(OrderDetails['LastOrderError'])
        _LogAngelStep("Angel LTP fetch failed", OrderDetails, ltp_response=LtpInfo, Level='error')
        return OrderDetails

    print('LTP Info')
    print(LtpInfo)
    _LogAngelStep("Angel LTP fetched", OrderDetails, ltp_response=LtpInfo)

    # Preserve an explicit limit price; only default to LTP when the request did not provide one.
    if OrderDetails['Ordertype'] != 'MARKET' and OrderDetails.get('Price') in (None, '', '0', 0, 0.0):
        OrderDetails['Price'] = Instrumentdata['ltp']

    return OrderDetails


def ModifyAngeOrder(smartAPI, OrderDetails):
    """
    Modifies an existing Angel One order by sending updated parameters
    to the SmartAPI modifyOrder endpoint.

    :param smartAPI:      The authenticated SmartAPI session object.
    :param OrderDetails:  A dictionary containing the details needed to modify the order.
                          Must include:
                           - Variety (e.g., "NORMAL", "STOPLOSS")
                           - OrderId (the existing order ID to modify)
                           - Tradingsymbol (symbol name used in the original order)
                           - Symboltoken (token for the symbol)
                           - Tradetype ("BUY" or "SELL")
                           - Exchange (e.g., "MCX")
                           - Ordertype ("MARKET", "LIMIT", "SL", etc.)
                           - Product (e.g., "CARRYFORWARD")
                           - Validity ("DAY", "IOC", etc.)
                           - Quantity (desired quantity to modify)
                           - Price (0 for market or limit price if needed)
    """

    # Prepare the parameters for the modifyOrder API call
    ModifyOrderParams = {
        "variety":         OrderDetails['Variety'],
        "orderid":         OrderDetails['OrderId'],     # The existing order ID
        "tradingsymbol":   OrderDetails['Tradingsymbol'],
        "symboltoken":     OrderDetails['Symboltoken'],
        "transactiontype": OrderDetails['Tradetype'],   # "BUY" or "SELL"
        "exchange":        OrderDetails['Exchange'],    # e.g., "MCX"
        "ordertype":       OrderDetails['Ordertype'],   # e.g., "MARKET", "LIMIT"
        "producttype":     OrderDetails['Product'],     # e.g., "CARRYFORWARD"
        "duration":        OrderDetails['Validity'],    # "DAY", "IOC", etc.
        "quantity":        OrderDetails['Quantity'],    # The updated order quantity
        "price":           OrderDetails['Price']        # 0 if MARKET order, else limit price
    }

    # Send the modify request to the API
    _LogAngelStep("Sending Angel modify order", OrderDetails, modify_params=ModifyOrderParams)
    response = smartAPI.modifyOrder(ModifyOrderParams)

    # Print the response to see if the modification succeeded or failed
    print(response)
    _LogAngelStep("Received Angel modify order response", OrderDetails, modify_response=response)


def _ExecuteAngelBrowserOrderFlow(smartAPI, OrderDetails):
    BrowserPlacements = []
    BrowserResult = PlaceOrderAngelBrowser(OrderDetails)
    if not BrowserResult:
        _LogAngelStep("Stopping Angel flow after browser order placement failure", OrderDetails, Level='error')
        return None
    BrowserPlacements.append(BrowserResult)

    if OrderDetails.get('ReEnterOrderLoop') == 'True':
        PrepareInstrumentContractName(smartAPI,OrderDetails)
        if OrderDetails.get('LastOrderError'):
            _LogAngelStep("Stopping Angel browser re-entry flow after contract resolution failure", OrderDetails, Level='error')
            return {
                'status': 'partial_failure',
                'placements': BrowserPlacements,
                'error': OrderDetails.get('LastOrderError'),
            }
        
        OrderDetails = PrepareOrderAngel(smartAPI, OrderDetails)
        if OrderDetails.get('LastOrderError'):
            _LogAngelStep("Stopping Angel browser re-entry flow after LTP failure", OrderDetails, Level='error')
            return {
                'status': 'partial_failure',
                'placements': BrowserPlacements,
                'error': OrderDetails.get('LastOrderError'),
            }
        BrowserReEntryResult = PlaceOrderAngelBrowser(OrderDetails)
        if not BrowserReEntryResult:
            _LogAngelStep("Stopping Angel browser re-entry flow after order placement failure", OrderDetails, Level='error')
            return {
                'status': 'partial_failure',
                'placements': BrowserPlacements,
                'error': OrderDetails.get('LastOrderError'),
            }
        BrowserPlacements.append(BrowserReEntryResult)

    WarningMessage = None
    if str(OrderDetails.get('ConvertToMarketOrder', '')).upper() == 'TRUE' and str(OrderDetails['Ordertype']).upper() != 'MARKET':
        WarningMessage = (
            'Angel browser routing submitted the initial limit order only; '
            'post-submit limit-to-market conversion is not supported in this route.'
        )
        OrderDetails['LastOrderWarning'] = WarningMessage
        _LogAngelStep("Angel browser route skipped post-submit limit conversion", OrderDetails, Level='warning')

    ResultPayload = {
        'status': 'submitted',
        'placements': BrowserPlacements,
    }
    if WarningMessage:
        ResultPayload['warning'] = WarningMessage

    _LogAngelStep("Completed Angel browser order flow", OrderDetails, placements=len(BrowserPlacements))
    return ResultPayload


def _ExecuteAngelSmartApiOrderFlow(smartAPI, OrderDetails):
    OrderIdDetails = PlaceOrderAngelAPI(smartAPI, OrderDetails)
    if not OrderIdDetails:
        _LogAngelStep("Stopping Angel flow after SmartAPI order placement failure", OrderDetails, Level='error')
        return None

    OrderDetails['OrderId'] = OrderIdDetails
    _LogAngelStep("Angel SmartAPI order id assigned to request", OrderDetails)

    if OrderDetails['Ordertype'] == 'MARKET':
        if OrderDetails.get('ReEnterOrderLoop') == 'True':
            PrepareInstrumentContractName(smartAPI,OrderDetails)
            if OrderDetails.get('LastOrderError'):
                _LogAngelStep("Stopping Angel SmartAPI re-entry flow after contract resolution failure", OrderDetails, Level='error')
                return None
            
            OrderDetails = PrepareOrderAngel(smartAPI, OrderDetails)
            if OrderDetails.get('LastOrderError'):
                _LogAngelStep("Stopping Angel SmartAPI re-entry flow after LTP failure", OrderDetails, Level='error')
                return None
            OrderIdDetails = PlaceOrderAngelAPI(smartAPI, OrderDetails)
            if not OrderIdDetails:
                _LogAngelStep("Stopping Angel SmartAPI re-entry flow after order placement failure", OrderDetails, Level='error')
                return None
            OrderDetails['OrderId'] = OrderIdDetails
            _LogAngelStep("Completed Angel SmartAPI re-entry flow", OrderDetails)
            return OrderDetails  
        _LogAngelStep("Completed Angel SmartAPI market order flow", OrderDetails)
        return OrderIdDetails
    else:
        if OrderDetails['ConvertToMarketOrder'] == 'True':
            if int(OrderDetails['Netposition']) != 0:
                print(f'Waiting for {OrderDetails["EntrySleepDuration"]} seconds')
                SleepForRequiredTime(int(OrderDetails['EntrySleepDuration']))
            else:
                print(f'Waiting for {OrderDetails["ExitSleepDuration"]} seconds')
                SleepForRequiredTime(int(OrderDetails['ExitSleepDuration']))
            _LogAngelStep("Finished Angel SmartAPI wait before market conversion", OrderDetails)
            
            OrderDetails['Ordertype'] = 'MARKET'
            OrderDetails['Price'] = '0'
            ModifyAngeOrder(smartAPI,OrderDetails)

            if OrderDetails.get('ReEnterOrderLoop') == 'True':
                OrderDetails['Ordertype'] = 'LIMIT'
                PrepareInstrumentContractName(smartAPI, OrderDetails)                
                if OrderDetails.get('LastOrderError'):
                    _LogAngelStep("Stopping Angel SmartAPI rollover flow after contract resolution failure", OrderDetails, Level='error')
                    return None
                OrderDetails = PrepareOrderAngel(smartAPI, OrderDetails)
                if OrderDetails.get('LastOrderError'):
                    _LogAngelStep("Stopping Angel SmartAPI rollover flow after LTP failure", OrderDetails, Level='error')
                    return None
                OrderIdDetails = PlaceOrderAngelAPI(smartAPI, OrderDetails)
                if not OrderIdDetails:
                    _LogAngelStep("Stopping Angel SmartAPI rollover flow after order placement failure", OrderDetails, Level='error')
                    return None
                OrderDetails['OrderId'] = OrderIdDetails
                
                OrderDetails['Ordertype'] = 'MARKET'
                print(f'Waiting for {OrderDetails["EntrySleepDuration"]} seconds')
                SleepForRequiredTime(int(OrderDetails['EntrySleepDuration']))
                ModifyAngeOrder(smartAPI,OrderDetails)
                
                _LogAngelStep("Completed Angel SmartAPI rollover flow", OrderDetails)
                return OrderDetails
        _LogAngelStep("Completed Angel SmartAPI limit order flow", OrderDetails)
        return OrderIdDetails


def _ControlOrderFlowAngelCore(OrderDetails):
    OrderDetails.pop('LastOrderError', None)

    ConfigureNetDirectionOfTrade(OrderDetails)

    Validate_Quantity(OrderDetails)

    if _ShouldUseAngelBrowserRoute(OrderDetails):
        if _CanUseAngelBrowserRouteWithoutSmartApi(OrderDetails):
            if OrderDetails['ContractNameProvided'] == 'False':
                PrepareInstrumentContractName(None, OrderDetails)
                if OrderDetails.get('LastOrderError'):
                    _LogAngelStep("Stopping Angel browser-only flow after contract resolution failure", OrderDetails, Level='error')
                    return None

            _LogAngelStep("Selected Angel browser execution route", OrderDetails, preflight_mode='browser_only')
            return _ExecuteAngelBrowserOrderFlow(None, OrderDetails)

        _LogAngelStep(
            "Angel browser route requires SmartAPI preflight",
            OrderDetails,
            preflight_mode='smartapi',
            reason=_GetAngelBrowserSmartApiPreflightReason(OrderDetails),
        )

    smartAPI = EstablishConnectionAngelAPI(OrderDetails)

    if OrderDetails['ContractNameProvided'] == 'False':
        PrepareInstrumentContractName(smartAPI,OrderDetails)
        if OrderDetails.get('LastOrderError'):
            _LogAngelStep("Stopping Angel flow after contract resolution failure", OrderDetails, Level='error')
            return None

    OrderDetails = PrepareOrderAngel(smartAPI, OrderDetails)
    if OrderDetails.get('LastOrderError'):
        _LogAngelStep("Stopping Angel flow after LTP failure", OrderDetails, Level='error')
        return None

    if _ShouldUseAngelBrowserRoute(OrderDetails):
        _LogAngelStep("Selected Angel browser execution route", OrderDetails, preflight_mode='smartapi')
        return _ExecuteAngelBrowserOrderFlow(smartAPI, OrderDetails)

    _LogAngelStep("Selected Angel SmartAPI execution route", OrderDetails)
    return _ExecuteAngelSmartApiOrderFlow(smartAPI, OrderDetails)


def ControlOrderFlowAngel(OrderDetails):
    # This function orchestrates the entire order flow for Angel, from contract selection to order placement
    _LogAngelStep("Starting Angel order flow", OrderDetails)

    if _ShouldUseAngelBrowserRoute(OrderDetails):
        with _AcquireAngelBrowserFifoTurn(OrderDetails):
            return _ControlOrderFlowAngelCore(OrderDetails)

    return _ControlOrderFlowAngelCore(OrderDetails)
