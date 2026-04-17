"""Exhaustive regression tests for the post-rollover contract-selection fixes
introduced in Kite_Server_Order_Handler.py and Server_Order_Handler.py.

Covers:
  1. forecast_db.GetRecentCompletedRollovers — SQL filter behavior
  2. _ComputeTradingDaysRolloverDate — weekend/holiday/exchange skipping
  3. _FindPinnedRolloverContractKite / _FindPinnedRolloverContractAngel
     — DB-pin lookup against instrument master CSV
  4. PrepareKiteInstrumentContractName / PrepareAngelInstrumentContractName
     — end-to-end contract selection, including the exact CRUDEOIL
       weekend-bridging scenario the bug report describes
  5. CheckIfExistingOldContractSqOffReq — the
     `KitePositions.empty` vs `KitePositionsFiltered.empty` fix
"""
import os
import pathlib
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

import pandas as pd

# ── Stub modules before importing the handlers (pattern from existing tests) ─
sys.path.insert(0, os.path.dirname(__file__))

_MISSING = object()


def _snapshot_modules(names):
    return {n: sys.modules.get(n, _MISSING) for n in names}


def _restore_modules(snapshot):
    for n, m in snapshot.items():
        if m is _MISSING:
            sys.modules.pop(n, None)
        else:
            sys.modules[n] = m


_MODULE_SNAPSHOT = _snapshot_modules(
    ["SmartApi", "pyotp", "Directories",
     "Server_Order_Handler", "Kite_Server_Order_Handler",
     "forecast_db"]
)

_smartapi = types.ModuleType("SmartApi")
_smartapi.SmartConnect = MagicMock()
sys.modules["SmartApi"] = _smartapi
sys.modules["pyotp"] = MagicMock()

_dirs = types.ModuleType("Directories")
_dirs.AngelInstrumentDirectory = "AngelInstrumentDetails.csv"
_dirs.ZerodhaInstrumentDirectory = "ZerodhaInstruments.csv"
_dirs.AngelNararushLoginCred = "unused.txt"
_dirs.AngelEkanshLoginCred = "unused.txt"
_dirs.AngelEshitaLoginCred = "unused.txt"
_dirs.KiteEkanshLogin = "unused.txt"
_dirs.KiteEkanshLoginAccessToken = "unused.txt"
_dirs.KiteRashmiLogin = "unused.txt"
_dirs.KiteRashmiLoginAccessToken = "unused.txt"
_dirs.KiteEshitaLogin = "unused.txt"
_dirs.KiteEshitaLoginAccessToken = "unused.txt"
_dirs.workInputRoot = pathlib.Path(tempfile.gettempdir())
sys.modules["Directories"] = _dirs

# Load the four modules we test fresh from disk.  The "full" pytest suite
# collects files alphabetically, and several earlier files (test_smart_chase,
# test_capital_model, etc.) install their own stubs for Kite_Server_Order_Handler
# / Server_Order_Handler / forecast_db / Holidays into sys.modules.  A plain
# `import Foo` here would then return the stub, not the real implementation.
# importlib.util.spec_from_file_location bypasses sys.modules and gives us
# an uncontaminated module object.
import importlib.util as _iu  # noqa: E402

_ThisDir = os.path.dirname(__file__)


def _LoadFreshModule(AliasName, Filename):
    Path = os.path.join(_ThisDir, Filename)
    Spec = _iu.spec_from_file_location(AliasName, Path)
    Mod = _iu.module_from_spec(Spec)
    Spec.loader.exec_module(Mod)
    return Mod


DB = _LoadFreshModule("forecast_db_real_for_tests", "forecast_db.py")
KiteH = _LoadFreshModule("Kite_Server_Order_Handler_real_for_tests",
                          "Kite_Server_Order_Handler.py")
AngelH = _LoadFreshModule("Server_Order_Handler_real_for_tests",
                           "Server_Order_Handler.py")

# Load a FRESH copy of Holidays.py directly from disk so we have an
# uncontaminated real CheckForDateHoliday, regardless of what other test
# files may have already written into sys.modules at collection time (pytest
# collects alphabetically, so test_itm_call_rollover.py — which mutates
# Holidays.CheckForDateHoliday — runs its module body before ours).
import importlib.util as _iu  # noqa: E402
_HolidaysPath = os.path.join(os.path.dirname(__file__), "Holidays.py")
_RealSpec = _iu.spec_from_file_location("Holidays_real_for_tests", _HolidaysPath)
_REAL_HOLIDAYS = _iu.module_from_spec(_RealSpec)
_RealSpec.loader.exec_module(_REAL_HOLIDAYS)
_REAL_CHECK_HOLIDAY = _REAL_HOLIDAYS.CheckForDateHoliday


class _RealHolidaysContext:
    """Context manager that swaps the REAL CheckForDateHoliday into sys.modules
    ['Holidays'] for the duration of the block, then restores whatever was
    there before (honoring mocks installed by other test modules).
    """

    def __enter__(self):
        self._PrevMod = sys.modules.get("Holidays")
        self._PrevFn = getattr(self._PrevMod, "CheckForDateHoliday", None) \
            if self._PrevMod is not None else None
        self._PrevImpl = getattr(self._PrevMod, "_holidays_impl", None) \
            if self._PrevMod is not None else None
        sys.modules["Holidays"] = _REAL_HOLIDAYS
        _REAL_HOLIDAYS.CheckForDateHoliday = _REAL_CHECK_HOLIDAY
        return self

    def __exit__(self, *Exc):
        if self._PrevMod is None:
            sys.modules.pop("Holidays", None)
        else:
            sys.modules["Holidays"] = self._PrevMod
            if self._PrevFn is not None:
                self._PrevMod.CheckForDateHoliday = self._PrevFn
            if self._PrevImpl is not None:
                self._PrevMod._holidays_impl = self._PrevImpl
        return False

# Restore the original module bindings so downstream test modules that rely on
# the real Directories/SmartApi/pyotp aren't affected when pytest collects the
# whole suite.  The three handler modules above have already captured their
# references, so this restore is safe.
_restore_modules(_MODULE_SNAPSHOT)


# ─── Helpers ────────────────────────────────────────────────────────────────

def _WriteZerodhaCsv(Rows):
    """Write a Zerodha-style instrument master CSV and return the path.

    Rows is a list of dicts with keys: symbol, token, name, expiry, exch_seg,
    instrumenttype. Extra columns are filled with sensible defaults.
    """
    Cols = ["serialnumber", "symbol", "token", "name", "expiry",
            "exch_seg", "instrumenttype", "strike", "tick_size", "lot_size"]
    with NamedTemporaryFile("w", suffix=".csv", delete=False) as TmpFile:
        TmpFile.write(",".join(Cols) + "\n")
        for I, R in enumerate(Rows):
            TmpFile.write(
                f"{I},{R['symbol']},{R['token']},{R['name']},{R['expiry']},"
                f"{R['exch_seg']},{R['instrumenttype']},-1,0.05,1\n"
            )
        return TmpFile.name


def _WriteAngelCsv(Rows):
    """Write an Angel-style instrument master CSV and return the path."""
    Cols = [",", "token", "symbol", "name", "expiry", "strike",
            "lotsize", "instrumenttype", "exch_seg", "tick_size"]
    # Match the ",token,symbol,..." header style used in the Angel CSV
    HeaderCols = ["", "token", "symbol", "name", "expiry", "strike",
                  "lotsize", "instrumenttype", "exch_seg", "tick_size"]
    with NamedTemporaryFile("w", suffix=".csv", delete=False) as TmpFile:
        TmpFile.write(",".join(HeaderCols) + "\n")
        for I, R in enumerate(Rows):
            TmpFile.write(
                f"{I},{R['token']},{R['symbol']},{R['name']},{R['expiry']},"
                f"-1,5,{R['instrumenttype']},{R['exch_seg']},200\n"
            )
        return TmpFile.name


def _IsolatedDB():
    """Return a forecast_db module bound to a fresh sqlite file.

    Patches the module-level DB_PATH so tests run in isolation.
    """
    TmpDb = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
    TmpDb.close()
    return TmpDb.name


def _MakeFrozenDatetimeClass(FrozenNow):
    """Return a `datetime` subclass whose `now()` is frozen at FrozenNow.

    The instance returned by `now()` IS an instance of the subclass so any
    downstream `isinstance(x, datetime)` checks (where the bound name is this
    subclass) still succeed.  `combine`, `min`, `strftime`, arithmetic with
    `timedelta`, etc. all continue to work via inheritance.
    """
    class _FrozenDt(datetime):
        pass

    FrozenInstance = _FrozenDt(
        FrozenNow.year, FrozenNow.month, FrozenNow.day,
        FrozenNow.hour, FrozenNow.minute, FrozenNow.second,
    )

    @classmethod
    def _now(cls, tz=None):
        return FrozenInstance

    _FrozenDt.now = _now
    return _FrozenDt


# ─── 1. forecast_db.GetRecentCompletedRollovers ─────────────────────────────

class TestGetRecentCompletedRollovers(unittest.TestCase):

    def setUp(self):
        self.DbPath = _IsolatedDB()
        # Reset the module-level connection so InitDB uses our tmp path
        if DB._Connection is not None:
            try:
                DB._Connection.close()
            except Exception:
                pass
        DB._Connection = None
        self.Patcher = patch.object(DB, "DB_PATH", self.DbPath)
        self.Patcher.start()
        DB.InitDB()

    def tearDown(self):
        if DB._Connection is not None:
            try:
                DB._Connection.close()
            except Exception:
                pass
        DB._Connection = None
        self.Patcher.stop()
        if os.path.exists(self.DbPath):
            os.unlink(self.DbPath)

    def test_returns_empty_when_no_rollovers(self):
        self.assertEqual(DB.GetRecentCompletedRollovers(), [])

    def test_returns_only_complete_status(self):
        Row1 = DB.LogRollover("CRUDEOIL", "2026-04-20", "CRUDEOIL20APR26FUT",
                              10, "LONG", Broker="ZERODHA", UserAccount="YD6016")
        Row2 = DB.LogRollover("DHANIYA", "2026-04-19", "DHANIYA19APR26",
                              5, "LONG", Broker="ANGEL", UserAccount="AABM826021")

        DB.UpdateRolloverStatus(Row1, "COMPLETE", new_contract="CRUDEOIL19MAY26FUT")
        DB.UpdateRolloverStatus(Row2, "LEG1_DONE")

        Results = DB.GetRecentCompletedRollovers()
        self.assertEqual(len(Results), 1)
        self.assertEqual(Results[0]["instrument"], "CRUDEOIL")
        self.assertEqual(Results[0]["status"], "COMPLETE")
        self.assertEqual(Results[0]["new_contract"], "CRUDEOIL19MAY26FUT")

    def test_broker_filter(self):
        R1 = DB.LogRollover("CRUDEOIL", "2026-04-20", "A", 1, "LONG",
                            Broker="ZERODHA", UserAccount="YD6016")
        R2 = DB.LogRollover("DHANIYA", "2026-04-19", "B", 1, "LONG",
                            Broker="ANGEL", UserAccount="AABM826021")
        DB.UpdateRolloverStatus(R1, "COMPLETE", new_contract="A2")
        DB.UpdateRolloverStatus(R2, "COMPLETE", new_contract="B2")

        KiteOnly = DB.GetRecentCompletedRollovers(Broker="ZERODHA")
        self.assertEqual(len(KiteOnly), 1)
        self.assertEqual(KiteOnly[0]["instrument"], "CRUDEOIL")

        AngelOnly = DB.GetRecentCompletedRollovers(Broker="ANGEL")
        self.assertEqual(len(AngelOnly), 1)
        self.assertEqual(AngelOnly[0]["instrument"], "DHANIYA")

    def test_ordering_most_recent_first(self):
        # Insert rows with explicit created_at so ordering is deterministic
        # even when wall-clock calls land in the same second.
        Conn = DB._GetConn()
        for I, (Inst, Ts) in enumerate([
            ("A", "2026-04-10 10:00:00"),
            ("B", "2026-04-11 10:00:00"),
            ("C", "2026-04-12 10:00:00"),
        ]):
            Conn.execute(
                """INSERT INTO rollover_log
                   (instrument, expiry_date, old_contract, quantity, direction,
                    broker, user_account, status, new_contract,
                    created_at, updated_at)
                   VALUES (?, '2026-04-20', ?, 1, 'LONG', 'ZERODHA', 'X',
                           'COMPLETE', ?, ?, ?)""",
                (Inst, f"{Inst}FUT", f"{Inst}NEW", Ts, Ts)
            )
        Conn.commit()

        Results = DB.GetRecentCompletedRollovers()
        self.assertEqual([R["instrument"] for R in Results], ["C", "B", "A"])

    def test_limit_is_honored(self):
        for Inst in [f"I{I}" for I in range(5)]:
            Rid = DB.LogRollover(Inst, "2026-04-20", f"{Inst}FUT",
                                 1, "LONG", Broker="ZERODHA", UserAccount="X")
            DB.UpdateRolloverStatus(Rid, "COMPLETE", new_contract=f"{Inst}NEW")
        self.assertEqual(len(DB.GetRecentCompletedRollovers(limit=2)), 2)


# ─── 2. _ComputeTradingDaysRolloverDate ─────────────────────────────────────

class TestComputeTradingDaysRolloverDate(unittest.TestCase):

    def setUp(self):
        # Force the real Holidays module into sys.modules so the lazy import
        # inside the helper sees the real calendar, regardless of whatever
        # stubs earlier test files may have installed.
        self._HPatch = patch.dict(sys.modules, {"Holidays": _REAL_HOLIDAYS})
        self._HPatch.start()

    def tearDown(self):
        self._HPatch.stop()

    def test_zero_trading_days_returns_today(self):
        Today = datetime(2026, 4, 17)  # Friday
        Result = KiteH._ComputeTradingDaysRolloverDate(Today, 0, "NFO")
        self.assertEqual(Result.date(), Today.date())

    def test_skips_weekend(self):
        # Friday Apr 17, 2026 + 1 trading day = Monday Apr 20
        Today = datetime(2026, 4, 17)
        Result = KiteH._ComputeTradingDaysRolloverDate(Today, 1, "NFO")
        self.assertEqual(Result.date().isoformat(), "2026-04-20")

    def test_three_trading_days_from_wednesday_skips_weekend(self):
        # This is the exact CRUDEOIL bug scenario: rollover runs Wed Apr 15,
        # expiry Mon Apr 20 is 3 trading days away. Picker now agrees.
        Today = datetime(2026, 4, 15)  # Wednesday
        Result = KiteH._ComputeTradingDaysRolloverDate(Today, 3, "MCX")
        # Thu 16, Fri 17, Mon 20 — 3 trading days land on Mon Apr 20
        self.assertEqual(Result.date().isoformat(), "2026-04-20")

    def test_skips_nse_holiday(self):
        # 2026-04-03 is Good Friday (in NSE list and MCX list)
        Today = datetime(2026, 4, 2)  # Thursday
        # NSE: skip Fri Apr 3 (holiday) and weekend → lands Mon Apr 6
        Result = KiteH._ComputeTradingDaysRolloverDate(Today, 1, "NFO")
        self.assertEqual(Result.date().isoformat(), "2026-04-06")

    def test_mcx_open_on_equity_only_holiday(self):
        # 2026-04-14 is Ambedkar Jayanti: NSE closed, MCX open
        Today = datetime(2026, 4, 13)  # Monday (trading day)
        # For MCX, the next trading day is Tue Apr 14 (MCX open)
        Result = KiteH._ComputeTradingDaysRolloverDate(Today, 1, "MCX")
        self.assertEqual(Result.date().isoformat(), "2026-04-14")
        # For NFO, Apr 14 is closed → lands Apr 15
        Result = KiteH._ComputeTradingDaysRolloverDate(Today, 1, "NFO")
        self.assertEqual(Result.date().isoformat(), "2026-04-15")

    def test_non_numeric_N_treated_as_zero(self):
        Today = datetime(2026, 4, 17)
        Result = KiteH._ComputeTradingDaysRolloverDate(Today, "abc", "NFO")
        self.assertEqual(Result.date(), Today.date())

    def test_accepts_string_integer(self):
        Today = datetime(2026, 4, 17)
        Result = KiteH._ComputeTradingDaysRolloverDate(Today, "3", "MCX")
        # Mon 20, Tue 21, Wed 22 (Apr 21 is regular trading day)
        self.assertEqual(Result.date().isoformat(), "2026-04-22")

    def test_angel_helper_matches_kite_helper(self):
        # The two helpers are independent copies; verify they agree on the
        # CRUDEOIL bug scenario.
        Today = datetime(2026, 4, 15)
        K = KiteH._ComputeTradingDaysRolloverDate(Today, 3, "MCX")
        A = AngelH._ComputeTradingDaysRolloverDateAngel(Today, 3, "MCX")
        self.assertEqual(K, A)


# ─── 3. _FindPinnedRolloverContractKite ─────────────────────────────────────

class TestFindPinnedRolloverContractKite(unittest.TestCase):

    def setUp(self):
        self.Today = datetime(2026, 4, 17)

    def _MakeDf(self):
        return pd.DataFrame([
            {
                "symbol": "CRUDEOIL20APR26FUT", "token": "111",
                "name": "CRUDEOIL", "exch_seg": "MCX", "instrumenttype": "FUT",
                "expiry": datetime(2026, 4, 20),
            },
            {
                "symbol": "CRUDEOIL19MAY26FUT", "token": "222",
                "name": "CRUDEOIL", "exch_seg": "MCX", "instrumenttype": "FUT",
                "expiry": datetime(2026, 5, 19),
            },
        ])

    def _MakeOrder(self):
        return {
            "Tradingsymbol": "CRUDEOIL",
            "Exchange": "MCX",
            "InstrumentType": "FUT",
        }

    def test_empty_when_db_has_no_rollovers(self):
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[])
        with patch.dict(sys.modules, {"forecast_db": FakeDb}):
            Result = KiteH._FindPinnedRolloverContractKite(
                self._MakeOrder(), self._MakeDf(), self.Today
            )
        self.assertTrue(Result.empty)

    def test_empty_when_db_import_fails(self):
        BadDb = types.ModuleType("forecast_db")

        def _raise(*_a, **_k):
            raise RuntimeError("db unavailable")

        BadDb.GetRecentCompletedRollovers = _raise
        with patch.dict(sys.modules, {"forecast_db": BadDb}):
            Result = KiteH._FindPinnedRolloverContractKite(
                self._MakeOrder(), self._MakeDf(), self.Today
            )
        self.assertTrue(Result.empty)

    def test_pins_to_new_contract_when_rollover_complete(self):
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[
            {
                "id": 42, "instrument": "CRUDEOIL",
                "new_contract": "CRUDEOIL19MAY26FUT", "status": "COMPLETE",
            }
        ])
        with patch.dict(sys.modules, {"forecast_db": FakeDb}):
            Result = KiteH._FindPinnedRolloverContractKite(
                self._MakeOrder(), self._MakeDf(), self.Today
            )
        self.assertFalse(Result.empty)
        self.assertEqual(Result["symbol"].iloc[0], "CRUDEOIL19MAY26FUT")
        self.assertEqual(Result["token"].iloc[0], "222")

    def test_ignores_expired_new_contract(self):
        # Defensive: a stale DB row pointing at a contract that has now expired
        # should not pin. Use a date past the new_contract's expiry.
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[
            {
                "id": 1, "instrument": "CRUDEOIL",
                "new_contract": "CRUDEOIL20APR26FUT",  # already-expired
                "status": "COMPLETE",
            }
        ])
        FutureDate = datetime(2026, 5, 1)
        with patch.dict(sys.modules, {"forecast_db": FakeDb}):
            Result = KiteH._FindPinnedRolloverContractKite(
                self._MakeOrder(), self._MakeDf(), FutureDate
            )
        self.assertTrue(Result.empty)

    def test_ignores_mismatched_instrument(self):
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[
            {
                "id": 1, "instrument": "GOLDM",
                "new_contract": "GOLDM19MAY26FUT",  # not in our CSV DF
                "status": "COMPLETE",
            }
        ])
        with patch.dict(sys.modules, {"forecast_db": FakeDb}):
            Result = KiteH._FindPinnedRolloverContractKite(
                self._MakeOrder(), self._MakeDf(), self.Today
            )
        self.assertTrue(Result.empty)

    def test_picks_first_matching_row_most_recent_first(self):
        # DB returns most recent first. If most-recent new_contract matches
        # the CSV, it wins over older rows.
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[
            {"id": 2, "instrument": "CRUDEOIL",
             "new_contract": "CRUDEOIL19MAY26FUT", "status": "COMPLETE"},
            {"id": 1, "instrument": "CRUDEOIL",
             "new_contract": "CRUDEOIL20APR26FUT", "status": "COMPLETE"},
        ])
        with patch.dict(sys.modules, {"forecast_db": FakeDb}):
            Result = KiteH._FindPinnedRolloverContractKite(
                self._MakeOrder(), self._MakeDf(), self.Today
            )
        self.assertFalse(Result.empty)
        self.assertEqual(Result["symbol"].iloc[0], "CRUDEOIL19MAY26FUT")

    def test_exchange_mismatch_is_ignored(self):
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[
            {"id": 1, "instrument": "CRUDEOIL",
             "new_contract": "CRUDEOIL19MAY26FUT", "status": "COMPLETE"}
        ])
        Order = self._MakeOrder()
        Order["Exchange"] = "NFO"  # wrong exchange for CRUDEOIL
        with patch.dict(sys.modules, {"forecast_db": FakeDb}):
            Result = KiteH._FindPinnedRolloverContractKite(
                Order, self._MakeDf(), self.Today
            )
        self.assertTrue(Result.empty)


class TestFindPinnedRolloverContractAngel(unittest.TestCase):

    def setUp(self):
        self.Today = datetime(2026, 4, 17)

    def _MakeDf(self):
        return pd.DataFrame([
            {"symbol": "DHANIYA20APR26", "token": "1",
             "name": "DHANIYA", "exch_seg": "NCDEX", "instrumenttype": "FUTCOM",
             "expiry": datetime(2026, 4, 20)},
            {"symbol": "DHANIYA20MAY26", "token": "2",
             "name": "DHANIYA", "exch_seg": "NCDEX", "instrumenttype": "FUTCOM",
             "expiry": datetime(2026, 5, 20)},
        ])

    def _MakeOrder(self):
        return {"Tradingsymbol": "DHANIYA", "Exchange": "NCDEX",
                "InstrumentType": "FUTCOM"}

    def test_pins_to_new_contract(self):
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[
            {"id": 7, "instrument": "DHANIYA",
             "new_contract": "DHANIYA20MAY26", "status": "COMPLETE"}
        ])
        with patch.dict(sys.modules, {"forecast_db": FakeDb}):
            Result = AngelH._FindPinnedRolloverContractAngel(
                self._MakeOrder(), self._MakeDf(), self.Today
            )
        self.assertFalse(Result.empty)
        self.assertEqual(Result["symbol"].iloc[0], "DHANIYA20MAY26")

    def test_queries_angel_broker(self):
        FakeDb = types.ModuleType("forecast_db")
        Capture = MagicMock(return_value=[])
        FakeDb.GetRecentCompletedRollovers = Capture
        with patch.dict(sys.modules, {"forecast_db": FakeDb}):
            AngelH._FindPinnedRolloverContractAngel(
                self._MakeOrder(), self._MakeDf(), self.Today
            )
        _Args, Kwargs = Capture.call_args
        self.assertEqual(Kwargs.get("Broker"), "ANGEL")


# ─── 4. End-to-end PrepareKiteInstrumentContractName ────────────────────────

class TestPrepareKiteInstrumentContractNameEndToEnd(unittest.TestCase):

    def setUp(self):
        self._HCtx = _RealHolidaysContext()
        self._HCtx.__enter__()

    def tearDown(self):
        self._HCtx.__exit__(None, None, None)

    def _MakeOrder(self, **Overrides):
        Order = {
            "Tradetype": "BUY",
            "Exchange": "MCX",
            "Tradingsymbol": "CRUDEOIL",
            "Quantity": "3",
            "Netposition": "3",
            "InstrumentType": "FUT",
            "DaysPostWhichSelectNextContract": "3",
            "ContractNameProvided": "False",
        }
        Order.update(Overrides)
        return Order

    def _WriteCrudeCsv(self):
        return _WriteZerodhaCsv([
            {"symbol": "CRUDEOIL20APR26FUT", "token": "111", "name": "CRUDEOIL",
             "expiry": "2026-04-20", "exch_seg": "MCX", "instrumenttype": "FUT"},
            {"symbol": "CRUDEOIL19MAY26FUT", "token": "222", "name": "CRUDEOIL",
             "expiry": "2026-05-19", "exch_seg": "MCX", "instrumenttype": "FUT"},
            {"symbol": "CRUDEOIL18JUN26FUT", "token": "333", "name": "CRUDEOIL",
             "expiry": "2026-06-18", "exch_seg": "MCX", "instrumenttype": "FUT"},
        ])

    def test_weekend_bridging_bug_scenario_with_db_pin(self):
        """The exact bug the user reported: rollover ran on Wed Apr 15, user
        places a new order on Thu Apr 16 — with DB pin, order must go to May.
        """
        CsvPath = self._WriteCrudeCsv()
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[
            {"id": 1, "instrument": "CRUDEOIL",
             "new_contract": "CRUDEOIL19MAY26FUT", "status": "COMPLETE"}
        ])
        try:
            # Freeze "today" at Thursday Apr 16 — the day the bug manifested
            FakeDt = _MakeFrozenDatetimeClass(datetime(2026, 4, 16, 10, 0))
            with patch.dict(sys.modules, {"forecast_db": FakeDb}), \
                 patch.object(KiteH, "ZerodhaInstrumentDirectory", CsvPath), \
                 patch.object(KiteH, "datetime", FakeDt):
                Result = KiteH.PrepareKiteInstrumentContractName(
                    MagicMock(), self._MakeOrder()
                )
            self.assertFalse(Result.empty)
            self.assertEqual(Result["symbol"].iloc[0], "CRUDEOIL19MAY26FUT",
                             "DB pin must override CSV filter after rollover")
        finally:
            os.unlink(CsvPath)

    def test_weekend_bridging_bug_scenario_without_db_pin_trading_day_filter(self):
        """Even if the DB is unavailable, the trading-day RolloverDate should
        now prevent the front-month from being reselected.
        """
        CsvPath = self._WriteCrudeCsv()
        # No DB rollover row; forecast_db module missing (import fails)
        BrokenDb = types.ModuleType("forecast_db")

        def _raise(*_a, **_k):
            raise RuntimeError("no db")

        BrokenDb.GetRecentCompletedRollovers = _raise
        try:
            # Thursday Apr 16 @ 10:00: 3 trading days away = Tue Apr 21
            # (Thu 16 → Fri 17 → Mon 20 → Tue 21).  RolloverDate therefore
            # falls AFTER Apr 20 expiry, so `expiry > RolloverDate` correctly
            # skips the front-month contract.
            FakeDt = _MakeFrozenDatetimeClass(datetime(2026, 4, 16, 10, 0))
            Kite = MagicMock()
            Kite.positions.return_value = {"net": []}
            with patch.dict(sys.modules, {"forecast_db": BrokenDb}), \
                 patch.object(KiteH, "ZerodhaInstrumentDirectory", CsvPath), \
                 patch.object(KiteH, "datetime", FakeDt):
                Result = KiteH.PrepareKiteInstrumentContractName(
                    Kite, self._MakeOrder()
                )
            self.assertFalse(Result.empty)
            self.assertEqual(Result["symbol"].iloc[0], "CRUDEOIL19MAY26FUT")
        finally:
            os.unlink(CsvPath)

    def test_normal_case_no_rollover_picks_nearest_future_contract(self):
        """Far from expiry: DB empty, filter picks the earliest future expiry
        past the trading-day RolloverDate.
        """
        CsvPath = self._WriteCrudeCsv()
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[])
        try:
            # Mar 1 2026: 3 trading days later = Mar 4. All contracts are
            # past March.  Front-month (Apr 20) is the earliest > RolloverDate.
            FakeDt = _MakeFrozenDatetimeClass(datetime(2026, 3, 1, 10, 0))
            Kite = MagicMock()
            Kite.positions.return_value = {"net": []}
            with patch.dict(sys.modules, {"forecast_db": FakeDb}), \
                 patch.object(KiteH, "ZerodhaInstrumentDirectory", CsvPath), \
                 patch.object(KiteH, "datetime", FakeDt):
                Result = KiteH.PrepareKiteInstrumentContractName(
                    Kite, self._MakeOrder()
                )
            self.assertEqual(Result["symbol"].iloc[0], "CRUDEOIL20APR26FUT")
        finally:
            os.unlink(CsvPath)

    def test_db_pin_beats_filter_even_when_front_month_selectable(self):
        """Regression: the fix must route to the pinned contract even when the
        CSV filter alone would have picked the front-month.
        """
        CsvPath = self._WriteCrudeCsv()
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[
            {"id": 9, "instrument": "CRUDEOIL",
             "new_contract": "CRUDEOIL19MAY26FUT", "status": "COMPLETE"}
        ])
        try:
            FakeDt = _MakeFrozenDatetimeClass(datetime(2026, 3, 1, 10, 0))  # far from expiry
            with patch.dict(sys.modules, {"forecast_db": FakeDb}), \
                 patch.object(KiteH, "ZerodhaInstrumentDirectory", CsvPath), \
                 patch.object(KiteH, "datetime", FakeDt):
                Result = KiteH.PrepareKiteInstrumentContractName(
                    MagicMock(), self._MakeOrder()
                )
            self.assertEqual(Result["symbol"].iloc[0], "CRUDEOIL19MAY26FUT")
        finally:
            os.unlink(CsvPath)


# ─── 5. CheckIfExistingOldContractSqOffReq .empty-bug fix ───────────────────

class TestCheckIfExistingOldContractSqOffReqFix(unittest.TestCase):

    def _MakeCsvDf(self):
        Df = pd.DataFrame([
            {"symbol": "CRUDEOIL20APR26FUT", "token": 111, "name": "CRUDEOIL",
             "exch_seg": "MCX", "instrumenttype": "FUT",
             "expiry": pd.to_datetime("2026-04-20")},
            {"symbol": "CRUDEOIL19MAY26FUT", "token": 222, "name": "CRUDEOIL",
             "exch_seg": "MCX", "instrumenttype": "FUT",
             "expiry": pd.to_datetime("2026-05-19")},
        ])
        return Df

    def _MakeOrder(self):
        return {
            "Tradetype": "BUY",
            "Exchange": "MCX",
            "Tradingsymbol": "CRUDEOIL",
            "InstrumentType": "FUT",
            "NetDirection": 1,
        }

    def test_returns_empty_when_user_only_has_new_month_position(self):
        """Core of the bug fix: user has NO position in the old (Apr) contract
        but DOES have a position in the new (May) one. Old behavior returned
        empty filtered DF inside a truthy outer check; still empty overall,
        but the intent was wrong. New behavior is clean: empty means empty.
        """
        Today = datetime(2026, 4, 17)
        RolloverDate = datetime(2026, 4, 20)
        # User has May position, no April position
        Positions = pd.DataFrame([
            {"tradingsymbol": "CRUDEOIL19MAY26FUT", "instrument_token": 222,
             "quantity": 3},
        ])
        Kite = MagicMock()
        with patch.object(KiteH, "FetchExistingNetKitePositions",
                          return_value=Positions):
            Result = KiteH.CheckIfExistingOldContractSqOffReq(
                Kite, self._MakeCsvDf(), self._MakeOrder(), Today, RolloverDate
            )
        self.assertTrue(Result.empty)

    def test_returns_old_contract_when_user_has_position_in_it(self):
        """The square-off path must still work when user genuinely holds the
        old front-month.
        """
        Today = datetime(2026, 4, 17)
        RolloverDate = datetime(2026, 4, 20)
        Positions = pd.DataFrame([
            {"tradingsymbol": "CRUDEOIL20APR26FUT", "instrument_token": 111,
             "quantity": -2},  # short position in old contract
        ])
        Kite = MagicMock()
        Order = self._MakeOrder()  # BUY; NetDirection=1; quantity<1 matches -2
        with patch.object(KiteH, "FetchExistingNetKitePositions",
                          return_value=Positions):
            Result = KiteH.CheckIfExistingOldContractSqOffReq(
                Kite, self._MakeCsvDf(), Order, Today, RolloverDate
            )
        self.assertFalse(Result.empty)
        # Result should expose the old contract symbol/token
        self.assertEqual(Result.iloc[0]["symbol"], "CRUDEOIL20APR26FUT")

    def test_returns_empty_when_positions_fetch_returns_nothing(self):
        Today = datetime(2026, 4, 17)
        RolloverDate = datetime(2026, 4, 20)
        Positions = pd.DataFrame(columns=["tradingsymbol", "instrument_token",
                                          "quantity"])
        Kite = MagicMock()
        with patch.object(KiteH, "FetchExistingNetKitePositions",
                          return_value=Positions):
            Result = KiteH.CheckIfExistingOldContractSqOffReq(
                Kite, self._MakeCsvDf(), self._MakeOrder(), Today, RolloverDate
            )
        self.assertTrue(Result.empty)


# ─── 6. PrepareAngelInstrumentContractName end-to-end ───────────────────────

class TestPrepareAngelInstrumentContractNameEndToEnd(unittest.TestCase):

    def setUp(self):
        self._HCtx = _RealHolidaysContext()
        self._HCtx.__enter__()

    def tearDown(self):
        self._HCtx.__exit__(None, None, None)

    def _MakeOrder(self, **Overrides):
        Order = {
            "Tradetype": "BUY",
            "Exchange": "NCDEX",
            "Tradingsymbol": "DHANIYA",
            "Quantity": "5",
            "Netposition": "5",
            "InstrumentType": "FUTCOM",
            "DaysPostWhichSelectNextContract": "9",
            "ContractNameProvided": "False",
        }
        Order.update(Overrides)
        return Order

    def _WriteDhaniyaCsv(self):
        return _WriteAngelCsv([
            {"symbol": "DHANIYA20APR26", "token": "10", "name": "DHANIYA",
             "expiry": "20APR2026", "exch_seg": "NCDEX",
             "instrumenttype": "FUTCOM"},
            {"symbol": "DHANIYA20MAY26", "token": "20", "name": "DHANIYA",
             "expiry": "20MAY2026", "exch_seg": "NCDEX",
             "instrumenttype": "FUTCOM"},
        ])

    def test_db_pin_wins_for_angel(self):
        CsvPath = self._WriteDhaniyaCsv()
        FakeDb = types.ModuleType("forecast_db")
        FakeDb.GetRecentCompletedRollovers = MagicMock(return_value=[
            {"id": 5, "instrument": "DHANIYA",
             "new_contract": "DHANIYA20MAY26", "status": "COMPLETE"}
        ])
        try:
            AngelH._ANGEL_INSTRUMENT_MASTER_CACHE.clear()
            with patch.dict(sys.modules, {"forecast_db": FakeDb}), \
                 patch.object(AngelH, "AngelInstrumentDirectory", CsvPath):
                Result = AngelH.PrepareAngelInstrumentContractName(
                    MagicMock(), self._MakeOrder()
                )
            self.assertFalse(Result.empty)
            self.assertEqual(Result["symbol"].iloc[0], "DHANIYA20MAY26")
        finally:
            AngelH._ANGEL_INSTRUMENT_MASTER_CACHE.clear()
            os.unlink(CsvPath)

    def test_trading_day_rollover_date_used_for_angel(self):
        """Without a DB pin, the trading-day RolloverDate should still prevent
        the front-month from being reselected across weekends (for NCDEX,
        which follows the NSE calendar).
        """
        CsvPath = self._WriteDhaniyaCsv()
        EmptyDb = types.ModuleType("forecast_db")
        EmptyDb.GetRecentCompletedRollovers = MagicMock(return_value=[])
        try:
            # On Thu Apr 16, 9 trading days later = late Apr (past Apr 20)
            FakeDt = _MakeFrozenDatetimeClass(datetime(2026, 4, 16, 10, 0))
            AngelH._ANGEL_INSTRUMENT_MASTER_CACHE.clear()
            with patch.dict(sys.modules, {"forecast_db": EmptyDb}), \
                 patch.object(AngelH, "AngelInstrumentDirectory", CsvPath), \
                 patch.object(AngelH, "datetime", FakeDt):
                Result = AngelH.PrepareAngelInstrumentContractName(
                    MagicMock(), self._MakeOrder()
                )
            self.assertFalse(Result.empty)
            # 9 trading days from Apr 16 lands in May, past both Apr 20 (old)
            # and Apr 20 expiry — should pick May.
            self.assertEqual(Result["symbol"].iloc[0], "DHANIYA20MAY26")
        finally:
            AngelH._ANGEL_INSTRUMENT_MASTER_CACHE.clear()
            os.unlink(CsvPath)


if __name__ == "__main__":
    try:
        unittest.main()
    finally:
        _restore_modules(_MODULE_SNAPSHOT)
