"""
SQLite database layer for the Forecast Orchestrator.
Stores subsystem forecasts, TradingView signals, system positions,
order logs, overrides, and reconciliation history.

Thread-safe: uses a module-level lock for all writes.
WAL mode enabled for concurrent read/write access.
"""

import sqlite3
import threading
import logging
from pathlib import Path
from datetime import datetime
from Directories import workInputRoot

Logger = logging.getLogger(__name__)

# DB file location (uses Directories.py for consistent path on Mac/AWS)
DB_PATH = str(workInputRoot / "forecast_store.db")

_DBLock = threading.Lock()
_Connection = None


def _GetConn():
    """Get or create the module-level SQLite connection (thread-safe reads via WAL)."""
    global _Connection
    if _Connection is None:
        _Connection = sqlite3.connect(DB_PATH, check_same_thread=False)
        _Connection.row_factory = sqlite3.Row
        _Connection.execute("PRAGMA journal_mode=WAL")
        _Connection.execute("PRAGMA busy_timeout=5000")
    return _Connection


def InitDB():
    """Create all tables if they don't exist."""
    Conn = _GetConn()
    with _DBLock:
        Conn.executescript("""
            CREATE TABLE IF NOT EXISTS subsystem_forecasts (
                instrument TEXT NOT NULL,
                system_name TEXT NOT NULL,
                forecast REAL NOT NULL,
                atr REAL NOT NULL,
                action TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (instrument, system_name)
            );

            CREATE TABLE IF NOT EXISTS tradingview_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument TEXT NOT NULL,
                system_name TEXT NOT NULL,
                netposition INTEGER NOT NULL,
                atr REAL NOT NULL,
                action TEXT,
                received_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_tv_signals_lookup
                ON tradingview_signals(instrument, system_name, received_at DESC);

            CREATE TABLE IF NOT EXISTS system_positions (
                instrument TEXT PRIMARY KEY,
                target_qty INTEGER NOT NULL DEFAULT 0,
                confirmed_qty INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS order_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument TEXT NOT NULL,
                action TEXT NOT NULL,
                qty INTEGER NOT NULL,
                status TEXT NOT NULL,
                broker_order_id TEXT,
                reason TEXT,
                execution_mode TEXT,
                initial_ltp REAL,
                initial_bid REAL,
                initial_ask REAL,
                initial_spread REAL,
                limit_price REAL,
                fill_price REAL,
                slippage REAL,
                chase_iterations INTEGER,
                chase_duration_seconds REAL,
                market_fallback INTEGER,
                spread_ratio REAL,
                range_ratio REAL,
                settle_wait_seconds REAL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS options_order_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                underlying TEXT NOT NULL,
                strategy_name TEXT NOT NULL,
                leg TEXT NOT NULL,
                contract TEXT NOT NULL,
                action TEXT NOT NULL,
                qty INTEGER NOT NULL,
                broker_order_id TEXT,
                execution_mode TEXT,
                initial_ltp REAL,
                initial_bid REAL,
                initial_ask REAL,
                initial_spread REAL,
                limit_price REAL,
                fill_price REAL,
                slippage REAL,
                chase_iterations INTEGER,
                chase_duration_seconds REAL,
                market_fallback INTEGER,
                spread_ratio REAL,
                range_ratio REAL,
                settle_wait_seconds REAL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS overrides (
                instrument TEXT PRIMARY KEY,
                override_type TEXT NOT NULL,
                value TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS reconciliation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument TEXT NOT NULL,
                system_qty INTEGER NOT NULL,
                broker_qty INTEGER NOT NULL,
                match INTEGER NOT NULL,
                checked_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)
        Conn.commit()

        # Migrations: add columns that may not exist in older DBs
        _RunMigrations(Conn)

    Logger.info("Forecast database initialized at %s", DB_PATH)


def _RunMigrations(Conn):
    """Add new columns to existing tables if they don't exist yet."""
    # Check if 'action' column exists in subsystem_forecasts
    Cols = [row[1] for row in Conn.execute("PRAGMA table_info(subsystem_forecasts)").fetchall()]
    if "action" not in Cols:
        Conn.execute("ALTER TABLE subsystem_forecasts ADD COLUMN action TEXT")
        Logger.info("Migration: added 'action' column to subsystem_forecasts")

    # Check if 'action' column exists in tradingview_signals
    Cols = [row[1] for row in Conn.execute("PRAGMA table_info(tradingview_signals)").fetchall()]
    if "action" not in Cols:
        Conn.execute("ALTER TABLE tradingview_signals ADD COLUMN action TEXT")
        Logger.info("Migration: added 'action' column to tradingview_signals")

    Conn.commit()


# ─── Subsystem Forecasts ────────────────────────────────────────────

def UpsertForecast(Instrument, SystemName, Forecast, ATR, Action=None):
    """INSERT OR REPLACE the current forecast for a subsystem."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT OR REPLACE INTO subsystem_forecasts
               (instrument, system_name, forecast, atr, action, updated_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))""",
            (Instrument, SystemName, Forecast, ATR, Action)
        )
        Conn.commit()


def GetForecastsForInstrument(Instrument):
    """Return all subsystem forecast rows for an instrument."""
    Conn = _GetConn()
    Rows = Conn.execute(
        "SELECT system_name, forecast, atr, action, updated_at FROM subsystem_forecasts WHERE instrument = ?",
        (Instrument,)
    ).fetchall()
    return [dict(r) for r in Rows]


def GetAllForecasts():
    """Return all subsystem forecasts (for /status endpoint)."""
    Conn = _GetConn()
    Rows = Conn.execute(
        "SELECT instrument, system_name, forecast, atr, updated_at FROM subsystem_forecasts ORDER BY instrument, system_name"
    ).fetchall()
    return [dict(r) for r in Rows]


# ─── TradingView Signals (append-only log) ──────────────────────────

def LogTVSignal(Instrument, SystemName, Netposition, ATR, Action=None):
    """Append a raw TradingView webhook signal. Never overwritten."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT INTO tradingview_signals
               (instrument, system_name, netposition, atr, action, received_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))""",
            (Instrument, SystemName, Netposition, ATR, Action)
        )
        Conn.commit()


def GetRecentTVSignals(Instrument=None, limit=50):
    """Recent TradingView signals for debugging. If Instrument is None, return all."""
    Conn = _GetConn()
    if Instrument:
        Rows = Conn.execute(
            "SELECT * FROM tradingview_signals WHERE instrument = ? ORDER BY received_at DESC LIMIT ?",
            (Instrument, limit)
        ).fetchall()
    else:
        Rows = Conn.execute(
            "SELECT * FROM tradingview_signals ORDER BY received_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return [dict(r) for r in Rows]


def GetLatestATR(Instrument):
    """Return the most recent ATR value for an instrument from tradingview_signals."""
    Conn = _GetConn()
    Row = Conn.execute(
        """SELECT atr FROM tradingview_signals
           WHERE instrument = ? ORDER BY received_at DESC LIMIT 1""",
        (Instrument,)
    ).fetchone()
    if Row:
        return Row["atr"]
    return None


# ─── System Positions ───────────────────────────────────────────────

def GetSystemPosition(Instrument):
    """Return target_qty and confirmed_qty for an instrument. Returns defaults if not found."""
    Conn = _GetConn()
    Row = Conn.execute(
        "SELECT target_qty, confirmed_qty, updated_at FROM system_positions WHERE instrument = ?",
        (Instrument,)
    ).fetchone()
    if Row:
        return dict(Row)
    return {"target_qty": 0, "confirmed_qty": 0, "updated_at": None}


def UpdateSystemPosition(Instrument, TargetQty, ConfirmedQty):
    """Upsert system position for an instrument."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT OR REPLACE INTO system_positions
               (instrument, target_qty, confirmed_qty, updated_at)
               VALUES (?, ?, ?, datetime('now'))""",
            (Instrument, TargetQty, ConfirmedQty)
        )
        Conn.commit()


def UpdateConfirmedQty(Instrument, ConfirmedQty):
    """Update only the confirmed_qty after order execution."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            "UPDATE system_positions SET confirmed_qty = ?, updated_at = datetime('now') WHERE instrument = ?",
            (ConfirmedQty, Instrument)
        )
        Conn.commit()


def GetAllPositions():
    """Return all system positions (for /status endpoint)."""
    Conn = _GetConn()
    Rows = Conn.execute(
        "SELECT instrument, target_qty, confirmed_qty, updated_at FROM system_positions ORDER BY instrument"
    ).fetchall()
    return [dict(r) for r in Rows]


# ─── Order Log ──────────────────────────────────────────────────────

def LogOrder(Instrument, Action, Qty, Status, BrokerOrderId=None, Reason=None):
    """Log an order attempt (PLACED, FAILED, etc.)."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT INTO order_log
               (instrument, action, qty, status, broker_order_id, reason, created_at)
               VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
            (Instrument, Action, Qty, Status, BrokerOrderId, Reason)
        )
        Conn.commit()


def LogSmartChaseOrder(Instrument, Action, Qty, Status, BrokerOrderId=None,
                       Reason=None, FillInfo=None):
    """Log a smart chase order with full execution details.
    FillInfo is a dict with keys: execution_mode, initial_ltp, initial_bid,
    initial_ask, initial_spread, limit_price, fill_price, slippage,
    chase_iterations, chase_duration_seconds, market_fallback,
    spread_ratio, range_ratio, settle_wait_seconds.
    """
    Info = FillInfo or {}
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT INTO order_log
               (instrument, action, qty, status, broker_order_id, reason,
                execution_mode, initial_ltp, initial_bid, initial_ask,
                initial_spread, limit_price, fill_price, slippage,
                chase_iterations, chase_duration_seconds, market_fallback,
                spread_ratio, range_ratio, settle_wait_seconds, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (Instrument, Action, Qty, Status, BrokerOrderId, Reason,
             Info.get("execution_mode"), Info.get("initial_ltp"),
             Info.get("initial_bid"), Info.get("initial_ask"),
             Info.get("initial_spread"), Info.get("limit_price"),
             Info.get("fill_price"), Info.get("slippage"),
             Info.get("chase_iterations"), Info.get("chase_duration_seconds"),
             Info.get("market_fallback"), Info.get("spread_ratio"),
             Info.get("range_ratio"), Info.get("settle_wait_seconds"))
        )
        Conn.commit()


def LogOptionsSmartChaseOrder(Underlying, StrategyName, Leg, Contract, Action, Qty,
                              BrokerOrderId=None, FillInfo=None):
    """Log an options smart chase order with full execution details."""
    Info = FillInfo or {}
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT INTO options_order_log
               (underlying, strategy_name, leg, contract, action, qty, broker_order_id,
                execution_mode, initial_ltp, initial_bid, initial_ask,
                initial_spread, limit_price, fill_price, slippage,
                chase_iterations, chase_duration_seconds, market_fallback,
                spread_ratio, range_ratio, settle_wait_seconds, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (Underlying, StrategyName, Leg, Contract, Action, Qty, BrokerOrderId,
             Info.get("execution_mode"), Info.get("initial_ltp"),
             Info.get("initial_bid"), Info.get("initial_ask"),
             Info.get("initial_spread"), Info.get("limit_price"),
             Info.get("fill_price"), Info.get("slippage"),
             Info.get("chase_iterations"), Info.get("chase_duration_seconds"),
             Info.get("market_fallback"), Info.get("spread_ratio"),
             Info.get("range_ratio"), Info.get("settle_wait_seconds"))
        )
        Conn.commit()
    Logger.info("Options order logged: %s %s %s %s qty=%s", Underlying, StrategyName, Leg, Action, Qty)


def GetRecentOrders(limit=50):
    """Return recent orders (for /status endpoint)."""
    Conn = _GetConn()
    Rows = Conn.execute(
        "SELECT * FROM order_log ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    return [dict(r) for r in Rows]


# ─── Overrides ──────────────────────────────────────────────────────

def SetOverride(Instrument, OverrideType, Value=None):
    """Set an override for an instrument (FORCE_FLAT, SET_POSITION, etc.)."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT OR REPLACE INTO overrides
               (instrument, override_type, value, created_at)
               VALUES (?, ?, ?, datetime('now'))""",
            (Instrument, OverrideType, Value)
        )
        Conn.commit()
    Logger.info("Override set: %s → %s (value=%s)", Instrument, OverrideType, Value)


def GetOverride(Instrument):
    """Get the current override for an instrument. Returns None if no override."""
    Conn = _GetConn()
    Row = Conn.execute(
        "SELECT override_type, value, created_at FROM overrides WHERE instrument = ?",
        (Instrument,)
    ).fetchone()
    return dict(Row) if Row else None


def ClearOverride(Instrument):
    """Remove the override for an instrument."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute("DELETE FROM overrides WHERE instrument = ?", (Instrument,))
        Conn.commit()
    Logger.info("Override cleared: %s", Instrument)


def GetAllOverrides():
    """Return all active overrides (for /status endpoint)."""
    Conn = _GetConn()
    Rows = Conn.execute(
        "SELECT instrument, override_type, value, created_at FROM overrides ORDER BY instrument"
    ).fetchall()
    return [dict(r) for r in Rows]


# ─── Reconciliation Log ────────────────────────────────────────────

def LogReconciliation(Instrument, SystemQty, BrokerQty, Match):
    """Log a reconciliation check result."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT INTO reconciliation_log
               (instrument, system_qty, broker_qty, match, checked_at)
               VALUES (?, ?, ?, ?, datetime('now'))""",
            (Instrument, SystemQty, BrokerQty, int(Match))
        )
        Conn.commit()


def GetRecentReconciliations(limit=50):
    """Return recent reconciliation checks (for /status endpoint)."""
    Conn = _GetConn()
    Rows = Conn.execute(
        "SELECT * FROM reconciliation_log ORDER BY checked_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    return [dict(r) for r in Rows]
