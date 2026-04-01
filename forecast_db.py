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

            CREATE TABLE IF NOT EXISTS rollover_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                old_contract TEXT NOT NULL,
                new_contract TEXT,
                quantity INTEGER NOT NULL,
                direction TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                leg1_order_id TEXT,
                leg1_fill_price REAL,
                leg1_slippage REAL,
                leg2_order_id TEXT,
                leg2_fill_price REAL,
                leg2_slippage REAL,
                roll_spread REAL,
                email_sent_at TEXT,
                executed_at TEXT,
                broker TEXT,
                user_account TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_rollover_lookup
                ON rollover_log(instrument, expiry_date, status);

            CREATE TABLE IF NOT EXISTS realized_pnl (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument TEXT NOT NULL,
                category TEXT NOT NULL,
                close_qty INTEGER NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL NOT NULL,
                point_value REAL NOT NULL,
                pnl_inr REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_realized_pnl_instrument
                ON realized_pnl(instrument, created_at DESC);

            CREATE TABLE IF NOT EXISTS daily_pnl_snapshot (
                report_date TEXT NOT NULL,
                instrument TEXT NOT NULL,
                confirmed_qty INTEGER NOT NULL,
                avg_entry_price REAL NOT NULL,
                ltp REAL NOT NULL,
                unrealized_pnl REAL NOT NULL,
                PRIMARY KEY (report_date, instrument)
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

    # Add cost basis columns to system_positions for P&L tracking
    Cols = [row[1] for row in Conn.execute("PRAGMA table_info(system_positions)").fetchall()]
    if "avg_entry_price" not in Cols:
        Conn.execute("ALTER TABLE system_positions ADD COLUMN avg_entry_price REAL DEFAULT 0")
        Logger.info("Migration: added 'avg_entry_price' column to system_positions")
    if "point_value" not in Cols:
        Conn.execute("ALTER TABLE system_positions ADD COLUMN point_value REAL DEFAULT 1")
        Logger.info("Migration: added 'point_value' column to system_positions")

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
    """Return target_qty, confirmed_qty, avg_entry_price, point_value for an instrument."""
    Conn = _GetConn()
    Row = Conn.execute(
        "SELECT target_qty, confirmed_qty, avg_entry_price, point_value, updated_at FROM system_positions WHERE instrument = ?",
        (Instrument,)
    ).fetchone()
    if Row:
        return dict(Row)
    return {"target_qty": 0, "confirmed_qty": 0, "avg_entry_price": 0, "point_value": 1, "updated_at": None}


def UpdateSystemPosition(Instrument, TargetQty, ConfirmedQty):
    """Upsert system position for an instrument (preserves cost basis columns)."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT INTO system_positions
               (instrument, target_qty, confirmed_qty, updated_at)
               VALUES (?, ?, ?, datetime('now'))
               ON CONFLICT(instrument) DO UPDATE SET
                   target_qty = excluded.target_qty,
                   confirmed_qty = excluded.confirmed_qty,
                   updated_at = excluded.updated_at""",
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


# ─── Rollover Log ─────────────────────────────────────────────────

def LogRollover(Instrument, ExpiryDate, OldContract, Quantity, Direction,
                Broker=None, UserAccount=None):
    """Insert a PENDING rollover row. Returns the row id."""
    Conn = _GetConn()
    with _DBLock:
        Cur = Conn.execute(
            """INSERT INTO rollover_log
               (instrument, expiry_date, old_contract, quantity, direction,
                broker, user_account, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', datetime('now'), datetime('now'))""",
            (Instrument, str(ExpiryDate), OldContract, Quantity, Direction,
             Broker, UserAccount)
        )
        Conn.commit()
        return Cur.lastrowid


def UpdateRolloverStatus(RowId, Status, **kwargs):
    """Update rollover status and any additional fields.

    Accepted kwargs: new_contract, leg1_order_id, leg1_fill_price, leg1_slippage,
    leg2_order_id, leg2_fill_price, leg2_slippage, roll_spread,
    email_sent_at, executed_at.
    """
    AllowedFields = {
        "new_contract", "leg1_order_id", "leg1_fill_price", "leg1_slippage",
        "leg2_order_id", "leg2_fill_price", "leg2_slippage", "roll_spread",
        "email_sent_at", "executed_at",
    }
    Sets = ["status = ?", "updated_at = datetime('now')"]
    Vals = [Status]
    for Key, Val in kwargs.items():
        if Key in AllowedFields:
            Sets.append(f"{Key} = ?")
            Vals.append(Val)
    Vals.append(RowId)

    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            f"UPDATE rollover_log SET {', '.join(Sets)} WHERE id = ?",
            tuple(Vals)
        )
        Conn.commit()


def GetPendingRollovers(Instrument, ExpiryDate):
    """Check if a rollover already exists for this instrument+expiry.

    Returns list of dicts. Empty list means no prior rollover attempt.
    """
    Conn = _GetConn()
    Rows = Conn.execute(
        """SELECT * FROM rollover_log
           WHERE instrument = ? AND expiry_date = ?
           ORDER BY created_at DESC""",
        (Instrument, str(ExpiryDate))
    ).fetchall()
    return [dict(r) for r in Rows]


def GetIncompleteRollovers():
    """Return rows with status LEG1_DONE (leg 2 still needed) for crash recovery."""
    Conn = _GetConn()
    Rows = Conn.execute(
        "SELECT * FROM rollover_log WHERE status = 'LEG1_DONE' ORDER BY created_at"
    ).fetchall()
    return [dict(r) for r in Rows]


def GetRecentRollovers(limit=20):
    """Return recent rollover attempts (for status/debugging)."""
    Conn = _GetConn()
    Rows = Conn.execute(
        "SELECT * FROM rollover_log ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    return [dict(r) for r in Rows]


# ─── Realized P&L ─────────────────────────────────────────────────


def _UpdateAvgEntry(Instrument, AvgPrice, PointValue):
    """Upsert avg_entry_price and point_value on system_positions row.

    Creates the row if it doesn't exist (needed for options instruments
    like NIFTY_OPT_CE which aren't tracked in the futures position table).
    """
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT INTO system_positions (instrument, target_qty, confirmed_qty, avg_entry_price, point_value, updated_at)
               VALUES (?, 0, 0, ?, ?, datetime('now'))
               ON CONFLICT(instrument) DO UPDATE SET
                   avg_entry_price = excluded.avg_entry_price,
                   point_value = excluded.point_value""",
            (Instrument, AvgPrice, PointValue)
        )
        Conn.commit()


def ResetCostBasis(Instrument):
    """Clear avg_entry_price (used before opening opposite direction on a flip)."""
    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            "UPDATE system_positions SET avg_entry_price = 0 WHERE instrument = ?",
            (Instrument,)
        )
        Conn.commit()


def UpdateCostBasis(Instrument, FillPrice, FillQty, PointValue, OldQty=None):
    """Weighted-average update of cost basis when position size increases.

    Parameters
    ----------
    Instrument : str
        e.g. "GOLDM", "NIFTY_OPT_CE"
    FillPrice : float
        Price at which the new quantity was filled.
    FillQty : int
        Number of lots/units added (always positive).
    PointValue : float
        Rupees per point move (e.g. 100 for GOLDM, 1.0 for options).
    OldQty : int or None
        Previous position size (before this fill). If None, reads from DB.
        Pass explicitly when confirmed_qty has already been updated.
    """
    Pos = GetSystemPosition(Instrument)
    if OldQty is None:
        OldQty = abs(Pos["confirmed_qty"])
    OldAvg = Pos.get("avg_entry_price", 0)

    if OldQty == 0 or OldAvg == 0:
        NewAvg = FillPrice
    else:
        NewAvg = (OldAvg * OldQty + FillPrice * FillQty) / (OldQty + FillQty)

    _UpdateAvgEntry(Instrument, NewAvg, PointValue)
    Logger.info("CostBasis %s: old_avg=%.2f old_qty=%d + fill=%.2f×%d → new_avg=%.2f",
                Instrument, OldAvg, OldQty, FillPrice, FillQty, NewAvg)


def RealizePnl(Instrument, FillPrice, CloseQty, PointValue, Category, WasLong=None):
    """Compute and log realized P&L when position size decreases.

    Parameters
    ----------
    Instrument : str
        e.g. "GOLDM", "NIFTY_OPT_CE"
    FillPrice : float
        Exit price (0 if expired worthless).
    CloseQty : int
        Number of lots/units closed (always positive).
    PointValue : float
        Rupees per point move.
    Category : str
        "futures" or "options".
    WasLong : bool or None
        If provided, overrides direction check from DB. Use when confirmed_qty
        has already been updated before this call.
    """
    Pos = GetSystemPosition(Instrument)
    AvgEntry = Pos.get("avg_entry_price", 0)

    # Determine direction: explicit override or read from DB
    if WasLong is not None:
        IsLong = WasLong
    else:
        IsLong = Pos["confirmed_qty"] > 0

    # Long: pnl = (exit - entry) × qty × point_value
    # Short: pnl = (entry - exit) × qty × point_value
    if IsLong:
        Pnl = (FillPrice - AvgEntry) * CloseQty * PointValue
    else:
        Pnl = (AvgEntry - FillPrice) * CloseQty * PointValue

    Conn = _GetConn()
    with _DBLock:
        Conn.execute(
            """INSERT INTO realized_pnl
               (instrument, category, close_qty, entry_price, exit_price, point_value, pnl_inr)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (Instrument, Category, CloseQty, AvgEntry, FillPrice, PointValue, Pnl)
        )
        Conn.commit()
    Logger.info("RealizePnl %s [%s]: entry=%.2f exit=%.2f qty=%d pv=%.2f → pnl=%.2f",
                Instrument, Category, AvgEntry, FillPrice, CloseQty, PointValue, Pnl)
    return Pnl


def GetCumulativeRealizedPnl():
    """Return the sum of all realized P&L in INR. Returns 0.0 if no rows."""
    Conn = _GetConn()
    Row = Conn.execute("SELECT COALESCE(SUM(pnl_inr), 0) FROM realized_pnl").fetchone()
    return float(Row[0])


def GetAvgEntryPrice(Instrument):
    """Return the current average entry price for an instrument."""
    Pos = GetSystemPosition(Instrument)
    return Pos.get("avg_entry_price", 0)


# ─── Daily P&L Report Queries ─────────────────────────────────────


def GetTodayRealizedPnl(DateStr=None):
    """Return today's realized P&L rows (IST date). DateStr overrides for backfill."""
    Conn = _GetConn()
    if DateStr is None:
        DateStr = datetime.now().strftime("%Y-%m-%d")
    Rows = Conn.execute(
        """SELECT instrument, category, close_qty, entry_price, exit_price,
                  point_value, pnl_inr, created_at
           FROM realized_pnl
           WHERE date(created_at, '+5 hours', '+30 minutes') = ?
           ORDER BY created_at DESC""",
        (DateStr,)
    ).fetchall()
    return [dict(r) for r in Rows]


def GetTodayFuturesOrders(DateStr=None):
    """Return today's filled futures orders."""
    Conn = _GetConn()
    if DateStr is None:
        DateStr = datetime.now().strftime("%Y-%m-%d")
    Rows = Conn.execute(
        """SELECT instrument, action, qty, fill_price, slippage,
                  execution_mode, status, created_at
           FROM order_log
           WHERE date(created_at, '+5 hours', '+30 minutes') = ?
             AND status IN ('FILLED', 'PLACED')
           ORDER BY created_at""",
        (DateStr,)
    ).fetchall()
    return [dict(r) for r in Rows]


def GetTodayOptionsOrders(DateStr=None):
    """Return today's options orders."""
    Conn = _GetConn()
    if DateStr is None:
        DateStr = datetime.now().strftime("%Y-%m-%d")
    Rows = Conn.execute(
        """SELECT underlying, strategy_name, leg, contract, action, qty,
                  fill_price, slippage, created_at
           FROM options_order_log
           WHERE date(created_at, '+5 hours', '+30 minutes') = ?
           ORDER BY created_at""",
        (DateStr,)
    ).fetchall()
    return [dict(r) for r in Rows]


def GetAllOpenPositions():
    """Return all system_positions with non-zero confirmed_qty."""
    Conn = _GetConn()
    Rows = Conn.execute(
        """SELECT instrument, target_qty, confirmed_qty, avg_entry_price,
                  point_value, updated_at
           FROM system_positions
           WHERE confirmed_qty != 0
           ORDER BY instrument"""
    ).fetchall()
    return [dict(r) for r in Rows]


def SaveDailySnapshot(ReportDate, Snapshots):
    """Save end-of-day unrealized P&L snapshot.

    Parameters
    ----------
    ReportDate : str
        Date string (YYYY-MM-DD).
    Snapshots : list of dict
        Each dict: instrument, confirmed_qty, avg_entry_price, ltp, unrealized_pnl.
    """
    Conn = _GetConn()
    with _DBLock:
        for S in Snapshots:
            Conn.execute(
                """INSERT OR REPLACE INTO daily_pnl_snapshot
                   (report_date, instrument, confirmed_qty, avg_entry_price, ltp, unrealized_pnl)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (ReportDate, S["instrument"], S["confirmed_qty"],
                 S["avg_entry_price"], S["ltp"], S["unrealized_pnl"])
            )
        Conn.commit()
    Logger.info("Saved daily snapshot for %s: %d instruments", ReportDate, len(Snapshots))


def GetPreviousSnapshot(ReportDate):
    """Load the most recent snapshot before ReportDate.

    Returns dict keyed by instrument: {instrument: unrealized_pnl}.
    """
    Conn = _GetConn()
    Rows = Conn.execute(
        """SELECT instrument, unrealized_pnl FROM daily_pnl_snapshot
           WHERE report_date = (
               SELECT MAX(report_date) FROM daily_pnl_snapshot
               WHERE report_date < ?
           )""",
        (ReportDate,)
    ).fetchall()
    return {r["instrument"]: r["unrealized_pnl"] for r in Rows}
