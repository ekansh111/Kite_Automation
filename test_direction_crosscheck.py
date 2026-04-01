"""
Test suite for the direction cross-check feature.

Tests all scenarios:
1. Normal flow: actions match delta direction → execute
2. Conflict: all subsystems say SELL but delta is BUY → HALT
3. Conflict: all subsystems say BUY but delta is SELL → HALT
4. Mixed actions (buy + sell) → always execute (no conflict)
5. No action field (backward compat) → always execute
6. Partial action (some None, some set) → only checks non-None
7. Single subsystem scenarios
8. Exact ZINCMINI scenario that triggered the bug
"""

import os
import sys
import json
import sqlite3
import tempfile
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

# Setup logging to see test output
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
_MISSING = object()


def _snapshot_modules(names):
    return {name: sys.modules.get(name, _MISSING) for name in names}


def _restore_modules(snapshot):
    for name, module in snapshot.items():
        if module is _MISSING:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module

# ─── Setup temp DB and config ────────────────────────────────────────

TmpDir = tempfile.mkdtemp()
TmpDB = os.path.join(TmpDir, "forecast_store.db")
TmpConfig = os.path.join(TmpDir, "instrument_config.json")
TmpEmailConfig = os.path.join(TmpDir, "email_config.json")

# Minimal config with 2 instruments for testing
TestConfig = {
    "account": {
        "base_capital": 10000000,
        "annual_vol_target_pct": 0.50,
        "dry_run": True
    },
    "instruments": {
        "ZINCMINI": {
            "enabled": True,
            "exchange": "MCX",
            "broker": "ZERODHA",
            "user": "YD6016",
            "point_value": 1000,
            "daily_vol_target": 10000,
            "FDM": 1.1,
            "forecast_cap": 20,
            "position_inertia_pct": 0.10,
            "subsystems": {
                "S15A": 0.50,
                "S45A": 0.50
            },
            "system_name_map": {
                "S15A_Zinc": "S15A",
                "S45A_Zinc": "S45A"
            },
            "order_routing": {
                "InstrumentType": "FUT",
                "Variety": "REGULAR",
                "Product": "NRML",
                "Validity": "DAY",
                "ContractNameProvided": "False",
                "QuantityMultiplier": 1
            }
        },
        "GOLDM": {
            "enabled": True,
            "exchange": "MCX",
            "broker": "ZERODHA",
            "user": "YD6016",
            "point_value": 10,
            "daily_vol_target": 40219,
            "FDM": 1.1,
            "forecast_cap": 20,
            "position_inertia_pct": 0.10,
            "subsystems": {
                "S30A": 0.34,
                "S30E": 0.33,
                "S30D": 0.33
            },
            "system_name_map": {
                "S30A_GoldM": "S30A",
                "S30E_GoldM": "S30E",
                "S30D_GoldM": "S30D"
            },
            "order_routing": {
                "InstrumentType": "FUT",
                "Variety": "REGULAR",
                "Product": "NRML",
                "Validity": "DAY",
                "ContractNameProvided": "False",
                "QuantityMultiplier": 1
            }
        }
    }
}

with open(TmpConfig, "w") as f:
    json.dump(TestConfig, f)

with open(TmpEmailConfig, "w") as f:
    json.dump({"sender": "test@test.com", "recipient": "test@test.com", "app_password": "pass"}, f)

_MODULE_SNAPSHOT = _snapshot_modules(
    [
        "Directories",
        "Kite_Server_Order_Handler",
        "Server_Order_Handler",
        "smart_chase",
        "kiteconnect",
        "forecast_db",
        "forecast_orchestrator",
    ]
)

# Patch modules before importing orchestrator
# Patch Directories.workInputRoot to use temp dir
DirectoriesModule = MagicMock()
DirectoriesModule.workInputRoot = Path(TmpDir)
sys.modules["Directories"] = DirectoriesModule

# Patch broker handlers
sys.modules["Kite_Server_Order_Handler"] = MagicMock()
sys.modules["Server_Order_Handler"] = MagicMock()
sys.modules["smart_chase"] = MagicMock()
sys.modules["kiteconnect"] = MagicMock()

# Patch DB path
import forecast_db as db
db.DB_PATH = TmpDB
db.InitDB()

# Now import orchestrator
from forecast_orchestrator import ForecastOrchestrator, EMAIL_CONFIG_PATH
_restore_modules(_MODULE_SNAPSHOT)

# ─── Test Helpers ────────────────────────────────────────────────────

PassCount = 0
FailCount = 0

def ResetDB():
    """Drop and recreate all tables."""
    global _Connection
    db._Connection = None
    if os.path.exists(TmpDB):
        os.remove(TmpDB)
    db.InitDB()

def SeedForecasts(Instrument, Forecasts):
    """Seed subsystem_forecasts with given data.
    Forecasts: list of (system_name, forecast, atr, action)
    """
    for SysName, FC, ATR, Action in Forecasts:
        db.UpsertForecast(Instrument, SysName, FC, ATR, Action)

def SeedPosition(Instrument, ConfirmedQty):
    """Seed system_positions."""
    db.UpdateSystemPosition(Instrument, ConfirmedQty, ConfirmedQty)

def GetHaltedOrders(Instrument):
    """Get all HALTED_CONFLICT orders for an instrument."""
    Conn = db._GetConn()
    Rows = Conn.execute(
        "SELECT * FROM order_log WHERE instrument = ? AND status = 'HALTED_CONFLICT'",
        (Instrument,)
    ).fetchall()
    return [dict(r) for r in Rows]

def GetDryRunOrders(Instrument):
    """Get all DRY_RUN orders for an instrument."""
    Conn = db._GetConn()
    Rows = Conn.execute(
        "SELECT * FROM order_log WHERE instrument = ? AND status = 'DRY_RUN'",
        (Instrument,)
    ).fetchall()
    return [dict(r) for r in Rows]

def AssertTest(Name, Condition, Detail=""):
    global PassCount, FailCount
    if Condition:
        print(f"  ✅ PASS: {Name}")
        PassCount += 1
    else:
        print(f"  ❌ FAIL: {Name} — {Detail}")
        FailCount += 1


# ─── Create Orchestrator ─────────────────────────────────────────────

Orch = ForecastOrchestrator(ConfigPath=TmpConfig)
# Don't call Start() — we'll call _ComputeAndExecute directly
# Patch email sending to avoid real SMTP
Orch._SendDirectionConflictAlert = MagicMock()
Orch._SendReconAlert = MagicMock()


# ═══════════════════════════════════════════════════════════════════════
# TEST SCENARIOS
# ═══════════════════════════════════════════════════════════════════════

# ─── SCENARIO 1: THE ZINCMINI BUG (exact reproduction) ──────────────
print("\n═══ SCENARIO 1: ZINCMINI Bug Reproduction ═══")
print("  DB thinks confirmed_qty=-3 (short), broker actually has +3 (long)")
print("  All subsystems go flat with action=sell (exiting long)")
print("  Orchestrator computes target=0, delta=+3 (BUY) → SHOULD HALT")

ResetDB()
# DB incorrectly thinks we're short 3
SeedPosition("ZINCMINI", -3)
# Both subsystems went flat (forecast=0) with action=sell (exiting their long)
SeedForecasts("ZINCMINI", [
    ("S15A", 0.0, 5.0, "sell"),
    ("S45A", 0.0, 5.0, "sell"),
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("ZINCMINI")

Halted = GetHaltedOrders("ZINCMINI")
DryRun = GetDryRunOrders("ZINCMINI")

AssertTest("Trade halted", len(Halted) == 1, f"Expected 1 halted, got {len(Halted)}")
AssertTest("No dry run executed", len(DryRun) == 0, f"Got {len(DryRun)} dry runs")
AssertTest("Conflict alert sent", Orch._SendDirectionConflictAlert.called, "Alert not sent")
if Halted:
    AssertTest("Logged as BUY halted", Halted[0]["action"] == "BUY", f"Got {Halted[0]['action']}")
    AssertTest("Reason contains DIRECTION CONFLICT", "DIRECTION CONFLICT" in (Halted[0].get("reason") or ""), "")


# ─── SCENARIO 2: Normal sell — all subsystems say sell, delta is sell ──
print("\n═══ SCENARIO 2: Normal SELL — actions match delta ═══")
print("  DB correctly shows confirmed_qty=+3, target becomes 0, delta=-3 (SELL)")
print("  Subsystems say sell → SHOULD EXECUTE (no conflict)")

ResetDB()
SeedPosition("ZINCMINI", 3)  # DB correctly shows long 3
SeedForecasts("ZINCMINI", [
    ("S15A", 0.0, 5.0, "sell"),
    ("S45A", 0.0, 5.0, "sell"),
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("ZINCMINI")

Halted = GetHaltedOrders("ZINCMINI")
DryRun = GetDryRunOrders("ZINCMINI")

AssertTest("Trade NOT halted", len(Halted) == 0, f"Got {len(Halted)} halted")
AssertTest("Dry run executed", len(DryRun) == 1, f"Got {len(DryRun)} dry runs")
AssertTest("No conflict alert", not Orch._SendDirectionConflictAlert.called, "Alert was sent")


# ─── SCENARIO 3: Normal buy — all subsystems say buy, delta is buy ──
print("\n═══ SCENARIO 3: Normal BUY — actions match delta ═══")
print("  DB at 0, subsystems say buy (entering long), target becomes positive")

ResetDB()
SeedPosition("ZINCMINI", 0)
SeedForecasts("ZINCMINI", [
    ("S15A", 10.0, 5.0, "buy"),
    ("S45A", 10.0, 5.0, "buy"),
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("ZINCMINI")

Halted = GetHaltedOrders("ZINCMINI")
DryRun = GetDryRunOrders("ZINCMINI")

AssertTest("Trade NOT halted", len(Halted) == 0, f"Got {len(Halted)} halted")
AssertTest("Dry run executed", len(DryRun) == 1, f"Got {len(DryRun)} dry runs")
if DryRun:
    AssertTest("Action is BUY", DryRun[0]["action"] == "BUY", f"Got {DryRun[0]['action']}")


# ─── SCENARIO 4: Reverse conflict — all BUY but delta is SELL ──
print("\n═══ SCENARIO 4: Reverse Conflict — all BUY but delta SELL ═══")
print("  DB incorrectly shows +5, subsystems say buy, but target is +2, delta=-3 (SELL)")
print("  This shouldn't happen normally... but if DB is way off → HALT")

ResetDB()
SeedPosition("ZINCMINI", 5)  # DB thinks we're long 5, way too high
SeedForecasts("ZINCMINI", [
    ("S15A", 10.0, 5.0, "buy"),
    ("S45A", 10.0, 5.0, "buy"),
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("ZINCMINI")

Halted = GetHaltedOrders("ZINCMINI")
DryRun = GetDryRunOrders("ZINCMINI")

# Target = combined(11) * vol_scalar / 10. With ATR=5, PV=1000, daily_vol=10000:
# vol_scalar = 10000 / (5*1000) = 2.0. pos = 11 * 2 / 10 = 2.2 → target=2
# delta = 2 - 5 = -3 (SELL). All actions say "buy" → CONFLICT!
AssertTest("Trade halted", len(Halted) == 1, f"Expected 1 halted, got {len(Halted)}")
AssertTest("No dry run", len(DryRun) == 0, f"Got {len(DryRun)} dry runs")
AssertTest("Conflict alert sent", Orch._SendDirectionConflictAlert.called, "Alert not sent")


# ─── SCENARIO 5: Mixed actions — no conflict possible ──
print("\n═══ SCENARIO 5: Mixed Actions — one buy, one sell ═══")
print("  Mixed signals → never a conflict, always execute")

ResetDB()
SeedPosition("ZINCMINI", 0)
SeedForecasts("ZINCMINI", [
    ("S15A", 10.0, 5.0, "buy"),   # S15A going long
    ("S45A", -10.0, 5.0, "sell"),  # S45A going short
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("ZINCMINI")

Halted = GetHaltedOrders("ZINCMINI")

# Combined = (0.5*10 + 0.5*-10) * 1.1 = 0 → target=0, delta=0 → nothing to do
# But let's check no halt happened
AssertTest("Trade NOT halted", len(Halted) == 0, f"Got {len(Halted)} halted")
AssertTest("No conflict alert", not Orch._SendDirectionConflictAlert.called, "Alert was sent")


# ─── SCENARIO 6: No action field (backward compatibility) ──
print("\n═══ SCENARIO 6: No Action Field — Backward Compat ═══")
print("  Old alerts without Action field → cross-check skipped, always execute")

ResetDB()
SeedPosition("ZINCMINI", -3)  # Wrong DB state (same as bug scenario)
SeedForecasts("ZINCMINI", [
    ("S15A", 0.0, 5.0, None),  # No action
    ("S45A", 0.0, 5.0, None),  # No action
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("ZINCMINI")

Halted = GetHaltedOrders("ZINCMINI")
DryRun = GetDryRunOrders("ZINCMINI")

# Without action field, cross-check is skipped → trade executes (even though wrong)
# This is expected — backward compat means old alerts still work, just without safety
AssertTest("Trade NOT halted (no action data)", len(Halted) == 0, f"Got {len(Halted)} halted")
AssertTest("Dry run executed", len(DryRun) == 1, f"Got {len(DryRun)} dry runs")
AssertTest("No conflict alert", not Orch._SendDirectionConflictAlert.called, "Alert was sent")


# ─── SCENARIO 7: Partial actions — one has action, one doesn't ──
print("\n═══ SCENARIO 7: Partial Actions — one set, one None ═══")
print("  Only subsystems WITH action are checked")

ResetDB()
SeedPosition("ZINCMINI", -3)  # Wrong DB state
SeedForecasts("ZINCMINI", [
    ("S15A", 0.0, 5.0, "sell"),  # Has action
    ("S45A", 0.0, 5.0, None),    # No action (old alert)
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("ZINCMINI")

Halted = GetHaltedOrders("ZINCMINI")

# Only S15A has action="sell", S45A has None (filtered out)
# SubsystemActions = ["sell"] → AllSell=True, delta=+3 (BUY) → CONFLICT → HALT
AssertTest("Trade halted (partial action still catches conflict)", len(Halted) == 1,
           f"Expected 1 halted, got {len(Halted)}")


# ─── SCENARIO 8: GOLDM with 3 subsystems — 2 sell, 1 buy ──
print("\n═══ SCENARIO 8: GOLDM — 2 sell, 1 buy (mixed) ═══")
print("  Not all same direction → no conflict, always execute")

ResetDB()
SeedPosition("GOLDM", 0)
SeedForecasts("GOLDM", [
    ("S30A", -10.0, 120.0, "sell"),
    ("S30E", -10.0, 120.0, "sell"),
    ("S30D", 10.0, 120.0, "buy"),
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("GOLDM")

Halted = GetHaltedOrders("GOLDM")
AssertTest("Trade NOT halted (mixed actions)", len(Halted) == 0, f"Got {len(Halted)} halted")
AssertTest("No conflict alert", not Orch._SendDirectionConflictAlert.called, "Alert was sent")


# ─── SCENARIO 9: All sell, delta also sell → normal execution ──
print("\n═══ SCENARIO 9: GOLDM — all sell, delta sell (correct) ═══")

ResetDB()
SeedPosition("GOLDM", 1)  # Currently long 1
SeedForecasts("GOLDM", [
    ("S30A", -10.0, 120.0, "sell"),
    ("S30E", -10.0, 120.0, "sell"),
    ("S30D", -10.0, 120.0, "sell"),
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("GOLDM")

Halted = GetHaltedOrders("GOLDM")
DryRun = GetDryRunOrders("GOLDM")

AssertTest("Trade NOT halted", len(Halted) == 0, f"Got {len(Halted)} halted")
AssertTest("Dry run executed", len(DryRun) >= 1, f"Got {len(DryRun)} dry runs")


# ─── SCENARIO 10: Webhook parsing of Action field ──
print("\n═══ SCENARIO 10: Webhook Action Parsing ═══")

ResetDB()
# Test various Action formats from TradingView
TestCases = [
    ({"SystemName": "S15A_Zinc", "Instrument": "ZINCMINI", "Netposition": 1, "ATR": 5.0, "Action": "buy"}, "buy"),
    ({"SystemName": "S15A_Zinc", "Instrument": "ZINCMINI", "Netposition": -1, "ATR": 5.0, "Action": "sell"}, "sell"),
    ({"SystemName": "S15A_Zinc", "Instrument": "ZINCMINI", "Netposition": 0, "ATR": 5.0, "Action": "sell"}, "sell"),
    ({"SystemName": "S15A_Zinc", "Instrument": "ZINCMINI", "Netposition": 1, "ATR": 5.0, "Action": "Buy"}, "buy"),  # uppercase
    ({"SystemName": "S15A_Zinc", "Instrument": "ZINCMINI", "Netposition": 1, "ATR": 5.0, "Action": " sell "}, "sell"),  # whitespace
    ({"SystemName": "S15A_Zinc", "Instrument": "ZINCMINI", "Netposition": 1, "ATR": 5.0}, None),  # missing
    ({"SystemName": "S15A_Zinc", "Instrument": "ZINCMINI", "Netposition": 1, "ATR": 5.0, "Action": ""}, None),  # empty
]

for Payload, ExpectedAction in TestCases:
    Result = Orch.HandleWebhook(Payload)
    AssertTest(
        f"Action='{Payload.get('Action', '<missing>')}' → parsed as {ExpectedAction}",
        Result.get("action") == ExpectedAction,
        f"Got {Result.get('action')}"
    )


# ─── SCENARIO 11: Delta = 0 with conflicting actions → no issue ──
print("\n═══ SCENARIO 11: Delta=0, target=current → nothing to do ═══")

ResetDB()
SeedPosition("ZINCMINI", 0)
SeedForecasts("ZINCMINI", [
    ("S15A", 0.0, 5.0, "sell"),
    ("S45A", 0.0, 5.0, "sell"),
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("ZINCMINI")

Halted = GetHaltedOrders("ZINCMINI")
DryRun = GetDryRunOrders("ZINCMINI")

# target=0, current=0, delta=0 → early return before cross-check
AssertTest("No halt (delta=0 exits early)", len(Halted) == 0, f"Got {len(Halted)} halted")
AssertTest("No dry run (nothing to do)", len(DryRun) == 0, f"Got {len(DryRun)} dry runs")


# ─── SCENARIO 12: Single subsystem entering short — sell action, sell delta ──
print("\n═══ SCENARIO 12: Single Subsystem — sell enters short (correct) ═══")

ResetDB()
SeedPosition("ZINCMINI", 0)
SeedForecasts("ZINCMINI", [
    ("S15A", -10.0, 5.0, "sell"),
    # S45A hasn't reported yet (not in DB)
])

Orch._SendDirectionConflictAlert.reset_mock()
Orch._ComputeAndExecute("ZINCMINI")

Halted = GetHaltedOrders("ZINCMINI")
DryRun = GetDryRunOrders("ZINCMINI")

# Only S15A: combined = 0.5*(-10)*1.1 = -5.5. vol_scalar=2. pos=-1.1. target=-1.
# delta = -1 - 0 = -1 (SELL). Action=sell → matches → execute
AssertTest("Trade NOT halted", len(Halted) == 0, f"Got {len(Halted)} halted")
AssertTest("Dry run executed", len(DryRun) == 1, f"Got {len(DryRun)} dry runs")


# ═══════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════

print(f"\n{'═' * 60}")
print(f"  RESULTS: {PassCount} passed, {FailCount} failed")
print(f"{'═' * 60}")

# Cleanup
import shutil
shutil.rmtree(TmpDir, ignore_errors=True)

if __name__ == "__main__":
    sys.exit(1 if FailCount > 0 else 0)

if FailCount > 0:
    raise AssertionError(f"Direction cross-check scenarios failed: {FailCount} failures")
