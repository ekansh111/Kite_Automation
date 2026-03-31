"""
Comprehensive tests for the P&L tracking system.

Tests:
  1. Database layer: UpdateCostBasis, RealizePnl, GetCumulativeRealizedPnl, ResetCostBasis
  2. Effective capital computation
  3. Options P&L (short straddle entry/exit/expire)
  4. Futures P&L (long entry/exit, short entry/exit, partial close, flip)
  5. Edge cases: zero fills, no prior position, multiple instruments
"""

import os
import sys
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

# Patch Directories before importing forecast_db
_TEST_DIR = tempfile.mkdtemp()
_MOCK_WORK_ROOT = Path(_TEST_DIR)


class MockDirectories:
    workInputRoot = _MOCK_WORK_ROOT


sys.modules["Directories"] = MockDirectories()

import forecast_db as db


class TestPnlDatabase(unittest.TestCase):
    """Test the P&L functions in forecast_db.py in isolation."""

    def setUp(self):
        """Create a fresh in-memory DB for each test."""
        # Reset module-level connection
        db._Connection = None
        db.DB_PATH = os.path.join(_TEST_DIR, f"test_{id(self)}.db")
        db.InitDB()

    def tearDown(self):
        if db._Connection:
            db._Connection.close()
            db._Connection = None
        path = db.DB_PATH
        if os.path.exists(path):
            os.unlink(path)

    # ── Helper ──────────────────────────────────────────────────────

    def _create_position(self, instrument, target_qty, confirmed_qty,
                         avg_entry_price=0, point_value=1):
        """Directly insert a position row for testing."""
        conn = db._GetConn()
        conn.execute(
            """INSERT OR REPLACE INTO system_positions
               (instrument, target_qty, confirmed_qty, avg_entry_price, point_value, updated_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))""",
            (instrument, target_qty, confirmed_qty, avg_entry_price, point_value)
        )
        conn.commit()

    # ── GetSystemPosition ───────────────────────────────────────────

    def test_get_system_position_missing(self):
        """Non-existent instrument returns defaults."""
        pos = db.GetSystemPosition("NONEXISTENT")
        self.assertEqual(pos["target_qty"], 0)
        self.assertEqual(pos["confirmed_qty"], 0)
        self.assertEqual(pos["avg_entry_price"], 0)
        self.assertEqual(pos["point_value"], 1)

    def test_get_system_position_existing(self):
        """Existing instrument returns stored values."""
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)
        pos = db.GetSystemPosition("GOLDM")
        self.assertEqual(pos["confirmed_qty"], 2)
        self.assertAlmostEqual(pos["avg_entry_price"], 72500.0)
        self.assertAlmostEqual(pos["point_value"], 100.0)

    # ── UpdateSystemPosition preserves cost basis ───────────────────

    def test_update_system_position_preserves_cost_basis(self):
        """INSERT ... ON CONFLICT must NOT wipe avg_entry_price/point_value."""
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)

        # Simulate what the orchestrator does after a fill
        db.UpdateSystemPosition("GOLDM", 3, 3)

        pos = db.GetSystemPosition("GOLDM")
        self.assertEqual(pos["target_qty"], 3)
        self.assertEqual(pos["confirmed_qty"], 3)
        self.assertAlmostEqual(pos["avg_entry_price"], 72500.0,
                               msg="avg_entry_price should survive UpdateSystemPosition")
        self.assertAlmostEqual(pos["point_value"], 100.0,
                               msg="point_value should survive UpdateSystemPosition")

    def test_update_system_position_creates_new_row(self):
        """UpdateSystemPosition should work even if the row doesn't exist yet."""
        db.UpdateSystemPosition("NEWTEST", 1, 1)
        pos = db.GetSystemPosition("NEWTEST")
        self.assertEqual(pos["target_qty"], 1)
        self.assertEqual(pos["confirmed_qty"], 1)
        self.assertEqual(pos["avg_entry_price"], 0)  # default

    # ── UpdateCostBasis ─────────────────────────────────────────────

    def test_cost_basis_fresh_position(self):
        """First fill sets avg_entry_price = fill_price."""
        self._create_position("GOLDM", 2, 2)  # no prior avg_entry

        db.UpdateCostBasis("GOLDM", 72500.0, 2, 100.0)

        pos = db.GetSystemPosition("GOLDM")
        self.assertAlmostEqual(pos["avg_entry_price"], 72500.0)
        self.assertAlmostEqual(pos["point_value"], 100.0)

    def test_cost_basis_weighted_average(self):
        """Second fill computes weighted average."""
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)

        # Add 1 more lot at 73000
        db.UpdateCostBasis("GOLDM", 73000.0, 1, 100.0)

        pos = db.GetSystemPosition("GOLDM")
        expected = (72500.0 * 2 + 73000.0 * 1) / 3  # 72666.67
        self.assertAlmostEqual(pos["avg_entry_price"], expected, places=2)

    def test_cost_basis_no_position_row(self):
        """UpdateCostBasis for an instrument with no system_positions row (options)."""
        # No prior position row exists
        db.UpdateCostBasis("NIFTY_OPT_CE", 150.0, 75, 1.0)

        pos = db.GetSystemPosition("NIFTY_OPT_CE")
        self.assertAlmostEqual(pos["avg_entry_price"], 150.0)
        self.assertAlmostEqual(pos["point_value"], 1.0)

    def test_cost_basis_multiple_adds_smart_chase(self):
        """Three successive adds (smart chase path: cost basis BEFORE position update)."""
        self._create_position("SILVERM", 0, 0)

        # Smart chase path: UpdateCostBasis runs before UpdateSystemPosition
        db.UpdateCostBasis("SILVERM", 90000.0, 1, 30.0)  # confirmed_qty=0 → fresh
        db.UpdateSystemPosition("SILVERM", 1, 1)

        db.UpdateCostBasis("SILVERM", 91000.0, 1, 30.0)  # confirmed_qty=1
        db.UpdateSystemPosition("SILVERM", 2, 2)

        db.UpdateCostBasis("SILVERM", 92000.0, 1, 30.0)  # confirmed_qty=2
        db.UpdateSystemPosition("SILVERM", 3, 3)

        pos = db.GetSystemPosition("SILVERM")
        expected = (90000 + 91000 + 92000) / 3  # 91000
        self.assertAlmostEqual(pos["avg_entry_price"], expected, places=2)

    def test_cost_basis_multiple_adds_legacy(self):
        """Three successive adds (legacy path: position update THEN cost basis with OldQty)."""
        self._create_position("SILVERM", 0, 0)

        # Legacy path: UpdateSystemPosition runs first, OldQty passed explicitly
        db.UpdateSystemPosition("SILVERM", 1, 1)
        db.UpdateCostBasis("SILVERM", 90000.0, 1, 30.0, OldQty=0)

        db.UpdateSystemPosition("SILVERM", 2, 2)
        db.UpdateCostBasis("SILVERM", 91000.0, 1, 30.0, OldQty=1)

        db.UpdateSystemPosition("SILVERM", 3, 3)
        db.UpdateCostBasis("SILVERM", 92000.0, 1, 30.0, OldQty=2)

        pos = db.GetSystemPosition("SILVERM")
        expected = (90000 + 91000 + 92000) / 3  # 91000
        self.assertAlmostEqual(pos["avg_entry_price"], expected, places=2)

    # ── RealizePnl ──────────────────────────────────────────────────

    def test_realize_pnl_long_profit(self):
        """Long position closed at higher price = profit."""
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)

        pnl = db.RealizePnl("GOLDM", 73000.0, 2, 100.0, "futures")

        # (73000 - 72500) * 2 * 100 = 100000
        self.assertAlmostEqual(pnl, 100000.0)

    def test_realize_pnl_long_loss(self):
        """Long position closed at lower price = loss."""
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)

        pnl = db.RealizePnl("GOLDM", 72000.0, 2, 100.0, "futures")

        # (72000 - 72500) * 2 * 100 = -100000
        self.assertAlmostEqual(pnl, -100000.0)

    def test_realize_pnl_short_profit(self):
        """Short position closed at lower price = profit."""
        self._create_position("CRUDEOILM", -3, -3, 5500.0, 100.0)

        pnl = db.RealizePnl("CRUDEOILM", 5400.0, 3, 100.0, "futures")

        # (5500 - 5400) * 3 * 100 = 30000
        self.assertAlmostEqual(pnl, 30000.0)

    def test_realize_pnl_short_loss(self):
        """Short position closed at higher price = loss."""
        self._create_position("CRUDEOILM", -3, -3, 5500.0, 100.0)

        pnl = db.RealizePnl("CRUDEOILM", 5700.0, 3, 100.0, "futures")

        # (5500 - 5700) * 3 * 100 = -60000
        self.assertAlmostEqual(pnl, -60000.0)

    def test_realize_pnl_partial_close(self):
        """Closing 1 of 3 lots."""
        self._create_position("GOLDM", 3, 3, 72500.0, 100.0)

        pnl = db.RealizePnl("GOLDM", 73000.0, 1, 100.0, "futures")

        # (73000 - 72500) * 1 * 100 = 50000
        self.assertAlmostEqual(pnl, 50000.0)

    def test_realize_pnl_was_long_override(self):
        """WasLong=True overrides DB confirmed_qty (used for legacy orders)."""
        # Position already updated to 0 in DB
        self._create_position("GOLDM", 0, 0, 72500.0, 100.0)

        pnl = db.RealizePnl("GOLDM", 73000.0, 2, 100.0, "futures", WasLong=True)

        # Should use long formula: (73000 - 72500) * 2 * 100 = 100000
        self.assertAlmostEqual(pnl, 100000.0)

    def test_realize_pnl_was_long_false_override(self):
        """WasLong=False forces short P&L even with zero confirmed_qty (options)."""
        # Options instrument - no real confirmed_qty
        self._create_position("NIFTY_OPT_CE", 0, 0, 150.0, 1.0)

        pnl = db.RealizePnl("NIFTY_OPT_CE", 120.0, 75, 1.0, "options", WasLong=False)

        # Short formula: (150 - 120) * 75 * 1.0 = 2250
        self.assertAlmostEqual(pnl, 2250.0)

    def test_realize_pnl_inserts_row(self):
        """RealizePnl should create a row in realized_pnl table."""
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)
        db.RealizePnl("GOLDM", 73000.0, 2, 100.0, "futures")

        conn = db._GetConn()
        row = conn.execute("SELECT * FROM realized_pnl WHERE instrument = 'GOLDM'").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["category"], "futures")
        self.assertEqual(row["close_qty"], 2)
        self.assertAlmostEqual(row["entry_price"], 72500.0)
        self.assertAlmostEqual(row["exit_price"], 73000.0)
        self.assertAlmostEqual(row["pnl_inr"], 100000.0)

    # ── Options P&L (short straddle) ────────────────────────────────

    def test_options_straddle_profit_on_expire(self):
        """Short straddle that expires worthless = full premium profit."""
        # Entry: SELL CE at 150, SELL PE at 130, qty=75
        db.UpdateCostBasis("NIFTY_OPT_CE", 150.0, 75, 1.0)
        db.UpdateCostBasis("NIFTY_OPT_PE", 130.0, 75, 1.0)

        # Expire worthless (exit at 0)
        ce_pnl = db.RealizePnl("NIFTY_OPT_CE", 0, 75, 1.0, "options", WasLong=False)
        pe_pnl = db.RealizePnl("NIFTY_OPT_PE", 0, 75, 1.0, "options", WasLong=False)

        # CE: (150 - 0) * 75 * 1 = 11250
        # PE: (130 - 0) * 75 * 1 = 9750
        self.assertAlmostEqual(ce_pnl, 11250.0)
        self.assertAlmostEqual(pe_pnl, 9750.0)
        self.assertAlmostEqual(db.GetCumulativeRealizedPnl(), 21000.0)

    def test_options_straddle_loss_on_exit(self):
        """Short straddle bought back at higher price = loss."""
        # Entry: SELL CE at 150, SELL PE at 130, qty=75
        db.UpdateCostBasis("NIFTY_OPT_CE", 150.0, 75, 1.0)
        db.UpdateCostBasis("NIFTY_OPT_PE", 130.0, 75, 1.0)

        # Exit: BUY CE at 200, BUY PE at 180 (big move, both legs go up)
        ce_pnl = db.RealizePnl("NIFTY_OPT_CE", 200.0, 75, 1.0, "options", WasLong=False)
        pe_pnl = db.RealizePnl("NIFTY_OPT_PE", 180.0, 75, 1.0, "options", WasLong=False)

        # CE: (150 - 200) * 75 * 1 = -3750
        # PE: (130 - 180) * 75 * 1 = -3750
        self.assertAlmostEqual(ce_pnl, -3750.0)
        self.assertAlmostEqual(pe_pnl, -3750.0)
        self.assertAlmostEqual(db.GetCumulativeRealizedPnl(), -7500.0)

    def test_options_mixed_outcome(self):
        """CE goes up (loss), PE expires worthless (profit)."""
        db.UpdateCostBasis("NIFTY_OPT_CE", 150.0, 75, 1.0)
        db.UpdateCostBasis("NIFTY_OPT_PE", 130.0, 75, 1.0)

        ce_pnl = db.RealizePnl("NIFTY_OPT_CE", 300.0, 75, 1.0, "options", WasLong=False)
        pe_pnl = db.RealizePnl("NIFTY_OPT_PE", 0, 75, 1.0, "options", WasLong=False)

        # CE: (150 - 300) * 75 = -11250
        # PE: (130 - 0) * 75 = 9750
        self.assertAlmostEqual(ce_pnl, -11250.0)
        self.assertAlmostEqual(pe_pnl, 9750.0)
        self.assertAlmostEqual(db.GetCumulativeRealizedPnl(), -1500.0)

    # ── ResetCostBasis ──────────────────────────────────────────────

    def test_reset_cost_basis(self):
        """ResetCostBasis zeroes avg_entry_price."""
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)
        db.ResetCostBasis("GOLDM")
        pos = db.GetSystemPosition("GOLDM")
        self.assertAlmostEqual(pos["avg_entry_price"], 0)

    # ── Position Flip ───────────────────────────────────────────────

    def test_flip_long_to_short(self):
        """Flip +3 → -2: realize P&L on 3 longs, then new short basis."""
        self._create_position("GOLDM", 3, 3, 72500.0, 100.0)

        # Step 1: Realize P&L on the close of 3 longs (exit at 73000)
        pnl = db.RealizePnl("GOLDM", 73000.0, 3, 100.0, "futures")
        self.assertAlmostEqual(pnl, 150000.0)  # (73000-72500)*3*100

        # Step 2: Reset cost basis
        db.ResetCostBasis("GOLDM")

        # Step 3: Set new short cost basis at 73000
        db.UpdateCostBasis("GOLDM", 73000.0, 2, 100.0)

        pos = db.GetSystemPosition("GOLDM")
        self.assertAlmostEqual(pos["avg_entry_price"], 73000.0,
                               msg="After flip, avg_entry should be the new fill price")

    def test_flip_short_to_long(self):
        """Flip -2 → +1: realize P&L on 2 shorts, then new long basis."""
        self._create_position("CRUDEOILM", -2, -2, 5500.0, 100.0)

        # Close 2 shorts at 5400 = profit
        pnl = db.RealizePnl("CRUDEOILM", 5400.0, 2, 100.0, "futures")
        self.assertAlmostEqual(pnl, 20000.0)  # (5500-5400)*2*100

        db.ResetCostBasis("CRUDEOILM")
        db.UpdateCostBasis("CRUDEOILM", 5400.0, 1, 100.0)

        pos = db.GetSystemPosition("CRUDEOILM")
        self.assertAlmostEqual(pos["avg_entry_price"], 5400.0)

    # ── GetCumulativeRealizedPnl ────────────────────────────────────

    def test_cumulative_pnl_empty(self):
        """No realized P&L rows → 0."""
        self.assertAlmostEqual(db.GetCumulativeRealizedPnl(), 0.0)

    def test_cumulative_pnl_multiple_instruments(self):
        """P&L across different instruments sums correctly."""
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)
        self._create_position("SILVERM", -1, -1, 90000.0, 30.0)

        db.RealizePnl("GOLDM", 73000.0, 2, 100.0, "futures")       # +100000
        db.RealizePnl("SILVERM", 89000.0, 1, 30.0, "futures")       # (90000-89000)*1*30 = +30000

        db.UpdateCostBasis("NIFTY_OPT_CE", 150.0, 75, 1.0)
        db.RealizePnl("NIFTY_OPT_CE", 0, 75, 1.0, "options", WasLong=False)  # +11250

        total = db.GetCumulativeRealizedPnl()
        self.assertAlmostEqual(total, 141250.0)

    def test_cumulative_pnl_with_losses(self):
        """Losses reduce the cumulative total."""
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)
        db.RealizePnl("GOLDM", 72000.0, 2, 100.0, "futures")  # -100000

        self._create_position("SILVERM", 1, 1, 90000.0, 30.0)
        db.RealizePnl("SILVERM", 92000.0, 1, 30.0, "futures")  # +60000

        self.assertAlmostEqual(db.GetCumulativeRealizedPnl(), -40000.0)

    # ── GetAvgEntryPrice ────────────────────────────────────────────

    def test_get_avg_entry_price(self):
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)
        self.assertAlmostEqual(db.GetAvgEntryPrice("GOLDM"), 72500.0)

    def test_get_avg_entry_price_missing(self):
        self.assertAlmostEqual(db.GetAvgEntryPrice("NONEXISTENT"), 0)

    # ── Edge Cases ──────────────────────────────────────────────────

    def test_realize_pnl_zero_avg_entry(self):
        """If avg_entry is 0 (no prior basis), P&L should still compute."""
        self._create_position("GOLDM", 2, 2, 0, 100.0)
        pnl = db.RealizePnl("GOLDM", 73000.0, 2, 100.0, "futures")
        # (73000 - 0) * 2 * 100 = 14600000 — this is technically wrong
        # but the safety is: don't call RealizePnl without a cost basis
        self.assertAlmostEqual(pnl, 14600000.0)

    def test_multiple_partial_closes(self):
        """Close a position in stages."""
        self._create_position("GOLDM", 5, 5, 72500.0, 100.0)

        # Close 2 at 73000
        pnl1 = db.RealizePnl("GOLDM", 73000.0, 2, 100.0, "futures")
        self.assertAlmostEqual(pnl1, 100000.0)

        # Close 2 more at 73500
        pnl2 = db.RealizePnl("GOLDM", 73500.0, 2, 100.0, "futures")
        self.assertAlmostEqual(pnl2, 200000.0)  # (73500-72500)*2*100

        # Close last 1 at 74000
        pnl3 = db.RealizePnl("GOLDM", 74000.0, 1, 100.0, "futures")
        self.assertAlmostEqual(pnl3, 150000.0)  # (74000-72500)*1*100

        total = db.GetCumulativeRealizedPnl()
        self.assertAlmostEqual(total, 450000.0)


class TestEffectiveCapital(unittest.TestCase):
    """Test that effective capital flows through to vol target computation."""

    def test_effective_capital_with_profit(self):
        """base_capital + positive P&L = larger targets."""
        from vol_target import compute_daily_vol_target

        base = 9999999
        pnl = 500000
        effective = base + pnl
        vol_pct = 0.50
        weights = {"sector_weight": 0.4, "sub_sector_weight": 0.3,
                   "sub_class_weight": 0.5, "asset_weight": 0.5,
                   "asset_DM": 3.9, "instrument_DM": 1.1}

        target_base = compute_daily_vol_target(base, vol_pct, weights)
        target_effective = compute_daily_vol_target(effective, vol_pct, weights)

        self.assertGreater(target_effective, target_base)
        ratio = target_effective / target_base
        expected_ratio = effective / base
        self.assertAlmostEqual(ratio, expected_ratio, places=6)

    def test_effective_capital_with_loss(self):
        """base_capital + negative P&L = smaller targets."""
        from vol_target import compute_daily_vol_target

        base = 9999999
        pnl = -200000
        effective = base + pnl
        vol_pct = 0.50
        weights = {"sector_weight": 0.4}

        target_base = compute_daily_vol_target(base, vol_pct, weights)
        target_effective = compute_daily_vol_target(effective, vol_pct, weights)

        self.assertLess(target_effective, target_base)

    def test_effective_capital_zero_pnl(self):
        """No P&L → effective = base."""
        from vol_target import compute_daily_vol_target

        base = 9999999
        vol_pct = 0.50
        weights = {"sector_weight": 0.4}

        target_base = compute_daily_vol_target(base, vol_pct, weights)
        target_zero = compute_daily_vol_target(base + 0, vol_pct, weights)

        self.assertAlmostEqual(target_base, target_zero)


class TestConfigKeyRename(unittest.TestCase):
    """Verify instrument_config.json uses base_capital."""

    def test_config_has_base_capital(self):
        config_path = Path(__file__).parent / "instrument_config.json"
        with open(config_path) as f:
            cfg = json.load(f)
        self.assertIn("base_capital", cfg["account"])
        self.assertNotIn("total_capital", cfg["account"])
        self.assertEqual(cfg["account"]["base_capital"], 9999999)


class TestEndToEndScenarios(unittest.TestCase):
    """Simulate realistic trading scenarios end-to-end."""

    def setUp(self):
        db._Connection = None
        db.DB_PATH = os.path.join(_TEST_DIR, f"e2e_{id(self)}.db")
        db.InitDB()

    def tearDown(self):
        if db._Connection:
            db._Connection.close()
            db._Connection = None
        if os.path.exists(db.DB_PATH):
            os.unlink(db.DB_PATH)

    def _create_position(self, instrument, target_qty, confirmed_qty,
                         avg_entry_price=0, point_value=1):
        conn = db._GetConn()
        conn.execute(
            """INSERT OR REPLACE INTO system_positions
               (instrument, target_qty, confirmed_qty, avg_entry_price, point_value, updated_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))""",
            (instrument, target_qty, confirmed_qty, avg_entry_price, point_value)
        )
        conn.commit()

    def test_scenario_gold_lifecycle(self):
        """Full GOLDM lifecycle (smart chase path): enter 2 → add 1 → partial close 1 → close all."""
        pv = 100.0  # GOLDM point value

        # 1. Enter 2 lots at 72500 (smart chase: P&L before position update)
        db.UpdateCostBasis("GOLDM", 72500.0, 2, pv)
        db.UpdateSystemPosition("GOLDM", 2, 2)
        self.assertAlmostEqual(db.GetAvgEntryPrice("GOLDM"), 72500.0)

        # 2. Add 1 lot at 73000
        db.UpdateCostBasis("GOLDM", 73000.0, 1, pv)  # confirmed_qty=2 in DB
        db.UpdateSystemPosition("GOLDM", 3, 3)
        expected_avg = (72500 * 2 + 73000 * 1) / 3
        self.assertAlmostEqual(db.GetAvgEntryPrice("GOLDM"), expected_avg, places=2)

        # 3. Close 1 lot at 73500 (exit: P&L before update)
        pnl1 = db.RealizePnl("GOLDM", 73500.0, 1, pv, "futures")
        db.UpdateSystemPosition("GOLDM", 2, 2)
        expected_pnl1 = (73500 - expected_avg) * 1 * pv
        self.assertAlmostEqual(pnl1, expected_pnl1, places=2)

        # 4. Close remaining 2 at 74000
        pnl2 = db.RealizePnl("GOLDM", 74000.0, 2, pv, "futures")
        db.UpdateSystemPosition("GOLDM", 0, 0)
        expected_pnl2 = (74000 - expected_avg) * 2 * pv
        self.assertAlmostEqual(pnl2, expected_pnl2, places=2)

        # Total P&L
        total = db.GetCumulativeRealizedPnl()
        self.assertAlmostEqual(total, expected_pnl1 + expected_pnl2, places=2)

    def test_scenario_short_crude_with_flip(self):
        """Short CRUDEOILM → flip to long."""
        pv = 100.0

        # 1. Enter short 3 at 5500
        db.UpdateSystemPosition("CRUDEOILM", -3, -3)
        db.UpdateCostBasis("CRUDEOILM", 5500.0, 3, pv)

        # 2. Flip to long 2 at 5400 (price dropped = profit on shorts)
        # Step a: realize P&L on closing 3 shorts
        pnl = db.RealizePnl("CRUDEOILM", 5400.0, 3, pv, "futures")
        self.assertAlmostEqual(pnl, 30000.0)  # (5500-5400)*3*100

        # Step b: reset cost basis
        db.ResetCostBasis("CRUDEOILM")

        # Step c: new long cost basis
        db.UpdateCostBasis("CRUDEOILM", 5400.0, 2, pv)
        db.UpdateSystemPosition("CRUDEOILM", 2, 2)

        pos = db.GetSystemPosition("CRUDEOILM")
        self.assertAlmostEqual(pos["avg_entry_price"], 5400.0)

        # 3. Close long 2 at 5600 (profit)
        pnl2 = db.RealizePnl("CRUDEOILM", 5600.0, 2, pv, "futures")
        self.assertAlmostEqual(pnl2, 40000.0)  # (5600-5400)*2*100

        self.assertAlmostEqual(db.GetCumulativeRealizedPnl(), 70000.0)

    def test_scenario_options_full_cycle(self):
        """NIFTY straddle: enter → exit with mixed P&L."""
        # Entry: SELL CE at 200, SELL PE at 180, qty=150 (2 lots × 75)
        db.UpdateCostBasis("NIFTY_OPT_CE", 200.0, 150, 1.0)
        db.UpdateCostBasis("NIFTY_OPT_PE", 180.0, 150, 1.0)

        self.assertAlmostEqual(db.GetAvgEntryPrice("NIFTY_OPT_CE"), 200.0)
        self.assertAlmostEqual(db.GetAvgEntryPrice("NIFTY_OPT_PE"), 180.0)

        # Exit: BUY CE at 250 (loss), BUY PE at 50 (profit)
        ce_pnl = db.RealizePnl("NIFTY_OPT_CE", 250.0, 150, 1.0, "options", WasLong=False)
        pe_pnl = db.RealizePnl("NIFTY_OPT_PE", 50.0, 150, 1.0, "options", WasLong=False)

        # CE: (200 - 250) * 150 = -7500
        # PE: (180 - 50) * 150 = 19500
        self.assertAlmostEqual(ce_pnl, -7500.0)
        self.assertAlmostEqual(pe_pnl, 19500.0)
        self.assertAlmostEqual(db.GetCumulativeRealizedPnl(), 12000.0)

    def test_scenario_multiple_instruments_cumulative(self):
        """Multiple instruments across futures and options affect effective capital."""
        # Futures trades
        self._create_position("GOLDM", 2, 2, 72500.0, 100.0)
        db.RealizePnl("GOLDM", 73000.0, 2, 100.0, "futures")  # +100000

        self._create_position("SILVERM", -1, -1, 90000.0, 30.0)
        db.RealizePnl("SILVERM", 91000.0, 1, 30.0, "futures")  # (90000-91000)*1*30 = -30000

        # Options trades
        db.UpdateCostBasis("NIFTY_OPT_CE", 150.0, 75, 1.0)
        db.UpdateCostBasis("NIFTY_OPT_PE", 130.0, 75, 1.0)
        db.RealizePnl("NIFTY_OPT_CE", 0, 75, 1.0, "options", WasLong=False)  # +11250
        db.RealizePnl("NIFTY_OPT_PE", 0, 75, 1.0, "options", WasLong=False)  # +9750

        cumulative = db.GetCumulativeRealizedPnl()
        # 100000 - 30000 + 11250 + 9750 = 91000
        self.assertAlmostEqual(cumulative, 91000.0)

        # Effective capital
        base = 9999999
        effective = base + cumulative
        self.assertAlmostEqual(effective, 10090999.0)

    def test_scenario_smart_chase_ordering(self):
        """Verify P&L tracking BEFORE position update works correctly (smart chase path)."""
        # Simulate smart chase flow: P&L tracked before UpdateSystemPosition
        pv = 100.0

        # Initial state: long 2 at 72500
        self._create_position("GOLDM", 2, 2, 72500.0, pv)

        # Smart chase fill: closing 2 at 73000 (Target=0)
        # Step 1: P&L tracking (BEFORE position update)
        pnl = db.RealizePnl("GOLDM", 73000.0, 2, pv, "futures")
        self.assertAlmostEqual(pnl, 100000.0)

        # Step 2: Position update (AFTER P&L)
        db.UpdateSystemPosition("GOLDM", 0, 0)

        # Verify: position zeroed, P&L recorded, cost basis preserved
        pos = db.GetSystemPosition("GOLDM")
        self.assertEqual(pos["confirmed_qty"], 0)
        self.assertAlmostEqual(pos["avg_entry_price"], 72500.0)  # preserved!
        self.assertAlmostEqual(db.GetCumulativeRealizedPnl(), 100000.0)

    def test_scenario_legacy_ordering(self):
        """Verify P&L tracking AFTER position update with WasLong works (legacy path)."""
        pv = 100.0

        # Initial state: long 2 at 72500
        self._create_position("GOLDM", 2, 2, 72500.0, pv)

        # Legacy flow: position updated FIRST, then P&L
        # Step 1: Position update (confirmed_qty → 0)
        db.UpdateSystemPosition("GOLDM", 0, 0)

        # Step 2: P&L tracking with WasLong override (confirmed_qty is now 0!)
        pnl = db.RealizePnl("GOLDM", 73000.0, 2, pv, "futures", WasLong=True)
        self.assertAlmostEqual(pnl, 100000.0,
                               msg="WasLong=True must override DB confirmed_qty=0")


class TestMigrations(unittest.TestCase):
    """Test that migrations handle pre-existing databases correctly."""

    def test_migration_adds_columns(self):
        """Running InitDB on an old DB adds avg_entry_price and point_value."""
        db._Connection = None
        db.DB_PATH = os.path.join(_TEST_DIR, "migration_test.db")

        # Create an old-style DB without the new columns
        conn = sqlite3.connect(db.DB_PATH)
        conn.execute("""CREATE TABLE IF NOT EXISTS system_positions (
            instrument TEXT PRIMARY KEY,
            target_qty INTEGER NOT NULL DEFAULT 0,
            confirmed_qty INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )""")
        conn.execute("INSERT INTO system_positions (instrument, target_qty, confirmed_qty) VALUES ('GOLDM', 2, 2)")
        conn.commit()
        conn.close()

        # Now run InitDB which should add columns via migration
        db.InitDB()

        pos = db.GetSystemPosition("GOLDM")
        self.assertEqual(pos["confirmed_qty"], 2)
        self.assertEqual(pos["avg_entry_price"], 0)  # default
        self.assertEqual(pos["point_value"], 1)  # default

        # Clean up
        if db._Connection:
            db._Connection.close()
            db._Connection = None
        os.unlink(db.DB_PATH)

    def test_realized_pnl_table_created(self):
        """InitDB creates realized_pnl table."""
        db._Connection = None
        db.DB_PATH = os.path.join(_TEST_DIR, "table_test.db")
        db.InitDB()

        conn = db._GetConn()
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        self.assertIn("realized_pnl", tables)

        if db._Connection:
            db._Connection.close()
            db._Connection = None
        os.unlink(db.DB_PATH)


if __name__ == "__main__":
    unittest.main(verbosity=2)
