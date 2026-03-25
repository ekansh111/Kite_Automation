"""
Tests for smart_chase.py — all broker interactions mocked.

Tests cover:
  1. Volatility assessment (decision matrix: all 9 cells)
  2. Price computation (modes A/B/C, BUY/SELL, tick rounding)
  3. Market open delay logic
  4. Circuit limit detection
  5. Spread gate logic
  6. Chase loop: fill on first check, fill after N iterations, market fallback
  7. Order rejection handling
  8. DB logging (LogSmartChaseOrder column mapping)
  9. End-to-end SmartChaseExecute with mocked broker
"""

import os
import sys
import math
import time
import sqlite3
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

# ── Make project root importable ──────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

# Stub Directories module before any project import
import types
from pathlib import Path as _Path
_dirs = types.ModuleType("Directories")
_dirs.workInputRoot = _Path("/tmp/test_kite")
sys.modules["Directories"] = _dirs

# Stub kiteconnect so imports don't fail
_kc = types.ModuleType("kiteconnect")
_kc.KiteConnect = MagicMock
sys.modules["kiteconnect"] = _kc

# Stub broker handler modules so forecast_orchestrator can import them
for mod_name in ("Kite_Server_Order_Handler", "Server_Order_Handler",
                 "Server_Order_Place", "Fetch_Positions_Data"):
    sys.modules[mod_name] = types.ModuleType(mod_name)

# Now safe to import
from smart_chase import (
    SmartChaseExecute, _AssessVolatility, _ComputeInitialPrice,
    _RoundToTick, _WaitForMarketOpen, _IsAtCircuit,
    _WaitForSpreadToSettle, _FetchQuote,
)
import forecast_db as db


# ── Helpers ───────────────────────────────────────────────────────

def _make_config(**overrides):
    """Build a default execution config, then apply overrides."""
    cfg = {
        "use_smart_chase": True,
        "market_open_delay_seconds": 10,
        "baseline_spread_ticks": 2,
        "max_settle_wait_seconds": 30,
        "buffer_ticks": 2,
        "chase_step_ticks": 1,
        "max_chase_ticks": 8,
        "poll_interval_seconds": 0.01,  # fast for tests
        "max_chase_seconds_entry": 0.5,
        "max_chase_seconds_exit": 0.3,
        "tick_size": 1.0,
    }
    cfg.update(overrides)
    return cfg


def _make_quote(ltp=72450, bid=72449, ask=72451, upper=74000, lower=70000,
                buy_depth=None, sell_depth=None):
    """Build a normalized quote dict."""
    return {
        "ltp": ltp,
        "best_bid": bid,
        "best_ask": ask,
        "upper_circuit_limit": upper,
        "lower_circuit_limit": lower,
        "depth": {
            "buy": buy_depth or [{"price": bid, "quantity": 100}],
            "sell": sell_depth or [{"price": ask, "quantity": 100}],
        },
    }


def _make_order_details(tradetype="buy", exchange="MCX", symbol="GOLDM25APRFUT",
                        qty="2", variety="REGULAR", product="NRML"):
    return {
        "Tradetype": tradetype,
        "Exchange": exchange,
        "Tradingsymbol": symbol,
        "Quantity": qty,
        "Variety": variety,
        "Product": product,
        "Validity": "DAY",
        "Price": "0",
        "User": "YD6016",
        "Symboltoken": "",
    }


# ══════════════════════════════════════════════════════════════════
# 1. Volatility Assessment (Decision Matrix)
# ══════════════════════════════════════════════════════════════════

class TestAssessVolatility(unittest.TestCase):
    """Test all 9 cells of the decision matrix."""

    def _run(self, spread_ratio, range_ratio, expected_mode):
        cfg = _make_config(baseline_spread_ticks=2, tick_size=1.0)
        baseline = 2 * 1.0  # = 2.0
        spread = spread_ratio * baseline
        bid = 72450
        ask = bid + spread
        quote = _make_quote(ltp=72450, bid=bid, ask=ask)

        atr = 500.0
        intra_range = range_ratio * atr
        ohlc = {"high": 72450 + intra_range / 2, "low": 72450 - intra_range / 2}

        mode = _AssessVolatility(quote, ohlc, atr, cfg)
        self.assertEqual(mode, expected_mode,
                         f"spread_ratio={spread_ratio} range_ratio={range_ratio} "
                         f"→ expected {expected_mode}, got {mode}")

    # Row: Range Low (<=0.4)
    def test_low_tight(self):    self._run(1.0, 0.2, "C")
    def test_low_normal(self):   self._run(2.0, 0.2, "C")
    def test_low_wide(self):     self._run(4.0, 0.2, "C")

    # Row: Range Normal (0.4 < r <= 0.8)
    def test_normal_tight(self):  self._run(1.0, 0.6, "C")
    def test_normal_normal(self): self._run(2.0, 0.6, "C")
    def test_normal_wide(self):   self._run(4.0, 0.6, "A")

    # Row: Range High (>0.8)
    def test_high_tight(self):  self._run(1.0, 1.2, "A")
    def test_high_normal(self): self._run(2.0, 1.2, "B")
    def test_high_wide(self):   self._run(4.0, 1.2, "B")

    def test_no_ohlc_defaults_normal(self):
        """When OHLC is None, range_ratio defaults to 0.5 (normal row)."""
        cfg = _make_config()
        quote = _make_quote(bid=72449, ask=72451)
        mode = _AssessVolatility(quote, None, 500, cfg)
        # spread=2, baseline=2 → ratio=1.0 → tight. range=normal. → C
        self.assertEqual(mode, "C")

    def test_zero_atr_defaults_normal(self):
        """When ATR is 0, range_ratio defaults to 0.5 (normal row)."""
        cfg = _make_config()
        quote = _make_quote(bid=72449, ask=72451)
        mode = _AssessVolatility(quote, {"high": 72500, "low": 72400}, 0, cfg)
        self.assertEqual(mode, "C")


# ══════════════════════════════════════════════════════════════════
# 2. Price Computation
# ══════════════════════════════════════════════════════════════════

class TestPriceComputation(unittest.TestCase):

    def test_mode_a_buy(self):
        """Mode A BUY: match at best_ask."""
        q = _make_quote(bid=72449, ask=72451)
        cfg = _make_config(tick_size=1.0)
        p = _ComputeInitialPrice("A", q, cfg, Direction=1, TickSize=1.0)
        self.assertEqual(p, 72451)

    def test_mode_a_sell(self):
        """Mode A SELL: match at best_bid."""
        q = _make_quote(bid=72449, ask=72451)
        cfg = _make_config()
        p = _ComputeInitialPrice("A", q, cfg, Direction=-1, TickSize=1.0)
        self.assertEqual(p, 72449)

    def test_mode_b_buy(self):
        """Mode B BUY: best_ask + buffer_ticks * tick_size."""
        q = _make_quote(bid=72449, ask=72451)
        cfg = _make_config(buffer_ticks=2, tick_size=1.0)
        p = _ComputeInitialPrice("B", q, cfg, Direction=1, TickSize=1.0)
        self.assertEqual(p, 72453)

    def test_mode_b_sell(self):
        """Mode B SELL: best_bid - buffer_ticks * tick_size."""
        q = _make_quote(bid=72449, ask=72451)
        cfg = _make_config(buffer_ticks=2, tick_size=1.0)
        p = _ComputeInitialPrice("B", q, cfg, Direction=-1, TickSize=1.0)
        self.assertEqual(p, 72447)

    def test_mode_c_buy(self):
        """Mode C BUY: passive at best_bid."""
        q = _make_quote(bid=72449, ask=72451)
        cfg = _make_config()
        p = _ComputeInitialPrice("C", q, cfg, Direction=1, TickSize=1.0)
        self.assertEqual(p, 72449)

    def test_mode_c_sell(self):
        """Mode C SELL: passive at best_ask."""
        q = _make_quote(bid=72449, ask=72451)
        cfg = _make_config()
        p = _ComputeInitialPrice("C", q, cfg, Direction=-1, TickSize=1.0)
        self.assertEqual(p, 72451)

    def test_round_to_tick_buy_ceil(self):
        """BUY rounds up to nearest tick."""
        self.assertEqual(_RoundToTick(100.3, 0.05, Direction=1), 100.30)
        self.assertEqual(_RoundToTick(100.31, 0.05, Direction=1), 100.35)
        self.assertEqual(_RoundToTick(100.36, 0.05, Direction=1), 100.40)

    def test_round_to_tick_sell_floor(self):
        """SELL rounds down to nearest tick."""
        self.assertEqual(_RoundToTick(100.3, 0.05, Direction=-1), 100.30)
        self.assertEqual(_RoundToTick(100.34, 0.05, Direction=-1), 100.30)
        self.assertEqual(_RoundToTick(100.39, 0.05, Direction=-1), 100.35)

    def test_naturalgas_tick_size(self):
        """NATURALGAS has tick_size=0.10."""
        self.assertEqual(_RoundToTick(350.75, 0.10, Direction=1), 350.80)
        self.assertEqual(_RoundToTick(350.75, 0.10, Direction=-1), 350.70)

    def test_zincmini_tick_size(self):
        """ZINCMINI has tick_size=0.05."""
        self.assertEqual(_RoundToTick(250.12, 0.05, Direction=1), 250.15)
        self.assertEqual(_RoundToTick(250.12, 0.05, Direction=-1), 250.10)


# ══════════════════════════════════════════════════════════════════
# 3. Market Open Delay
# ══════════════════════════════════════════════════════════════════

class TestMarketOpenDelay(unittest.TestCase):

    @patch("smart_chase.time.sleep")
    @patch("smart_chase.datetime")
    def test_waits_if_within_delay_window(self, mock_dt, mock_sleep):
        """If now is 09:00:03 and delay=10, should sleep ~7s."""
        mock_dt.now.return_value = datetime(2026, 3, 25, 9, 0, 3)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        cfg = _make_config(market_open_delay_seconds=10)
        _WaitForMarketOpen("MCX", cfg)
        mock_sleep.assert_called_once()
        slept = mock_sleep.call_args[0][0]
        self.assertAlmostEqual(slept, 7.0, places=0)

    @patch("smart_chase.time.sleep")
    @patch("smart_chase.datetime")
    def test_no_wait_if_past_delay(self, mock_dt, mock_sleep):
        """If now is 09:00:15 and delay=10, no sleep."""
        mock_dt.now.return_value = datetime(2026, 3, 25, 9, 0, 15)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        cfg = _make_config(market_open_delay_seconds=10)
        _WaitForMarketOpen("MCX", cfg)
        mock_sleep.assert_not_called()

    @patch("smart_chase.time.sleep")
    @patch("smart_chase.datetime")
    def test_no_wait_if_before_open(self, mock_dt, mock_sleep):
        """If now is 08:59:50 (before open), no sleep."""
        mock_dt.now.return_value = datetime(2026, 3, 25, 8, 59, 50)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        cfg = _make_config()
        _WaitForMarketOpen("MCX", cfg)
        mock_sleep.assert_not_called()

    @patch("smart_chase.time.sleep")
    @patch("smart_chase.datetime")
    def test_nfo_opens_at_915(self, mock_dt, mock_sleep):
        """NFO opens at 09:15, not 09:00."""
        mock_dt.now.return_value = datetime(2026, 3, 25, 9, 15, 2)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        cfg = _make_config(market_open_delay_seconds=10)
        _WaitForMarketOpen("NFO", cfg)
        mock_sleep.assert_called_once()
        slept = mock_sleep.call_args[0][0]
        self.assertAlmostEqual(slept, 8.0, places=0)


# ══════════════════════════════════════════════════════════════════
# 4. Circuit Limit Detection
# ══════════════════════════════════════════════════════════════════

class TestCircuitDetection(unittest.TestCase):

    def test_not_at_circuit_normal(self):
        q = _make_quote(ltp=72450, upper=74000, lower=70000)
        self.assertFalse(_IsAtCircuit(q, 1.0))

    def test_at_upper_circuit(self):
        q = _make_quote(ltp=74000, upper=74000, lower=70000)
        self.assertTrue(_IsAtCircuit(q, 1.0))

    def test_at_lower_circuit(self):
        q = _make_quote(ltp=70000, upper=74000, lower=70000)
        self.assertTrue(_IsAtCircuit(q, 1.0))

    def test_within_one_tick_of_upper(self):
        q = _make_quote(ltp=73999, upper=74000, lower=70000)
        self.assertTrue(_IsAtCircuit(q, 1.0))

    def test_within_one_tick_of_lower(self):
        q = _make_quote(ltp=70001, upper=74000, lower=70000)
        self.assertTrue(_IsAtCircuit(q, 1.0))

    def test_no_sellers_detected(self):
        """Empty sell depth = circuit indicator."""
        q = _make_quote(ltp=72450, upper=74000, lower=70000,
                        sell_depth=[{"price": 0, "quantity": 0}])
        self.assertTrue(_IsAtCircuit(q, 1.0))

    def test_no_buyers_detected(self):
        """Empty buy depth = circuit indicator."""
        q = _make_quote(ltp=72450, upper=74000, lower=70000,
                        buy_depth=[{"price": 0, "quantity": 0}])
        self.assertTrue(_IsAtCircuit(q, 1.0))

    def test_none_circuit_limits_no_crash(self):
        """If circuit limits are None (e.g. Angel), don't crash."""
        q = _make_quote(ltp=72450, upper=None, lower=None)
        self.assertFalse(_IsAtCircuit(q, 1.0))


# ══════════════════════════════════════════════════════════════════
# 5. Spread Gate
# ══════════════════════════════════════════════════════════════════

class TestSpreadGate(unittest.TestCase):

    def test_passes_immediately_if_tight(self):
        """Spread=2 vs threshold=10 → passes immediately."""
        cfg = _make_config(baseline_spread_ticks=2, tick_size=1.0,
                           max_settle_wait_seconds=30)
        q = _make_quote(bid=72449, ask=72451)  # spread=2
        result = _WaitForSpreadToSettle(None, {}, q, cfg, 1.0, "ZERODHA", "GOLDM")
        self.assertEqual(result, q)

    @patch("smart_chase._FetchQuote")
    @patch("smart_chase.time.sleep")
    def test_waits_then_settles(self, mock_sleep, mock_fetch):
        """Spread starts wide, settles after 2 polls."""
        cfg = _make_config(baseline_spread_ticks=2, tick_size=1.0,
                           max_settle_wait_seconds=30)
        wide = _make_quote(bid=72445, ask=72460)   # spread=15 > threshold 10
        tight = _make_quote(bid=72449, ask=72451)   # spread=2 < threshold 10
        mock_fetch.side_effect = [wide, tight]

        result = _WaitForSpreadToSettle(None, {}, wide, cfg, 1.0, "ZERODHA", "GOLDM")
        self.assertEqual(result["best_bid"], 72449)

    @patch("smart_chase._FetchQuote")
    @patch("smart_chase.time.sleep")
    @patch("smart_chase.time.time")
    def test_timeout_proceeds_anyway(self, mock_time, mock_sleep, mock_fetch):
        """If spread never settles, proceed after max_settle_wait_seconds."""
        cfg = _make_config(baseline_spread_ticks=2, tick_size=1.0,
                           max_settle_wait_seconds=5)
        wide = _make_quote(bid=72440, ask=72460)  # spread=20 > threshold 10
        # Simulate time passing beyond max_settle_wait
        mock_time.side_effect = [0, 0, 3, 6]  # Start=0, then loop checks: 3<5 (continue), 6>=5 (exit)
        mock_fetch.return_value = wide

        result = _WaitForSpreadToSettle(None, {}, wide, cfg, 1.0, "ZERODHA", "GOLDM")
        # Should return the last quote even though spread is still wide
        self.assertEqual(result["best_bid"], 72440)

    def test_zero_max_wait_proceeds_immediately(self):
        """Edge case: max_settle_wait=0 should not crash."""
        cfg = _make_config(baseline_spread_ticks=2, tick_size=1.0,
                           max_settle_wait_seconds=0)
        wide = _make_quote(bid=72440, ask=72460)
        result = _WaitForSpreadToSettle(None, {}, wide, cfg, 1.0, "ZERODHA", "GOLDM")
        # Should return original quote (FreshQuote is None, falls back to Quote)
        self.assertEqual(result["best_bid"], 72440)


# ══════════════════════════════════════════════════════════════════
# 6. Full SmartChaseExecute — Mocked Broker
# ══════════════════════════════════════════════════════════════════

class TestSmartChaseExecute(unittest.TestCase):
    """End-to-end tests with mocked broker calls."""

    def _mock_session(self, quote_data, ohlc_data, order_id="ORD123",
                      order_statuses=None):
        """Build a mock KiteConnect session."""
        session = MagicMock()

        # quote() returns dict keyed by "MCX:GOLDM25APRFUT"
        def mock_quote(symbols):
            return {symbols[0]: quote_data}
        session.quote = MagicMock(side_effect=mock_quote)

        # ohlc()
        def mock_ohlc(symbols):
            return {symbols[0]: {"ohlc": ohlc_data}}
        session.ohlc = MagicMock(side_effect=mock_ohlc)

        # order_history() — returns list of status dicts
        if order_statuses is None:
            order_statuses = [{"status": "COMPLETE", "filled_quantity": 2,
                               "pending_quantity": 0, "average_price": 72451}]
        session.order_history = MagicMock(return_value=order_statuses)

        # modify_order
        session.modify_order = MagicMock()
        session.VARIETY_REGULAR = "regular"
        session.ORDER_TYPE_LIMIT = "LIMIT"
        session.ORDER_TYPE_MARKET = "MARKET"

        return session

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD123")
    @patch("smart_chase._WaitForMarketOpen")
    def test_immediate_fill(self, mock_open, mock_place, mock_email):
        """Order fills on the first status check after placement."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        ohlc_data = {"high": 72500, "low": 72400}

        session = self._mock_session(quote_data, ohlc_data)
        order_details = _make_order_details()
        cfg = _make_config(poll_interval_seconds=0.01,
                           max_chase_seconds_entry=2)

        success, order_id, info = SmartChaseExecute(
            session, order_details, cfg, IsEntry=True, Broker="ZERODHA", ATR=500
        )

        self.assertTrue(success)
        self.assertEqual(order_id, "ORD123")
        self.assertIn(info["execution_mode"], ("A", "B", "C"))
        self.assertEqual(info["fill_price"], 72451)
        self.assertEqual(info["market_fallback"], 0)

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD456")
    @patch("smart_chase._WaitForMarketOpen")
    def test_chase_then_fill(self, mock_open, mock_place, mock_email):
        """Order is OPEN for 2 iterations, then fills."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        ohlc_data = {"high": 72500, "low": 72400}
        session = self._mock_session(quote_data, ohlc_data)

        # First two checks: OPEN, third: COMPLETE
        status_sequence = [
            [{"status": "OPEN", "filled_quantity": 0,
              "pending_quantity": 2, "average_price": 0}],
            [{"status": "OPEN", "filled_quantity": 0,
              "pending_quantity": 2, "average_price": 0}],
            [{"status": "COMPLETE", "filled_quantity": 2,
              "pending_quantity": 0, "average_price": 72452}],
        ]
        session.order_history = MagicMock(side_effect=status_sequence)

        order_details = _make_order_details()
        cfg = _make_config(poll_interval_seconds=0.01,
                           max_chase_seconds_entry=5)

        success, order_id, info = SmartChaseExecute(
            session, order_details, cfg, IsEntry=True, Broker="ZERODHA", ATR=500
        )

        self.assertTrue(success)
        self.assertEqual(info["chase_iterations"], 3)
        self.assertEqual(info["fill_price"], 72452)

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD789")
    @patch("smart_chase._WaitForMarketOpen")
    def test_market_fallback(self, mock_open, mock_place, mock_email):
        """Chase times out → market fallback → fills."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        ohlc_data = {"high": 72500, "low": 72400}
        session = self._mock_session(quote_data, ohlc_data)

        # Track whether _ConvertToMarket has been called (not just modify_order)
        market_converted = {"flag": False}
        original_modify = session.modify_order

        def tracking_modify(**kwargs):
            if kwargs.get("order_type") == "MARKET":
                market_converted["flag"] = True
            return original_modify(**kwargs)
        session.modify_order = MagicMock(side_effect=tracking_modify)
        session.VARIETY_REGULAR = "regular"
        session.ORDER_TYPE_LIMIT = "LIMIT"
        session.ORDER_TYPE_MARKET = "MARKET"

        # Always OPEN during chase, COMPLETE only after market conversion
        def order_history_side_effect(order_id):
            if market_converted["flag"]:
                return [{"status": "COMPLETE", "filled_quantity": 2,
                         "pending_quantity": 0, "average_price": 72455}]
            return [{"status": "OPEN", "filled_quantity": 0,
                     "pending_quantity": 2, "average_price": 0}]

        session.order_history = MagicMock(side_effect=order_history_side_effect)

        order_details = _make_order_details()
        cfg = _make_config(poll_interval_seconds=0.01,
                           max_chase_seconds_entry=0.05)

        success, order_id, info = SmartChaseExecute(
            session, order_details, cfg, IsEntry=True, Broker="ZERODHA", ATR=500
        )

        self.assertTrue(success)
        self.assertEqual(info["market_fallback"], 1)
        self.assertEqual(info["fill_price"], 72455)

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD_REJ")
    @patch("smart_chase._WaitForMarketOpen")
    def test_order_rejected(self, mock_open, mock_place, mock_email):
        """If broker rejects the order, return failure."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        ohlc_data = {"high": 72500, "low": 72400}
        session = self._mock_session(quote_data, ohlc_data)
        session.order_history = MagicMock(return_value=[
            {"status": "REJECTED", "filled_quantity": 0,
             "pending_quantity": 0, "average_price": 0}
        ])

        order_details = _make_order_details()
        cfg = _make_config(poll_interval_seconds=0.01)

        success, order_id, info = SmartChaseExecute(
            session, order_details, cfg, IsEntry=True, Broker="ZERODHA", ATR=500
        )

        self.assertFalse(success)
        self.assertEqual(order_id, "ORD_REJ")

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value=None)
    @patch("smart_chase._WaitForMarketOpen")
    def test_place_order_fails(self, mock_open, mock_place, mock_email):
        """If limit order placement returns None, return failure."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        ohlc_data = {"high": 72500, "low": 72400}
        session = self._mock_session(quote_data, ohlc_data)

        order_details = _make_order_details()
        cfg = _make_config()

        success, order_id, info = SmartChaseExecute(
            session, order_details, cfg, IsEntry=True, Broker="ZERODHA", ATR=500
        )

        self.assertFalse(success)
        self.assertIsNone(order_id)

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD_SELL")
    @patch("smart_chase._WaitForMarketOpen")
    def test_sell_direction(self, mock_open, mock_place, mock_email):
        """Selling should use correct direction logic."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        ohlc_data = {"high": 72500, "low": 72400}
        session = self._mock_session(quote_data, ohlc_data)
        session.order_history = MagicMock(return_value=[
            {"status": "COMPLETE", "filled_quantity": 2,
             "pending_quantity": 0, "average_price": 72449}
        ])

        order_details = _make_order_details(tradetype="sell")
        cfg = _make_config(poll_interval_seconds=0.01,
                           max_chase_seconds_exit=2)

        success, order_id, info = SmartChaseExecute(
            session, order_details, cfg, IsEntry=False, Broker="ZERODHA", ATR=500
        )

        self.assertTrue(success)
        self.assertEqual(info["fill_price"], 72449)


# ══════════════════════════════════════════════════════════════════
# 7. Database Logging
# ══════════════════════════════════════════════════════════════════

class TestDBLogging(unittest.TestCase):

    def setUp(self):
        """Fresh in-memory SQLite for each test."""
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()

    def test_log_smart_chase_order(self):
        """LogSmartChaseOrder stores all FillInfo columns."""
        fill_info = {
            "execution_mode": "A",
            "initial_ltp": 72450,
            "initial_bid": 72449,
            "initial_ask": 72451,
            "initial_spread": 2.0,
            "limit_price": 72451,
            "fill_price": 72451,
            "slippage": 1.0,
            "chase_iterations": 3,
            "chase_duration_seconds": 12.5,
            "market_fallback": 0,
            "spread_ratio": 1.0,
            "range_ratio": 0.3,
            "settle_wait_seconds": 0.0,
        }
        db.LogSmartChaseOrder("GOLDM", "BUY", 2, "FILLED",
                              BrokerOrderId="ORD123", FillInfo=fill_info)

        rows = db.GetRecentOrders(1)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["instrument"], "GOLDM")
        self.assertEqual(row["execution_mode"], "A")
        self.assertEqual(row["initial_ltp"], 72450)
        self.assertEqual(row["fill_price"], 72451)
        self.assertEqual(row["slippage"], 1.0)
        self.assertEqual(row["chase_iterations"], 3)
        self.assertEqual(row["market_fallback"], 0)
        self.assertEqual(row["spread_ratio"], 1.0)
        self.assertEqual(row["settle_wait_seconds"], 0.0)

    def test_log_basic_order_still_works(self):
        """Legacy LogOrder still works with the extended schema."""
        db.LogOrder("SILVERM", "SELL", 1, "PLACED",
                    BrokerOrderId="ORD456", Reason="target=0")
        rows = db.GetRecentOrders(1)
        row = rows[0]
        self.assertEqual(row["instrument"], "SILVERM")
        self.assertEqual(row["execution_mode"], None)
        self.assertEqual(row["fill_price"], None)

    def test_get_latest_atr(self):
        """GetLatestATR returns the most recent ATR from tradingview_signals."""
        # Insert with explicit different timestamps to guarantee ordering
        conn = db._GetConn()
        conn.execute(
            "INSERT INTO tradingview_signals (instrument, system_name, netposition, atr, received_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("TESTGOLD", "S30A", 1, 1200.5, "2026-03-25 09:00:00")
        )
        conn.execute(
            "INSERT INTO tradingview_signals (instrument, system_name, netposition, atr, received_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("TESTGOLD", "S30E", 1, 1250.0, "2026-03-25 09:01:00")
        )
        conn.commit()
        atr = db.GetLatestATR("TESTGOLD")
        self.assertEqual(atr, 1250.0)

    def test_get_latest_atr_no_data(self):
        """GetLatestATR returns None if no signals for instrument."""
        atr = db.GetLatestATR("NOSUCHINSTRUMENT")
        self.assertIsNone(atr)


# ══════════════════════════════════════════════════════════════════
# 8. Chase Loop Price Widening
# ══════════════════════════════════════════════════════════════════

class TestChaseWidening(unittest.TestCase):
    """Verify that the chase loop progressively widens the limit price."""

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._WaitForMarketOpen")
    def test_buy_chase_widens_upward(self, mock_open, mock_email):
        """BUY chase: offset increases, price goes UP from ask."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        ohlc_data = {"high": 72500, "low": 72400}

        session = MagicMock()

        def mock_quote(symbols):
            return {symbols[0]: quote_data}
        session.quote = MagicMock(side_effect=mock_quote)

        def mock_ohlc(symbols):
            return {symbols[0]: {"ohlc": ohlc_data}}
        session.ohlc = MagicMock(side_effect=mock_ohlc)

        modify_prices = []
        def track_modify(**kwargs):
            modify_prices.append(kwargs.get("price"))
        session.modify_order = MagicMock(side_effect=track_modify)
        session.VARIETY_REGULAR = "regular"
        session.ORDER_TYPE_LIMIT = "LIMIT"
        session.ORDER_TYPE_MARKET = "MARKET"

        # OPEN for 3 iterations, then COMPLETE
        status_seq = [
            [{"status": "OPEN", "filled_quantity": 0, "pending_quantity": 2, "average_price": 0}],
            [{"status": "OPEN", "filled_quantity": 0, "pending_quantity": 2, "average_price": 0}],
            [{"status": "OPEN", "filled_quantity": 0, "pending_quantity": 2, "average_price": 0}],
            [{"status": "COMPLETE", "filled_quantity": 2, "pending_quantity": 0, "average_price": 72454}],
        ]
        session.order_history = MagicMock(side_effect=status_seq)

        order_details = _make_order_details(tradetype="buy")
        cfg = _make_config(poll_interval_seconds=0.01,
                           max_chase_seconds_entry=5,
                           chase_step_ticks=1, tick_size=1.0)

        with patch("smart_chase._PlaceLimitOrder", return_value="ORD_W"):
            success, _, info = SmartChaseExecute(
                session, order_details, cfg, IsEntry=True, Broker="ZERODHA", ATR=500
            )

        self.assertTrue(success)
        # Each modify should have progressively higher prices
        if len(modify_prices) >= 2:
            for i in range(1, len(modify_prices)):
                self.assertGreaterEqual(modify_prices[i], modify_prices[i - 1])


# ══════════════════════════════════════════════════════════════════
# 9. Edge Cases
# ══════════════════════════════════════════════════════════════════

class TestEdgeCases(unittest.TestCase):

    def test_quote_fetch_failure_returns_false(self):
        """If initial quote fetch fails, SmartChaseExecute returns failure."""
        session = MagicMock()
        session.quote = MagicMock(side_effect=Exception("API error"))
        order_details = _make_order_details()
        cfg = _make_config()

        with patch("smart_chase._WaitForMarketOpen"):
            success, _, _ = SmartChaseExecute(
                session, order_details, cfg, True, "ZERODHA", 500
            )
        self.assertFalse(success)

    def test_angel_broker_defaults_to_ltp(self):
        """Angel broker: bid=ask=LTP since no depth data."""
        session = MagicMock()
        session.ltpData = MagicMock(return_value={
            "data": {"ltp": 350.5}
        })
        order_details = _make_order_details(exchange="NCDEX")
        quote = _FetchQuote(session, order_details, "ANGEL")
        self.assertEqual(quote["best_bid"], 350.5)
        self.assertEqual(quote["best_ask"], 350.5)
        self.assertEqual(quote["ltp"], 350.5)
        self.assertIsNone(quote["upper_circuit_limit"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
