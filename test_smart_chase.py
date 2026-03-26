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

import json
import os
import sys
import math
import time
import sqlite3
import unittest
from tempfile import TemporaryDirectory
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
_kite_handler = types.ModuleType("Kite_Server_Order_Handler")
_kite_handler.ControlOrderFlowKite = MagicMock()
_kite_handler.EstablishConnectionKiteAPI = MagicMock()
_kite_handler.ConfigureNetDirectionOfTrade = MagicMock()
_kite_handler.Validate_Quantity = MagicMock()
_kite_handler.PrepareInstrumentContractNameKite = MagicMock()
sys.modules["Kite_Server_Order_Handler"] = _kite_handler

_angel_handler = types.ModuleType("Server_Order_Handler")
_angel_handler.ControlOrderFlowAngel = MagicMock()
_angel_handler.EstablishConnectionAngelAPI = MagicMock()
_angel_handler.ConfigureNetDirectionOfTrade = MagicMock()
_angel_handler.Validate_Quantity = MagicMock()
_angel_handler.PrepareInstrumentContractName = MagicMock()
sys.modules["Server_Order_Handler"] = _angel_handler

for mod_name in ("Server_Order_Place", "Fetch_Positions_Data"):
    sys.modules[mod_name] = types.ModuleType(mod_name)

# Now safe to import
import smart_chase
from smart_chase import (
    SmartChaseExecute, _AssessVolatility, _ComputeInitialPrice,
    _RoundToTick, _WaitForMarketOpen, _IsAtCircuit,
    _WaitForSpreadToSettle, _WaitForCircuitRelease, _FetchQuote,
    _FetchOHLC, _CheckOrderStatus, _ModifyOrderPrice,
    _ConvertToMarket, _PlaceLimitOrder,
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

        mode, spread_level, range_level = _AssessVolatility(quote, ohlc, atr, cfg)
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
        mode, sl, rl = _AssessVolatility(quote, None, 500, cfg)
        # spread=2, baseline=2 → ratio=1.0 → tight. range=normal. → C
        self.assertEqual(mode, "C")

    def test_zero_atr_defaults_normal(self):
        """When ATR is 0, range_ratio defaults to 0.5 (normal row)."""
        cfg = _make_config()
        quote = _make_quote(bid=72449, ask=72451)
        mode, sl, rl = _AssessVolatility(quote, {"high": 72500, "low": 72400}, 0, cfg)
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


# ══════════════════════════════════════════════════════════════════
# 10. Circuit Wait Loop (_WaitForCircuitRelease)
# ══════════════════════════════════════════════════════════════════

class TestCircuitWaitLoop(unittest.TestCase):
    """Tests for the circuit wait + release cycle."""

    @patch("smart_chase._SendCircuitAlert")
    @patch("smart_chase._FetchQuote")
    @patch("smart_chase.time.sleep")
    def test_circuit_releases_after_polls(self, mock_sleep, mock_fetch, mock_alert):
        """At circuit → polls 15s → circuit clears → returns fresh quote."""
        circuit_quote = _make_quote(ltp=74000, upper=74000, lower=70000)
        free_quote = _make_quote(ltp=73500, upper=74000, lower=70000)
        mock_fetch.side_effect = [circuit_quote, free_quote]

        result = _WaitForCircuitRelease(None, {}, circuit_quote, 1.0, "ZERODHA", "GOLDM")
        self.assertEqual(result["ltp"], 73500)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_alert.assert_called_once()

    @patch("smart_chase._SendCircuitAlert")
    @patch("smart_chase._FetchQuote")
    @patch("smart_chase.time.sleep")
    def test_not_at_circuit_returns_immediately(self, mock_sleep, mock_fetch, mock_alert):
        """If not at circuit, returns immediately."""
        normal = _make_quote(ltp=72450, upper=74000, lower=70000)
        result = _WaitForCircuitRelease(None, {}, normal, 1.0, "ZERODHA", "GOLDM")
        self.assertEqual(result["ltp"], 72450)
        mock_sleep.assert_not_called()
        mock_alert.assert_not_called()

    @patch("smart_chase._SendCircuitAlert")
    @patch("smart_chase._FetchQuote")
    @patch("smart_chase.time.sleep")
    def test_fetch_returns_none_continues_polling(self, mock_sleep, mock_fetch, mock_alert):
        """If fetch returns None mid-wait, keep polling."""
        circuit = _make_quote(ltp=70000, upper=74000, lower=70000)
        free = _make_quote(ltp=71000, upper=74000, lower=70000)
        mock_fetch.side_effect = [None, None, free]

        result = _WaitForCircuitRelease(None, {}, circuit, 1.0, "ZERODHA", "GOLDM")
        self.assertEqual(result["ltp"], 71000)
        self.assertEqual(mock_sleep.call_count, 3)


# ══════════════════════════════════════════════════════════════════
# 11. SELL Chase Widening
# ══════════════════════════════════════════════════════════════════

class TestSellChaseWidening(unittest.TestCase):
    """Verify SELL chase widens downward."""

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._WaitForMarketOpen")
    def test_sell_chase_widens_downward(self, mock_open, mock_email):
        """SELL chase: offset increases, price goes DOWN from bid."""
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

        status_seq = [
            [{"status": "OPEN", "filled_quantity": 0, "pending_quantity": 2, "average_price": 0}],
            [{"status": "OPEN", "filled_quantity": 0, "pending_quantity": 2, "average_price": 0}],
            [{"status": "OPEN", "filled_quantity": 0, "pending_quantity": 2, "average_price": 0}],
            [{"status": "COMPLETE", "filled_quantity": 2, "pending_quantity": 0, "average_price": 72446}],
        ]
        session.order_history = MagicMock(side_effect=status_seq)

        order_details = _make_order_details(tradetype="sell")
        cfg = _make_config(poll_interval_seconds=0.01,
                           max_chase_seconds_exit=5,
                           chase_step_ticks=1, tick_size=1.0)

        with patch("smart_chase._PlaceLimitOrder", return_value="ORD_SW"):
            success, _, info = SmartChaseExecute(
                session, order_details, cfg, IsEntry=False, Broker="ZERODHA", ATR=500
            )

        self.assertTrue(success)
        # SELL modify prices should be decreasing (or equal)
        if len(modify_prices) >= 2:
            for i in range(1, len(modify_prices)):
                self.assertLessEqual(modify_prices[i], modify_prices[i - 1])


# ══════════════════════════════════════════════════════════════════
# 12. Max Chase Ticks Cap
# ══════════════════════════════════════════════════════════════════

class TestMaxChaseCap(unittest.TestCase):

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._WaitForMarketOpen")
    def test_chase_offset_never_exceeds_max_ticks(self, mock_open, mock_email):
        """Offset should cap at max_chase_ticks, not keep increasing."""
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

        # Open for 10 iterations, then complete
        statuses = ([
            [{"status": "OPEN", "filled_quantity": 0, "pending_quantity": 2, "average_price": 0}]
        ] * 10) + [
            [{"status": "COMPLETE", "filled_quantity": 2, "pending_quantity": 0, "average_price": 72458}]
        ]
        session.order_history = MagicMock(side_effect=statuses)

        cfg = _make_config(poll_interval_seconds=0.001,
                           max_chase_seconds_entry=5,
                           chase_step_ticks=1, max_chase_ticks=3, tick_size=1.0)

        with patch("smart_chase._PlaceLimitOrder", return_value="ORD_CAP"):
            success, _, info = SmartChaseExecute(
                session, _make_order_details(), cfg, IsEntry=True, Broker="ZERODHA", ATR=500
            )

        self.assertTrue(success)
        # Max price should be ask(72451) + max_chase_ticks(3) * tick(1) = 72454
        # Prices should plateau, not keep climbing
        if modify_prices:
            self.assertLessEqual(max(modify_prices), 72451 + 3 + 1)  # +1 for rounding tolerance


# ══════════════════════════════════════════════════════════════════
# 13. Exit vs Entry Timing
# ══════════════════════════════════════════════════════════════════

class TestEntryExitTiming(unittest.TestCase):

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD_T")
    @patch("smart_chase._WaitForMarketOpen")
    def test_exit_uses_shorter_timeout(self, mock_open, mock_place, mock_email):
        """IsEntry=False should use max_chase_seconds_exit, not entry."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }

        session = MagicMock()
        def mock_quote(symbols):
            return {symbols[0]: quote_data}
        session.quote = MagicMock(side_effect=mock_quote)
        def mock_ohlc(symbols):
            return {symbols[0]: {"ohlc": {"high": 72500, "low": 72400}}}
        session.ohlc = MagicMock(side_effect=mock_ohlc)

        # Track that we fall through to market quickly
        market_converted = {"flag": False}
        def tracking_modify(**kwargs):
            if kwargs.get("order_type") == "MARKET":
                market_converted["flag"] = True
        session.modify_order = MagicMock(side_effect=tracking_modify)
        session.VARIETY_REGULAR = "regular"
        session.ORDER_TYPE_LIMIT = "LIMIT"
        session.ORDER_TYPE_MARKET = "MARKET"

        def order_history_effect(order_id):
            if market_converted["flag"]:
                return [{"status": "COMPLETE", "filled_quantity": 2,
                         "pending_quantity": 0, "average_price": 72448}]
            return [{"status": "OPEN", "filled_quantity": 0,
                     "pending_quantity": 2, "average_price": 0}]
        session.order_history = MagicMock(side_effect=order_history_effect)

        # Entry: 5s, Exit: 0.05s — exit should fall through much faster
        cfg = _make_config(poll_interval_seconds=0.01,
                           max_chase_seconds_entry=5,
                           max_chase_seconds_exit=0.05)

        start = time.time()
        success, _, info = SmartChaseExecute(
            session, _make_order_details(tradetype="sell"), cfg,
            IsEntry=False, Broker="ZERODHA", ATR=500
        )
        elapsed = time.time() - start

        self.assertTrue(success)
        self.assertEqual(info["market_fallback"], 1)
        # Should finish much faster than 5s (entry timeout)
        # Note: includes 3s post-market-conversion sleep, so allow up to 4s
        self.assertLess(elapsed, 4.0)


# ══════════════════════════════════════════════════════════════════
# 14. Broker-Specific Operations
# ══════════════════════════════════════════════════════════════════

class TestBrokerOperations(unittest.TestCase):

    def test_fetch_quote_zerodha_empty_depth(self):
        """If Zerodha depth is empty, bid/ask should fallback to 0."""
        session = MagicMock()
        session.quote.return_value = {
            "MCX:GOLDM25APRFUT": {
                "last_price": 72450,
                "depth": {"buy": [], "sell": []},
                "upper_circuit_limit": 74000,
                "lower_circuit_limit": 70000,
            }
        }
        od = _make_order_details()
        q = _FetchQuote(session, od, "ZERODHA")
        self.assertEqual(q["ltp"], 72450)
        self.assertEqual(q["best_bid"], 0)
        self.assertEqual(q["best_ask"], 0)

    def test_fetch_ohlc_zerodha(self):
        """_FetchOHLC returns high/low for Zerodha."""
        session = MagicMock()
        session.ohlc.return_value = {
            "MCX:GOLDM25APRFUT": {"ohlc": {"high": 72600, "low": 72200}}
        }
        od = _make_order_details()
        ohlc = _FetchOHLC(session, od, "ZERODHA")
        self.assertEqual(ohlc["high"], 72600)
        self.assertEqual(ohlc["low"], 72200)

    def test_fetch_ohlc_angel_returns_none(self):
        """Angel has no OHLC endpoint → returns None."""
        session = MagicMock()
        od = _make_order_details(exchange="NCDEX")
        ohlc = _FetchOHLC(session, od, "ANGEL")
        self.assertIsNone(ohlc)

    def test_fetch_ohlc_exception_returns_none(self):
        """If OHLC fetch throws, returns None gracefully."""
        session = MagicMock()
        session.ohlc.side_effect = Exception("network error")
        od = _make_order_details()
        ohlc = _FetchOHLC(session, od, "ZERODHA")
        self.assertIsNone(ohlc)

    def test_check_order_status_zerodha(self):
        """_CheckOrderStatus normalizes Kite status."""
        session = MagicMock()
        session.order_history.return_value = [
            {"status": "COMPLETE", "filled_quantity": 2,
             "pending_quantity": 0, "average_price": 72451}
        ]
        status, filled, pending, avg = _CheckOrderStatus(session, "ORD1", "ZERODHA")
        self.assertEqual(status, "COMPLETE")
        self.assertEqual(filled, 2)
        self.assertEqual(avg, 72451)

    def test_check_order_status_zerodha_completed_variant(self):
        """Kite sometimes returns 'COMPLETED' — should normalize to 'COMPLETE'."""
        session = MagicMock()
        session.order_history.return_value = [
            {"status": "COMPLETED", "filled_quantity": 5,
             "pending_quantity": 0, "average_price": 300.5}
        ]
        status, filled, _, avg = _CheckOrderStatus(session, "ORD2", "ZERODHA")
        self.assertEqual(status, "COMPLETE")
        self.assertEqual(filled, 5)

    def test_check_order_status_angel(self):
        """_CheckOrderStatus works with Angel orderBook format."""
        session = MagicMock()
        session.orderBook.return_value = {
            "data": [
                {"orderid": "123456", "status": "complete",
                 "filledshares": "3", "unfilledshares": "0",
                 "averageprice": "350.5"},
                {"orderid": "999999", "status": "open",
                 "filledshares": "0", "unfilledshares": "5",
                 "averageprice": "0"},
            ]
        }
        status, filled, pending, avg = _CheckOrderStatus(session, "123456", "ANGEL")
        self.assertEqual(status, "COMPLETE")
        self.assertEqual(filled, 3)
        self.assertAlmostEqual(avg, 350.5)

    def test_check_order_status_angel_rejected(self):
        """Angel rejected status normalizes."""
        session = MagicMock()
        session.orderBook.return_value = {
            "data": [
                {"orderid": "111", "status": "rejected",
                 "filledshares": "0", "unfilledshares": "0",
                 "averageprice": "0"},
            ]
        }
        status, _, _, _ = _CheckOrderStatus(session, "111", "ANGEL")
        self.assertEqual(status, "REJECTED")

    def test_check_order_status_exception_returns_unknown(self):
        """If status check throws, return UNKNOWN."""
        session = MagicMock()
        session.order_history.side_effect = Exception("timeout")
        status, filled, pending, avg = _CheckOrderStatus(session, "X", "ZERODHA")
        self.assertEqual(status, "UNKNOWN")
        self.assertEqual(filled, 0)

    def test_modify_order_price_zerodha(self):
        """_ModifyOrderPrice calls session.modify_order with correct params."""
        session = MagicMock()
        session.VARIETY_REGULAR = "regular"
        session.ORDER_TYPE_LIMIT = "LIMIT"
        _ModifyOrderPrice(session, _make_order_details(), "ORD1", 72455, "ZERODHA")
        session.modify_order.assert_called_once_with(
            variety="regular", order_id="ORD1", price=72455, order_type="LIMIT"
        )

    def test_convert_to_market_zerodha(self):
        """_ConvertToMarket sends MARKET order type."""
        session = MagicMock()
        session.VARIETY_REGULAR = "regular"
        session.ORDER_TYPE_MARKET = "MARKET"
        _ConvertToMarket(session, _make_order_details(), "ORD2", "ZERODHA")
        session.modify_order.assert_called_once_with(
            variety="regular", order_id="ORD2", order_type="MARKET"
        )

    def test_convert_to_market_angel(self):
        """_ConvertToMarket for Angel sends correct modifyOrder params."""
        session = MagicMock()
        od = _make_order_details(variety="NORMAL", product="CARRYFORWARD", exchange="NCDEX")
        _ConvertToMarket(session, od, "ANG_ORD", "ANGEL")
        session.modifyOrder.assert_called_once()
        call_params = session.modifyOrder.call_args[0][0]
        self.assertEqual(call_params["ordertype"], "MARKET")
        self.assertEqual(call_params["price"], "0")
        self.assertEqual(call_params["orderid"], "ANG_ORD")


# ══════════════════════════════════════════════════════════════════
# 15. Spread Gate with Circuit During Wait
# ══════════════════════════════════════════════════════════════════

class TestSpreadGateCircuitReenter(unittest.TestCase):
    """Test that spread gate re-enters circuit wait if circuit hits during settling."""

    @patch("smart_chase._WaitForCircuitRelease")
    @patch("smart_chase._FetchQuote")
    @patch("smart_chase.time.sleep")
    @patch("smart_chase.time.time")
    def test_circuit_during_spread_gate(self, mock_time, mock_sleep, mock_fetch, mock_circuit):
        """If circuit hits during spread gate, re-enters circuit wait."""
        cfg = _make_config(baseline_spread_ticks=2, tick_size=1.0,
                           max_settle_wait_seconds=30)
        wide = _make_quote(bid=72440, ask=72460)  # spread=20 > threshold 10

        # First poll: still wide AND at circuit
        circuit_quote = _make_quote(ltp=74000, bid=72440, ask=72460, upper=74000, lower=70000)
        # After circuit release: spread settles
        settled = _make_quote(bid=72449, ask=72451)

        mock_time.side_effect = [0, 0, 5]  # Start, then loop checks
        mock_fetch.return_value = circuit_quote
        mock_circuit.return_value = settled  # circuit release returns settled quote

        result = _WaitForSpreadToSettle(None, {}, wide, cfg, 1.0, "ZERODHA", "GOLDM")

        # Should have called _WaitForCircuitRelease
        mock_circuit.assert_called_once()
        # Result should be the settled quote
        self.assertEqual(result["best_bid"], 72449)


# ══════════════════════════════════════════════════════════════════
# 16. Market Open Delay — Additional Cases
# ══════════════════════════════════════════════════════════════════

class TestMarketOpenDelayExtra(unittest.TestCase):

    @patch("smart_chase.time.sleep")
    @patch("smart_chase.datetime")
    def test_unknown_exchange_no_wait(self, mock_dt, mock_sleep):
        """Unknown exchange skips market open delay."""
        mock_dt.now.return_value = datetime(2026, 3, 25, 9, 0, 5)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        _WaitForMarketOpen("UNKNOWN_EX", _make_config())
        mock_sleep.assert_not_called()

    @patch("smart_chase.time.sleep")
    @patch("smart_chase.datetime")
    def test_ncdex_opens_at_1000(self, mock_dt, mock_sleep):
        """NCDEX opens at 10:00."""
        mock_dt.now.return_value = datetime(2026, 3, 25, 10, 0, 5)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        _WaitForMarketOpen("NCDEX", _make_config(market_open_delay_seconds=10))
        mock_sleep.assert_called_once()
        slept = mock_sleep.call_args[0][0]
        self.assertAlmostEqual(slept, 5.0, places=0)


# ══════════════════════════════════════════════════════════════════
# 17. Price Computation — Boundary & Edge Cases
# ══════════════════════════════════════════════════════════════════

class TestPriceBoundary(unittest.TestCase):

    def test_round_to_tick_exact_value(self):
        """Already on tick → no change for either direction."""
        self.assertEqual(_RoundToTick(100.00, 0.05, 1), 100.00)
        self.assertEqual(_RoundToTick(100.00, 0.05, -1), 100.00)

    def test_round_to_tick_zero_price(self):
        """Price 0 stays 0."""
        self.assertEqual(_RoundToTick(0, 1.0, 1), 0)
        self.assertEqual(_RoundToTick(0, 1.0, -1), 0)

    def test_round_to_tick_zero_tick_size(self):
        """TickSize 0 or negative returns price unchanged."""
        self.assertEqual(_RoundToTick(100.3, 0, 1), 100.3)
        self.assertEqual(_RoundToTick(100.3, -1, 1), 100.3)

    def test_round_to_tick_goldm_integer_tick(self):
        """GOLDM tick=1.0: already on grid."""
        self.assertEqual(_RoundToTick(72449, 1.0, 1), 72449)
        self.assertEqual(_RoundToTick(72449, 1.0, -1), 72449)

    def test_mode_b_with_small_tick_size(self):
        """Mode B with ZINCMINI tick=0.05 and buffer=10."""
        q = _make_quote(bid=250.10, ask=250.15)
        cfg = _make_config(buffer_ticks=10, tick_size=0.05)
        # BUY: ask(250.15) + 10*0.05=0.50 = 250.65
        p = _ComputeInitialPrice("B", q, cfg, Direction=1, TickSize=0.05)
        self.assertAlmostEqual(p, 250.65, places=2)
        # SELL: bid(250.10) - 0.50 = 249.60
        p = _ComputeInitialPrice("B", q, cfg, Direction=-1, TickSize=0.05)
        self.assertAlmostEqual(p, 249.60, places=2)


# ══════════════════════════════════════════════════════════════════
# 18. Volatility Assessment — Boundary Thresholds
# ══════════════════════════════════════════════════════════════════

class TestVolatilityBoundary(unittest.TestCase):
    """Test exact boundary values of the decision matrix."""

    def _assess(self, spread_ratio, range_ratio):
        cfg = _make_config(baseline_spread_ticks=2, tick_size=1.0)
        baseline = 2.0
        spread = spread_ratio * baseline
        bid = 72450
        ask = bid + spread
        q = _make_quote(ltp=72450, bid=bid, ask=ask)
        atr = 500.0
        intra_range = range_ratio * atr
        ohlc = {"high": 72450 + intra_range / 2, "low": 72450 - intra_range / 2}
        mode, sl, rl = _AssessVolatility(q, ohlc, atr, cfg)
        return mode

    def test_spread_at_exact_1_5(self):
        """Spread ratio exactly 1.5 → tight."""
        mode = self._assess(1.5, 0.2)
        self.assertEqual(mode, "C")

    def test_spread_just_above_1_5(self):
        """Spread ratio 1.6 → normal."""
        mode = self._assess(1.6, 0.2)
        self.assertEqual(mode, "C")  # low range, normal spread → C

    def test_spread_at_exact_3_0(self):
        """Spread ratio exactly 3.0 → normal."""
        mode = self._assess(3.0, 0.2)
        self.assertEqual(mode, "C")

    def test_spread_just_above_3_0(self):
        """Spread ratio 3.1 → wide."""
        mode = self._assess(3.1, 0.2)
        self.assertEqual(mode, "C")  # low range, wide spread → C

    def test_range_at_exact_0_4(self):
        """Range ratio exactly 0.4 → low."""
        mode = self._assess(1.0, 0.4)
        self.assertEqual(mode, "C")

    def test_range_just_above_0_4(self):
        """Range ratio 0.41 → normal."""
        mode = self._assess(1.0, 0.41)
        self.assertEqual(mode, "C")  # normal range, tight spread → C

    def test_range_at_exact_0_8(self):
        """Range ratio exactly 0.8 → normal."""
        mode = self._assess(1.0, 0.8)
        self.assertEqual(mode, "C")

    def test_range_just_above_0_8(self):
        """Range ratio 0.81 → high."""
        mode = self._assess(1.0, 0.81)
        self.assertEqual(mode, "A")  # high range, tight spread → A

    def test_spread_zero_baseline_defaults(self):
        """If baseline_spread_ticks=0, spread_ratio defaults to 1.0."""
        cfg = _make_config(baseline_spread_ticks=0, tick_size=1.0)
        q = _make_quote(bid=72449, ask=72451)
        mode, sl, rl = _AssessVolatility(q, {"high": 72500, "low": 72400}, 500, cfg)
        # spread_ratio=1.0 → tight; range=200/500=0.4 → low → C
        self.assertEqual(mode, "C")


# ══════════════════════════════════════════════════════════════════
# 19. Email Notification Verification
# ══════════════════════════════════════════════════════════════════

class TestEmailNotification(unittest.TestCase):
    """Verify email functions are called with correct parameters."""

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD_EMAIL")
    @patch("smart_chase._WaitForMarketOpen")
    def test_email_sent_on_fill(self, mock_open, mock_place, mock_email):
        """_SendOrderEmail is called with FILLED on success."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        session = MagicMock()
        def mock_quote(symbols):
            return {symbols[0]: quote_data}
        session.quote = MagicMock(side_effect=mock_quote)
        def mock_ohlc(symbols):
            return {symbols[0]: {"ohlc": {"high": 72500, "low": 72400}}}
        session.ohlc = MagicMock(side_effect=mock_ohlc)
        session.order_history = MagicMock(return_value=[
            {"status": "COMPLETE", "filled_quantity": 2,
             "pending_quantity": 0, "average_price": 72451}
        ])

        SmartChaseExecute(session, _make_order_details(),
                         _make_config(poll_interval_seconds=0.01, max_chase_seconds_entry=2),
                         IsEntry=True, Broker="ZERODHA", ATR=500)

        mock_email.assert_called_once()
        args = mock_email.call_args[0]
        self.assertEqual(args[2], "FILLED")  # outcome

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD_RJ2")
    @patch("smart_chase._WaitForMarketOpen")
    def test_email_sent_on_rejection(self, mock_open, mock_place, mock_email):
        """_SendOrderEmail is called with REJECTED on rejection."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        session = MagicMock()
        def mock_quote(symbols):
            return {symbols[0]: quote_data}
        session.quote = MagicMock(side_effect=mock_quote)
        def mock_ohlc(symbols):
            return {symbols[0]: {"ohlc": {"high": 72500, "low": 72400}}}
        session.ohlc = MagicMock(side_effect=mock_ohlc)
        session.order_history = MagicMock(return_value=[
            {"status": "REJECTED", "filled_quantity": 0,
             "pending_quantity": 0, "average_price": 0}
        ])

        SmartChaseExecute(session, _make_order_details(),
                         _make_config(poll_interval_seconds=0.01),
                         IsEntry=True, Broker="ZERODHA", ATR=500)

        mock_email.assert_called_once()
        args = mock_email.call_args[0]
        self.assertEqual(args[2], "REJECTED")

    @patch("smart_chase.smtplib.SMTP_SSL")
    def test_send_order_email_renders_html_without_format_error(self, mock_smtp):
        """_SendOrderEmail should build and send the HTML email for filled orders."""
        fill_info = {
            "fill_price": 224465.0,
            "slippage": 102.0,
            "execution_mode": "A",
            "spread_level": "normal",
            "range_level": "high",
            "ohlc": {"high": 224500.0, "low": 224300.0},
            "atr": 17906.78,
            "baseline_spread": 64.0,
            "range_ratio": 1.6,
            "initial_spread": 105.0,
            "spread_ratio": 1.64,
            "initial_ltp": 224363.0,
            "initial_bid": 224368.0,
            "initial_ask": 224473.0,
            "depth": {
                "buy": [{"price": 224368.0, "quantity": 2, "orders": 1}],
                "sell": [{"price": 224473.0, "quantity": 3, "orders": 1}],
            },
            "chase_iterations": 1,
            "chase_duration_seconds": 1.5,
            "market_fallback": False,
            "settle_wait_seconds": 0,
        }

        with TemporaryDirectory() as tmpdir:
            cfg_path = _Path(tmpdir) / "email_config.json"
            cfg_path.write_text(json.dumps({
                "sender": "sender@example.com",
                "recipient": "recipient@example.com",
                "app_password": "test-password",
            }), encoding="utf-8")

            with patch("smart_chase.EMAIL_CONFIG_PATH", cfg_path):
                smart_chase._SendOrderEmail(
                    _make_order_details(tradetype="sell", symbol="SILVERMIC26APRFUT", qty="1"),
                    fill_info,
                    "FILLED",
                )

        smtp_server = mock_smtp.return_value.__enter__.return_value
        smtp_server.login.assert_called_once_with("sender@example.com", "test-password")
        smtp_server.send_message.assert_called_once()

        sent_msg = smtp_server.send_message.call_args[0][0]
        payload = sent_msg.get_payload(decode=True).decode(sent_msg.get_content_charset())
        self.assertIn("+102.00", payload)
        self.assertIn("vs LTP 224363.0", payload)
        self.assertIn("background:#d9f1ff", payload)
        self.assertIn("color:#0f2f57", payload)
        self.assertIn('font-size:24px;font-weight:700;line-height:1.2;">₹224465.0</span>', payload)


# ══════════════════════════════════════════════════════════════════
# 20. Market Conversion Failure
# ══════════════════════════════════════════════════════════════════

class TestMarketConversionFailure(unittest.TestCase):

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD_MCF")
    @patch("smart_chase._WaitForMarketOpen")
    def test_market_conversion_exception(self, mock_open, mock_place, mock_email):
        """If market conversion throws, return failure."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        session = MagicMock()
        def mock_quote(symbols):
            return {symbols[0]: quote_data}
        session.quote = MagicMock(side_effect=mock_quote)
        def mock_ohlc(symbols):
            return {symbols[0]: {"ohlc": {"high": 72500, "low": 72400}}}
        session.ohlc = MagicMock(side_effect=mock_ohlc)

        # Always OPEN
        session.order_history = MagicMock(return_value=[
            {"status": "OPEN", "filled_quantity": 0,
             "pending_quantity": 2, "average_price": 0}
        ])
        # Market conversion fails
        session.modify_order = MagicMock(side_effect=Exception("Broker timeout"))
        session.VARIETY_REGULAR = "regular"
        session.ORDER_TYPE_LIMIT = "LIMIT"
        session.ORDER_TYPE_MARKET = "MARKET"

        cfg = _make_config(poll_interval_seconds=0.01,
                           max_chase_seconds_entry=0.03)

        success, oid, info = SmartChaseExecute(
            session, _make_order_details(), cfg, IsEntry=True, Broker="ZERODHA", ATR=500
        )

        self.assertFalse(success)
        self.assertEqual(oid, "ORD_MCF")
        self.assertEqual(info["market_fallback"], 1)


# ══════════════════════════════════════════════════════════════════
# 21. Database — Extended Tests
# ══════════════════════════════════════════════════════════════════

class TestDBExtended(unittest.TestCase):

    def setUp(self):
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()

    def test_upsert_forecast_overwrites(self):
        """UpsertForecast replaces existing entry."""
        db.UpsertForecast("GOLDM", "S30A", 10.0, 1200)
        db.UpsertForecast("GOLDM", "S30A", -10.0, 1300)
        rows = db.GetForecastsForInstrument("GOLDM")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["forecast"], -10.0)
        self.assertEqual(rows[0]["atr"], 1300)

    def test_multiple_forecasts_per_instrument(self):
        """Multiple subsystems for one instrument."""
        db.UpsertForecast("GOLDM", "S30A", 10.0, 1200)
        db.UpsertForecast("GOLDM", "S30E", -10.0, 1200)
        db.UpsertForecast("GOLDM", "S30D", 0.0, 1200)
        rows = db.GetForecastsForInstrument("GOLDM")
        self.assertEqual(len(rows), 3)

    def test_system_position_crud(self):
        """Position create, read, update cycle."""
        # Default when no row
        pos = db.GetSystemPosition("GOLDM")
        self.assertEqual(pos["target_qty"], 0)
        self.assertEqual(pos["confirmed_qty"], 0)

        # Create
        db.UpdateSystemPosition("GOLDM", 5, 3)
        pos = db.GetSystemPosition("GOLDM")
        self.assertEqual(pos["target_qty"], 5)
        self.assertEqual(pos["confirmed_qty"], 3)

        # Update confirmed only
        db.UpdateConfirmedQty("GOLDM", 5)
        pos = db.GetSystemPosition("GOLDM")
        self.assertEqual(pos["confirmed_qty"], 5)

    def test_override_crud(self):
        """Override set, get, clear cycle."""
        self.assertIsNone(db.GetOverride("GOLDM"))

        db.SetOverride("GOLDM", "FORCE_FLAT")
        ov = db.GetOverride("GOLDM")
        self.assertEqual(ov["override_type"], "FORCE_FLAT")

        db.ClearOverride("GOLDM")
        self.assertIsNone(db.GetOverride("GOLDM"))

    def test_set_position_override(self):
        """SET_POSITION override stores value."""
        db.SetOverride("GOLDM", "SET_POSITION", "10")
        ov = db.GetOverride("GOLDM")
        self.assertEqual(ov["override_type"], "SET_POSITION")
        self.assertEqual(ov["value"], "10")

    def test_reconciliation_logging(self):
        """LogReconciliation stores and retrieves."""
        db.LogReconciliation("GOLDM", 5, 5, True)
        db.LogReconciliation("GOLDM", 5, 3, False)
        rows = db.GetRecentReconciliations(2)
        self.assertEqual(len(rows), 2)
        # Both match and mismatch should be present
        match_values = {r["match"] for r in rows}
        self.assertIn(1, match_values)
        self.assertIn(0, match_values)

    def test_log_smart_chase_with_none_fill_info(self):
        """LogSmartChaseOrder with FillInfo=None doesn't crash."""
        db.LogSmartChaseOrder("GOLDM", "BUY", 2, "FAILED",
                              BrokerOrderId="X", FillInfo=None)
        rows = db.GetRecentOrders(1)
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["execution_mode"])

    def test_get_all_positions(self):
        """GetAllPositions returns all instruments."""
        db.UpdateSystemPosition("GOLDM", 5, 5)
        db.UpdateSystemPosition("SILVERMIC", 3, 3)
        positions = db.GetAllPositions()
        self.assertEqual(len(positions), 2)

    def test_get_all_forecasts(self):
        """GetAllForecasts returns across instruments."""
        db.UpsertForecast("GOLDM", "S30A", 10.0, 1200)
        db.UpsertForecast("SILVERMIC", "S30C", -10.0, 50)
        all_fc = db.GetAllForecasts()
        self.assertEqual(len(all_fc), 2)

    def test_recent_tv_signals_filter(self):
        """GetRecentTVSignals filters by instrument."""
        db.LogTVSignal("GOLDM", "S30A", 1, 1200)
        db.LogTVSignal("SILVERMIC", "S30C", -1, 50)
        gold = db.GetRecentTVSignals("GOLDM")
        self.assertEqual(len(gold), 1)
        all_signals = db.GetRecentTVSignals()
        self.assertEqual(len(all_signals), 2)


# ══════════════════════════════════════════════════════════════════
# 22. Orchestrator — BuildOrderDict
# ══════════════════════════════════════════════════════════════════

class TestBuildOrderDict(unittest.TestCase):
    """Test _BuildOrderDict order dict construction."""

    def setUp(self):
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()

        # Need a real orchestrator with stubbed config
        import json, tempfile
        self.cfg = {
            "account": {"total_capital": 10000000, "annual_vol_target_pct": 0.50, "dry_run": True},
            "instruments": {
                "GOLDM": {
                    "enabled": True, "exchange": "MCX", "broker": "ZERODHA",
                    "user": "YD6016", "point_value": 10, "daily_vol_target": 40219,
                    "FDM": 1.1, "forecast_cap": 20, "position_inertia_pct": 0.10,
                    "subsystems": {"S30A": 0.34, "S30E": 0.33, "S30D": 0.33},
                    "system_name_map": {
                        "S30A_GoldM": "S30A", "AUTO_S30A_GoldM": "S30A",
                    },
                    "order_routing": {
                        "InstrumentType": "FUT", "Variety": "REGULAR",
                        "Product": "NRML", "Validity": "DAY",
                        "DaysPostWhichSelectNextContract": "9",
                        "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                        "ConvertToMarketOrder": "True",
                        "ContractNameProvided": "False", "QuantityMultiplier": 1,
                    },
                },
                "NATURALGAS": {
                    "enabled": True, "exchange": "MCX", "broker": "ZERODHA",
                    "user": "IK6635", "point_value": 1250, "daily_vol_target": 73125,
                    "FDM": 1.1, "forecast_cap": 20, "position_inertia_pct": 0.10,
                    "subsystems": {"S60A": 0.50, "S60B": 0.50},
                    "order_routing": {
                        "InstrumentType": "FUT", "Variety": "REGULAR",
                        "Product": "NRML", "Validity": "DAY",
                        "DaysPostWhichSelectNextContract": "9",
                        "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                        "ConvertToMarketOrder": "True",
                        "ContractNameProvided": "False", "QuantityMultiplier": 1,
                    },
                },
            }
        }
        self.tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(self.cfg, self.tmp)
        self.tmp.close()

        from forecast_orchestrator import ForecastOrchestrator
        self.orch = ForecastOrchestrator(ConfigPath=self.tmp.name)

    def tearDown(self):
        import os
        os.unlink(self.tmp.name)

    def test_buy_entry_from_zero(self):
        """Delta=+3, Current=0 → BUY, Netposition=3 (entering)."""
        db.UpdateSystemPosition("GOLDM", 0, 0)
        od = self.orch._BuildOrderDict("GOLDM", Delta=3, Target=3, Current=0)
        self.assertEqual(od["Tradetype"], "buy")
        self.assertEqual(od["Quantity"], "3")
        self.assertEqual(od["Netposition"], "3")
        self.assertEqual(od["Exchange"], "MCX")

    def test_sell_exit_to_zero(self):
        """Delta=-3, Current=3, Target=0 → SELL, Netposition=0 (full exit)."""
        db.UpdateSystemPosition("GOLDM", 3, 3)
        od = self.orch._BuildOrderDict("GOLDM", Delta=-3, Target=0, Current=3)
        self.assertEqual(od["Tradetype"], "sell")
        self.assertEqual(od["Quantity"], "3")
        self.assertEqual(od["Netposition"], "0")

    def test_reduce_position(self):
        """Delta=-1, Current=3, Target=2 → SELL, Netposition=0 (reducing)."""
        od = self.orch._BuildOrderDict("GOLDM", Delta=-1, Target=2, Current=3)
        self.assertEqual(od["Tradetype"], "sell")
        self.assertEqual(od["Netposition"], "0")

    def test_add_to_position(self):
        """Delta=+2, Current=3, Target=5 → BUY, Netposition=2 (adding)."""
        od = self.orch._BuildOrderDict("GOLDM", Delta=2, Target=5, Current=3)
        self.assertEqual(od["Tradetype"], "buy")
        self.assertEqual(od["Netposition"], "2")

    def test_flip_position(self):
        """Delta=-5, Current=3, Target=-2 → SELL, Netposition=0 (reducing past 0)."""
        od = self.orch._BuildOrderDict("GOLDM", Delta=-5, Target=-2, Current=3)
        self.assertEqual(od["Tradetype"], "sell")
        self.assertEqual(od["Quantity"], "5")
        # NewTarget=-2, abs(-2) < abs(3) → reducing
        self.assertEqual(od["Netposition"], "0")


# ══════════════════════════════════════════════════════════════════
# 23. Orchestrator — System Name Resolution
# ══════════════════════════════════════════════════════════════════

class TestSystemNameResolution(unittest.TestCase):

    def setUp(self):
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()

        import json, tempfile
        self.cfg = {
            "account": {"total_capital": 10000000, "annual_vol_target_pct": 0.50, "dry_run": True},
            "instruments": {
                "GOLDM": {
                    "enabled": True, "exchange": "MCX", "broker": "ZERODHA",
                    "user": "YD6016", "point_value": 10, "daily_vol_target": 40219,
                    "FDM": 1.1, "forecast_cap": 20, "position_inertia_pct": 0.10,
                    "subsystems": {"S30A": 0.34, "S30E": 0.33, "S30D": 0.33},
                    "system_name_map": {
                        "S30A_GoldM": "S30A", "S30E_GoldM": "S30E", "S30D_GoldM": "S30D",
                        "AUTO_S30A_GoldM": "S30A", "AUTO_S30E_GoldM": "S30E", "AUTO_S30D_GoldM": "S30D",
                    },
                    "order_routing": {
                        "InstrumentType": "FUT", "Variety": "REGULAR", "Product": "NRML",
                        "Validity": "DAY", "DaysPostWhichSelectNextContract": "9",
                        "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                        "ConvertToMarketOrder": "True", "ContractNameProvided": "False",
                        "QuantityMultiplier": 1,
                    },
                },
            }
        }
        self.tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(self.cfg, self.tmp)
        self.tmp.close()

        from forecast_orchestrator import ForecastOrchestrator
        self.orch = ForecastOrchestrator(ConfigPath=self.tmp.name)

    def tearDown(self):
        import os
        os.unlink(self.tmp.name)

    def test_exact_config_key(self):
        self.assertEqual(self.orch._ResolveSystemName("S30A", "GOLDM"), "S30A")

    def test_webhook_name_with_suffix(self):
        self.assertEqual(self.orch._ResolveSystemName("S30A_GoldM", "GOLDM"), "S30A")

    def test_auto_prefix(self):
        self.assertEqual(self.orch._ResolveSystemName("AUTO_S30A_GoldM", "GOLDM"), "S30A")

    def test_unknown_name_falls_back(self):
        """Unknown system name returns raw name."""
        result = self.orch._ResolveSystemName("UNKNOWN_SYSTEM", "GOLDM")
        self.assertEqual(result, "UNKNOWN_SYSTEM")


# ══════════════════════════════════════════════════════════════════
# 24. Orchestrator — ComputeAndExecute (Dry Run)
# ══════════════════════════════════════════════════════════════════

class TestComputeAndExecute(unittest.TestCase):
    """Test the core compute algorithm in dry_run mode."""

    def setUp(self):
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()

        import json, tempfile
        self.cfg = {
            "account": {"total_capital": 10000000, "annual_vol_target_pct": 0.50, "dry_run": True},
            "instruments": {
                "GOLDM": {
                    "enabled": True, "exchange": "MCX", "broker": "ZERODHA",
                    "user": "YD6016", "point_value": 10, "daily_vol_target": 40219,
                    "FDM": 1.1, "forecast_cap": 20, "position_inertia_pct": 0.10,
                    "subsystems": {"S30A": 0.34, "S30E": 0.33, "S30D": 0.33},
                    "system_name_map": {},
                    "order_routing": {
                        "InstrumentType": "FUT", "Variety": "REGULAR", "Product": "NRML",
                        "Validity": "DAY", "DaysPostWhichSelectNextContract": "9",
                        "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                        "ConvertToMarketOrder": "True", "ContractNameProvided": "False",
                        "QuantityMultiplier": 1,
                    },
                },
            }
        }
        self.tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(self.cfg, self.tmp)
        self.tmp.close()

        from forecast_orchestrator import ForecastOrchestrator
        self.orch = ForecastOrchestrator(ConfigPath=self.tmp.name)

    def tearDown(self):
        import os
        os.unlink(self.tmp.name)

    def test_all_systems_long_computes_positive_target(self):
        """All 3 subsystems at +10 → combined = (0.34+0.33+0.33)*10*1.1 = 11.0."""
        db.UpsertForecast("GOLDM", "S30A", 10.0, 1200)
        db.UpsertForecast("GOLDM", "S30E", 10.0, 1200)
        db.UpsertForecast("GOLDM", "S30D", 10.0, 1200)
        db.UpdateSystemPosition("GOLDM", 0, 0)

        self.orch._ComputeAndExecute("GOLDM")

        pos = db.GetSystemPosition("GOLDM")
        # combined=11, vol_scalar=40219/(1200*10)=3.35, pos=(11*3.35)/10=3.685 → round to 4
        self.assertGreater(pos["target_qty"], 0)

    def test_all_systems_flat_no_order(self):
        """All subsystems at 0 → combined = 0 → target = 0 → no trade."""
        db.UpsertForecast("GOLDM", "S30A", 0.0, 1200)
        db.UpsertForecast("GOLDM", "S30E", 0.0, 1200)
        db.UpsertForecast("GOLDM", "S30D", 0.0, 1200)
        db.UpdateSystemPosition("GOLDM", 0, 0)

        self.orch._ComputeAndExecute("GOLDM")

        orders = db.GetRecentOrders(10)
        self.assertEqual(len(orders), 0)

    def test_force_flat_override(self):
        """FORCE_FLAT override sets target to 0 regardless of forecasts."""
        db.UpsertForecast("GOLDM", "S30A", 10.0, 1200)
        db.UpsertForecast("GOLDM", "S30E", 10.0, 1200)
        db.UpsertForecast("GOLDM", "S30D", 10.0, 1200)
        db.UpdateSystemPosition("GOLDM", 5, 5)
        db.SetOverride("GOLDM", "FORCE_FLAT")

        self.orch._ComputeAndExecute("GOLDM")

        orders = db.GetRecentOrders(10)
        # Should have placed a SELL order (DRY_RUN) to flatten
        self.assertTrue(any(o["action"] == "SELL" for o in orders))

    def test_position_inertia_blocks_small_delta(self):
        """If delta < 10% of target, skip execution."""
        db.UpsertForecast("GOLDM", "S30A", 10.0, 1200)
        db.UpsertForecast("GOLDM", "S30E", 10.0, 1200)
        db.UpsertForecast("GOLDM", "S30D", 10.0, 1200)

        # Set confirmed_qty very close to what target will be
        # target ≈ 4, so confirmed=4 → delta=0 → no trade
        db.UpdateSystemPosition("GOLDM", 4, 4)

        self.orch._ComputeAndExecute("GOLDM")

        orders = db.GetRecentOrders(10)
        # No orders because delta is 0 or within inertia
        self.assertEqual(len(orders), 0)

    def test_disabled_instrument_skips(self):
        """Disabled instrument doesn't compute."""
        self.orch.Instruments["GOLDM"]["enabled"] = False
        db.UpsertForecast("GOLDM", "S30A", 10.0, 1200)
        db.UpdateSystemPosition("GOLDM", 0, 0)

        self.orch._ComputeAndExecute("GOLDM")

        orders = db.GetRecentOrders(10)
        self.assertEqual(len(orders), 0)

    def test_zero_atr_skips(self):
        """ATR <= 0 causes skip."""
        db.UpsertForecast("GOLDM", "S30A", 10.0, 0)
        db.UpdateSystemPosition("GOLDM", 0, 0)

        self.orch._ComputeAndExecute("GOLDM")

        orders = db.GetRecentOrders(10)
        self.assertEqual(len(orders), 0)


# ══════════════════════════════════════════════════════════════════
# 25. Orchestrator — HandleWebhook
# ══════════════════════════════════════════════════════════════════

class TestHandleWebhook(unittest.TestCase):

    def setUp(self):
        db._Connection = sqlite3.connect(":memory:", check_same_thread=False)
        db._Connection.row_factory = sqlite3.Row
        db.InitDB()

        import json, tempfile
        self.cfg = {
            "account": {"total_capital": 10000000, "annual_vol_target_pct": 0.50, "dry_run": True},
            "instruments": {
                "GOLDM": {
                    "enabled": True, "exchange": "MCX", "broker": "ZERODHA",
                    "user": "YD6016", "point_value": 10, "daily_vol_target": 40219,
                    "FDM": 1.1, "forecast_cap": 20, "position_inertia_pct": 0.10,
                    "subsystems": {"S30A": 0.34, "S30E": 0.33, "S30D": 0.33},
                    "system_name_map": {
                        "S30A_GoldM": "S30A", "AUTO_S30A_GoldM": "S30A",
                    },
                    "order_routing": {
                        "InstrumentType": "FUT", "Variety": "REGULAR", "Product": "NRML",
                        "Validity": "DAY", "DaysPostWhichSelectNextContract": "9",
                        "EntrySleepDuration": "60", "ExitSleepDuration": "45",
                        "ConvertToMarketOrder": "True", "ContractNameProvided": "False",
                        "QuantityMultiplier": 1,
                    },
                },
            }
        }
        self.tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(self.cfg, self.tmp)
        self.tmp.close()

        from forecast_orchestrator import ForecastOrchestrator
        self.orch = ForecastOrchestrator(ConfigPath=self.tmp.name)

    def tearDown(self):
        import os
        os.unlink(self.tmp.name)

    def test_valid_webhook_returns_ok(self):
        """Valid webhook → OK response, signal logged, forecast upserted."""
        result = self.orch.HandleWebhook({
            "SystemName": "S30A_GoldM",
            "Instrument": "GOLDM",
            "Netposition": 1,
            "ATR": 1200.5
        })
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["system"], "S30A")

        signals = db.GetRecentTVSignals("GOLDM")
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["atr"], 1200.5)

        forecasts = db.GetForecastsForInstrument("GOLDM")
        self.assertEqual(len(forecasts), 1)
        self.assertEqual(forecasts[0]["forecast"], 10.0)

    def test_negative_netposition(self):
        """Netposition -1 → forecast -10."""
        self.orch.HandleWebhook({
            "SystemName": "S30A_GoldM",
            "Instrument": "GOLDM",
            "Netposition": -1,
            "ATR": 1200
        })
        forecasts = db.GetForecastsForInstrument("GOLDM")
        self.assertEqual(forecasts[0]["forecast"], -10.0)

    def test_zero_netposition(self):
        """Netposition 0 → forecast 0."""
        self.orch.HandleWebhook({
            "SystemName": "S30A_GoldM",
            "Instrument": "GOLDM",
            "Netposition": 0,
            "ATR": 1200
        })
        forecasts = db.GetForecastsForInstrument("GOLDM")
        self.assertEqual(forecasts[0]["forecast"], 0.0)

    def test_unknown_instrument_returns_error(self):
        """Unknown instrument → error response."""
        result = self.orch.HandleWebhook({
            "SystemName": "S30A",
            "Instrument": "UNKNOWN_INST",
            "Netposition": 1,
            "ATR": 100
        })
        self.assertEqual(result["status"], "error")

    def test_auto_prefix_resolved(self):
        """AUTO_ prefix webhooks resolve correctly."""
        result = self.orch.HandleWebhook({
            "SystemName": "AUTO_S30A_GoldM",
            "Instrument": "GOLDM",
            "Netposition": 1,
            "ATR": 1200
        })
        self.assertEqual(result["system"], "S30A")


# ══════════════════════════════════════════════════════════════════
# 26. FillInfo Completeness
# ══════════════════════════════════════════════════════════════════

class TestFillInfoCompleteness(unittest.TestCase):
    """Verify FillInfo dict has all expected keys after execution."""

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD_FI")
    @patch("smart_chase._WaitForMarketOpen")
    def test_fill_info_all_keys_present(self, mock_open, mock_place, mock_email):
        """All 14 FillInfo keys should be populated after a fill."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        session = MagicMock()
        def mock_quote(symbols):
            return {symbols[0]: quote_data}
        session.quote = MagicMock(side_effect=mock_quote)
        def mock_ohlc(symbols):
            return {symbols[0]: {"ohlc": {"high": 72500, "low": 72400}}}
        session.ohlc = MagicMock(side_effect=mock_ohlc)
        session.order_history = MagicMock(return_value=[
            {"status": "COMPLETE", "filled_quantity": 2,
             "pending_quantity": 0, "average_price": 72451}
        ])

        _, _, info = SmartChaseExecute(
            session, _make_order_details(),
            _make_config(poll_interval_seconds=0.01, max_chase_seconds_entry=2),
            IsEntry=True, Broker="ZERODHA", ATR=500
        )

        expected_keys = {
            "execution_mode", "initial_ltp", "initial_bid", "initial_ask",
            "initial_spread", "limit_price", "fill_price", "slippage",
            "chase_iterations", "chase_duration_seconds", "market_fallback",
            "spread_ratio", "range_ratio", "settle_wait_seconds",
        }
        self.assertTrue(expected_keys.issubset(info.keys()))
        # All should be non-None after a fill
        self.assertIsNotNone(info["execution_mode"])
        self.assertIsNotNone(info["initial_ltp"])
        self.assertIsNotNone(info["fill_price"])
        self.assertIsNotNone(info["slippage"])
        self.assertIsInstance(info["chase_iterations"], int)
        self.assertGreater(info["chase_iterations"], 0)
        self.assertEqual(info["market_fallback"], 0)


# ══════════════════════════════════════════════════════════════════
# 27. Slippage Calculation
# ══════════════════════════════════════════════════════════════════

class TestSlippageCalculation(unittest.TestCase):

    @patch("smart_chase._SendOrderEmail")
    @patch("smart_chase._PlaceLimitOrder", return_value="ORD_SL")
    @patch("smart_chase._WaitForMarketOpen")
    def test_slippage_is_fill_minus_ltp(self, mock_open, mock_place, mock_email):
        """Slippage = fill_price - initial_ltp."""
        quote_data = {
            "last_price": 72450,
            "depth": {
                "buy": [{"price": 72449, "quantity": 100}],
                "sell": [{"price": 72451, "quantity": 100}],
            },
            "upper_circuit_limit": 74000,
            "lower_circuit_limit": 70000,
        }
        session = MagicMock()
        def mock_quote(symbols):
            return {symbols[0]: quote_data}
        session.quote = MagicMock(side_effect=mock_quote)
        def mock_ohlc(symbols):
            return {symbols[0]: {"ohlc": {"high": 72500, "low": 72400}}}
        session.ohlc = MagicMock(side_effect=mock_ohlc)
        session.order_history = MagicMock(return_value=[
            {"status": "COMPLETE", "filled_quantity": 2,
             "pending_quantity": 0, "average_price": 72455}
        ])

        _, _, info = SmartChaseExecute(
            session, _make_order_details(),
            _make_config(poll_interval_seconds=0.01, max_chase_seconds_entry=2),
            IsEntry=True, Broker="ZERODHA", ATR=500
        )

        # LTP=72450, fill=72455 → slippage=+5
        self.assertEqual(info["slippage"], 5.0)
        self.assertEqual(info["initial_ltp"], 72450)
        self.assertEqual(info["fill_price"], 72455)


if __name__ == "__main__":
    unittest.main(verbosity=2)
