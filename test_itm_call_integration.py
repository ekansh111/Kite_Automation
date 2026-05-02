"""Integration tests for ITM call dynamic K + pooled allocation pipeline.

Uses MOCKED Kite client to verify the end-to-end flow works correctly:
  1. resolveKLongSingle with quote-quality gates
  2. PrepareSizingForIndex full pipeline
  3. AllocateLotsBalanced with realistic inputs
  4. RunCoordinatedRollover dispatch
  5. Email rendering with all sections

Does NOT touch real broker — pure unit isolation via Mock.
"""
import json
import math
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, str(Path(__file__).parent))


# ─── Helpers ───────────────────────────────────────────────────────

def _build_kite_mock(spot_nifty=23997.55, spot_bank=54863.35, vix=18.46,
                     prem_nifty=1405.15, prem_bank=3517.24,
                     nifty_token=256265, bank_token=260105):
    """Construct a Mock Kite client returning realistic market data."""
    m = MagicMock()

    def _ltp(keys):
        if isinstance(keys, str):
            keys = [keys]
        out = {}
        for k in keys:
            if "NIFTY 50" in k:
                out[k] = {"instrument_token": nifty_token, "last_price": spot_nifty}
            elif "NIFTY BANK" in k:
                out[k] = {"instrument_token": bank_token, "last_price": spot_bank}
            elif "INDIA VIX" in k:
                out[k] = {"instrument_token": 264969, "last_price": vix}
            else:
                out[k] = {"instrument_token": 0, "last_price": 100.0}
        return out
    m.ltp.side_effect = _ltp

    def _quote(keys):
        out = {}
        for k in keys:
            now_str = datetime.now().isoformat()
            if "NIFTY26JUN" in k:
                prem = prem_nifty if "NIFTY26JUN" in k else prem_bank
                out[k] = {
                    "last_price": prem_nifty,
                    "last_trade_time": now_str,
                    "depth": {
                        "buy": [{"price": prem_nifty - 1, "quantity": 100}],
                        "sell": [{"price": prem_nifty + 1, "quantity": 100}],
                    },
                }
            elif "BANKNIFTY26JUN" in k:
                out[k] = {
                    "last_price": prem_bank,
                    "last_trade_time": now_str,
                    "depth": {
                        "buy": [{"price": prem_bank - 2, "quantity": 30}],
                        "sell": [{"price": prem_bank + 2, "quantity": 30}],
                    },
                }
            else:
                out[k] = {"last_price": 100, "last_trade_time": now_str,
                          "depth": {"buy": [{"price": 99, "quantity": 1}],
                                    "sell": [{"price": 101, "quantity": 1}]}}
        return out
    m.quote.side_effect = _quote

    def _ohlc(keys):
        return {k: {"ohlc": {"open": 100, "high": 101, "low": 99, "close": 100}} for k in keys}
    m.ohlc.side_effect = _ohlc

    def _historical(token, from_date, to_date, interval):
        # Realistic-ish daily bars: 120 days of 23k-24k for NIFTY-like
        bars = []
        base = 23500
        # Construct 120 trading days with slight realized vol
        for i in range(120):
            d = from_date + timedelta(days=i)
            base *= 1 + (math.sin(i / 7.0) * 0.015)  # ~1.5% swings
            bars.append({
                "date": d, "open": base * 0.998, "high": base * 1.012,
                "low": base * 0.988, "close": base, "volume": 1_000_000,
            })
        return bars
    m.historical_data.side_effect = _historical

    def _instruments(exchange):
        # Mock NFO / BFO instruments cache
        # Generate strikes for NIFTY and BANKNIFTY June expiry
        target_expiry = date(2026, 6, 30)
        out = []
        for strike in range(22000, 25000, 50):
            out.append({
                "instrument_token": 100000 + strike, "exchange": "NFO",
                "tradingsymbol": f"NIFTY26JUN{strike}CE",
                "name": "NIFTY", "strike": float(strike), "expiry": target_expiry,
                "instrument_type": "CE", "segment": "NFO-OPT",
                "lot_size": 65,
            })
        for strike in range(50000, 60000, 100):
            out.append({
                "instrument_token": 200000 + strike, "exchange": "NFO",
                "tradingsymbol": f"BANKNIFTY26JUN{strike}CE",
                "name": "BANKNIFTY", "strike": float(strike), "expiry": target_expiry,
                "instrument_type": "CE", "segment": "NFO-OPT",
                "lot_size": 30,
            })
        # Also add the May expiry for "current month" detection
        may_expiry = date(2026, 5, 26)
        for strike in range(22000, 25000, 50):
            out.append({
                "instrument_token": 300000 + strike, "exchange": "NFO",
                "tradingsymbol": f"NIFTY26MAY{strike}CE",
                "name": "NIFTY", "strike": float(strike), "expiry": may_expiry,
                "instrument_type": "CE", "segment": "NFO-OPT",
                "lot_size": 65,
            })
        return out
    m.instruments.side_effect = _instruments

    return m


# ─── Tests ─────────────────────────────────────────────────────────

class TestResolveKLongSingle(unittest.TestCase):
    """End-to-end test of resolveKLongSingle with mocked Kite."""

    def test_full_pipeline_returns_dynamic_K(self):
        from PlaceOptionsSystemsV2 import resolveKLongSingle
        kite = _build_kite_mock()

        # Patch the historical-cache lookup to return our mocked bars consistently
        with patch("PlaceOptionsSystemsV2._HISTORICAL_CACHE", {}):
            with patch("PlaceOptionsSystemsV2.lookupStrikeFromInstruments",
                        return_value=23050):
                k_value, meta = resolveKLongSingle(
                    kite=kite, optSymbol="NIFTY26JUN23050CE",
                    exchange="NFO", underlying="NIFTY",
                    sizingDte=40, premium=1405.15, lotSize=65,
                    expiryDate=date(2026, 6, 30), optionType="CE",
                    staticKFallback=0.18,
                )

        self.assertIsNotNone(k_value)
        self.assertEqual(meta["source"], "dynamic")
        self.assertIn(meta["kBindingScenario"], ["kBase", "kStressMove", "kVegaCrush"])
        self.assertIsNotNone(meta["kBase"])
        self.assertIsNotNone(meta["kStressMove"])
        self.assertIsNotNone(meta["kVegaCrush"])
        # Sanity bounds: K should be between K_FLOOR (0.20) and K_CEILING (5.0)
        self.assertGreaterEqual(k_value, 0.20)
        self.assertLessEqual(k_value, 5.0)

    def test_fallback_to_static_when_quote_fails(self):
        from PlaceOptionsSystemsV2 import resolveKLongSingle
        kite = MagicMock()
        kite.ltp.return_value = {"NSE:NIFTY 50":
                                   {"instrument_token": 256265, "last_price": 24000}}
        kite.quote.side_effect = Exception("network error")
        with patch("PlaceOptionsSystemsV2.lookupStrikeFromInstruments",
                    return_value=23050):
            k, meta = resolveKLongSingle(
                kite=kite, optSymbol="NIFTY26JUN23050CE",
                exchange="NFO", underlying="NIFTY",
                sizingDte=40, premium=1405.15, lotSize=65,
                expiryDate=date(2026, 6, 30), optionType="CE",
                staticKFallback=0.18,
            )
        self.assertEqual(k, 0.18)
        self.assertEqual(meta["source"], "static_fallback")
        self.assertIn("quote", meta["fallbackReason"].lower())

    def test_fallback_when_iv_out_of_bounds(self):
        from PlaceOptionsSystemsV2 import resolveKLongSingle
        kite = _build_kite_mock(prem_nifty=0.01)  # absurdly low premium → IV near floor
        with patch("PlaceOptionsSystemsV2.lookupStrikeFromInstruments",
                    return_value=23050):
            k, meta = resolveKLongSingle(
                kite=kite, optSymbol="NIFTY26JUN23050CE",
                exchange="NFO", underlying="NIFTY",
                sizingDte=40, premium=0.01, lotSize=65,
                expiryDate=date(2026, 6, 30), optionType="CE",
                staticKFallback=0.18,
            )
        # Should fall back due to dust premium or IV bounds
        self.assertEqual(k, 0.18)
        self.assertEqual(meta["source"], "static_fallback")

    def test_strict_no_fallback_returns_None_on_failure(self):
        from PlaceOptionsSystemsV2 import resolveKLongSingle
        kite = MagicMock()
        kite.ltp.side_effect = Exception("network down")
        k, meta = resolveKLongSingle(
            kite=kite, optSymbol="NIFTY26JUN23050CE",
            exchange="NFO", underlying="NIFTY",
            sizingDte=40, premium=1405.15, lotSize=65,
            expiryDate=date(2026, 6, 30), optionType="CE",
            staticKFallback=None,
        )
        self.assertIsNone(k)
        self.assertEqual(meta["source"], "failed")


class TestGetRegimeAddon(unittest.TestCase):
    def test_returns_zero_addon_when_no_data(self):
        from PlaceOptionsSystemsV2 import getRegimeAddon
        kite = MagicMock()
        kite.ltp.side_effect = Exception("no token")
        addon, ratio, recent, baseline = getRegimeAddon(kite, "NIFTY")
        self.assertEqual(addon, 0.0)
        self.assertIsNone(ratio)

    def test_returns_addon_with_mocked_history(self):
        from PlaceOptionsSystemsV2 import getRegimeAddon
        kite = _build_kite_mock()
        with patch("PlaceOptionsSystemsV2._HISTORICAL_CACHE", {}):
            addon, ratio, recent, baseline = getRegimeAddon(kite, "NIFTY")
        # The synthetic data is roughly stable so ratio should be near 1.0
        self.assertIsNotNone(ratio)
        self.assertIsNotNone(recent)
        self.assertIsNotNone(baseline)
        # addon should be in the table range
        self.assertIn(addon * 100, [-2, -1, 0, 2, 4, 6, 8])


class TestRunCoordinatedRollover(unittest.TestCase):
    """Test the full coordinated entry orchestration with mocked dependencies."""

    def test_dry_run_executes_without_orders(self):
        from itm_call_rollover import RunCoordinatedRollover, DEFAULT_STATE

        kite = _build_kite_mock()

        # Mock state — both indices in NONE state (cold start = first_run)
        State = json.loads(json.dumps(DEFAULT_STATE))

        # Patch heavy external dependencies
        with patch("PlaceOptionsSystemsV2._HISTORICAL_CACHE", {}), \
             patch("PlaceOptionsSystemsV2.lookupStrikeFromInstruments",
                    side_effect=lambda sym, *_args, **_kw: int(sym[10:-2]) if "NIFTY" in sym else 50000), \
             patch("itm_call_rollover.SelectBestITMStrike",
                    return_value=(23050, "NIFTY26JUN23050CE", 65, 1405.15,
                                   {"best": {"bs_theo": 1402, "value_pct": 0.2, "spread_pct": 1.5},
                                    "all_candidates": [], "rejected": []})), \
             patch("itm_call_rollover.GetInstrumentsCached",
                    return_value=kite.instruments("NFO")), \
             patch("itm_call_rollover.GetMonthlyExpiries",
                    return_value=[date(2026, 5, 26), date(2026, 6, 30)]):

            results, alloc = RunCoordinatedRollover(
                Kite=kite, State=State, Indices=["NIFTY"],
                DryRun=True, FirstRun=True,
            )

        self.assertIn("NIFTY", results)
        # In dry-run mode the result should still be generated
        # Either success or controlled failure (the SelectBestITMStrike mock guarantees data)
        self.assertIn("k_value", results["NIFTY"])
        self.assertIsNotNone(results["NIFTY"].get("k_metadata"))


class TestEmailRendering(unittest.TestCase):
    """Test all 4 email types render with realistic data without throwing."""

    def _make_dynamic_result(self, name="NIFTY"):
        return {
            "success": True, "index": name,
            "spot": 23997.55, "strike": 23050,
            "symbol": f"{name}26JUN23050CE",
            "lot_size": 65, "premium": 1405.15, "dte": 40,
            "k_value": 0.300, "daily_vol_budget": 45703,
            "effective_capital": 9999999,
            "current_expiry": "2026-05-26", "next_expiry": "2026-06-30",
            "use_dynamic_k": True, "max_premium_outlay": 300000,
            "k_metadata": {
                "source": "dynamic",
                "optGreeks": {"delta": 0.792, "gamma": 0.000180,
                              "vega": 2791, "theta": -7.19},
                "ivShockBase": 0.04, "ivShockVixAddon": 0.04,
                "ivShockRegimeAddon": 0.0, "ivShockTotal": 0.08,
                "vixLevel": 18.46, "regimeRatio": 1.08,
                "optIV": 0.164, "expectedMove": 248,
                "kBindingScenario": "kVegaCrush",
                "kBase": 0.140, "kStressMove": 0.206, "kVegaCrush": 0.300,
                "pnlBreakdown": {"basePnl": -197, "stressMovePnl": -289,
                                  "vegaCrushPnl": -421},
            },
            "size_result": {
                "finalLots": 2, "allowedLots": 2, "lotsVol": 2, "lotsCap": 3,
                "bindingConstraint": "vol-target",
                "dailyVolPerLot": 27379, "costPerLot": 91335,
                "premium": 1405.15, "kValue": 0.300, "dailyVolBudget": 45703,
                "maxPremiumOutlay": 300000, "skipped": False, "skipReason": None,
            },
            "allocation_meta": {
                "pool": {
                    "allocations": {"NIFTY": 2, "BANKNIFTY": 1},
                    "pooled_budget": 91406, "vol_used_total": 89858,
                    "leftover_pool": 1548, "utilization_pct": 98.3,
                    "iterations": [
                        {"round": 1, "tried": "NIFTY", "preference": "primary",
                         "need": 27379, "had": 28927, "pct_of_need": 1.06,
                         "added": True, "new_lots": {"NIFTY": 2, "BANKNIFTY": 1}},
                        {"round": 2, "tried": "BANKNIFTY", "preference": "primary",
                         "need": 35100, "had": 1548, "pct_of_need": 0.04,
                         "added": False, "reason": "below threshold"},
                    ],
                },
            },
            "leg2": {"contract": f"{name}26JUN23050CE", "quantity": 130,
                      "lots": 2, "premium": 1405.15, "fill_price": 1408,
                      "slippage": 3, "expiry": "2026-06-30"},
            "selection": {"best": {"bs_theo": 1402, "value_pct": 0.2,
                                    "spread_pct": 1.5},
                           "all_candidates": [], "rejected": []},
        }

    def test_dynamic_K_email_renders_all_sections(self):
        from itm_call_rollover import BuildRolloverEmailHtml
        result = self._make_dynamic_result()
        html = BuildRolloverEmailHtml("NIFTY", result)
        self.assertIn("Dynamic K Computation", html)
        self.assertIn("IV Shock Construction", html)
        self.assertIn("Pooled Allocation", html)
        self.assertIn("kVegaCrush", html)
        self.assertIn("kBase", html)
        self.assertIn("kStressMove", html)

    def test_static_K_email_still_works(self):
        from itm_call_rollover import BuildRolloverEmailHtml
        result = self._make_dynamic_result()
        result["k_metadata"] = None  # static path
        html = BuildRolloverEmailHtml("NIFTY", result)
        self.assertIn("STATIC", html)
        self.assertIn("K_TABLE_SINGLE", html)
        self.assertNotIn("Dynamic K Computation", html)

    def test_combined_portfolio_email(self):
        from itm_call_rollover import BuildCombinedPortfolioEmail
        results = {
            "NIFTY": self._make_dynamic_result("NIFTY"),
            "BANKNIFTY": self._make_dynamic_result("BANKNIFTY"),
        }
        alloc = results["NIFTY"]["allocation_meta"]["pool"]
        cfg = {"account": {"base_capital": 9999999}}
        html = BuildCombinedPortfolioEmail(results, alloc, cfg)
        self.assertIn("Combined Portfolio", html)
        self.assertIn("NIFTY", html)
        self.assertIn("BANKNIFTY", html)
        self.assertIn("Per-Index Breakdown", html)
        self.assertIn("Combined outlay", html)

    def test_daily_monitor_email(self):
        from itm_call_daily_monitor import BuildDailyMonitorEmail
        html = BuildDailyMonitorEmail(
            "NIFTY",
            Drift=[
                ("Spot", "23,997.55", "25,200.00", 5.01, True),
                ("VIX", "18.46", "22.80", 23.5, False),
            ],
            Position={"symbol": "NIFTY26JUN23050CE", "lots": 2, "quantity": 130,
                       "entry_premium": 1405, "current_premium": 2200,
                       "mtm_pct": 56.5, "current_outlay": 286000,
                       "current_outlay_pct": 2.86, "dte": 22},
            Alerts=["SPOT_DRIFT: +5.01%"],
            Recommendation="Hold to expiry recommended.",
        )
        self.assertIn("Daily Monitor", html)
        self.assertIn("DRIFT ALERT", html)

    def test_auto_trim_email(self):
        from itm_call_daily_monitor import BuildAutoTrimEmail
        html = BuildAutoTrimEmail("NIFTY", 415000, 285000, 3, 2, 116675, 9999999)
        self.assertIn("AUTO-TRIM", html)


class TestEdgeCases(unittest.TestCase):
    def test_allocate_with_empty_inputs(self):
        from itm_call_rollover import AllocateLotsBalanced
        r = AllocateLotsBalanced({})
        self.assertEqual(r["allocations"], {})
        self.assertEqual(r["pooled_budget"], 0)

    def test_allocate_with_single_index(self):
        from itm_call_rollover import AllocateLotsBalanced
        inputs = {
            "NIFTY": {"dvpl": 20000, "cost_per_lot": 90000, "max_outlay": 300000,
                       "daily_budget_per_idx": 45703, "floor_lots": 2},
        }
        r = AllocateLotsBalanced(inputs)
        # Pool = 45703, used = 40000, left = 5703
        # 5703/20000 = 28% — below 80% threshold → no extra
        self.assertEqual(r["allocations"]["NIFTY"], 2)

    def test_compute_position_invalid_inputs(self):
        from itm_call_rollover import ComputePositionSizeITM
        for bad in [(0, 65, 0.3, 45703), (1405, 0, 0.3, 45703),
                    (1405, 65, 0, 45703), (-100, 65, 0.3, 45703)]:
            r = ComputePositionSizeITM(*bad)
            self.assertTrue(r["skipped"])
            self.assertEqual(r["finalLots"], 0)


if __name__ == "__main__":
    unittest.main()
