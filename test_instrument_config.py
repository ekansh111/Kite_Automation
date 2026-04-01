import json
import unittest
from pathlib import Path

from vol_target import compute_daily_vol_target


CONFIG_PATH = Path(__file__).with_name("instrument_config.json")


class TestInstrumentConfig(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        cls.account = cls.config["account"]
        cls.instruments = cls.config["instruments"]

    def test_account_fields_are_present(self):
        self.assertGreater(self.account["base_capital"], 0)
        self.assertGreater(self.account["annual_vol_target_pct"], 0)
        self.assertIn("dry_run", self.account)

    def test_enabled_instruments_have_required_runtime_fields(self):
        required_top = {
            "broker",
            "enabled",
            "exchange",
            "FDM",
            "forecast_cap",
            "order_routing",
            "point_value",
            "position_inertia_pct",
            "rollover",
            "subsystems",
            "system_name_map",
            "user",
        }
        for name, cfg in self.instruments.items():
            if not cfg.get("enabled"):
                continue
            missing = sorted(required_top - cfg.keys())
            self.assertEqual(missing, [], f"{name} missing runtime fields: {missing}")
            self.assertIn("execution", cfg, f"{name} should explicitly declare execution.use_smart_chase")

    def test_enabled_instrument_subsystem_weights_sum_to_one(self):
        for name, cfg in self.instruments.items():
            if not cfg.get("enabled"):
                continue
            total = sum(cfg["subsystems"].values())
            self.assertAlmostEqual(total, 1.0, places=2, msg=f"{name} subsystem weights sum to {total}")

    def test_system_name_map_targets_known_subsystems(self):
        for name, cfg in self.instruments.items():
            subsystems = set(cfg["subsystems"].keys())
            bad = {webhook: target for webhook, target in cfg.get("system_name_map", {}).items() if target not in subsystems}
            self.assertEqual(bad, {}, f"{name} has unknown system_name_map targets: {bad}")

    def test_order_routing_fields_are_complete_for_enabled_instruments(self):
        required_order = {
            "ContractNameProvided",
            "ConvertToMarketOrder",
            "DaysPostWhichSelectNextContract",
            "EntrySleepDuration",
            "ExitSleepDuration",
            "InstrumentType",
            "Product",
            "QuantityMultiplier",
            "Validity",
            "Variety",
        }
        for name, cfg in self.instruments.items():
            if not cfg.get("enabled"):
                continue
            routing = cfg["order_routing"]
            missing = sorted(required_order - routing.keys())
            self.assertEqual(missing, [], f"{name} missing order_routing fields: {missing}")
            self.assertGreater(int(routing["QuantityMultiplier"]), 0, f"{name} has non-positive QuantityMultiplier")
            self.assertIn(
                routing["ConvertToMarketOrder"],
                {"True", "False"},
                f"{name} ConvertToMarketOrder should be string boolean",
            )
            self.assertIn(
                routing["ContractNameProvided"],
                {"True", "False"},
                f"{name} ContractNameProvided should be string boolean",
            )
            if cfg["broker"] == "ANGEL":
                self.assertGreaterEqual(
                    len(routing.get("ReconciliationPrefixes", [])),
                    1,
                    f"{name} should define at least one reconciliation prefix",
                )

    def test_rollover_fields_are_complete_for_enabled_instruments(self):
        required_rollover = {
            "alert_days_before_expiry",
            "enabled",
            "execute_days_before_expiry",
            "liquidity_urgency",
            "preferred_window_end",
            "preferred_window_start",
        }
        for name, cfg in self.instruments.items():
            if not cfg.get("enabled"):
                continue
            rollover = cfg["rollover"]
            missing = sorted(required_rollover - rollover.keys())
            self.assertEqual(missing, [], f"{name} missing rollover fields: {missing}")
            self.assertLessEqual(
                rollover["execute_days_before_expiry"],
                rollover["alert_days_before_expiry"],
                f"{name} executes rollover after the alert threshold",
            )

    def test_execution_config_is_explicit_and_complete_when_enabled(self):
        required_execution = {
            "baseline_spread_ticks",
            "buffer_ticks",
            "chase_step_ticks",
            "market_open_delay_seconds",
            "max_chase_seconds_entry",
            "max_chase_seconds_exit",
            "max_chase_ticks",
            "max_settle_wait_seconds",
            "poll_interval_seconds",
            "tick_size",
            "use_smart_chase",
        }
        for name, cfg in self.instruments.items():
            if not cfg.get("enabled"):
                continue
            execution = cfg["execution"]
            self.assertIsInstance(execution.get("use_smart_chase"), bool, f"{name} use_smart_chase should be boolean")
            if execution["use_smart_chase"]:
                missing = sorted(required_execution - execution.keys())
                self.assertEqual(missing, [], f"{name} missing execution fields: {missing}")
                self.assertGreater(execution["tick_size"], 0, f"{name} has invalid tick_size")
            else:
                self.assertEqual(
                    set(execution.keys()),
                    {"use_smart_chase"},
                    f"{name} legacy execution config should stay minimal and explicit",
                )

    def test_each_instrument_has_position_sizing_source(self):
        for name, cfg in self.instruments.items():
            has_weights = "vol_weights" in cfg
            has_daily_target = "daily_vol_target" in cfg
            self.assertTrue(has_weights or has_daily_target, f"{name} has no position sizing source")

    def test_vol_weights_compute_positive_daily_targets(self):
        for name, cfg in self.instruments.items():
            vol_weights = cfg.get("vol_weights")
            if not vol_weights:
                continue
            daily_target = compute_daily_vol_target(
                self.account["base_capital"],
                self.account["annual_vol_target_pct"],
                vol_weights,
            )
            self.assertGreater(daily_target, 0, f"{name} computed non-positive daily target")

    def test_numeric_ranges_are_sane(self):
        for name, cfg in self.instruments.items():
            self.assertGreater(cfg["point_value"], 0, f"{name} point_value should be positive")
            self.assertGreater(cfg["forecast_cap"], 0, f"{name} forecast_cap should be positive")
            self.assertGreater(cfg["FDM"], 0, f"{name} FDM should be positive")
            self.assertGreaterEqual(cfg["position_inertia_pct"], 0, f"{name} position_inertia_pct should be non-negative")
            self.assertLessEqual(cfg["position_inertia_pct"], 1, f"{name} position_inertia_pct should be <= 1")

    def test_auto2_aliases_match_subsystem_suffixes(self):
        for name, cfg in self.instruments.items():
            subsystem_names = set(cfg["subsystems"])
            for alias, target in cfg.get("system_name_map", {}).items():
                if not alias.startswith("AUTO2_"):
                    continue
                self.assertIn(target, subsystem_names, f"{name} alias {alias} points to unknown subsystem {target}")
                valid_forms = {
                    f"_{target}",
                    f"_{target}_",
                }
                self.assertTrue(
                    any(token in alias for token in valid_forms),
                    f"{name} alias {alias} does not encode subsystem {target}",
                )

    def test_dhaniya_auto2_alias_is_present(self):
        self.assertEqual(
            self.instruments["DHANIYA"]["system_name_map"]["AUTO2_DHANIYA_S45A"],
            "S45A",
        )


if __name__ == "__main__":
    unittest.main()
