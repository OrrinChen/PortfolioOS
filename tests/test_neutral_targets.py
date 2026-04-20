from __future__ import annotations

import unittest

from portfolio_os.alpha.neutral_targets import (
    build_neutral_order_frame,
    build_neutral_target_frame,
    build_neutral_target_manifest,
)


class NeutralTargetTests(unittest.TestCase):
    def test_build_neutral_target_frame_is_deterministic_for_spy_buy_and_hold(self) -> None:
        frame_a = build_neutral_target_frame(
            tickers=["SPY"],
            gross_target_weight=1.0,
        )
        frame_b = build_neutral_target_frame(
            tickers=["SPY"],
            gross_target_weight=1.0,
        )

        self.assertEqual(frame_a.to_dict(orient="records"), frame_b.to_dict(orient="records"))
        self.assertEqual(frame_a["ticker"].tolist(), ["SPY"])
        self.assertAlmostEqual(float(frame_a["target_weight"].sum()), 1.0)

    def test_build_neutral_target_frame_applies_small_deterministic_perturbation(self) -> None:
        frame = build_neutral_target_frame(
            tickers=["SPY", "IVV"],
            gross_target_weight=1.0,
            perturbation_bps=10.0,
            perturbation_seed=7,
        )

        self.assertEqual(sorted(frame["ticker"].tolist()), ["IVV", "SPY"])
        self.assertAlmostEqual(float(frame["target_weight"].sum()), 1.0, places=9)

    def test_build_neutral_target_manifest_captures_selection_inputs(self) -> None:
        frame = build_neutral_target_frame(
            tickers=["SPY"],
            gross_target_weight=1.0,
        )

        manifest = build_neutral_target_manifest(
            target_frame=frame,
            strategy_name="neutral_buy_and_hold",
            perturbation_bps=0.0,
            perturbation_seed=None,
        )

        self.assertEqual(manifest["strategy_name"], "neutral_buy_and_hold")
        self.assertEqual(manifest["selected_tickers"], ["SPY"])
        self.assertEqual(manifest["target_weight_sum"], 1.0)

    def test_build_neutral_order_frame_creates_buy_order_for_fixed_quantity(self) -> None:
        frame = build_neutral_order_frame(
            tickers=["SPY"],
            quantity=3,
        )

        self.assertEqual(frame.to_dict(orient="records"), [{"ticker": "SPY", "direction": "buy", "quantity": 3.0}])


if __name__ == "__main__":
    unittest.main()
