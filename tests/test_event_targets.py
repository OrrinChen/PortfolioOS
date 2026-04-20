from __future__ import annotations

import unittest

import pandas as pd

from portfolio_os.alpha.event_targets import build_event_basket_target_frame, build_event_target_manifest
from portfolio_os.data.portfolio import load_target_weights


class EventTargetTests(unittest.TestCase):
    def test_build_event_basket_target_frame_enforces_whitelist_and_top_tercile_cap(self) -> None:
        candidates = pd.DataFrame(
            {
                "ticker": ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "ZZZ"],
                "score": [7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 10.0],
            }
        )

        target_frame = build_event_basket_target_frame(
            candidates,
            whitelist={"AAA", "BBB", "CCC", "DDD", "EEE", "FFF"},
            min_cohort_size=6,
            top_fraction=1.0 / 3.0,
            max_new_entries=5,
        )

        self.assertEqual(target_frame["ticker"].tolist(), ["AAA", "BBB"])
        self.assertAlmostEqual(float(target_frame["target_weight"].sum()), 1.0)
        self.assertAlmostEqual(float(target_frame.iloc[0]["target_weight"]), 0.5)

    def test_build_event_basket_target_frame_returns_empty_when_filtered_cohort_is_too_small(self) -> None:
        candidates = pd.DataFrame(
            {
                "ticker": ["AAA", "BBB", "CCC"],
                "score": [3.0, 2.0, 1.0],
            }
        )

        target_frame = build_event_basket_target_frame(
            candidates,
            whitelist={"AAA", "BBB", "CCC"},
            min_cohort_size=6,
        )

        self.assertTrue(target_frame.empty)

    def test_event_target_output_is_compatible_with_target_loader_and_manifest(self) -> None:
        candidates = pd.DataFrame(
            {
                "ticker": ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"],
                "score": [6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
            }
        )

        target_frame = build_event_basket_target_frame(
            candidates,
            whitelist={"AAA", "BBB", "CCC", "DDD", "EEE", "FFF"},
            min_cohort_size=6,
            top_fraction=1.0 / 3.0,
            max_new_entries=5,
        )
        manifest = build_event_target_manifest(
            event_date="2026-04-08",
            target_frame=target_frame,
            cohort_size=6,
            whitelist_size=6,
            min_cohort_size=6,
            top_fraction=1.0 / 3.0,
            max_new_entries=5,
        )

        self.assertEqual(manifest["selected_count"], 2)
        self.assertEqual(manifest["selection_policy"]["max_new_entries"], 5)

        with self.subTest("target loader"):
            import tempfile
            from pathlib import Path

            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "target.csv"
                target_frame.to_csv(path, index=False)
                loaded = load_target_weights(path)
                self.assertEqual(len(loaded), 2)


if __name__ == "__main__":
    unittest.main()
