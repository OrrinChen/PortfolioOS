from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_direction_remap import (
    run_small_emotion_direction_remap_audit,
)


def test_direction_remap_recognizes_reversal_as_new_no_formula_mechanism(tmp_path: Path) -> None:
    input_dir = _write_remap_fixture(tmp_path / "d2")

    result = run_small_emotion_direction_remap_audit(
        input_dir=input_dir,
        output_dir=tmp_path / "remap",
        minimum_subset_events=20,
        minimum_event_month_count=12,
        minimum_label_coverage_share=0.70,
    )

    assert result.summary["schema_version"] == "small_emotion_direction_remap_summary.v1"
    assert result.summary["stage"] == "D2-SMALL-EMOTION-01B"
    assert result.summary["overall_decision"] == "observable_up_shock_reversal"
    assert result.summary["allow_d3_charter_for"] == ["up_shock_reversal"]
    assert result.summary["source_d2_modified"] is False
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["portfolio_construction_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    grid = pd.read_csv(result.artifacts["shock_direction_remap_grid"])
    assert set(grid["mechanism"]) == {
        "up_shock_continuation",
        "up_shock_reversal",
        "down_shock_reversal",
        "down_shock_continuation",
    }
    selected = grid[grid["mechanism"].eq("up_shock_reversal")].iloc[0]
    assert selected["source_subset"] == "fomo_continuation_candidate"
    assert bool(selected["direction_matches_preregistered_mechanism"]) is True
    assert bool(selected["pre_event_dominates_post"]) is False
    assert bool(selected["placebo_dominates_live"]) is False
    assert bool(selected["eligible_for_d3_charter"]) is True

    continuation = grid[grid["mechanism"].eq("up_shock_continuation")].iloc[0]
    assert bool(continuation["direction_matches_preregistered_mechanism"]) is False
    assert bool(continuation["eligible_for_d3_charter"]) is False

    placebo = pd.read_csv(result.artifacts["shock_direction_placebo_audit"])
    assert "stale_price_matched" in set(placebo["placebo_name"])
    assert not placebo[placebo["mechanism"].eq("up_shock_reversal")]["placebo_dominates_live"].any()

    report = result.artifacts["shock_direction_remap_report"].read_text(encoding="utf-8").lower()
    assert "no-formula remap audit only" in report
    assert "not alpha evidence" in report
    for forbidden in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "alpha passed",
        "q2-ready",
        "tradable alpha",
    ]:
        assert forbidden not in report
    assert not (tmp_path / "remap" / "measurement_spec.yaml").exists()
    assert not (tmp_path / "remap" / "expected_return_panel.csv").exists()


def test_direction_remap_blocks_subset_when_transformed_placebo_dominates(tmp_path: Path) -> None:
    input_dir = _write_remap_fixture(tmp_path / "d2")
    placebo_path = input_dir / "placebo_report.csv"
    placebo = pd.read_csv(placebo_path)
    mask = placebo["event_subset"].eq("fomo_continuation_candidate") & placebo["placebo_name"].eq("shift_minus_5")
    placebo.loc[mask, "placebo_directional_return"] = -0.08
    placebo.to_csv(placebo_path, index=False)

    result = run_small_emotion_direction_remap_audit(
        input_dir=input_dir,
        output_dir=tmp_path / "remap",
        minimum_subset_events=20,
        minimum_event_month_count=12,
        minimum_label_coverage_share=0.70,
    )

    assert result.summary["overall_decision"] == "blocked_placebo_dominance"
    assert result.summary["allow_d3_charter_for"] == []
    grid = pd.read_csv(result.artifacts["shock_direction_remap_grid"])
    selected = grid[grid["mechanism"].eq("up_shock_reversal")].iloc[0]
    assert bool(selected["placebo_dominates_live"]) is True
    assert bool(selected["eligible_for_d3_charter"]) is False


def _write_remap_fixture(path: Path) -> Path:
    path.mkdir(parents=True)
    guard = {
        "formula_score_written": False,
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "optimizer_entry_allowed": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
    }
    pd.DataFrame(
        [
            {
                "event_subset": "fomo_continuation_candidate",
                "event_count": 100,
                "active_event_count": 100,
                "event_month_count": 12,
                "issuer_cluster_count": 80,
                "observed_post_1_22_count": 90,
                "label_coverage_share": 0.90,
                **guard,
            },
            {
                "event_subset": "panic_overreaction_candidate",
                "event_count": 100,
                "active_event_count": 100,
                "event_month_count": 12,
                "issuer_cluster_count": 80,
                "observed_post_1_22_count": 90,
                "label_coverage_share": 0.90,
                **guard,
            },
        ]
    ).to_csv(path / "subset_counts.csv", index=False)
    pd.DataFrame(
        [
            {
                "event_subset": "fomo_continuation_candidate",
                "window": "post_1_22",
                "label_status": "observed",
                "mean_directional_return": -0.030,
                "mean_abnormal_return": -0.030,
                **guard,
            },
            {
                "event_subset": "fomo_continuation_candidate",
                "window": "pre_5_1",
                "label_status": "observed",
                "mean_directional_return": -0.005,
                "mean_abnormal_return": -0.005,
                **guard,
            },
            {
                "event_subset": "panic_overreaction_candidate",
                "window": "post_1_22",
                "label_status": "observed",
                "mean_directional_return": 0.010,
                "mean_abnormal_return": 0.010,
                **guard,
            },
            {
                "event_subset": "panic_overreaction_candidate",
                "window": "pre_5_1",
                "label_status": "observed",
                "mean_directional_return": 0.020,
                "mean_abnormal_return": 0.020,
                **guard,
            },
        ]
    ).to_csv(path / "car_window_panel.csv", index=False)
    pd.DataFrame(
        [
            {
                "event_subset": "fomo_continuation_candidate",
                "placebo_name": "shift_minus_5",
                "live_post_1_22_directional_return": -0.030,
                "placebo_directional_return": -0.010,
                "placebo_dominates_live": False,
                "status": "pass",
                **guard,
            },
            {
                "event_subset": "fomo_continuation_candidate",
                "placebo_name": "stale_price_matched",
                "live_post_1_22_directional_return": -0.030,
                "placebo_directional_return": -0.008,
                "placebo_dominates_live": False,
                "status": "pass",
                **guard,
            },
            {
                "event_subset": "panic_overreaction_candidate",
                "placebo_name": "shift_minus_5",
                "live_post_1_22_directional_return": -0.010,
                "placebo_directional_return": -0.005,
                "placebo_dominates_live": False,
                "status": "pass",
                **guard,
            },
            {
                "event_subset": "panic_overreaction_candidate",
                "placebo_name": "stale_price_matched",
                "live_post_1_22_directional_return": -0.010,
                "placebo_directional_return": -0.002,
                "placebo_dominates_live": False,
                "status": "pass",
                **guard,
            },
        ]
    ).to_csv(path / "placebo_report.csv", index=False)
    pd.DataFrame(
        [
            {
                "stale_price_guard_generated": True,
                "stale_placebo_dominates_live": True,
                "stale_rows_marked_no_view": True,
                **guard,
            }
        ]
    ).to_csv(path / "stale_price_guard_report.csv", index=False)
    pd.DataFrame(
        [
            {
                "capacity_guard_generated": True,
                "capacity_guard_fatal": False,
                **guard,
            }
        ]
    ).to_csv(path / "adv_capacity_guard_report.csv", index=False)
    (path / "d2_small_emotion_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_d2_summary.v1",
                "overall_decision": "blocked_stale_price",
                "formula_score_written": False,
                "measurement_spec_written": False,
                "q1_entry_allowed": False,
                "q2_entry_allowed": False,
                "expected_return_panel_written": False,
                "optimizer_entry_allowed": False,
                "production_approval_claimed": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path
