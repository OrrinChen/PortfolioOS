from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.factor_placebo import run_real_placebo_robustness
from factor_discovery_sandbox.real_factor_replay import run_real_factor_replay
from factor_discovery_sandbox.real_rolling_oos import run_real_rolling_oos
from test_real_factor_replay import _write_daily_bundle


def test_real_placebo_robustness_blocks_allocator_when_live_oos_fails_placebos(tmp_path: Path) -> None:
    manifest = _write_daily_bundle(tmp_path)
    replay = run_real_factor_replay(manifest, tmp_path / "fd_r3")
    oos = run_real_rolling_oos(
        manifest_path=manifest,
        factor_panel_path=replay.artifacts["real_factor_panel"],
        output_dir=tmp_path / "fd_r4",
        train_window_months=3,
        validation_window_months=2,
        horizons=(1, 3),
        min_ic_observations=2,
        min_cross_section=2,
    )

    result = run_real_placebo_robustness(
        manifest_path=manifest,
        factor_panel_path=replay.artifacts["real_factor_panel"],
        oos_score_panel_path=oos.artifacts["oos_factor_score_panel_real"],
        oos_decile_spread_path=oos.artifacts["oos_decile_spread_real"],
        output_dir=tmp_path / "fd_r5",
    )

    assert result.summary["schema_version"] == "fd_real_placebo_robustness_summary.v1"
    assert result.summary["stage"] == "FD-R5"
    assert result.summary["allocator_entry_allowed"] is False
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["alpha_success_claimed"] is False
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["recommended_next_action"] in {
        "stop_before_allocator",
        "needs_more_evidence_before_allocator",
    }

    assert {
        "placebo_report",
        "robustness_by_period",
        "robustness_by_regime",
        "factor_family_diagnostics",
        "placebo_robustness_summary",
    } == set(result.artifacts)

    placebo = pd.read_csv(result.artifacts["placebo_report"])
    required_placebos = {
        "live_oos_score",
        "shuffled_cross_section_placebo",
        "lagged_signal_placebo",
        "future_return_leakage_negative_control",
        "random_same_coverage_placebo",
        "sector_neutral_placebo",
        "rebalance_date_shifted_placebo",
    }
    assert required_placebos.issubset(set(placebo["test_name"]))
    assert {"period", "horizon_months", "mean_rank_ic", "mean_top_bottom_spread"}.issubset(placebo.columns)
    assert placebo["not_alpha_evidence"].eq(True).all()

    by_period = pd.read_csv(result.artifacts["robustness_by_period"])
    assert {"period", "horizon_months", "positive_spread_rate", "mean_rank_ic"}.issubset(by_period.columns)

    by_regime = pd.read_csv(result.artifacts["robustness_by_regime"])
    assert {"period", "horizon_months", "benchmark_regime", "mean_top_bottom_spread"}.issubset(by_regime.columns)

    family_report = result.artifacts["factor_family_diagnostics"].read_text(encoding="utf-8").lower()
    assert "factor family diagnostics" in family_report
    assert "not alpha evidence" in family_report
    assert "allocator entry" in family_report

    summary = json.loads(result.artifacts["placebo_robustness_summary"].read_text(encoding="utf-8"))
    assert summary == result.summary
