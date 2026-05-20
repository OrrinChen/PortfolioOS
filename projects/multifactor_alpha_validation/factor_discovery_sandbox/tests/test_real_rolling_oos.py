from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.real_factor_replay import run_real_factor_replay
from factor_discovery_sandbox.real_rolling_oos import run_real_rolling_oos
from test_real_factor_replay import _write_daily_bundle


def test_real_rolling_oos_uses_prior_visible_returns_and_writes_required_artifacts(tmp_path: Path) -> None:
    manifest = _write_daily_bundle(tmp_path)
    replay = run_real_factor_replay(manifest, tmp_path / "fd_r3")

    result = run_real_rolling_oos(
        manifest_path=manifest,
        factor_panel_path=replay.artifacts["real_factor_panel"],
        output_dir=tmp_path / "fd_r4",
        train_window_months=3,
        validation_window_months=2,
        horizons=(1, 3),
        min_ic_observations=2,
        min_cross_section=2,
    )

    assert result.summary["schema_version"] == "fd_real_rolling_oos_summary.v1"
    assert result.summary["stage"] == "FD-R4"
    assert result.summary["dataset_frequency"] == "daily"
    assert result.summary["train_window_months"] == 3
    assert result.summary["validation_window_months"] == 2
    assert result.summary["horizons_months"] == [1, 3]
    assert result.summary["uses_full_sample_icir"] is False
    assert result.summary["future_universe_used"] is False
    assert result.summary["future_normalization_used"] is False
    assert result.summary["post_period_factor_selection_used"] is False
    assert result.summary["allocator_ran"] is False
    assert result.summary["alpha_success_claimed"] is False
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["not_alpha_evidence"] is True

    assert {
        "rolling_icir_real",
        "oos_factor_score_panel_real",
        "oos_decile_spread_real",
        "oos_validation_report",
        "oos_validation_summary",
    } == set(result.artifacts)

    weights = pd.read_csv(result.artifacts["rolling_icir_real"])
    required_weight_columns = {
        "rebalance_date",
        "period",
        "horizon_months",
        "estimation_window_start",
        "estimation_window_end",
        "return_visibility_cutoff",
        "factor_id",
        "rolling_icir",
        "weight",
        "uses_full_sample_icir",
        "future_universe_used",
        "future_normalization_used",
        "post_period_factor_selection_used",
    }
    assert required_weight_columns.issubset(weights.columns)
    assert set(weights["period"]).issuperset({"validation", "test"})
    assert set(weights["horizon_months"]) == {1, 3}
    assert weights["uses_full_sample_icir"].eq(False).all()
    assert weights["future_universe_used"].eq(False).all()
    assert weights["future_normalization_used"].eq(False).all()
    assert weights["post_period_factor_selection_used"].eq(False).all()
    no_history = weights["history_observation_count"].eq(0)
    assert weights.loc[no_history, "weight"].eq(0.0).all()
    with_history = weights[~no_history]
    assert (pd.to_datetime(with_history["estimation_window_end"]) < pd.to_datetime(with_history["rebalance_date"])).all()
    assert (
        pd.to_datetime(with_history["return_visibility_cutoff"]) < pd.to_datetime(with_history["rebalance_date"])
    ).all()

    score_panel = pd.read_csv(result.artifacts["oos_factor_score_panel_real"])
    required_score_columns = {
        "rebalance_date",
        "period",
        "horizon_months",
        "asset_id",
        "score",
        "coverage_state",
        "forward_excess_return",
        "forward_return_available",
        "signal_timestamp",
        "visibility_timestamp",
        "tradable_timestamp",
        "target_return_visible_timestamp",
        "no_view_is_not_zero_alpha",
    }
    assert required_score_columns.issubset(score_panel.columns)
    assert score_panel["coverage_state"].isin({"active_score", "explicit_abstain"}).all()
    assert (pd.to_datetime(score_panel["tradable_timestamp"]) > pd.to_datetime(score_panel["signal_timestamp"])).all()
    assert (
        pd.to_datetime(score_panel["target_return_visible_timestamp"]) > pd.to_datetime(score_panel["tradable_timestamp"])
    ).all()
    assert score_panel["no_view_is_not_zero_alpha"].eq(True).all()

    deciles = pd.read_csv(result.artifacts["oos_decile_spread_real"])
    assert {
        "rebalance_date",
        "period",
        "horizon_months",
        "top_decile_excess_return",
        "bottom_decile_excess_return",
        "top_bottom_spread",
        "rank_ic",
    }.issubset(deciles.columns)
    assert deciles["horizon_months"].isin({1, 3}).all()

    summary = json.loads(result.artifacts["oos_validation_summary"].read_text(encoding="utf-8"))
    assert summary == result.summary

    report = result.artifacts["oos_validation_report"].read_text(encoding="utf-8").lower()
    assert "fd-r4 true rolling oos validation" in report
    assert "full-sample icir: forbidden" in report
    assert "not alpha evidence" in report
    assert "direct q2 entry: not allowed" in report
