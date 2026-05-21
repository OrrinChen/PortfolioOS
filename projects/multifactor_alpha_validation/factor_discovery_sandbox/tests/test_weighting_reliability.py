from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.teaching_baseline import FACTOR_NAMES
from factor_discovery_sandbox.weighting_reliability import run_weighting_reliability_gate


def test_weighting_reliability_blocks_allocator_q1_q2_registry(tmp_path: Path) -> None:
    artifacts = _write_weighting_fixture(tmp_path)

    result = run_weighting_reliability_gate(
        factor_panel_path=artifacts["factor_panel"],
        rolling_weights_path=artifacts["weights"],
        oos_score_panel_path=artifacts["scores"],
        placebo_report_path=artifacts["placebo"],
        output_dir=tmp_path / "research_mode",
        report_path=tmp_path / "weighting_report.md",
        train_window_months=4,
        shrink_lambdas=(3.0,),
        ridge_alphas=(1.0,),
    )

    assert result.summary["schema_version"] == "fd_weighting_reliability_summary.v1"
    assert result.summary["allocator_entry_allowed"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["not_alpha_evidence"] is True

    assert {
        "weighting_estimator_comparison",
        "weight_stability_diagnostics",
        "weighting_placebo_comparison",
        "weighting_failure_diagnosis",
        "weighting_reliability_report",
    } == set(result.artifacts)


def test_failure_diagnosis_marks_rolling_icir_overfit_when_placebo_gate_fails(tmp_path: Path) -> None:
    artifacts = _write_weighting_fixture(tmp_path)

    result = run_weighting_reliability_gate(
        factor_panel_path=artifacts["factor_panel"],
        rolling_weights_path=artifacts["weights"],
        oos_score_panel_path=artifacts["scores"],
        placebo_report_path=artifacts["placebo"],
        output_dir=tmp_path / "research_mode",
        report_path=tmp_path / "weighting_report.md",
        train_window_months=4,
        shrink_lambdas=(3.0,),
        ridge_alphas=(1.0,),
    )

    diagnosis = json.loads(result.artifacts["weighting_failure_diagnosis"].read_text(encoding="utf-8"))
    assert diagnosis["placebo_status"] == "failed_placebo_gate"
    assert diagnosis["rolling_icir_overfit_noise_failure"] is True
    assert diagnosis["decision"] in {"fail", "close"}
    assert diagnosis["recommended_next_action"] == "do_not_enter_allocator"


def test_outputs_are_not_alpha_evidence(tmp_path: Path) -> None:
    artifacts = _write_weighting_fixture(tmp_path)

    result = run_weighting_reliability_gate(
        factor_panel_path=artifacts["factor_panel"],
        rolling_weights_path=artifacts["weights"],
        oos_score_panel_path=artifacts["scores"],
        placebo_report_path=artifacts["placebo"],
        output_dir=tmp_path / "research_mode",
        report_path=tmp_path / "weighting_report.md",
        train_window_months=4,
        shrink_lambdas=(3.0,),
        ridge_alphas=(1.0,),
    )

    comparison = pd.read_csv(result.artifacts["weighting_estimator_comparison"])
    stability = pd.read_csv(result.artifacts["weight_stability_diagnostics"])
    placebo = pd.read_csv(result.artifacts["weighting_placebo_comparison"])
    assert comparison["not_alpha_evidence"].eq(True).all()
    assert comparison["direct_q2_entry_allowed"].eq(False).all()
    assert stability["not_alpha_evidence"].eq(True).all()
    assert placebo["not_alpha_evidence"].eq(True).all()

    required_columns = {
        "mean_rank_ic_1m",
        "mean_rank_ic_3m",
        "spread_1m",
        "spread_3m",
        "rank_ic_tstat",
        "spread_tstat",
        "weight_turnover",
        "weight_entropy",
        "max_single_factor_weight",
        "max_family_weight",
        "sign_flip_rate",
        "factor_entry_exit_rate",
        "expected_vs_realized_ic_corr",
        "placebo_weight_correlation",
        "subperiod_survival_rate",
        "bootstrap_weight_stability",
    }
    assert required_columns.issubset(comparison.columns)


def _write_weighting_fixture(tmp_path: Path) -> dict[str, Path]:
    dates = pd.to_datetime(
        [
            "2020-01-31",
            "2020-02-28",
            "2020-03-31",
            "2020-04-30",
            "2020-05-29",
            "2020-06-30",
            "2020-07-31",
            "2020-08-31",
        ]
    )
    assets = ["A", "B", "C", "D", "E", "F"]
    families = {
        factor_id: (
            "price_momentum"
            if factor_id.startswith("momentum_")
            else "trend_quality"
            if factor_id.startswith("trend_")
            else "risk_volatility"
            if factor_id.startswith("volatility_")
            else "other"
        )
        for factor_id in FACTOR_NAMES
    }
    factor_rows = []
    score_rows = []
    weight_rows = []
    placebo_rows = []
    for date_index, date in enumerate(dates):
        period = "validation" if date_index < 4 else "test"
        for asset_index, asset in enumerate(assets):
            base_signal = float(asset_index - 2.5)
            forward = base_signal * 0.01
            for factor_index, factor_id in enumerate(FACTOR_NAMES):
                value = base_signal if factor_id.startswith("momentum_") else -base_signal if factor_index % 2 else 0.1 * base_signal
                factor_rows.append(
                    {
                        "schema_version": "fd_real_factor_panel.v2",
                        "factor_id": factor_id,
                        "rebalance_date": date.date().isoformat(),
                        "date": date.date().isoformat(),
                        "asset_id": asset,
                        "ticker": asset,
                        "normalized_value": value,
                        "coverage_status": "active_view",
                        "mechanism_family": families[factor_id],
                        "known_correlation_family": families[factor_id],
                        "no_view_is_not_zero_alpha": True,
                        "not_alpha_evidence": True,
                        "direct_q2_entry_allowed": False,
                    }
                )
            for horizon in [1, 3]:
                score_rows.append(
                    {
                        "schema_version": "fd_real_oos_factor_score.v1",
                        "rebalance_date": date.date().isoformat(),
                        "period": period,
                        "horizon_months": horizon,
                        "asset_id": asset,
                        "ticker": asset,
                        "score": -base_signal,
                        "coverage_state": "active_score",
                        "forward_excess_return": forward,
                        "forward_return_available": True,
                        "target_return_visible_timestamp": (date + pd.Timedelta(days=20 * horizon)).isoformat(),
                        "not_alpha_evidence": True,
                    }
                )
        for horizon in [1, 3]:
            for factor_id in FACTOR_NAMES:
                weight_rows.append(
                    {
                        "schema_version": "fd_real_rolling_icir.v1",
                        "rebalance_date": date.date().isoformat(),
                        "period": period,
                        "horizon_months": horizon,
                        "estimation_window_start": dates[max(0, date_index - 4)].date().isoformat(),
                        "estimation_window_end": dates[max(0, date_index - 1)].date().isoformat()
                        if date_index
                        else "",
                        "return_visibility_cutoff": (date - pd.Timedelta(days=1)).date().isoformat(),
                        "factor_id": factor_id,
                        "history_observation_count": min(date_index, 4),
                        "rolling_ic_mean": -0.1 if factor_id.startswith("momentum_") else 0.0,
                        "rolling_ic_std": 0.1,
                        "rolling_icir": -1.0 if factor_id.startswith("momentum_") else 0.0,
                        "weight": -1.0 / 6.0 if factor_id.startswith("momentum_") else 0.0,
                        "weight_status": "active" if date_index else "insufficient_visible_history",
                        "uses_full_sample_icir": False,
                        "future_universe_used": False,
                        "future_normalization_used": False,
                        "post_period_factor_selection_used": False,
                        "not_alpha_evidence": True,
                    }
                )
    for horizon in [1, 3]:
        for test_name, spread, rank_ic in [
            ("live_oos_score", -0.01, -0.1),
            ("shuffled_cross_section_placebo", 0.0, 0.0),
            ("random_same_coverage_placebo", 0.01, 0.05),
            ("rebalance_date_shifted_placebo", 0.0, 0.0),
        ]:
            placebo_rows.append(
                {
                    "schema_version": "fd_real_placebo_report.v1",
                    "test_name": test_name,
                    "period": "test",
                    "horizon_months": horizon,
                    "rebalance_count": 4,
                    "mean_rank_ic": rank_ic,
                    "mean_top_bottom_spread": spread,
                    "positive_spread_rate": 0.0,
                    "not_alpha_evidence": True,
                }
            )

    artifacts = {
        "factor_panel": tmp_path / "real_factor_panel.csv",
        "weights": tmp_path / "rolling_icir_real.csv",
        "scores": tmp_path / "oos_factor_score_panel_real.csv",
        "placebo": tmp_path / "placebo_report.csv",
    }
    pd.DataFrame(factor_rows).to_csv(artifacts["factor_panel"], index=False)
    pd.DataFrame(weight_rows).to_csv(artifacts["weights"], index=False)
    pd.DataFrame(score_rows).to_csv(artifacts["scores"], index=False)
    pd.DataFrame(placebo_rows).to_csv(artifacts["placebo"], index=False)
    return artifacts
