from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.portfolio_component_gate import run_portfolio_component_gate


def test_portfolio_component_gate_classifies_components_without_requiring_clean_residual(tmp_path: Path) -> None:
    input_dir = tmp_path / "risk_model"
    input_dir.mkdir()
    pd.DataFrame(
        [
            {
                "factor_id": "clean_factor",
                "closeout_status": "ready_for_redundancy_gate",
                "dominant_failure_layer": "none",
                "gross_spread_mean": 0.010,
                "qqq_relative_spread_mean": 0.002,
                "beta_adjusted_spread_mean": 0.006,
                "industry_adjusted_spread_mean": 0.005,
                "style_proxy_adjusted_spread_mean": 0.004,
                "full_residual_spread_mean": 0.004,
                "residual_positive_rate": 0.70,
                "would_pass_without_qqq_guard": True,
                "recommended_action": "allow_redundancy",
            },
            {
                "factor_id": "momentum_12_1",
                "closeout_status": "insufficient_residual_evidence",
                "dominant_failure_layer": "residual_stability",
                "gross_spread_mean": 0.010,
                "qqq_relative_spread_mean": -0.020,
                "beta_adjusted_spread_mean": 0.006,
                "industry_adjusted_spread_mean": 0.005,
                "style_proxy_adjusted_spread_mean": -0.001,
                "full_residual_spread_mean": -0.001,
                "residual_positive_rate": 0.52,
                "would_pass_without_qqq_guard": False,
                "recommended_action": "stop_residual_not_stable",
            },
            {
                "factor_id": "low_vol_60d",
                "closeout_status": "style_proxy_conflict",
                "dominant_failure_layer": "beta_exposure",
                "gross_spread_mean": -0.004,
                "qqq_relative_spread_mean": -0.015,
                "beta_adjusted_spread_mean": -0.006,
                "industry_adjusted_spread_mean": -0.005,
                "style_proxy_adjusted_spread_mean": 0.003,
                "full_residual_spread_mean": 0.003,
                "residual_positive_rate": 0.58,
                "would_pass_without_qqq_guard": False,
                "recommended_action": "stop_exposure_conflict",
            },
            {
                "factor_id": "bad_timestamp_factor",
                "closeout_status": "blocked_pit_failure",
                "dominant_failure_layer": "pit_timestamp",
                "gross_spread_mean": 0.100,
                "qqq_relative_spread_mean": 0.090,
                "beta_adjusted_spread_mean": 0.080,
                "industry_adjusted_spread_mean": 0.070,
                "style_proxy_adjusted_spread_mean": 0.060,
                "full_residual_spread_mean": 0.050,
                "residual_positive_rate": 0.90,
                "would_pass_without_qqq_guard": True,
                "recommended_action": "blocked",
            },
        ]
    ).to_csv(input_dir / "factor_failure_diagnosis.csv", index=False)
    (input_dir / "qqq_relative_guard_review.json").write_text(
        json.dumps(
            {
                "schema_version": "qqq_relative_guard_review.v1",
                "hard_gate_recommended_for_long_short_spread": False,
                "over_strict_as_hard_gate": True,
                "rescued_by_softening_count": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = run_portfolio_component_gate(input_dir, tmp_path / "r14")

    table = pd.read_csv(result.component_table_path).set_index("factor_id")
    assert result.component_candidate_count == 3
    assert result.standalone_clean_alpha_count == 1
    assert result.portfolio_validation_mode == "diagnostic_ensemble_only"
    assert table.loc["clean_factor", "component_status"] == "standalone_clean_alpha"
    assert table.loc["momentum_12_1", "component_status"] == "eligible_benchmark_premia_component"
    assert table.loc["momentum_12_1", "component_role"] == "style_premia_return_driver"
    assert table.loc["low_vol_60d", "component_status"] == "eligible_hedge_component"
    assert table.loc["low_vol_60d", "component_role"] == "hedge_or_diversifier_component"
    assert table.loc["bad_timestamp_factor", "component_status"] == "blocked_component"
    assert bool(table.loc["bad_timestamp_factor", "portfolio_validation_allowed"]) is False
    assert table["alpha_claim_allowed"].eq(False).all()

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert summary["schema_version"] == "portfolio_component_gate_summary.v1"
    assert summary["component_candidate_count"] == 3
    assert summary["blocked_component_count"] == 1
    assert summary["portfolio_validation_mode"] == "diagnostic_ensemble_only"
    assert summary["non_claims"]["production_approval"] is False

    report = Path(result.report_path).read_text(encoding="utf-8").lower()
    assert "portfolio component gate" in report
    assert "does not require standalone clean residual alpha" in report
    assert "diagnostic ensemble" in report
    assert "does not promote" in report
