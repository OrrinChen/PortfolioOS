from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.failure_diagnosis_report import run_failure_diagnosis_report


def test_failure_diagnosis_report_records_stop_layers_and_qqq_guard_review(tmp_path: Path) -> None:
    input_dir = tmp_path / "risk_model"
    input_dir.mkdir()
    pd.DataFrame(
        [
            {
                "factor_id": "momentum_12_1",
                "closeout_status": "insufficient_residual_evidence",
                "decision_reason": "Full residual evidence is not stable enough.",
                "primary_blocker": "residual_not_stable_positive",
                "period_count": 36,
                "residual_positive_rate": 0.50,
                "gross_spread_mean": 0.01,
                "qqq_relative_spread_mean": -0.02,
                "beta_adjusted_spread_mean": 0.01,
                "industry_adjusted_spread_mean": 0.01,
                "style_proxy_adjusted_spread_mean": -0.001,
                "full_residual_spread_mean": -0.001,
                "style_proxy_conflict_count": 0,
                "redundancy_gate_allowed": False,
                "allocator_entry_allowed": False,
                "stop_layer": "strict_residual_closeout",
            },
            {
                "factor_id": "qqq_only_factor",
                "closeout_status": "style_proxy_conflict",
                "decision_reason": "Positive proxy residual but QQQ-relative readout is negative.",
                "primary_blocker": "benchmark_or_beta_negative",
                "period_count": 36,
                "residual_positive_rate": 0.70,
                "gross_spread_mean": 0.01,
                "qqq_relative_spread_mean": -0.02,
                "beta_adjusted_spread_mean": 0.01,
                "industry_adjusted_spread_mean": 0.01,
                "style_proxy_adjusted_spread_mean": 0.008,
                "full_residual_spread_mean": 0.008,
                "style_proxy_conflict_count": 6,
                "redundancy_gate_allowed": False,
                "allocator_entry_allowed": False,
                "stop_layer": "strict_residual_closeout",
            },
        ]
    ).to_csv(input_dir / "strict_residual_closeout_decision_table.csv", index=False)
    pd.DataFrame(
        [
            *(_period_rows("momentum_12_1", residual=-0.001, qqq_relative=-0.02, count=36)),
            *(_period_rows("qqq_only_factor", residual=0.008, qqq_relative=-0.02, count=36)),
        ]
    ).to_csv(input_dir / "factor_attribution_waterfall_by_period.csv", index=False)

    result = run_failure_diagnosis_report(input_dir, tmp_path / "diagnosis")

    diagnosis = pd.read_csv(result.factor_diagnosis_path).set_index("factor_id")
    assert result.factor_count == 2
    assert result.qqq_guard_hard_gate_recommended is False
    assert diagnosis.loc["momentum_12_1", "dominant_failure_layer"] == "residual_stability"
    assert bool(diagnosis.loc["momentum_12_1", "would_pass_without_qqq_guard"]) is False
    assert diagnosis.loc["qqq_only_factor", "dominant_failure_layer"] == "qqq_relative_guard"
    assert bool(diagnosis.loc["qqq_only_factor", "would_pass_without_qqq_guard"]) is True
    assert diagnosis.loc["qqq_only_factor", "recommended_action"] == "review_qqq_guard_not_promote"

    qqq_review = json.loads(Path(result.qqq_guard_review_path).read_text(encoding="utf-8"))
    assert qqq_review["schema_version"] == "qqq_relative_guard_review.v1"
    assert qqq_review["hard_gate_recommended_for_long_short_spread"] is False
    assert qqq_review["over_strict_as_hard_gate"] is True
    assert qqq_review["rescued_by_softening_count"] == 1
    assert qqq_review["non_claims"]["production_approval"] is False

    report = Path(result.report_path).read_text(encoding="utf-8").lower()
    assert "failure diagnosis report" in report
    assert "stop layer" in report
    assert "qqq-relative guard review" in report
    assert "over-strict as a hard gate" in report
    assert "does not promote" in report


def _period_rows(factor_id: str, residual: float, qqq_relative: float, count: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(count):
        rows.append(
            {
                "factor_id": factor_id,
                "date": f"2020-{(index % 12) + 1:02d}-28",
                "gross_spread": residual + 0.01,
                "qqq_relative_spread": qqq_relative,
                "beta_adjusted_spread": residual + 0.01,
                "industry_adjusted_spread": residual + 0.01,
                "style_proxy_adjusted_spread": residual,
                "full_residual_spread": residual,
                "waterfall_status": "proxy_residual_positive" if residual > 0 else "residual_not_positive",
            }
        )
    return rows
