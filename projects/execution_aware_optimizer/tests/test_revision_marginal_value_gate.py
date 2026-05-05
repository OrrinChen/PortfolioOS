from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from execution_aware_optimizer.revision_marginal_value_gate import (
    load_revision_marginal_value_input,
    render_revision_marginal_value_report,
    run_revision_marginal_value_gate,
    write_revision_marginal_value_artifacts,
)
from execution_aware_optimizer.revision_marginal_value_schema import (
    RevisionMarginalValueInput,
)


def _required_tests() -> list[dict[str, object]]:
    return [
        {
            "test_name": "sue_only_baseline",
            "status": "passed",
            "metric_name": "net_return",
            "metric_value": 0.010,
            "details": "Local SUE-only baseline fixture was evaluated.",
        },
        {
            "test_name": "revision_only_shadow_branch",
            "status": "passed",
            "metric_name": "net_return",
            "metric_value": 0.012,
            "details": "Revision-only shadow branch was evaluated.",
        },
        {
            "test_name": "sue_revision_equal_composite",
            "status": "passed",
            "metric_name": "net_return",
            "metric_value": 0.013,
            "details": "Equal composite branch was evaluated.",
        },
        {
            "test_name": "sue_revision_confidence_weighted_composite",
            "status": "passed",
            "metric_name": "net_return",
            "metric_value": 0.014,
            "details": "Confidence-weighted composite branch was evaluated.",
        },
        {
            "test_name": "sue_residualized_against_revision",
            "status": "passed",
            "metric_name": "residual_rank_ic_t",
            "metric_value": 3.2,
            "details": "SUE residual branch was evaluated.",
        },
        {
            "test_name": "revision_residualized_against_sue",
            "status": "passed",
            "metric_name": "residual_rank_ic_t",
            "metric_value": 2.9,
            "details": "Revision residual branch was evaluated.",
        },
        {
            "test_name": "event_overlap_coverage_overlap",
            "status": "passed",
            "metric_name": "coverage_overlap",
            "metric_value": 0.22,
            "details": "Event and coverage overlap are disclosed.",
        },
        {
            "test_name": "cost_aware_marginal_contribution",
            "status": "passed",
            "metric_name": "net_improvement",
            "metric_value": 0.004,
            "details": "Cost-aware marginal contribution was evaluated.",
        },
    ]


def _gate_input(**updates: object) -> RevisionMarginalValueInput:
    payload: dict[str, object] = {
        "run_id": "revision-marginal-value-fixture",
        "pit_source": "WRDS",
        "revision_data_source": "wrds_ibes_point_in_time",
        "horizon_type": "to_next_announcement",
        "proof_type": "event_aware_marginal_diagnostics",
        "required_test_results": _required_tests(),
        "marginal_metrics": {
            "marginal_rank_ic_t": 3.1,
            "marginal_alpha_only_t": 2.8,
            "sue_adjusted_net_improvement": 0.003,
            "cost_aware_net_improvement": 0.002,
            "turnover_delta": 0.04,
            "gross_to_net_retention": 0.82,
            "event_overlap_ratio": 0.18,
            "coverage_overlap_ratio": 0.24,
        },
        "marginal_thresholds": {
            "min_marginal_rank_ic_t": 2.0,
            "min_marginal_alpha_only_t": 2.0,
            "min_sue_adjusted_net_improvement": 0.001,
            "min_cost_aware_net_improvement": 0.001,
            "min_gross_to_net_retention": 0.50,
            "max_event_overlap_ratio": 0.75,
            "max_coverage_overlap_ratio": 0.85,
        },
        "no_network": True,
        "no_broker": True,
    }
    payload.update(updates)
    return RevisionMarginalValueInput.model_validate(payload)


def test_gate_promotes_only_when_wrds_pit_and_marginal_thresholds_pass() -> None:
    result = run_revision_marginal_value_gate(_gate_input())

    assert result.summary.gate_decision == "revision_promote_to_composite_eval"
    assert result.summary.pit_source_accepted is True
    assert result.summary.required_tests_passed is True
    assert result.summary.beats_sue_adjusted_marginal_threshold is True
    assert result.summary.composite_promotion_allowed is True
    assert result.summary.production_approval_claimed is False
    assert result.no_live_data_confirmed is True
    assert result.no_orders_confirmed is True
    assert result.no_broker_confirmed is True
    assert "raw tree importance" not in " ".join(result.decision_reasons).lower()


def test_gate_rejects_fmp_frozen_estimate_history_as_not_pit_safe() -> None:
    result = run_revision_marginal_value_gate(
        _gate_input(
            pit_source="FMP",
            revision_data_source="fmp_frozen_estimate_history",
        )
    )

    assert result.summary.gate_decision == "revision_reject_due_to_pit_or_horizon"
    assert result.summary.pit_source_accepted is False
    assert result.summary.fmp_estimate_history_rejected is True
    assert result.summary.composite_promotion_allowed is False
    assert any("WRDS" in reason for reason in result.decision_reasons)
    assert any("FMP" in reason for reason in result.decision_reasons)


def test_gate_treats_feature_importance_as_needs_more_evidence_not_proof() -> None:
    result = run_revision_marginal_value_gate(_gate_input(proof_type="raw_tree_feature_importance"))

    assert result.summary.gate_decision == "revision_needs_more_evidence"
    assert result.summary.feature_importance_rejected is True
    assert result.summary.composite_promotion_allowed is False
    assert any("feature importance" in reason.lower() for reason in result.decision_reasons)


def test_gate_archives_real_revision_when_marginal_threshold_does_not_pass() -> None:
    result = run_revision_marginal_value_gate(
        _gate_input(
            marginal_metrics={
                "marginal_rank_ic_t": 3.1,
                "marginal_alpha_only_t": 2.8,
                "sue_adjusted_net_improvement": 0.0002,
                "cost_aware_net_improvement": -0.001,
                "turnover_delta": 0.12,
                "gross_to_net_retention": 0.41,
                "event_overlap_ratio": 0.18,
                "coverage_overlap_ratio": 0.24,
            }
        )
    )

    assert result.summary.gate_decision == "revision_real_but_no_marginal_value"
    assert result.summary.required_tests_passed is True
    assert result.summary.beats_sue_adjusted_marginal_threshold is False
    assert result.summary.composite_promotion_allowed is False
    assert any("cost-aware" in reason.lower() for reason in result.decision_reasons)


def test_gate_loads_fixture_and_writes_required_artifacts(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "revision_marginal_value" / "gate_input.json"
    result = run_revision_marginal_value_gate(load_revision_marginal_value_input(fixture))

    artifacts = write_revision_marginal_value_artifacts(result, tmp_path)

    assert {
        "revision_marginal_value_summary.json",
        "sue_vs_revision_overlap.csv",
        "marginal_ic_report.json",
        "marginal_q2_report.json",
        "revision_gate_decision.json",
        "revision_marginal_value_report.md",
    } == {path.name for path in artifacts.values()}

    summary = json.loads((tmp_path / "revision_marginal_value_summary.json").read_text(encoding="utf-8"))
    assert summary["schema_version"] == "revision_marginal_value_summary.v1"
    assert summary["gate_decision"] == "revision_real_but_no_marginal_value"
    assert summary["composite_promotion_allowed"] is False

    overlap = pd.read_csv(tmp_path / "sue_vs_revision_overlap.csv")
    assert {"metric", "value", "threshold", "passed"}.issubset(overlap.columns)
    assert set(overlap["metric"]) == {"event_overlap_ratio", "coverage_overlap_ratio"}

    report = render_revision_marginal_value_report(result)
    assert "Revision Marginal-Value Gate" in report
    assert "production approval: not claimed" in report
    assert "WRDS" in report
    assert "FMP frozen estimate history is rejected" in report
    assert "broker_output" not in report
    assert "recommended_trade" not in report
    assert "production_alpha_approved" not in report
