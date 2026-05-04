from __future__ import annotations

from pathlib import Path
from typing import Any

from evidence_bundle import load_evidence_bundle

from portfolio_os.alpha.event_evaluation import EventWindowLabel, build_event_evidence_bundle
from portfolio_os.alpha.projection import AlphaProjectionConfig, project_alpha_views_to_expected_returns
from portfolio_os.alpha.view_contract import load_alpha_view
from promotion_gate.gate import (
    evaluate_typed_promotion_candidate,
    evaluate_typed_promotion_candidate_from_paths,
    write_promotion_v2_artifacts,
)
from promotion_gate.schema import PromotionDecisionV2


REPO_ROOT = Path(__file__).resolve().parents[3]
EVIDENCE_EXAMPLES = REPO_ROOT / "projects" / "evidence_bundle" / "examples"
ALPHA_VIEW_EXAMPLES = REPO_ROOT / "projects" / "alpha_view_contract" / "examples"
FORBIDDEN_OUTPUT_KEYS = {
    "orders",
    "broker_output",
    "live_performance",
    "trading_recommendation",
    "trading_instruction",
    "hidden_q2_results",
}


def test_sue_typed_promotion_reaches_q2_input_contract_v2() -> None:
    bundle = load_evidence_bundle(EVIDENCE_EXAMPLES / "valid_bundle.yaml")
    alpha_view = load_alpha_view(ALPHA_VIEW_EXAMPLES / "event_sue_pead_view.json")
    projection = project_alpha_views_to_expected_returns(
        alpha_views=[alpha_view],
        config=AlphaProjectionConfig(
            rebalance_dates=["2025-02-10"],
            universe_symbols=["AAPL", "MSFT", "TSLA"],
            risk_horizon_days=21,
            cost_assumptions={"cost_bps": 5.0},
        ),
    )
    event_evidence = build_event_evidence_bundle(
        alpha_view=alpha_view,
        labels=[EventWindowLabel(name="sue_plus_2_to_22", start_offset_days=2, end_offset_days=22)],
        placebo_tests_required=["event_date_shift"],
        overlap_reference_family_ids=["US_ANALYST_REVISION"],
    )

    decision = evaluate_typed_promotion_candidate(
        bundle=bundle,
        alpha_view=alpha_view,
        projection_manifest=projection.alpha_projection_manifest,
        projection_diagnostics=projection.alpha_projection_diagnostics,
        alpha_abstain_report=projection.alpha_abstain_report,
        event_overlap_diagnostics=event_evidence.event_overlap_diagnostics,
    )

    assert decision.decision == "promote_to_execution_eval"
    assert decision.q2_allowed_inputs is not None
    assert decision.q2_allowed_inputs.input_type == "projected_expected_return_panel"
    assert decision.q2_allowed_inputs.expected_return_panel_artifact == "expected_return_panel.csv"
    assert decision.q2_allowed_inputs.direct_q2_execution_allowed is False
    assert decision.typed_alpha_view_checked is True
    assert decision.projection_manifest_checked is True
    assert FORBIDDEN_OUTPUT_KEYS.isdisjoint(_collect_keys(decision.model_dump(mode="json")))


def test_forward_return_alpha_view_fixture_is_rejected_by_v2_path() -> None:
    decision = evaluate_typed_promotion_candidate_from_paths(
        bundle_path=EVIDENCE_EXAMPLES / "valid_bundle.yaml",
        alpha_view_path=ALPHA_VIEW_EXAMPLES / "rejected" / "rejected_forward_return_leakage_alpha_view.json",
        projection_manifest_path=None,
    )

    assert decision.decision == "reject"
    assert decision.q2_allowed_inputs is None
    assert "forward-return leakage" in " ".join(decision.reasons)


def test_revision_typed_promotion_requires_marginal_value_disclosure() -> None:
    bundle = load_evidence_bundle(EVIDENCE_EXAMPLES / "valid_bundle.yaml")
    alpha_view = load_alpha_view(ALPHA_VIEW_EXAMPLES / "event_revision_view.json")
    projection = project_alpha_views_to_expected_returns(
        alpha_views=[alpha_view],
        config=AlphaProjectionConfig(
            rebalance_dates=["2025-04-15"],
            universe_symbols=["AAPL", "MSFT"],
            risk_horizon_days=21,
            cost_assumptions={"cost_bps": 5.0},
        ),
    )

    decision = evaluate_typed_promotion_candidate(
        bundle=bundle,
        alpha_view=alpha_view,
        projection_manifest=projection.alpha_projection_manifest,
        projection_diagnostics=projection.alpha_projection_diagnostics,
        alpha_abstain_report=projection.alpha_abstain_report,
        event_overlap_diagnostics=None,
    )

    assert decision.decision == "needs_more_evidence"
    assert decision.q2_allowed_inputs is None
    assert decision.marginal_value_disclosure_required is True
    assert decision.reasons == ["analyst revision AlphaViews require event overlap / marginal-value disclosure"]


def test_q2_input_contract_v2_forbids_direct_execution_and_trading_outputs() -> None:
    payload = {
        "alpha_view_id": "AV-US-SUE-PEAD-001",
        "bundle_id": "EB-GUIDANCE-RAISE-DRIFT-001",
        "decision": "promote_to_execution_eval",
        "forbidden_outputs_checked": True,
        "projection_manifest_checked": True,
        "q2_allowed_inputs": {
            "alpha_abstain_report_artifact": "alpha_abstain_report.json",
            "alpha_projection_diagnostics_artifact": "alpha_projection_diagnostics.json",
            "alpha_view_id": "AV-US-SUE-PEAD-001",
            "allowed_consumer": "projects/execution_aware_optimizer",
            "bundle_id": "EB-GUIDANCE-RAISE-DRIFT-001",
            "direct_q2_execution_allowed": False,
            "expected_return_panel_artifact": "expected_return_panel.csv",
            "input_type": "projected_expected_return_panel",
            "projection_manifest_hash": "abc123",
        },
        "reasons": ["all typed promotion preconditions satisfied"],
        "typed_alpha_view_checked": True,
    }

    decision = PromotionDecisionV2.model_validate(payload)

    assert decision.q2_allowed_inputs is not None
    assert decision.q2_allowed_inputs.direct_q2_execution_allowed is False
    assert FORBIDDEN_OUTPUT_KEYS.isdisjoint(_collect_keys(decision.model_dump(mode="json")))


def test_promotion_v2_artifacts_are_written_deterministically(tmp_path: Path) -> None:
    bundle = load_evidence_bundle(EVIDENCE_EXAMPLES / "valid_bundle.yaml")
    alpha_view = load_alpha_view(ALPHA_VIEW_EXAMPLES / "event_sue_pead_view.json")
    projection = project_alpha_views_to_expected_returns(
        alpha_views=[alpha_view],
        config=AlphaProjectionConfig(
            rebalance_dates=["2025-02-10"],
            universe_symbols=["AAPL", "MSFT", "TSLA"],
            risk_horizon_days=21,
            cost_assumptions={"cost_bps": 5.0},
        ),
    )
    event_evidence = build_event_evidence_bundle(
        alpha_view=alpha_view,
        labels=[EventWindowLabel(name="sue_plus_2_to_22", start_offset_days=2, end_offset_days=22)],
        placebo_tests_required=["event_date_shift"],
        overlap_reference_family_ids=["US_ANALYST_REVISION"],
    )
    decision = evaluate_typed_promotion_candidate(
        bundle=bundle,
        alpha_view=alpha_view,
        projection_manifest=projection.alpha_projection_manifest,
        projection_diagnostics=projection.alpha_projection_diagnostics,
        alpha_abstain_report=projection.alpha_abstain_report,
        event_overlap_diagnostics=event_evidence.event_overlap_diagnostics,
    )

    artifacts = write_promotion_v2_artifacts(decision, tmp_path)

    assert set(artifacts) == {
        "promotion_decision_v2.json",
        "q2_input_contract_v2.json",
        "promotion_explanation_v2.md",
    }
    assert (tmp_path / "promotion_decision_v2.json").read_text(encoding="utf-8").endswith("\n")
    explanation = (tmp_path / "promotion_explanation_v2.md").read_text(encoding="utf-8")
    assert "Promotion Gate v2" in explanation
    assert "does not run Q2" in explanation


def _collect_keys(payload: Any) -> set[str]:
    if isinstance(payload, dict):
        keys = set(payload)
        for value in payload.values():
            keys.update(_collect_keys(value))
        return keys
    if isinstance(payload, list):
        keys: set[str] = set()
        for value in payload:
            keys.update(_collect_keys(value))
        return keys
    return set()
