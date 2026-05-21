from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from promotion_gate import PromotionDecision, evaluate_promotion_candidate


REPO_ROOT = Path(__file__).resolve().parents[3]
EVIDENCE_EXAMPLES = REPO_ROOT / "projects" / "evidence_bundle" / "examples"
PROMOTION_EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
FORBIDDEN_OUTPUT_KEYS = {
    "orders",
    "broker_output",
    "live_performance",
    "trading_recommendation",
    "hidden_q2_results",
}


def test_valid_evidence_bundle_promotes_to_q2_input_contract() -> None:
    decision = evaluate_promotion_candidate(EVIDENCE_EXAMPLES / "valid_bundle.yaml")

    assert decision.bundle_id == "EB-GUIDANCE-RAISE-DRIFT-001"
    assert decision.decision == "promote_to_execution_eval"
    assert decision.q2_allowed_inputs is not None
    assert decision.q2_allowed_inputs.alpha_score_columns == [
        "date",
        "symbol",
        "alpha_score",
        "alpha_source",
        "alpha_confidence",
    ]
    assert decision.q2_allowed_inputs.direct_q2_execution_allowed is False
    assert decision.forbidden_outputs_checked is True
    assert FORBIDDEN_OUTPUT_KEYS.isdisjoint(_collect_keys(decision.model_dump(mode="json")))


def test_forward_leakage_bundle_is_rejected_without_q2_contract() -> None:
    decision = evaluate_promotion_candidate(
        EVIDENCE_EXAMPLES / "rejected_bundle_forward_leakage.yaml"
    )

    assert decision.decision == "reject"
    assert decision.q2_allowed_inputs is None
    assert decision.forbidden_outputs_checked is True
    assert "forward-return leakage" in " ".join(decision.reasons)


def test_bundle_without_coverage_needs_more_evidence(tmp_path: Path) -> None:
    payload = _load_yaml(EVIDENCE_EXAMPLES / "valid_bundle.yaml")
    payload["bundle_id"] = "EB-MISSING-COVERAGE"
    payload["coverage_requirements"] = []
    bundle_path = tmp_path / "missing_coverage.yaml"
    bundle_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    decision = evaluate_promotion_candidate(bundle_path)

    assert decision.decision == "needs_more_evidence"
    assert decision.q2_allowed_inputs is None
    assert decision.reasons == ["coverage_requirements must be declared before Q2 promotion"]


def test_bundle_without_cost_assumptions_needs_more_evidence(tmp_path: Path) -> None:
    payload = _load_yaml(EVIDENCE_EXAMPLES / "valid_bundle.yaml")
    payload["bundle_id"] = "EB-MISSING-COST"
    payload["cost_assumptions"] = []
    bundle_path = tmp_path / "missing_cost.yaml"
    bundle_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    decision = evaluate_promotion_candidate(bundle_path)

    assert decision.decision == "needs_more_evidence"
    assert decision.q2_allowed_inputs is None
    assert decision.reasons == ["cost_assumptions must be declared before Q2 promotion"]


def test_bundle_with_unspecified_horizon_needs_more_evidence(tmp_path: Path) -> None:
    payload = _load_yaml(EVIDENCE_EXAMPLES / "valid_bundle.yaml")
    payload["bundle_id"] = "EB-UNSPECIFIED-HORIZON"
    payload["evaluation_horizon"] = "short term"
    bundle_path = tmp_path / "unspecified_horizon.yaml"
    bundle_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    decision = evaluate_promotion_candidate(bundle_path)

    assert decision.decision == "needs_more_evidence"
    assert decision.q2_allowed_inputs is None
    assert decision.reasons == [
        "evaluation_horizon must specify a bounded day/month horizon before Q2 promotion"
    ]


@pytest.mark.parametrize(
    "example_name",
    ["promoted_guidance_raise.yaml", "rejected_leakage.yaml"],
)
def test_committed_promotion_decision_examples_validate(example_name: str) -> None:
    payload = _load_yaml(PROMOTION_EXAMPLES / example_name)

    decision = PromotionDecision.model_validate(payload)

    assert decision.bundle_id
    assert decision.forbidden_outputs_checked is True
    assert FORBIDDEN_OUTPUT_KEYS.isdisjoint(_collect_keys(decision.model_dump(mode="json")))


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    assert isinstance(payload, dict)
    return payload


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
