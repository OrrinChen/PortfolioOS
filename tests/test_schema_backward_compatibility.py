from __future__ import annotations

import sys
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
for project_src in (
    REPO_ROOT / "projects" / "evidence_bundle" / "src",
    REPO_ROOT / "projects" / "promotion_gate" / "src",
):
    if str(project_src) not in sys.path:
        sys.path.insert(0, str(project_src))

from evidence_bundle import load_evidence_bundle
from promotion_gate import PromotionDecision


def test_evidence_bundle_valid_example_remains_schema_compatible() -> None:
    bundle = load_evidence_bundle(REPO_ROOT / "projects" / "evidence_bundle" / "examples" / "valid_bundle.yaml")

    assert bundle.bundle_id == "EB-GUIDANCE-RAISE-DRIFT-001"
    assert bundle.status == "ready"
    assert bundle.promotion_eligibility == "eligible_for_promotion_review"
    assert {"event_available_timestamp", "signal_timestamp", "alpha_score"}.issubset(
        set(bundle.required_columns)
    )


def test_promotion_decision_example_remains_schema_compatible() -> None:
    path = REPO_ROOT / "projects" / "promotion_gate" / "examples" / "promoted_guidance_raise.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))

    decision = PromotionDecision.model_validate(payload)

    assert decision.decision == "promote_to_execution_eval"
    assert decision.forbidden_outputs_checked is True
    assert decision.q2_allowed_inputs is not None
    assert decision.q2_allowed_inputs.direct_q2_execution_allowed is False
