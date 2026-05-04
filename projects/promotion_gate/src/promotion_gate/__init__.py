"""Promotion gate contracts between evidence bundles and Q2 evaluation."""

from promotion_gate.gate import (
    evaluate_promotion_candidate,
    evaluate_typed_promotion_candidate,
    evaluate_typed_promotion_candidate_from_paths,
    render_promotion_explanation_v2,
    write_promotion_v2_artifacts,
)
from promotion_gate.schema import PromotionDecision, PromotionDecisionV2, Q2InputContract, Q2InputContractV2

__all__ = [
    "PromotionDecision",
    "PromotionDecisionV2",
    "Q2InputContract",
    "Q2InputContractV2",
    "evaluate_promotion_candidate",
    "evaluate_typed_promotion_candidate",
    "evaluate_typed_promotion_candidate_from_paths",
    "render_promotion_explanation_v2",
    "write_promotion_v2_artifacts",
]
