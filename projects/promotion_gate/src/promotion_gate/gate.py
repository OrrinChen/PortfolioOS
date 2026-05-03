"""Promotion gate logic for evidence bundles."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from evidence_bundle import EvidenceBundle, load_evidence_bundle

from promotion_gate.schema import PromotionDecision, Q2InputContract


FORBIDDEN_OUTPUT_KEYS = {
    "orders",
    "broker_output",
    "live_performance",
    "trading_recommendation",
    "hidden_q2_results",
}


def evaluate_promotion_candidate(bundle_path: str | Path) -> PromotionDecision:
    """Evaluate one local evidence bundle for possible Q2 handoff."""

    resolved_path = Path(bundle_path)
    try:
        bundle = load_evidence_bundle(resolved_path)
    except ValueError as exc:
        return PromotionDecision(
            bundle_id=resolved_path.stem,
            decision="reject",
            reasons=[str(exc)],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
        )

    forbidden_keys = _collect_keys(bundle.model_dump(mode="json")).intersection(
        FORBIDDEN_OUTPUT_KEYS
    )
    if forbidden_keys:
        return PromotionDecision(
            bundle_id=bundle.bundle_id,
            decision="reject",
            reasons=["forbidden evidence outputs present: " + ", ".join(sorted(forbidden_keys))],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
        )

    if bundle.status != "ready":
        return PromotionDecision(
            bundle_id=bundle.bundle_id,
            decision="reject",
            reasons=bundle.rejection_reasons or ["evidence bundle status is not ready"],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
        )

    if not bundle.coverage_requirements:
        return PromotionDecision(
            bundle_id=bundle.bundle_id,
            decision="needs_more_evidence",
            reasons=["coverage_requirements must be declared before Q2 promotion"],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
        )

    if not bundle.cost_assumptions:
        return PromotionDecision(
            bundle_id=bundle.bundle_id,
            decision="needs_more_evidence",
            reasons=["cost_assumptions must be declared before Q2 promotion"],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
        )

    if not _has_bounded_horizon(bundle.evaluation_horizon):
        return PromotionDecision(
            bundle_id=bundle.bundle_id,
            decision="needs_more_evidence",
            reasons=[
                "evaluation_horizon must specify a bounded day/month horizon before Q2 promotion"
            ],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
        )

    return PromotionDecision(
        bundle_id=bundle.bundle_id,
        decision="promote_to_execution_eval",
        reasons=["all promotion preconditions satisfied"],
        q2_allowed_inputs=_build_q2_input_contract(bundle),
        forbidden_outputs_checked=True,
    )


def _build_q2_input_contract(bundle: EvidenceBundle) -> Q2InputContract:
    return Q2InputContract(
        bundle_id=bundle.bundle_id,
        alpha_score_columns=[
            "date",
            "symbol",
            "alpha_score",
            "alpha_source",
            "alpha_confidence",
        ],
        direct_q2_execution_allowed=False,
    )


def _has_bounded_horizon(value: str | None) -> bool:
    if value is None:
        return False
    normalized = value.strip().lower()
    if not normalized:
        return False
    return bool(re.search(r"\b\d+\s*(trading\s*)?(day|days|month|months)\b", normalized))


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
