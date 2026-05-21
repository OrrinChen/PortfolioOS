"""Promotion gate logic for evidence bundles."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from evidence_bundle import EvidenceBundle, load_evidence_bundle

from promotion_gate.schema import PromotionDecision, PromotionDecisionV2, Q2InputContract, Q2InputContractV2


FORBIDDEN_OUTPUT_KEYS = {
    "orders",
    "broker_output",
    "live_performance",
    "trading_recommendation",
    "trading_instruction",
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


def evaluate_typed_promotion_candidate_from_paths(
    *,
    bundle_path: str | Path,
    alpha_view_path: str | Path,
    projection_manifest_path: str | Path | None,
    projection_diagnostics_path: str | Path | None = None,
    alpha_abstain_report_path: str | Path | None = None,
    event_overlap_diagnostics_path: str | Path | None = None,
) -> PromotionDecisionV2:
    """Evaluate a typed AlphaView promotion candidate from local artifacts."""

    resolved_bundle_path = Path(bundle_path)
    try:
        bundle = load_evidence_bundle(resolved_bundle_path)
    except ValueError as exc:
        return _reject_v2(
            bundle_id=resolved_bundle_path.stem,
            alpha_view_id=None,
            reasons=[str(exc)],
            typed_alpha_view_checked=False,
            projection_manifest_checked=False,
        )

    resolved_alpha_path = Path(alpha_view_path)
    try:
        from portfolio_os.alpha.view_contract import load_alpha_view

        alpha_view = load_alpha_view(resolved_alpha_path)
    except ValueError as exc:
        return _reject_v2(
            bundle_id=bundle.bundle_id,
            alpha_view_id=resolved_alpha_path.stem,
            reasons=[str(exc)],
            typed_alpha_view_checked=False,
            projection_manifest_checked=False,
        )

    projection_manifest = _load_json_artifact(projection_manifest_path) if projection_manifest_path else {}
    projection_diagnostics = (
        _load_json_artifact(projection_diagnostics_path).get("diagnostics", [])
        if projection_diagnostics_path
        else None
    )
    alpha_abstain_report = (
        _load_json_artifact(alpha_abstain_report_path).get("abstain_report", [])
        if alpha_abstain_report_path
        else None
    )
    event_overlap_diagnostics = (
        _load_json_artifact(event_overlap_diagnostics_path) if event_overlap_diagnostics_path else None
    )
    return evaluate_typed_promotion_candidate(
        bundle=bundle,
        alpha_view=alpha_view,
        projection_manifest=projection_manifest,
        projection_diagnostics=projection_diagnostics,
        alpha_abstain_report=alpha_abstain_report,
        event_overlap_diagnostics=event_overlap_diagnostics,
    )


def evaluate_typed_promotion_candidate(
    *,
    bundle: EvidenceBundle,
    alpha_view: Any,
    projection_manifest: dict[str, Any],
    projection_diagnostics: list[dict[str, Any]] | None = None,
    alpha_abstain_report: list[dict[str, Any]] | None = None,
    event_overlap_diagnostics: dict[str, Any] | None = None,
) -> PromotionDecisionV2:
    """Evaluate EvidenceBundle + AlphaView + ProjectionManifest for Q2 v2 handoff."""

    payload = {
        "bundle": bundle.model_dump(mode="json"),
        "alpha_view": alpha_view.model_dump(mode="json"),
        "projection_manifest": projection_manifest,
        "projection_diagnostics": projection_diagnostics or [],
        "alpha_abstain_report": alpha_abstain_report or [],
        "event_overlap_diagnostics": event_overlap_diagnostics or {},
    }
    forbidden_keys = _collect_keys(payload).intersection(FORBIDDEN_OUTPUT_KEYS)
    if forbidden_keys:
        return _reject_v2(
            bundle_id=bundle.bundle_id,
            alpha_view_id=alpha_view.alpha_view_id,
            reasons=["forbidden typed promotion outputs present: " + ", ".join(sorted(forbidden_keys))],
            typed_alpha_view_checked=True,
            projection_manifest_checked=bool(projection_manifest),
        )

    base_decision = _evaluate_base_bundle_preconditions(bundle)
    if base_decision is not None:
        return PromotionDecisionV2(
            bundle_id=bundle.bundle_id,
            alpha_view_id=alpha_view.alpha_view_id,
            decision=base_decision.decision,
            reasons=base_decision.reasons,
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
            typed_alpha_view_checked=True,
            projection_manifest_checked=bool(projection_manifest),
        )

    horizon_reason = _typed_horizon_gap(alpha_view)
    if horizon_reason:
        return PromotionDecisionV2(
            bundle_id=bundle.bundle_id,
            alpha_view_id=alpha_view.alpha_view_id,
            decision="needs_more_evidence",
            reasons=[horizon_reason],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
            typed_alpha_view_checked=True,
            projection_manifest_checked=bool(projection_manifest),
        )

    if alpha_view.visibility_timestamp > alpha_view.tradable_timestamp:
        return PromotionDecisionV2(
            bundle_id=bundle.bundle_id,
            alpha_view_id=alpha_view.alpha_view_id,
            decision="reject",
            reasons=["visibility_timestamp cannot be after tradable_timestamp"],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
            typed_alpha_view_checked=True,
            projection_manifest_checked=bool(projection_manifest),
        )

    if alpha_view.abstain_policy.mode != "explicit_abstain" or alpha_view.coverage_mask.mode != "explicit_abstain":
        return PromotionDecisionV2(
            bundle_id=bundle.bundle_id,
            alpha_view_id=alpha_view.alpha_view_id,
            decision="needs_more_evidence",
            reasons=["typed promotion requires explicit abstain semantics"],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
            typed_alpha_view_checked=True,
            projection_manifest_checked=bool(projection_manifest),
        )

    manifest_reason = _projection_manifest_gap(alpha_view, projection_manifest)
    if manifest_reason:
        return PromotionDecisionV2(
            bundle_id=bundle.bundle_id,
            alpha_view_id=alpha_view.alpha_view_id,
            decision="needs_more_evidence",
            reasons=[manifest_reason],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
            typed_alpha_view_checked=True,
            projection_manifest_checked=bool(projection_manifest),
        )

    if alpha_abstain_report is None:
        return PromotionDecisionV2(
            bundle_id=bundle.bundle_id,
            alpha_view_id=alpha_view.alpha_view_id,
            decision="needs_more_evidence",
            reasons=["alpha_abstain_report must be supplied before Q2 v2 handoff"],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
            typed_alpha_view_checked=True,
            projection_manifest_checked=True,
        )

    if _requires_marginal_value_disclosure(alpha_view) and not _has_overlap_disclosure(event_overlap_diagnostics):
        return PromotionDecisionV2(
            bundle_id=bundle.bundle_id,
            alpha_view_id=alpha_view.alpha_view_id,
            decision="needs_more_evidence",
            reasons=["analyst revision AlphaViews require event overlap / marginal-value disclosure"],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
            typed_alpha_view_checked=True,
            projection_manifest_checked=True,
            marginal_value_disclosure_required=True,
        )

    return PromotionDecisionV2(
        bundle_id=bundle.bundle_id,
        alpha_view_id=alpha_view.alpha_view_id,
        decision="promote_to_execution_eval",
        reasons=["all typed promotion preconditions satisfied"],
        q2_allowed_inputs=_build_q2_input_contract_v2(bundle, alpha_view, projection_manifest),
        forbidden_outputs_checked=True,
        typed_alpha_view_checked=True,
        projection_manifest_checked=True,
    )


def _evaluate_base_bundle_preconditions(bundle: EvidenceBundle) -> PromotionDecision | None:
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
            reasons=["evaluation_horizon must specify a bounded day/month horizon before Q2 promotion"],
            q2_allowed_inputs=None,
            forbidden_outputs_checked=True,
        )
    return None


def _build_q2_input_contract_v2(
    bundle: EvidenceBundle,
    alpha_view: Any,
    projection_manifest: dict[str, Any],
) -> Q2InputContractV2:
    return Q2InputContractV2(
        bundle_id=bundle.bundle_id,
        alpha_view_id=alpha_view.alpha_view_id,
        projection_manifest_hash=str(projection_manifest["content_hash"]),
        direct_q2_execution_allowed=False,
    )


def _reject_v2(
    *,
    bundle_id: str,
    alpha_view_id: str | None,
    reasons: list[str],
    typed_alpha_view_checked: bool,
    projection_manifest_checked: bool,
) -> PromotionDecisionV2:
    return PromotionDecisionV2(
        bundle_id=bundle_id,
        alpha_view_id=alpha_view_id,
        decision="reject",
        reasons=reasons,
        q2_allowed_inputs=None,
        forbidden_outputs_checked=True,
        typed_alpha_view_checked=typed_alpha_view_checked,
        projection_manifest_checked=projection_manifest_checked,
    )


def _typed_horizon_gap(alpha_view: Any) -> str | None:
    if alpha_view.mechanism_type != "event":
        return "Promotion Gate v2 currently requires event AlphaView pilots"
    if alpha_view.horizon_type == "event_window":
        required = {"start_offset_days", "end_offset_days"}
        if not required.issubset(alpha_view.holding_window):
            return "event_window AlphaViews require start_offset_days and end_offset_days"
        return None
    if alpha_view.horizon_type == "to_next_event":
        required = {"start_offset_days", "end_event", "next_event_timestamp"}
        if not required.issubset(alpha_view.holding_window):
            return "to_next_event AlphaViews require start_offset_days, end_event, and next_event_timestamp"
        return None
    return f"unsupported AlphaView horizon_type for v2 promotion: {alpha_view.horizon_type}"


def _projection_manifest_gap(alpha_view: Any, projection_manifest: dict[str, Any]) -> str | None:
    if not projection_manifest:
        return "projection manifest must be supplied before Q2 v2 handoff"
    if projection_manifest.get("schema_version") != "alpha_projection.v2":
        return "projection manifest must use alpha_projection.v2 schema"
    if alpha_view.alpha_view_id not in set(projection_manifest.get("alpha_view_ids", [])):
        return "projection manifest does not reference the AlphaView"
    if not projection_manifest.get("content_hash"):
        return "projection manifest must include content_hash"
    if int(projection_manifest.get("panel_row_count", 0)) <= 0:
        return "projection manifest has no active expected-return rows"
    if "abstain_row_count" not in projection_manifest:
        return "projection manifest must report abstain_row_count"
    return None


def _requires_marginal_value_disclosure(alpha_view: Any) -> bool:
    family = alpha_view.family_id.upper()
    return "REVISION" in family or "ANALYST" in family


def _has_overlap_disclosure(event_overlap_diagnostics: dict[str, Any] | None) -> bool:
    if not event_overlap_diagnostics:
        return False
    references = event_overlap_diagnostics.get("reference_family_ids", [])
    return bool(references)


def _load_json_artifact(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}

    resolved_path = Path(path)
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{resolved_path} must contain a JSON object")
    return payload


def write_promotion_v2_artifacts(decision: PromotionDecisionV2, output_dir: str | Path) -> dict[str, Path]:
    """Write the standard Promotion Gate v2 artifact set."""

    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "promotion_decision_v2.json": resolved_output_dir / "promotion_decision_v2.json",
        "q2_input_contract_v2.json": resolved_output_dir / "q2_input_contract_v2.json",
        "promotion_explanation_v2.md": resolved_output_dir / "promotion_explanation_v2.md",
    }
    artifacts["promotion_decision_v2.json"].write_text(
        _dump_json(decision.model_dump(mode="json")),
        encoding="utf-8",
    )
    q2_payload: dict[str, Any]
    if decision.q2_allowed_inputs is None:
        q2_payload = {
            "available": False,
            "bundle_id": decision.bundle_id,
            "alpha_view_id": decision.alpha_view_id,
            "decision": decision.decision,
            "reasons": decision.reasons,
        }
    else:
        q2_payload = decision.q2_allowed_inputs.model_dump(mode="json")
    artifacts["q2_input_contract_v2.json"].write_text(_dump_json(q2_payload), encoding="utf-8")
    artifacts["promotion_explanation_v2.md"].write_text(render_promotion_explanation_v2(decision), encoding="utf-8")
    return artifacts


def render_promotion_explanation_v2(decision: PromotionDecisionV2) -> str:
    """Render a short human-readable Promotion Gate v2 explanation."""

    lines = [
        "# Promotion Gate v2",
        "",
        f"- bundle_id: `{decision.bundle_id}`",
        f"- alpha_view_id: `{decision.alpha_view_id or 'not_available'}`",
        f"- decision: `{decision.decision}`",
        f"- typed_alpha_view_checked: `{decision.typed_alpha_view_checked}`",
        f"- projection_manifest_checked: `{decision.projection_manifest_checked}`",
        f"- forbidden_outputs_checked: `{decision.forbidden_outputs_checked}`",
        "- boundary: Promotion Gate v2 emits a Q2 input contract only; it does not run Q2, create orders, call brokers, or report live performance.",
        "",
        "## Reasons",
        "",
    ]
    lines.extend(f"- {reason}" for reason in decision.reasons)
    if decision.q2_allowed_inputs is not None:
        lines.extend(
            [
                "",
                "## Q2 Input Contract",
                "",
                f"- input_type: `{decision.q2_allowed_inputs.input_type}`",
                f"- expected_return_panel_artifact: `{decision.q2_allowed_inputs.expected_return_panel_artifact}`",
                f"- direct_q2_execution_allowed: `{decision.q2_allowed_inputs.direct_q2_execution_allowed}`",
            ]
        )
    return "\n".join(lines) + "\n"


def _dump_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


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
