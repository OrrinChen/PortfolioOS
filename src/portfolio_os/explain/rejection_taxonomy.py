"""Decision explanation taxonomy for audit-ready evaluation flows."""

from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, field_validator


ExplanationSeverity = Literal["info", "warning", "critical"]


class DecisionExplanation(BaseModel):
    """Structured explanation for a rejection, promotion, or unavailable status."""

    decision: str
    primary_reason: str
    severity: ExplanationSeverity
    human_readable: str
    fix_hint: str
    source: str = "decision_explainability"

    @field_validator("decision", "primary_reason", "human_readable", "fix_hint", "source")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    def to_deterministic_json(self) -> str:
        """Serialize the explanation with stable key ordering."""

        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def explain_rejection_reason(reason: str) -> DecisionExplanation:
    """Map one local rejection/unavailable reason into a structured explanation."""

    normalized = str(reason).strip()
    lowered = normalized.lower()

    if _looks_like_forward_return_leakage(lowered):
        return DecisionExplanation(
            decision="reject",
            primary_reason="forward_return_feature_leakage",
            severity="critical",
            human_readable=(
                "The candidate uses future return information as an input, so it is not "
                "a point-in-time signal."
            ),
            fix_hint=(
                "Remove future-return fields from the signal contract and rebuild the evidence."
            ),
        )

    if "anchor_trade_date before" in lowered or ("anchor" in lowered and "before" in lowered):
        return DecisionExplanation(
            decision="reject",
            primary_reason="unsafe_event_anchor",
            severity="critical",
            human_readable=(
                "The event anchor is earlier than the information timestamp, so the "
                "evaluation would not be point-in-time safe."
            ),
            fix_hint=(
                "Move the anchor to the first valid date after the event and signal timestamps."
            ),
        )

    if "timestamp" in lowered or "pit safety" in lowered:
        return DecisionExplanation(
            decision="reject",
            primary_reason="missing_timestamp",
            severity="critical",
            human_readable="The candidate is missing a required timestamp safety field.",
            fix_hint=(
                "Add event availability, signal, and anchor timestamp fields before evaluation."
            ),
        )

    if "coverage_requirements" in lowered or "coverage" in lowered:
        return DecisionExplanation(
            decision="needs_more_evidence",
            primary_reason="insufficient_coverage_evidence",
            severity="warning",
            human_readable="The evidence package has not declared enough coverage requirements.",
            fix_hint="Declare minimum event and benchmark coverage before promotion review.",
        )

    if "cost_assumptions" in lowered or "cost assumption" in lowered:
        return DecisionExplanation(
            decision="needs_more_evidence",
            primary_reason="missing_cost_assumptions",
            severity="warning",
            human_readable="The evidence package has not declared execution-cost assumptions.",
            fix_hint="Add explicit cost assumptions before execution-aware evaluation.",
        )

    if "evaluation_horizon" in lowered or "horizon" in lowered:
        return DecisionExplanation(
            decision="needs_more_evidence",
            primary_reason="missing_or_unbounded_horizon",
            severity="warning",
            human_readable="The evidence package does not specify a bounded evaluation horizon.",
            fix_hint="Use a bounded day or month horizon such as 5 trading days.",
        )

    if _looks_like_q2_adapter_gap(lowered):
        return DecisionExplanation(
            decision="unavailable",
            primary_reason="q2_adapter_unavailable",
            severity="info",
            human_readable=(
                "The execution-evaluation adapter did not produce an observed row for "
                "this scenario."
            ),
            fix_hint=(
                "Run only after the required adapter hook is explicitly enabled and validated."
            ),
        )

    if "gross-to-net" in lowered or "retention" in lowered or "cost drag" in lowered:
        return DecisionExplanation(
            decision="review",
            primary_reason="cost_retention_failed",
            severity="warning",
            human_readable="The candidate may lose too much gross signal after estimated costs.",
            fix_hint="Review cost sensitivity and capacity assumptions before promotion.",
        )

    if "execution risk" in lowered or "participation" in lowered or "liquidity" in lowered:
        return DecisionExplanation(
            decision="review",
            primary_reason="execution_risk_high",
            severity="warning",
            human_readable="The candidate has elevated execution or liquidity risk.",
            fix_hint="Review participation limits and liquidity buckets before promotion.",
        )

    return DecisionExplanation(
        decision="review",
        primary_reason="unclassified_decision_reason",
        severity="warning",
        human_readable="The decision reason is not yet classified by the explanation taxonomy.",
        fix_hint="Add a taxonomy entry if this reason appears in committed fixtures.",
    )


def explain_rejection_reasons(reasons: list[str]) -> list[DecisionExplanation]:
    """Explain a list of local rejection reasons deterministically."""

    return [explain_rejection_reason(reason) for reason in reasons]


def _looks_like_forward_return_leakage(value: str) -> bool:
    return (
        "forward-return" in value
        or "forward_return" in value
        or "future_return" in value
        or "fwd_ret" in value
    )


def _looks_like_q2_adapter_gap(value: str) -> bool:
    return (
        "portfolioos run disabled" in value
        or "no stable portfolioos adapter" in value
        or "not executed" in value
        or "unavailable" in value
    )
