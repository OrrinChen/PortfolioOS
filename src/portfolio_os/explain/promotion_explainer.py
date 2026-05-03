"""Promotion decision explanation helpers."""

from __future__ import annotations

from portfolio_os.explain.rejection_taxonomy import DecisionExplanation, explain_rejection_reason


def explain_promotion_decision(
    *,
    decision: str,
    reasons: list[str],
) -> list[DecisionExplanation]:
    """Explain a promotion-gate decision without importing project-specific schemas."""

    if decision == "promote_to_execution_eval":
        return [
            DecisionExplanation(
                decision=decision,
                primary_reason="promotion_ready",
                severity="info",
                human_readable="The evidence package is eligible for execution-aware evaluation.",
                fix_hint=(
                    "Pass only the allowed Q2 input contract to the execution-evaluation layer."
                ),
                source="promotion_gate",
            )
        ]

    return [
        explain_rejection_reason(reason).model_copy(
            update={
                "decision": decision,
                "source": "promotion_gate",
            }
        )
        for reason in reasons
    ]
