"""Execution and optimizer status explanation helpers."""

from __future__ import annotations

from portfolio_os.explain.rejection_taxonomy import DecisionExplanation, explain_rejection_reason


def explain_q2_unavailable(reason: str) -> DecisionExplanation:
    """Explain why a Q2 scenario or layer is unavailable."""

    return explain_rejection_reason(reason).model_copy(
        update={
            "decision": "unavailable",
            "source": "q2_execution_matrix",
        }
    )
