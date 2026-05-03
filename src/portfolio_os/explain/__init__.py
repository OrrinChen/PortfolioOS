"""Explain package."""

from portfolio_os.explain.optimizer_explainer import explain_q2_unavailable
from portfolio_os.explain.promotion_explainer import explain_promotion_decision
from portfolio_os.explain.rejection_taxonomy import (
    DecisionExplanation,
    explain_rejection_reason,
    explain_rejection_reasons,
)
from portfolio_os.explain.report_sections import render_explanations_table

__all__ = [
    "DecisionExplanation",
    "explain_promotion_decision",
    "explain_q2_unavailable",
    "explain_rejection_reason",
    "explain_rejection_reasons",
    "render_explanations_table",
]
