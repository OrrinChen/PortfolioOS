from __future__ import annotations

from portfolio_os.explain import (
    DecisionExplanation,
    explain_promotion_decision,
    explain_q2_unavailable,
    explain_rejection_reason,
    render_explanations_table,
)


FORBIDDEN_OUTPUT_TERMS = {
    "orders",
    "broker",
    "live performance",
    "realized return",
    "alpha performance",
}


def test_forward_return_leakage_has_critical_explanation() -> None:
    explanation = explain_rejection_reason(
        "forward-return leakage in required_columns: realized_forward_return_5d"
    )

    assert explanation.primary_reason == "forward_return_feature_leakage"
    assert explanation.severity == "critical"
    assert "future return" in explanation.human_readable
    assert _safe_text(explanation)


def test_timestamp_and_anchor_rejections_are_deterministic() -> None:
    missing_timestamp = explain_rejection_reason(
        "timestamp/PIT safety checks failed: signal_timestamp_present"
    )
    unsafe_anchor = explain_rejection_reason(
        "timestamp/PIT safety checks failed: anchor_trade_date before signal timestamp"
    )

    assert missing_timestamp.primary_reason == "missing_timestamp"
    assert unsafe_anchor.primary_reason == "unsafe_event_anchor"
    assert missing_timestamp.to_deterministic_json() == missing_timestamp.to_deterministic_json()


def test_promotion_decision_explains_missing_coverage() -> None:
    explanations = explain_promotion_decision(
        decision="needs_more_evidence",
        reasons=["coverage_requirements must be declared before Q2 promotion"],
    )

    assert [item.primary_reason for item in explanations] == ["insufficient_coverage_evidence"]
    assert explanations[0].decision == "needs_more_evidence"
    assert explanations[0].severity == "warning"


def test_q2_unavailable_rows_can_carry_structured_explanation() -> None:
    explanation = explain_q2_unavailable(
        "PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true "
        "to execute the configured backtest adapter explicitly."
    )

    assert explanation.primary_reason == "q2_adapter_unavailable"
    assert explanation.decision == "unavailable"
    assert explanation.severity == "info"
    assert "explicitly enabled" in explanation.fix_hint
    assert _safe_text(explanation)


def test_explanation_table_is_deterministic() -> None:
    explanations = [
        explain_rejection_reason("forward-return leakage in required_columns"),
        explain_q2_unavailable("No stable PortfolioOS adapter exists yet for this layer."),
    ]

    rendered = render_explanations_table(explanations)

    assert "| decision | primary_reason | severity | human_readable | fix_hint |" in rendered
    assert "forward_return_feature_leakage" in rendered
    assert "q2_adapter_unavailable" in rendered


def _safe_text(explanation: DecisionExplanation) -> bool:
    text = explanation.to_deterministic_json().lower()
    return all(term not in text for term in FORBIDDEN_OUTPUT_TERMS)
