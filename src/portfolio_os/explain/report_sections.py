"""Markdown report sections for decision explanations."""

from __future__ import annotations

from collections.abc import Iterable

from portfolio_os.explain.rejection_taxonomy import DecisionExplanation


def render_explanations_table(explanations: Iterable[DecisionExplanation]) -> str:
    """Render explanations as a deterministic markdown table."""

    lines = [
        "| decision | primary_reason | severity | human_readable | fix_hint |",
        "|---|---|---|---|---|",
    ]
    for explanation in explanations:
        lines.append(
            "| {decision} | {primary_reason} | {severity} | {human} | {hint} |".format(
                decision=_escape_cell(explanation.decision),
                primary_reason=_escape_cell(explanation.primary_reason),
                severity=_escape_cell(explanation.severity),
                human=_escape_cell(explanation.human_readable),
                hint=_escape_cell(explanation.fix_hint),
            )
        )
    return "\n".join(lines)


def _escape_cell(value: object) -> str:
    return str(value).replace("|", "\\|")
