"""Scenario comparison and decision-pack rendering."""

from __future__ import annotations

from typing import Any

from portfolio_os.simulation.scenarios import ScenarioRun


def _scenario_row_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Index scenario rows by ID."""

    return {row["scenario_id"]: row for row in payload["scenarios"]}


def render_scenario_comparison_markdown(payload: dict[str, Any]) -> str:
    """Render the client-facing scenario comparison Markdown."""

    labels = payload["labels"]
    scenario_rows = _scenario_row_map(payload)
    recommended = scenario_rows[labels["recommended_scenario"]]
    lowest_cost = scenario_rows[labels["lowest_cost_scenario"]]
    lowest_turnover = scenario_rows[labels["lowest_turnover_scenario"]]
    fewest_blocked = scenario_rows[labels["fewest_blocked_trades_scenario"]]
    best_target_fit = scenario_rows[labels["best_target_fit_scenario"]]
    cross = payload["cross_scenario_explanation"]
    diagnostics = payload.get("recommendation_diagnostics", {})
    second_best_id = diagnostics.get("second_best_scenario")
    second_best = scenario_rows.get(second_best_id, recommended)

    lines = [
        "# Scenario Comparison",
        "",
        "> Auxiliary decision-support tool only. Not investment advice.",
        "",
        "## Scenario List",
    ]
    for row in payload["scenarios"]:
        lines.append(f"- {row['scenario_id']}: {row['scenario_label']} - {row['positioning']}")

    lines.extend(
        [
            "",
            "## Scenario Table",
            "",
            "| Scenario | Score | Target Deviation | Cost | Turnover | Blocked Trades | Blocking Findings | Warning Findings |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in sorted(payload["scenarios"], key=lambda item: item["score"]):
        lines.append(
            f"| {row['scenario_id']} | {row['score']:.4f} | {row['target_deviation_after']:.6f} | "
            f"{row['estimated_total_cost']:.2f} | {row['turnover']:.4f} | {row['blocked_trade_count']} | "
            f"{row['blocking_finding_count']} | {row['warning_finding_count']} |"
        )

    lines.extend(
        [
            "",
            "## Recommended Scenario",
            f"- scenario: {recommended['scenario_id']} ({recommended['scenario_label']})",
            f"- reason: {recommended['positioning']}",
            f"- score: {recommended['score']:.4f}",
            f"- second_best: {second_best['scenario_id']} ({second_best['scenario_label']})",
            f"- score_gap_to_second: {float(diagnostics.get('score_gap_to_second', 0.0)):.4f}",
            (
                "- why_not_second_best: "
                f"{recommended['scenario_id']} keeps lower weighted blocked/cost/turnover pressure "
                "under the tie-break scoring rule."
            ),
            "",
            "## Alternatives",
            f"- lowest_cost_scenario: {lowest_cost['scenario_id']} - {lowest_cost['tradeoff_explanation']}",
            f"- lowest_turnover_scenario: {lowest_turnover['scenario_id']} - {lowest_turnover['tradeoff_explanation']}",
            f"- fewest_blocked_trades_scenario: {fewest_blocked['scenario_id']} - {fewest_blocked['tradeoff_explanation']}",
            f"- best_target_fit_scenario: {best_target_fit['scenario_id']} - {best_target_fit['tradeoff_explanation']}",
            "",
            "## Cross-Scenario Trade-Offs",
            f"- largest_varying_tickers_vs_recommended: {', '.join(cross['largest_varying_tickers_vs_recommended']) or 'none'}",
            f"- scenario_with_most_warnings: {cross['scenario_with_most_warnings']}",
            f"- scenario_with_most_blocking_findings: {cross['scenario_with_most_blocking_findings']}",
            f"- scenario_with_most_regulatory_findings: {cross['scenario_with_most_regulatory_findings']}",
            f"- tie_break_rule: {payload['scoring_rule']['tie_break']['rule']}",
            "",
            "## Conclusion",
            "The recommended scenario is the lowest-score option under the current workflow scoring rule. "
            "The alternatives remain visible so PM, trading, and risk teams can discuss cost, target fit, "
            "turnover, and blocked-trade trade-offs explicitly.",
            "",
        ]
    )
    return "\n".join(lines)


def render_decision_pack_markdown(
    payload: dict[str, Any],
    scenario_runs: list[ScenarioRun],
) -> str:
    """Render the PM / trading / risk decision pack."""

    row_by_id = _scenario_row_map(payload)
    labels = payload["labels"]
    run_by_id = {run.scenario.id: run for run in scenario_runs}

    recommended_id = labels["recommended_scenario"]
    recommended_row = row_by_id[recommended_id]
    recommended_run = run_by_id[recommended_id]
    diagnostics = payload.get("recommendation_diagnostics", {})
    second_best_id = str(diagnostics.get("second_best_scenario") or recommended_id)
    second_best_row = row_by_id.get(second_best_id, recommended_row)
    lowest_cost_row = row_by_id[labels["lowest_cost_scenario"]]
    lowest_turnover_row = row_by_id[labels["lowest_turnover_scenario"]]
    fewest_blocked_row = row_by_id[labels["fewest_blocked_trades_scenario"]]
    best_target_fit_row = row_by_id[labels["best_target_fit_scenario"]]
    summary = recommended_run.summary

    lines = [
        "# Decision Pack",
        "",
        "> Recommended scenario under the current workflow scoring rule. Not investment advice.",
        "",
        "## Recommended Scenario",
        f"- scenario: {recommended_row['scenario_id']} ({recommended_row['scenario_label']})",
        f"- why_recommended: {recommended_row['positioning']}",
        f"- score: {recommended_row['score']:.4f}",
        f"- second_best_scenario: {second_best_row['scenario_id']} ({second_best_row['scenario_label']})",
        f"- score_gap_to_second: {float(diagnostics.get('score_gap_to_second', 0.0)):.4f}",
        (
            "- why_not_second_best: "
            f"{recommended_row['scenario_id']} improves weighted blocked/cost/turnover balance "
            "under the tie-break rule, with all else visible in the comparison table."
        ),
        "",
        "## Trade-Off Versus Named Alternatives",
        f"- versus_lowest_cost: {lowest_cost_row['scenario_id']} costs {lowest_cost_row['estimated_total_cost']:.2f} versus {recommended_row['estimated_total_cost']:.2f}; {lowest_cost_row['tradeoff_explanation']}",
        f"- versus_lowest_turnover: {lowest_turnover_row['scenario_id']} turns over {lowest_turnover_row['turnover']:.4f} versus {recommended_row['turnover']:.4f}; {lowest_turnover_row['tradeoff_explanation']}",
        f"- versus_fewest_blocked: {fewest_blocked_row['scenario_id']} has {fewest_blocked_row['blocked_trade_count']} blocked trades; {fewest_blocked_row['tradeoff_explanation']}",
        f"- versus_best_target_fit: {best_target_fit_row['scenario_id']} reaches target deviation {best_target_fit_row['target_deviation_after']:.6f}; {best_target_fit_row['tradeoff_explanation']}",
        "",
        "## Major Risk Reminders",
        f"- blocking_findings: {summary['blocking_finding_count']}",
        f"- warnings: {summary['warning_count']}",
        f"- blocked_reasons: {', '.join(f'{key}={value}' for key, value in sorted(summary['blocked_reason_counts'].items())) or 'none'}",
        f"- repair_reasons: {', '.join(f'{key}={value}' for key, value in sorted(summary['repair_reason_counts'].items())) or 'none'}",
        "",
        "## Findings Summary",
        f"- category_counts: {summary['finding_category_counts']}",
        f"- severity_counts: {summary['finding_severity_counts']}",
        f"- repair_status_counts: {summary['finding_repair_status_counts']}",
        "",
        "## Decision Note",
        "Use the recommended scenario as the default discussion draft, then compare it with the named alternatives "
        "if the PM prioritizes lower cost, lower turnover, fewer blocked trades, or tighter target fit on the day.",
        "",
    ]
    return "\n".join(lines)
