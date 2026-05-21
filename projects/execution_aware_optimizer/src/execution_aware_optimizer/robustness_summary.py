"""Robustness summaries for Q2 execution matrix rows."""

from __future__ import annotations

import json
from collections import Counter

from pydantic import BaseModel, Field

from execution_aware_optimizer.execution_matrix import ExecutionMatrixRow


class RobustnessSummary(BaseModel):
    """Aggregate status of an execution evaluation matrix."""

    total_scenarios: int
    total_rows: int
    observed_rows: int
    unavailable_rows: int
    unique_source_config_hashes: int
    unavailable_reason_counts: dict[str, int] = Field(default_factory=dict)

    def to_deterministic_json(self) -> str:
        """Serialize the summary with stable key ordering."""

        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def summarize_execution_matrix(rows: list[ExecutionMatrixRow]) -> RobustnessSummary:
    """Summarize observed and unavailable execution matrix rows."""

    scenario_ids = {row.scenario_id for row in rows}
    config_hashes = {row.source_config_hash for row in rows}
    unavailable_reasons = Counter(
        row.unavailable_reason or "Not available"
        for row in rows
        if row.status == "unavailable"
    )
    return RobustnessSummary(
        total_scenarios=len(scenario_ids),
        total_rows=len(rows),
        observed_rows=sum(1 for row in rows if row.status == "observed"),
        unavailable_rows=sum(1 for row in rows if row.status == "unavailable"),
        unique_source_config_hashes=len(config_hashes),
        unavailable_reason_counts=dict(sorted(unavailable_reasons.items())),
    )


def render_execution_matrix_report(
    rows: list[ExecutionMatrixRow],
    *,
    summary: RobustnessSummary | None = None,
) -> str:
    """Render markdown sections for the execution evaluation matrix."""

    summary = summary or summarize_execution_matrix(rows)
    lines = [
        "# Q2 Execution Evaluation Matrix",
        "",
        "## Scenario Coverage",
        "",
        "| total_scenarios | total_rows | observed_rows | unavailable_rows |",
        "|---:|---:|---:|---:|",
        (
            f"| {summary.total_scenarios} | {summary.total_rows} | "
            f"{summary.observed_rows} | {summary.unavailable_rows} |"
        ),
        "",
        "## Unavailable Reasons",
        "",
        "| unavailable_reason | row_count |",
        "|---|---:|",
    ]
    if summary.unavailable_reason_counts:
        for reason, count in summary.unavailable_reason_counts.items():
            lines.append(f"| {reason} | {count} |")
    else:
        lines.append("| Not available | 0 |")

    lines.extend(
        [
            "",
            "## Matrix Rows",
            "",
            (
                "| scenario_id | layer | cost_bps | participation_rate | liquidity_bucket | "
                "constraint_level | execution_mode | status | net_return | unavailable_reason |"
            ),
            "|---|---|---:|---:|---|---|---|---|---:|---|",
        ]
    )
    for row in rows:
        lines.append(
            "| {scenario} | {layer} | {cost} | {participation} | {liquidity} | "
            "{constraint} | {execution} | {status} | {net_return} | {reason} |".format(
                scenario=row.scenario_id,
                layer=row.layer_name,
                cost=row.cost_bps,
                participation=row.participation_rate,
                liquidity=row.liquidity_bucket,
                constraint=row.constraint_level,
                execution=row.execution_mode,
                status=row.status,
                net_return=_fmt_optional(row.net_return),
                reason=row.unavailable_reason or "",
            )
        )
    return "\n".join(lines)


def _fmt_optional(value: object) -> str:
    if value is None:
        return "Not available"
    return str(value)
