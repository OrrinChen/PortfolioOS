"""SUE execution-survival attribution report builder."""

from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any

from execution_aware_optimizer.sue_execution_survival_attribution_schema import (
    SueExecutionSurvivalAttribution,
    SueLayerAttribution,
)
from execution_aware_optimizer.sue_typed_q2_survival_schema import (
    SueTypedQ2SurvivalResult,
    SueTypedQ2SurvivalRow,
)
from portfolio_os.provenance.hashing import canonical_json


def build_sue_execution_survival_attribution(
    survival_result: SueTypedQ2SurvivalResult,
) -> SueExecutionSurvivalAttribution:
    """Interpret Phase 50 SUE survival rows without fabricating alpha conclusions."""

    layer_attribution = _layer_attribution(survival_result)
    decision_label = _decision_label(survival_result, layer_attribution)
    phase52_should_proceed = _phase52_should_proceed(decision_label, survival_result)
    return SueExecutionSurvivalAttribution(
        run_id=survival_result.run_id,
        decision_label=decision_label,
        primary_stop_layer=_primary_stop_layer(decision_label, survival_result),
        phase52_revision_marginal_value_should_proceed=phase52_should_proceed,
        phase52_recommendation=_phase52_recommendation(phase52_should_proceed),
        alpha_failure_detected=False,
        execution_failure_detected=_execution_failure_detected(decision_label),
        projection_sparsity_detected=decision_label == "sue_q2_projection_too_sparse",
        optimizer_failure_detected=False,
        production_approval_claimed=False,
        layer_attribution=layer_attribution,
        what_this_proves=[
            "SUE typed expected-return values can be represented and passed into the local optimizer input path.",
            "Local Q2 adapter rows can be classified as observed or unavailable without fabricating missing layers.",
            "Observed rows expose gross, net, turnover, cost drag, and gross-to-net retention where local fixture hooks exist.",
        ],
        what_this_does_not_prove=[
            "SUE alpha success is not proven by this local fixture attribution.",
            "Revision marginal value is not established by this report.",
            "Paper-stage readiness and production approval are not claimed.",
            "Unavailable intermediate hooks are not treated as zero performance.",
        ],
        limitations=[
            "Observed Q2 rows come from stable local fixture mappings, not live execution.",
            "The risk-controlled intermediate layer may remain unavailable until a stable adapter hook exists.",
            "The SUE fixture covers a small local typed projection and should be read as an integration benchmark.",
        ],
    )


def render_sue_execution_survival_attribution_report(attribution: SueExecutionSurvivalAttribution) -> str:
    """Render the Phase 51 attribution report as Markdown."""

    lines = [
        "# SUE Execution-Survival Attribution Report",
        "",
        "This report is local-only. SUE remains an integration benchmark and Q2 candidate.",
        "production approval: not claimed",
        "",
        "## Decision",
        "",
        f"- decision_label: `{attribution.decision_label}`",
        f"- primary_stop_layer: `{attribution.primary_stop_layer}`",
        f"- phase52_revision_marginal_value_should_proceed: `{str(attribution.phase52_revision_marginal_value_should_proceed).lower()}`",
        "",
        "## What This Proves",
        "",
        *_bullet_lines(attribution.what_this_proves),
        "",
        "## What This Does Not Prove",
        "",
        *_bullet_lines(attribution.what_this_does_not_prove),
        "",
        "## Attribution Layers",
        "",
        "| layer | status | observed_rows | unavailable_rows | details |",
        "|---|---|---:|---:|---|",
    ]
    for layer in attribution.layer_attribution:
        lines.append(
            "| "
            + " | ".join(
                [
                    layer.layer_name,
                    layer.status,
                    str(layer.observed_rows),
                    str(layer.unavailable_rows),
                    _escape_table(layer.details),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Alpha Failure vs Execution Failure",
            "",
            f"- alpha_failure_detected: `{str(attribution.alpha_failure_detected).lower()}`",
            f"- execution_failure_detected: `{str(attribution.execution_failure_detected).lower()}`",
            "- Interpretation: a local Q2 execution limitation is not the same thing as an alpha failure.",
            "",
            "## Projection Sparsity vs Optimizer Response",
            "",
            f"- projection_sparsity_detected: `{str(attribution.projection_sparsity_detected).lower()}`",
            f"- optimizer_failure_detected: `{str(attribution.optimizer_failure_detected).lower()}`",
            "- Interpretation: sparse or unavailable projection coverage is tracked separately from optimizer response.",
            "",
            "## Phase 52 Recommendation",
            "",
            attribution.phase52_recommendation,
            "",
            "## Limitations",
            "",
            *_bullet_lines(attribution.limitations),
            "",
            "## Safety Boundaries",
            "",
            "- no live data workflow",
            "- no broker workflow",
            "- no orders or trading instructions",
            "- no production alpha approval",
            "",
        ]
    )
    return "\n".join(lines)


def write_sue_execution_survival_attribution_artifacts(
    attribution: SueExecutionSurvivalAttribution,
    *,
    output_dir: str | Path = "outputs/sue_typed_q2_survival",
    report_path: str | Path = "reports/sue_typed_q2_survival_attribution.md",
) -> dict[str, Path]:
    """Write Phase 51 JSON and Markdown artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_destination = Path(report_path)
    report_destination.parent.mkdir(parents=True, exist_ok=True)

    json_path = output_path / "failure_attribution.json"
    json_path.write_text(canonical_json(attribution.model_dump(mode="json")) + "\n", encoding="utf-8")
    report_destination.write_text(render_sue_execution_survival_attribution_report(attribution), encoding="utf-8")
    return {"json": json_path, "report": report_destination}


def load_sue_survival_result(path: str | Path) -> SueTypedQ2SurvivalResult:
    """Load a Phase 50 result JSON artifact."""

    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return SueTypedQ2SurvivalResult.model_validate(payload)


def _layer_attribution(survival_result: SueTypedQ2SurvivalResult) -> list[SueLayerAttribution]:
    observed_rows = [row for row in survival_result.matrix_rows if row.status == "observed"]
    unavailable_rows = [row for row in survival_result.matrix_rows if row.status == "unavailable"]
    layers = [
        SueLayerAttribution(
            layer_name="evidence",
            status="passed",
            details="SUE event evidence and Promotion Gate artifacts were accepted before Phase 50.",
        ),
        SueLayerAttribution(
            layer_name="projection",
            status="observed" if survival_result.active_rebalance_count > 0 else "failed",
            details=(
                f"active_rebalance_count={survival_result.active_rebalance_count}; "
                f"active_name_count={survival_result.active_name_count}; "
                f"expected_return_used_share={survival_result.expected_return_used_share:.6f}"
            ),
        ),
        SueLayerAttribution(
            layer_name="injection",
            status="observed" if survival_result.expected_return_reached_optimizer_input else "unavailable",
            details=(
                f"injection_status={survival_result.injection_status}; "
                f"optimizer_rebalance_date={survival_result.optimizer_rebalance_date}"
            ),
        ),
        SueLayerAttribution(
            layer_name="optimizer_response",
            status="observed" if survival_result.expected_return_reached_optimizer_input else "unavailable",
            details="Phase 49 separately validated directional optimizer response to typed expected-return variants.",
        ),
        _observed_layer_summary("constraint_repair", observed_rows),
        _observed_layer_summary("cost", observed_rows),
        _observed_layer_summary("turnover", observed_rows),
        SueLayerAttribution(
            layer_name="coverage_abstain",
            status="observed" if survival_result.expected_return_used_share > 0.0 else "failed",
            details="SUE uses explicit abstain for missing coverage; no_view is not silently encoded as zero alpha.",
        ),
    ]
    if unavailable_rows:
        layers.append(
            SueLayerAttribution(
                layer_name="unavailable_local_fixture_hook",
                status="unavailable",
                details="; ".join(sorted({f"{row.layer}: {row.unavailable_reason}" for row in unavailable_rows})),
                unavailable_rows=len(unavailable_rows),
            )
        )
    return layers


def _observed_layer_summary(layer_name: str, rows: list[SueTypedQ2SurvivalRow]) -> SueLayerAttribution:
    if not rows:
        return SueLayerAttribution(
            layer_name=layer_name,
            status="unavailable",
            details="No observed local Q2 rows were available for this layer.",
        )
    return SueLayerAttribution(
        layer_name=layer_name,
        status="observed",
        details="Observed local Q2 rows expose this metric where fixture hooks exist.",
        observed_rows=len(rows),
        mean_gross_return=_mean_attr(rows, "gross_return"),
        mean_net_return=_mean_attr(rows, "net_return"),
        mean_turnover=_mean_attr(rows, "turnover"),
        mean_cost_drag=_mean_attr(rows, "cost_drag"),
        mean_gross_to_net_retention=_mean_attr(rows, "gross_to_net_retention"),
    )


def _decision_label(
    survival_result: SueTypedQ2SurvivalResult,
    layers: list[SueLayerAttribution],
) -> str:
    if survival_result.injection_status != "injected" or not survival_result.expected_return_reached_optimizer_input:
        return "sue_q2_injection_unavailable"
    if survival_result.active_rebalance_count <= 0 or survival_result.expected_return_used_share <= 0.0:
        return "sue_q2_projection_too_sparse"
    observed_rows = [row for row in survival_result.matrix_rows if row.status == "observed"]
    if not observed_rows:
        return "sue_q2_fixture_unavailable"
    full_rows = [row for row in observed_rows if row.layer == "full_execution_aware_cost_adjusted"]
    if full_rows and _mean_attr(full_rows, "gross_to_net_retention") is not None:
        retention = _mean_attr(full_rows, "gross_to_net_retention") or 0.0
        net_return = _mean_attr(full_rows, "net_return") or 0.0
        if retention < 0.8:
            return "sue_q2_observed_cost_failure"
        if net_return < -0.02:
            return "sue_q2_observed_constraint_failure"
    if any(layer.layer_name == "unavailable_local_fixture_hook" for layer in layers):
        return "sue_q2_inconclusive"
    return "sue_q2_observed_survives"


def _primary_stop_layer(decision_label: str, survival_result: SueTypedQ2SurvivalResult) -> str:
    if decision_label == "sue_q2_injection_unavailable":
        return "injection"
    if decision_label == "sue_q2_projection_too_sparse":
        return "projection"
    if decision_label == "sue_q2_fixture_unavailable":
        return "unavailable_local_fixture_hook"
    if decision_label == "sue_q2_observed_cost_failure":
        return "cost"
    if decision_label == "sue_q2_observed_constraint_failure":
        return "constraint_repair"
    if decision_label == "sue_q2_inconclusive" and survival_result.q2_unavailable_rows:
        return "unavailable_local_fixture_hook"
    return "none"


def _phase52_should_proceed(decision_label: str, survival_result: SueTypedQ2SurvivalResult) -> bool:
    return bool(
        decision_label in {"sue_q2_observed_survives", "sue_q2_inconclusive"}
        and survival_result.expected_return_reached_optimizer_input
        and survival_result.q2_observed_rows > 0
    )


def _phase52_recommendation(should_proceed: bool) -> str:
    if should_proceed:
        return (
            "Proceed to Phase 52 as a marginal-value diagnostic. This is not a production approval; "
            "it only asks whether revision adds information beyond the SUE typed benchmark."
        )
    return (
        "Do not proceed to Phase 52 until the identified SUE stop layer is closed. "
        "Revision marginal-value testing would be ambiguous before that."
    )


def _execution_failure_detected(decision_label: str) -> bool:
    return decision_label in {
        "sue_q2_observed_cost_failure",
        "sue_q2_observed_constraint_failure",
        "sue_q2_fixture_unavailable",
    }


def _mean_attr(rows: list[SueTypedQ2SurvivalRow], attr: str) -> float | None:
    values = [getattr(row, attr) for row in rows if getattr(row, attr) is not None]
    return float(mean(values)) if values else None


def _bullet_lines(items: list[str]) -> list[str]:
    return [f"- {item}" for item in items]


def _escape_table(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")
