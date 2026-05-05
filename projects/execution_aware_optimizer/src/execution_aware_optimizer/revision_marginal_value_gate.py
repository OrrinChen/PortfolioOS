"""Local-only Revision Marginal-Value Gate.

Phase 52 decides whether finalized revision evidence adds enough marginal
value beyond SUE to justify composite evaluation. It does not run live data,
place orders, call brokers, or grant production approval.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from execution_aware_optimizer.revision_marginal_value_schema import (
    RevisionGateDecision,
    RevisionMarginalMetrics,
    RevisionMarginalThresholds,
    RevisionMarginalValueInput,
    RevisionMarginalValueResult,
    RevisionMarginalValueSummary,
    RevisionOverlapRow,
    RevisionTestName,
)
from portfolio_os.provenance.hashing import canonical_json


REQUIRED_TEST_NAMES: tuple[RevisionTestName, ...] = (
    "sue_only_baseline",
    "revision_only_shadow_branch",
    "sue_revision_equal_composite",
    "sue_revision_confidence_weighted_composite",
    "sue_residualized_against_revision",
    "revision_residualized_against_sue",
    "event_overlap_coverage_overlap",
    "cost_aware_marginal_contribution",
)

FEATURE_IMPORTANCE_MARKERS = ("feature_importance", "tree_importance", "raw_tree")
FMP_MARKERS = ("fmp", "frozen_estimate_history")


def run_revision_marginal_value_gate(
    gate_input: RevisionMarginalValueInput,
) -> RevisionMarginalValueResult:
    """Create a Phase 52 gate decision from deterministic local diagnostics."""

    pit_source_accepted = gate_input.pit_source.strip().upper() == "WRDS"
    fmp_rejected = _contains_fmp_marker(gate_input.revision_data_source) or _contains_fmp_marker(gate_input.pit_source)
    horizon_accepted = gate_input.horizon_type == "to_next_announcement"
    required_tests_passed = _required_tests_passed(gate_input)
    feature_importance_rejected = _feature_importance_only(gate_input.proof_type)
    beats_threshold = _beats_marginal_thresholds(gate_input.marginal_metrics, gate_input.marginal_thresholds)

    decision, reasons = _gate_decision(
        gate_input=gate_input,
        pit_source_accepted=pit_source_accepted,
        fmp_rejected=fmp_rejected,
        horizon_accepted=horizon_accepted,
        required_tests_passed=required_tests_passed,
        feature_importance_rejected=feature_importance_rejected,
        beats_threshold=beats_threshold,
    )
    summary = RevisionMarginalValueSummary(
        run_id=gate_input.run_id,
        gate_decision=decision,
        pit_source_accepted=pit_source_accepted and not fmp_rejected,
        fmp_estimate_history_rejected=fmp_rejected,
        required_tests_passed=required_tests_passed,
        feature_importance_rejected=feature_importance_rejected,
        beats_sue_adjusted_marginal_threshold=beats_threshold,
        composite_promotion_allowed=decision == "revision_promote_to_composite_eval",
        production_approval_claimed=False,
    )
    return RevisionMarginalValueResult(
        run_id=gate_input.run_id,
        summary=summary,
        required_test_results=gate_input.required_test_results,
        marginal_metrics=gate_input.marginal_metrics,
        marginal_thresholds=gate_input.marginal_thresholds,
        overlap_rows=_overlap_rows(gate_input.marginal_metrics, gate_input.marginal_thresholds),
        decision_reasons=reasons,
        no_live_data_confirmed=True,
        no_orders_confirmed=True,
        no_broker_confirmed=True,
        local_only=True,
    )


def load_revision_marginal_value_input(path: str | Path) -> RevisionMarginalValueInput:
    """Load a Phase 52 input JSON artifact."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return RevisionMarginalValueInput.model_validate(payload)


def write_revision_marginal_value_artifacts(
    result: RevisionMarginalValueResult,
    output_dir: str | Path = "outputs/revision_marginal_value_gate",
    *,
    report_path: str | Path | None = None,
) -> dict[str, Path]:
    """Write Phase 52 JSON, CSV, and Markdown artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_destination = Path(report_path) if report_path is not None else output_path / "revision_marginal_value_report.md"
    report_destination.parent.mkdir(parents=True, exist_ok=True)

    summary_path = output_path / "revision_marginal_value_summary.json"
    overlap_path = output_path / "sue_vs_revision_overlap.csv"
    marginal_ic_path = output_path / "marginal_ic_report.json"
    marginal_q2_path = output_path / "marginal_q2_report.json"
    decision_path = output_path / "revision_gate_decision.json"

    _write_json(summary_path, result.summary.model_dump(mode="json"))
    pd.DataFrame([row.model_dump(mode="json") for row in result.overlap_rows]).to_csv(overlap_path, index=False)
    _write_json(marginal_ic_path, _marginal_ic_report(result))
    _write_json(marginal_q2_path, _marginal_q2_report(result))
    _write_json(decision_path, result.model_dump(mode="json"))
    report_destination.write_text(render_revision_marginal_value_report(result), encoding="utf-8")

    return {
        "summary": summary_path,
        "overlap": overlap_path,
        "marginal_ic": marginal_ic_path,
        "marginal_q2": marginal_q2_path,
        "decision": decision_path,
        "report": report_destination,
    }


def render_revision_marginal_value_report(result: RevisionMarginalValueResult) -> str:
    """Render a concise Markdown report for the Phase 52 gate."""

    summary = result.summary
    lines = [
        "# Revision Marginal-Value Gate",
        "",
        "This report is local-only. It decides whether revision should enter composite evaluation after SUE.",
        "production approval: not claimed",
        "",
        "## Decision",
        "",
        f"- gate_decision: `{summary.gate_decision}`",
        f"- composite_promotion_allowed: `{str(summary.composite_promotion_allowed).lower()}`",
        f"- beats_sue_adjusted_marginal_threshold: `{str(summary.beats_sue_adjusted_marginal_threshold).lower()}`",
        "",
        "## PIT Source",
        "",
        "- Required PIT source for analyst revision research: `WRDS`.",
        f"- observed_pit_source: `{_escape_inline(result_input_source(result))}`",
        "- FMP frozen estimate history is rejected as PIT-safe analyst revision data.",
        "",
        "## Required Tests",
        "",
        "| test | status | metric | value | details |",
        "|---|---|---|---:|---|",
    ]
    for test in result.required_test_results:
        value = "" if test.metric_value is None else f"{test.metric_value:.6f}"
        lines.append(
            "| "
            + " | ".join(
                [
                    test.test_name,
                    test.status,
                    test.metric_name,
                    value,
                    _escape_table(test.details),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Marginal Metrics",
            "",
            "| metric | value | threshold | passed |",
            "|---|---:|---:|---|",
            *_metric_table_lines(result),
            "",
            "## Decision Reasons",
            "",
            *_bullet_lines(result.decision_reasons),
            "",
            "## Non-Claims",
            "",
            "- no live data workflow",
            "- no broker workflow",
            "- no orders or trading instructions",
            "- no production alpha approval",
            "- no conclusion that SUE or revision is ready for paper trading",
            "",
        ]
    )
    return "\n".join(lines)


def result_input_source(result: RevisionMarginalValueResult) -> str:
    """Return a report-friendly PIT source summary from decision reasons."""

    if result.summary.pit_source_accepted:
        return "WRDS"
    if result.summary.fmp_estimate_history_rejected:
        return "FMP / frozen estimate history"
    return "not accepted"


def _gate_decision(
    *,
    gate_input: RevisionMarginalValueInput,
    pit_source_accepted: bool,
    fmp_rejected: bool,
    horizon_accepted: bool,
    required_tests_passed: bool,
    feature_importance_rejected: bool,
    beats_threshold: bool,
) -> tuple[RevisionGateDecision, list[str]]:
    reasons: list[str] = []
    if not pit_source_accepted:
        reasons.append("WRDS is required as the PIT source for analyst revision research.")
    if fmp_rejected:
        reasons.append("FMP frozen estimate history is rejected as PIT-safe analyst revision data.")
    if not horizon_accepted:
        reasons.append("Revision marginal-value evaluation requires a to-next-announcement horizon.")
    if not pit_source_accepted or fmp_rejected or not horizon_accepted:
        return "revision_reject_due_to_pit_or_horizon", reasons

    if feature_importance_rejected:
        reasons.append("Raw tree importance or feature importance is not accepted as marginal-value proof.")
        return "revision_needs_more_evidence", reasons

    missing = _missing_required_tests(gate_input)
    if missing:
        reasons.append(f"Required Phase 52 diagnostics are missing or not passed: {', '.join(missing)}.")
    if not required_tests_passed:
        reasons.append("All required SUE, revision, composite, residual, overlap, and cost-aware diagnostics must pass.")
        return "revision_needs_more_evidence", reasons

    if beats_threshold:
        reasons.append("Revision beats the SUE-adjusted marginal threshold under PIT-safe local diagnostics.")
        reasons.append("Composite evaluation may proceed, but this is not paper-stage or production approval.")
        return "revision_promote_to_composite_eval", reasons

    reasons.extend(_threshold_failure_reasons(gate_input.marginal_metrics, gate_input.marginal_thresholds))
    reasons.append("Revision remains a real shadow branch but does not clear the composite-evaluation gate.")
    return "revision_real_but_no_marginal_value", reasons


def _required_tests_passed(gate_input: RevisionMarginalValueInput) -> bool:
    passed_names = {test.test_name for test in gate_input.required_test_results if test.status == "passed"}
    return set(REQUIRED_TEST_NAMES).issubset(passed_names)


def _missing_required_tests(gate_input: RevisionMarginalValueInput) -> list[str]:
    passed_names = {test.test_name for test in gate_input.required_test_results if test.status == "passed"}
    return [name for name in REQUIRED_TEST_NAMES if name not in passed_names]


def _beats_marginal_thresholds(
    metrics: RevisionMarginalMetrics,
    thresholds: RevisionMarginalThresholds,
) -> bool:
    return (
        metrics.marginal_rank_ic_t >= thresholds.min_marginal_rank_ic_t
        and metrics.marginal_alpha_only_t >= thresholds.min_marginal_alpha_only_t
        and metrics.sue_adjusted_net_improvement >= thresholds.min_sue_adjusted_net_improvement
        and metrics.cost_aware_net_improvement >= thresholds.min_cost_aware_net_improvement
        and metrics.gross_to_net_retention >= thresholds.min_gross_to_net_retention
        and metrics.event_overlap_ratio <= thresholds.max_event_overlap_ratio
        and metrics.coverage_overlap_ratio <= thresholds.max_coverage_overlap_ratio
    )


def _threshold_failure_reasons(
    metrics: RevisionMarginalMetrics,
    thresholds: RevisionMarginalThresholds,
) -> list[str]:
    checks = [
        (
            "marginal rank IC t-stat",
            metrics.marginal_rank_ic_t,
            thresholds.min_marginal_rank_ic_t,
            ">=",
        ),
        (
            "marginal alpha-only t-stat",
            metrics.marginal_alpha_only_t,
            thresholds.min_marginal_alpha_only_t,
            ">=",
        ),
        (
            "SUE-adjusted net improvement",
            metrics.sue_adjusted_net_improvement,
            thresholds.min_sue_adjusted_net_improvement,
            ">=",
        ),
        (
            "cost-aware net improvement",
            metrics.cost_aware_net_improvement,
            thresholds.min_cost_aware_net_improvement,
            ">=",
        ),
        (
            "gross-to-net retention",
            metrics.gross_to_net_retention,
            thresholds.min_gross_to_net_retention,
            ">=",
        ),
        (
            "event overlap ratio",
            metrics.event_overlap_ratio,
            thresholds.max_event_overlap_ratio,
            "<=",
        ),
        (
            "coverage overlap ratio",
            metrics.coverage_overlap_ratio,
            thresholds.max_coverage_overlap_ratio,
            "<=",
        ),
    ]
    reasons = []
    for label, value, threshold, operator in checks:
        passed = value >= threshold if operator == ">=" else value <= threshold
        if not passed:
            reasons.append(f"{label} failed threshold: {value:.6f} {operator} {threshold:.6f}.")
    return reasons


def _overlap_rows(
    metrics: RevisionMarginalMetrics,
    thresholds: RevisionMarginalThresholds,
) -> list[RevisionOverlapRow]:
    return [
        RevisionOverlapRow(
            metric="event_overlap_ratio",
            value=metrics.event_overlap_ratio,
            threshold=thresholds.max_event_overlap_ratio,
            passed=metrics.event_overlap_ratio <= thresholds.max_event_overlap_ratio,
        ),
        RevisionOverlapRow(
            metric="coverage_overlap_ratio",
            value=metrics.coverage_overlap_ratio,
            threshold=thresholds.max_coverage_overlap_ratio,
            passed=metrics.coverage_overlap_ratio <= thresholds.max_coverage_overlap_ratio,
        ),
    ]


def _marginal_ic_report(result: RevisionMarginalValueResult) -> dict[str, Any]:
    metrics = result.marginal_metrics
    thresholds = result.marginal_thresholds
    return {
        "schema_version": "revision_marginal_ic_report.v1",
        "run_id": result.run_id,
        "marginal_rank_ic_t": metrics.marginal_rank_ic_t,
        "min_marginal_rank_ic_t": thresholds.min_marginal_rank_ic_t,
        "marginal_alpha_only_t": metrics.marginal_alpha_only_t,
        "min_marginal_alpha_only_t": thresholds.min_marginal_alpha_only_t,
        "required_tests": [test.model_dump(mode="json") for test in result.required_test_results],
    }


def _marginal_q2_report(result: RevisionMarginalValueResult) -> dict[str, Any]:
    metrics = result.marginal_metrics
    thresholds = result.marginal_thresholds
    return {
        "schema_version": "revision_marginal_q2_report.v1",
        "run_id": result.run_id,
        "sue_adjusted_net_improvement": metrics.sue_adjusted_net_improvement,
        "min_sue_adjusted_net_improvement": thresholds.min_sue_adjusted_net_improvement,
        "cost_aware_net_improvement": metrics.cost_aware_net_improvement,
        "min_cost_aware_net_improvement": thresholds.min_cost_aware_net_improvement,
        "turnover_delta": metrics.turnover_delta,
        "gross_to_net_retention": metrics.gross_to_net_retention,
        "min_gross_to_net_retention": thresholds.min_gross_to_net_retention,
        "gate_decision": result.summary.gate_decision,
        "composite_promotion_allowed": result.summary.composite_promotion_allowed,
    }


def _metric_table_lines(result: RevisionMarginalValueResult) -> list[str]:
    metrics = result.marginal_metrics
    thresholds = result.marginal_thresholds
    rows = [
        (
            "marginal_rank_ic_t",
            metrics.marginal_rank_ic_t,
            thresholds.min_marginal_rank_ic_t,
            metrics.marginal_rank_ic_t >= thresholds.min_marginal_rank_ic_t,
        ),
        (
            "marginal_alpha_only_t",
            metrics.marginal_alpha_only_t,
            thresholds.min_marginal_alpha_only_t,
            metrics.marginal_alpha_only_t >= thresholds.min_marginal_alpha_only_t,
        ),
        (
            "sue_adjusted_net_improvement",
            metrics.sue_adjusted_net_improvement,
            thresholds.min_sue_adjusted_net_improvement,
            metrics.sue_adjusted_net_improvement >= thresholds.min_sue_adjusted_net_improvement,
        ),
        (
            "cost_aware_net_improvement",
            metrics.cost_aware_net_improvement,
            thresholds.min_cost_aware_net_improvement,
            metrics.cost_aware_net_improvement >= thresholds.min_cost_aware_net_improvement,
        ),
        (
            "gross_to_net_retention",
            metrics.gross_to_net_retention,
            thresholds.min_gross_to_net_retention,
            metrics.gross_to_net_retention >= thresholds.min_gross_to_net_retention,
        ),
        (
            "event_overlap_ratio",
            metrics.event_overlap_ratio,
            thresholds.max_event_overlap_ratio,
            metrics.event_overlap_ratio <= thresholds.max_event_overlap_ratio,
        ),
        (
            "coverage_overlap_ratio",
            metrics.coverage_overlap_ratio,
            thresholds.max_coverage_overlap_ratio,
            metrics.coverage_overlap_ratio <= thresholds.max_coverage_overlap_ratio,
        ),
    ]
    return [f"| {name} | {value:.6f} | {threshold:.6f} | {str(passed).lower()} |" for name, value, threshold, passed in rows]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def _feature_importance_only(proof_type: str) -> bool:
    proof = proof_type.strip().lower()
    return any(marker in proof for marker in FEATURE_IMPORTANCE_MARKERS)


def _contains_fmp_marker(value: str) -> bool:
    normalized = value.strip().lower()
    return any(marker in normalized for marker in FMP_MARKERS)


def _bullet_lines(values: list[str]) -> list[str]:
    return [f"- {value}" for value in values]


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def _escape_inline(value: str) -> str:
    return value.replace("`", "")
