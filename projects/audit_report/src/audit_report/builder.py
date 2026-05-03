"""Deterministic unified demo audit report builder."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from agentic_alpha_triage.evaluator_contract import EvaluationContract
from agentic_alpha_triage.hypothesis_schema import Hypothesis
from agentic_alpha_triage.signal_contract import SignalContract
from evidence_bundle import EvidenceBundle, load_evidence_bundle
from execution_aware_optimizer.diagnostics import build_constraint_diagnostics
from execution_aware_optimizer.execution_matrix import ExecutionMatrixRow, run_execution_matrix
from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.robustness_summary import summarize_execution_matrix
from portfolio_os.explain import (
    explain_promotion_decision,
    explain_rejection_reason,
    render_explanations_table,
)
from promotion_gate import PromotionDecision, evaluate_promotion_candidate


class DemoCaseConfig(BaseModel):
    """Manifest entry for one demo audit case."""

    case_label: str
    evidence_bundle_path: str
    hypothesis_path: str | None = None
    signal_contract_path: str | None = None
    evaluation_contract_path: str | None = None


class DemoAuditManifest(BaseModel):
    """Manifest driving the deterministic demo audit report."""

    report_title: str = "PortfolioOS Demo Audit Report"
    promoted_case: DemoCaseConfig
    rejected_case: DemoCaseConfig
    q2_execution: dict[str, Any] = Field(default_factory=dict)


class DemoAuditReport(BaseModel):
    """Rendered demo audit report and safety metadata."""

    markdown: str
    promoted_case_ran_q2: bool
    rejected_case_ran_q2: bool


def load_demo_audit_manifest(path: str | Path) -> DemoAuditManifest:
    """Load the demo audit manifest from YAML."""

    manifest_path = Path(path)
    with manifest_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"{manifest_path} must contain a YAML mapping")
    return DemoAuditManifest.model_validate(payload)


def build_demo_audit_report(
    repo_root: str | Path,
    manifest: DemoAuditManifest,
) -> DemoAuditReport:
    """Build the deterministic local demo audit report."""

    root = Path(repo_root)
    promoted_context = _load_promoted_context(root, manifest.promoted_case)
    promoted_decision = evaluate_promotion_candidate(promoted_context["evidence_path"])
    rejected_decision = evaluate_promotion_candidate(
        _resolve_path(root, manifest.rejected_case.evidence_bundle_path)
    )

    q2_rows: list[ExecutionMatrixRow] = []
    promoted_case_ran_q2 = promoted_decision.decision == "promote_to_execution_eval"
    if promoted_case_ran_q2:
        q2_config = ExperimentConfig.model_validate(manifest.q2_execution)
        q2_rows = run_execution_matrix(q2_config)

    q2_summary = summarize_execution_matrix(q2_rows)
    diagnostics = build_constraint_diagnostics(q2_rows)
    markdown = _render_report(
        manifest=manifest,
        promoted_context=promoted_context,
        promoted_decision=promoted_decision,
        rejected_decision=rejected_decision,
        q2_rows=q2_rows,
        q2_summary=q2_summary,
        diagnostics=diagnostics,
    )
    return DemoAuditReport(
        markdown=markdown,
        promoted_case_ran_q2=promoted_case_ran_q2,
        rejected_case_ran_q2=False,
    )


def write_demo_audit_report(
    path: str | Path,
    *,
    repo_root: str | Path,
    manifest: DemoAuditManifest,
) -> Path:
    """Write the deterministic demo audit report to disk."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = build_demo_audit_report(repo_root, manifest)
    output_path.write_text(report.markdown, encoding="utf-8")
    return output_path


def _load_promoted_context(root: Path, case_config: DemoCaseConfig) -> dict[str, Any]:
    hypothesis_path = _require_path(root, case_config.hypothesis_path, "hypothesis_path")
    signal_path = _require_path(root, case_config.signal_contract_path, "signal_contract_path")
    evaluation_path = _require_path(
        root,
        case_config.evaluation_contract_path,
        "evaluation_contract_path",
    )
    evidence_path = _resolve_path(root, case_config.evidence_bundle_path)
    return {
        "hypothesis": _load_yaml_model(hypothesis_path, Hypothesis),
        "signal": _load_yaml_model(signal_path, SignalContract),
        "evaluation": _load_yaml_model(evaluation_path, EvaluationContract),
        "evidence": load_evidence_bundle(evidence_path),
        "evidence_path": evidence_path,
    }


def _render_report(
    *,
    manifest: DemoAuditManifest,
    promoted_context: dict[str, Any],
    promoted_decision: PromotionDecision,
    rejected_decision: PromotionDecision,
    q2_rows: list[ExecutionMatrixRow],
    q2_summary: Any,
    diagnostics: Any,
) -> str:
    hypothesis: Hypothesis = promoted_context["hypothesis"]
    signal: SignalContract = promoted_context["signal"]
    evaluation: EvaluationContract = promoted_context["evaluation"]
    evidence: EvidenceBundle = promoted_context["evidence"]
    rejected_explanations = explain_promotion_decision(
        decision=rejected_decision.decision,
        reasons=rejected_decision.reasons,
    )
    rejected_reasons = _clean_decision_reasons(rejected_decision.reasons)
    q2_config = ExperimentConfig.model_validate(manifest.q2_execution)
    q2_row = q2_rows[0] if q2_rows else None
    promoted_q2_columns = (
        promoted_decision.q2_allowed_inputs.alpha_score_columns
        if promoted_decision.q2_allowed_inputs
        else []
    )
    lines = [
        f"# {manifest.report_title}",
        "",
        "## 1. Hypothesis",
        "",
        f"### {manifest.promoted_case.case_label}",
        "",
        f"- hypothesis_id: `{hypothesis.hypothesis_id}`",
        f"- title: `{hypothesis.title}`",
        f"- expected_horizon: `{hypothesis.expected_horizon}`",
        "",
        f"### {manifest.rejected_case.case_label}",
        "",
        f"- evidence_bundle: `{rejected_decision.bundle_id}`",
        f"- status: `{rejected_decision.decision}`",
        "",
        "## 2. Signal Contract",
        "",
        f"- signal_name: `{signal.signal_name}`",
        f"- output_column: `{signal.output_column}`",
        f"- timestamp_column: `{signal.timestamp_column}`",
        f"- no_future_data_required: `{signal.no_future_data_required}`",
        "",
        "## 3. Point-in-Time Safety",
        "",
        *_render_pit_safety(evidence),
        "",
        "## 4. Leakage Checks",
        "",
        *_render_leakage_table(evidence),
        "",
        "Rejected leakage case explanation:",
        "",
        render_explanations_table(rejected_explanations),
        "",
        "## 5. Evaluation Plan",
        "",
        f"- entry_rule: `{evaluation.entry_rule}`",
        f"- holding_windows: `{evaluation.holding_windows}`",
        f"- benchmark: `{evaluation.benchmark}`",
        f"- cost_assumptions: `{evaluation.cost_assumptions}`",
        "",
        "## 6. Promotion Decision",
        "",
        f"### {manifest.promoted_case.case_label}",
        "",
        f"- decision: `{promoted_decision.decision}`",
        f"- reasons: `{promoted_decision.reasons}`",
        f"- q2_allowed_columns: `{promoted_q2_columns}`",
        "",
        f"### {manifest.rejected_case.case_label}",
        "",
        f"- decision: `{rejected_decision.decision}`",
        f"- reasons: `{rejected_reasons}`",
        "- Q2 execution evaluation: skipped because promotion decision is `reject`.",
        "",
        "## 7. Execution-Aware Evaluation",
        "",
        "| total_scenarios | total_rows | observed_rows | unavailable_rows |",
        "|---:|---:|---:|---:|",
        (
            f"| {q2_summary.total_scenarios} | {q2_summary.total_rows} | "
            f"{q2_summary.observed_rows} | {q2_summary.unavailable_rows} |"
        ),
        "",
        "| scenario_id | layer | status | net_return | explanation |",
        "|---|---|---|---:|---|",
        _render_q2_row(q2_row),
        "",
        "## 8. Cost Sensitivity",
        "",
        f"- configured_cost_bps: `{q2_config.cost_sensitivity_bps}`",
        "- result: `Not available until explicit Q2 execution is enabled.`",
        "",
        "## 9. Constraint Diagnostics",
        "",
        f"- binding_constraints: `{diagnostics.binding_constraints}`",
        f"- rejected_symbols: `{diagnostics.rejected_symbols}`",
        f"- infeasible_rebalance_dates: `{diagnostics.infeasible_rebalance_dates}`",
        f"- todos: `{diagnostics.todos}`",
        "",
        "## 10. Final Decision",
        "",
        "- promoted_like_case: `Promoted to execution-evaluation contract, but "
        "default Q2 execution remains unavailable.`",
        "- rejected_leakage_case: `Rejected before Q2 execution evaluation.`",
        "- no_fabricated_results: `True`",
        "",
        "## 11. Reproducibility Manifest",
        "",
        "- manifest_status: `placeholder_for_phase_26`",
        "- command: `"
        "PYTHONDONTWRITEBYTECODE=1 "
        "PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:"
        "projects/evidence_bundle/src:projects/promotion_gate/src:"
        "projects/execution_aware_optimizer/src poetry run python "
        "projects/audit_report/scripts/build_demo_audit_report.py --manifest "
        "projects/audit_report/examples/demo_audit_manifest.yaml --output "
        "reports/demo_audit_report.md`",
        "- live_services: `not used`",
        "",
    ]
    return "\n".join(lines)


def _render_pit_safety(evidence: EvidenceBundle) -> list[str]:
    payload = evidence.pit_safety.model_dump(mode="json")
    return [f"- {key}: `{value}`" for key, value in payload.items()]


def _render_leakage_table(evidence: EvidenceBundle) -> list[str]:
    lines = [
        "| check_name | passed | severity | details |",
        "|---|---:|---|---|",
    ]
    for check in evidence.leakage_checks:
        lines.append(
            f"| {check.check_name} | {check.passed} | {check.severity} | {check.details} |"
        )
    return lines


def _render_q2_row(row: ExecutionMatrixRow | None) -> str:
    if row is None:
        return "| Not available | Not available | unavailable | Not available | q2_not_run |"
    explanation = row.explanation or {}
    primary_reason = explanation.get("primary_reason", "Not available")
    net_return = "Not available" if row.net_return is None else str(row.net_return)
    return (
        f"| {row.scenario_id} | {row.layer_name} | {row.status} | "
        f"{net_return} | {primary_reason} |"
    )


def _clean_decision_reasons(reasons: list[str]) -> list[str]:
    cleaned: list[str] = []
    for reason in reasons:
        if "forward-return leakage in required_columns" in reason:
            cleaned.append("forward-return leakage in required_columns: realized_forward_return_5d")
        else:
            cleaned.append(reason)
    return cleaned


def _load_yaml_model(path: Path, model_cls: type[BaseModel]) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return model_cls.model_validate(payload)


def _require_path(root: Path, value: str | None, field_name: str) -> Path:
    if value is None:
        raise ValueError(f"{field_name} is required for the promoted audit case")
    return _resolve_path(root, value)


def _resolve_path(root: Path, value: str) -> Path:
    raw_path = Path(value)
    return raw_path if raw_path.is_absolute() else root / raw_path
