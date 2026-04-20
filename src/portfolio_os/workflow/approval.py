"""Approval, freeze, and handoff helpers for scenario outputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from shutil import copy2
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, model_validator

from portfolio_os.compliance.findings import suggest_blocking_action
from portfolio_os.data.loaders import read_yaml
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.domain.models import ComplianceFinding
from portfolio_os.storage.snapshots import file_metadata


APPROVAL_REQUIRED_SCENARIO_FILES = {
    "orders": "orders.csv",
    "orders_oms": "orders_oms.csv",
    "audit": "audit.json",
    "summary": "summary.md",
}

FINAL_FILE_NAMES = {
    "orders": "final_orders.csv",
    "orders_oms": "final_orders_oms.csv",
    "audit": "final_audit.json",
    "summary": "final_summary.md",
}


class ApprovalHandoff(BaseModel):
    """Named handoff contacts."""

    trader: str | None = None
    reviewer: str | None = None
    compliance_contact: str | None = None


class ApprovalOverride(BaseModel):
    """Optional controlled override metadata for blocking findings."""

    enabled: bool = False
    reason: str | None = None
    override_reason_code: Literal["risk_acceptance", "data_degradation", "workflow_continuity", "exception_policy"] | None = None
    approver: str | None = None
    approved_at: str | datetime | None = None

    @model_validator(mode="after")
    def validate_enabled_payload(self) -> "ApprovalOverride":
        """Require a complete payload when override is enabled."""

        if not self.enabled:
            return self
        missing: list[str] = []
        if not str(self.reason or "").strip():
            missing.append("reason")
        if not str(self.override_reason_code or "").strip():
            missing.append("override_reason_code")
        if not str(self.approver or "").strip():
            missing.append("approver")
        if not str(self.approved_at or "").strip():
            missing.append("approved_at")
        if missing:
            raise ValueError(
                "override requires non-empty fields when enabled: "
                + ", ".join(missing)
            )
        return self


class ApprovalRequest(BaseModel):
    """Approval request payload."""

    name: str
    description: str | None = None
    scenario_output_dir: str
    selected_scenario: str | None = None
    decision_maker: str
    decision_role: str
    rationale: str
    acknowledged_warning_codes: list[str] = Field(default_factory=list)
    override: ApprovalOverride | None = None
    handoff: ApprovalHandoff | None = None
    tags: list[str] = Field(default_factory=list)


@dataclass
class ApprovalEvaluation:
    """Resolved approval request and selected scenario state."""

    request: ApprovalRequest
    request_path: Path
    scenario_output_dir: Path
    scenario_comparison_payload: dict[str, Any]
    selected_scenario_id: str
    recommended_scenario_id: str
    selected_scenario_row: dict[str, Any]
    selected_audit_payload: dict[str, Any]
    selected_summary_text: str
    selected_artifact_paths: dict[str, Path]
    warning_codes: list[str]
    blocking_finding_count: int
    blocking_action_suggestions: list[str]
    warning_finding_count: int
    approval_status: str
    unacknowledged_warning_codes: list[str]
    override_used: bool


def _resolve_request_path(path_text: str, *, request_dir: Path, cwd: Path) -> Path:
    """Resolve a request-relative or cwd-relative path."""

    raw_path = Path(path_text)
    if raw_path.is_absolute():
        return raw_path
    request_candidate = (request_dir / raw_path).resolve()
    if request_candidate.exists():
        return request_candidate
    cwd_candidate = (cwd / raw_path).resolve()
    if cwd_candidate.exists():
        return cwd_candidate
    return request_candidate


def load_approval_request(path: str | Path) -> ApprovalRequest:
    """Load and validate an approval request."""

    payload = read_yaml(path)
    try:
        return ApprovalRequest.model_validate(payload)
    except ValidationError as exc:
        raise InputValidationError(f"Invalid approval request: {exc}") from exc


def evaluate_approval_request(path: str | Path) -> ApprovalEvaluation:
    """Evaluate the selected scenario against approval rules."""

    request_path = Path(path).resolve()
    request = load_approval_request(request_path)
    request_dir = request_path.parent
    cwd = Path.cwd().resolve()
    scenario_output_dir = _resolve_request_path(
        request.scenario_output_dir,
        request_dir=request_dir,
        cwd=cwd,
    )
    comparison_path = scenario_output_dir / "scenario_comparison.json"
    if not comparison_path.exists():
        raise InputValidationError(
            f"Scenario output directory does not contain scenario_comparison.json: {scenario_output_dir}"
        )
    import json

    with comparison_path.open("r", encoding="utf-8") as handle:
        comparison_payload = json.load(handle)

    recommended_scenario_id = str(comparison_payload["labels"]["recommended_scenario"])
    selected_scenario_id = request.selected_scenario or recommended_scenario_id
    scenario_rows = {
        row["scenario_id"]: row for row in comparison_payload["scenarios"]
    }
    if selected_scenario_id not in scenario_rows:
        raise InputValidationError(
            f"Selected scenario {selected_scenario_id!r} does not exist in scenario_comparison.json."
        )
    selected_scenario_row = scenario_rows[selected_scenario_id]

    scenario_result_dir = scenario_output_dir / "scenario_results" / selected_scenario_id
    selected_artifact_paths = {
        key: scenario_result_dir / filename
        for key, filename in APPROVAL_REQUIRED_SCENARIO_FILES.items()
    }
    missing = [key for key, path_obj in selected_artifact_paths.items() if not path_obj.exists()]
    if missing:
        raise InputValidationError(
            f"Selected scenario {selected_scenario_id!r} is missing required artifact(s): {', '.join(missing)}"
        )

    with selected_artifact_paths["audit"].open("r", encoding="utf-8") as handle:
        selected_audit_payload = json.load(handle)
    selected_summary_text = selected_artifact_paths["summary"].read_text(encoding="utf-8")

    findings = selected_audit_payload.get("findings", [])
    typed_findings = [ComplianceFinding.model_validate(finding) for finding in findings]
    warning_codes = sorted({finding["code"] for finding in findings if finding["severity"] == "WARNING"})
    blocking_finding_count = sum(1 for finding in findings if bool(finding.get("blocking", False)))
    blocking_action_suggestions = sorted(
        {
            f"{finding.ticker or 'portfolio'}:{suggest_blocking_action(finding)}"
            for finding in typed_findings
            if finding.blocking
        }
    )
    warning_finding_count = sum(1 for finding in findings if finding["severity"] == "WARNING")
    acknowledged_warning_codes = set(request.acknowledged_warning_codes)
    unacknowledged_warning_codes = sorted(set(warning_codes) - acknowledged_warning_codes)
    override_enabled = bool(request.override is not None and request.override.enabled)

    if blocking_finding_count > 0 and override_enabled and unacknowledged_warning_codes:
        approval_status = "incomplete_request"
    elif blocking_finding_count > 0 and override_enabled:
        approval_status = "approved_with_override"
    elif blocking_finding_count > 0:
        approval_status = "rejected"
    elif unacknowledged_warning_codes:
        approval_status = "incomplete_request"
    else:
        approval_status = "approved"

    return ApprovalEvaluation(
        request=request,
        request_path=request_path,
        scenario_output_dir=scenario_output_dir,
        scenario_comparison_payload=comparison_payload,
        selected_scenario_id=selected_scenario_id,
        recommended_scenario_id=recommended_scenario_id,
        selected_scenario_row=selected_scenario_row,
        selected_audit_payload=selected_audit_payload,
        selected_summary_text=selected_summary_text,
        selected_artifact_paths=selected_artifact_paths,
        warning_codes=warning_codes,
        blocking_finding_count=blocking_finding_count,
        blocking_action_suggestions=blocking_action_suggestions,
        warning_finding_count=warning_finding_count,
        approval_status=approval_status,
        unacknowledged_warning_codes=unacknowledged_warning_codes,
        override_used=approval_status == "approved_with_override",
    )


def build_approval_record_payload(
    evaluation: ApprovalEvaluation,
    *,
    created_at: str,
) -> dict[str, Any]:
    """Build the structured approval record."""

    comparison_path = evaluation.scenario_output_dir / "scenario_comparison.json"
    source_hashes = {
        "approval_request": file_metadata(evaluation.request_path),
        "scenario_comparison": file_metadata(comparison_path),
        **{
            key: file_metadata(path_obj)
            for key, path_obj in evaluation.selected_artifact_paths.items()
        },
    }
    override_payload = (
        evaluation.request.override.model_dump(mode="json")
        if evaluation.request.override is not None
        else {
            "enabled": False,
            "reason": None,
            "override_reason_code": None,
            "approver": None,
            "approved_at": None,
        }
    )
    return {
        "name": evaluation.request.name,
        "description": evaluation.request.description,
        "created_at": created_at,
        "approval_status": evaluation.approval_status,
        "decision_maker": evaluation.request.decision_maker,
        "decision_role": evaluation.request.decision_role,
        "selected_scenario": evaluation.selected_scenario_id,
        "recommended_scenario": evaluation.recommended_scenario_id,
        "selected_differs_from_recommended": evaluation.selected_scenario_id != evaluation.recommended_scenario_id,
        "rationale": evaluation.request.rationale,
        "acknowledged_warning_codes": evaluation.request.acknowledged_warning_codes,
        "warning_codes_present": evaluation.warning_codes,
        "unacknowledged_warning_codes": evaluation.unacknowledged_warning_codes,
        "blocking_finding_count": evaluation.blocking_finding_count,
        "blocking_action_suggestions": evaluation.blocking_action_suggestions,
        "warning_finding_count": evaluation.warning_finding_count,
        "override_used": evaluation.override_used,
        "override": override_payload,
        "handoff": (
            evaluation.request.handoff.model_dump(mode="json")
            if evaluation.request.handoff is not None
            else {}
        ),
        "tags": evaluation.request.tags,
        "source_hashes": source_hashes,
    }


def build_approval_summary_markdown(
    evaluation: ApprovalEvaluation,
    approval_record: dict[str, Any],
) -> str:
    """Render the approval summary for PM / trader / risk / compliance."""

    selected = evaluation.selected_scenario_row
    recommended_id = evaluation.recommended_scenario_id
    recommended_row = next(
        row for row in evaluation.scenario_comparison_payload["scenarios"] if row["scenario_id"] == recommended_id
    )
    handoff = approval_record["handoff"]
    warning_text = ", ".join(evaluation.warning_codes) if evaluation.warning_codes else "none"
    unack_text = ", ".join(evaluation.unacknowledged_warning_codes) if evaluation.unacknowledged_warning_codes else "none"
    override_payload = approval_record.get("override", {})
    lines = [
        "# Approval Summary",
        "",
        "> Auxiliary decision-support tool only. Not investment advice.",
        "",
        "## Decision",
        f"- approval_status: {evaluation.approval_status}",
        f"- selected_scenario: {evaluation.selected_scenario_id}",
        f"- recommended_scenario: {evaluation.recommended_scenario_id}",
        f"- selected_differs_from_recommended: {evaluation.selected_scenario_id != evaluation.recommended_scenario_id}",
        f"- rationale: {evaluation.request.rationale}",
        f"- override_used: {evaluation.override_used}",
        "",
        "## Selected Scenario Snapshot",
        f"- scenario_label: {selected['scenario_label']}",
        f"- target_deviation_after: {selected['target_deviation_after']:.6f}",
        f"- estimated_total_cost: {selected['estimated_total_cost']:.2f}",
        f"- turnover: {selected['turnover']:.4f}",
        f"- blocked_trade_count: {selected['blocked_trade_count']}",
        f"- blocking_finding_count: {selected['blocking_finding_count']}",
        f"- blocking_action_suggestions: {', '.join(evaluation.blocking_action_suggestions) or 'none'}",
        f"- warning_finding_count: {selected['warning_finding_count']}",
        f"- key_tradeoff: {selected['tradeoff_explanation']}",
        "",
        "## Warning Acknowledgement",
        f"- warning_codes_present: {warning_text}",
        f"- acknowledged_warning_codes: {', '.join(evaluation.request.acknowledged_warning_codes) or 'none'}",
        f"- unacknowledged_warning_codes: {unack_text}",
    ]
    if bool(override_payload.get("enabled")):
        lines.extend(
            [
                "",
                "## Override",
                f"- enabled: {override_payload.get('enabled')}",
                f"- override_reason_code: {override_payload.get('override_reason_code') or 'N/A'}",
                f"- approver: {override_payload.get('approver') or 'N/A'}",
                f"- approved_at: {override_payload.get('approved_at') or 'N/A'}",
                f"- reason: {override_payload.get('reason') or 'N/A'}",
            ]
        )
    if evaluation.selected_scenario_id != evaluation.recommended_scenario_id:
        lines.extend(
            [
                "",
                "## Selected vs Recommended",
                f"- recommended_label: {recommended_row['scenario_label']}",
                f"- selected_label: {selected['scenario_label']}",
                f"- selected_tradeoff: {selected['tradeoff_explanation']}",
                f"- recommended_tradeoff: {recommended_row['tradeoff_explanation']}",
            ]
        )
    lines.extend(
        [
            "",
            "## Handoff",
            f"- trader: {handoff.get('trader') or 'N/A'}",
            f"- reviewer: {handoff.get('reviewer') or 'N/A'}",
            f"- compliance_contact: {handoff.get('compliance_contact') or 'N/A'}",
            "",
            "## Risk Statement",
            "This freeze package records an operational approval decision under the current workflow rules. "
            "It does not constitute investment advice and does not replace PM, trader, risk, or compliance oversight.",
            "",
        ]
    )
    return "\n".join(lines)


def freeze_selected_scenario(
    evaluation: ApprovalEvaluation,
    *,
    output_dir: str | Path,
    created_at: str,
) -> dict[str, Any]:
    """Copy the selected scenario artifacts into a frozen execution package."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    source_mapping = {
        key: path_obj for key, path_obj in evaluation.selected_artifact_paths.items()
    }
    final_paths = {
        key: output_path / FINAL_FILE_NAMES[key] for key in FINAL_FILE_NAMES
    }
    for key, source_path in source_mapping.items():
        copy2(source_path, final_paths[key])
    override_payload = (
        evaluation.request.override.model_dump(mode="json")
        if evaluation.request.override is not None
        else {
            "enabled": False,
            "reason": None,
            "override_reason_code": None,
            "approver": None,
            "approved_at": None,
        }
    )
    return {
        "created_at": created_at,
        "approval_status": evaluation.approval_status,
        "selected_scenario": evaluation.selected_scenario_id,
        "override_used": evaluation.override_used,
        "override": override_payload,
        "source_artifacts": {key: file_metadata(path_obj) for key, path_obj in source_mapping.items()},
        "final_artifacts": {key: file_metadata(path_obj) for key, path_obj in final_paths.items()},
    }


def build_approval_request_template_payload(
    *,
    scenario_output_dir: str | Path,
    selected_scenario: str | None = None,
) -> dict[str, Any]:
    """Build a draft approval request payload from scenario outputs."""

    scenario_dir = Path(scenario_output_dir).resolve()
    comparison_path = scenario_dir / "scenario_comparison.json"
    if not comparison_path.exists():
        raise InputValidationError(
            f"Scenario output directory does not contain scenario_comparison.json: {scenario_dir}"
        )
    import json

    with comparison_path.open("r", encoding="utf-8") as handle:
        comparison_payload = json.load(handle)
    recommended_scenario = str(comparison_payload["labels"]["recommended_scenario"])
    selected_scenario_id = selected_scenario or recommended_scenario
    scenario_ids = {row["scenario_id"] for row in comparison_payload.get("scenarios", [])}
    if selected_scenario_id not in scenario_ids:
        raise InputValidationError(
            f"Selected scenario {selected_scenario_id!r} does not exist in scenario_comparison.json."
        )

    audit_path = scenario_dir / "scenario_results" / selected_scenario_id / "audit.json"
    if not audit_path.exists():
        raise InputValidationError(
            f"Scenario audit not found for selected scenario {selected_scenario_id!r}: {audit_path}"
        )
    with audit_path.open("r", encoding="utf-8") as handle:
        audit_payload = json.load(handle)
    warnings = sorted(
        {
            str(finding.get("code"))
            for finding in audit_payload.get("findings", [])
            if str(finding.get("severity", "")).upper() == "WARNING"
        }
    )
    return {
        "name": f"approval_{selected_scenario_id}",
        "description": "Approval request template generated from scenario outputs.",
        "scenario_output_dir": str(scenario_dir),
        "selected_scenario": selected_scenario_id,
        "decision_maker": "pm_owner",
        "decision_role": "portfolio_manager",
        "rationale": "Document rationale for selecting this scenario.",
        "acknowledged_warning_codes": warnings,
        "override": {
            "enabled": False,
            "reason": None,
            "override_reason_code": None,
            "approver": None,
            "approved_at": None,
        },
        "handoff": {
            "trader": "trader_owner",
            "reviewer": "risk_owner",
            "compliance_contact": "compliance_owner",
        },
        "tags": ["approval_template", selected_scenario_id],
    }
