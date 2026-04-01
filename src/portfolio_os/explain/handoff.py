"""Handoff checklist rendering for approval and execution workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from portfolio_os.execution.simulator import ExecutionSimulationResult


def _yes_no(value: bool) -> str:
    """Format a boolean as yes/no text."""

    return "yes" if value else "no"


def _source_package_label(
    *,
    approval_record: dict[str, Any] | None,
    freeze_manifest: dict[str, Any] | None,
    default_label: str,
) -> str:
    """Return the best available package-source label."""

    if approval_record is not None:
        return str(approval_record.get("selected_scenario") or default_label)
    if freeze_manifest is not None:
        return str(freeze_manifest.get("selected_scenario") or default_label)
    return default_label


def render_approval_handoff_checklist(
    evaluation,
    approval_record: dict[str, Any],
) -> str:
    """Render a handoff checklist from the approval/freeze stage."""

    selected = evaluation.selected_scenario_row
    handoff = approval_record.get("handoff", {})
    warnings_acknowledged = not evaluation.unacknowledged_warning_codes
    override_used = bool(approval_record.get("override_used", False))
    override_payload = approval_record.get("override", {})
    lines = [
        "# Handoff Checklist",
        "",
        "> Auxiliary decision-support tool only. Not investment advice.",
        "",
        "## Package",
        f"- selected_scenario: {evaluation.selected_scenario_id}",
        f"- final_package_source: {evaluation.scenario_output_dir}",
        f"- approval_status: {evaluation.approval_status}",
        f"- blocking_findings_zero: {_yes_no(evaluation.blocking_finding_count == 0)}",
        f"- blocking_findings_accepted_by_override: {_yes_no(override_used)}",
        f"- warnings_acknowledged: {_yes_no(warnings_acknowledged)}",
        f"- blocked_trade_count: {selected['blocked_trade_count']}",
        "",
        "## Override",
        f"- override_used: {_yes_no(override_used)}",
        f"- override_approver: {override_payload.get('approver') or 'N/A'}",
        f"- override_approved_at: {override_payload.get('approved_at') or 'N/A'}",
        "",
        "## Contacts",
        f"- trader: {handoff.get('trader') or 'N/A'}",
        f"- reviewer: {handoff.get('reviewer') or 'N/A'}",
        f"- compliance_contact: {handoff.get('compliance_contact') or 'N/A'}",
        "",
        "## Execution Risk",
        "- execution_simulation_available: no",
        "- partial_fill_or_unfilled_risk: pending execution simulation",
        "",
        "## Manual Checklist",
        "- [ ] Final orders reviewed by PM",
        "- [ ] Warning codes acknowledged",
        "- [ ] OMS import file reviewed",
        "- [ ] Execution simulation reviewed",
        "- [ ] Residual unfilled risk accepted",
        "",
    ]
    return "\n".join(lines)


def render_execution_handoff_checklist(
    simulation_result: ExecutionSimulationResult,
    *,
    approval_record: dict[str, Any] | None,
    freeze_manifest: dict[str, Any] | None,
    audit_payload: dict[str, Any] | None,
) -> str:
    """Render a handoff checklist after execution simulation."""

    handoff = approval_record.get("handoff", {}) if approval_record is not None else {}
    blocking_findings = (
        int(approval_record.get("blocking_finding_count", 0))
        if approval_record is not None
        else int(audit_payload.get("summary", {}).get("blocking_finding_count", 0) if audit_payload else 0)
    )
    warnings_acknowledged = (
        not bool(approval_record.get("unacknowledged_warning_codes"))
        if approval_record is not None
        else True
    )
    blocked_trade_count = int(audit_payload.get("summary", {}).get("blocked_trade_count", 0) if audit_payload else 0)
    override_used = bool(approval_record.get("override_used", False)) if approval_record is not None else False
    partial_or_unfilled_risk = (
        simulation_result.portfolio_summary.partial_fill_count > 0
        or simulation_result.portfolio_summary.unfilled_order_count > 0
    )
    lines = [
        "# Handoff Checklist",
        "",
        "> Auxiliary decision-support tool only. Not investment advice.",
        "",
        "## Package",
        f"- selected_scenario: {_source_package_label(approval_record=approval_record, freeze_manifest=freeze_manifest, default_label='frozen_package')}",
        f"- final_package_source: {simulation_result.request_metadata['artifact_dir']}",
        f"- blocking_findings_zero: {_yes_no(blocking_findings == 0)}",
        f"- blocking_findings_accepted_by_override: {_yes_no(override_used)}",
        f"- warnings_acknowledged: {_yes_no(warnings_acknowledged)}",
        f"- blocked_trade_count: {blocked_trade_count}",
        "",
        "## Execution Risk",
        "- execution_simulation_available: yes",
        f"- partial_fill_or_unfilled_risk: {_yes_no(partial_or_unfilled_risk)}",
        f"- partial_fill_count: {simulation_result.portfolio_summary.partial_fill_count}",
        f"- unfilled_order_count: {simulation_result.portfolio_summary.unfilled_order_count}",
        f"- inactive_bucket_count: {simulation_result.portfolio_summary.inactive_bucket_count}",
        f"- fill_rate: {simulation_result.portfolio_summary.fill_rate:.1%}",
        "",
        "## Contacts",
        f"- trader: {handoff.get('trader') or 'N/A'}",
        f"- reviewer: {handoff.get('reviewer') or 'N/A'}",
        f"- compliance_contact: {handoff.get('compliance_contact') or 'N/A'}",
        "",
        "## Manual Checklist",
        "- [ ] Final orders reviewed by PM",
        "- [ ] Warning codes acknowledged",
        "- [ ] OMS import file reviewed",
        "- [ ] Execution simulation reviewed",
        "- [ ] Residual unfilled risk accepted",
        "",
    ]
    return "\n".join(lines)


def load_optional_json(path: str | Path | None) -> dict[str, Any] | None:
    """Load a JSON mapping when a path exists, otherwise return None."""

    if path is None:
        return None
    file_path = Path(path)
    if not file_path.exists():
        return None
    import json

    with file_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else None
