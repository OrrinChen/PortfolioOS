from __future__ import annotations

import json
from shutil import copytree

import pytest

from portfolio_os.workflow.approval import (
    build_approval_record_payload,
    evaluate_approval_request,
    freeze_selected_scenario,
    load_approval_request,
)


def _write_request(path, *, scenario_output_dir, selected_scenario=None, acknowledged_warning_codes=None) -> None:
    selected_line = (
        f"selected_scenario: {selected_scenario}\n" if selected_scenario is not None else ""
    )
    ack_codes = acknowledged_warning_codes or []
    if ack_codes:
        ack_block = "acknowledged_warning_codes:\n" + "\n".join(f"  - {code}" for code in ack_codes) + "\n"
    else:
        ack_block = "acknowledged_warning_codes: []\n"
    path.write_text(
        (
            "name: approval_test\n"
            "description: test request\n"
            f"scenario_output_dir: {scenario_output_dir}\n"
            f"{selected_line}"
            "decision_maker: pm_test\n"
            "decision_role: portfolio_manager\n"
            "rationale: Test approval rationale.\n"
            f"{ack_block}"
            "handoff:\n"
            "  trader: trader_test\n"
            "  reviewer: risk_test\n"
            "  compliance_contact: compliance_test\n"
        ),
        encoding="utf-8",
    )


def _write_request_with_override(
    path,
    *,
    scenario_output_dir,
    selected_scenario=None,
    acknowledged_warning_codes=None,
    override_enabled=False,
    include_override_fields=True,
) -> None:
    _write_request(
        path,
        scenario_output_dir=scenario_output_dir,
        selected_scenario=selected_scenario,
        acknowledged_warning_codes=acknowledged_warning_codes,
    )
    lines = [path.read_text(encoding="utf-8")]
    lines.append("override:")
    lines.append(f"  enabled: {'true' if override_enabled else 'false'}")
    if include_override_fields:
        lines.append("  reason: Controlled override for blocking findings in pilot run.")
        lines.append("  override_reason_code: workflow_continuity")
        lines.append("  approver: risk_officer_test")
        lines.append("  approved_at: 2026-03-24T09:30:00+00:00")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _prepare_non_blocking_warning_scenario(
    *,
    scenario_output_dir,
    tmp_path,
    selected_scenario: str = "public_conservative",
    warning_code: str = "synthetic_warning_test",
):
    scenario_copy = tmp_path / "scenario_copy_warning_only"
    copytree(scenario_output_dir, scenario_copy)
    audit_path = scenario_copy / "scenario_results" / selected_scenario / "audit.json"
    with audit_path.open("r", encoding="utf-8") as handle:
        audit_payload = json.load(handle)
    findings = [
        finding for finding in audit_payload.get("findings", []) if not bool(finding.get("blocking", False))
    ]
    findings.append(
        {
            "code": warning_code,
            "category": "risk",
            "severity": "WARNING",
            "ticker": "600519",
            "message": "Synthetic warning finding for approval acknowledgement test.",
            "rule_source": "test",
            "blocking": False,
            "repair_status": "unresolved",
            "details": {},
        }
    )
    audit_payload["findings"] = findings
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(audit_payload, handle, indent=2, ensure_ascii=False)
    return scenario_copy, warning_code


def test_approval_request_parsing(tmp_path, scenario_output_dir) -> None:
    request_path = tmp_path / "request.yaml"
    _write_request(request_path, scenario_output_dir=scenario_output_dir)

    request = load_approval_request(request_path)
    assert request.name == "approval_test"
    assert request.decision_maker == "pm_test"


def test_approval_defaults_to_recommended_scenario(tmp_path, scenario_output_dir) -> None:
    request_path = tmp_path / "request.yaml"
    _write_request(request_path, scenario_output_dir=scenario_output_dir)

    evaluation = evaluate_approval_request(request_path)
    assert evaluation.selected_scenario_id == evaluation.recommended_scenario_id


def test_approval_respects_explicit_selected_scenario(tmp_path, scenario_output_dir) -> None:
    request_path = tmp_path / "request.yaml"
    _write_request(
        request_path,
        scenario_output_dir=scenario_output_dir,
        selected_scenario="private_flexible",
    )

    evaluation = evaluate_approval_request(request_path)
    assert evaluation.selected_scenario_id == "private_flexible"


def test_approval_rejects_when_blocking_findings_exist(tmp_path, scenario_output_dir) -> None:
    scenario_copy = tmp_path / "scenario_copy"
    copytree(scenario_output_dir, scenario_copy)
    audit_path = scenario_copy / "scenario_results" / "public_conservative" / "audit.json"
    with audit_path.open("r", encoding="utf-8") as handle:
        audit_payload = json.load(handle)
    audit_payload["findings"].append(
        {
            "code": "synthetic_blocking_test",
            "category": "risk",
            "severity": "BREACH",
            "ticker": "600519",
            "message": "Synthetic blocking finding for approval test.",
            "rule_source": "test",
            "blocking": True,
            "repair_status": "unresolved",
            "details": {},
        }
    )
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(audit_payload, handle, indent=2, ensure_ascii=False)

    request_path = tmp_path / "request.yaml"
    _write_request(
        request_path,
        scenario_output_dir=scenario_copy,
        selected_scenario="public_conservative",
        acknowledged_warning_codes=[],
    )

    evaluation = evaluate_approval_request(request_path)
    assert evaluation.approval_status == "rejected"


def test_approval_is_incomplete_when_warning_acknowledgement_is_missing(tmp_path, scenario_output_dir) -> None:
    scenario_copy, warning_code = _prepare_non_blocking_warning_scenario(
        scenario_output_dir=scenario_output_dir,
        tmp_path=tmp_path,
    )
    request_path = tmp_path / "request.yaml"
    _write_request(
        request_path,
        scenario_output_dir=scenario_copy,
        selected_scenario="public_conservative",
        acknowledged_warning_codes=[],
    )

    evaluation = evaluate_approval_request(request_path)
    assert evaluation.approval_status == "incomplete_request"
    assert warning_code in evaluation.unacknowledged_warning_codes


def test_approval_can_be_approved_with_complete_acknowledgement(tmp_path, scenario_output_dir) -> None:
    scenario_copy, warning_code = _prepare_non_blocking_warning_scenario(
        scenario_output_dir=scenario_output_dir,
        tmp_path=tmp_path,
    )
    request_path = tmp_path / "request.yaml"
    _write_request(
        request_path,
        scenario_output_dir=scenario_copy,
        selected_scenario="public_conservative",
        acknowledged_warning_codes=[warning_code],
    )

    evaluation = evaluate_approval_request(request_path)
    assert evaluation.approval_status == "approved"
    approval_record = build_approval_record_payload(evaluation, created_at="2026-03-23T00:00:00+00:00")
    assert approval_record["selected_scenario"] == "public_conservative"


def test_approval_rejects_incomplete_override_payload_when_enabled(tmp_path, scenario_output_dir) -> None:
    scenario_copy = tmp_path / "scenario_copy_incomplete_override"
    copytree(scenario_output_dir, scenario_copy)
    audit_path = scenario_copy / "scenario_results" / "public_conservative" / "audit.json"
    with audit_path.open("r", encoding="utf-8") as handle:
        audit_payload = json.load(handle)
    audit_payload["findings"].append(
        {
            "code": "synthetic_blocking_test",
            "category": "risk",
            "severity": "BREACH",
            "ticker": "600519",
            "message": "Synthetic blocking finding for override validation.",
            "rule_source": "test",
            "blocking": True,
            "repair_status": "unresolved",
            "details": {},
        }
    )
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(audit_payload, handle, indent=2, ensure_ascii=False)

    request_path = tmp_path / "request_incomplete_override.yaml"
    _write_request_with_override(
        request_path,
        scenario_output_dir=scenario_copy,
        selected_scenario="public_conservative",
        acknowledged_warning_codes=[],
        override_enabled=True,
        include_override_fields=False,
    )

    with pytest.raises(Exception, match="override requires non-empty fields when enabled"):
        evaluate_approval_request(request_path)


def test_approval_supports_controlled_override_for_blocking_findings(tmp_path, scenario_output_dir) -> None:
    scenario_copy = tmp_path / "scenario_copy_override"
    copytree(scenario_output_dir, scenario_copy)
    audit_path = scenario_copy / "scenario_results" / "public_conservative" / "audit.json"
    with audit_path.open("r", encoding="utf-8") as handle:
        audit_payload = json.load(handle)
    audit_payload["findings"].append(
        {
            "code": "synthetic_blocking_test",
            "category": "risk",
            "severity": "BREACH",
            "ticker": "600519",
            "message": "Synthetic blocking finding for override flow.",
            "rule_source": "test",
            "blocking": True,
            "repair_status": "unresolved",
            "details": {},
        }
    )
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(audit_payload, handle, indent=2, ensure_ascii=False)

    request_path = tmp_path / "request_override.yaml"
    _write_request_with_override(
        request_path,
        scenario_output_dir=scenario_copy,
        selected_scenario="public_conservative",
        acknowledged_warning_codes=[],
        override_enabled=True,
    )

    evaluation = evaluate_approval_request(request_path)
    assert evaluation.approval_status == "approved_with_override"
    assert evaluation.override_used is True
    approval_record = build_approval_record_payload(evaluation, created_at="2026-03-24T00:00:00+00:00")
    assert approval_record["override_used"] is True
    assert approval_record["override"]["enabled"] is True
    assert approval_record["override"]["approver"] == "risk_officer_test"


def test_freeze_manifest_tracks_source_and_final_artifacts(tmp_path, scenario_output_dir) -> None:
    request_path = tmp_path / "request.yaml"
    freeze_dir = tmp_path / "freeze"
    _write_request(
        request_path,
        scenario_output_dir=scenario_output_dir,
        selected_scenario="public_conservative",
        acknowledged_warning_codes=[],
    )

    evaluation = evaluate_approval_request(request_path)
    freeze_manifest = freeze_selected_scenario(
        evaluation,
        output_dir=freeze_dir,
        created_at="2026-03-23T00:00:00+00:00",
    )

    assert freeze_manifest["source_artifacts"]["orders"]["sha256"]
    assert freeze_manifest["final_artifacts"]["orders"]["path"].endswith("final_orders.csv")
