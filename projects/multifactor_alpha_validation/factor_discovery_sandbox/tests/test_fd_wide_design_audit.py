from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.factor_design import build_candidate_design_manifest
from factor_discovery_sandbox.fd_wide_design_audit import run_fd_wide_design_audit


def test_fd_wide_design_audit_passes_when_candidate_manifests_are_valid(tmp_path: Path) -> None:
    root = tmp_path / "outputs" / "factor_discovery"
    _write_candidate_dir(root / "research_mode" / "momentum_low_vol_candidate", "candidate_summary.json")
    _write_candidate_dir(root / "small_cap" / "family_candidates" / "quality_residual_momentum", "family_decision.json")

    result = run_fd_wide_design_audit(
        scan_roots=[root],
        output_dir=tmp_path / "audit",
        report_path=tmp_path / "report.md",
    )

    summary = json.loads(result.artifacts["audit_summary"].read_text(encoding="utf-8"))
    rows = pd.read_csv(result.artifacts["audit_table"])

    assert summary["candidate_directory_count"] == 2
    assert summary["blocker_count"] == 0
    assert summary["audit_passed"] is True
    assert rows["audit_status"].eq("pass").all()
    assert rows["not_alpha_evidence"].eq(True).all()
    assert rows["direct_q2_entry_allowed"].eq(False).all()


def test_fd_wide_design_audit_blocks_candidate_directory_missing_manifest(tmp_path: Path) -> None:
    root = tmp_path / "outputs" / "factor_discovery"
    candidate_dir = root / "research_mode" / "formula_only_candidate"
    candidate_dir.mkdir(parents=True)
    (candidate_dir / "candidate_summary.json").write_text(
        json.dumps({"candidate_id": "formula_only_candidate", "not_alpha_evidence": True}) + "\n",
        encoding="utf-8",
    )

    result = run_fd_wide_design_audit(
        scan_roots=[root],
        output_dir=tmp_path / "audit",
        report_path=tmp_path / "report.md",
    )

    summary = json.loads(result.artifacts["audit_summary"].read_text(encoding="utf-8"))
    rows = pd.read_csv(result.artifacts["audit_table"])

    assert summary["audit_passed"] is False
    assert summary["blocker_count"] == 1
    assert rows.iloc[0]["audit_status"] == "blocker"
    assert "missing_candidate_design_manifest" in rows.iloc[0]["failure_reasons"]


def test_fd_wide_design_audit_blocks_invalid_manifest(tmp_path: Path) -> None:
    root = tmp_path / "outputs" / "factor_discovery"
    candidate_dir = root / "research_mode" / "bad_manifest_candidate"
    candidate_dir.mkdir(parents=True)
    (candidate_dir / "candidate_summary.json").write_text(
        json.dumps({"candidate_id": "bad_manifest_candidate", "not_alpha_evidence": True}) + "\n",
        encoding="utf-8",
    )
    manifest = build_candidate_design_manifest(
        candidate_id="bad_manifest_candidate",
        family_id="bad_manifest_candidate",
        mechanism_family="momentum_low_vol",
    )
    manifest["design_contract"].pop("placebo_design")
    (candidate_dir / "candidate_design_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    result = run_fd_wide_design_audit(
        scan_roots=[root],
        output_dir=tmp_path / "audit",
        report_path=tmp_path / "report.md",
    )

    rows = pd.read_csv(result.artifacts["audit_table"])

    assert rows.iloc[0]["audit_status"] == "blocker"
    assert "missing_design_fields:placebo_design" in rows.iloc[0]["failure_reasons"]


def test_fd_wide_design_audit_ignores_non_candidate_directories(tmp_path: Path) -> None:
    root = tmp_path / "outputs" / "factor_discovery"
    non_candidate = root / "design_layer"
    non_candidate.mkdir(parents=True)
    (non_candidate / "factor_design_contract_validation.json").write_text("{}\n", encoding="utf-8")

    result = run_fd_wide_design_audit(
        scan_roots=[root],
        output_dir=tmp_path / "audit",
        report_path=tmp_path / "report.md",
    )

    summary = json.loads(result.artifacts["audit_summary"].read_text(encoding="utf-8"))
    rows = pd.read_csv(result.artifacts["audit_table"])

    assert summary["candidate_directory_count"] == 0
    assert summary["audit_passed"] is True
    assert rows.empty


def _write_candidate_dir(path: Path, marker_name: str) -> None:
    path.mkdir(parents=True)
    candidate_id = path.name
    manifest = build_candidate_design_manifest(
        candidate_id=candidate_id,
        family_id=candidate_id,
        mechanism_family="momentum_low_vol",
    )
    (path / marker_name).write_text(
        json.dumps({"candidate_id": candidate_id, "not_alpha_evidence": True}) + "\n",
        encoding="utf-8",
    )
    (path / "candidate_design_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
