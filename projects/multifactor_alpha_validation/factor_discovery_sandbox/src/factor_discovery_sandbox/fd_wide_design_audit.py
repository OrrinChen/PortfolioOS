"""FD-wide audit for candidate design manifests."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from factor_discovery_sandbox.factor_design import DESIGN_GUARDS, validate_design_contract


AUDIT_SCHEMA_VERSION = "fd_wide_design_manifest_audit.v1"
CANDIDATE_MANIFEST_NAME = "candidate_design_manifest.json"
CANDIDATE_MARKER_FILES = (
    "candidate_summary.json",
    "family_decision.json",
    "revision_confirmed_alpha_summary.json",
)
AUDIT_COLUMNS = (
    "schema_version",
    "stage",
    "candidate_directory",
    "marker_files",
    "candidate_id",
    "family_id",
    "mechanism_family",
    "manifest_path",
    "manifest_found",
    "manifest_schema_version",
    "schema_valid",
    "design_contract_valid",
    "candidate_validation_allowed",
    "design_layer_required_before_formula",
    "formula_is_measurement_not_thesis",
    "manifest_written_before_validation",
    "audit_status",
    "failure_reasons",
    "allocator_entry_allowed",
    "q1_entry_allowed",
    "q2_entry_allowed",
    "alpha_registry_update_allowed",
    "production_approval_claimed",
    "direct_q2_entry_allowed",
    "not_alpha_evidence",
)
BLOCKED_DECISION = "block_fd_candidate_validation_until_design_manifest_fixed"
PASS_DECISION = "all_candidate_design_manifests_valid"


@dataclass(frozen=True)
class FDWideDesignAuditResult:
    """Artifacts and summary for the FD-wide design-manifest audit."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_fd_wide_design_audit(
    scan_roots: Iterable[str | Path],
    output_dir: str | Path,
    report_path: str | Path,
) -> FDWideDesignAuditResult:
    """Scan candidate output directories for valid design manifests."""

    roots = [Path(root) for root in scan_roots]
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)

    candidate_dirs = _discover_candidate_dirs(roots)
    rows = [_audit_candidate_dir(path, marker_files) for path, marker_files in candidate_dirs]
    blocker_count = sum(1 for row in rows if row["audit_status"] == "blocker")
    manifest_found_count = sum(1 for row in rows if row["manifest_found"])
    valid_manifest_count = sum(1 for row in rows if row["audit_status"] == "pass")
    audit_passed = blocker_count == 0

    artifacts = {
        "audit_table": output_path / "fd_wide_design_manifest_audit.csv",
        "audit_summary": output_path / "fd_wide_design_manifest_audit.json",
        "audit_report": report_file,
    }
    _write_csv(artifacts["audit_table"], rows)

    summary = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "stage": "FD-D0-WIDE-AUDIT",
        "scan_roots": [str(root) for root in roots],
        "candidate_directory_count": len(rows),
        "manifest_found_count": manifest_found_count,
        "valid_manifest_count": valid_manifest_count,
        "blocker_count": blocker_count,
        "audit_passed": audit_passed,
        "decision": PASS_DECISION if audit_passed else BLOCKED_DECISION,
        "candidate_marker_files": list(CANDIDATE_MARKER_FILES),
        "required_manifest_name": CANDIDATE_MANIFEST_NAME,
        "design_layer_required_before_formula": True,
        "formula_is_measurement_not_thesis": True,
        **DESIGN_GUARDS,
    }
    artifacts["audit_summary"].write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["audit_report"].write_text(_render_report(summary, rows), encoding="utf-8")
    return FDWideDesignAuditResult(summary=summary, artifacts=artifacts)


def _discover_candidate_dirs(roots: Iterable[Path]) -> list[tuple[Path, list[str]]]:
    candidates: dict[Path, set[str]] = {}
    for root in roots:
        if not root.exists():
            continue
        for marker_name in CANDIDATE_MARKER_FILES:
            for marker in root.rglob(marker_name):
                candidates.setdefault(marker.parent, set()).add(marker.name)
    return [(path, sorted(markers)) for path, markers in sorted(candidates.items(), key=lambda item: str(item[0]))]


def _audit_candidate_dir(candidate_dir: Path, marker_files: list[str]) -> dict[str, object]:
    manifest_path = candidate_dir / CANDIDATE_MANIFEST_NAME
    failures: list[str] = []
    manifest: Mapping[str, object] = {}
    manifest_found = manifest_path.exists()
    manifest_schema_version = ""
    schema_valid = False
    contract_valid = False
    candidate_validation_allowed = False
    design_layer_required = False
    formula_measurement_boundary = False
    manifest_written_before_validation = False

    if not manifest_found:
        failures.append("missing_candidate_design_manifest")
    else:
        try:
            loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            loaded = {}
            failures.append("invalid_candidate_design_manifest_json")
        if not isinstance(loaded, Mapping):
            loaded = {}
            failures.append("candidate_design_manifest_not_object")
        manifest = loaded
        manifest_schema_version = str(manifest.get("schema_version", ""))
        schema_valid = manifest.get("schema_version") == "fd_candidate_design_manifest.v1"
        if not schema_valid:
            failures.append("invalid_candidate_design_manifest_schema")

        validation = validate_design_contract({"factor_id": manifest.get("candidate_id", candidate_dir.name), **manifest})
        contract_valid = bool(validation["valid"]) and manifest.get("design_contract_valid") is True
        candidate_validation_allowed = manifest.get("candidate_validation_allowed") is True
        design_layer_required = manifest.get("design_layer_required_before_formula") is True
        formula_measurement_boundary = manifest.get("formula_is_measurement_not_thesis") is True
        manifest_written_before_validation = manifest.get("manifest_written_before_validation") is True
        failures.extend(str(reason) for reason in validation["failure_reasons"])
        if not contract_valid:
            failures.append("design_contract_invalid")
        if not candidate_validation_allowed:
            failures.append("candidate_validation_not_allowed_by_manifest")
        if not design_layer_required:
            failures.append("design_layer_required_before_formula_missing")
        if not formula_measurement_boundary:
            failures.append("formula_measurement_boundary_missing")
        if not manifest_written_before_validation:
            failures.append("manifest_written_before_validation_missing")
        failures.extend(_guard_failures(manifest))

    failures = sorted(set(failures))
    status = "blocker" if failures else "pass"
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "stage": "FD-D0-WIDE-AUDIT",
        "candidate_directory": str(candidate_dir),
        "marker_files": "|".join(marker_files),
        "candidate_id": str(manifest.get("candidate_id", candidate_dir.name)),
        "family_id": str(manifest.get("family_id", manifest.get("candidate_id", candidate_dir.name))),
        "mechanism_family": str(manifest.get("mechanism_family", "")),
        "manifest_path": str(manifest_path),
        "manifest_found": manifest_found,
        "manifest_schema_version": manifest_schema_version,
        "schema_valid": schema_valid,
        "design_contract_valid": contract_valid,
        "candidate_validation_allowed": candidate_validation_allowed,
        "design_layer_required_before_formula": design_layer_required,
        "formula_is_measurement_not_thesis": formula_measurement_boundary,
        "manifest_written_before_validation": manifest_written_before_validation,
        "audit_status": status,
        "failure_reasons": "|".join(failures),
        **DESIGN_GUARDS,
    }


def _guard_failures(manifest: Mapping[str, object]) -> list[str]:
    failures: list[str] = []
    for key, expected in DESIGN_GUARDS.items():
        if manifest.get(key) is not expected:
            failures.append(f"{key}_guard_invalid")
    return failures


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(AUDIT_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in AUDIT_COLUMNS})


def _render_report(summary: Mapping[str, object], rows: list[Mapping[str, object]]) -> str:
    blockers = [row for row in rows if row["audit_status"] == "blocker"]
    blocker_lines = (
        ["- none"]
        if not blockers
        else [
            f"- {row['candidate_directory']}: {row['failure_reasons']}"
            for row in blockers
        ]
    )
    return "\n".join(
        [
            "# FD-Wide Candidate Design Manifest Audit",
            "",
            "not alpha evidence",
            "allocator entry: blocked",
            "Q1 entry: blocked",
            "Q2 entry: blocked",
            "Alpha Registry update: blocked",
            "production approval: not claimed",
            "",
            "This audit scans Factor Discovery candidate output directories and requires a valid "
            "`candidate_design_manifest.json` beside each candidate or family decision artifact.",
            "",
            "## Summary",
            "",
            f"- candidate directories: {summary['candidate_directory_count']}",
            f"- manifests found: {summary['manifest_found_count']}",
            f"- valid manifests: {summary['valid_manifest_count']}",
            f"- blockers: {summary['blocker_count']}",
            f"- decision: {summary['decision']}",
            "",
            "## Blockers",
            "",
            *blocker_lines,
            "",
            "## Boundary",
            "",
            "A passing FD-wide audit only allows candidate-family validation to continue inside Factor Discovery. "
            "It does not approve allocator entry, Q1, Promotion Gate, Q2, Alpha Registry updates, broker/order "
            "workflows, live trading, or production use.",
            "",
        ]
    )
