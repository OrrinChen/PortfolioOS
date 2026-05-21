#!/usr/bin/env python3
"""Build the local PortfolioOS demo audit report."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
for path in (
    REPO_ROOT / "src",
    PROJECT_ROOT / "src",
    REPO_ROOT / "projects" / "agentic_alpha_triage" / "src",
    REPO_ROOT / "projects" / "evidence_bundle" / "src",
    REPO_ROOT / "projects" / "promotion_gate" / "src",
    REPO_ROOT / "projects" / "execution_aware_optimizer" / "src",
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from audit_report import build_demo_audit_report, load_demo_audit_manifest
from portfolio_os.observability import StructuredTraceLogger, TraceWriter
from portfolio_os.provenance import build_provenance_manifest, write_provenance_manifest


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description="Build the PortfolioOS demo audit report.")
    parser.add_argument(
        "--manifest",
        default=str(PROJECT_ROOT / "examples" / "demo_audit_manifest.yaml"),
    )
    parser.add_argument("--output", default=str(REPO_ROOT / "reports" / "demo_audit_report.md"))
    parser.add_argument(
        "--provenance-output",
        default=str(REPO_ROOT / "reports" / "demo_run_manifest.json"),
    )
    parser.add_argument(
        "--trace-jsonl",
        default=None,
        help="Optional path for structured JSONL trace events.",
    )
    args = parser.parse_args()

    trace_logger = StructuredTraceLogger(
        TraceWriter(args.trace_jsonl) if args.trace_jsonl is not None else None
    )
    manifest = load_demo_audit_manifest(args.manifest)
    trace_logger.emit(
        "schema_validated",
        payload={"schema": "demo_audit_manifest", "manifest_path": _repo_relative(args.manifest)},
    )
    trace_logger.emit(
        "bundle_loaded",
        payload={
            "case_label": manifest.promoted_case.case_label,
            "evidence_bundle_path": manifest.promoted_case.evidence_bundle_path,
        },
    )
    trace_logger.emit(
        "bundle_loaded",
        payload={
            "case_label": manifest.rejected_case.case_label,
            "evidence_bundle_path": manifest.rejected_case.evidence_bundle_path,
        },
    )

    report = build_demo_audit_report(REPO_ROOT, manifest)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report.markdown, encoding="utf-8")
    trace_logger.emit(
        "promotion_decision_created",
        payload={
            "case_label": manifest.promoted_case.case_label,
            "decision": report.promoted_decision.decision,
            "reason_count": len(report.promoted_decision.reasons),
        },
    )
    trace_logger.emit(
        "promotion_decision_created",
        payload={
            "case_label": manifest.rejected_case.case_label,
            "decision": report.rejected_decision.decision,
            "reason_count": len(report.rejected_decision.reasons),
        },
    )
    for row in report.q2_rows:
        if row.status == "unavailable":
            explanation = row.explanation or {}
            trace_logger.emit(
                "q2_scenario_unavailable",
                payload={
                    "scenario_id": row.scenario_id,
                    "layer_name": row.layer_name,
                    "reason": row.infeasibility_reason
                    or explanation.get("primary_reason", "unavailable"),
                },
            )
    provenance = build_provenance_manifest(
        repo_root=REPO_ROOT,
        run_id="demo_audit_report",
        command=_provenance_command(args),
        config_path=args.manifest,
        input_paths=_manifest_input_paths(manifest),
        output_paths={"demo_audit_report": output_path},
        runner_version="portfolioos-audit-report-v1",
    )
    provenance_path = write_provenance_manifest(args.provenance_output, provenance)
    trace_logger.emit(
        "report_written",
        payload={
            "report_path": _repo_relative(str(output_path)),
            "provenance_path": _repo_relative(str(provenance_path)),
        },
    )
    print(f"demo_audit_report: {output_path}")
    print(f"demo_run_manifest: {provenance_path}")


def _provenance_command(args: argparse.Namespace) -> list[str]:
    """Build a sanitized replay-shape command for provenance."""

    command = [
        "projects/audit_report/scripts/build_demo_audit_report.py",
        "--manifest",
        _repo_relative(args.manifest),
        "--output",
        _repo_relative(args.output),
    ]
    if args.trace_jsonl is not None:
        command.extend(["--trace-jsonl", _repo_relative(args.trace_jsonl)])
    return command


def _manifest_input_paths(manifest: object) -> dict[str, Path]:
    """Collect local manifest-referenced inputs for the provenance sidecar."""

    promoted_case = manifest.promoted_case
    rejected_case = manifest.rejected_case
    return {
        "promoted_hypothesis": REPO_ROOT / promoted_case.hypothesis_path,
        "promoted_signal_contract": REPO_ROOT / promoted_case.signal_contract_path,
        "promoted_evaluation_contract": REPO_ROOT / promoted_case.evaluation_contract_path,
        "promoted_evidence_bundle": REPO_ROOT / promoted_case.evidence_bundle_path,
        "rejected_evidence_bundle": REPO_ROOT / rejected_case.evidence_bundle_path,
    }


def _repo_relative(path_text: str) -> str:
    path = Path(path_text)
    resolved = path if path.is_absolute() else REPO_ROOT / path
    try:
        return resolved.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


if __name__ == "__main__":
    main()
