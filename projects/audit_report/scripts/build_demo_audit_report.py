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

from audit_report import load_demo_audit_manifest, write_demo_audit_report
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
    args = parser.parse_args()

    manifest = load_demo_audit_manifest(args.manifest)
    output_path = write_demo_audit_report(
        args.output,
        repo_root=REPO_ROOT,
        manifest=manifest,
    )
    provenance = build_provenance_manifest(
        repo_root=REPO_ROOT,
        run_id="demo_audit_report",
        command=[
            "projects/audit_report/scripts/build_demo_audit_report.py",
            "--manifest",
            _repo_relative(args.manifest),
            "--output",
            _repo_relative(args.output),
        ],
        config_path=args.manifest,
        input_paths=_manifest_input_paths(manifest),
        output_paths={"demo_audit_report": output_path},
        runner_version="portfolioos-audit-report-v1",
    )
    provenance_path = write_provenance_manifest(args.provenance_output, provenance)
    print(f"demo_audit_report: {output_path}")
    print(f"demo_run_manifest: {provenance_path}")


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
