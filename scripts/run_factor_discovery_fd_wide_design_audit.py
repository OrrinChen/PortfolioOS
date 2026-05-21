"""Run the FD-wide candidate design manifest audit."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.fd_wide_design_audit import run_fd_wide_design_audit


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCAN_ROOTS = (
    REPO_ROOT / "outputs" / "factor_discovery" / "research_mode",
    REPO_ROOT / "outputs" / "factor_discovery" / "small_cap" / "family_candidates",
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "factor_discovery" / "design_audit"
DEFAULT_REPORT = REPO_ROOT / "reports" / "factor_discovery_fd_wide_design_audit.md"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the FD-wide candidate design manifest audit.")
    parser.add_argument(
        "--scan-root",
        action="append",
        dest="scan_roots",
        help="Output root to scan. May be provided more than once.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    scan_roots = tuple(Path(root) for root in (args.scan_roots or DEFAULT_SCAN_ROOTS))
    result = run_fd_wide_design_audit(
        scan_roots=scan_roots,
        output_dir=args.output_dir,
        report_path=args.report,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"candidate_directory_count={result.summary['candidate_directory_count']}")
    print(f"manifest_found_count={result.summary['manifest_found_count']}")
    print(f"valid_manifest_count={result.summary['valid_manifest_count']}")
    print(f"blocker_count={result.summary['blocker_count']}")
    print(f"audit_passed={str(result.summary['audit_passed']).lower()}")
    print(f"decision={result.summary['decision']}")
    print(f"allocator_entry_allowed={str(result.summary['allocator_entry_allowed']).lower()}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"alpha_registry_update_allowed={str(result.summary['alpha_registry_update_allowed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
