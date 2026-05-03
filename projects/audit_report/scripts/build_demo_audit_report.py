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


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description="Build the PortfolioOS demo audit report.")
    parser.add_argument(
        "--manifest",
        default=str(PROJECT_ROOT / "examples" / "demo_audit_manifest.yaml"),
    )
    parser.add_argument("--output", default=str(REPO_ROOT / "reports" / "demo_audit_report.md"))
    args = parser.parse_args()

    output_path = write_demo_audit_report(
        args.output,
        repo_root=REPO_ROOT,
        manifest=load_demo_audit_manifest(args.manifest),
    )
    print(f"demo_audit_report: {output_path}")


if __name__ == "__main__":
    main()
