"""Run FD-S3/S4 small-cap quality residual momentum family diagnostic."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.small_cap_quality_family import run_small_cap_quality_residual_momentum


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = (
    REPO_ROOT
    / "data"
    / "cache"
    / "wrds_multifactor"
    / "small_cap_us_daily"
    / "standardized"
    / "research_mode_dataset_manifest.yaml"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Factor Discovery small-cap quality residual momentum family.")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument(
        "--output-dir",
        default=str(
            REPO_ROOT
            / "outputs"
            / "factor_discovery"
            / "small_cap"
            / "family_candidates"
            / "quality_residual_momentum"
        ),
    )
    parser.add_argument(
        "--report",
        default=str(REPO_ROOT / "reports" / "factor_discovery_small_cap_quality_residual_momentum.md"),
    )
    args = parser.parse_args()

    result = run_small_cap_quality_residual_momentum(
        manifest_path=args.manifest,
        output_dir=args.output_dir,
        report_path=args.report,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"family_id={result.summary['family_id']}")
    print(f"primary_signal={result.summary['primary_signal']}")
    print(f"decision_label={result.summary['decision_label']}")
    print(f"candidate_family_run_allowed={str(result.summary['candidate_family_run_allowed']).lower()}")
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
