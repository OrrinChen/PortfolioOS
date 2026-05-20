"""Run FD-S0 small-cap data admission gate."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.small_cap_data_admission import run_small_cap_data_admission


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
    parser = argparse.ArgumentParser(description="Run Factor Discovery FD-S0 small-cap data admission.")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "factor_discovery" / "small_cap"))
    parser.add_argument(
        "--report",
        default=str(REPO_ROOT / "reports" / "factor_discovery_small_cap_data_admission.md"),
    )
    args = parser.parse_args()

    result = run_small_cap_data_admission(args.manifest, args.output_dir, args.report)
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"small_cap_research_admitted={str(result.summary['small_cap_research_admitted']).lower()}")
    print(f"candidate_family_run_allowed={str(result.summary['candidate_family_run_allowed']).lower()}")
    print(f"delisting_handling_status={result.summary['delisting_handling_status']}")
    print(f"liquidity_cost_data_status={result.summary['liquidity_cost_data_status']}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
