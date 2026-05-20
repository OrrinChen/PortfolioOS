"""Run FD-S4.1 small-cap lag / capacity dominance diagnosis."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.small_cap_lag_capacity_diagnosis import run_small_cap_lag_capacity_diagnosis


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FAMILY_OUTPUT_DIR = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "small_cap"
    / "family_candidates"
    / "quality_residual_momentum"
)
DEFAULT_REPORT = REPO_ROOT / "reports" / "factor_discovery_small_cap_dominance_diagnosis.md"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run FD-S4.1 small-cap dominance diagnosis.")
    parser.add_argument("--family-output-dir", default=str(DEFAULT_FAMILY_OUTPUT_DIR))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    result = run_small_cap_lag_capacity_diagnosis(
        family_output_dir=args.family_output_dir,
        report_path=args.report,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"decision_label={result.summary['decision_label']}")
    print(f"fixed_weighting_scheme_count={result.summary['fixed_weighting_scheme_count']}")
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
