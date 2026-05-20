from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.candidate_filter_audit import run_candidate_filter_audit


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R14.5 candidate filter audit and component resurrection.")
    parser.add_argument(
        "--spec-dir",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/factor_specs"),
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model"),
    )
    args = parser.parse_args()

    result = run_candidate_filter_audit(args.spec_dir, args.input_dir, args.output_dir)
    print(
        "candidate_filter_audit_built "
        f"total_candidate_count={result.total_candidate_count} "
        f"component_pool_count={result.component_pool_count} "
        f"hard_excluded_count={result.hard_excluded_count} "
        f"not_alpha_evidence={str(result.not_alpha_evidence).lower()} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.candidate_filter_audit_path}"
    )


if __name__ == "__main__":
    main()
