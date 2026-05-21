from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.failure_diagnosis_report import run_failure_diagnosis_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the multifactor failure diagnosis report.")
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

    result = run_failure_diagnosis_report(args.input_dir, args.output_dir)
    print(
        "failure_diagnosis_report_built "
        f"factor_count={result.factor_count} "
        f"qqq_guard_hard_gate_recommended={str(result.qqq_guard_hard_gate_recommended).lower()} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.report_path}"
    )


if __name__ == "__main__":
    main()
