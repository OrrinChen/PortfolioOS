from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.strict_residual_closeout import run_strict_residual_closeout


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R13 strict residual evidence closeout.")
    parser.add_argument(
        "--waterfall-input-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model"),
    )
    args = parser.parse_args()

    result = run_strict_residual_closeout(args.waterfall_input_dir, args.output_dir)
    print(
        "strict_residual_closeout_built "
        f"factor_count={result.factor_count} "
        f"ready_for_redundancy_count={result.ready_for_redundancy_count} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.decision_table_path}"
    )


if __name__ == "__main__":
    main()
