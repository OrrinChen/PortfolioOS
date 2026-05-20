from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.portfolio_component_gate import run_portfolio_component_gate


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R14 portfolio component gate.")
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

    result = run_portfolio_component_gate(args.input_dir, args.output_dir)
    print(
        "portfolio_component_gate_built "
        f"factor_count={result.factor_count} "
        f"component_candidate_count={result.component_candidate_count} "
        f"standalone_clean_alpha_count={result.standalone_clean_alpha_count} "
        f"portfolio_validation_mode={result.portfolio_validation_mode} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.component_table_path}"
    )


if __name__ == "__main__":
    main()
