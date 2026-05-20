"""Run FD-D2 pre-formula diagnostics."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.factor_design_d2 import run_factor_design_d2


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "factor_discovery" / "design_layer" / "d2"
DEFAULT_D1_LEDGER = REPO_ROOT / "outputs" / "factor_discovery" / "design_layer" / "d1" / "factor_design_ledger.csv"


def main() -> None:
    parser = argparse.ArgumentParser(description="Write FD-D2 pre-formula diagnostic artifacts.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument(
        "--d1-ledger",
        default=str(DEFAULT_D1_LEDGER) if DEFAULT_D1_LEDGER.exists() else None,
    )
    args = parser.parse_args()

    result = run_factor_design_d2(output_dir=args.output_dir, d1_ledger_path=args.d1_ledger)
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"diagnostic_row_count={result.summary['diagnostic_row_count']}")
    print(f"ready_for_d3_count={result.summary['ready_for_d3_count']}")
    print(f"formula_validation_allowed_count={result.summary['formula_validation_allowed_count']}")
    print(f"diagnostics_valid={str(result.summary['diagnostics_valid']).lower()}")
    print(f"formula_validation_ran={str(result.summary['formula_validation_ran']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
