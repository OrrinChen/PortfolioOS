"""Run FD-D1 Factor Discovery pain-point mapping."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.factor_design_d1 import run_factor_design_d1


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "factor_discovery" / "design_layer" / "d1"


def main() -> None:
    parser = argparse.ArgumentParser(description="Write FD-D1 factor pain-point map artifacts.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    result = run_factor_design_d1(output_dir=args.output_dir)
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"ledger_row_count={result.summary['ledger_row_count']}")
    print(f"candidate_family_count={result.summary['candidate_family_count']}")
    print(f"ledger_valid={str(result.summary['ledger_valid']).lower()}")
    print(
        "design_layer_required_before_formula="
        f"{str(result.summary['design_layer_required_before_formula']).lower()}"
    )
    print(f"formula_first_candidates_blocked={str(result.summary['formula_first_candidates_blocked']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
