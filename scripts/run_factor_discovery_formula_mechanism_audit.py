"""Run FD-R5.1 formula mechanism separation audit."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.formula_mechanism_audit import run_formula_mechanism_audit


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Factor Discovery formula mechanism v2 audit.")
    parser.add_argument(
        "--factor-panel",
        default=str(
            REPO_ROOT
            / "outputs"
            / "factor_discovery"
            / "real_data_daily"
            / "fd_r3"
            / "real_factor_panel.csv"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "research_mode"),
    )
    parser.add_argument(
        "--report",
        default=str(REPO_ROOT / "reports" / "factor_formula_mechanism_v2_audit.md"),
    )
    args = parser.parse_args()

    result = run_formula_mechanism_audit(
        factor_panel_path=args.factor_panel,
        output_dir=args.output_dir,
        report_path=args.report,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"formula_version={result.summary['formula_version']}")
    print(f"factor_count={result.summary['factor_count']}")
    print(f"pair_count={result.summary['pair_count']}")
    print(f"hard_fail_pair_count={result.summary['hard_fail_pair_count']}")
    print(f"allocator_entry_allowed={str(result.summary['allocator_entry_allowed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
