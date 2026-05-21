"""Run FD-R5.1 candidate failure diagnosis."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.failure_diagnosis import run_real_failure_diagnosis


REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_DATA_DAILY = REPO_ROOT / "outputs" / "factor_discovery" / "real_data_daily"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Factor Discovery FD-R5.1 failure diagnosis.")
    parser.add_argument(
        "--factor-panel",
        default=str(REAL_DATA_DAILY / "fd_r3" / "real_factor_panel.csv"),
    )
    parser.add_argument(
        "--rolling-weights",
        default=str(REAL_DATA_DAILY / "fd_r4" / "rolling_icir_real.csv"),
    )
    parser.add_argument(
        "--oos-score-panel",
        default=str(REAL_DATA_DAILY / "fd_r4" / "oos_factor_score_panel_real.csv"),
    )
    parser.add_argument(
        "--placebo-report",
        default=str(REAL_DATA_DAILY / "fd_r5" / "placebo_report.csv"),
    )
    parser.add_argument(
        "--output-dir",
        default=str(REAL_DATA_DAILY / "fd_r5_1"),
    )
    args = parser.parse_args()

    result = run_real_failure_diagnosis(
        factor_panel_path=args.factor_panel,
        rolling_weights_path=args.rolling_weights,
        oos_score_panel_path=args.oos_score_panel,
        placebo_report_path=args.placebo_report,
        output_dir=args.output_dir,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"recommended_next_action={result.summary['recommended_next_action']}")
    print(f"allocator_entry_allowed={str(result.summary['allocator_entry_allowed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"alpha_success_claimed={str(result.summary['alpha_success_claimed']).lower()}")
    for key, value in result.summary["failure_flags"].items():
        print(f"failure_flag.{key}={value}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
