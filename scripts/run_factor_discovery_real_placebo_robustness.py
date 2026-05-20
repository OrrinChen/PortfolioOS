"""Run FD-R5 placebo and robustness diagnostics."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.factor_placebo import run_real_placebo_robustness


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Factor Discovery FD-R5 placebo and robustness diagnostics.")
    parser.add_argument(
        "--manifest",
        default=str(
            REPO_ROOT
            / "data"
            / "cache"
            / "wrds_multifactor"
            / "nasdaq100_daily_full10"
            / "standardized"
            / "research_mode_dataset_manifest.yaml"
        ),
    )
    parser.add_argument(
        "--factor-panel",
        default=str(
            REPO_ROOT / "outputs" / "factor_discovery" / "real_data_daily" / "fd_r3" / "real_factor_panel.csv"
        ),
    )
    parser.add_argument(
        "--oos-score-panel",
        default=str(
            REPO_ROOT
            / "outputs"
            / "factor_discovery"
            / "real_data_daily"
            / "fd_r4"
            / "oos_factor_score_panel_real.csv"
        ),
    )
    parser.add_argument(
        "--oos-decile-spread",
        default=str(
            REPO_ROOT
            / "outputs"
            / "factor_discovery"
            / "real_data_daily"
            / "fd_r4"
            / "oos_decile_spread_real.csv"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "real_data_daily" / "fd_r5"),
    )
    args = parser.parse_args()

    result = run_real_placebo_robustness(
        manifest_path=args.manifest,
        factor_panel_path=args.factor_panel,
        oos_score_panel_path=args.oos_score_panel,
        oos_decile_spread_path=args.oos_decile_spread,
        output_dir=args.output_dir,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"placebo_test_count={result.summary['placebo_test_count']}")
    print(f"placebo_status={result.summary['placebo_status']}")
    print(f"recommended_next_action={result.summary['recommended_next_action']}")
    print(f"allocator_entry_allowed={str(result.summary['allocator_entry_allowed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"alpha_success_claimed={str(result.summary['alpha_success_claimed']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
