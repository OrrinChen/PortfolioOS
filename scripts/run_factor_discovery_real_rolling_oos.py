"""Run FD-R4 true rolling OOS validation on the real FD-R3 panel."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.real_rolling_oos import run_real_rolling_oos


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Factor Discovery FD-R4 real rolling OOS validation.")
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
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "real_data_daily" / "fd_r4"),
    )
    parser.add_argument("--train-window-months", type=int, default=36)
    parser.add_argument("--validation-window-months", type=int, default=12)
    parser.add_argument("--horizons", default="1,3")
    args = parser.parse_args()

    horizons = tuple(int(item.strip()) for item in args.horizons.split(",") if item.strip())
    result = run_real_rolling_oos(
        manifest_path=args.manifest,
        factor_panel_path=args.factor_panel,
        output_dir=args.output_dir,
        train_window_months=args.train_window_months,
        validation_window_months=args.validation_window_months,
        horizons=horizons,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"dataset_frequency={result.summary['dataset_frequency']}")
    print(f"factor_count={result.summary['factor_count']}")
    print(f"rebalance_count={result.summary['rebalance_count']}")
    print(f"validation_rebalance_count={result.summary['validation_rebalance_count']}")
    print(f"test_rebalance_count={result.summary['test_rebalance_count']}")
    print(f"score_row_count={result.summary['score_row_count']}")
    print(f"decile_row_count={result.summary['decile_row_count']}")
    print(f"uses_full_sample_icir={str(result.summary['uses_full_sample_icir']).lower()}")
    print(f"future_universe_used={str(result.summary['future_universe_used']).lower()}")
    print(f"future_normalization_used={str(result.summary['future_normalization_used']).lower()}")
    print(f"post_period_factor_selection_used={str(result.summary['post_period_factor_selection_used']).lower()}")
    print(f"allocator_ran={str(result.summary['allocator_ran']).lower()}")
    print(f"alpha_success_claimed={str(result.summary['alpha_success_claimed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
