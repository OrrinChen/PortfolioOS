"""Run the standalone FD diagnostic for the 12-1 momentum low-vol candidate."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.momentum_low_vol_candidate import run_momentum_low_vol_candidate_validation


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the FD momentum low-vol candidate diagnostic.")
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
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "research_mode" / "momentum_low_vol_candidate"),
    )
    parser.add_argument("--horizons", default="1,3")
    parser.add_argument("--train-window-months", type=int, default=36)
    parser.add_argument("--validation-window-months", type=int, default=12)
    parser.add_argument("--min-cross-section", type=int, default=5)
    args = parser.parse_args()

    horizons = tuple(int(item.strip()) for item in args.horizons.split(",") if item.strip())
    result = run_momentum_low_vol_candidate_validation(
        manifest_path=args.manifest,
        output_dir=args.output_dir,
        horizons=horizons,
        train_window_months=args.train_window_months,
        validation_window_months=args.validation_window_months,
        min_cross_section=args.min_cross_section,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"candidate_id={result.summary['candidate_id']}")
    print(f"dataset_frequency={result.summary['dataset_frequency']}")
    print(f"signal_date_count={result.summary['signal_date_count']}")
    print(f"active_signal_rows={result.summary['active_signal_rows']}")
    print(f"explicit_abstain_rows={result.summary['explicit_abstain_rows']}")
    print(f"validation_row_count={result.summary['validation_row_count']}")
    print(f"placebo_test_count={result.summary['placebo_test_count']}")
    print(f"candidate_validation_status={result.summary['candidate_validation_status']}")
    print(f"alpha_success_claimed={str(result.summary['alpha_success_claimed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
