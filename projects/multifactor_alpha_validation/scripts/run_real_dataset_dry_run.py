from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.real_dataset_dry_run import run_real_dataset_dry_run


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R7 real dataset dry checks without factor claims.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("data/cache/wrds_multifactor/nasdaq100/standardized/research_mode_dataset_manifest.yaml"),
        help="Ready research-mode dataset manifest produced by the WRDS monthly PIT ingest.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/wrds_real_dataset_dry_run"),
    )
    args = parser.parse_args()

    result = run_real_dataset_dry_run(args.manifest, args.output_dir)
    print(
        "multifactor_real_dataset_dry_run "
        f"preflight_ready={str(result.preflight_ready).lower()} "
        f"dataset_frequency={result.dataset_frequency} "
        "daily_price_volume_validation_status=separate_long_task_not_started "
        f"daily_price_volume_validation_started={str(result.daily_price_volume_validation_started).lower()} "
        f"allocator_ran={str(result.allocator_ran).lower()} "
        f"factor_ranking_ran={str(result.factor_ranking_ran).lower()} "
        f"strategy_return_claimed={str(result.strategy_return_claimed).lower()} "
        f"alpha_conclusion_claimed={str(result.alpha_conclusion_claimed).lower()}"
    )


if __name__ == "__main__":
    main()
