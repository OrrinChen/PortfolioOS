from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.risk_exposure_store import run_pit_exposure_store


def main() -> None:
    parser = argparse.ArgumentParser(description="Build MF-R10 PIT exposure store for risk attribution.")
    parser.add_argument(
        "--research-manifest",
        type=Path,
        default=Path("data/cache/wrds_multifactor/nasdaq100_daily_size/standardized/research_mode_dataset_manifest.yaml"),
    )
    parser.add_argument(
        "--fundamentals-manifest",
        type=Path,
        default=Path("data/cache/wrds_multifactor/nasdaq100_fundamentals/standardized/fundamentals_manifest.yaml"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model"),
    )
    args = parser.parse_args()

    fundamentals_manifest = args.fundamentals_manifest if args.fundamentals_manifest.exists() else None
    result = run_pit_exposure_store(args.research_manifest, fundamentals_manifest, args.output_dir)
    print(
        "pit_exposure_store_built "
        f"exposure_count={result.exposure_count} "
        f"date_count={result.date_count} "
        f"asset_count={result.asset_count} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.exposure_panel_path}"
    )


if __name__ == "__main__":
    main()
