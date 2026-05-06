from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.real_rolling_oos import run_first_real_rolling_oos_evidence


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R8 first real rolling OOS evidence.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("data/cache/wrds_multifactor/nasdaq100_daily/standardized/research_mode_dataset_manifest.yaml"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/wrds_real_oos_evidence"),
    )
    args = parser.parse_args()

    result = run_first_real_rolling_oos_evidence(args.manifest, args.output_dir)
    print(
        "multifactor_real_rolling_oos "
        f"status={result.oos_status} "
        f"dataset_frequency={result.dataset_frequency} "
        f"uses_full_sample_icir={str(result.uses_full_sample_icir).lower()} "
        f"alpha_success_claimed={str(result.alpha_success_claimed).lower()}"
    )


if __name__ == "__main__":
    main()
