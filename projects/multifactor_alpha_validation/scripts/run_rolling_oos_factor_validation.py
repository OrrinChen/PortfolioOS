from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.rolling_oos_validation import run_rolling_oos_factor_validation


def main() -> None:
    parser = argparse.ArgumentParser(description="Run rolling OOS factor validation for multifactor research mode.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(
            "projects/multifactor_alpha_validation/fixtures/research_dataset/research_mode_dataset_manifest_fixture.yaml"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/rolling_oos_validation"),
    )
    args = parser.parse_args()

    result = run_rolling_oos_factor_validation(args.manifest, args.output_dir)
    print(
        "multifactor_rolling_oos_validation "
        f"preflight_ready={str(result.preflight_ready).lower()} "
        f"uses_full_sample_icir={str(result.uses_full_sample_icir).lower()} "
        f"honest_null_recorded={str(result.honest_null_recorded).lower()}"
    )


if __name__ == "__main__":
    main()
