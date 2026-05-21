"""Run FD-R0/R1/R2 real-data validation checks."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.real_data_validation import run_real_data_validation_r0_r2


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Factor Discovery real-data admission, universe, and return audits.")
    parser.add_argument(
        "--manifest",
        default=str(
            REPO_ROOT
            / "data"
            / "cache"
            / "wrds_multifactor"
            / "nasdaq100"
            / "standardized"
            / "research_mode_dataset_manifest.yaml"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "real_data"),
    )
    args = parser.parse_args()

    result = run_real_data_validation_r0_r2(args.manifest, args.output_dir)
    print(f"schema_version={result.summary['schema_version']}")
    print(f"admission_status={result.summary['admission_status']}")
    print(f"full_daily_price_volume_ready={str(result.summary['full_daily_price_volume_ready']).lower()}")
    print(f"factor_ranking_ran={str(result.summary['factor_ranking_ran']).lower()}")
    print(f"allocator_ran={str(result.summary['allocator_ran']).lower()}")
    print(f"alpha_success_claimed={str(result.summary['alpha_success_claimed']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
