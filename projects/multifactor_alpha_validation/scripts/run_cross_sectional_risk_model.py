from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.cross_sectional_risk_model import run_cross_sectional_risk_model


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R11 cross-sectional risk model attribution.")
    parser.add_argument(
        "--research-manifest",
        type=Path,
        default=Path(
            "data/cache/wrds_multifactor/nasdaq100_daily_size/standardized/research_mode_dataset_manifest.yaml"
        ),
    )
    parser.add_argument(
        "--exposure-panel",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model/exposure_panel.csv"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model"),
    )
    args = parser.parse_args()

    result = run_cross_sectional_risk_model(args.research_manifest, args.exposure_panel, args.output_dir)
    print(
        "cross_sectional_risk_model_built "
        f"period_count={result.period_count} "
        f"residual_return_count={result.residual_return_count} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.residual_returns_path}"
    )


if __name__ == "__main__":
    main()
