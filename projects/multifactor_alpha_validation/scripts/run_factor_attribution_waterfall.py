from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.factor_attribution_waterfall import run_factor_attribution_waterfall


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R12 factor attribution waterfall.")
    parser.add_argument(
        "--research-manifest",
        type=Path,
        default=Path(
            "data/cache/wrds_multifactor/nasdaq100_daily_size/standardized/research_mode_dataset_manifest.yaml"
        ),
    )
    parser.add_argument(
        "--residual-returns",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model/risk_model_residual_returns.csv"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model"),
    )
    args = parser.parse_args()

    result = run_factor_attribution_waterfall(args.research_manifest, args.residual_returns, args.output_dir)
    print(
        "factor_attribution_waterfall_built "
        f"factor_count={result.factor_count} "
        f"period_row_count={result.period_row_count} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.waterfall_path}"
    )


if __name__ == "__main__":
    main()
