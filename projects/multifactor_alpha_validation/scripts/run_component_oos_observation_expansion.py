from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.component_oos_observation_expansion import (
    run_component_oos_observation_expansion,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run component OOS observation expansion.")
    parser.add_argument(
        "--source-observations",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/wrds_real_oos_evidence_size/real_oos_observations.csv"),
    )
    parser.add_argument(
        "--component-pool",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model/soft_resurrected_component_pool.csv"),
    )
    parser.add_argument(
        "--daily-manifest",
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
        default=Path("outputs/multifactor_alpha_validation/component_oos_observations"),
    )
    args = parser.parse_args()

    result = run_component_oos_observation_expansion(
        source_observation_path=args.source_observations,
        component_pool_path=args.component_pool,
        daily_manifest_path=args.daily_manifest,
        fundamentals_manifest_path=args.fundamentals_manifest,
        output_dir=args.output_dir,
    )
    print(
        "component_oos_observations_built "
        f"generated_factor_ids={','.join(result.generated_factor_ids)} "
        f"observed_factor_count_after_expansion={result.observed_factor_count_after_expansion} "
        f"unavailable_factor_ids_after_expansion={','.join(result.unavailable_factor_ids_after_expansion)} "
        f"not_alpha_evidence={str(result.not_alpha_evidence).lower()} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.observation_path}"
    )


if __name__ == "__main__":
    main()
