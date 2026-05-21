from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.component_oos_availability import (
    run_component_oos_availability_expansion,
)


def _default_oos_observations() -> Path:
    expanded = Path("outputs/multifactor_alpha_validation/component_oos_observations/real_oos_observations.csv")
    if expanded.exists():
        return expanded
    return Path("outputs/multifactor_alpha_validation/wrds_real_oos_evidence_size/real_oos_observations.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R15.6 component OOS availability expansion.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument(
        "--component-pool",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model/soft_resurrected_component_pool.csv"),
    )
    parser.add_argument(
        "--oos-observations",
        type=Path,
        default=_default_oos_observations(),
    )
    parser.add_argument(
        "--portfolio-validation-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/portfolio_validation"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/portfolio_validation"),
    )
    parser.add_argument("--min-coverage", type=float, default=0.60)
    args = parser.parse_args()

    result = run_component_oos_availability_expansion(
        spec_dir=args.spec_dir,
        component_pool_path=args.component_pool,
        oos_observation_path=args.oos_observations,
        portfolio_validation_dir=args.portfolio_validation_dir,
        output_dir=args.output_dir,
        min_coverage=args.min_coverage,
    )
    print(
        "component_oos_availability_built "
        f"eligible_component_count={result.eligible_component_count} "
        f"observed_component_count={result.observed_component_count} "
        f"unavailable_component_count={result.unavailable_component_count} "
        f"coverage_ratio={result.coverage_ratio} "
        f"component_pool_validation_state={result.component_pool_validation_state} "
        f"full_pool_decision_allowed={str(result.full_pool_decision_allowed).lower()} "
        f"not_alpha_evidence={str(result.not_alpha_evidence).lower()} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.availability_report_path}"
    )


if __name__ == "__main__":
    main()
