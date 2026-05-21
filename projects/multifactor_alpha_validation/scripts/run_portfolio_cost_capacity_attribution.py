from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.portfolio_cost_capacity import run_portfolio_cost_capacity_attribution


def _default_oos_observations() -> Path:
    expanded = Path("outputs/multifactor_alpha_validation/component_oos_observations/real_oos_observations.csv")
    if expanded.exists():
        return expanded
    return Path("outputs/multifactor_alpha_validation/wrds_real_oos_evidence_size/real_oos_observations.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R17 bounded cost/capacity attribution diagnostics.")
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
        "--portfolio-contribution-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/portfolio_contribution"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/portfolio_cost_capacity"),
    )
    args = parser.parse_args()

    result = run_portfolio_cost_capacity_attribution(
        component_pool_path=args.component_pool,
        oos_observation_path=args.oos_observations,
        portfolio_validation_dir=args.portfolio_validation_dir,
        portfolio_contribution_dir=args.portfolio_contribution_dir,
        output_dir=args.output_dir,
    )
    print(
        "portfolio_cost_capacity_built "
        f"validation_status={result.validation_status} "
        f"decision_state={result.decision_state} "
        f"observed_component_count={result.observed_component_count} "
        f"not_alpha_evidence={str(result.not_alpha_evidence).lower()} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.component_cost_capacity_attribution_path}"
    )


if __name__ == "__main__":
    main()
