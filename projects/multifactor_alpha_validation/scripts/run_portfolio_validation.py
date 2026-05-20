from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.portfolio_validation import run_portfolio_ensemble_validation


def _default_oos_observations() -> Path:
    expanded = Path("outputs/multifactor_alpha_validation/component_oos_observations/real_oos_observations.csv")
    if expanded.exists():
        return expanded
    return Path("outputs/multifactor_alpha_validation/wrds_real_oos_evidence_size/real_oos_observations.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R15 portfolio-level OOS ensemble validation.")
    parser.add_argument(
        "--component-pool",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model/soft_resurrected_component_pool.csv"),
    )
    parser.add_argument(
        "--component-candidates",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model/component_candidate_table.csv"),
    )
    parser.add_argument(
        "--oos-observations",
        type=Path,
        default=_default_oos_observations(),
    )
    parser.add_argument(
        "--waterfall-by-period",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/risk_model/factor_attribution_waterfall_by_period.csv"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/portfolio_validation"),
    )
    args = parser.parse_args()

    result = run_portfolio_ensemble_validation(
        component_pool_path=args.component_pool,
        component_candidate_path=args.component_candidates,
        oos_observation_path=args.oos_observations,
        waterfall_by_period_path=args.waterfall_by_period,
        output_dir=args.output_dir,
    )
    print(
        "portfolio_validation_built "
        f"validation_status={result.validation_status} "
        f"decision_state={result.decision_state} "
        f"input_component_count={result.input_component_count} "
        f"available_component_count={result.available_component_count} "
        f"unavailable_component_count={result.unavailable_component_count} "
        f"hard_blocked_component_count={result.hard_blocked_component_count} "
        f"not_alpha_evidence={str(result.not_alpha_evidence).lower()} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.portfolio_ensemble_oos_report_path}"
    )


if __name__ == "__main__":
    main()
