from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.portfolio_assembly_audit import run_portfolio_assembly_audit


def _default_oos_observations() -> Path:
    expanded = Path("outputs/multifactor_alpha_validation/component_oos_observations/real_oos_observations.csv")
    if expanded.exists():
        return expanded
    return Path("outputs/multifactor_alpha_validation/wrds_real_oos_evidence_size/real_oos_observations.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R15.5 portfolio assembly audit.")
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
        "--portfolio-validation-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/portfolio_validation"),
    )
    parser.add_argument(
        "--oos-observations",
        type=Path,
        default=_default_oos_observations(),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/portfolio_validation"),
    )
    args = parser.parse_args()

    result = run_portfolio_assembly_audit(
        component_pool_path=args.component_pool,
        component_candidate_path=args.component_candidates,
        portfolio_validation_dir=args.portfolio_validation_dir,
        oos_observation_path=args.oos_observations,
        output_dir=args.output_dir,
    )
    print(
        "portfolio_assembly_audit_built "
        f"original_decision_state={result.original_decision_state} "
        f"reclassified_decision_state={result.reclassified_decision_state} "
        f"component_pool_validation_state={result.component_pool_validation_state} "
        f"not_alpha_evidence={str(result.not_alpha_evidence).lower()} "
        f"production_approval={str(result.production_approval).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()} "
        f"path={result.audit_path}"
    )


if __name__ == "__main__":
    main()
