from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.allocator import build_allocator_result
from multifactor_alpha_validation.cost_capacity import build_survival_result, write_survival_outputs
from multifactor_alpha_validation.covariance import build_covariance_diagnostics
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate
from multifactor_alpha_validation.shrinkage import build_shrinkage_results
from multifactor_alpha_validation.signal_builders import build_signal_panels


def main() -> None:
    parser = argparse.ArgumentParser(description="Build multifactor cost, capacity, and benchmark survival artifacts.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/factor_survival"))
    args = parser.parse_args()

    specs = load_factor_specs(args.spec_dir)
    signals = build_signal_panels(specs)
    evidence = build_q1_evidence(specs, signals.signal_panels)
    redundancy = build_redundancy_gate(specs, signals.signal_panels, evidence.factor_evidence_table)
    shrinkage = build_shrinkage_results(specs, evidence.factor_evidence_table, redundancy.marginal_value_decision_table)
    covariance = build_covariance_diagnostics(signals.signal_panels, redundancy.factor_clusters, shrinkage.posterior_mu)
    allocator = build_allocator_result(
        specs,
        shrinkage.posterior_mu,
        covariance.shrunk_covariance,
        redundancy.marginal_value_decision_table,
    )
    survival = build_survival_result(specs, evidence.factor_evidence_table, allocator, shrinkage.posterior_mu)
    write_survival_outputs(survival, args.output_dir)
    print(
        "factor_survival_built "
        f"funnel_layers={len(survival.survival_funnel)} "
        f"cost_killed_rows={(survival.cost_stress_matrix['cost_survival_status'] == 'cost_killed').sum()} "
        f"capacity_rows={len(survival.capacity_frontier)}"
    )


if __name__ == "__main__":
    main()
