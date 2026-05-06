from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.allocator import build_allocator_result, write_allocator_outputs
from multifactor_alpha_validation.covariance import build_covariance_diagnostics
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate
from multifactor_alpha_validation.shrinkage import build_shrinkage_results
from multifactor_alpha_validation.signal_builders import build_signal_panels


def main() -> None:
    parser = argparse.ArgumentParser(description="Build multifactor allocator artifacts.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/factor_allocator"))
    args = parser.parse_args()

    specs = load_factor_specs(args.spec_dir)
    signals = build_signal_panels(specs)
    evidence = build_q1_evidence(specs, signals.signal_panels)
    redundancy = build_redundancy_gate(specs, signals.signal_panels, evidence.factor_evidence_table)
    shrinkage = build_shrinkage_results(specs, evidence.factor_evidence_table, redundancy.marginal_value_decision_table)
    covariance = build_covariance_diagnostics(signals.signal_panels, redundancy.factor_clusters, shrinkage.posterior_mu)
    result = build_allocator_result(
        specs,
        shrinkage.posterior_mu,
        covariance.shrunk_covariance,
        redundancy.marginal_value_decision_table,
    )
    write_allocator_outputs(result, args.output_dir)
    print(
        "factor_allocator_built "
        f"active_factor_count={result.diagnostics['active_factor_count']} "
        f"zero_weight_count={result.diagnostics['zero_weight_count']} "
        f"sign_flip_check_passed={str(result.sanity_checks['sign_flip_check_passed']).lower()}"
    )


if __name__ == "__main__":
    main()
