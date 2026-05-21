from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.covariance import build_covariance_diagnostics, write_covariance_outputs
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate
from multifactor_alpha_validation.shrinkage import build_shrinkage_results, write_shrinkage_outputs
from multifactor_alpha_validation.signal_builders import build_signal_panels


def main() -> None:
    parser = argparse.ArgumentParser(description="Build multifactor shrinkage and covariance artifacts.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--shrinkage-output-dir", type=Path, default=Path("outputs/factor_shrinkage"))
    parser.add_argument("--covariance-output-dir", type=Path, default=Path("outputs/factor_covariance"))
    args = parser.parse_args()

    specs = load_factor_specs(args.spec_dir)
    signals = build_signal_panels(specs)
    evidence = build_q1_evidence(specs, signals.signal_panels)
    redundancy = build_redundancy_gate(specs, signals.signal_panels, evidence.factor_evidence_table)
    shrinkage = build_shrinkage_results(specs, evidence.factor_evidence_table, redundancy.marginal_value_decision_table)
    covariance = build_covariance_diagnostics(signals.signal_panels, redundancy.factor_clusters, shrinkage.posterior_mu)
    write_shrinkage_outputs(shrinkage, args.shrinkage_output_dir)
    write_covariance_outputs(covariance, args.covariance_output_dir)
    print(
        "factor_shrinkage_built "
        f"factor_count={len(shrinkage.posterior_mu)} "
        f"condition_before={covariance.diagnostics['condition_number_before']} "
        f"condition_after={covariance.diagnostics['condition_number_after']}"
    )


if __name__ == "__main__":
    main()
