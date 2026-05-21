from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate, write_redundancy_outputs
from multifactor_alpha_validation.signal_builders import build_signal_panels


def main() -> None:
    parser = argparse.ArgumentParser(description="Build multifactor redundancy and marginal-value artifacts.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/factor_redundancy"))
    args = parser.parse_args()

    specs = load_factor_specs(args.spec_dir)
    signals = build_signal_panels(specs)
    evidence = build_q1_evidence(specs, signals.signal_panels)
    result = build_redundancy_gate(specs, signals.signal_panels, evidence.factor_evidence_table)
    write_redundancy_outputs(result, args.output_dir)
    print(
        "factor_redundancy_built "
        f"factor_count={len(result.marginal_value_decision_table)} "
        f"cluster_count={result.factor_clusters['cluster_id'].nunique()} "
        f"promoted_count={(result.marginal_value_decision_table['decision'] == 'promote_to_allocator').sum()}"
    )


if __name__ == "__main__":
    main()
