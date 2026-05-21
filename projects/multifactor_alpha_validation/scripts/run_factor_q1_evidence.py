from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence, write_q1_evidence_outputs
from multifactor_alpha_validation.signal_builders import build_signal_panels


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deterministic multifactor Q1 evidence.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/factor_q1"))
    args = parser.parse_args()

    specs = load_factor_specs(args.spec_dir)
    signals = build_signal_panels(specs)
    result = build_q1_evidence(specs, signals.signal_panels)
    written = write_q1_evidence_outputs(result, args.output_dir)
    print(
        "factor_q1_evidence_built "
        f"factor_count={len(result.factor_evidence_table)} "
        f"q1_pass_count={(result.factor_evidence_table['q1_decision'] == 'q1_pass').sum()} "
        f"written_files={len(written)}"
    )


if __name__ == "__main__":
    main()
