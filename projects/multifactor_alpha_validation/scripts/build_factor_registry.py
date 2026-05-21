from __future__ import annotations

import argparse
from pathlib import Path

from _pipeline import build_pipeline
from multifactor_alpha_validation.registry import write_factor_registry


def main() -> None:
    parser = argparse.ArgumentParser(description="Build multifactor factor registry.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/factor_registry"))
    args = parser.parse_args()

    *_, registry, _report, _dashboard = build_pipeline(args.spec_dir)
    write_factor_registry(registry, args.output_dir)
    print(f"factor_registry_built factor_count={len(registry.decision_table)} output_dir={args.output_dir}")


if __name__ == "__main__":
    main()
