from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.factor_library import validate_factor_spec_directory


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate multifactor FactorSpecs.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/factor_spec_validation"))
    args = parser.parse_args()

    report = validate_factor_spec_directory(args.spec_dir, args.output_dir)
    print(
        "factor_specs_validated "
        f"factor_count={report['factor_count']} "
        f"enabled_factor_count={report['enabled_factor_count']} "
        f"all_specs_valid={str(report['all_specs_valid']).lower()}"
    )


if __name__ == "__main__":
    main()
