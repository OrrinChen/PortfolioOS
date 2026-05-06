"""Write the local Factor Discovery Sandbox price-volume FactorSpecs."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.factor_specs import write_price_volume_factor_specs


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Write Factor Discovery Sandbox FactorSpecs.")
    parser.add_argument(
        "--spec-dir",
        default=str(
            REPO_ROOT
            / "projects"
            / "multifactor_alpha_validation"
            / "factor_discovery_sandbox"
            / "factor_specs"
            / "price_volume_29"
        ),
    )
    parser.add_argument(
        "--validation-output",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "factor_spec_validation.json"),
    )
    args = parser.parse_args()

    validation = write_price_volume_factor_specs(args.spec_dir, args.validation_output)
    print(f"factor_count={validation['factor_count']}")
    print(f"all_specs_valid={str(validation['all_specs_valid']).lower()}")
    print(f"no_view_is_not_zero_alpha={str(validation['no_view_is_not_zero_alpha']).lower()}")
    print(f"validation_output={args.validation_output}")


if __name__ == "__main__":
    main()
