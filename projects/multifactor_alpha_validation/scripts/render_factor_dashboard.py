from __future__ import annotations

import argparse
from pathlib import Path

from _pipeline import build_pipeline
from multifactor_alpha_validation.dashboard import write_factor_dashboard


def main() -> None:
    parser = argparse.ArgumentParser(description="Render static multifactor dashboard.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/factor_dashboard"))
    args = parser.parse_args()

    *_, dashboard = build_pipeline(args.spec_dir)
    path = write_factor_dashboard(dashboard, args.output_dir)
    print(f"factor_dashboard_rendered path={path}")


if __name__ == "__main__":
    main()

