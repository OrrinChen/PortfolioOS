from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.external_source_adapter import validate_external_pit_dataset_source


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate external PIT dataset source configuration.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/configs/wrds_nasdaq100_research_mode.yaml"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/external_dataset_source"),
    )
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args()

    result = validate_external_pit_dataset_source(args.config, args.output_dir)
    print(
        "multifactor_external_source_check "
        f"status={result.status} "
        f"blocker_count={len(result.blockers)} "
        f"output_dir={args.output_dir}"
    )
    if args.require_ready and result.status != "ready":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
