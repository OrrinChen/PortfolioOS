from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.research_dataset import validate_delisting_inactive_handling


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate delisting and inactive-asset handling.")
    parser.add_argument(
        "--membership",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/fixtures/research_dataset/historical_membership.csv"),
    )
    parser.add_argument(
        "--delistings",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/fixtures/research_dataset/delistings.csv"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/research_dataset"),
    )
    parser.add_argument("--terminal-return-policy", default="use_delisting_return")
    args = parser.parse_args()

    result = validate_delisting_inactive_handling(
        args.membership,
        args.delistings,
        output_dir=args.output_dir,
        terminal_return_policy=args.terminal_return_policy,
    )
    print(
        "multifactor_research_delistings "
        f"delisting_ready={str(result.delisting_ready).lower()} "
        f"covered_inactive_assets={len(result.covered_inactive_assets)} "
        f"missing_inactive_assets={len(result.missing_inactive_assets)}"
    )


if __name__ == "__main__":
    main()
