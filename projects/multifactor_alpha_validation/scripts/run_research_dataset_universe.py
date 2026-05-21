from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.research_dataset import load_historical_universe_membership


def main() -> None:
    parser = argparse.ArgumentParser(description="Build PIT historical universe snapshots for multifactor research.")
    parser.add_argument(
        "--membership",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/fixtures/research_dataset/historical_membership.csv"),
    )
    parser.add_argument(
        "--rebalance-date",
        action="append",
        dest="rebalance_dates",
        default=None,
        help="Rebalance date to snapshot; may be provided multiple times.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/research_dataset"),
    )
    args = parser.parse_args()

    rebalance_dates = args.rebalance_dates or ["2011-06-30", "2013-06-30"]
    result = load_historical_universe_membership(
        args.membership,
        rebalance_dates=rebalance_dates,
        output_dir=args.output_dir,
    )
    print(
        "multifactor_research_universe "
        f"snapshot_count={len(result.snapshot_counts)} "
        f"validation={result.validation_path}"
    )


if __name__ == "__main__":
    main()
