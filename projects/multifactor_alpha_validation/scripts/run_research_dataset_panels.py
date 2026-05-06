from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.research_dataset import (
    load_historical_universe_membership,
    validate_adjusted_price_volume_and_benchmark,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate adjusted price-volume and QQQ benchmark panels.")
    parser.add_argument(
        "--membership",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/fixtures/research_dataset/historical_membership.csv"),
    )
    parser.add_argument(
        "--prices",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/fixtures/research_dataset/adjusted_price_volume.csv"),
    )
    parser.add_argument(
        "--benchmark",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/fixtures/research_dataset/qqq_benchmark.csv"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/research_dataset"),
    )
    parser.add_argument("--adjusted-price-convention", default="split_dividend_adjusted")
    args = parser.parse_args()

    universe = load_historical_universe_membership(
        args.membership,
        rebalance_dates=["2011-06-30", "2013-06-30"],
        output_dir=args.output_dir,
    )
    result = validate_adjusted_price_volume_and_benchmark(
        args.prices,
        args.benchmark,
        universe.snapshot_paths,
        output_dir=args.output_dir,
        adjusted_price_convention=args.adjusted_price_convention,
    )
    print(
        "multifactor_research_panels "
        f"price_panel_ready={str(result.price_panel_ready).lower()} "
        f"benchmark_ready={str(result.benchmark_ready).lower()} "
        f"missing_price_rows={result.missing_price_rows}"
    )


if __name__ == "__main__":
    main()
