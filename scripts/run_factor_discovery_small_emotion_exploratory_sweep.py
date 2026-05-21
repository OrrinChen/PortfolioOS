from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_exploratory_sweep import (
    run_small_emotion_exploratory_sweep,
)


DEFAULT_BASE = Path("data/cache/wrds_multifactor/small_cap_us_daily/standardized")


def _float_list(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def _str_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run E0-SMALL-EMOTION-02 exploratory parameter sweep."
    )
    parser.add_argument("--price-panel", type=Path, default=DEFAULT_BASE / "adjusted_price_volume_panel.csv")
    parser.add_argument("--benchmark-panel", type=Path, default=DEFAULT_BASE / "small_cap_benchmark_panel.csv")
    parser.add_argument("--delisting", type=Path, default=DEFAULT_BASE / "delisting_returns.csv")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/e0_exploratory_sweep"),
    )
    parser.add_argument("--shock-thresholds", default="0.05,0.08,0.10,0.15")
    parser.add_argument("--volume-spike-thresholds", default="1.5,2.0,3.0")
    parser.add_argument("--windows", default="post_1_3,post_1_5,post_1_10,post_1_22")
    parser.add_argument(
        "--mechanisms",
        default=(
            "up_shock_continuation,up_shock_reversal,"
            "down_shock_reversal,down_shock_continuation,liquidity_vacuum_reversal"
        ),
    )
    parser.add_argument("--market-cap-buckets", default="micro,small,lower_mid,all_small_cap")
    parser.add_argument("--liquidity-filters", default="all,low,mid,high,weak_liquidity")
    parser.add_argument("--stale-filters", default="medium,strict")
    parser.add_argument("--adv-min-dollars", default="250000,500000,1000000")
    parser.add_argument("--min-history-observations", type=int, default=60)
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument(
        "--max-rows",
        type=int,
        default=750_000,
        help="Optional controlled row cap for exploratory smoke. Use 0 for full file.",
    )
    args = parser.parse_args()

    result = run_small_emotion_exploratory_sweep(
        price_panel_path=args.price_panel,
        benchmark_panel_path=args.benchmark_panel,
        delisting_path=args.delisting,
        output_dir=args.output_dir,
        shock_thresholds=_float_list(args.shock_thresholds),
        volume_spike_thresholds=_float_list(args.volume_spike_thresholds),
        windows=_str_list(args.windows),
        mechanisms=_str_list(args.mechanisms),
        market_cap_buckets=_str_list(args.market_cap_buckets),
        liquidity_filters=_str_list(args.liquidity_filters),
        stale_filters=_str_list(args.stale_filters),
        adv_min_dollars=_float_list(args.adv_min_dollars),
        min_history_observations=args.min_history_observations,
        top_n=args.top_n,
        max_rows=None if args.max_rows == 0 else args.max_rows,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
