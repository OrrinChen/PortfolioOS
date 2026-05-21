from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_sharpening_sweep import (
    run_small_emotion_sharpening_sweep,
)


DEFAULT_BASE = Path("data/cache/wrds_multifactor/small_cap_us_daily/standardized")


def _float_or_none_list(value: str) -> list[float | None]:
    output: list[float | None] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        output.append(None if item.lower() in {"none", "null", "all"} else float(item))
    return output


def _float_list(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def _str_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run E0-SMALL-EMOTION-04 aggressive mechanism sharpening sweep."
    )
    parser.add_argument("--price-panel", type=Path, default=DEFAULT_BASE / "adjusted_price_volume_panel.csv")
    parser.add_argument("--benchmark-panel", type=Path, default=DEFAULT_BASE / "small_cap_benchmark_panel.csv")
    parser.add_argument("--delisting", type=Path, default=DEFAULT_BASE / "delisting_returns.csv")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/e0_sharpening_sweep"),
    )
    parser.add_argument("--shock-thresholds", default="0.05,0.08,0.10")
    parser.add_argument("--volume-spike-thresholds", default="1.5,2.0")
    parser.add_argument("--prior-5d-min-returns", default="none,0.10,0.20")
    parser.add_argument("--prior-20d-min-returns", default="none,0.20")
    parser.add_argument("--close-location-filters", default="all,top_quartile")
    parser.add_argument("--low-price-filters", default="all,under_10")
    parser.add_argument("--market-cap-buckets", default="all_small_cap,micro,small")
    parser.add_argument("--liquidity-filters", default="all,weak_liquidity")
    parser.add_argument("--spread-filters", default="all,wide")
    parser.add_argument("--regime-filters", default="all,market_up_20d,market_down_20d")
    parser.add_argument("--windows", default="post_1_5,post_6_22,post_1_22")
    parser.add_argument("--mechanisms", default="up_shock_reversal")
    parser.add_argument("--adv-min-dollars", default="250000")
    parser.add_argument("--min-history-observations", type=int, default=60)
    parser.add_argument("--min-events", type=int, default=50)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--max-rows", type=int, default=750_000, help="Use 0 for full single-file replay.")
    args = parser.parse_args()

    result = run_small_emotion_sharpening_sweep(
        price_panel_path=args.price_panel,
        benchmark_panel_path=args.benchmark_panel,
        delisting_path=args.delisting,
        output_dir=args.output_dir,
        shock_thresholds=_float_list(args.shock_thresholds),
        volume_spike_thresholds=_float_list(args.volume_spike_thresholds),
        prior_5d_min_returns=_float_or_none_list(args.prior_5d_min_returns),
        prior_20d_min_returns=_float_or_none_list(args.prior_20d_min_returns),
        close_location_filters=_str_list(args.close_location_filters),
        low_price_filters=_str_list(args.low_price_filters),
        market_cap_buckets=_str_list(args.market_cap_buckets),
        liquidity_filters=_str_list(args.liquidity_filters),
        spread_filters=_str_list(args.spread_filters),
        regime_filters=_str_list(args.regime_filters),
        windows=_str_list(args.windows),
        mechanisms=_str_list(args.mechanisms),
        adv_min_dollars=_float_list(args.adv_min_dollars),
        min_history_observations=args.min_history_observations,
        min_events=args.min_events,
        top_n=args.top_n,
        max_rows=None if args.max_rows == 0 else args.max_rows,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
