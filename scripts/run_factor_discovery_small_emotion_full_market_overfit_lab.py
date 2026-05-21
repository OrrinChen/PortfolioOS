from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_full_market_overfit_lab import (
    run_small_emotion_full_market_overfit_lab,
)


DEFAULT_BASE = Path("data/cache/wrds_multifactor/small_cap_us_daily/standardized")


def _str_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _float_list(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run E1 full-market small-emotion overfit lab.")
    parser.add_argument("--price-panel", type=Path, default=DEFAULT_BASE / "adjusted_price_volume_panel.csv")
    parser.add_argument("--benchmark-panel", type=Path, default=DEFAULT_BASE / "small_cap_benchmark_panel.csv")
    parser.add_argument("--delisting", type=Path, default=DEFAULT_BASE / "delisting_returns.csv")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/e1_full_market_overfit_lab"),
    )
    parser.add_argument(
        "--mechanisms",
        default="up_shock_reversal,up_shock_continuation,down_shock_reversal,down_shock_continuation",
    )
    parser.add_argument("--windows", default="post_1_5,post_1_10,post_6_22,post_1_22")
    parser.add_argument("--shock-thresholds", default="0.05,0.08,0.10")
    parser.add_argument("--volume-spike-thresholds", default="1.0,1.5,2.0")
    parser.add_argument("--adv-min-dollars", default="250000")
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--beam-width", type=int, default=16)
    parser.add_argument("--min-events", type=int, default=50)
    parser.add_argument("--min-event-months", type=int, default=6)
    parser.add_argument("--min-history-observations", type=int, default=60)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--max-rows", type=int, default=750_000, help="Use 0 for full no-cap replay.")
    parser.add_argument("--feature-cache-dir", type=Path, default=None)
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--force-rebuild-cache", action="store_true")
    parser.add_argument("--feature-cache-shards", type=int, default=32)
    parser.add_argument("--feature-cache-chunk-rows", type=int, default=250_000)
    parser.add_argument(
        "--exclude-predicates",
        default="",
        help="Comma-separated predicate ids to remove from the greedy leaf search.",
    )
    parser.add_argument(
        "--exclude-stale-price-events",
        action="store_true",
        help="Remove stale-price and zero-volume candidate events before exploratory leaf search.",
    )
    args = parser.parse_args()

    result = run_small_emotion_full_market_overfit_lab(
        price_panel_path=args.price_panel,
        benchmark_panel_path=args.benchmark_panel,
        delisting_path=args.delisting,
        output_dir=args.output_dir,
        mechanisms=_str_list(args.mechanisms),
        windows=_str_list(args.windows),
        shock_thresholds=_float_list(args.shock_thresholds),
        volume_spike_thresholds=_float_list(args.volume_spike_thresholds),
        adv_min_dollars=_float_list(args.adv_min_dollars),
        max_depth=args.max_depth,
        beam_width=args.beam_width,
        min_events=args.min_events,
        min_event_months=args.min_event_months,
        min_history_observations=args.min_history_observations,
        top_n=args.top_n,
        max_rows=None if args.max_rows == 0 else args.max_rows,
        feature_cache_dir=args.feature_cache_dir,
        cache_only=args.cache_only,
        force_rebuild_cache=args.force_rebuild_cache,
        feature_cache_shards=args.feature_cache_shards,
        feature_cache_chunk_rows=args.feature_cache_chunk_rows,
        excluded_predicates=_str_list(args.exclude_predicates),
        exclude_stale_price_events=args.exclude_stale_price_events,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
