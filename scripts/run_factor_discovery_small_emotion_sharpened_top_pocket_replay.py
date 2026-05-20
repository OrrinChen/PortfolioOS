from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_sharpened_top_pocket_replay import (
    run_small_emotion_sharpened_top_pocket_replay,
)


DEFAULT_BASE = Path("data/cache/wrds_multifactor/small_cap_us_daily")
DEFAULT_CHUNK_DIR = DEFAULT_BASE / "raw" / "_chunks" / "adjusted_price_volume_panel"
DEFAULT_STANDARDIZED = DEFAULT_BASE / "standardized"


def _float_or_none(value: str) -> float | None:
    value = value.strip()
    if value.lower() in {"", "none", "null", "all"}:
        return None
    return float(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run E0-SMALL-EMOTION-04A sharpened top-pocket chunked replay."
    )
    parser.add_argument("--price-chunk-dir", type=Path, default=DEFAULT_CHUNK_DIR)
    parser.add_argument("--price-chunk-glob", default="*.csv")
    parser.add_argument("--benchmark-panel", type=Path, default=DEFAULT_STANDARDIZED / "small_cap_benchmark_panel.csv")
    parser.add_argument("--delisting", type=Path, default=DEFAULT_STANDARDIZED / "delisting_returns.csv")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/e0_sharpened_top_pocket_replay"),
    )
    parser.add_argument("--mechanism", default="up_shock_reversal")
    parser.add_argument("--shock-threshold", type=float, default=0.05)
    parser.add_argument("--volume-spike-threshold", type=float, default=1.5)
    parser.add_argument("--prior-5d-min-return", default="0.20")
    parser.add_argument("--prior-20d-min-return", default="none")
    parser.add_argument("--close-location-filter", default="all")
    parser.add_argument("--low-price-filter", default="all")
    parser.add_argument("--market-cap-bucket", default="micro")
    parser.add_argument("--liquidity-filter", default="all")
    parser.add_argument("--spread-filter", default="all")
    parser.add_argument("--regime-filter", default="market_up_20d")
    parser.add_argument("--adv-min-dollars", type=float, default=250_000.0)
    parser.add_argument("--window", default="post_1_22")
    parser.add_argument("--min-history-observations", type=int, default=60)
    parser.add_argument("--minimum-observed-chunks", type=int, default=3)
    parser.add_argument("--minimum-positive-chunks", type=int, default=3)
    parser.add_argument("--minimum-aggregate-events", type=int, default=50)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    chunks = sorted(args.price_chunk_dir.glob(args.price_chunk_glob))
    result = run_small_emotion_sharpened_top_pocket_replay(
        price_chunk_paths=chunks,
        benchmark_panel_path=args.benchmark_panel,
        delisting_path=args.delisting,
        output_dir=args.output_dir,
        mechanism=args.mechanism,
        shock_threshold=args.shock_threshold,
        volume_spike_threshold=args.volume_spike_threshold,
        prior_5d_min_return=_float_or_none(args.prior_5d_min_return),
        prior_20d_min_return=_float_or_none(args.prior_20d_min_return),
        close_location_filter=args.close_location_filter,
        low_price_filter=args.low_price_filter,
        market_cap_bucket=args.market_cap_bucket,
        liquidity_filter=args.liquidity_filter,
        spread_filter=args.spread_filter,
        regime_filter=args.regime_filter,
        adv_min_dollars=args.adv_min_dollars,
        window=args.window,
        min_history_observations=args.min_history_observations,
        minimum_observed_chunks=args.minimum_observed_chunks,
        minimum_positive_chunks=args.minimum_positive_chunks,
        minimum_aggregate_events=args.minimum_aggregate_events,
        refresh=args.refresh,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
