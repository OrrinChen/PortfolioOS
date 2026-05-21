from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_leaf_search import run_small_emotion_leaf_search


DEFAULT_BASE = Path("data/cache/wrds_multifactor/small_cap_us_daily/standardized")


def _str_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run E0-SMALL-EMOTION-05 greedy leaf search.")
    parser.add_argument("--price-panel", type=Path, default=DEFAULT_BASE / "adjusted_price_volume_panel.csv")
    parser.add_argument("--benchmark-panel", type=Path, default=DEFAULT_BASE / "small_cap_benchmark_panel.csv")
    parser.add_argument("--delisting", type=Path, default=DEFAULT_BASE / "delisting_returns.csv")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/e0_leaf_search"),
    )
    parser.add_argument(
        "--mechanisms",
        default="up_shock_reversal,up_shock_continuation,down_shock_reversal,down_shock_continuation",
    )
    parser.add_argument("--windows", default="post_1_5,post_1_10,post_6_22,post_1_22")
    parser.add_argument("--base-shock-threshold", type=float, default=0.05)
    parser.add_argument("--base-volume-spike-threshold", type=float, default=1.5)
    parser.add_argument("--adv-min-dollars", type=float, default=250_000.0)
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--beam-width", type=int, default=8)
    parser.add_argument("--min-events", type=int, default=50)
    parser.add_argument("--min-event-months", type=int, default=3)
    parser.add_argument("--min-history-observations", type=int, default=60)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--max-rows", type=int, default=750_000, help="Use 0 for full single-file replay.")
    args = parser.parse_args()

    result = run_small_emotion_leaf_search(
        price_panel_path=args.price_panel,
        benchmark_panel_path=args.benchmark_panel,
        delisting_path=args.delisting,
        output_dir=args.output_dir,
        mechanisms=_str_list(args.mechanisms),
        windows=_str_list(args.windows),
        base_shock_threshold=args.base_shock_threshold,
        base_volume_spike_threshold=args.base_volume_spike_threshold,
        adv_min_dollars=args.adv_min_dollars,
        max_depth=args.max_depth,
        beam_width=args.beam_width,
        min_events=args.min_events,
        min_event_months=args.min_event_months,
        min_history_observations=args.min_history_observations,
        top_n=args.top_n,
        max_rows=None if args.max_rows == 0 else args.max_rows,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
