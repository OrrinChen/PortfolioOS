from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_q1_oos import run_small_emotion_q1_oos_review


DEFAULT_BASE = Path("data/cache/wrds_multifactor/small_cap_us_daily/standardized")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Q1-SMALL-EMOTION-01 falsifier/OOS review.")
    parser.add_argument(
        "--measurement-spec",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/d4_sharpened_measurement_spec/measurement_spec.yaml"),
    )
    parser.add_argument("--price-panel", type=Path, default=DEFAULT_BASE / "adjusted_price_volume_panel.csv")
    parser.add_argument("--benchmark-panel", type=Path, default=DEFAULT_BASE / "small_cap_benchmark_panel.csv")
    parser.add_argument("--delisting", type=Path, default=DEFAULT_BASE / "delisting_returns.csv")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q1_sharpened_up_shock_reversal_oos"),
    )
    parser.add_argument("--minimum-event-count", type=int, default=50)
    parser.add_argument("--minimum-event-month-count", type=int, default=3)
    parser.add_argument("--minimum-oos-event-count", type=int, default=10)
    parser.add_argument("--min-history-observations", type=int, default=60)
    parser.add_argument("--random-seed", type=int, default=20260514)
    parser.add_argument("--max-falsifier-events", type=int, default=5_000)
    parser.add_argument("--max-rows", type=int, default=750_000, help="Use 0 for full single-file Q1 review.")
    parser.add_argument(
        "--exclude-stale-price-events",
        action="store_true",
        help="Exclude candidate event rows with any 5-day stale-close flag or zero volume.",
    )
    args = parser.parse_args()

    result = run_small_emotion_q1_oos_review(
        measurement_spec_path=args.measurement_spec,
        price_panel_path=args.price_panel,
        benchmark_panel_path=args.benchmark_panel,
        delisting_path=args.delisting,
        output_dir=args.output_dir,
        min_history_observations=args.min_history_observations,
        minimum_event_count=args.minimum_event_count,
        minimum_event_month_count=args.minimum_event_month_count,
        minimum_oos_event_count=args.minimum_oos_event_count,
        random_seed=args.random_seed,
        max_falsifier_events=args.max_falsifier_events,
        max_rows=None if args.max_rows == 0 else args.max_rows,
        exclude_stale_price_events=args.exclude_stale_price_events,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
