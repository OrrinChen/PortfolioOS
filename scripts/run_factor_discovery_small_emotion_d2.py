from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_d2 import run_small_emotion_d2_observability


DEFAULT_BASE = Path("data/cache/wrds_multifactor/small_cap_us_daily/standardized")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run D2-SMALL-EMOTION-01 no-formula small-cap shock observability."
    )
    parser.add_argument(
        "--price-panel",
        type=Path,
        default=DEFAULT_BASE / "adjusted_price_volume_panel.csv",
        help="PIT daily price/volume panel.",
    )
    parser.add_argument(
        "--benchmark-panel",
        type=Path,
        default=DEFAULT_BASE / "small_cap_benchmark_panel.csv",
        help="Benchmark return panel.",
    )
    parser.add_argument(
        "--delisting",
        type=Path,
        default=DEFAULT_BASE / "delisting_returns.csv",
        help="Delisting return/event panel.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/d2_observability"),
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=750_000,
        help="Optional controlled row cap for local smoke replay. Use 0 for full file.",
    )
    parser.add_argument("--minimum-subset-events", type=int, default=50)
    parser.add_argument("--minimum-event-month-count", type=int, default=12)
    parser.add_argument("--minimum-label-coverage-share", type=float, default=0.70)
    parser.add_argument("--min-history-observations", type=int, default=60)
    parser.add_argument("--min-adv-dollars", type=float, default=250_000.0)
    args = parser.parse_args()

    max_rows = None if args.max_rows == 0 else args.max_rows
    result = run_small_emotion_d2_observability(
        price_panel_path=args.price_panel,
        benchmark_panel_path=args.benchmark_panel,
        delisting_path=args.delisting,
        output_dir=args.output_dir,
        minimum_subset_events=args.minimum_subset_events,
        minimum_event_month_count=args.minimum_event_month_count,
        minimum_label_coverage_share=args.minimum_label_coverage_share,
        min_history_observations=args.min_history_observations,
        min_adv_dollars=args.min_adv_dollars,
        max_rows=max_rows,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
