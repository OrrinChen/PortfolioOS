from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_full_replay import run_small_emotion_chunked_full_replay


DEFAULT_BASE = Path("data/cache/wrds_multifactor/small_cap_us_daily")
DEFAULT_CHUNK_DIR = DEFAULT_BASE / "raw" / "_chunks" / "adjusted_price_volume_panel"
DEFAULT_STANDARDIZED = DEFAULT_BASE / "standardized"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run D2-SMALL-EMOTION-01A chunked full replay with subset-level guards."
    )
    parser.add_argument(
        "--price-chunk-dir",
        type=Path,
        default=DEFAULT_CHUNK_DIR,
        help="Directory containing local chunked daily price panels.",
    )
    parser.add_argument(
        "--price-chunk-glob",
        default="*.csv",
        help="Glob pattern for price chunks inside --price-chunk-dir.",
    )
    parser.add_argument(
        "--benchmark-panel",
        type=Path,
        default=DEFAULT_STANDARDIZED / "small_cap_benchmark_panel.csv",
    )
    parser.add_argument(
        "--delisting",
        type=Path,
        default=DEFAULT_STANDARDIZED / "delisting_returns.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/d2_full_replay"),
    )
    parser.add_argument("--minimum-subset-events", type=int, default=50)
    parser.add_argument("--minimum-event-month-count", type=int, default=12)
    parser.add_argument("--minimum-label-coverage-share", type=float, default=0.70)
    parser.add_argument("--min-history-observations", type=int, default=60)
    parser.add_argument("--min-adv-dollars", type=float, default=250_000.0)
    parser.add_argument("--minimum-observable-chunks", type=int, default=2)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    chunks = sorted(args.price_chunk_dir.glob(args.price_chunk_glob))
    result = run_small_emotion_chunked_full_replay(
        price_chunk_paths=chunks,
        benchmark_panel_path=args.benchmark_panel,
        delisting_path=args.delisting,
        output_dir=args.output_dir,
        minimum_subset_events=args.minimum_subset_events,
        minimum_event_month_count=args.minimum_event_month_count,
        minimum_label_coverage_share=args.minimum_label_coverage_share,
        min_history_observations=args.min_history_observations,
        min_adv_dollars=args.min_adv_dollars,
        minimum_observable_chunks=args.minimum_observable_chunks,
        refresh=args.refresh,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
