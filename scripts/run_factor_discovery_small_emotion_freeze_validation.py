from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_freeze_validation import run_small_emotion_freeze_validation


PRIOR_MEASUREMENT_SPEC_HASH = "eb56b3e27b0e0b397e3143b7a01e0d8e089b25a560dbc53dcf7ee94f51d2b976"


def _str_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SMALL-EMOTION-FREEZE-02 locked validation.")
    parser.add_argument(
        "--top-pockets",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/e1_full_market_overfit_lab_full_cached/top_50_overfit_pockets.csv"),
    )
    parser.add_argument(
        "--search-grid",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/e1_full_market_overfit_lab_full_cached/full_market_overfit_grid.csv"),
    )
    parser.add_argument(
        "--feature-cache-dir",
        type=Path,
        default=Path("data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full"),
    )
    parser.add_argument(
        "--prior-measurement-spec",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/d4_sharpened_measurement_spec/measurement_spec.yaml"),
    )
    parser.add_argument("--prior-measurement-spec-hash", default=PRIOR_MEASUREMENT_SPEC_HASH)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/freeze_02_full_market_locked_validation"),
    )
    parser.add_argument("--random-seed", type=int, default=20260517)
    parser.add_argument("--min-events", type=int, default=50)
    parser.add_argument("--min-event-months", type=int, default=6)
    parser.add_argument(
        "--exclude-predicates",
        default="",
        help="Comma-separated predicate ids to remove from placebo-selected freeze audits.",
    )
    parser.add_argument(
        "--exclude-stale-price-events",
        action="store_true",
        help="Remove stale-price and zero-volume candidate events before locked validation and placebo search.",
    )
    args = parser.parse_args()

    result = run_small_emotion_freeze_validation(
        top_pockets_path=args.top_pockets,
        search_grid_path=args.search_grid,
        feature_cache_dir=args.feature_cache_dir,
        prior_measurement_spec_path=args.prior_measurement_spec,
        prior_measurement_spec_hash=args.prior_measurement_spec_hash,
        output_dir=args.output_dir,
        random_seed=args.random_seed,
        min_events=args.min_events,
        min_event_months=args.min_event_months,
        excluded_predicates=_str_list(args.exclude_predicates),
        exclude_stale_price_events=args.exclude_stale_price_events,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
