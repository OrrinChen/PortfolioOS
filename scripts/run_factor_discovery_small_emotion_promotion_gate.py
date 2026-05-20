from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_promotion_gate import run_small_emotion_promotion_gate
from factor_discovery_sandbox.small_emotion_q1_oos import run_small_emotion_q1_oos_review


DEFAULT_BASE = Path("data/cache/wrds_multifactor/small_cap_us_daily/standardized")
REQUIRED_MEASUREMENT_SPEC_HASH = "eb56b3e27b0e0b397e3143b7a01e0d8e089b25a560dbc53dcf7ee94f51d2b976"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run PG-SMALL-EMOTION-01 Promotion Gate.")
    parser.add_argument(
        "--measurement-spec",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/d4_sharpened_measurement_spec/measurement_spec.yaml"),
    )
    parser.add_argument("--price-panel", type=Path, default=DEFAULT_BASE / "adjusted_price_volume_panel.csv")
    parser.add_argument("--benchmark-panel", type=Path, default=DEFAULT_BASE / "small_cap_benchmark_panel.csv")
    parser.add_argument("--delisting", type=Path, default=DEFAULT_BASE / "delisting_returns.csv")
    parser.add_argument(
        "--q1-output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q1_sharpened_up_shock_reversal_full_oos"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/pg_sharpened_up_shock_reversal"),
    )
    parser.add_argument(
        "--search-grid",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/e0_sharpening_sweep/sharpening_sweep_grid.csv"),
    )
    parser.add_argument("--required-measurement-spec-hash", default=REQUIRED_MEASUREMENT_SPEC_HASH)
    parser.add_argument("--q1-max-rows", type=int, default=0, help="Promotion Gate defaults to full no-cap Q1.")
    parser.add_argument("--max-falsifier-events", type=int, default=5_000)
    parser.add_argument("--skip-q1-replay", action="store_true", help="Use an existing Q1 output directory.")
    args = parser.parse_args()

    if not args.skip_q1_replay:
        run_small_emotion_q1_oos_review(
            measurement_spec_path=args.measurement_spec,
            price_panel_path=args.price_panel,
            benchmark_panel_path=args.benchmark_panel,
            delisting_path=args.delisting,
            output_dir=args.q1_output_dir,
            max_rows=None if args.q1_max_rows == 0 else args.q1_max_rows,
            max_falsifier_events=args.max_falsifier_events,
        )

    result = run_small_emotion_promotion_gate(
        measurement_spec_path=args.measurement_spec,
        q1_output_dir=args.q1_output_dir,
        output_dir=args.output_dir,
        required_measurement_spec_hash=args.required_measurement_spec_hash,
        search_grid_path=args.search_grid,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
