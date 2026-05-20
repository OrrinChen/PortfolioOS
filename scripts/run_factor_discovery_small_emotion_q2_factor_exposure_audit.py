from __future__ import annotations

import argparse
import json
from pathlib import Path

from execution_aware_optimizer.small_emotion_q2_factor_exposure_audit import (
    run_small_emotion_q2_factor_exposure_audit,
)


DEFAULT_Q1_EVENT_PANELS = {
    "rank1_micro_post_1_22": Path(
        "outputs/factor_discovery/small_emotion/q1_profile_rank1_micro_post_1_22_oos_20260519/q1_event_panel.csv"
    ),
    "rank2_broad_post_1_22": Path(
        "outputs/factor_discovery/small_emotion/q1_profile_rank2_broad_post_1_22_oos_20260518/q1_event_panel.csv"
    ),
    "rank3_broad_post_1_10": Path(
        "outputs/factor_discovery/small_emotion/q1_profile_rank3_broad_post_1_10_oos_20260519/q1_event_panel.csv"
    ),
}

DEFAULT_Q1_WINDOW_PANELS = {
    "rank1_micro_post_1_22": Path(
        "outputs/factor_discovery/small_emotion/q1_profile_rank1_micro_post_1_22_oos_20260519/q1_window_return_panel.csv"
    ),
    "rank2_broad_post_1_22": Path(
        "outputs/factor_discovery/small_emotion/q1_profile_rank2_broad_post_1_22_oos_20260518/q1_window_return_panel.csv"
    ),
    "rank3_broad_post_1_10": Path(
        "outputs/factor_discovery/small_emotion/q1_profile_rank3_broad_post_1_10_oos_20260519/q1_window_return_panel.csv"
    ),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Q2-SMALL-EMOTION-06 factor exposure / beta residual audit.")
    parser.add_argument(
        "--q2-complete-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_complete_20260520"),
    )
    parser.add_argument(
        "--q1-event-panel",
        action="append",
        default=[],
        help="Candidate-to-Q1 event panel mapping in candidate_name|path format. Defaults cover the three Q2 candidates.",
    )
    parser.add_argument(
        "--q1-window-panel",
        action="append",
        default=[],
        help="Candidate-to-Q1 window panel mapping in candidate_name|path format. Defaults cover the three Q2 candidates.",
    )
    parser.add_argument(
        "--price-panel",
        type=Path,
        default=Path("data/cache/wrds_multifactor/small_cap_us_daily/standardized/adjusted_price_volume_panel.csv"),
    )
    parser.add_argument(
        "--benchmark-panel",
        type=Path,
        default=Path("data/cache/wrds_multifactor/small_cap_us_daily/standardized/small_cap_benchmark_panel.csv"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_factor_exposure_audit_20260520"),
    )
    parser.add_argument("--minimum-event-count", type=int, default=100)
    parser.add_argument("--beta-lookback-days", type=int, default=60)
    args = parser.parse_args()

    q1_event_panels = dict(DEFAULT_Q1_EVENT_PANELS)
    for item in args.q1_event_panel:
        candidate, separator, path = str(item).partition("|")
        if not candidate or not separator or not path:
            raise SystemExit("--q1-event-panel must use candidate_name|path format")
        q1_event_panels[candidate] = Path(path)

    q1_window_panels = dict(DEFAULT_Q1_WINDOW_PANELS)
    for item in args.q1_window_panel:
        candidate, separator, path = str(item).partition("|")
        if not candidate or not separator or not path:
            raise SystemExit("--q1-window-panel must use candidate_name|path format")
        q1_window_panels[candidate] = Path(path)

    result = run_small_emotion_q2_factor_exposure_audit(
        q2_complete_dir=args.q2_complete_dir,
        q1_event_panels=q1_event_panels,
        q1_window_panels=q1_window_panels,
        price_panel_path=args.price_panel,
        benchmark_panel_path=args.benchmark_panel,
        output_dir=args.output_dir,
        minimum_event_count=args.minimum_event_count,
        beta_lookback_days=args.beta_lookback_days,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
