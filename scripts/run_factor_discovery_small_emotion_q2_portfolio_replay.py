from __future__ import annotations

import argparse
import json
from pathlib import Path

from execution_aware_optimizer.small_emotion_q2_portfolio_replay import (
    run_small_emotion_q2_portfolio_replay,
)


DEFAULT_Q1_PANELS = {
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
    parser = argparse.ArgumentParser(description="Run Q2-SMALL-EMOTION-05 portfolio quant replay.")
    parser.add_argument(
        "--q2-complete-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_complete_20260520"),
    )
    parser.add_argument(
        "--q2-intake-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_candidate_intake_20260519"),
    )
    parser.add_argument(
        "--q1-window-panel",
        action="append",
        default=[],
        help="Candidate-to-Q1 panel mapping in candidate_name|path format. Defaults cover the three promoted candidates.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_portfolio_replay_20260520"),
    )
    parser.add_argument("--notional-usd", type=float, default=25_000.0)
    parser.add_argument("--minimum-event-count", type=int, default=100)
    parser.add_argument("--minimum-event-month-count", type=int, default=24)
    args = parser.parse_args()

    q1_panels = dict(DEFAULT_Q1_PANELS)
    for item in args.q1_window_panel:
        candidate, separator, path = str(item).partition("|")
        if not candidate or not separator or not path:
            raise SystemExit("--q1-window-panel must use candidate_name|path format")
        q1_panels[candidate] = Path(path)

    result = run_small_emotion_q2_portfolio_replay(
        q2_complete_dir=args.q2_complete_dir,
        q2_intake_dir=args.q2_intake_dir,
        q1_window_panels=q1_panels,
        output_dir=args.output_dir,
        notional_usd=args.notional_usd,
        minimum_event_count=args.minimum_event_count,
        minimum_event_month_count=args.minimum_event_month_count,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
