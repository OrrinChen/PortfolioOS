from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_d3_charter import write_small_emotion_d3_charter


DEFAULT_REPLAY_DIR = Path("outputs/factor_discovery/small_emotion/e0_top_pocket_replay")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Write D3-SMALL-EMOTION-03 candidate charter from E0 top-pocket replay."
    )
    parser.add_argument(
        "--freeze-review",
        type=Path,
        default=DEFAULT_REPLAY_DIR / "candidate_freeze_review.json",
    )
    parser.add_argument(
        "--top-pocket-summary",
        type=Path,
        default=DEFAULT_REPLAY_DIR / "top_pocket_replay_summary.json",
    )
    parser.add_argument(
        "--chunk-metrics",
        type=Path,
        default=DEFAULT_REPLAY_DIR / "top_pocket_chunk_metrics.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/d3_up_shock_reversal_charter"),
    )
    args = parser.parse_args()

    result = write_small_emotion_d3_charter(
        freeze_review_path=args.freeze_review,
        top_pocket_summary_path=args.top_pocket_summary,
        chunk_metrics_path=args.chunk_metrics,
        output_dir=args.output_dir,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
