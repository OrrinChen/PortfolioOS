from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_direction_remap import (
    run_small_emotion_direction_remap_audit,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run D2-SMALL-EMOTION-01B no-formula shock-direction remap audit."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/d2_observability"),
        help="Existing D2-SMALL-EMOTION artifact directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/d2_direction_remap"),
    )
    parser.add_argument("--minimum-subset-events", type=int, default=50)
    parser.add_argument("--minimum-event-month-count", type=int, default=12)
    parser.add_argument("--minimum-label-coverage-share", type=float, default=0.70)
    args = parser.parse_args()

    result = run_small_emotion_direction_remap_audit(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        minimum_subset_events=args.minimum_subset_events,
        minimum_event_month_count=args.minimum_event_month_count,
        minimum_label_coverage_share=args.minimum_label_coverage_share,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
