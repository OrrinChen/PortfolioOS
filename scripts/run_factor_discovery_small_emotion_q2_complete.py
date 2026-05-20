from __future__ import annotations

import argparse
import json
from pathlib import Path

from execution_aware_optimizer.small_emotion_q2_complete import (
    run_small_emotion_q2_complete,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Q2-SMALL-EMOTION-04 execution-survival closeout.")
    parser.add_argument(
        "--q2-intake-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_candidate_intake_20260519"),
    )
    parser.add_argument(
        "--q2-survival-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_execution_survival_20260519"),
    )
    parser.add_argument(
        "--optimizer-dry-run-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_optimizer_dry_run_20260520"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_complete_20260520"),
    )
    args = parser.parse_args()

    result = run_small_emotion_q2_complete(
        q2_intake_dir=args.q2_intake_dir,
        q2_survival_dir=args.q2_survival_dir,
        optimizer_dry_run_dir=args.optimizer_dry_run_dir,
        output_dir=args.output_dir,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
