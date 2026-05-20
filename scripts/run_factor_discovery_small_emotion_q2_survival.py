from __future__ import annotations

import argparse
import json
from pathlib import Path

from execution_aware_optimizer.small_emotion_q2_execution_survival import (
    run_small_emotion_q2_execution_survival,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Q2-SMALL-EMOTION-02 execution-survival diagnostics.")
    parser.add_argument(
        "--q2-intake-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_candidate_intake_20260519"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_execution_survival_20260519"),
    )
    parser.add_argument("--notional-usd", type=float, default=25_000.0)
    parser.add_argument("--stress-notional-usd", type=float, default=100_000.0)
    args = parser.parse_args()

    result = run_small_emotion_q2_execution_survival(
        q2_intake_dir=args.q2_intake_dir,
        output_dir=args.output_dir,
        notional_usd=args.notional_usd,
        stress_notional_usd=args.stress_notional_usd,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
