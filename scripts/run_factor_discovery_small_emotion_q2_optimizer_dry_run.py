from __future__ import annotations

import argparse
import json
from pathlib import Path

from execution_aware_optimizer.small_emotion_q2_optimizer_dry_run import (
    run_small_emotion_q2_optimizer_dry_run,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Q2-SMALL-EMOTION-03 optimizer adapter dry-run.")
    parser.add_argument(
        "--q2-survival-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_execution_survival_20260519"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_optimizer_dry_run_20260520"),
    )
    parser.add_argument("--config-path", type=Path, default=Path("config/us_expanded_alpha_phase_1_5.yaml"))
    parser.add_argument("--constraints-path", type=Path, default=Path("config/constraints/us_public_fund.yaml"))
    parser.add_argument("--execution-path", type=Path, default=Path("config/execution/conservative.yaml"))
    parser.add_argument("--alpha-weight", type=float, default=8.0)
    args = parser.parse_args()

    result = run_small_emotion_q2_optimizer_dry_run(
        q2_survival_dir=args.q2_survival_dir,
        output_dir=args.output_dir,
        config_path=args.config_path,
        constraints_path=args.constraints_path,
        execution_path=args.execution_path,
        alpha_weight=args.alpha_weight,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
