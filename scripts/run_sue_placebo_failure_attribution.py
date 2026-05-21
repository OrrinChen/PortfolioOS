"""Run Reopen-H1E.1 SUE event-date-shift placebo failure attribution."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_placebo_failure_attribution import (  # noqa: E402
    SuePlaceboFailureAttributionConfig,
    build_sue_placebo_failure_attribution,
    load_sue_placebo_failure_attribution_config,
    write_sue_placebo_failure_attribution_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SUE placebo-failure attribution diagnostics.")
    parser.add_argument("--config", default="configs/sue_placebo_failure_attribution.yaml")
    parser.add_argument("--events-path", default=None)
    parser.add_argument("--crsp-daily-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--score-name", default=None)
    args = parser.parse_args()

    config = (
        load_sue_placebo_failure_attribution_config(args.config)
        if args.config
        else SuePlaceboFailureAttributionConfig()
    )
    updates = {
        key: value
        for key, value in {
            "events_path": args.events_path,
            "crsp_daily_path": args.crsp_daily_path,
            "output_dir": args.output_dir,
            "report_path": args.report_path,
            "score_name": args.score_name,
        }.items()
        if value is not None
    }
    if updates:
        config = config.model_copy(update=updates)

    result = build_sue_placebo_failure_attribution(config)
    artifacts = write_sue_placebo_failure_attribution_artifacts(result)
    summary = result.attribution_summary
    print("status=completed")
    print(f"score_name={summary['score_name']}")
    print(f"interpretation={summary['interpretation']}")
    print(f"best_placebo_shift_trading_days={summary['best_placebo_shift_trading_days']}")
    print(f"q2_evaluation_ran={summary['q2_evaluation_ran']}")
    print(f"optimizer_path_evaluation_ran={summary['optimizer_path_evaluation_ran']}")
    print(f"production_approval_claimed={summary['production_approval_claimed']}")
    print(f"placebo_shift_curve={artifacts['placebo_shift_curve']}")
    print(f"attribution_summary={artifacts['attribution_summary']}")
    print(f"report={artifacts['report']}")


if __name__ == "__main__":
    main()
