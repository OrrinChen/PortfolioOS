"""Run Reopen-H1E.2 SUE market-regime placebo filter check."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_regime_filter_placebo_check import (  # noqa: E402
    SueRegimeFilterPlaceboConfig,
    build_sue_regime_filter_placebo_check,
    load_sue_regime_filter_placebo_config,
    write_sue_regime_filter_placebo_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SUE market-regime placebo filter diagnostics.")
    parser.add_argument("--config", default="configs/sue_regime_filter_placebo_check.yaml")
    parser.add_argument("--events-path", default=None)
    parser.add_argument("--crsp-daily-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--score-name", default=None)
    args = parser.parse_args()

    config = (
        load_sue_regime_filter_placebo_config(args.config)
        if args.config
        else SueRegimeFilterPlaceboConfig()
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

    result = build_sue_regime_filter_placebo_check(config)
    artifacts = write_sue_regime_filter_placebo_artifacts(result)
    summary = result.regime_filter_summary
    print("status=completed")
    print(f"score_name={summary['score_name']}")
    print(f"interpretation={summary['interpretation']}")
    print(f"selected_score={summary['selected_score']}")
    print(f"low_liquidity_filter_source={summary['low_liquidity_filter_source']}")
    print(f"q2_evaluation_ran={summary['q2_evaluation_ran']}")
    print(f"optimizer_path_evaluation_ran={summary['optimizer_path_evaluation_ran']}")
    print(f"production_approval_claimed={summary['production_approval_claimed']}")
    print(f"score_gate_summary={artifacts['score_gate_summary']}")
    print(f"filtered_placebo_shift_curve={artifacts['filtered_placebo_shift_curve']}")
    print(f"report={artifacts['report']}")


if __name__ == "__main__":
    main()
