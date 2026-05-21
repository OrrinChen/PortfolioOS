"""Run H1D SUE score-definition diagnostics."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_score_definition_diagnostics import (  # noqa: E402
    SueScoreDefinitionDiagnosticsConfig,
    build_sue_score_definition_diagnostics,
    load_sue_score_definition_diagnostics_config,
    write_sue_score_definition_diagnostics_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SUE score-definition diagnostics.")
    parser.add_argument("--config", default="configs/sue_score_definition_diagnostics.yaml")
    parser.add_argument("--events-path", default=None)
    parser.add_argument("--crsp-daily-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--report-path", default=None)
    args = parser.parse_args()

    config = load_sue_score_definition_diagnostics_config(args.config) if args.config else SueScoreDefinitionDiagnosticsConfig()
    updates = {
        key: value
        for key, value in {
            "events_path": args.events_path,
            "crsp_daily_path": args.crsp_daily_path,
            "output_dir": args.output_dir,
            "report_path": args.report_path,
        }.items()
        if value is not None
    }
    if updates:
        config = config.model_copy(update=updates)

    result = build_sue_score_definition_diagnostics(config)
    artifacts = write_sue_score_definition_diagnostics_artifacts(result)
    summary = result.diagnostic_summary
    print("status=completed")
    print(f"preferred_diagnostic_score={summary['preferred_diagnostic_score']}")
    print(f"raw_eps_diff_scale_warning={summary['raw_eps_diff_scale_warning']}")
    print(f"q2_evaluation_ran={summary['q2_evaluation_ran']}")
    print(f"optimizer_path_evaluation_ran={summary['optimizer_path_evaluation_ran']}")
    print(f"production_approval_claimed={summary['production_approval_claimed']}")
    print(f"score_definition_grid={artifacts['score_definition_grid']}")
    print(f"diagnostic_summary={artifacts['diagnostic_summary']}")
    print(f"report={artifacts['report']}")


if __name__ == "__main__":
    main()
