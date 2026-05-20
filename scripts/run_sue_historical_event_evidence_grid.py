"""Run the bounded historical SUE event evidence grid."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_historical_event_evidence import (  # noqa: E402
    SueHistoricalEventEvidenceConfig,
    build_sue_historical_event_evidence_grid,
    load_sue_historical_event_evidence_config,
    write_sue_historical_event_evidence_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bounded WRDS/PIT-safe historical SUE event evidence grid.")
    parser.add_argument("--config", default="configs/sue_historical_event_evidence_grid.yaml")
    parser.add_argument("--events-path", default=None)
    parser.add_argument("--sue-values-path", default=None)
    parser.add_argument("--crsp-daily-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--report-path", default=None)
    args = parser.parse_args()

    config = load_sue_historical_event_evidence_config(args.config) if args.config else SueHistoricalEventEvidenceConfig()
    updates = {
        key: value
        for key, value in {
            "events_path": args.events_path,
            "sue_values_path": args.sue_values_path,
            "crsp_daily_path": args.crsp_daily_path,
            "output_dir": args.output_dir,
            "report_path": args.report_path,
        }.items()
        if value is not None
    }
    if updates:
        config = config.model_copy(update=updates)

    result = build_sue_historical_event_evidence_grid(config)
    artifacts = write_sue_historical_event_evidence_artifacts(result)
    print(f"interpretation={result.evidence_summary['interpretation']}")
    print(f"pit_safe_rows={result.evidence_summary['pit_safe_rows']}")
    print(f"safe_rebalance_dates={result.evidence_summary['safe_rebalance_dates']}")
    print(f"best_window={result.evidence_summary['best_window']['window_name']}")
    print(f"production_approval_claimed={result.evidence_summary['production_approval_claimed']}")
    print(f"q2_evaluation_ran={result.evidence_summary['q2_evaluation_ran']}")
    print(f"event_window_grid={artifacts['event_window_grid']}")
    print(f"evidence_summary={artifacts['evidence_summary']}")
    print(f"report={artifacts['report']}")


if __name__ == "__main__":
    main()
