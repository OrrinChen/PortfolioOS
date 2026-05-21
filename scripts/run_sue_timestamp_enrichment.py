"""Run Reopen-H1E.5 SUE timestamp-source enrichment."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_timestamp_enrichment import (  # noqa: E402
    SueTimestampEnrichmentConfig,
    build_sue_timestamp_enrichment,
    load_sue_timestamp_enrichment_config,
    write_sue_timestamp_enrichment_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SUE timestamp enrichment.")
    parser.add_argument("--config", default="configs/sue_timestamp_enrichment.yaml")
    parser.add_argument("--events-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--report-path", default=None)
    args = parser.parse_args()

    config = load_sue_timestamp_enrichment_config(args.config) if args.config else SueTimestampEnrichmentConfig()
    updates = {
        key: value
        for key, value in {
            "events_path": args.events_path,
            "output_dir": args.output_dir,
            "report_path": args.report_path,
        }.items()
        if value is not None
    }
    if updates:
        config = config.model_copy(update=updates)

    result = build_sue_timestamp_enrichment(config)
    artifacts = write_sue_timestamp_enrichment_artifacts(result)
    decision = result.timestamp_enrichment_decision
    print("status=completed")
    print(f"decision_label={decision['decision_label']}")
    print(f"event_count={decision['event_count']}")
    print(f"repairable_event_count={decision['repairable_event_count']}")
    print(f"selected_score={decision['selected_score']}")
    print(f"q2_evaluation_ran={decision['q2_evaluation_ran']}")
    print(f"optimizer_path_evaluation_ran={decision['optimizer_path_evaluation_ran']}")
    print(f"production_approval_claimed={decision['production_approval_claimed']}")
    print(f"timestamp_source_comparison={artifacts['timestamp_source_comparison']}")
    print(f"timestamp_enrichment_decision={artifacts['timestamp_enrichment_decision']}")
    print(f"report={artifacts['report']}")


if __name__ == "__main__":
    main()
