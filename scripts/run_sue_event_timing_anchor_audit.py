"""Run Reopen-H1E.3 SUE event timing / anchor definition audit."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_event_timing_anchor_audit import (  # noqa: E402
    SueEventTimingAnchorAuditConfig,
    build_sue_event_timing_anchor_audit,
    load_sue_event_timing_anchor_audit_config,
    write_sue_event_timing_anchor_audit_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SUE event timing / anchor audit.")
    parser.add_argument("--config", default="configs/sue_event_timing_anchor_audit.yaml")
    parser.add_argument("--events-path", default=None)
    parser.add_argument("--crsp-daily-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--score-name", default=None)
    args = parser.parse_args()

    config = (
        load_sue_event_timing_anchor_audit_config(args.config)
        if args.config
        else SueEventTimingAnchorAuditConfig()
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

    result = build_sue_event_timing_anchor_audit(config)
    artifacts = write_sue_event_timing_anchor_audit_artifacts(result)
    diagnostic = result.anchor_selection_diagnostic
    print("status=completed")
    print(f"score_name={diagnostic['score_name']}")
    print(f"interpretation={diagnostic['interpretation']}")
    print(f"selected_score={diagnostic['selected_score']}")
    print(f"best_anchor_definition={diagnostic['best_anchor_definition']}")
    print(f"best_pre_event_window={diagnostic['best_pre_event_window']}")
    print(f"q2_evaluation_ran={diagnostic['q2_evaluation_ran']}")
    print(f"optimizer_path_evaluation_ran={diagnostic['optimizer_path_evaluation_ran']}")
    print(f"production_approval_claimed={diagnostic['production_approval_claimed']}")
    print(f"anchor_grid={artifacts['anchor_grid']}")
    print(f"pre_event_drift_grid={artifacts['pre_event_drift_grid']}")
    print(f"report={artifacts['report']}")


if __name__ == "__main__":
    main()
