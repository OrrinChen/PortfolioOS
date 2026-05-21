"""Run Reopen-H1E.4 SUE announcement timestamp policy audit."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_announcement_timestamp_policy import (  # noqa: E402
    SueAnnouncementTimestampPolicyConfig,
    build_sue_announcement_timestamp_policy_audit,
    load_sue_announcement_timestamp_policy_config,
    write_sue_announcement_timestamp_policy_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SUE announcement timestamp policy audit.")
    parser.add_argument("--config", default="configs/sue_announcement_timestamp_policy.yaml")
    parser.add_argument("--events-path", default=None)
    parser.add_argument("--crsp-daily-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--report-path", default=None)
    args = parser.parse_args()

    config = (
        load_sue_announcement_timestamp_policy_config(args.config)
        if args.config
        else SueAnnouncementTimestampPolicyConfig()
    )
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

    result = build_sue_announcement_timestamp_policy_audit(config)
    artifacts = write_sue_announcement_timestamp_policy_artifacts(result)
    decision = result.timing_policy_decision
    print("status=completed")
    print(f"decision_label={decision['decision_label']}")
    print(f"event_count={decision['event_count']}")
    print(f"auditable_source_event_count={decision['auditable_source_event_count']}")
    print(f"repaired_event_count={decision['repaired_event_count']}")
    print(f"selected_score={decision['selected_score']}")
    print(f"q2_evaluation_ran={decision['q2_evaluation_ran']}")
    print(f"optimizer_path_evaluation_ran={decision['optimizer_path_evaluation_ran']}")
    print(f"production_approval_claimed={decision['production_approval_claimed']}")
    print(f"timestamp_source_comparison={artifacts['timestamp_source_comparison']}")
    print(f"timing_policy_decision={artifacts['timing_policy_decision']}")
    print(f"report={artifacts['report']}")


if __name__ == "__main__":
    main()
