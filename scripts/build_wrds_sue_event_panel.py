"""Build WRDS PIT-labeled SUE event panel artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_historical_panel import (  # noqa: E402
    SueHistoricalPanelConfig,
    build_sue_historical_event_panel,
    write_sue_historical_panel_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build WRDS PIT-labeled SUE event panel artifacts.")
    parser.add_argument("--mode", choices=["smoke", "full"], default="smoke")
    parser.add_argument("--sample-event-count", type=int, default=60)
    parser.add_argument("--fetched-at", default=None)
    parser.add_argument("--earnings-events", default=None)
    parser.add_argument("--estimate-snapshots", default=None)
    parser.add_argument("--security-links", default=None)
    parser.add_argument("--crsp-daily", default=None)
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "sue_historical_event_panel"))
    parser.add_argument(
        "--report-path",
        default=str(REPO_ROOT / "reports" / "sue_historical_event_panel_report.md"),
    )
    args = parser.parse_args()

    result = build_sue_historical_event_panel(
        SueHistoricalPanelConfig(
            mode=args.mode,
            sample_event_count=args.sample_event_count,
            fetched_at=args.fetched_at,
            earnings_events_path=args.earnings_events,
            estimate_snapshots_path=args.estimate_snapshots,
            security_links_path=args.security_links,
            crsp_daily_path=args.crsp_daily,
        )
    )
    artifacts = write_sue_historical_panel_artifacts(
        result,
        output_dir=args.output_dir,
        report_path=args.report_path,
    )
    print(f"mode={result.mode}")
    print(f"event_count={result.event_count}")
    print(f"rebalance_date_count={result.rebalance_date_count}")
    print(f"linked_rows={result.coverage_report['linked_rows']}")
    print(f"unlinked_rows={result.coverage_report['unlinked_rows']}")
    print(f"missing_estimates={result.coverage_report['missing_estimates']}")
    print(f"missing_actuals={result.coverage_report['missing_actuals']}")
    print(f"missing_prices={result.coverage_report['missing_prices']}")
    print(f"diagnostic_only_rows={result.coverage_report['diagnostic_only_rows']}")
    print("production_approval_claimed=False")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
