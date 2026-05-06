"""Build WRDS PIT-labeled SUE event panel artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_historical_panel import (  # noqa: E402
    DEFAULT_FULL_OUTPUT_DIR,
    DEFAULT_FULL_REPORT_PATH,
    DEFAULT_SMOKE_OUTPUT_DIR,
    DEFAULT_SMOKE_REPORT_PATH,
    SueHistoricalPanelConfig,
    build_sue_historical_event_panel,
    load_sue_historical_panel_run_config,
    missing_full_mode_inputs,
    write_sue_historical_missing_inputs_artifacts,
    write_sue_historical_panel_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build WRDS PIT-labeled SUE event panel artifacts.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--mode", choices=["smoke", "full"], default="smoke")
    parser.add_argument("--sample-event-count", type=int, default=60)
    parser.add_argument("--fetched-at", default=None)
    parser.add_argument("--earnings-events", default=None)
    parser.add_argument("--estimate-snapshots", default=None)
    parser.add_argument("--security-links", default=None)
    parser.add_argument("--crsp-daily", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument(
        "--report-path",
        default=None,
    )
    parser.add_argument("--strict-missing-inputs", action="store_true")
    args = parser.parse_args()

    if args.config:
        run_config = load_sue_historical_panel_run_config(args.config)
        panel_config = run_config.panel_config
        if args.fetched_at:
            panel_config = panel_config.model_copy(update={"fetched_at": args.fetched_at})
        output_dir = args.output_dir or run_config.output_dir
        report_path = args.report_path or run_config.report_path
    else:
        panel_config = SueHistoricalPanelConfig(
            mode=args.mode,
            sample_event_count=args.sample_event_count,
            fetched_at=args.fetched_at,
            earnings_events_path=args.earnings_events,
            estimate_snapshots_path=args.estimate_snapshots,
            security_links_path=args.security_links,
            crsp_daily_path=args.crsp_daily,
        )
        output_dir = args.output_dir or (
            str(REPO_ROOT / DEFAULT_FULL_OUTPUT_DIR)
            if panel_config.mode == "full"
            else str(REPO_ROOT / DEFAULT_SMOKE_OUTPUT_DIR)
        )
        report_path = args.report_path or (
            str(REPO_ROOT / DEFAULT_FULL_REPORT_PATH)
            if panel_config.mode == "full"
            else str(REPO_ROOT / DEFAULT_SMOKE_REPORT_PATH)
        )

    missing_inputs = missing_full_mode_inputs(panel_config)
    if missing_inputs:
        artifacts = write_sue_historical_missing_inputs_artifacts(
            panel_config,
            output_dir=output_dir,
            report_path=report_path,
        )
        print("mode=full")
        print("status=unavailable")
        print(f"missing_inputs={len(missing_inputs)}")
        print("smoke_fallback_used=False")
        print("production_approval_claimed=False")
        for name, path in artifacts.items():
            print(f"{name}={path}")
        if args.strict_missing_inputs:
            raise SystemExit(2)
        return

    result = build_sue_historical_event_panel(panel_config)
    artifacts = write_sue_historical_panel_artifacts(
        result,
        output_dir=output_dir,
        report_path=report_path,
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
