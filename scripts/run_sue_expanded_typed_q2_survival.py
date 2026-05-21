"""Run Phase 56A expanded SUE typed-Q2 candidate benchmark."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "projects" / "execution_aware_optimizer" / "src"))

from execution_aware_optimizer.sue_expanded_survival_schema import SueExpandedTypedQ2SurvivalInput
from execution_aware_optimizer.sue_expanded_typed_q2_survival import (
    run_sue_expanded_typed_q2_survival,
    write_sue_expanded_typed_q2_survival_artifacts,
)


FIXTURE_CONFIG = REPO_ROOT / "projects" / "typed_alpha_pilot" / "fixtures" / "sue_expanded" / "fixture_config.json"
SURVIVAL_FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "sue_expanded_survival"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run expanded SUE typed-Q2 candidate benchmark.")
    parser.add_argument("--run-id", default="sue_expanded_typed_q2_candidate")
    parser.add_argument("--fixture-config", default=str(FIXTURE_CONFIG))
    parser.add_argument("--adapter-config", default=str(SURVIVAL_FIXTURE_DIR / "adapter_config.yaml"))
    parser.add_argument(
        "--local-backtest-manifest",
        default=str(REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"),
    )
    parser.add_argument("--local-rebalance-date", default="2026-02-27")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "sue_expanded_typed_q2_survival"))
    parser.add_argument(
        "--report-path",
        default=str(REPO_ROOT / "reports" / "sue_expanded_typed_q2_survival_report.md"),
    )
    parser.add_argument("--allow-portfolioos-run", action="store_true")
    args = parser.parse_args()

    survival_input = SueExpandedTypedQ2SurvivalInput.model_validate(
        {
            "adapter_config_path": args.adapter_config,
            "allow_portfolioos_run": args.allow_portfolioos_run,
            "fixture_config_path": args.fixture_config,
            "local_backtest_manifest_path": args.local_backtest_manifest,
            "local_rebalance_date": args.local_rebalance_date,
            "no_broker": True,
            "no_network": True,
            "run_id": args.run_id,
        }
    )
    result = run_sue_expanded_typed_q2_survival(survival_input)
    artifacts = write_sue_expanded_typed_q2_survival_artifacts(
        result,
        args.output_dir,
        report_path=args.report_path,
    )

    print(f"survival_status={result.survival_status}")
    print(f"injection_status={result.injection_status}")
    print(f"event_count={result.event_count}")
    print(f"rebalance_date_count={result.rebalance_date_count}")
    print(f"active_rebalance_count={result.active_rebalance_count}")
    print(f"median_active_names_per_active_date={result.median_active_names_per_active_date:.2f}")
    print(f"q2_observed_rows={result.q2_observed_rows}")
    print(f"q2_unavailable_rows={result.q2_unavailable_rows}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
