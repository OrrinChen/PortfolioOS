"""Run the local SUE typed Q2 survival matrix fixture."""

from __future__ import annotations

import argparse
from pathlib import Path

from execution_aware_optimizer.sue_typed_q2_survival import (
    run_sue_typed_q2_survival,
    write_sue_typed_q2_survival_artifacts,
)
from execution_aware_optimizer.sue_typed_q2_survival_schema import SueTypedQ2SurvivalInput


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "sue_survival"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local SUE typed Q2 survival matrix fixture.")
    parser.add_argument("--run-id", default="sue_typed_q2_survival")
    parser.add_argument("--q2-input", default=str(FIXTURE_DIR / "q2_input_contract_v2.json"))
    parser.add_argument("--expected-return-panel", default=str(FIXTURE_DIR / "expected_return_panel.csv"))
    parser.add_argument("--projection-manifest", default=str(FIXTURE_DIR / "projection_manifest.json"))
    parser.add_argument(
        "--local-backtest-manifest",
        default=str(REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"),
    )
    parser.add_argument("--adapter-config", default=str(FIXTURE_DIR / "adapter_config.yaml"))
    parser.add_argument("--local-rebalance-date", default="2026-02-27")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "sue_typed_q2_survival"),
    )
    parser.add_argument(
        "--allow-portfolioos-run",
        action="store_true",
        help="Explicitly run local PortfolioOS SUE survival diagnostics. No network or broker access is used.",
    )
    args = parser.parse_args()

    survival_input = SueTypedQ2SurvivalInput.model_validate(
        {
            "adapter_config_path": args.adapter_config,
            "allow_portfolioos_run": args.allow_portfolioos_run,
            "expected_return_panel_path": args.expected_return_panel,
            "local_backtest_manifest_path": args.local_backtest_manifest,
            "local_rebalance_date": args.local_rebalance_date,
            "no_broker": True,
            "no_network": True,
            "projection_manifest_path": args.projection_manifest,
            "q2_input_contract_v2_path": args.q2_input,
            "run_id": args.run_id,
        }
    )
    result = run_sue_typed_q2_survival(survival_input)
    artifacts = write_sue_typed_q2_survival_artifacts(result, args.output_dir)
    print(f"survival_status={result.survival_status}")
    print(f"injection_status={result.injection_status}")
    print(f"expected_return_reached_optimizer_input={result.expected_return_reached_optimizer_input}")
    print(f"optimizer_rebalance_date={result.optimizer_rebalance_date}")
    print(f"q2_observed_rows={result.q2_observed_rows}")
    print(f"q2_unavailable_rows={result.q2_unavailable_rows}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
