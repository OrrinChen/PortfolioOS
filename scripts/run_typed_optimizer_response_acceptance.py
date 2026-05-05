"""Run the local typed optimizer response acceptance suite."""

from __future__ import annotations

import argparse
from pathlib import Path

from execution_aware_optimizer.typed_optimizer_response import (
    run_typed_optimizer_response_acceptance,
    write_typed_optimizer_response_artifacts,
)
from execution_aware_optimizer.typed_optimizer_response_schema import TypedOptimizerResponseInput


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "typed_injection"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local typed optimizer response acceptance suite.")
    parser.add_argument("--run-id", default="typed_optimizer_response_acceptance")
    parser.add_argument("--q2-input", default=str(FIXTURE_DIR / "q2_input_contract_v2.json"))
    parser.add_argument("--expected-return-panel", default=str(FIXTURE_DIR / "expected_return_panel.csv"))
    parser.add_argument("--projection-manifest", default=str(FIXTURE_DIR / "projection_manifest.json"))
    parser.add_argument(
        "--local-backtest-manifest",
        default=str(REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"),
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "typed_optimizer_response_acceptance"),
    )
    parser.add_argument("--rebalance-date", default=None)
    parser.add_argument("--base-expected-return-unit", type=float, default=0.01)
    parser.add_argument(
        "--allow-portfolioos-run",
        action="store_true",
        help="Explicitly run local PortfolioOS optimizer response diagnostics. No network or broker access is used.",
    )
    args = parser.parse_args()

    response_input = TypedOptimizerResponseInput.model_validate(
        {
            "allow_portfolioos_run": args.allow_portfolioos_run,
            "base_expected_return_unit": args.base_expected_return_unit,
            "expected_return_panel_path": args.expected_return_panel,
            "local_backtest_manifest_path": args.local_backtest_manifest,
            "no_broker": True,
            "no_network": True,
            "projection_manifest_path": args.projection_manifest,
            "q2_input_contract_v2_path": args.q2_input,
            "rebalance_date": args.rebalance_date,
            "run_id": args.run_id,
        }
    )
    result = run_typed_optimizer_response_acceptance(response_input)
    artifacts = write_typed_optimizer_response_artifacts(result, args.output_dir)
    print(f"response_status={result.response_status}")
    print(f"optimizer_status={result.summary.optimizer_status}")
    print(f"panel_count={result.summary.panel_count}")
    print(f"positive_rank_alignment_passed={result.summary.positive_rank_alignment_passed}")
    print(f"scaled_alpha_reward_monotone={result.summary.scaled_alpha_reward_monotone}")
    print(f"sign_flip_reverses_ordering={result.summary.sign_flip_reverses_ordering}")
    print(f"no_view_distinct_from_zero_alpha={result.summary.no_view_distinct_from_zero_alpha}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
