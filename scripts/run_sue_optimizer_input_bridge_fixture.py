"""Run the explicit SUE optimizer input bridge fixture."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "projects" / "execution_aware_optimizer" / "src"))

from execution_aware_optimizer.sue_optimizer_input_bridge import (  # noqa: E402
    SueOptimizerInputBridgeInput,
    run_sue_optimizer_input_bridge_fixture,
    write_sue_optimizer_input_bridge_artifacts,
)


FIXTURE_CONFIG = REPO_ROOT / "projects" / "typed_alpha_pilot" / "fixtures" / "sue_expanded" / "fixture_config.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SUE optimizer input bridge fixture.")
    parser.add_argument("--run-id", default="sue_optimizer_input_bridge")
    parser.add_argument("--fixture-config", default=str(FIXTURE_CONFIG))
    parser.add_argument(
        "--local-backtest-manifest",
        default=str(REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"),
    )
    parser.add_argument("--local-rebalance-date", default="2026-02-27")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "sue_optimizer_input_bridge"))
    parser.add_argument(
        "--report-path",
        default=str(REPO_ROOT / "reports" / "sue_optimizer_input_bridge_report.md"),
    )
    parser.add_argument("--allow-typed-alpha-optimizer-injection", action="store_true")
    args = parser.parse_args()

    bridge_input = SueOptimizerInputBridgeInput.model_validate(
        {
            "allow_typed_alpha_optimizer_injection": args.allow_typed_alpha_optimizer_injection,
            "fixture_config_path": args.fixture_config,
            "local_backtest_manifest_path": args.local_backtest_manifest,
            "local_rebalance_date": args.local_rebalance_date,
            "no_broker": True,
            "no_network": True,
            "run_id": args.run_id,
        }
    )
    result = run_sue_optimizer_input_bridge_fixture(bridge_input)
    artifacts = write_sue_optimizer_input_bridge_artifacts(
        result,
        args.output_dir,
        report_path=args.report_path,
    )
    print(f"bridge_status={result.bridge_status}")
    print(f"expected_return_reached_actual_optimizer_input={result.expected_return_reached_actual_optimizer_input}")
    print(f"optimizer_decision_used_typed_expected_return={result.optimizer_decision_used_typed_expected_return}")
    print(f"sue_rank_weight_alignment_observed={result.sue_rank_weight_alignment_observed}")
    print(f"sign_flip_reversal_observed={result.sign_flip_reversal_observed}")
    print(f"scaled_alpha_monotonicity_observed={result.scaled_alpha_monotonicity_observed}")
    print(f"no_view_not_encoded_as_zero={result.no_view_not_encoded_as_zero}")
    print(f"actual_optimizer_output_rows={result.actual_optimizer_output_rows}")
    print(f"adapter_hook_only={result.adapter_hook_only}")
    print(f"production_approval_claimed={result.production_approval_claimed}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
