"""Run the local typed expected-return injection fixture."""

from __future__ import annotations

import argparse
from pathlib import Path

from execution_aware_optimizer.typed_expected_return_injection import (
    run_typed_expected_return_injection,
    write_typed_expected_return_injection_artifacts,
)
from execution_aware_optimizer.typed_injection_schema import TypedExpectedReturnInjectionInput


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "typed_injection"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local typed expected-return injection fixture.")
    parser.add_argument("--run-id", default="typed_expected_return_injection_fixture")
    parser.add_argument("--q2-input", default=str(FIXTURE_DIR / "q2_input_contract_v2.json"))
    parser.add_argument("--expected-return-panel", default=str(FIXTURE_DIR / "expected_return_panel.csv"))
    parser.add_argument("--projection-manifest", default=str(FIXTURE_DIR / "projection_manifest.json"))
    parser.add_argument(
        "--local-backtest-manifest",
        default=str(REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"),
    )
    parser.add_argument("--adapter-config", default=str(FIXTURE_DIR / "adapter_config.yaml"))
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "typed_expected_return_injection_fixture"),
    )
    parser.add_argument("--expected-return-scale", type=float, default=1.0)
    parser.add_argument("--expected-return-sign", type=int, choices=[-1, 1], default=1)
    parser.add_argument("--rebalance-date", default=None)
    parser.add_argument(
        "--allow-portfolioos-run",
        action="store_true",
        help="Explicitly build the local PortfolioOS optimizer-input snapshot. No network or broker access is used.",
    )
    args = parser.parse_args()

    injection_input = TypedExpectedReturnInjectionInput.model_validate(
        {
            "adapter_config_path": args.adapter_config,
            "allow_portfolioos_run": args.allow_portfolioos_run,
            "expected_return_panel_path": args.expected_return_panel,
            "expected_return_scale": args.expected_return_scale,
            "expected_return_sign": args.expected_return_sign,
            "local_backtest_manifest_path": args.local_backtest_manifest,
            "no_broker": True,
            "no_network": True,
            "projection_manifest_path": args.projection_manifest,
            "q2_input_contract_v2_path": args.q2_input,
            "rebalance_date": args.rebalance_date,
            "run_id": args.run_id,
        }
    )
    result = run_typed_expected_return_injection(injection_input)
    artifacts = write_typed_expected_return_injection_artifacts(result, args.output_dir)
    print(f"injection_status={result.injection_status}")
    print(f"expected_return_reached_optimizer_input={result.expected_return_reached_optimizer_input}")
    print(f"optimizer_input_snapshot_rows={result.optimizer_input_snapshot_rows}")
    print(f"injected_expected_return_count={result.injected_expected_return_count}")
    print(f"q2_adapter_status={result.q2_adapter_status}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
