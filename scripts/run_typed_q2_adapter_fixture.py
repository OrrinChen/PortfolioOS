"""Run the local typed Q2 adapter fixture."""

from __future__ import annotations

import argparse
from pathlib import Path

from execution_aware_optimizer.typed_adapter_schema import TypedQ2AdapterInput
from execution_aware_optimizer.typed_portfolioos_adapter import (
    run_typed_q2_adapter,
    write_typed_q2_adapter_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "typed_q2"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local typed Q2 adapter fixture.")
    parser.add_argument("--run-id", default="typed_q2_adapter_fixture")
    parser.add_argument("--q2-input", default=str(FIXTURE_DIR / "q2_input_contract_v2.json"))
    parser.add_argument("--expected-return-panel", default=str(FIXTURE_DIR / "expected_return_panel.csv"))
    parser.add_argument("--projection-manifest", default=str(FIXTURE_DIR / "projection_manifest.json"))
    parser.add_argument(
        "--local-backtest-manifest",
        default=str(REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"),
    )
    parser.add_argument("--adapter-config", default=str(FIXTURE_DIR / "adapter_config.yaml"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "typed_q2_adapter_fixture"))
    parser.add_argument(
        "--allow-portfolioos-run",
        action="store_true",
        help="Explicitly run the local PortfolioOS fixture adapter. No network or broker access is used.",
    )
    args = parser.parse_args()

    adapter_input = TypedQ2AdapterInput.model_validate(
        {
            "adapter_config_path": args.adapter_config,
            "allow_portfolioos_run": args.allow_portfolioos_run,
            "expected_return_panel_path": args.expected_return_panel,
            "local_backtest_manifest_path": args.local_backtest_manifest,
            "no_broker": True,
            "no_network": True,
            "projection_manifest_path": args.projection_manifest,
            "q2_input_contract_v2_path": args.q2_input,
            "run_id": args.run_id,
        }
    )
    result = run_typed_q2_adapter(adapter_input)
    artifacts = write_typed_q2_adapter_artifacts(result, args.output_dir)
    print(f"adapter_status={result.adapter_status}")
    print(f"observed_rows={result.observed_rows}")
    print(f"unavailable_rows={result.unavailable_rows}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
