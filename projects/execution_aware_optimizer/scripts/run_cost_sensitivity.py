#!/usr/bin/env python3
"""Build the Q2 cost-sensitivity result skeleton."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
for path in (PROJECT_ROOT / "src", REPO_ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from execution_aware_optimizer.cost_sensitivity import build_cost_sensitivity_scenarios
from execution_aware_optimizer.experiment_config import load_experiment_config
from execution_aware_optimizer.ladder import build_unavailable_ladder_rows


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description="Run Q2 cost sensitivity assumptions.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "configs" / "cost_sensitivity.yaml"))
    parser.add_argument("--output", default=str(PROJECT_ROOT / "reports" / "cost_sensitivity_results.csv"))
    args = parser.parse_args()

    config = load_experiment_config(args.config)
    payload_rows: list[dict[str, object]] = []
    for scenario in build_cost_sensitivity_scenarios(config):
        rows = build_unavailable_ladder_rows(
            scenario.config,
            reason=(
                f"Cost sensitivity at {scenario.cost_bps} bps is configured but not executed by default. "
                "Enable an explicit PortfolioOS cost adapter before treating this as a result."
            ),
        )
        for row in rows:
            payload = row.model_dump(mode="json")
            payload["cost_bps"] = int(scenario.cost_bps)
            payload["transaction_cost_objective_mode"] = scenario.config.portfolioos.transaction_cost_objective_mode
            payload["portfolioos_overrides"] = scenario.portfolioos_overrides
            payload_rows.append(payload)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(payload_rows).to_csv(output_path, index=False)
    print(f"cost_sensitivity_csv: {output_path}")


if __name__ == "__main__":
    main()
