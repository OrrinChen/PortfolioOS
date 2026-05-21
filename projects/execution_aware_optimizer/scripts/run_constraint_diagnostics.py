#!/usr/bin/env python3
"""Run Q2 constraint diagnostics over ladder rows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
for path in (PROJECT_ROOT / "src", REPO_ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from execution_aware_optimizer.diagnostics import build_constraint_diagnostics
from execution_aware_optimizer.experiment_config import load_experiment_config
from execution_aware_optimizer.ladder import build_unavailable_ladder_rows


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description="Write Q2 constraint diagnostics.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "configs" / "alpha_decay_ladder.yaml"))
    parser.add_argument("--output", default=str(PROJECT_ROOT / "reports" / "constraint_diagnostics.json"))
    parser.add_argument("--turnover-budget", type=float, default=None)
    parser.add_argument("--liquidity-budget", type=float, default=None)
    args = parser.parse_args()

    config = load_experiment_config(args.config)
    rows = build_unavailable_ladder_rows(
        config,
        reason="Diagnostics script was run without a realized ladder result file.",
    )
    diagnostics = build_constraint_diagnostics(
        rows,
        turnover_budget=args.turnover_budget,
        liquidity_budget=args.liquidity_budget,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(diagnostics.model_dump(mode="json"), indent=2), encoding="utf-8")
    print(f"constraint_diagnostics_json: {output_path}")


if __name__ == "__main__":
    main()
