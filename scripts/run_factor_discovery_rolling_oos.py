"""Run the local Factor Discovery Sandbox rolling OOS weighting path."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.rolling_oos import run_rolling_oos


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run rolling ICIR OOS factor scoring.")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "research_mode"),
    )
    parser.add_argument("--min-history-months", type=int, default=12)
    args = parser.parse_args()

    result = run_rolling_oos(args.output_dir, min_history_months=args.min_history_months)
    print(f"mode={result.summary['mode']}")
    print(f"uses_full_sample_icir={str(result.summary['uses_full_sample_icir']).lower()}")
    print(f"trade_timing={result.summary['trade_timing']}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
