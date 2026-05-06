"""Run the local Factor Discovery Sandbox redundancy and marginal-value gate."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.marginal_value import run_marginal_value_gate


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run factor redundancy and marginal-value diagnostics.")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "research_mode"),
    )
    args = parser.parse_args()

    result = run_marginal_value_gate(args.output_dir)
    print(f"factor_count={result.summary['factor_count']}")
    print(f"high_correlation_kept_by_icir_only={str(result.summary['high_correlation_kept_by_icir_only']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
