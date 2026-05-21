"""Run the local Factor Discovery Sandbox cost/capacity/benchmark survival path."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.survival import run_survival_analysis


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run final factor discovery survival diagnostics.")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "research_mode"),
    )
    args = parser.parse_args()

    result = run_survival_analysis(args.output_dir)
    print(f"recommended_import_decision={result.summary['recommended_import_decision']}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"production_approval_claimed={str(result.summary['production_approval_claimed']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
