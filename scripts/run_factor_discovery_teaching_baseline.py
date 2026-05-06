"""Run the local Factor Discovery Sandbox teaching baseline."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.teaching_baseline import run_teaching_baseline


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local-only factor discovery teaching baseline.")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "teaching_mode"),
    )
    args = parser.parse_args()

    result = run_teaching_baseline(args.output_dir)
    print(f"mode={result.summary['mode']}")
    print(f"benchmark={result.summary['benchmark']}")
    print(f"survivorship_biased={str(result.summary['survivorship_biased']).lower()}")
    print(f"educational_only={str(result.summary['educational_only']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
