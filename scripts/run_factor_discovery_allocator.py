"""Run the local Factor Discovery Sandbox shrinkage allocator diagnostics."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.allocator import run_allocator


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run factor shrinkage allocator diagnostics.")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "research_mode"),
    )
    args = parser.parse_args()

    result = run_allocator(args.output_dir)
    print(f"allocated_factor_count={result.summary['allocated_factor_count']}")
    print(f"zero_weight_count={result.summary['zero_weight_count']}")
    print(f"sign_flip_sanity_check_passed={str(result.summary['sign_flip_sanity_check_passed']).lower()}")
    print(f"scale_response_sanity_check_passed={str(result.summary['scale_response_sanity_check_passed']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
