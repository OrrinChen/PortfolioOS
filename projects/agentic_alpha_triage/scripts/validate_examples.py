"""Validate Q1 example YAML artifacts against the package contracts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from agentic_alpha_triage.example_validation import validate_example_directory  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--examples-dir",
        type=Path,
        default=PROJECT_ROOT / "examples",
        help="Directory containing Q1 hypothesis, signal, and evaluation example YAML files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = validate_example_directory(args.examples_dir)
    print(
        "validated_examples: "
        f"hypotheses={result.hypothesis_count} "
        f"signals={result.signal_contract_count} "
        f"evaluations={result.evaluation_contract_count}"
    )
    for path in result.validated_paths:
        print(f"validated_path: {path}")


if __name__ == "__main__":
    main()
