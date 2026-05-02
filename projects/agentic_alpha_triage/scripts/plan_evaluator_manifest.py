"""Print local Q1 dry-run evaluator plans for a manifest as JSON."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from agentic_alpha_triage.evaluator_plan_batch import run_evaluator_plan_manifest  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse an explicit local evaluator-plan manifest path."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Local evaluator-plan manifest YAML path.",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation level. Use 0 for compact output.",
    )
    return parser.parse_args()


def main() -> None:
    """Build and print local dry-run evaluator plan payloads for a manifest."""

    args = parse_args()
    result = run_evaluator_plan_manifest(args.manifest)
    json.dump(
        result.model_dump(mode="json"),
        sys.stdout,
        indent=None if args.indent == 0 else args.indent,
        sort_keys=True,
    )
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
