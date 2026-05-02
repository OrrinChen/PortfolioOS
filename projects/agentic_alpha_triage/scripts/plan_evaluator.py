"""Print a local Q1 dry-run evaluator plan as JSON."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from agentic_alpha_triage.evaluator_planner import build_evaluator_plan  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse explicit local fixture paths."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture",
        type=Path,
        required=True,
        help="Local evaluator fixture YAML path.",
    )
    parser.add_argument(
        "--event-registry-dir",
        type=Path,
        required=True,
        help="Local directory containing compatible event-registry YAML examples.",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation level. Use 0 for compact output.",
    )
    return parser.parse_args()


def main() -> None:
    """Build and print one local dry-run evaluator plan."""

    args = parse_args()
    plan = build_evaluator_plan(args.fixture, event_registry_dir=args.event_registry_dir)
    indent = None if args.indent == 0 else args.indent
    json.dump(plan.model_dump(mode="json"), sys.stdout, indent=indent, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
