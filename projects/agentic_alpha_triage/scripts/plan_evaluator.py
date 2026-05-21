"""Print a local Q1 dry-run evaluator plan as JSON."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pydantic import ValidationError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from agentic_alpha_triage.evaluator_planner import (  # noqa: E402
    RejectedEvaluatorPlan,
    build_evaluator_plan,
)


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
    parser.add_argument(
        "--emit-rejected-json",
        action="store_true",
        help="Emit an audit-only rejected-plan JSON response for local contract failures.",
    )
    return parser.parse_args()


def _dump_json(payload: dict[str, object], *, indent: int) -> None:
    json.dump(payload, sys.stdout, indent=None if indent == 0 else indent, sort_keys=True)
    sys.stdout.write("\n")


def main() -> None:
    """Build and print one local dry-run evaluator plan."""

    args = parse_args()
    try:
        plan = build_evaluator_plan(args.fixture, event_registry_dir=args.event_registry_dir)
    except (ValueError, ValidationError) as exc:
        if not args.emit_rejected_json:
            raise
        rejected_plan = RejectedEvaluatorPlan(
            fixture_path=str(args.fixture),
            event_registry_dir=str(args.event_registry_dir),
            rejection_reasons=[str(exc)],
        )
        _dump_json(rejected_plan.model_dump(mode="json"), indent=args.indent)
        return

    _dump_json(plan.model_dump(mode="json"), indent=args.indent)


if __name__ == "__main__":
    main()
