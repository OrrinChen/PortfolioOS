"""Run D2-8K-01 subtype underreaction no-formula observability."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.eightk_subtype_d2 import (
    run_eightk_subtype_observability_d2,
    write_deterministic_eightk_fixture_inputs,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "factor_discovery" / "8k_subtype" / "d2"
DEFAULT_FIXTURE_INPUT_DIR = DEFAULT_OUTPUT_DIR / "_fixture_inputs"


def main() -> None:
    parser = argparse.ArgumentParser(description="Write D2-8K-01 no-formula observability artifacts.")
    parser.add_argument("--event-registry", default=None)
    parser.add_argument("--price-panel", default=None)
    parser.add_argument("--benchmark-panel", default=None)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--fixture-input-dir", default=str(DEFAULT_FIXTURE_INPUT_DIR))
    parser.add_argument("--minimum-subtype-events", type=int, default=None)
    parser.add_argument("--minimum-event-month-count", type=int, default=None)
    parser.add_argument("--minimum-label-coverage-share", type=float, default=0.75)
    args = parser.parse_args()

    if args.event_registry and args.price_panel:
        event_registry = Path(args.event_registry)
        price_panel = Path(args.price_panel)
        benchmark_panel = Path(args.benchmark_panel) if args.benchmark_panel else None
        evidence_type = "local_8k_input"
        minimum_subtype_events = args.minimum_subtype_events or 100
        minimum_event_month_count = args.minimum_event_month_count or 24
    else:
        fixture_paths = write_deterministic_eightk_fixture_inputs(args.fixture_input_dir)
        event_registry = fixture_paths["event_registry"]
        price_panel = fixture_paths["price_panel"]
        benchmark_panel = fixture_paths["benchmark_panel"]
        evidence_type = "deterministic_fixture"
        minimum_subtype_events = args.minimum_subtype_events or 3
        minimum_event_month_count = args.minimum_event_month_count or 2

    result = run_eightk_subtype_observability_d2(
        event_registry_path=event_registry,
        price_panel_path=price_panel,
        benchmark_panel_path=benchmark_panel,
        output_dir=args.output_dir,
        minimum_subtype_events=minimum_subtype_events,
        minimum_event_month_count=minimum_event_month_count,
        minimum_label_coverage_share=args.minimum_label_coverage_share,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"candidate_id={result.summary['candidate_id']}")
    print(f"evidence_type={evidence_type}")
    print(f"overall_decision={result.summary['overall_decision']}")
    print(f"decision_reason={result.summary['decision_reason']}")
    print(f"event_count={result.summary['event_count']}")
    print(f"priority_event_count={result.summary['priority_event_count']}")
    print(f"routine_control_event_count={result.summary['routine_control_event_count']}")
    print(f"unknown_no_view_event_count={result.summary['unknown_no_view_event_count']}")
    print(f"allow_d3_charter_for={result.summary['allow_d3_charter_for']}")
    print(f"formula_score_written={str(result.summary['formula_score_written']).lower()}")
    print(f"measurement_spec_written={str(result.summary['measurement_spec_written']).lower()}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"expected_return_panel_written={str(result.summary['expected_return_panel_written']).lower()}")
    print(f"production_approval_claimed={str(result.summary['production_approval_claimed']).lower()}")
    for name, path in result.artifacts.items():
        if path.exists():
            print(f"{name}={path}")


if __name__ == "__main__":
    main()
