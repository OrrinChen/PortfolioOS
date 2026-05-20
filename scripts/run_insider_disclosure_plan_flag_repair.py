"""Run D2-INSIDER-02B source locator and plan-flag parser repair gate."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.insider_disclosure_plan_flag_repair import run_plan_flag_source_locator_parser_repair


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVENT_REGISTRY = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "d2_real_archive_batched_aggregate"
    / "insider_event_registry_real.csv"
)
DEFAULT_PARSE_COVERAGE = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "d2_real_archive_batched_aggregate"
    / "form4_xml_parse_coverage.csv"
)
DEFAULT_SOURCE_ROOTS = [
    REPO_ROOT / "outputs" / "factor_discovery" / "insider_disclosure" / "d2_real_archive_ps_probe_source",
    REPO_ROOT / "outputs" / "factor_discovery" / "insider_disclosure" / "d2_real_archive_probe_source",
]
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "d2_plan_flag_repair"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Write D2-INSIDER-02B plan-flag source locator/parser repair artifacts.")
    parser.add_argument("--event-registry", default=str(DEFAULT_EVENT_REGISTRY))
    parser.add_argument("--parse-coverage", default=str(DEFAULT_PARSE_COVERAGE))
    parser.add_argument("--source-root", action="append", default=None)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    source_roots = [Path(value) for value in args.source_root] if args.source_root else DEFAULT_SOURCE_ROOTS
    result = run_plan_flag_source_locator_parser_repair(
        event_registry_path=args.event_registry,
        parse_coverage_path=args.parse_coverage,
        source_roots=source_roots,
        output_dir=args.output_dir,
    )

    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"candidate_id={result.summary['candidate_id']}")
    print(f"overall_decision={result.summary['overall_decision']}")
    print(f"decision_reason={result.summary['decision_reason']}")
    print(f"next_action={result.summary['next_action']}")
    print(f"d2_insider_02_status={result.summary['d2_insider_02_status']}")
    print(f"allow_d3_charter_for={result.summary['allow_d3_charter_for']}")
    print(f"raw_file_found_share={result.summary['raw_file_found_share']}")
    print(f"known_plan_flag_share={result.summary['known_plan_flag_share']}")
    print(f"structured_or_high_confidence_source_share={result.summary['structured_or_high_confidence_source_share']}")
    print(f"repaired_planned_sell_event_count={result.summary['repaired_planned_sell_event_count']}")
    print(f"repaired_planned_sell_month_count={result.summary['repaired_planned_sell_month_count']}")
    print(f"repaired_discretionary_sell_event_count={result.summary['repaired_discretionary_sell_event_count']}")
    print(f"repaired_unknown_plan_flag_event_count={result.summary['repaired_unknown_plan_flag_event_count']}")
    print(f"measurement_spec_written={str(result.summary['measurement_spec_written']).lower()}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"production_approval_claimed={str(result.summary['production_approval_claimed']).lower()}")
    for name, path in result.artifacts.items():
        if path.exists():
            print(f"{name}={path}")


if __name__ == "__main__":
    main()
