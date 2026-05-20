"""Run D2-INSIDER-02A Form 4 plan-flag parser/source audit."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.insider_disclosure_plan_flag_audit import run_plan_flag_parser_source_audit


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
    / "d2_plan_flag_audit"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Write D2-INSIDER-02A plan-flag parser/source audit artifacts.")
    parser.add_argument("--event-registry", default=str(DEFAULT_EVENT_REGISTRY))
    parser.add_argument("--parse-coverage", default=str(DEFAULT_PARSE_COVERAGE))
    parser.add_argument("--source-root", action="append", default=None)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--max-samples-per-bucket", type=int, default=100)
    args = parser.parse_args()

    source_roots = [Path(value) for value in args.source_root] if args.source_root else DEFAULT_SOURCE_ROOTS
    result = run_plan_flag_parser_source_audit(
        event_registry_path=args.event_registry,
        parse_coverage_path=args.parse_coverage,
        source_roots=source_roots,
        output_dir=args.output_dir,
        max_samples_per_bucket=args.max_samples_per_bucket,
    )

    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"candidate_id={result.summary['candidate_id']}")
    print(f"overall_decision={result.summary['overall_decision']}")
    print(f"decision_reason={result.summary['decision_reason']}")
    print(f"d2_insider_02_status={result.summary['d2_insider_02_status']}")
    print(f"allow_d3_charter_for={result.summary['allow_d3_charter_for']}")
    print(f"s_code_event_count={result.summary['s_code_event_count']}")
    print(f"registry_false_count={result.summary['registry_false_count']}")
    print(f"registry_true_count={result.summary['registry_true_count']}")
    print(f"registry_unknown_count={result.summary['registry_unknown_count']}")
    print(f"structured_true_count={result.summary['structured_true_count']}")
    print(f"structured_false_count={result.summary['structured_false_count']}")
    print(f"structured_missing_count={result.summary['structured_missing_count']}")
    print(f"footnote_10b5_candidate_count={result.summary['footnote_10b5_candidate_count']}")
    print(f"footnote_adoption_date_candidate_count={result.summary['footnote_adoption_date_candidate_count']}")
    print(f"false_without_structured_source_count={result.summary['false_without_structured_source_count']}")
    print(f"measurement_spec_written={str(result.summary['measurement_spec_written']).lower()}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"production_approval_claimed={str(result.summary['production_approval_claimed']).lower()}")
    for name, path in result.artifacts.items():
        if path.exists():
            print(f"{name}={path}")


if __name__ == "__main__":
    main()
