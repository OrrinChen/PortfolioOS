"""Run D2-8K-01R real local EDGAR 8-K subtype observability replay."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.eightk_subtype_d2_real import (
    run_real_eightk_subtype_observability,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPO_SOURCE_DIR = REPO_ROOT / "data" / "cache" / "sec_8k_archive"
DEFAULT_EXTERNAL_SOURCE_DIR = REPO_ROOT.parent / "sec_filing_archive" / "20260501T145601Z"
DEFAULT_PRICE_PANEL_CANDIDATES = [
    REPO_ROOT / "data" / "cache" / "factor_discovery_8k" / "wrds_priority_8k_price_rescue.csv",
    REPO_ROOT / "data" / "cache" / "wrds_multifactor" / "nasdaq100_daily_size" / "standardized" / "adjusted_price_volume_panel.csv",
    REPO_ROOT / "data" / "cache" / "wrds_multifactor" / "nasdaq100_daily_full10" / "standardized" / "adjusted_price_volume_panel.csv",
    REPO_ROOT / "data" / "cache" / "wrds_multifactor" / "nasdaq100" / "standardized" / "adjusted_price_volume_panel.csv",
    REPO_ROOT / "data" / "cache" / "wrds_multifactor" / "small_cap_us_daily" / "standardized" / "adjusted_price_volume_panel.csv",
]
DEFAULT_BENCHMARK_PANEL = (
    REPO_ROOT
    / "data"
    / "cache"
    / "wrds_multifactor"
    / "nasdaq100_daily"
    / "standardized"
    / "qqq_benchmark_panel.csv"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "factor_discovery" / "8k_subtype" / "d2_real"


def main() -> None:
    parser = argparse.ArgumentParser(description="Write D2-8K-01R real local EDGAR 8-K observability artifacts.")
    parser.add_argument("--source-dir", default=None)
    parser.add_argument("--price-panel", default=None)
    parser.add_argument("--additional-price-panel", action="append", default=[])
    parser.add_argument("--benchmark-panel", default=str(DEFAULT_BENCHMARK_PANEL))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--max-files", type=int, default=2000)
    parser.add_argument("--minimum-subtype-events", type=int, default=100)
    parser.add_argument("--minimum-event-month-count", type=int, default=12)
    parser.add_argument("--minimum-label-coverage-share", type=float, default=0.70)
    args = parser.parse_args()

    source_dir = Path(args.source_dir) if args.source_dir else _default_source_dir()
    price_panel, default_additional_price_panels = _default_price_panels(Path(args.price_panel) if args.price_panel else None)
    additional_price_panels = default_additional_price_panels + [Path(path) for path in args.additional_price_panel]
    benchmark_panel = Path(args.benchmark_panel) if args.benchmark_panel else None
    result = run_real_eightk_subtype_observability(
        source_dir=source_dir,
        price_panel_path=price_panel,
        additional_price_panel_paths=additional_price_panels,
        benchmark_panel_path=benchmark_panel,
        output_dir=args.output_dir,
        start_offset=args.start_offset,
        max_files=args.max_files,
        minimum_subtype_events=args.minimum_subtype_events,
        minimum_event_month_count=args.minimum_event_month_count,
        minimum_label_coverage_share=args.minimum_label_coverage_share,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"candidate_id={result.summary['candidate_id']}")
    print(f"real_data_status={result.summary['real_data_status']}")
    print(f"network_used={str(result.summary['network_used']).lower()}")
    print(f"source_dir={source_dir}")
    print(f"price_panel={price_panel}")
    print(f"additional_price_panels={[str(path) for path in additional_price_panels]}")
    print(f"source_index_total_count={result.summary.get('source_index_total_count', 0)}")
    print(f"indexed_file_count={result.summary.get('indexed_file_count', 0)}")
    print(f"raw_file_found_share={result.summary.get('raw_file_found_share', 0)}")
    print(f"accepted_timestamp_coverage_share={result.summary.get('accepted_timestamp_coverage_share', 0)}")
    print(f"item_header_parse_coverage_share={result.summary.get('item_header_parse_coverage_share', 0)}")
    print(f"market_coverage_share={result.summary.get('market_coverage_share', 0)}")
    print(f"priority_market_coverage_share={result.summary.get('priority_market_coverage_share', 0)}")
    print(f"priority_market_event_count={result.summary.get('priority_market_event_count', 0)}")
    print(f"priority_market_covered_count={result.summary.get('priority_market_covered_count', 0)}")
    print(f"price_panel_count={result.summary.get('price_panel_count', 0)}")
    print(f"filtered_price_row_count={result.summary.get('filtered_price_row_count', 0)}")
    print(f"filtered_price_ticker_count={result.summary.get('filtered_price_ticker_count', 0)}")
    print(f"event_count={result.summary['event_count']}")
    print(f"priority_event_count={result.summary.get('priority_event_count', 0)}")
    print(f"overall_decision={result.summary['overall_decision']}")
    print(f"decision_reason={result.summary['decision_reason']}")
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


def _default_source_dir() -> Path:
    if DEFAULT_REPO_SOURCE_DIR.exists():
        return DEFAULT_REPO_SOURCE_DIR
    return DEFAULT_EXTERNAL_SOURCE_DIR


def _default_price_panels(explicit_primary: Path | None = None) -> tuple[Path, list[Path]]:
    if explicit_primary is not None:
        return explicit_primary, [candidate for candidate in DEFAULT_PRICE_PANEL_CANDIDATES if candidate.exists() and candidate != explicit_primary]
    existing = [candidate for candidate in DEFAULT_PRICE_PANEL_CANDIDATES if candidate.exists()]
    if existing:
        return existing[0], existing[1:]
    return DEFAULT_PRICE_PANEL_CANDIDATES[0], []


if __name__ == "__main__":
    main()
