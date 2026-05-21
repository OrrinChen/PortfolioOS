"""Run D2-INSIDER-02 planned vs discretionary sell contrast observability."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.insider_disclosure_d2_sell_contrast import (
    run_planned_vs_discretionary_sell_contrast_d2,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVENT_REGISTRY = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "d2_real_archive_batched_aggregate"
    / "insider_event_registry_real.csv"
)
DEFAULT_PRICE_PANEL = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "q1_label_coverage_rescue"
    / "rescued_price_panel.csv"
)
DEFAULT_FALLBACK_PRICE_PANEL = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "market_cache"
    / "insider_replay_market_subset_all_archive_symbols.csv"
)
DEFAULT_BENCHMARK_PANEL = (
    REPO_ROOT
    / "data"
    / "cache"
    / "wrds_multifactor"
    / "nasdaq100_daily"
    / "standardized"
    / "qqq_benchmark_panel.csv"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "d2_sell_contrast"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Write no-formula D2 sell contrast observability artifacts.")
    parser.add_argument("--event-registry", default=str(DEFAULT_EVENT_REGISTRY))
    parser.add_argument("--price-panel", default=None)
    parser.add_argument("--benchmark-panel", default=str(DEFAULT_BENCHMARK_PANEL))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    price_panel = Path(args.price_panel) if args.price_panel else DEFAULT_PRICE_PANEL
    if not price_panel.exists():
        price_panel = DEFAULT_FALLBACK_PRICE_PANEL
    result = run_planned_vs_discretionary_sell_contrast_d2(
        event_registry_path=args.event_registry,
        price_panel_path=price_panel,
        benchmark_panel_path=args.benchmark_panel,
        output_dir=args.output_dir,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"candidate_id={result.summary['candidate_id']}")
    print(f"overall_decision={result.summary['overall_decision']}")
    print(f"decision_reason={result.summary['decision_reason']}")
    print(f"allow_d3_charter_for={result.summary['allow_d3_charter_for']}")
    print(f"event_count={result.summary['event_count']}")
    print(f"discretionary_sell_event_count={result.summary['discretionary_sell_event_count']}")
    print(f"planned_sell_event_count={result.summary['planned_sell_event_count']}")
    print(f"unknown_plan_flag_event_count={result.summary['unknown_plan_flag_event_count']}")
    print(f"discretionary_sell_label_coverage_share={result.summary['discretionary_sell_label_coverage_share']}")
    print(f"planned_sell_label_coverage_share={result.summary['planned_sell_label_coverage_share']}")
    print(f"discretionary_sell_primary_mean_abnormal_return={result.summary['discretionary_sell_primary_mean_abnormal_return']}")
    print(f"planned_sell_primary_mean_abnormal_return={result.summary['planned_sell_primary_mean_abnormal_return']}")
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
