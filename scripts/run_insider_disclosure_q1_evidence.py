"""Run Q1-INSIDER-01 evidence review for D3 open-market insider buying."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.insider_disclosure_q1_evidence import (
    run_open_market_buying_q1_evidence_review,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SIGNAL_PANEL = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "d3_open_market_buying_v0"
    / "signal_panel.csv"
)
DEFAULT_PRICE_PANEL = (
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
    / "q1_open_market_buying_v0"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Write Q1 insider buying evidence artifacts.")
    parser.add_argument("--signal-panel", default=str(DEFAULT_SIGNAL_PANEL))
    parser.add_argument("--price-panel", default=str(DEFAULT_PRICE_PANEL))
    parser.add_argument("--benchmark-panel", default=str(DEFAULT_BENCHMARK_PANEL))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    result = run_open_market_buying_q1_evidence_review(
        signal_panel_path=args.signal_panel,
        price_panel_path=args.price_panel,
        benchmark_panel_path=args.benchmark_panel,
        output_dir=args.output_dir,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"measurement_spec_id={result.summary['measurement_spec_id']}")
    print(f"q1_decision={result.summary['q1_decision']}")
    print(f"q1_result_interpretation={result.summary['q1_result_interpretation']}")
    print(f"active_event_clusters={result.summary['active_event_clusters']}")
    print(f"observed_primary_label_clusters={result.summary['observed_primary_label_clusters']}")
    print(f"observed_event_month_count={result.summary['observed_event_month_count']}")
    print(f"label_coverage_share={result.summary['label_coverage_share']}")
    print(f"primary_mean_abnormal_return={result.summary['primary_mean_abnormal_return']}")
    print(f"rank_ic_mean={result.summary['rank_ic_mean']}")
    print(f"top_bottom_spread_mean={result.summary['top_bottom_spread_mean']}")
    print(f"promotion_gate_allowed={str(result.summary['promotion_gate_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"optimizer_entry_allowed={str(result.summary['optimizer_entry_allowed']).lower()}")
    print(f"alpha_registry_update_allowed={str(result.summary['alpha_registry_update_allowed']).lower()}")
    print(f"production_approval_claimed={str(result.summary['production_approval_claimed']).lower()}")
    for name, path in result.artifacts.items():
        if path.exists():
            print(f"{name}={path}")


if __name__ == "__main__":
    main()
