"""Run the FD-S6 revision-confirmed earnings underreaction diagnostic."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.revision_confirmed_earnings_underreaction import (
    run_revision_confirmed_earnings_underreaction,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the FD-S6 sandbox diagnostic for revision-confirmed earnings underreaction.",
    )
    parser.add_argument(
        "--prices",
        default=str(
            REPO_ROOT
            / "data"
            / "cache"
            / "wrds_multifactor"
            / "nasdaq100_daily_full10"
            / "standardized"
            / "adjusted_price_volume_panel.csv"
        ),
    )
    parser.add_argument(
        "--estimates",
        default=str(REPO_ROOT / "data" / "cache" / "wrds_sue_event_panel_expanded" / "ibes_estimates.csv"),
    )
    parser.add_argument(
        "--events",
        default=str(REPO_ROOT / "outputs" / "sue_historical_event_panel_expanded" / "events.csv"),
    )
    parser.add_argument(
        "--universe",
        default=str(
            REPO_ROOT
            / "data"
            / "cache"
            / "wrds_multifactor"
            / "nasdaq100_daily_full10"
            / "standardized"
            / "historical_universe_membership.csv"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(
            REPO_ROOT
            / "outputs"
            / "factor_discovery"
            / "research_mode"
            / "revision_confirmed_earnings_underreaction"
        ),
    )
    parser.add_argument("--min-cross-section", type=int, default=5)
    parser.add_argument("--train-test-split-date", default=None)
    parser.add_argument("--cost-bps-per-side", type=float, default=10.0)
    args = parser.parse_args()

    result = run_revision_confirmed_earnings_underreaction(
        prices_path=args.prices,
        estimates_path=args.estimates,
        events_path=args.events,
        universe_path=args.universe,
        output_dir=args.output_dir,
        min_cross_section=args.min_cross_section,
        train_test_split_date=args.train_test_split_date,
        cost_bps_per_side=args.cost_bps_per_side,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"candidate_id={result.summary['candidate_id']}")
    print(f"decision_label={result.summary['decision_label']}")
    print(f"q1_candidate_review_eligible={str(result.summary['q1_candidate_review_eligible']).lower()}")
    print(f"active_row_count={result.summary['active_row_count']}")
    print(f"active_date_count={result.summary['active_date_count']}")
    print(f"explicit_abstain_rows={result.summary['explicit_abstain_rows']}")
    print(f"alpha_success_claimed={str(result.summary['alpha_success_claimed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"production_approval_claimed={str(result.summary['production_approval_claimed']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
