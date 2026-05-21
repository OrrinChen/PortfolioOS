from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.full_market_candidate_audit import run_full_market_candidate_full_audit


def _default_returns_panel() -> Path:
    expanded = Path("data/risk_inputs_us_expanded/returns_long.csv")
    if expanded.exists():
        return expanded
    return Path("data/risk_inputs/returns_long.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-E0 full-market candidate full audit.")
    parser.add_argument("--returns-panel", type=Path, default=_default_returns_panel())
    parser.add_argument(
        "--supervisor-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/full_market_supervisor"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/full_market_candidate_audit"),
    )
    parser.add_argument(
        "--market-reference",
        type=Path,
        default=Path("data/universe/us_universe_reference.csv"),
        help="Optional static universe reference with market cap, ADV, liquidity bucket, and close fields.",
    )
    parser.add_argument(
        "--market-snapshot",
        type=Path,
        default=Path("data/universe/us_universe_market_2026-03-27.csv"),
        help="Optional market snapshot with close and adv_shares fields.",
    )
    args = parser.parse_args()

    result = run_full_market_candidate_full_audit(
        returns_panel_path=args.returns_panel,
        supervisor_dir=args.supervisor_dir,
        output_dir=args.output_dir,
        market_reference_path=args.market_reference,
        market_snapshot_path=args.market_snapshot,
    )
    print(
        "full_market_candidate_full_audit_built "
        f"validation_status={result.validation_status} "
        f"decision_label={result.decision_label} "
        f"path={result.summary_path}"
    )


if __name__ == "__main__":
    main()
