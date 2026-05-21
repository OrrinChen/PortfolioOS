from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.full_market_sweep import run_full_market_multifactor_sweep


def _default_returns_panel() -> Path:
    expanded = Path("data/risk_inputs_us_expanded/returns_long.csv")
    if expanded.exists():
        return expanded
    return Path("data/risk_inputs/returns_long.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-E0 full-market multifactor discovery sweep.")
    parser.add_argument("--returns-panel", type=Path, default=_default_returns_panel())
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/full_market_sweep"),
    )
    parser.add_argument("--top-n", type=int, default=5)
    args = parser.parse_args()

    result = run_full_market_multifactor_sweep(
        returns_panel_path=args.returns_panel,
        output_dir=args.output_dir,
        top_n=args.top_n,
    )
    print(
        "full_market_multifactor_sweep_built "
        f"validation_status={result.validation_status} "
        f"decision_state={result.decision_state} "
        f"path={result.summary_path}"
    )


if __name__ == "__main__":
    main()
