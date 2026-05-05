"""Run the local Phase 52 Revision Marginal-Value Gate."""

from __future__ import annotations

import argparse
from pathlib import Path

from execution_aware_optimizer.revision_marginal_value_gate import (
    load_revision_marginal_value_input,
    run_revision_marginal_value_gate,
    write_revision_marginal_value_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local-only revision marginal-value gate.")
    parser.add_argument(
        "--input",
        default=str(
            REPO_ROOT
            / "projects"
            / "execution_aware_optimizer"
            / "fixtures"
            / "revision_marginal_value"
            / "gate_input.json"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "revision_marginal_value_gate"),
    )
    parser.add_argument(
        "--report",
        default=str(REPO_ROOT / "reports" / "revision_marginal_value_report.md"),
    )
    args = parser.parse_args()

    gate_input = load_revision_marginal_value_input(args.input)
    result = run_revision_marginal_value_gate(gate_input)
    artifacts = write_revision_marginal_value_artifacts(
        result,
        output_dir=args.output_dir,
        report_path=args.report,
    )

    print(f"gate_decision={result.summary.gate_decision}")
    print(f"composite_promotion_allowed={result.summary.composite_promotion_allowed}")
    print(f"beats_sue_adjusted_marginal_threshold={result.summary.beats_sue_adjusted_marginal_threshold}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
