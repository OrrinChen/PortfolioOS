#!/usr/bin/env python3
"""Run the Q2 execution evaluation matrix."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
for path in (PROJECT_ROOT / "src", REPO_ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from execution_aware_optimizer.execution_matrix import (
    execution_matrix_rows_to_frame,
    run_execution_matrix,
)
from execution_aware_optimizer.experiment_config import load_experiment_config
from execution_aware_optimizer.robustness_summary import (
    render_execution_matrix_report,
    summarize_execution_matrix,
)


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description="Run the Q2 execution evaluation matrix.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "configs" / "execution_matrix.yaml"))
    parser.add_argument("--output", default=str(PROJECT_ROOT / "reports" / "execution_matrix.csv"))
    parser.add_argument(
        "--summary-output",
        default=str(PROJECT_ROOT / "reports" / "robustness_summary.json"),
    )
    parser.add_argument("--report", default=str(PROJECT_ROOT / "reports" / "execution_report.md"))
    args = parser.parse_args()

    config = load_experiment_config(args.config)
    rows = run_execution_matrix(config)
    summary = summarize_execution_matrix(rows)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    execution_matrix_rows_to_frame(rows).to_csv(output_path, index=False)

    summary_path = Path(args.summary_output)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        render_execution_matrix_report(rows, summary=summary),
        encoding="utf-8",
    )

    print(f"execution_matrix_csv: {output_path}")
    print(f"robustness_summary_json: {summary_path}")
    print(f"execution_report_markdown: {report_path}")


if __name__ == "__main__":
    main()
