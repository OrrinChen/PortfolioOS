#!/usr/bin/env python3
"""Run the Q2 alpha-decay ladder project script."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
for path in (PROJECT_ROOT / "src", REPO_ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from execution_aware_optimizer.alpha_input import load_alpha_scores
from execution_aware_optimizer.diagnostics import build_constraint_diagnostics
from execution_aware_optimizer.experiment_config import load_experiment_config
from execution_aware_optimizer.ladder import ladder_rows_to_frame, run_alpha_decay_ladder
from execution_aware_optimizer.reports import write_execution_aware_optimizer_report


def _resolve_path(path_text: str | None) -> Path | None:
    """Resolve a config path against repo root first, then project root."""

    if path_text is None:
        return None
    raw = Path(path_text)
    if raw.is_absolute():
        return raw
    repo_candidate = REPO_ROOT / raw
    if repo_candidate.exists():
        return repo_candidate
    return PROJECT_ROOT / raw


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description="Run the Execution-Aware Portfolio Optimizer ladder.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "configs" / "alpha_decay_ladder.yaml"))
    parser.add_argument("--output", default=str(PROJECT_ROOT / "reports" / "alpha_decay_ladder_results.csv"))
    parser.add_argument("--report", default=None)
    args = parser.parse_args()

    config = load_experiment_config(args.config)
    alpha_result = None
    alpha_panel = None
    alpha_path = _resolve_path(config.alpha_input.path)
    if alpha_path is not None:
        alpha_result = load_alpha_scores(
            alpha_path,
            rank_normalize_by_date=config.alpha_input.rank_normalize_by_date,
            winsorize_quantile=config.alpha_input.winsorize_quantile,
        )
        alpha_panel = alpha_result.panel

    rows = run_alpha_decay_ladder(config, alpha_panel=alpha_panel)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ladder_rows_to_frame(rows).to_csv(output_path, index=False)

    diagnostics = build_constraint_diagnostics(rows)
    report_path = Path(args.report or config.report_path)
    if not report_path.is_absolute():
        report_path = REPO_ROOT / report_path
    write_execution_aware_optimizer_report(
        report_path,
        config=config,
        alpha_report=alpha_result.report if alpha_result is not None else None,
        ladder_rows=rows,
        diagnostics=diagnostics,
    )
    print(f"ladder_results_csv: {output_path}")
    print(f"report_markdown: {report_path}")


if __name__ == "__main__":
    main()
