"""Portfolio quant artifact wrapper around the dedicated walk-forward engine."""

from __future__ import annotations

from dataclasses import dataclass
import shutil
from pathlib import Path
from typing import Any

from portfolio_os.backtest.walk_forward import WalkForwardResult, run_walk_forward


@dataclass(frozen=True)
class PortfolioQuantWalkForwardResult:
    """Artifacts written by the portfolio quant walk-forward runner."""

    output_dir: Path
    summary_path: Path
    walk_forward_result: WalkForwardResult
    summary: dict[str, Any]


def _write_portfolio_quant_aliases(result: WalkForwardResult) -> Path:
    """Write portfolio_quant_* aliases for the walk-forward artifacts."""

    alias_pairs = {
        "walk_forward_summary.json": "portfolio_quant_summary.json",
        "walk_forward_nav_curve.csv": "portfolio_quant_nav_curve.csv",
        "walk_forward_drawdown_curve.csv": "portfolio_quant_drawdown_curve.csv",
        "walk_forward_turnover_distribution.csv": "portfolio_quant_turnover_distribution.csv",
        "walk_forward_cost_attribution.csv": "portfolio_quant_cost_attribution.csv",
        "walk_forward_multi_snapshot_replay.csv": "portfolio_quant_multi_snapshot_replay.csv",
        "walk_forward_strategy_comparison.csv": "portfolio_quant_strategy_comparison.csv",
        "walk_forward_policy_breaches.csv": "portfolio_quant_policy_breaches.csv",
        "walk_forward_report.md": "portfolio_quant_report.md",
    }
    for source_name, alias_name in alias_pairs.items():
        shutil.copyfile(result.output_dir / source_name, result.output_dir / alias_name)
    return result.output_dir / "portfolio_quant_summary.json"


def run_portfolio_quant_walk_forward(
    *,
    manifest_path: str | Path,
    output_dir: str | Path,
) -> PortfolioQuantWalkForwardResult:
    """Run the PortfolioOS historical walk-forward portfolio quant smoke path."""

    walk_forward_result = run_walk_forward(
        manifest_path=manifest_path,
        output_dir=output_dir,
        frequency="monthly",
    )
    summary_path = _write_portfolio_quant_aliases(walk_forward_result)
    return PortfolioQuantWalkForwardResult(
        output_dir=walk_forward_result.output_dir,
        summary_path=summary_path,
        walk_forward_result=walk_forward_result,
        summary=walk_forward_result.summary,
    )
