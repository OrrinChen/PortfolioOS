from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from portfolio_os.backtest.walk_forward import run_walk_forward
from portfolio_os.cli.walk_forward import app as walk_forward_app


def test_walk_forward_runs_monthly_and_weekly_with_no_lookahead(project_root: Path, tmp_path: Path) -> None:
    manifest = project_root / "data" / "backtest_samples" / "manifest_us_expanded.yaml"

    monthly = run_walk_forward(manifest_path=manifest, output_dir=tmp_path / "monthly", frequency="monthly")
    weekly = run_walk_forward(manifest_path=manifest, output_dir=tmp_path / "weekly", frequency="weekly")

    expected_strategies = {
        "equal_weight",
        "mean_variance",
        "risk_parity",
        "cost_unaware_rebalance",
        "portfolio_os_cost_aware_rebalance",
    }
    assert expected_strategies <= set(monthly.nav_curve["strategy"])
    assert expected_strategies <= set(monthly.summary["strategies"])
    assert expected_strategies <= set(weekly.summary["strategies"])
    assert weekly.summary["metadata"]["rebalance_count"] > monthly.summary["metadata"]["rebalance_count"]

    no_lookahead = monthly.no_lookahead_report
    assert not no_lookahead.empty
    assert no_lookahead["no_lookahead_passed"].all()
    assert (
        pd.to_datetime(no_lookahead["estimation_end_date"])
        < pd.to_datetime(no_lookahead["rebalance_date"])
    ).all()
    assert "future_return_columns_used" not in set(no_lookahead.columns)


def test_walk_forward_outputs_portfolio_quant_risk_metrics(project_root: Path, tmp_path: Path) -> None:
    manifest = project_root / "data" / "backtest_samples" / "manifest_us_expanded.yaml"

    result = run_walk_forward(manifest_path=manifest, output_dir=tmp_path / "walk_forward", frequency="monthly")

    assert (result.output_dir / "walk_forward_summary.json").exists()
    assert (result.output_dir / "walk_forward_nav_curve.csv").exists()
    assert (result.output_dir / "walk_forward_no_lookahead_report.csv").exists()
    assert (result.output_dir / "walk_forward_report.md").exists()

    summary = json.loads((result.output_dir / "walk_forward_summary.json").read_text(encoding="utf-8"))
    cost_aware = summary["strategies"]["portfolio_os_cost_aware_rebalance"]
    cost_unaware = summary["strategies"]["cost_unaware_rebalance"]

    for metric in (
        "annualized_return",
        "annualized_volatility",
        "sharpe",
        "max_drawdown",
        "turnover",
        "transaction_cost",
        "cvar_5",
        "max_exposure_drift",
    ):
        assert metric in cost_aware
    assert summary["metadata"]["no_lookahead_validated"] is True
    assert summary["downstream_flags"]["q2_entry_allowed"] is False
    assert cost_aware["turnover"] <= cost_unaware["turnover"]


def test_walk_forward_cli_writes_outputs(project_root: Path, tmp_path: Path) -> None:
    manifest = project_root / "data" / "backtest_samples" / "manifest_us_expanded.yaml"
    output_dir = tmp_path / "cli_walk_forward"
    runner = CliRunner()

    result = runner.invoke(
        walk_forward_app,
        [
            "--manifest",
            str(manifest),
            "--output-dir",
            str(output_dir),
            "--frequency",
            "weekly",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "walk_forward_summary.json").exists()
    assert (output_dir / "portfolio_quant_summary.json").exists()
    assert "walk_forward_summary.json" in result.output
