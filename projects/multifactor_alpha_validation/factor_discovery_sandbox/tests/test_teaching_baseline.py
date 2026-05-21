from __future__ import annotations

from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.teaching_baseline import run_teaching_baseline


def test_teaching_baseline_writes_required_biased_educational_artifacts(tmp_path: Path) -> None:
    result = run_teaching_baseline(tmp_path)

    expected_names = {
        "nasdaq100_factor_table.csv",
        "qqq_benchmark_report.csv",
        "factor_ic_table.csv",
        "factor_correlation_matrix.csv",
        "icir_weight_table.csv",
        "teaching_backtest_report.md",
    }
    assert expected_names == {path.name for path in result.artifacts.values()}
    assert result.summary["benchmark"] == "QQQ"
    assert result.summary["survivorship_biased"] is True
    assert result.summary["educational_only"] is True
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["same_close_trading_allowed"] is False

    benchmark = pd.read_csv(tmp_path / "qqq_benchmark_report.csv")
    required_metrics = {
        "total_return",
        "annualized_return",
        "sharpe",
        "max_drawdown",
        "alpha",
        "beta",
        "excess_annualized_return",
    }
    assert required_metrics.issubset(set(benchmark["metric"]))
    assert set(benchmark["series"]) == {"teaching_factor_rotation", "QQQ"}

    factor_table = pd.read_csv(tmp_path / "nasdaq100_factor_table.csv")
    assert {"date", "ticker", "factor", "value", "coverage_state"}.issubset(factor_table.columns)
    assert factor_table["factor"].nunique() == 29
    assert set(factor_table["coverage_state"]) == {"active_view"}

    weights = pd.read_csv(tmp_path / "icir_weight_table.csv")
    assert {"factor", "ic_mean", "ic_std", "icir", "weight"}.issubset(weights.columns)
    assert abs(weights["weight"].abs().sum() - 1.0) < 1e-9

    report = (tmp_path / "teaching_backtest_report.md").read_text(encoding="utf-8")
    assert "Teaching Baseline" in report
    assert "QQQ" in report
    assert "survivorship_biased: true" in report
    assert "educational_only: true" in report
    assert "not_alpha_evidence: true" in report
    assert "current-constituent survivorship bias" in report
    assert "production approval: not claimed" in report
    assert "broker_output" not in report
    assert "recommended_trade" not in report
