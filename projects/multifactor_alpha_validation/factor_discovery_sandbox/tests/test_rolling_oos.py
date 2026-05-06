from __future__ import annotations

from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.rolling_oos import run_rolling_oos


def test_rolling_oos_uses_only_prior_history_and_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_rolling_oos(tmp_path, min_history_months=12)

    assert {
        "rolling_icir_weights.csv",
        "oos_factor_score_panel.csv",
        "oos_backtest_report.md",
    } == {path.name for path in result.artifacts.values()}
    assert result.summary["mode"] == "research_mode_oos"
    assert result.summary["uses_full_sample_icir"] is False
    assert result.summary["trade_timing"] == "score_at_t_trade_at_t_plus_1"
    assert result.summary["train_boundary"] < result.summary["test_start"]

    weights = pd.read_csv(tmp_path / "rolling_icir_weights.csv")
    assert {"rebalance_date", "estimation_window_end", "factor", "rolling_icir", "weight"}.issubset(
        weights.columns
    )
    assert (pd.to_datetime(weights["estimation_window_end"]) < pd.to_datetime(weights["rebalance_date"])).all()
    assert set(weights["uses_full_sample_icir"]) == {False}
    assert weights.groupby("rebalance_date")["weight"].apply(lambda series: round(series.abs().sum(), 10)).eq(1.0).all()

    scores = pd.read_csv(tmp_path / "oos_factor_score_panel.csv")
    assert {"date", "ticker", "score", "coverage_state", "signal_timestamp", "tradable_timestamp"}.issubset(
        scores.columns
    )
    assert set(scores["coverage_state"]) == {"active_view"}
    assert (pd.to_datetime(scores["tradable_timestamp"]) > pd.to_datetime(scores["signal_timestamp"])).all()

    report = (tmp_path / "oos_backtest_report.md").read_text(encoding="utf-8")
    assert "Rolling ICIR OOS Backtest" in report
    assert "full-sample ICIR: forbidden" in report
    assert "train_boundary:" in report
    assert "test_start:" in report
    assert "teaching-mode result: separate" in report
    assert "production approval: not claimed" in report
