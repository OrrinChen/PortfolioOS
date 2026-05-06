from __future__ import annotations

from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.marginal_value import run_marginal_value_gate


def test_marginal_value_gate_writes_cluster_residual_and_decision_artifacts(tmp_path: Path) -> None:
    result = run_marginal_value_gate(tmp_path)

    assert {
        "factor_cluster_report.csv",
        "residual_ic_report.csv",
        "marginal_value_decision_table.csv",
    } == {path.name for path in result.artifacts.values()}
    assert result.summary["factor_count"] == 29
    assert result.summary["high_correlation_kept_by_icir_only"] is False
    assert result.summary["production_approval_claimed"] is False

    clusters = pd.read_csv(tmp_path / "factor_cluster_report.csv")
    assert {"factor", "cluster_id", "known_correlation_family", "max_abs_corr"}.issubset(clusters.columns)
    assert clusters["factor"].nunique() == 29

    residual = pd.read_csv(tmp_path / "residual_ic_report.csv")
    assert {"factor", "ic_mean", "max_abs_corr", "residual_ic", "residual_contribution"}.issubset(
        residual.columns
    )
    assert residual["factor"].nunique() == 29
    assert residual["residual_contribution"].notna().all()

    decisions = pd.read_csv(tmp_path / "marginal_value_decision_table.csv")
    required_decisions = {
        "promote_to_allocator",
        "real_but_redundant",
        "archive_no_marginal_value",
        "needs_more_evidence",
        "diagnostic_only",
    }
    assert {"factor", "incremental_net_return", "incremental_turnover", "cost_drag", "decision"}.issubset(
        decisions.columns
    )
    assert set(decisions["decision"]).issubset(required_decisions)
    assert decisions["factor"].nunique() == 29
    assert (decisions["max_abs_corr"] >= 0.8).any()
    assert (
        decisions.loc[decisions["max_abs_corr"] >= 0.8, "decision"] != "promote_to_allocator"
    ).any()
