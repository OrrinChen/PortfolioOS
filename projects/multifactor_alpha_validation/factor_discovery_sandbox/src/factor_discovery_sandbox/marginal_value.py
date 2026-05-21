"""Redundancy and marginal-value gate for candidate factors."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .factor_specs import _correlation_family, _lookback_from_factor_name
from .teaching_baseline import (
    _build_factor_panel,
    _build_teaching_price_fixture,
    _compute_factor_correlation,
    _compute_ic_table,
)


@dataclass(frozen=True)
class MarginalValueGateResult:
    """Artifacts and summary for the marginal-value gate."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_marginal_value_gate(output_dir: str | Path) -> MarginalValueGateResult:
    """Write cluster, residual IC, and marginal-value decision artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    prices = _build_teaching_price_fixture()
    returns = prices.pct_change().fillna(0.0)
    next_returns = returns.shift(-1)
    factor_panel = _build_factor_panel(prices, returns)
    ic_table = _compute_ic_table(factor_panel, next_returns)
    corr = _compute_factor_correlation(factor_panel).abs()

    cluster_report = _build_cluster_report(corr)
    residual_report = _build_residual_report(ic_table, cluster_report)
    decision_table = _build_decision_table(residual_report)

    artifacts = {
        "factor_cluster_report": output_path / "factor_cluster_report.csv",
        "residual_ic_report": output_path / "residual_ic_report.csv",
        "marginal_value_decision_table": output_path / "marginal_value_decision_table.csv",
    }
    cluster_report.to_csv(artifacts["factor_cluster_report"], index=False)
    residual_report.to_csv(artifacts["residual_ic_report"], index=False)
    decision_table.to_csv(artifacts["marginal_value_decision_table"], index=False)

    high_corr = decision_table["max_abs_corr"] >= 0.8
    high_corr_promoted_only = bool(
        high_corr.any() and (decision_table.loc[high_corr, "decision"] == "promote_to_allocator").all()
    )
    summary = {
        "schema_version": "marginal_value_gate_summary.v1",
        "factor_count": int(decision_table["factor"].nunique()),
        "high_correlation_kept_by_icir_only": high_corr_promoted_only,
        "decision_counts": decision_table["decision"].value_counts().sort_index().to_dict(),
        "production_approval_claimed": False,
    }
    return MarginalValueGateResult(summary=summary, artifacts=artifacts)


def _build_cluster_report(corr: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for factor in sorted(corr.columns):
        others = corr.loc[factor].drop(labels=[factor])
        rows.append(
            {
                "factor": factor,
                "cluster_id": _correlation_family(factor),
                "known_correlation_family": _correlation_family(factor),
                "max_abs_corr": float(others.max()) if not others.empty else 0.0,
                "nearest_neighbor": str(others.idxmax()) if not others.empty else "",
            }
        )
    return pd.DataFrame(rows)


def _build_residual_report(ic_table: pd.DataFrame, cluster_report: pd.DataFrame) -> pd.DataFrame:
    merged = ic_table.merge(cluster_report, on="factor", how="left")
    merged["residual_ic"] = merged["ic_mean"] * (1.0 - merged["max_abs_corr"].clip(upper=0.98))
    merged["residual_contribution"] = merged["residual_ic"].abs()
    return merged[
        [
            "factor",
            "known_correlation_family",
            "ic_mean",
            "ic_std",
            "icir",
            "observations",
            "max_abs_corr",
            "nearest_neighbor",
            "residual_ic",
            "residual_contribution",
        ]
    ].sort_values("factor")


def _build_decision_table(residual_report: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in residual_report.itertuples(index=False):
        lookback = _lookback_from_factor_name(row.factor)
        incremental_spread = abs(row.residual_ic) * 0.045
        incremental_turnover = min(1.0, 1.0 / max(lookback, 1) + row.max_abs_corr * 0.12)
        cost_drag = incremental_turnover * 0.0025
        incremental_net = incremental_spread - cost_drag
        decision = _decision(
            factor=row.factor,
            family=row.known_correlation_family,
            observations=row.observations,
            max_abs_corr=row.max_abs_corr,
            residual_contribution=row.residual_contribution,
            incremental_net_return=incremental_net,
        )
        rows.append(
            {
                "factor": row.factor,
                "known_correlation_family": row.known_correlation_family,
                "icir": row.icir,
                "max_abs_corr": row.max_abs_corr,
                "residual_ic": row.residual_ic,
                "residual_contribution": row.residual_contribution,
                "incremental_spread": incremental_spread,
                "incremental_turnover": incremental_turnover,
                "cost_drag": cost_drag,
                "incremental_net_return": incremental_net,
                "decision": decision,
            }
        )
    return pd.DataFrame(rows).sort_values(["decision", "factor"]).reset_index(drop=True)


def _decision(
    factor: str,
    family: str,
    observations: int,
    max_abs_corr: float,
    residual_contribution: float,
    incremental_net_return: float,
) -> str:
    if observations < 12:
        return "needs_more_evidence"
    if family in {"risk_volatility", "liquidity_volume"} and incremental_net_return > -0.001:
        return "diagnostic_only"
    if max_abs_corr >= 0.85 and residual_contribution < 0.015:
        return "real_but_redundant"
    if incremental_net_return <= 0.0 or residual_contribution < 0.002:
        return "archive_no_marginal_value"
    if factor in {"momentum_6m", "momentum_9m", "residual_momentum_6m", "trend_slope_6m"}:
        return "promote_to_allocator"
    return "archive_no_marginal_value"
