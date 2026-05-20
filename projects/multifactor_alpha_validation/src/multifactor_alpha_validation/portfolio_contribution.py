from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PortfolioContributionResult:
    factor_ablation_report_path: str
    cluster_ablation_report_path: str
    factor_role_contribution_path: str
    contribution_by_regime_path: str
    summary_path: str
    report_path: str
    validation_status: str
    decision_state: str
    observed_component_count: int
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


_PERIODS_PER_YEAR = 12
_PRIMARY_CONSTRUCTION = "equal_weight_all_components"


def run_post_portfolio_contribution(
    component_pool_path: Path,
    oos_observation_path: Path,
    portfolio_validation_dir: Path,
    output_dir: Path,
) -> PortfolioContributionResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    factor_path = output_dir / "factor_ablation_report.csv"
    cluster_path = output_dir / "cluster_ablation_report.csv"
    role_path = output_dir / "factor_role_contribution.csv"
    regime_path = output_dir / "contribution_by_regime.csv"
    summary_path = output_dir / "portfolio_contribution_summary.json"
    markdown_path = output_dir / "post_portfolio_contribution_report.md"

    pool = _normalize_pool(_read_csv(component_pool_path))
    observations = _normalize_observations(_read_csv(oos_observation_path))
    r15_summary = _read_json(portfolio_validation_dir / "ensemble_validation_summary.json")
    eligible_pool = _eligible_pool(pool)

    if observations.empty:
        factor_report = pd.DataFrame(columns=_factor_columns())
        cluster_report = pd.DataFrame(columns=_cluster_columns())
        role_report = pd.DataFrame(columns=_role_columns())
        regime_report = pd.DataFrame(columns=_regime_columns())
        summary = _unavailable_summary(component_pool_path, oos_observation_path, "missing_oos_observations")
        _write_outputs(factor_path, cluster_path, role_path, regime_path, factor_report, cluster_report, role_report, regime_report)
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(_render_markdown(factor_report, cluster_report, role_report, summary), encoding="utf-8")
        return _result(
            factor_path,
            cluster_path,
            role_path,
            regime_path,
            summary_path,
            markdown_path,
            validation_status="unavailable",
            decision_state=str(summary["decision_state"]),
            observed_count=0,
        )

    observed_ids = sorted(set(observations["factor_id"].astype(str)))
    eligible_ids = set(eligible_pool["factor_id"].astype(str)) if not eligible_pool.empty else set(observed_ids)
    observed_ids = sorted(set(observed_ids) & eligible_ids)
    observations = observations[observations["factor_id"].isin(observed_ids)].copy()
    if not observed_ids or observations.empty:
        factor_report = pd.DataFrame(columns=_factor_columns())
        cluster_report = pd.DataFrame(columns=_cluster_columns())
        role_report = pd.DataFrame(columns=_role_columns())
        regime_report = pd.DataFrame(columns=_regime_columns())
        summary = _unavailable_summary(component_pool_path, oos_observation_path, "no_eligible_observed_components")
        _write_outputs(factor_path, cluster_path, role_path, regime_path, factor_report, cluster_report, role_report, regime_report)
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(_render_markdown(factor_report, cluster_report, role_report, summary), encoding="utf-8")
        return _result(
            factor_path,
            cluster_path,
            role_path,
            regime_path,
            summary_path,
            markdown_path,
            validation_status="unavailable",
            decision_state=str(summary["decision_state"]),
            observed_count=0,
        )

    baseline = _portfolio_period_series(observations, include_ids=observed_ids)
    baseline_metrics = _metrics(baseline)
    factor_report = _factor_ablation_report(observations, eligible_pool, observed_ids, baseline, baseline_metrics)
    cluster_report = _cluster_ablation_report(observations, eligible_pool, observed_ids, baseline_metrics)
    role_report = _role_contribution_report(observations, eligible_pool, observed_ids, baseline_metrics)
    regime_report = _regime_contribution_report(observations, eligible_pool, observed_ids, baseline)
    summary = _summary(
        component_pool_path=component_pool_path,
        oos_observation_path=oos_observation_path,
        r15_summary=r15_summary,
        observed_ids=observed_ids,
        factor_report=factor_report,
        baseline_metrics=baseline_metrics,
    )

    _write_outputs(factor_path, cluster_path, role_path, regime_path, factor_report, cluster_report, role_report, regime_report)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(factor_report, cluster_report, role_report, summary), encoding="utf-8")

    return _result(
        factor_path,
        cluster_path,
        role_path,
        regime_path,
        summary_path,
        markdown_path,
        validation_status="evaluated",
        decision_state=str(summary["decision_state"]),
        observed_count=len(observed_ids),
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _normalize_pool(pool: pd.DataFrame) -> pd.DataFrame:
    if pool.empty:
        return pd.DataFrame(columns=["factor_id", "family_id", "cluster_id", "component_role", "component_status", "component_pool_eligible", "filter_class"])
    normalized = pool.copy()
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    if "family_id" not in normalized.columns:
        normalized["family_id"] = normalized["factor_id"].str.split("_").str[0]
    if "cluster_id" not in normalized.columns:
        normalized["cluster_id"] = normalized["family_id"]
    if "component_role" not in normalized.columns:
        normalized["component_role"] = "component"
    if "component_status" not in normalized.columns:
        normalized["component_status"] = "eligible_component"
    if "component_pool_eligible" not in normalized.columns:
        normalized["component_pool_eligible"] = normalized.get("portfolio_validation_allowed", True)
    if "filter_class" not in normalized.columns:
        normalized["filter_class"] = "soft_resurrected"
    return normalized


def _eligible_pool(pool: pd.DataFrame) -> pd.DataFrame:
    if pool.empty:
        return pool
    eligible = (
        pool["component_pool_eligible"].map(_as_bool)
        & ~pool["component_status"].astype(str).str.contains("blocked", case=False, na=False)
        & ~pool["filter_class"].astype(str).eq("hard_excluded")
    )
    return pool.loc[eligible].copy()


def _normalize_observations(observations: pd.DataFrame) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame(columns=["factor_id", "date", "gross_spread", "net_spread", "qqq_return", "cost_drag"])
    required = {"factor_id", "rebalance_date", "gross_spread", "qqq_return"}
    missing = sorted(required - set(observations.columns))
    if missing:
        raise ValueError(f"portfolio contribution observations missing required columns: {missing}")
    normalized = observations.copy()
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    for column in [
        "gross_spread",
        "net_spread",
        "qqq_return",
        "beta_adjusted_spread",
        "sector_adjusted_spread",
        "style_adjusted_spread",
        "cost_drag",
    ]:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    if "net_spread" not in normalized.columns:
        normalized["net_spread"] = normalized["gross_spread"] - 0.001
    if "cost_drag" not in normalized.columns:
        normalized["cost_drag"] = normalized["gross_spread"] - normalized["net_spread"]
    return normalized.dropna(subset=["factor_id", "date", "gross_spread", "net_spread", "qqq_return"]).sort_values(["date", "factor_id"])


def _factor_ablation_report(
    observations: pd.DataFrame,
    pool: pd.DataFrame,
    observed_ids: list[str],
    baseline: pd.DataFrame,
    baseline_metrics: dict[str, float],
) -> pd.DataFrame:
    metadata = _metadata(pool)
    rows: list[dict[str, Any]] = []
    for factor_id in observed_ids:
        remaining = [value for value in observed_ids if value != factor_id]
        leave_one_out = _portfolio_period_series(observations, include_ids=remaining)
        loo_metrics = _metrics(leave_one_out)
        contribution = _contribution_row(baseline_metrics, loo_metrics)
        factor_rows = observations[observations["factor_id"].eq(factor_id)].copy()
        regime = _factor_regime_stats(observations, baseline, factor_id)
        role = str(metadata.get(factor_id, {}).get("component_role", "component"))
        decision = _component_decision(
            role=role,
            factor_rows=factor_rows,
            contribution=contribution,
            qqq_up_contribution=regime.get("QQQ_up", 0.0),
            qqq_down_contribution=regime.get("QQQ_down", 0.0),
        )
        rows.append(
            {
                "schema_version": "factor_ablation_report.v1",
                "factor_id": factor_id,
                "family_id": metadata.get(factor_id, {}).get("family_id", factor_id),
                "cluster_id": metadata.get(factor_id, {}).get("cluster_id", factor_id),
                "component_role": role,
                "component_status": metadata.get(factor_id, {}).get("component_status", "eligible_component"),
                "baseline_cost_adjusted_return": _round(baseline_metrics["cost_adjusted_return"]),
                "leave_one_out_cost_adjusted_return": _round(loo_metrics["cost_adjusted_return"]),
                "contribution_to_cost_adjusted_return": _round(contribution["cost_adjusted_return"]),
                "contribution_to_gross_return": _round(contribution["gross_return"]),
                "contribution_to_sharpe": _round(contribution["sharpe"]),
                "contribution_to_max_drawdown": _round(contribution["max_drawdown"]),
                "contribution_to_qqq_relative_return": _round(contribution["qqq_relative_return"]),
                "gross_spread_mean": _round(factor_rows["gross_spread"].mean()),
                "net_spread_mean": _round(factor_rows["net_spread"].mean()),
                "cost_drag_mean": _round(factor_rows["cost_drag"].mean()),
                "qqq_up_contribution": _round(regime.get("QQQ_up", 0.0)),
                "qqq_down_contribution": _round(regime.get("QQQ_down", 0.0)),
                "component_decision": decision,
                "or_optimizer_used": False,
                "security_level_portfolio_construction_used": False,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_factor_columns())


def _cluster_ablation_report(
    observations: pd.DataFrame,
    pool: pd.DataFrame,
    observed_ids: list[str],
    baseline_metrics: dict[str, float],
) -> pd.DataFrame:
    metadata = _metadata(pool)
    cluster_map: dict[str, list[str]] = {}
    for factor_id in observed_ids:
        cluster = str(metadata.get(factor_id, {}).get("cluster_id", metadata.get(factor_id, {}).get("family_id", factor_id)))
        cluster_map.setdefault(cluster, []).append(factor_id)
    rows: list[dict[str, Any]] = []
    for cluster_id, factors in sorted(cluster_map.items()):
        remaining = [factor_id for factor_id in observed_ids if factor_id not in set(factors)]
        dropped = _portfolio_period_series(observations, include_ids=remaining)
        dropped_metrics = _metrics(dropped)
        contribution = _contribution_row(baseline_metrics, dropped_metrics)
        rows.append(
            {
                "schema_version": "cluster_ablation_report.v1",
                "cluster_id": cluster_id,
                "dropped_factor_count": len(factors),
                "dropped_factor_ids": ",".join(sorted(factors)),
                "drop_cluster_cost_adjusted_return": _round(dropped_metrics["cost_adjusted_return"]),
                "contribution_to_cost_adjusted_return": _round(contribution["cost_adjusted_return"]),
                "contribution_to_sharpe": _round(contribution["sharpe"]),
                "contribution_to_max_drawdown": _round(contribution["max_drawdown"]),
                "cluster_decision": _aggregate_decision(contribution),
                "or_optimizer_used": False,
                "security_level_portfolio_construction_used": False,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_cluster_columns())


def _role_contribution_report(
    observations: pd.DataFrame,
    pool: pd.DataFrame,
    observed_ids: list[str],
    baseline_metrics: dict[str, float],
) -> pd.DataFrame:
    metadata = _metadata(pool)
    role_map: dict[str, list[str]] = {}
    for factor_id in observed_ids:
        role = str(metadata.get(factor_id, {}).get("component_role", "component"))
        role_map.setdefault(role, []).append(factor_id)
    rows: list[dict[str, Any]] = []
    for role, factors in sorted(role_map.items()):
        role_only = _portfolio_period_series(observations, include_ids=factors)
        drop_role = _portfolio_period_series(observations, include_ids=[factor_id for factor_id in observed_ids if factor_id not in set(factors)])
        role_metrics = _metrics(role_only)
        drop_metrics = _metrics(drop_role)
        contribution = _contribution_row(baseline_metrics, drop_metrics)
        rows.append(
            {
                "schema_version": "factor_role_contribution.v1",
                "component_role": role,
                "factor_count": len(factors),
                "factor_ids": ",".join(sorted(factors)),
                "role_cost_adjusted_return": _round(role_metrics["cost_adjusted_return"]),
                "drop_role_cost_adjusted_return": _round(drop_metrics["cost_adjusted_return"]),
                "contribution_to_cost_adjusted_return": _round(contribution["cost_adjusted_return"]),
                "contribution_to_sharpe": _round(contribution["sharpe"]),
                "contribution_to_max_drawdown": _round(contribution["max_drawdown"]),
                "role_decision": _role_decision(role, contribution),
                "or_optimizer_used": False,
                "security_level_portfolio_construction_used": False,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_role_columns())


def _regime_contribution_report(
    observations: pd.DataFrame,
    pool: pd.DataFrame,
    observed_ids: list[str],
    baseline: pd.DataFrame,
) -> pd.DataFrame:
    metadata = _metadata(pool)
    rows: list[dict[str, Any]] = []
    for factor_id in observed_ids:
        contributions = _factor_period_contributions(observations, baseline, factor_id)
        for regime, group in contributions.groupby("regime", sort=True):
            rows.append(
                {
                    "schema_version": "contribution_by_regime.v1",
                    "factor_id": factor_id,
                    "family_id": metadata.get(factor_id, {}).get("family_id", factor_id),
                    "component_role": metadata.get(factor_id, {}).get("component_role", "component"),
                    "regime": regime,
                    "period_count": int(len(group)),
                    "average_net_contribution": _round(group["net_contribution"].mean()),
                    "average_gross_contribution": _round(group["gross_contribution"].mean()),
                    "positive_contribution_rate": _round(group["net_contribution"].gt(0.0).mean()),
                    "not_alpha_evidence": True,
                }
            )
    return pd.DataFrame(rows, columns=_regime_columns())


def _portfolio_period_series(observations: pd.DataFrame, include_ids: list[str]) -> pd.DataFrame:
    if not include_ids:
        return pd.DataFrame(columns=["date", "gross_return", "net_return", "qqq_return"])
    frame = observations[observations["factor_id"].isin(include_ids)].copy()
    rows: list[dict[str, Any]] = []
    for date, group in frame.groupby("date", sort=True):
        rows.append(
            {
                "date": date,
                "gross_return": float(group["gross_spread"].mean()),
                "net_return": float(group["net_spread"].mean()),
                "qqq_return": float(group["qqq_return"].iloc[0]),
            }
        )
    return pd.DataFrame(rows)


def _factor_period_contributions(observations: pd.DataFrame, baseline: pd.DataFrame, factor_id: str) -> pd.DataFrame:
    factor = observations[observations["factor_id"].eq(factor_id)][["date", "gross_spread", "net_spread", "qqq_return"]].copy()
    if factor.empty:
        return pd.DataFrame(columns=["date", "regime", "gross_contribution", "net_contribution"])
    merged = factor.merge(baseline[["date", "gross_return", "net_return"]], on="date", how="inner")
    merged["gross_contribution"] = merged["gross_spread"] - merged["gross_return"]
    merged["net_contribution"] = merged["net_spread"] - merged["net_return"]
    merged["regime"] = np.where(merged["qqq_return"] >= 0.0, "QQQ_up", "QQQ_down")
    return merged


def _factor_regime_stats(observations: pd.DataFrame, baseline: pd.DataFrame, factor_id: str) -> dict[str, float]:
    contributions = _factor_period_contributions(observations, baseline, factor_id)
    if contributions.empty:
        return {}
    return {str(regime): float(group["net_contribution"].mean()) for regime, group in contributions.groupby("regime")}


def _metrics(periods: pd.DataFrame) -> dict[str, float]:
    if periods.empty:
        return {
            "gross_return": 0.0,
            "cost_adjusted_return": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "qqq_relative_return": 0.0,
        }
    gross = pd.to_numeric(periods["gross_return"], errors="coerce").fillna(0.0)
    net = pd.to_numeric(periods["net_return"], errors="coerce").fillna(0.0)
    qqq = pd.to_numeric(periods["qqq_return"], errors="coerce").fillna(0.0)
    return {
        "gross_return": _annualized_return(gross),
        "cost_adjusted_return": _annualized_return(net),
        "sharpe": _sharpe(net),
        "max_drawdown": _max_drawdown(net),
        "qqq_relative_return": float((net - qqq).mean()) * _PERIODS_PER_YEAR if len(net) else 0.0,
    }


def _contribution_row(baseline: dict[str, float], comparison: dict[str, float]) -> dict[str, float]:
    return {
        "gross_return": baseline["gross_return"] - comparison["gross_return"],
        "cost_adjusted_return": baseline["cost_adjusted_return"] - comparison["cost_adjusted_return"],
        "sharpe": baseline["sharpe"] - comparison["sharpe"],
        "max_drawdown": baseline["max_drawdown"] - comparison["max_drawdown"],
        "qqq_relative_return": baseline["qqq_relative_return"] - comparison["qqq_relative_return"],
    }


def _component_decision(
    role: str,
    factor_rows: pd.DataFrame,
    contribution: dict[str, float],
    qqq_up_contribution: float,
    qqq_down_contribution: float,
) -> str:
    gross_mean = float(factor_rows["gross_spread"].mean()) if not factor_rows.empty else 0.0
    net_mean = float(factor_rows["net_spread"].mean()) if not factor_rows.empty else 0.0
    role_lower = role.lower()
    if gross_mean > 0.0 and net_mean <= 0.0 and contribution["cost_adjusted_return"] < 0.0:
        return "cost_negative_component"
    if "hedge" in role_lower and qqq_down_contribution > 0.0:
        return "hedge_component"
    if qqq_up_contribution * qqq_down_contribution < 0.0:
        return "regime_specific_component"
    if contribution["cost_adjusted_return"] > 0.0 and contribution["sharpe"] > 0.0:
        return "core_component"
    if contribution["max_drawdown"] > 0.0:
        return "diversifier_component"
    if abs(contribution["cost_adjusted_return"]) < 1e-6:
        return "redundant_after_portfolio"
    return "diagnostic_component"


def _aggregate_decision(contribution: dict[str, float]) -> str:
    if contribution["cost_adjusted_return"] > 0.0 and contribution["sharpe"] > 0.0:
        return "core_component"
    if contribution["max_drawdown"] > 0.0:
        return "diversifier_component"
    if contribution["cost_adjusted_return"] < 0.0:
        return "cost_negative_component"
    if abs(contribution["cost_adjusted_return"]) < 1e-6:
        return "redundant_after_portfolio"
    return "diagnostic_component"


def _role_decision(role: str, contribution: dict[str, float]) -> str:
    if "hedge" in role.lower() and contribution["max_drawdown"] > 0.0:
        return "hedge_component"
    return _aggregate_decision(contribution)


def _metadata(pool: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if pool.empty:
        return {}
    return {str(row.factor_id): row._asdict() for row in pool.itertuples(index=False)}


def _summary(
    component_pool_path: Path,
    oos_observation_path: Path,
    r15_summary: dict[str, Any],
    observed_ids: list[str],
    factor_report: pd.DataFrame,
    baseline_metrics: dict[str, float],
) -> dict[str, Any]:
    decision_counts = (
        factor_report["component_decision"].astype(str).value_counts().to_dict() if "component_decision" in factor_report else {}
    )
    return {
        "schema_version": "portfolio_contribution_summary.v1",
        "validation_status": "evaluated",
        "decision_state": "portfolio_contribution_diagnostic_only",
        "component_pool_path": str(component_pool_path),
        "oos_observation_path": str(oos_observation_path),
        "source_r15_decision_state": str(r15_summary.get("decision_state", "unknown")),
        "primary_construction": _PRIMARY_CONSTRUCTION,
        "observed_component_count": len(observed_ids),
        "observed_component_ids": observed_ids,
        "baseline_gross_annualized_return": _round(baseline_metrics["gross_return"]),
        "baseline_cost_adjusted_return": _round(baseline_metrics["cost_adjusted_return"]),
        "baseline_sharpe": _round(baseline_metrics["sharpe"]),
        "baseline_max_drawdown": _round(baseline_metrics["max_drawdown"]),
        "component_decision_counts": decision_counts,
        "fabricated_returns": False,
        "full_sample_weights_used": False,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "non_claims": _non_claims(),
    }


def _unavailable_summary(component_pool_path: Path, oos_observation_path: Path, reason: str) -> dict[str, Any]:
    return {
        "schema_version": "portfolio_contribution_summary.v1",
        "validation_status": "unavailable",
        "decision_state": "portfolio_contribution_unavailable",
        "unavailable_reason": reason,
        "component_pool_path": str(component_pool_path),
        "oos_observation_path": str(oos_observation_path),
        "observed_component_count": 0,
        "fabricated_returns": False,
        "full_sample_weights_used": False,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "non_claims": _non_claims(),
    }


def _render_markdown(
    factor_report: pd.DataFrame,
    cluster_report: pd.DataFrame,
    role_report: pd.DataFrame,
    summary: dict[str, Any],
) -> str:
    lines = [
        "# Post-Portfolio Contribution / Ablation",
        "",
        "This is not alpha evidence. It is a diagnostic attribution pass on observed portfolio components.",
        "",
        "OR remains locked. This report does not run an OR optimizer, security-level portfolio construction, Q2, live trading, broker/order workflows, or production approval.",
        "",
        f"Decision state: `{summary['decision_state']}`",
        f"Observed components: `{summary.get('observed_component_count', 0)}`",
        f"Source R15 decision: `{summary.get('source_r15_decision_state', 'unknown')}`",
        "",
        "## Factor Ablation",
        "",
    ]
    if factor_report.empty:
        lines.append("No factor ablation rows were available.")
    else:
        for row in factor_report.itertuples(index=False):
            lines.append(
                f"- `{row.factor_id}`: contribution `{row.contribution_to_cost_adjusted_return}`, "
                f"decision `{row.component_decision}`."
            )
    lines.extend(["", "## Cluster Ablation", ""])
    if cluster_report.empty:
        lines.append("No cluster ablation rows were available.")
    else:
        for row in cluster_report.itertuples(index=False):
            lines.append(
                f"- `{row.cluster_id}`: contribution `{row.contribution_to_cost_adjusted_return}`, "
                f"decision `{row.cluster_decision}`."
            )
    lines.extend(["", "## Role Contribution", ""])
    if role_report.empty:
        lines.append("No role contribution rows were available.")
    else:
        for row in role_report.itertuples(index=False):
            lines.append(
                f"- `{row.component_role}`: contribution `{row.contribution_to_cost_adjusted_return}`, "
                f"decision `{row.role_decision}`."
            )
    lines.append("")
    return "\n".join(lines)


def _write_outputs(
    factor_path: Path,
    cluster_path: Path,
    role_path: Path,
    regime_path: Path,
    factor_report: pd.DataFrame,
    cluster_report: pd.DataFrame,
    role_report: pd.DataFrame,
    regime_report: pd.DataFrame,
) -> None:
    factor_report.to_csv(factor_path, index=False)
    cluster_report.to_csv(cluster_path, index=False)
    role_report.to_csv(role_path, index=False)
    regime_report.to_csv(regime_path, index=False)


def _result(
    factor_path: Path,
    cluster_path: Path,
    role_path: Path,
    regime_path: Path,
    summary_path: Path,
    markdown_path: Path,
    *,
    validation_status: str,
    decision_state: str,
    observed_count: int,
) -> PortfolioContributionResult:
    return PortfolioContributionResult(
        factor_ablation_report_path=str(factor_path),
        cluster_ablation_report_path=str(cluster_path),
        factor_role_contribution_path=str(role_path),
        contribution_by_regime_path=str(regime_path),
        summary_path=str(summary_path),
        report_path=str(markdown_path),
        validation_status=validation_status,
        decision_state=decision_state,
        observed_component_count=observed_count,
        production_approval=False,
        live_trading=False,
        direct_q2_entry=False,
        not_alpha_evidence=True,
    )


def _annualized_return(returns: pd.Series) -> float:
    if len(returns) == 0:
        return 0.0
    total = float((1.0 + returns.fillna(0.0)).prod() - 1.0)
    base = max(1.0 + total, 1e-9)
    return float(base ** (_PERIODS_PER_YEAR / len(returns)) - 1.0)


def _sharpe(returns: pd.Series) -> float:
    if len(returns) < 2:
        return 0.0
    std = float(returns.std(ddof=0))
    if std <= 1e-12:
        return 0.0
    return float(returns.mean()) / std * float(np.sqrt(_PERIODS_PER_YEAR))


def _max_drawdown(returns: pd.Series) -> float:
    if len(returns) == 0:
        return 0.0
    cumulative = (1.0 + returns.fillna(0.0)).cumprod()
    running_max = cumulative.cummax()
    return float((cumulative / running_max - 1.0).min())


def _round(value: object) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return 0.0
    return round(float(numeric), 10)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _non_claims() -> dict[str, bool]:
    return {
        "not_alpha_evidence": True,
        "production_approval": False,
        "paper_canary": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
        "or_optimizer": False,
        "security_level_portfolio_construction": False,
    }


def _factor_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "family_id",
        "cluster_id",
        "component_role",
        "component_status",
        "baseline_cost_adjusted_return",
        "leave_one_out_cost_adjusted_return",
        "contribution_to_cost_adjusted_return",
        "contribution_to_gross_return",
        "contribution_to_sharpe",
        "contribution_to_max_drawdown",
        "contribution_to_qqq_relative_return",
        "gross_spread_mean",
        "net_spread_mean",
        "cost_drag_mean",
        "qqq_up_contribution",
        "qqq_down_contribution",
        "component_decision",
        "or_optimizer_used",
        "security_level_portfolio_construction_used",
        "not_alpha_evidence",
    ]


def _cluster_columns() -> list[str]:
    return [
        "schema_version",
        "cluster_id",
        "dropped_factor_count",
        "dropped_factor_ids",
        "drop_cluster_cost_adjusted_return",
        "contribution_to_cost_adjusted_return",
        "contribution_to_sharpe",
        "contribution_to_max_drawdown",
        "cluster_decision",
        "or_optimizer_used",
        "security_level_portfolio_construction_used",
        "not_alpha_evidence",
    ]


def _role_columns() -> list[str]:
    return [
        "schema_version",
        "component_role",
        "factor_count",
        "factor_ids",
        "role_cost_adjusted_return",
        "drop_role_cost_adjusted_return",
        "contribution_to_cost_adjusted_return",
        "contribution_to_sharpe",
        "contribution_to_max_drawdown",
        "role_decision",
        "or_optimizer_used",
        "security_level_portfolio_construction_used",
        "not_alpha_evidence",
    ]


def _regime_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "family_id",
        "component_role",
        "regime",
        "period_count",
        "average_net_contribution",
        "average_gross_contribution",
        "positive_contribution_rate",
        "not_alpha_evidence",
    ]
