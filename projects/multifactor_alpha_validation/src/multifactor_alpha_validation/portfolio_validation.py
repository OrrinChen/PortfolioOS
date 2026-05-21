from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PortfolioValidationResult:
    portfolio_ensemble_oos_report_path: str
    ensemble_vs_baselines_path: str
    summary_path: str
    random_weight_placebo_report_path: str
    permuted_signal_placebo_report_path: str
    report_path: str
    validation_status: str
    decision_state: str
    input_component_count: int
    available_component_count: int
    unavailable_component_count: int
    hard_blocked_component_count: int
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


_ENSEMBLE_IDS = [
    "equal_weight_all_components",
    "equal_weight_by_cluster",
    "inverse_vol_ensemble",
    "simple_shrinkage_ensemble",
    "current_three_factor_component_ensemble",
    "best_single_factor",
    "QQQ_benchmark",
    "random_weight_placebo",
    "permuted_signal_placebo",
]
_COST_DRAG_DEFAULT = 0.001
_PERIODS_PER_YEAR = 12


def run_portfolio_ensemble_validation(
    component_pool_path: Path,
    component_candidate_path: Path,
    oos_observation_path: Path,
    waterfall_by_period_path: Path,
    output_dir: Path,
    random_seed: int = 17,
) -> PortfolioValidationResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "portfolio_ensemble_oos_report.csv"
    baseline_path = output_dir / "ensemble_vs_baselines.csv"
    summary_path = output_dir / "ensemble_validation_summary.json"
    random_path = output_dir / "random_weight_placebo_report.csv"
    permuted_path = output_dir / "permuted_signal_placebo_report.csv"
    markdown_path = output_dir / "portfolio_validation_report.md"

    component_pool = _read_csv(component_pool_path)
    component_table = _read_csv(component_candidate_path)
    observations = _read_csv(oos_observation_path)
    waterfall = _read_csv(waterfall_by_period_path)
    pool = _normalize_component_pool(component_pool, component_table)
    input_count = int(len(pool))
    hard_blocked_count = _hard_blocked_count(pool)

    if observations.empty:
        summary = _unavailable_summary(
            reason="missing_oos_observations",
            component_pool_path=component_pool_path,
            input_count=input_count,
            hard_blocked_count=hard_blocked_count,
        )
        _write_unavailable_outputs(report_path, baseline_path, random_path, permuted_path)
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(_render_markdown(pd.DataFrame(), summary), encoding="utf-8")
        return _result(
            report_path,
            baseline_path,
            summary_path,
            random_path,
            permuted_path,
            markdown_path,
            validation_status="unavailable",
            decision_state=str(summary["decision_state"]),
            input_count=input_count,
            available_count=0,
            unavailable_count=int(summary["unavailable_component_count"]),
            hard_blocked_count=hard_blocked_count,
        )

    normalized_observations = _normalize_observations(observations, waterfall)
    eligible_pool = _eligible_pool(pool)
    eligible_ids = set(eligible_pool["factor_id"].astype(str)) if not eligible_pool.empty else set()
    observed_ids = set(normalized_observations["factor_id"].astype(str))
    available_ids = sorted(eligible_ids & observed_ids)
    unavailable_ids = sorted(eligible_ids - observed_ids)
    if not available_ids:
        summary = _unavailable_summary(
            reason="no_component_oos_overlap",
            component_pool_path=component_pool_path,
            input_count=input_count,
            hard_blocked_count=hard_blocked_count,
            unavailable_ids=unavailable_ids,
        )
        _write_unavailable_outputs(report_path, baseline_path, random_path, permuted_path)
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(_render_markdown(pd.DataFrame(), summary), encoding="utf-8")
        return _result(
            report_path,
            baseline_path,
            summary_path,
            random_path,
            permuted_path,
            markdown_path,
            validation_status="unavailable",
            decision_state=str(summary["decision_state"]),
            input_count=input_count,
            available_count=0,
            unavailable_count=len(unavailable_ids),
            hard_blocked_count=hard_blocked_count,
        )

    observations_for_pool = normalized_observations[normalized_observations["factor_id"].isin(available_ids)].copy()
    period_returns = _build_all_ensemble_period_returns(
        observations_for_pool=observations_for_pool,
        eligible_pool=eligible_pool,
        component_table=component_table,
        random_seed=random_seed,
    )
    report = _build_metric_report(period_returns)
    baselines = _build_baseline_comparisons(report)
    decision_state = _decision_state(report)
    summary = _summary(
        component_pool_path=component_pool_path,
        observation_path=oos_observation_path,
        input_count=input_count,
        hard_blocked_count=hard_blocked_count,
        available_ids=available_ids,
        unavailable_ids=unavailable_ids,
        period_returns=period_returns,
        report=report,
        baselines=baselines,
        decision_state=decision_state,
    )

    report.to_csv(report_path, index=False)
    baselines.to_csv(baseline_path, index=False)
    period_returns[period_returns["ensemble_id"].eq("random_weight_placebo")].to_csv(random_path, index=False)
    period_returns[period_returns["ensemble_id"].eq("permuted_signal_placebo")].to_csv(permuted_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report, summary), encoding="utf-8")

    return _result(
        report_path,
        baseline_path,
        summary_path,
        random_path,
        permuted_path,
        markdown_path,
        validation_status="evaluated",
        decision_state=decision_state,
        input_count=input_count,
        available_count=len(available_ids),
        unavailable_count=len(unavailable_ids),
        hard_blocked_count=hard_blocked_count,
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _normalize_component_pool(component_pool: pd.DataFrame, component_table: pd.DataFrame) -> pd.DataFrame:
    if not component_pool.empty:
        pool = component_pool.copy()
    else:
        pool = component_table.copy()
    if pool.empty:
        return pd.DataFrame(columns=["factor_id", "family_id", "component_pool_eligible", "component_status"])
    if "factor_id" not in pool.columns:
        raise ValueError("component pool requires factor_id")
    pool["factor_id"] = pool["factor_id"].astype(str)
    if "family_id" not in pool.columns:
        pool["family_id"] = pool["factor_id"].str.split("_").str[0]
    if "component_pool_eligible" not in pool.columns:
        pool["component_pool_eligible"] = pool.get("portfolio_validation_allowed", True)
    if "component_status" not in pool.columns:
        pool["component_status"] = "eligible_component"
    if "component_role" not in pool.columns:
        pool["component_role"] = "component"
    if "filter_class" not in pool.columns:
        pool["filter_class"] = "soft_resurrected"
    if "cluster_id" not in pool.columns:
        pool["cluster_id"] = pool["family_id"]
    return pool


def _hard_blocked_count(pool: pd.DataFrame) -> int:
    if pool.empty:
        return 0
    blocked = (
        ~pool["component_pool_eligible"].map(_as_bool)
        | pool["component_status"].astype(str).str.contains("blocked", case=False, na=False)
        | pool["filter_class"].astype(str).eq("hard_excluded")
    )
    return int(blocked.sum())


def _eligible_pool(pool: pd.DataFrame) -> pd.DataFrame:
    if pool.empty:
        return pool.copy()
    eligible = (
        pool["component_pool_eligible"].map(_as_bool)
        & ~pool["component_status"].astype(str).str.contains("blocked", case=False, na=False)
        & ~pool["filter_class"].astype(str).eq("hard_excluded")
    )
    return pool.loc[eligible].copy()


def _normalize_observations(observations: pd.DataFrame, waterfall: pd.DataFrame) -> pd.DataFrame:
    required = {"factor_id", "rebalance_date", "gross_spread", "qqq_return"}
    missing = sorted(required - set(observations.columns))
    if missing:
        raise ValueError(f"oos observations missing required columns: {missing}")
    normalized = observations.copy()
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    for column in [
        "gross_spread",
        "net_spread",
        "qqq_return",
        "qqq_relative_spread",
        "beta_adjusted_spread",
        "sector_adjusted_spread",
        "style_adjusted_spread",
        "cost_drag",
    ]:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    if "net_spread" not in normalized.columns:
        normalized["net_spread"] = normalized["gross_spread"] - _COST_DRAG_DEFAULT
    if "cost_drag" not in normalized.columns:
        normalized["cost_drag"] = normalized["gross_spread"] - normalized["net_spread"]
    normalized = normalized.dropna(subset=["factor_id", "date", "gross_spread", "qqq_return"])
    if not waterfall.empty and {"factor_id", "date"} <= set(waterfall.columns):
        wf = waterfall.copy()
        wf["factor_id"] = wf["factor_id"].astype(str)
        wf["date"] = pd.to_datetime(wf["date"], errors="coerce")
        keep = [
            column
            for column in [
                "factor_id",
                "date",
                "industry_adjusted_spread",
                "style_proxy_adjusted_spread",
                "full_residual_spread",
            ]
            if column in wf.columns
        ]
        wf = wf[keep].copy()
        for column in keep:
            if column not in {"factor_id", "date"}:
                wf[column] = pd.to_numeric(wf[column], errors="coerce")
        normalized = normalized.merge(wf, on=["factor_id", "date"], how="left")
    if "industry_adjusted_spread" not in normalized.columns:
        normalized["industry_adjusted_spread"] = normalized.get("sector_adjusted_spread", np.nan)
    if "style_proxy_adjusted_spread" not in normalized.columns:
        normalized["style_proxy_adjusted_spread"] = normalized.get("style_adjusted_spread", np.nan)
    if "full_residual_spread" not in normalized.columns:
        normalized["full_residual_spread"] = normalized.get("style_proxy_adjusted_spread", np.nan)
    return normalized.sort_values(["date", "factor_id"])


def _build_all_ensemble_period_returns(
    observations_for_pool: pd.DataFrame,
    eligible_pool: pd.DataFrame,
    component_table: pd.DataFrame,
    random_seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(random_seed)
    permuted = _permuted_observations(observations_for_pool, rng)
    rows: list[dict[str, Any]] = []
    for ensemble_id in _ENSEMBLE_IDS:
        source = permuted if ensemble_id == "permuted_signal_placebo" else observations_for_pool
        rows.extend(
            _period_returns_for_ensemble(
                ensemble_id=ensemble_id,
                observations=source,
                original_observations=observations_for_pool,
                eligible_pool=eligible_pool,
                component_table=component_table,
                rng=rng,
            )
        )
    return pd.DataFrame(rows, columns=_period_columns())


def _permuted_observations(observations: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    permuted = observations.copy()
    for column in [
        "gross_spread",
        "net_spread",
        "qqq_relative_spread",
        "beta_adjusted_spread",
        "sector_adjusted_spread",
        "industry_adjusted_spread",
        "style_adjusted_spread",
        "style_proxy_adjusted_spread",
        "full_residual_spread",
    ]:
        if column in permuted.columns:
            values = pd.to_numeric(permuted[column], errors="coerce").to_numpy()
            permuted[column] = rng.permutation(values)
    return permuted


def _period_returns_for_ensemble(
    ensemble_id: str,
    observations: pd.DataFrame,
    original_observations: pd.DataFrame,
    eligible_pool: pd.DataFrame,
    component_table: pd.DataFrame,
    rng: np.random.Generator,
) -> list[dict[str, Any]]:
    dates = sorted(pd.Timestamp(value) for value in original_observations["date"].dropna().unique())
    rows: list[dict[str, Any]] = []
    if ensemble_id == "QQQ_benchmark":
        for date in dates:
            qqq_return = _date_qqq_return(original_observations, date)
            rows.append(_period_row(ensemble_id, date, {"QQQ": 1.0}, qqq_return, qqq_return, qqq_return, qqq_return, qqq_return, qqq_return, qqq_return, 0.0))
        return rows

    previous_weights: dict[str, float] | None = None
    for date in dates:
        current = observations[observations["date"].eq(date)].copy()
        original_current = original_observations[original_observations["date"].eq(date)].copy()
        if current.empty or original_current.empty:
            continue
        prior = original_observations[original_observations["date"] < date].copy()
        factor_ids = sorted(set(current["factor_id"].astype(str)))
        if ensemble_id == "equal_weight_all_components":
            weights = _equal_weights(factor_ids)
        elif ensemble_id == "equal_weight_by_cluster":
            weights = _cluster_equal_weights(factor_ids, eligible_pool)
        elif ensemble_id == "inverse_vol_ensemble":
            weights = _inverse_vol_weights(factor_ids, prior)
        elif ensemble_id == "simple_shrinkage_ensemble":
            weights = _simple_shrinkage_weights(factor_ids, prior)
        elif ensemble_id == "current_three_factor_component_ensemble":
            ids = _current_component_ids(factor_ids, component_table)
            weights = _equal_weights(ids)
        elif ensemble_id == "best_single_factor":
            weights = _best_single_factor_weights(factor_ids, prior)
        elif ensemble_id == "random_weight_placebo":
            weights = _random_weights(factor_ids, rng)
        elif ensemble_id == "permuted_signal_placebo":
            weights = _equal_weights(factor_ids)
        else:
            weights = _equal_weights(factor_ids)
        turnover = _weight_turnover(previous_weights, weights)
        previous_weights = weights
        qqq_return = _date_qqq_return(original_observations, date)
        rows.append(
            _period_row(
                ensemble_id=ensemble_id,
                date=date,
                weights=weights,
                gross_return=_weighted_value(current, weights, "gross_spread"),
                cost_adjusted_return=_weighted_value(current, weights, "net_spread"),
                qqq_return=qqq_return,
                beta_adjusted_return=_weighted_value(current, weights, "beta_adjusted_spread"),
                sector_proxy_residual_return=_weighted_value(current, weights, "industry_adjusted_spread"),
                style_proxy_residual_return=_weighted_value(current, weights, "style_proxy_adjusted_spread"),
                full_residual_return=_weighted_value(current, weights, "full_residual_spread"),
                turnover=turnover,
            )
        )
    return rows


def _equal_weights(factor_ids: list[str]) -> dict[str, float]:
    if not factor_ids:
        return {}
    weight = 1.0 / len(factor_ids)
    return {factor_id: weight for factor_id in factor_ids}


def _cluster_equal_weights(factor_ids: list[str], eligible_pool: pd.DataFrame) -> dict[str, float]:
    if not factor_ids:
        return {}
    pool = eligible_pool.set_index("factor_id") if not eligible_pool.empty else pd.DataFrame()
    cluster_map: dict[str, list[str]] = {}
    for factor_id in factor_ids:
        cluster = factor_id
        if not pool.empty and factor_id in pool.index:
            value = pool.loc[factor_id].get("cluster_id", pool.loc[factor_id].get("family_id", factor_id))
            cluster = str(value)
        cluster_map.setdefault(cluster, []).append(factor_id)
    weights: dict[str, float] = {}
    cluster_weight = 1.0 / len(cluster_map)
    for ids in cluster_map.values():
        for factor_id in ids:
            weights[factor_id] = cluster_weight / len(ids)
    return weights


def _inverse_vol_weights(factor_ids: list[str], prior: pd.DataFrame) -> dict[str, float]:
    if len(prior) < 2:
        return _equal_weights(factor_ids)
    scores: dict[str, float] = {}
    for factor_id in factor_ids:
        series = pd.to_numeric(prior.loc[prior["factor_id"].eq(factor_id), "gross_spread"], errors="coerce").dropna()
        if len(series) < 2:
            scores[factor_id] = 1.0
        else:
            scores[factor_id] = 1.0 / max(float(series.std(ddof=0)), 1e-6)
    return _normalize_scores(scores)


def _simple_shrinkage_weights(factor_ids: list[str], prior: pd.DataFrame) -> dict[str, float]:
    if len(prior) < 2:
        return _equal_weights(factor_ids)
    equal = _equal_weights(factor_ids)
    scores: dict[str, float] = {}
    for factor_id in factor_ids:
        series = pd.to_numeric(prior.loc[prior["factor_id"].eq(factor_id), "net_spread"], errors="coerce").dropna()
        if len(series) < 2:
            scores[factor_id] = 0.0
        else:
            mean = float(series.mean())
            vol = max(float(series.std(ddof=0)), 1e-6)
            scores[factor_id] = max(mean, 0.0) / vol
    if sum(scores.values()) <= 0.0:
        return equal
    active = _normalize_scores(scores)
    return {factor_id: 0.5 * equal[factor_id] + 0.5 * active.get(factor_id, 0.0) for factor_id in factor_ids}


def _current_component_ids(factor_ids: list[str], component_table: pd.DataFrame) -> list[str]:
    if component_table.empty or "factor_id" not in component_table.columns:
        return factor_ids
    allowed = component_table.copy()
    if "portfolio_validation_allowed" in allowed.columns:
        allowed = allowed[allowed["portfolio_validation_allowed"].map(_as_bool)]
    if "component_status" in allowed.columns:
        allowed = allowed[~allowed["component_status"].astype(str).str.contains("blocked", case=False, na=False)]
    ids = sorted(set(allowed["factor_id"].astype(str)) & set(factor_ids))
    return ids or factor_ids


def _best_single_factor_weights(factor_ids: list[str], prior: pd.DataFrame) -> dict[str, float]:
    if prior.empty:
        best = sorted(factor_ids)[0]
    else:
        means = (
            prior[prior["factor_id"].isin(factor_ids)]
            .groupby("factor_id")["net_spread"]
            .mean()
            .sort_values(ascending=False)
        )
        best = str(means.index[0]) if len(means) else sorted(factor_ids)[0]
    return {factor_id: 1.0 if factor_id == best else 0.0 for factor_id in factor_ids}


def _random_weights(factor_ids: list[str], rng: np.random.Generator) -> dict[str, float]:
    if not factor_ids:
        return {}
    draws = rng.dirichlet(np.ones(len(factor_ids)))
    return {factor_id: float(weight) for factor_id, weight in zip(factor_ids, draws)}


def _normalize_scores(scores: dict[str, float]) -> dict[str, float]:
    total = float(sum(max(value, 0.0) for value in scores.values()))
    if total <= 0.0:
        return _equal_weights(sorted(scores))
    return {key: max(value, 0.0) / total for key, value in scores.items()}


def _weighted_value(frame: pd.DataFrame, weights: dict[str, float], column: str) -> float:
    if column not in frame.columns:
        return float("nan")
    values = frame.set_index("factor_id")[column]
    total = 0.0
    active_weight = 0.0
    for factor_id, weight in weights.items():
        if factor_id in values.index and pd.notna(values.loc[factor_id]):
            total += float(weight) * float(values.loc[factor_id])
            active_weight += float(weight)
    if active_weight <= 0.0:
        return float("nan")
    return total / active_weight


def _date_qqq_return(observations: pd.DataFrame, date: pd.Timestamp) -> float:
    values = pd.to_numeric(observations.loc[observations["date"].eq(date), "qqq_return"], errors="coerce").dropna()
    return float(values.iloc[0]) if len(values) else 0.0


def _weight_turnover(previous: dict[str, float] | None, current: dict[str, float]) -> float:
    if previous is None:
        return 0.0
    keys = set(previous) | set(current)
    return 0.5 * sum(abs(float(current.get(key, 0.0)) - float(previous.get(key, 0.0))) for key in keys)


def _period_row(
    ensemble_id: str,
    date: pd.Timestamp,
    weights: dict[str, float],
    gross_return: float,
    cost_adjusted_return: float,
    qqq_return: float,
    beta_adjusted_return: float,
    sector_proxy_residual_return: float,
    style_proxy_residual_return: float,
    full_residual_return: float,
    turnover: float,
) -> dict[str, Any]:
    return {
        "schema_version": "portfolio_ensemble_period_return.v1",
        "ensemble_id": ensemble_id,
        "date": date.date().isoformat(),
        "component_count": len([weight for weight in weights.values() if weight > 0.0]),
        "gross_return": _round(gross_return),
        "cost_adjusted_period_return": _round(cost_adjusted_return),
        "qqq_return": _round(qqq_return),
        "qqq_relative_period_return": _round(cost_adjusted_return - qqq_return),
        "beta_adjusted_period_return": _round(beta_adjusted_return),
        "sector_style_proxy_residual_period_return": _round(sector_proxy_residual_return),
        "style_proxy_residual_period_return": _round(style_proxy_residual_return),
        "full_residual_period_return": _round(full_residual_return),
        "turnover": _round(turnover),
        "weights_json": json.dumps(weights, sort_keys=True),
        "full_sample_weights_used": False,
        "prior_history_only": True,
        "uses_unrestricted_optimizer": False,
        "not_alpha_evidence": True,
    }


def _build_metric_report(period_returns: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for ensemble_id, group in period_returns.groupby("ensemble_id", sort=False):
        returns = pd.to_numeric(group["gross_return"], errors="coerce").fillna(0.0)
        cost_returns = pd.to_numeric(group["cost_adjusted_period_return"], errors="coerce").fillna(0.0)
        qqq = pd.to_numeric(group["qqq_return"], errors="coerce").fillna(0.0)
        beta = _beta(cost_returns, qqq)
        rows.append(
            {
                "schema_version": "portfolio_ensemble_oos_report.v1",
                "ensemble_id": ensemble_id,
                "period_count": int(len(group)),
                "total_return": _round(_compound_return(returns)),
                "annualized_return": _round(_annualized_return(returns)),
                "annualized_excess_return_vs_QQQ": _round(_annualized_return(cost_returns) - _annualized_return(qqq)),
                "Sharpe": _round(_sharpe(cost_returns)),
                "max_drawdown": _round(_max_drawdown(cost_returns)),
                "hit_rate": _round(float(cost_returns.gt(0.0).mean()) if len(cost_returns) else 0.0),
                "turnover": _round(pd.to_numeric(group["turnover"], errors="coerce").fillna(0.0).mean()),
                "cost_adjusted_return": _round(_annualized_return(cost_returns)),
                "cost_adjusted_total_return": _round(_compound_return(cost_returns)),
                "beta": _round(beta),
                "QQQ_relative_return": _round(float((cost_returns - qqq).mean()) * _PERIODS_PER_YEAR),
                "beta_adjusted_return": _round(float((cost_returns - beta * qqq).mean()) * _PERIODS_PER_YEAR),
                "sector_style_proxy_residual": _round(_annualized_mean(group, "sector_style_proxy_residual_period_return")),
                "style_proxy_residual": _round(_annualized_mean(group, "style_proxy_residual_period_return")),
                "full_residual_return": _round(_annualized_mean(group, "full_residual_period_return")),
                "portfolio_volatility": _round(float(cost_returns.std(ddof=0)) * np.sqrt(_PERIODS_PER_YEAR)),
                "full_sample_weights_used": False,
                "prior_history_only": True,
                "uses_unrestricted_optimizer": False,
                "not_alpha_evidence": True,
            }
        )
    report = pd.DataFrame(rows, columns=_metric_columns())
    best_drawdown = _metric_value(report, "best_single_factor", "max_drawdown")
    report["drawdown_vs_best_single_factor"] = report["max_drawdown"] - best_drawdown
    return report


def _build_baseline_comparisons(report: pd.DataFrame) -> pd.DataFrame:
    if report.empty:
        return pd.DataFrame(columns=_baseline_columns())
    best_sharpe = _metric_value(report, "best_single_factor", "Sharpe")
    random_sharpe = _metric_value(report, "random_weight_placebo", "Sharpe")
    permuted_sharpe = _metric_value(report, "permuted_signal_placebo", "Sharpe")
    rows: list[dict[str, Any]] = []
    for row in report.itertuples(index=False):
        rows.append(
            {
                "schema_version": "ensemble_vs_baselines.v1",
                "ensemble_id": row.ensemble_id,
                "comparison_role": _comparison_role(str(row.ensemble_id)),
                "annualized_excess_return_vs_QQQ": row.annualized_excess_return_vs_QQQ,
                "sharpe_delta_vs_best_single_factor": _round(float(row.Sharpe) - best_sharpe),
                "beats_best_single_factor_on_sharpe": float(row.Sharpe) > best_sharpe,
                "beats_random_weight_placebo": float(row.Sharpe) > random_sharpe,
                "beats_permuted_signal_placebo": float(row.Sharpe) > permuted_sharpe,
                "benchmark_exposure_flag": float(row.QQQ_relative_return) <= 0.0 and abs(float(row.beta)) >= 0.5,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_baseline_columns())


def _decision_state(report: pd.DataFrame) -> str:
    if report.empty:
        return "portfolio_component_pool_inconclusive"
    primary = _metric_row(report, "equal_weight_all_components")
    if primary is None:
        return "portfolio_component_pool_inconclusive"
    if float(primary["cost_adjusted_return"]) <= 0.0:
        return "portfolio_component_pool_fails_cost"
    if float(primary["QQQ_relative_return"]) <= 0.0 and abs(float(primary["beta"])) >= 0.5:
        return "portfolio_component_pool_is_benchmark_exposure"
    if float(primary["annualized_excess_return_vs_QQQ"]) <= 0.0:
        return "portfolio_component_pool_fails_oos"
    best = _metric_value(report, "best_single_factor", "Sharpe")
    random = _metric_value(report, "random_weight_placebo", "Sharpe")
    permuted = _metric_value(report, "permuted_signal_placebo", "Sharpe")
    if float(primary["Sharpe"]) <= best:
        return "portfolio_component_pool_fails_oos"
    if float(primary["Sharpe"]) <= random or float(primary["Sharpe"]) <= permuted:
        return "portfolio_component_pool_fails_placebo"
    return "portfolio_component_pool_survives"


def _summary(
    component_pool_path: Path,
    observation_path: Path,
    input_count: int,
    hard_blocked_count: int,
    available_ids: list[str],
    unavailable_ids: list[str],
    period_returns: pd.DataFrame,
    report: pd.DataFrame,
    baselines: pd.DataFrame,
    decision_state: str,
) -> dict[str, Any]:
    return {
        "schema_version": "portfolio_validation_summary.v1",
        "validation_status": "evaluated",
        "decision_state": decision_state,
        "component_pool_path": str(component_pool_path),
        "oos_observation_path": str(observation_path),
        "input_component_count": input_count,
        "available_component_count": len(available_ids),
        "unavailable_component_count": len(unavailable_ids),
        "hard_blocked_component_count": hard_blocked_count,
        "available_component_ids": available_ids,
        "unavailable_component_ids": unavailable_ids,
        "ensemble_ids": _ENSEMBLE_IDS,
        "period_count": int(period_returns["date"].nunique()) if not period_returns.empty else 0,
        "primary_ensemble_id": "equal_weight_all_components",
        "full_sample_weights_used": False,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "fabricated_returns": False,
        "random_weight_placebo_included": "random_weight_placebo" in set(report["ensemble_id"].astype(str)),
        "permuted_signal_placebo_included": "permuted_signal_placebo" in set(report["ensemble_id"].astype(str)),
        "benchmark_exposure_flag": _primary_benchmark_exposure(baselines),
        "non_claims": _non_claims(),
    }


def _unavailable_summary(
    reason: str,
    component_pool_path: Path,
    input_count: int,
    hard_blocked_count: int,
    unavailable_ids: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "portfolio_validation_summary.v1",
        "validation_status": "unavailable",
        "decision_state": "portfolio_component_pool_inconclusive",
        "unavailable_reason": reason,
        "component_pool_path": str(component_pool_path),
        "input_component_count": input_count,
        "available_component_count": 0,
        "unavailable_component_count": len(unavailable_ids or []),
        "hard_blocked_component_count": hard_blocked_count,
        "available_component_ids": [],
        "unavailable_component_ids": unavailable_ids or [],
        "ensemble_ids": _ENSEMBLE_IDS,
        "full_sample_weights_used": False,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "fabricated_returns": False,
        "non_claims": _non_claims(),
    }


def _render_markdown(report: pd.DataFrame, summary: dict[str, Any]) -> str:
    lines = [
        "# Portfolio-Level OOS Ensemble Validation",
        "",
        "This is diagnostic ensemble validation. It is not production approval, Q2 input, an OR optimizer, or security-level portfolio construction.",
        "",
        "OR does not create alpha; it can only preserve usable component value under constraints after portfolio-level evidence exists.",
        "",
        f"Decision state: `{summary['decision_state']}`",
        f"Input components: `{summary.get('input_component_count', 0)}`",
        f"Available components: `{summary.get('available_component_count', 0)}`",
        f"Unavailable components: `{summary.get('unavailable_component_count', 0)}`",
        "",
        "## Ensembles",
        "",
    ]
    if report.empty:
        lines.append("No ensemble returns were evaluated because required OOS inputs were unavailable.")
    else:
        for row in report.itertuples(index=False):
            lines.append(
                f"- `{row.ensemble_id}`: cost-adjusted annualized `{row.cost_adjusted_return}`, "
                f"Sharpe `{row.Sharpe}`, QQQ-relative `{row.QQQ_relative_return}`."
            )
    lines.extend(
        [
            "",
            "Hard-blocked components are excluded. Soft-labeled components are allowed when OOS observations exist.",
            "Unavailable component observations stay unavailable; returns are not fabricated.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_unavailable_outputs(*paths: Path) -> None:
    frames = [
        pd.DataFrame(columns=_metric_columns() + ["drawdown_vs_best_single_factor"]),
        pd.DataFrame(columns=_baseline_columns()),
        pd.DataFrame(columns=_period_columns()),
        pd.DataFrame(columns=_period_columns()),
    ]
    for frame, path in zip(frames, paths):
        frame.to_csv(path, index=False)


def _result(
    report_path: Path,
    baseline_path: Path,
    summary_path: Path,
    random_path: Path,
    permuted_path: Path,
    markdown_path: Path,
    validation_status: str,
    decision_state: str,
    input_count: int,
    available_count: int,
    unavailable_count: int,
    hard_blocked_count: int,
) -> PortfolioValidationResult:
    return PortfolioValidationResult(
        portfolio_ensemble_oos_report_path=str(report_path),
        ensemble_vs_baselines_path=str(baseline_path),
        summary_path=str(summary_path),
        random_weight_placebo_report_path=str(random_path),
        permuted_signal_placebo_report_path=str(permuted_path),
        report_path=str(markdown_path),
        validation_status=validation_status,
        decision_state=decision_state,
        input_component_count=input_count,
        available_component_count=available_count,
        unavailable_component_count=unavailable_count,
        hard_blocked_component_count=hard_blocked_count,
        production_approval=False,
        live_trading=False,
        direct_q2_entry=False,
        not_alpha_evidence=True,
    )


def _metric_row(report: pd.DataFrame, ensemble_id: str) -> pd.Series | None:
    rows = report[report["ensemble_id"].astype(str).eq(ensemble_id)]
    if rows.empty:
        return None
    return rows.iloc[0]


def _metric_value(report: pd.DataFrame, ensemble_id: str, column: str) -> float:
    row = _metric_row(report, ensemble_id)
    if row is None:
        return 0.0
    value = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
    return float(value) if pd.notna(value) else 0.0


def _primary_benchmark_exposure(baselines: pd.DataFrame) -> bool:
    if baselines.empty:
        return False
    row = baselines[baselines["ensemble_id"].astype(str).eq("equal_weight_all_components")]
    if row.empty:
        return False
    return _as_bool(row.iloc[0].get("benchmark_exposure_flag"))


def _comparison_role(ensemble_id: str) -> str:
    if ensemble_id == "QQQ_benchmark":
        return "benchmark"
    if "placebo" in ensemble_id:
        return "placebo"
    if ensemble_id == "best_single_factor":
        return "single_factor_baseline"
    return "diagnostic_ensemble"


def _compound_return(returns: pd.Series) -> float:
    return float((1.0 + returns.fillna(0.0)).prod() - 1.0)


def _annualized_return(returns: pd.Series) -> float:
    if len(returns) == 0:
        return 0.0
    total = _compound_return(returns)
    base = max(1.0 + total, 1e-9)
    return float(base ** (_PERIODS_PER_YEAR / len(returns)) - 1.0)


def _annualized_mean(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns:
        return 0.0
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(values.mean()) * _PERIODS_PER_YEAR if len(values) else 0.0


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
    drawdown = cumulative / running_max - 1.0
    return float(drawdown.min())


def _beta(returns: pd.Series, benchmark: pd.Series) -> float:
    if len(returns) < 2 or len(benchmark) < 2:
        return 0.0
    variance = float(np.var(benchmark, ddof=0))
    if variance <= 1e-12:
        return 0.0
    return float(np.cov(returns, benchmark, ddof=0)[0, 1] / variance)


def _round(value: object) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return 0.0
    return round(float(numeric), 10)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


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


def _period_columns() -> list[str]:
    return [
        "schema_version",
        "ensemble_id",
        "date",
        "component_count",
        "gross_return",
        "cost_adjusted_period_return",
        "qqq_return",
        "qqq_relative_period_return",
        "beta_adjusted_period_return",
        "sector_style_proxy_residual_period_return",
        "style_proxy_residual_period_return",
        "full_residual_period_return",
        "turnover",
        "weights_json",
        "full_sample_weights_used",
        "prior_history_only",
        "uses_unrestricted_optimizer",
        "not_alpha_evidence",
    ]


def _metric_columns() -> list[str]:
    return [
        "schema_version",
        "ensemble_id",
        "period_count",
        "total_return",
        "annualized_return",
        "annualized_excess_return_vs_QQQ",
        "Sharpe",
        "max_drawdown",
        "hit_rate",
        "turnover",
        "cost_adjusted_return",
        "cost_adjusted_total_return",
        "beta",
        "QQQ_relative_return",
        "beta_adjusted_return",
        "sector_style_proxy_residual",
        "style_proxy_residual",
        "full_residual_return",
        "portfolio_volatility",
        "full_sample_weights_used",
        "prior_history_only",
        "uses_unrestricted_optimizer",
        "not_alpha_evidence",
    ]


def _baseline_columns() -> list[str]:
    return [
        "schema_version",
        "ensemble_id",
        "comparison_role",
        "annualized_excess_return_vs_QQQ",
        "sharpe_delta_vs_best_single_factor",
        "beats_best_single_factor_on_sharpe",
        "beats_random_weight_placebo",
        "beats_permuted_signal_placebo",
        "benchmark_exposure_flag",
        "not_alpha_evidence",
    ]
