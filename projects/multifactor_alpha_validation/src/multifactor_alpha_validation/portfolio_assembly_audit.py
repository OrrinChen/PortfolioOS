from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PortfolioAssemblyAuditResult:
    audit_path: str
    coverage_report_path: str
    direction_audit_path: str
    gross_to_net_waterfall_path: str
    role_aware_ensemble_report_path: str
    reclassification_report_path: str
    original_decision_state: str
    reclassified_decision_state: str
    component_pool_validation_state: str
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


_PRIMARY_ENSEMBLE = "equal_weight_all_components"
_PERIODS_PER_YEAR = 12
_COVERAGE_COMPLETE_THRESHOLD = 0.8


def run_portfolio_assembly_audit(
    component_pool_path: Path,
    component_candidate_path: Path,
    portfolio_validation_dir: Path,
    oos_observation_path: Path,
    output_dir: Path,
) -> PortfolioAssemblyAuditResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    component_pool = _read_csv(component_pool_path)
    component_candidates = _read_csv(component_candidate_path)
    observations = _read_csv(oos_observation_path)
    validation_report = _read_csv(portfolio_validation_dir / "portfolio_ensemble_oos_report.csv")
    validation_summary = _read_json(portfolio_validation_dir / "ensemble_validation_summary.json")

    pool = _normalize_pool(component_pool)
    coverage = _build_coverage_report(pool, validation_summary)
    direction = _build_direction_audit(observations, pool)
    waterfall = _build_gross_to_net_waterfall(validation_report)
    role_report = _build_role_aware_ensembles(observations, pool, component_candidates)
    audit = _build_audit(validation_summary, validation_report, coverage, waterfall)

    audit_path = output_dir / "portfolio_assembly_audit.json"
    coverage_path = output_dir / "observed_subset_coverage_report.csv"
    direction_path = output_dir / "component_direction_audit.csv"
    waterfall_path = output_dir / "gross_to_net_waterfall.csv"
    role_report_path = output_dir / "role_aware_ensemble_report.csv"
    reclassification_path = output_dir / "decision_state_reclassification.md"

    audit_path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    coverage.to_csv(coverage_path, index=False)
    direction.to_csv(direction_path, index=False)
    waterfall.to_csv(waterfall_path, index=False)
    role_report.to_csv(role_report_path, index=False)
    reclassification_path.write_text(_render_reclassification_report(audit, waterfall, role_report), encoding="utf-8")

    return PortfolioAssemblyAuditResult(
        audit_path=str(audit_path),
        coverage_report_path=str(coverage_path),
        direction_audit_path=str(direction_path),
        gross_to_net_waterfall_path=str(waterfall_path),
        role_aware_ensemble_report_path=str(role_report_path),
        reclassification_report_path=str(reclassification_path),
        original_decision_state=str(audit["original_decision_state"]),
        reclassified_decision_state=str(audit["reclassified_decision_state"]),
        component_pool_validation_state=str(audit["component_pool_validation_state"]),
        production_approval=False,
        live_trading=False,
        direct_q2_entry=False,
        not_alpha_evidence=True,
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
        return pd.DataFrame(columns=["factor_id", "family_id", "component_status", "component_role", "component_pool_eligible"])
    normalized = pool.copy()
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    if "family_id" not in normalized.columns:
        normalized["family_id"] = normalized["factor_id"].str.split("_").str[0]
    if "component_status" not in normalized.columns:
        normalized["component_status"] = "eligible_component"
    if "component_role" not in normalized.columns:
        normalized["component_role"] = "component"
    if "component_pool_eligible" not in normalized.columns:
        normalized["component_pool_eligible"] = normalized.get("portfolio_validation_allowed", True)
    if "filter_class" not in normalized.columns:
        normalized["filter_class"] = "soft_resurrected"
    return normalized


def _build_coverage_report(pool: pd.DataFrame, validation_summary: dict[str, Any]) -> pd.DataFrame:
    available = set(str(value) for value in validation_summary.get("available_component_ids", []))
    unavailable = set(str(value) for value in validation_summary.get("unavailable_component_ids", []))
    rows: list[dict[str, Any]] = []
    for row in pool.itertuples(index=False):
        factor_id = str(row.factor_id)
        eligible = _as_bool(getattr(row, "component_pool_eligible", True)) and str(getattr(row, "filter_class", "")) != "hard_excluded"
        if not eligible:
            observation_status = "hard_blocked"
        elif factor_id in available:
            observation_status = "observed"
        elif factor_id in unavailable:
            observation_status = "unavailable"
        else:
            observation_status = "not_in_r15_observation_set"
        rows.append(
            {
                "schema_version": "observed_subset_coverage_report.v1",
                "factor_id": factor_id,
                "family_id": str(getattr(row, "family_id", "")),
                "component_status": str(getattr(row, "component_status", "")),
                "component_role": str(getattr(row, "component_role", "")),
                "component_pool_eligible": eligible,
                "observation_status": observation_status,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _build_direction_audit(observations: pd.DataFrame, pool: pd.DataFrame) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame(columns=_direction_columns())
    working = observations.copy()
    working["factor_id"] = working["factor_id"].astype(str)
    for column in ["gross_spread", "net_spread", "rank_ic", "cost_drag"]:
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")
    role_by_factor = pool.set_index("factor_id")["component_role"].to_dict() if not pool.empty else {}
    status_by_factor = pool.set_index("factor_id")["component_status"].to_dict() if not pool.empty else {}
    rows: list[dict[str, Any]] = []
    for factor_id, group in working.groupby("factor_id", sort=True):
        gross_mean = float(group["gross_spread"].mean()) if "gross_spread" in group.columns else 0.0
        net_mean = float(group["net_spread"].mean()) if "net_spread" in group.columns else gross_mean
        cost_drag = float(group["cost_drag"].mean()) if "cost_drag" in group.columns else gross_mean - net_mean
        if gross_mean > 0.0 and net_mean <= 0.0:
            direction_check = "cost_killed_after_positive_gross"
        elif gross_mean > 0.0:
            direction_check = "direction_positive_gross"
        else:
            direction_check = "direction_negative_gross"
        rows.append(
            {
                "schema_version": "component_direction_audit.v1",
                "factor_id": factor_id,
                "expected_long_leg": _expected_long_leg(factor_id),
                "component_role": str(role_by_factor.get(factor_id, "")),
                "component_status": str(status_by_factor.get(factor_id, "")),
                "gross_spread_mean": _round(gross_mean),
                "net_spread_mean": _round(net_mean),
                "rank_ic_mean": _round(group["rank_ic"].mean()) if "rank_ic" in group.columns else 0.0,
                "positive_gross_rate": _round(group["gross_spread"].gt(0.0).mean()) if "gross_spread" in group.columns else 0.0,
                "positive_net_rate": _round(group["net_spread"].gt(0.0).mean()) if "net_spread" in group.columns else 0.0,
                "cost_drag_mean": _round(cost_drag),
                "direction_check": direction_check,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_direction_columns())


def _build_gross_to_net_waterfall(validation_report: pd.DataFrame) -> pd.DataFrame:
    primary = _primary_row(validation_report)
    gross = _numeric(primary.get("annualized_return")) if primary is not None else 0.0
    net = _numeric(primary.get("cost_adjusted_return")) if primary is not None else 0.0
    cost_drag = net - gross
    return pd.DataFrame(
        [
            {
                "schema_version": "gross_to_net_waterfall.v1",
                "layer": "gross_return",
                "annualized_return": _round(gross),
                "interpretation": "observed_subset_gross_return",
                "not_alpha_evidence": True,
            },
            {
                "schema_version": "gross_to_net_waterfall.v1",
                "layer": "estimated_cost_drag",
                "annualized_return": _round(cost_drag),
                "interpretation": "net_minus_gross_cost_drag_proxy",
                "not_alpha_evidence": True,
            },
            {
                "schema_version": "gross_to_net_waterfall.v1",
                "layer": "cost_adjusted_return",
                "annualized_return": _round(net),
                "interpretation": "observed_subset_cost_adjusted_return",
                "not_alpha_evidence": True,
            },
        ]
    )


def _build_role_aware_ensembles(
    observations: pd.DataFrame,
    pool: pd.DataFrame,
    component_candidates: pd.DataFrame,
) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame(columns=_role_columns())
    working = observations.copy()
    working["factor_id"] = working["factor_id"].astype(str)
    working["date"] = pd.to_datetime(working["rebalance_date"], errors="coerce")
    for column in ["gross_spread", "net_spread", "qqq_return", "beta_adjusted_spread", "cost_drag"]:
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")
    role_pool = _merge_roles(pool, component_candidates)
    observed_ids = sorted(set(working["factor_id"]))
    ensemble_weights = {
        "return_driver_only": _role_weights(observed_ids, role_pool, include="return_driver"),
        "hedge_only": _role_weights(observed_ids, role_pool, include="hedge"),
        "benchmark_premia_only": _role_weights(observed_ids, role_pool, include="benchmark"),
        "return_driver_plus_hedge_80_20": _return_driver_hedge_weights(observed_ids, role_pool),
        "momentum_plus_low_vol": _named_pair_weights(observed_ids, ["momentum_12_1", "low_vol_60d"]),
        "momentum_plus_reversal": _named_pair_weights(observed_ids, ["momentum_12_1", "reversal_5_1"]),
        "risk_balanced_by_component_vol": _inverse_vol_weights(observed_ids, working),
        "turnover_capped_equal_weight": _turnover_capped_weights(observed_ids),
    }
    rows: list[dict[str, Any]] = []
    for ensemble_id, weights in ensemble_weights.items():
        if not weights:
            rows.append(_empty_role_row(ensemble_id))
            continue
        returns = _weighted_series(working, weights, "gross_spread")
        net = _weighted_series(working, weights, "net_spread")
        qqq = _date_series(working, "qqq_return")
        beta_adjusted = _weighted_series(working, weights, "beta_adjusted_spread")
        rows.append(
            {
                "schema_version": "role_aware_ensemble_report.v1",
                "ensemble_id": ensemble_id,
                "component_count": len(weights),
                "gross_annualized_return": _round(_annualized_return(returns)),
                "cost_adjusted_annualized_return": _round(_annualized_return(net)),
                "cost_drag_annualized": _round(_annualized_return(net) - _annualized_return(returns)),
                "qqq_relative_return": _round(float((net - qqq).mean()) * _PERIODS_PER_YEAR) if len(net) else 0.0,
                "beta_adjusted_return": _round(float(beta_adjusted.mean()) * _PERIODS_PER_YEAR) if len(beta_adjusted) else 0.0,
                "max_drawdown": _round(_max_drawdown(net)),
                "hit_rate": _round(float(net.gt(0.0).mean()) if len(net) else 0.0),
                "weights_json": json.dumps(weights, sort_keys=True),
                "or_optimizer_used": False,
                "security_level_portfolio_construction_used": False,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_role_columns())


def _build_audit(
    validation_summary: dict[str, Any],
    validation_report: pd.DataFrame,
    coverage: pd.DataFrame,
    waterfall: pd.DataFrame,
) -> dict[str, Any]:
    original = str(validation_summary.get("decision_state", "portfolio_component_pool_inconclusive"))
    observed = int(validation_summary.get("available_component_count", 0))
    unavailable = int(validation_summary.get("unavailable_component_count", 0))
    hard_blocked = int(validation_summary.get("hard_blocked_component_count", 0))
    eligible_count = int(coverage["component_pool_eligible"].map(_as_bool).sum()) if not coverage.empty else observed + unavailable
    coverage_ratio = observed / eligible_count if eligible_count else 0.0
    primary = _primary_row(validation_report)
    gross = _numeric(primary.get("annualized_return")) if primary is not None else 0.0
    net = _numeric(primary.get("cost_adjusted_return")) if primary is not None else 0.0
    qqq_relative = _numeric(primary.get("QQQ_relative_return")) if primary is not None else 0.0
    beta = _numeric(primary.get("beta")) if primary is not None else 0.0
    gross_failure = gross <= 0.0
    cost_killed = gross > 0.0 and net <= 0.0
    benchmark_conflict = qqq_relative < 0.0 and beta < 0.0
    component_pool_decision_allowed = coverage_ratio >= _COVERAGE_COMPLETE_THRESHOLD
    decision_prefix = "component_pool" if component_pool_decision_allowed else "observed_subset"
    if gross_failure:
        reclassified = f"{decision_prefix}_fails_gross"
    elif cost_killed:
        reclassified = f"{decision_prefix}_turnover_killed"
    elif original == "portfolio_component_pool_fails_cost":
        reclassified = f"{decision_prefix}_fails_cost"
    elif benchmark_conflict:
        reclassified = f"{decision_prefix}_benchmark_exposure_conflict"
    elif component_pool_decision_allowed:
        reclassified = original
    else:
        reclassified = original.replace("portfolio_component_pool", "observed_subset")
    addenda: list[str] = []
    component_pool_state = "component_pool_observation_sufficient"
    if coverage_ratio < _COVERAGE_COMPLETE_THRESHOLD:
        component_pool_state = "component_pool_unavailable_coverage_gap"
        addenda.append(component_pool_state)
    if gross_failure:
        addenda.append(f"{decision_prefix}_fails_gross")
    if cost_killed:
        addenda.append(f"{decision_prefix}_turnover_killed")
    if benchmark_conflict:
        addenda.append(f"{decision_prefix}_benchmark_exposure_conflict")
    return {
        "schema_version": "portfolio_assembly_audit.v1",
        "original_decision_state": original,
        "reclassified_decision_state": reclassified,
        "component_pool_validation_state": component_pool_state,
        "decision_state_addenda": sorted(set(addenda)),
        "observed_component_count": observed,
        "unavailable_component_count": unavailable,
        "hard_blocked_component_count": hard_blocked,
        "eligible_component_count": eligible_count,
        "coverage_ratio": _round(coverage_ratio),
        "primary_gross_annualized_return": _round(gross),
        "primary_cost_adjusted_annualized_return": _round(net),
        "primary_qqq_relative_return": _round(qqq_relative),
        "primary_beta": _round(beta),
        "gross_failure": gross_failure,
        "cost_killed_after_positive_gross": cost_killed,
        "benchmark_exposure_conflict": benchmark_conflict,
        "component_pool_decision_allowed": component_pool_decision_allowed,
        "full_pool_failure_claim_allowed": component_pool_decision_allowed and unavailable == 0,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "fabricated_returns": False,
        "non_claims": _non_claims(),
    }


def _render_reclassification_report(audit: dict[str, Any], waterfall: pd.DataFrame, role_report: pd.DataFrame) -> str:
    lines = [
        "# Portfolio Assembly Audit",
        "",
        f"Original R15 decision: `{audit['original_decision_state']}`",
        f"Reclassified decision: `{audit['reclassified_decision_state']}`",
        f"Component pool validation state: `{audit['component_pool_validation_state']}`",
        "",
        _render_scope_sentence(audit),
        "",
        "OR remains blocked. This audit does not run an OR optimizer, security-level construction, Q2, live trading, broker/order workflows, or production approval.",
        "",
        "## Gross To Net",
        "",
    ]
    for row in waterfall.itertuples(index=False):
        lines.append(f"- `{row.layer}`: `{row.annualized_return}`")
    lines.extend(["", "## Role-Aware Diagnostics", ""])
    if role_report.empty:
        lines.append("No role-aware ensembles were available.")
    else:
        for row in role_report.itertuples(index=False):
            lines.append(
                f"- `{row.ensemble_id}`: gross `{row.gross_annualized_return}`, "
                f"net `{row.cost_adjusted_annualized_return}`, QQQ-relative `{row.qqq_relative_return}`."
            )
    lines.extend(["", "This is not alpha evidence.", ""])
    return "\n".join(lines)


def _render_scope_sentence(audit: dict[str, Any]) -> str:
    if audit.get("component_pool_decision_allowed"):
        return (
            "R15.5 has sufficient observed component coverage for a current observed component pool "
            "diagnostic under the configured coverage threshold. Remaining unavailable components are "
            "still explicit unavailable and are not fabricated."
        )
    return (
        "R15 is an observed subset diagnostic, not full component pool failure. "
        "The current conclusion applies to the available observed subset only."
    )


def _merge_roles(pool: pd.DataFrame, component_candidates: pd.DataFrame) -> pd.DataFrame:
    if pool.empty:
        return pd.DataFrame(columns=["factor_id", "component_status", "component_role"])
    merged = pool[["factor_id", "component_status", "component_role"]].copy()
    if not component_candidates.empty and {"factor_id", "component_status", "component_role"} <= set(component_candidates.columns):
        candidates = component_candidates[["factor_id", "component_status", "component_role"]].copy()
        candidates["factor_id"] = candidates["factor_id"].astype(str)
        merged = merged.set_index("factor_id")
        candidates = candidates.set_index("factor_id")
        for factor_id in candidates.index:
            if factor_id in merged.index:
                merged.loc[factor_id, "component_status"] = candidates.loc[factor_id, "component_status"]
                merged.loc[factor_id, "component_role"] = candidates.loc[factor_id, "component_role"]
        merged = merged.reset_index()
    return merged


def _role_weights(observed_ids: list[str], role_pool: pd.DataFrame, *, include: str) -> dict[str, float]:
    if role_pool.empty:
        return {}
    role_pool = role_pool[role_pool["factor_id"].isin(observed_ids)].copy()
    status = role_pool["component_status"].astype(str).str.lower()
    role = role_pool["component_role"].astype(str).str.lower()
    if include == "return_driver":
        selected = role_pool[role.str.contains("return_driver", na=False)]
    elif include == "hedge":
        selected = role_pool[role.str.contains("hedge|diversifier", na=False) | status.str.contains("hedge", na=False)]
    elif include == "benchmark":
        selected = role_pool[status.str.contains("benchmark|premia", na=False) | role.str.contains("premia", na=False)]
    else:
        selected = pd.DataFrame()
    return _equal_weights(sorted(selected["factor_id"].astype(str))) if not selected.empty else {}


def _return_driver_hedge_weights(observed_ids: list[str], role_pool: pd.DataFrame) -> dict[str, float]:
    drivers = _role_weights(observed_ids, role_pool, include="return_driver")
    hedges = _role_weights(observed_ids, role_pool, include="hedge")
    weights: dict[str, float] = {}
    if drivers:
        for factor_id, weight in drivers.items():
            weights[factor_id] = weights.get(factor_id, 0.0) + 0.8 * weight
    if hedges:
        for factor_id, weight in hedges.items():
            weights[factor_id] = weights.get(factor_id, 0.0) + 0.2 * weight
    return weights or _equal_weights(observed_ids)


def _named_pair_weights(observed_ids: list[str], names: list[str]) -> dict[str, float]:
    selected = [factor_id for factor_id in names if factor_id in set(observed_ids)]
    return _equal_weights(selected)


def _inverse_vol_weights(observed_ids: list[str], observations: pd.DataFrame) -> dict[str, float]:
    scores: dict[str, float] = {}
    for factor_id in observed_ids:
        series = pd.to_numeric(observations.loc[observations["factor_id"].eq(factor_id), "gross_spread"], errors="coerce").dropna()
        scores[factor_id] = 1.0 / max(float(series.std(ddof=0)), 1e-6) if len(series) >= 2 else 1.0
    return _normalize_scores(scores)


def _turnover_capped_weights(observed_ids: list[str]) -> dict[str, float]:
    if not observed_ids:
        return {}
    weights = _equal_weights(observed_ids)
    if "reversal_5_1" in weights:
        weights["reversal_5_1"] = min(weights["reversal_5_1"], 0.2)
        remainder_ids = [factor_id for factor_id in observed_ids if factor_id != "reversal_5_1"]
        remainder = 1.0 - weights["reversal_5_1"]
        for factor_id in remainder_ids:
            weights[factor_id] = remainder / len(remainder_ids) if remainder_ids else 0.0
    return weights


def _weighted_series(observations: pd.DataFrame, weights: dict[str, float], column: str) -> pd.Series:
    if column not in observations.columns or not weights:
        return pd.Series(dtype=float)
    dates = sorted(pd.Timestamp(value) for value in observations["date"].dropna().unique())
    values: list[float] = []
    for date in dates:
        current = observations[observations["date"].eq(date)].set_index("factor_id")
        total = 0.0
        active = 0.0
        for factor_id, weight in weights.items():
            if factor_id in current.index and pd.notna(current.loc[factor_id, column]):
                total += float(weight) * float(current.loc[factor_id, column])
                active += float(weight)
        if active > 0.0:
            values.append(total / active)
    return pd.Series(values, dtype=float)


def _date_series(observations: pd.DataFrame, column: str) -> pd.Series:
    if observations.empty or column not in observations.columns:
        return pd.Series(dtype=float)
    values = observations.groupby("date")[column].first().sort_index()
    return pd.to_numeric(values, errors="coerce").dropna().reset_index(drop=True)


def _empty_role_row(ensemble_id: str) -> dict[str, Any]:
    return {
        "schema_version": "role_aware_ensemble_report.v1",
        "ensemble_id": ensemble_id,
        "component_count": 0,
        "gross_annualized_return": 0.0,
        "cost_adjusted_annualized_return": 0.0,
        "cost_drag_annualized": 0.0,
        "qqq_relative_return": 0.0,
        "beta_adjusted_return": 0.0,
        "max_drawdown": 0.0,
        "hit_rate": 0.0,
        "weights_json": "{}",
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "not_alpha_evidence": True,
    }


def _primary_row(validation_report: pd.DataFrame) -> pd.Series | None:
    if validation_report.empty or "ensemble_id" not in validation_report.columns:
        return None
    primary = validation_report[validation_report["ensemble_id"].astype(str).eq(_PRIMARY_ENSEMBLE)]
    if primary.empty:
        return None
    return primary.iloc[0]


def _expected_long_leg(factor_id: str) -> str:
    mapping = {
        "momentum_12_1": "high_medium_term_momentum_assets",
        "reversal_5_1": "short_term_loser_reversal_assets",
        "low_vol_60d": "low_volatility_assets",
        "liquidity_turnover": "liquid_low_turnover_assets",
        "value_bm": "high_book_to_market_assets",
        "profitability_quality": "high_profitability_assets",
        "investment_asset_growth": "low_asset_growth_assets",
        "accruals": "low_accruals_assets",
        "sue_event_reference": "positive_earnings_surprise_events",
    }
    return mapping.get(factor_id, "configured_high_score_assets")


def _equal_weights(factor_ids: list[str]) -> dict[str, float]:
    if not factor_ids:
        return {}
    weight = 1.0 / len(factor_ids)
    return {factor_id: weight for factor_id in factor_ids}


def _normalize_scores(scores: dict[str, float]) -> dict[str, float]:
    total = sum(max(value, 0.0) for value in scores.values())
    if total <= 0.0:
        return _equal_weights(sorted(scores))
    return {factor_id: max(value, 0.0) / total for factor_id, value in scores.items()}


def _annualized_return(returns: pd.Series) -> float:
    if len(returns) == 0:
        return 0.0
    total = float((1.0 + returns.fillna(0.0)).prod() - 1.0)
    base = max(1.0 + total, 1e-9)
    return float(base ** (_PERIODS_PER_YEAR / len(returns)) - 1.0)


def _max_drawdown(returns: pd.Series) -> float:
    if len(returns) == 0:
        return 0.0
    cumulative = (1.0 + returns.fillna(0.0)).cumprod()
    running_max = cumulative.cummax()
    return float((cumulative / running_max - 1.0).min())


def _numeric(value: object, default: float = 0.0) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return default
    return float(numeric)


def _round(value: object) -> float:
    return round(_numeric(value), 10)


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


def _direction_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "expected_long_leg",
        "component_role",
        "component_status",
        "gross_spread_mean",
        "net_spread_mean",
        "rank_ic_mean",
        "positive_gross_rate",
        "positive_net_rate",
        "cost_drag_mean",
        "direction_check",
        "not_alpha_evidence",
    ]


def _role_columns() -> list[str]:
    return [
        "schema_version",
        "ensemble_id",
        "component_count",
        "gross_annualized_return",
        "cost_adjusted_annualized_return",
        "cost_drag_annualized",
        "qqq_relative_return",
        "beta_adjusted_return",
        "max_drawdown",
        "hit_rate",
        "weights_json",
        "or_optimizer_used",
        "security_level_portfolio_construction_used",
        "not_alpha_evidence",
    ]
