from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class PortfolioCostCapacityResult:
    component_cost_capacity_attribution_path: str
    capacity_frontier_path: str
    cost_stress_report_path: str
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


def run_portfolio_cost_capacity_attribution(
    component_pool_path: Path,
    oos_observation_path: Path,
    portfolio_validation_dir: Path,
    portfolio_contribution_dir: Path,
    output_dir: Path,
) -> PortfolioCostCapacityResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    attribution_path = output_dir / "component_cost_capacity_attribution.csv"
    frontier_path = output_dir / "capacity_frontier.csv"
    stress_path = output_dir / "cost_stress_report.csv"
    summary_path = output_dir / "portfolio_cost_capacity_summary.json"
    markdown_path = output_dir / "portfolio_cost_capacity_report.md"

    pool = _normalize_pool(_read_csv(component_pool_path))
    observations = _normalize_observations(_read_csv(oos_observation_path))
    r15_summary = _read_json(portfolio_validation_dir / "ensemble_validation_summary.json")
    r16_summary = _read_json(portfolio_contribution_dir / "portfolio_contribution_summary.json")
    eligible_pool = _eligible_pool(pool)

    if observations.empty:
        attribution = pd.DataFrame(columns=_attribution_columns())
        frontier = pd.DataFrame(columns=_frontier_columns())
        stress = pd.DataFrame(columns=_stress_columns())
        summary = _unavailable_summary(
            component_pool_path=component_pool_path,
            oos_observation_path=oos_observation_path,
            reason="missing_oos_observations",
        )
        _write_outputs(attribution_path, frontier_path, stress_path, attribution, frontier, stress)
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(_render_markdown(attribution, frontier, summary), encoding="utf-8")
        return _result(
            attribution_path,
            frontier_path,
            stress_path,
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
        attribution = pd.DataFrame(columns=_attribution_columns())
        frontier = pd.DataFrame(columns=_frontier_columns())
        stress = pd.DataFrame(columns=_stress_columns())
        summary = _unavailable_summary(
            component_pool_path=component_pool_path,
            oos_observation_path=oos_observation_path,
            reason="no_eligible_observed_components",
        )
        _write_outputs(attribution_path, frontier_path, stress_path, attribution, frontier, stress)
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(_render_markdown(attribution, frontier, summary), encoding="utf-8")
        return _result(
            attribution_path,
            frontier_path,
            stress_path,
            summary_path,
            markdown_path,
            validation_status="unavailable",
            decision_state=str(summary["decision_state"]),
            observed_count=0,
        )

    security_level_adv_available = _security_level_adv_available(observations)
    attribution = _component_attribution(observations, eligible_pool, observed_ids, security_level_adv_available)
    stress = _cost_stress_report(attribution)
    frontier = _capacity_frontier(attribution, security_level_adv_available)
    summary = _summary(
        component_pool_path=component_pool_path,
        oos_observation_path=oos_observation_path,
        r15_summary=r15_summary,
        r16_summary=r16_summary,
        attribution=attribution,
        security_level_adv_available=security_level_adv_available,
    )

    _write_outputs(attribution_path, frontier_path, stress_path, attribution, frontier, stress)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(attribution, frontier, summary), encoding="utf-8")
    return _result(
        attribution_path,
        frontier_path,
        stress_path,
        summary_path,
        markdown_path,
        validation_status="evaluated",
        decision_state=str(summary["decision_state"]),
        observed_count=int(summary["observed_component_count"]),
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
        return pd.DataFrame(columns=["factor_id", "date", "gross_spread", "net_spread", "cost_drag", "asset_count"])
    required = {"factor_id", "rebalance_date", "gross_spread", "qqq_return"}
    missing = sorted(required - set(observations.columns))
    if missing:
        raise ValueError(f"cost/capacity observations missing required columns: {missing}")
    normalized = observations.copy()
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    for column in [
        "gross_spread",
        "net_spread",
        "qqq_return",
        "cost_drag",
        "asset_count",
        "adv_usd",
        "dollar_volume",
        "spread_proxy",
        "turnover",
    ]:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    if "net_spread" not in normalized.columns:
        normalized["net_spread"] = normalized["gross_spread"] - 0.001
    if "cost_drag" not in normalized.columns:
        normalized["cost_drag"] = normalized["gross_spread"] - normalized["net_spread"]
    if "asset_count" not in normalized.columns:
        normalized["asset_count"] = 0
    return normalized.dropna(subset=["factor_id", "date", "gross_spread", "net_spread", "cost_drag"]).sort_values(["date", "factor_id"])


def _security_level_adv_available(observations: pd.DataFrame) -> bool:
    for column in ("adv_usd", "dollar_volume"):
        if column in observations.columns and pd.to_numeric(observations[column], errors="coerce").notna().any():
            return True
    return False


def _component_attribution(
    observations: pd.DataFrame,
    pool: pd.DataFrame,
    observed_ids: list[str],
    security_level_adv_available: bool,
) -> pd.DataFrame:
    metadata = _metadata(pool)
    rows: list[dict[str, Any]] = []
    for factor_id in observed_ids:
        frame = observations[observations["factor_id"].eq(factor_id)].copy()
        gross = float(frame["gross_spread"].mean()) if not frame.empty else 0.0
        net = float(frame["net_spread"].mean()) if not frame.empty else 0.0
        cost_drag = float(frame["cost_drag"].mean()) if not frame.empty else 0.0
        asset_count = float(frame["asset_count"].mean()) if "asset_count" in frame else 0.0
        ratio = cost_drag / max(abs(gross), 1e-8)
        rows.append(
            {
                "schema_version": "portfolio_cost_capacity_attribution.v1",
                "factor_id": factor_id,
                "family_id": metadata.get(factor_id, {}).get("family_id", factor_id),
                "cluster_id": metadata.get(factor_id, {}).get("cluster_id", factor_id),
                "component_role": metadata.get(factor_id, {}).get("component_role", "component"),
                "observed_period_count": int(len(frame)),
                "gross_spread_mean": _round(gross),
                "net_spread_mean": _round(net),
                "cost_drag_mean": _round(cost_drag),
                "annualized_gross_return_proxy": _round(gross * _PERIODS_PER_YEAR),
                "annualized_cost_drag_proxy": _round(cost_drag * _PERIODS_PER_YEAR),
                "annualized_net_return_proxy": _round(net * _PERIODS_PER_YEAR),
                "cost_drag_to_gross_ratio": _round(ratio),
                "asset_count_mean": _round(asset_count),
                "security_level_adv_available": security_level_adv_available,
                "capacity_model_scope": "security_level_adv" if security_level_adv_available else "component_proxy_only",
                "component_cost_capacity_decision": _component_decision(gross, net, ratio, asset_count),
                "or_optimizer_used": False,
                "security_level_portfolio_construction_used": False,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_attribution_columns())


def _cost_stress_report(attribution: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    scenarios = [("base_cost", 1.0), ("cost_2x", 2.0), ("cost_3x", 3.0)]
    for row in attribution.itertuples(index=False):
        gross = float(row.gross_spread_mean)
        cost = float(row.cost_drag_mean)
        for scenario, multiplier in scenarios:
            stressed_cost = cost * multiplier
            stressed_net = gross - stressed_cost
            rows.append(
                {
                    "schema_version": "portfolio_cost_stress.v1",
                    "factor_id": row.factor_id,
                    "cost_stress_scenario": scenario,
                    "cost_multiplier": multiplier,
                    "gross_spread_mean": _round(gross),
                    "stressed_cost_drag": _round(stressed_cost),
                    "post_stress_net_spread": _round(stressed_net),
                    "cost_stress_status": "cost_survives" if stressed_net > 0.0 else "cost_kills_component",
                    "not_alpha_evidence": True,
                }
            )
    return pd.DataFrame(rows, columns=_stress_columns())


def _capacity_frontier(attribution: pd.DataFrame, security_level_adv_available: bool) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    status = "security_level_adv_observed" if security_level_adv_available else "proxy_only_missing_security_level_adv"
    for row in attribution.itertuples(index=False):
        cost = float(row.cost_drag_mean)
        asset_count = max(float(row.asset_count_mean), 1.0)
        for aum in (10_000_000, 50_000_000, 100_000_000):
            for cap in (0.01, 0.05, 0.10):
                proxy_pressure = (aum / 10_000_000) * (0.05 / cap) / asset_count
                capacity_drag = cost * min(proxy_pressure, 10.0)
                rows.append(
                    {
                        "schema_version": "portfolio_capacity_frontier.v1",
                        "component_id": row.factor_id,
                        "aum_usd": aum,
                        "participation_cap": cap,
                        "asset_count_mean": _round(asset_count),
                        "capacity_drag_proxy": _round(capacity_drag),
                        "capacity_proxy_status": status,
                        "capacity_status": "capacity_watch" if capacity_drag > cost * 2.0 else "capacity_proxy_ok",
                        "security_level_portfolio_construction_used": False,
                        "not_alpha_evidence": True,
                    }
                )
    return pd.DataFrame(rows, columns=_frontier_columns())


def _component_decision(gross: float, net: float, cost_ratio: float, asset_count: float) -> str:
    if gross > 0.0 and (net <= 0.0 or cost_ratio > 1.0):
        return "cost_toxic_component"
    if asset_count and asset_count < 10:
        return "capacity_fragile_component"
    if cost_ratio >= 0.5:
        return "cost_capacity_watch_component"
    return "cost_capacity_ok_component"


def _summary(
    component_pool_path: Path,
    oos_observation_path: Path,
    r15_summary: dict[str, Any],
    r16_summary: dict[str, Any],
    attribution: pd.DataFrame,
    security_level_adv_available: bool,
) -> dict[str, Any]:
    decision_counts = (
        attribution["component_cost_capacity_decision"].astype(str).value_counts().to_dict()
        if "component_cost_capacity_decision" in attribution
        else {}
    )
    return {
        "schema_version": "portfolio_cost_capacity_summary.v1",
        "validation_status": "evaluated",
        "decision_state": "cost_capacity_attribution_diagnostic_only",
        "component_pool_path": str(component_pool_path),
        "oos_observation_path": str(oos_observation_path),
        "source_r15_decision_state": str(r15_summary.get("decision_state", "unknown")),
        "source_r16_decision_state": str(r16_summary.get("decision_state", "unknown")),
        "primary_construction": _PRIMARY_CONSTRUCTION,
        "observed_component_count": int(len(attribution)),
        "cost_toxic_component_count": int(decision_counts.get("cost_toxic_component", 0)),
        "capacity_fragile_component_count": int(decision_counts.get("capacity_fragile_component", 0)),
        "component_cost_capacity_decision_counts": decision_counts,
        "security_level_adv_available": security_level_adv_available,
        "capacity_model_scope": "security_level_adv" if security_level_adv_available else "component_proxy_only",
        "fabricated_capacity": False,
        "fabricated_returns": False,
        "full_sample_weights_used": False,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "direct_q2_entry": False,
        "non_claims": _non_claims(),
    }


def _unavailable_summary(component_pool_path: Path, oos_observation_path: Path, reason: str) -> dict[str, Any]:
    return {
        "schema_version": "portfolio_cost_capacity_summary.v1",
        "validation_status": "unavailable",
        "decision_state": "cost_capacity_attribution_unavailable",
        "unavailable_reason": reason,
        "component_pool_path": str(component_pool_path),
        "oos_observation_path": str(oos_observation_path),
        "observed_component_count": 0,
        "security_level_adv_available": False,
        "capacity_model_scope": "component_proxy_only",
        "fabricated_capacity": False,
        "fabricated_returns": False,
        "full_sample_weights_used": False,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "direct_q2_entry": False,
        "non_claims": _non_claims(),
    }


def _render_markdown(attribution: pd.DataFrame, frontier: pd.DataFrame, summary: dict[str, Any]) -> str:
    lines = [
        "# Portfolio Cost / Capacity Attribution",
        "",
        "This is diagnostic only. It is not alpha evidence and not a portfolio construction approval.",
        "",
        "OR remains locked. This report does not run an OR optimizer, security-level portfolio construction, Q2, live trading, broker/order workflows, or production approval.",
        "",
        "Security-level ADV unavailable means capacity rows are component-proxy diagnostics, not executable capacity proof.",
        "",
        f"Decision state: `{summary['decision_state']}`",
        f"Observed components: `{summary.get('observed_component_count', 0)}`",
        f"Capacity model scope: `{summary.get('capacity_model_scope', 'unknown')}`",
        "",
        "## Component Decisions",
        "",
    ]
    if attribution.empty:
        lines.append("No component cost/capacity attribution rows were available.")
    else:
        for row in attribution.itertuples(index=False):
            lines.append(
                f"- `{row.factor_id}`: cost drag `{row.cost_drag_mean}`, "
                f"asset count `{row.asset_count_mean}`, decision `{row.component_cost_capacity_decision}`."
            )
    lines.extend(["", "## Capacity Frontier", ""])
    if frontier.empty:
        lines.append("No capacity frontier rows were available.")
    else:
        lines.append(
            "Capacity frontier rows use component-level proxies unless `security_level_adv_available=true`; "
            "they must not be read as executable capacity estimates."
        )
    lines.append("")
    return "\n".join(lines)


def _metadata(pool: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if pool.empty:
        return {}
    return {str(row.factor_id): row._asdict() for row in pool.itertuples(index=False)}


def _write_outputs(
    attribution_path: Path,
    frontier_path: Path,
    stress_path: Path,
    attribution: pd.DataFrame,
    frontier: pd.DataFrame,
    stress: pd.DataFrame,
) -> None:
    attribution.to_csv(attribution_path, index=False)
    frontier.to_csv(frontier_path, index=False)
    stress.to_csv(stress_path, index=False)


def _result(
    attribution_path: Path,
    frontier_path: Path,
    stress_path: Path,
    summary_path: Path,
    markdown_path: Path,
    *,
    validation_status: str,
    decision_state: str,
    observed_count: int,
) -> PortfolioCostCapacityResult:
    return PortfolioCostCapacityResult(
        component_cost_capacity_attribution_path=str(attribution_path),
        capacity_frontier_path=str(frontier_path),
        cost_stress_report_path=str(stress_path),
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


def _non_claims() -> dict[str, bool]:
    return {
        "alpha_evidence": False,
        "q2_entry": False,
        "or_optimizer": False,
        "security_level_portfolio_construction": False,
        "paper_canary": False,
        "live_trading": False,
        "broker_order_workflow": False,
        "production_approval": False,
    }


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _round(value: float) -> float:
    return round(float(value), 10)


def _attribution_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "family_id",
        "cluster_id",
        "component_role",
        "observed_period_count",
        "gross_spread_mean",
        "net_spread_mean",
        "cost_drag_mean",
        "annualized_gross_return_proxy",
        "annualized_cost_drag_proxy",
        "annualized_net_return_proxy",
        "cost_drag_to_gross_ratio",
        "asset_count_mean",
        "security_level_adv_available",
        "capacity_model_scope",
        "component_cost_capacity_decision",
        "or_optimizer_used",
        "security_level_portfolio_construction_used",
        "not_alpha_evidence",
    ]


def _frontier_columns() -> list[str]:
    return [
        "schema_version",
        "component_id",
        "aum_usd",
        "participation_cap",
        "asset_count_mean",
        "capacity_drag_proxy",
        "capacity_proxy_status",
        "capacity_status",
        "security_level_portfolio_construction_used",
        "not_alpha_evidence",
    ]


def _stress_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "cost_stress_scenario",
        "cost_multiplier",
        "gross_spread_mean",
        "stressed_cost_drag",
        "post_stress_net_spread",
        "cost_stress_status",
        "not_alpha_evidence",
    ]
