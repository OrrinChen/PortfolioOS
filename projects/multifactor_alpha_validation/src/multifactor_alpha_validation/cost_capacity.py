from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.allocator import AllocatorResult
from multifactor_alpha_validation.benchmark_attribution import build_benchmark_attribution
from multifactor_alpha_validation.schema import FactorSpec


@dataclass(frozen=True)
class SurvivalResult:
    survival_funnel: pd.DataFrame
    cost_stress_matrix: pd.DataFrame
    capacity_frontier: pd.DataFrame
    benchmark_attribution: pd.DataFrame
    failure_attribution: dict[str, dict[str, object]]
    survival_summary_markdown: str


def build_survival_result(
    specs: list[FactorSpec],
    factor_evidence_table: pd.DataFrame,
    allocator: AllocatorResult,
    posterior_mu: pd.DataFrame,
) -> SurvivalResult:
    cost = _cost_stress_matrix(allocator.factor_weights)
    capacity = _capacity_frontier(allocator.factor_weights)
    attribution = build_benchmark_attribution(factor_evidence_table, allocator.factor_weights)
    failure = _failure_attribution(specs, allocator.factor_weights, cost)
    funnel = _survival_funnel(specs, factor_evidence_table, allocator.factor_weights, cost, capacity)
    summary = _render_summary(funnel, cost, capacity, posterior_mu)
    return SurvivalResult(
        survival_funnel=funnel,
        cost_stress_matrix=cost,
        capacity_frontier=capacity,
        benchmark_attribution=attribution,
        failure_attribution=failure,
        survival_summary_markdown=summary,
    )


def write_survival_outputs(result: SurvivalResult, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    result.survival_funnel.to_csv(output_dir / "survival_funnel.csv", index=False)
    result.cost_stress_matrix.to_csv(output_dir / "cost_stress_matrix.csv", index=False)
    result.capacity_frontier.to_csv(output_dir / "capacity_frontier.csv", index=False)
    result.benchmark_attribution.to_csv(output_dir / "benchmark_attribution.csv", index=False)
    (output_dir / "failure_attribution.json").write_text(
        json.dumps(result.failure_attribution, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "survival_summary.md").write_text(result.survival_summary_markdown, encoding="utf-8")
    return [
        "survival_funnel.csv",
        "cost_stress_matrix.csv",
        "capacity_frontier.csv",
        "benchmark_attribution.csv",
        "failure_attribution.json",
        "survival_summary.md",
    ]


def _cost_stress_matrix(factor_weights: pd.DataFrame) -> pd.DataFrame:
    scenarios = [
        ("fixed_low", 0.0004),
        ("spread_base", 0.0020),
        ("impact_high", 0.0200),
    ]
    rows: list[dict[str, object]] = []
    for factor in factor_weights.itertuples(index=False):
        gross = float(factor.weight) * float(factor.posterior_expected_return)
        for scenario, cost_rate in scenarios:
            turnover_cost = float(factor.expected_turnover_contribution) * 0.003
            cost = cost_rate + turnover_cost
            net = gross - cost
            rows.append(
                {
                    "schema_version": "cost_survival.v1",
                    "factor_id": factor.factor_id,
                    "cost_scenario": scenario,
                    "gross_alpha": round(gross, 8),
                    "fixed_cost": round(cost_rate, 8),
                    "turnover_cost": round(turnover_cost, 8),
                    "net_alpha_after_cost": round(net, 8),
                    "cost_survival_status": "cost_survived" if net >= 0 else "cost_killed",
                }
            )
    return pd.DataFrame(rows)


def _capacity_frontier(factor_weights: pd.DataFrame) -> pd.DataFrame:
    active = factor_weights[factor_weights["weight"] > 0]
    gross = float((active["weight"] * active["posterior_expected_return"]).sum()) if not active.empty else 0.0
    bottleneck = ",".join(active["factor_id"].tolist()) if not active.empty else "no_active_factor"
    rows: list[dict[str, object]] = []
    for aum in (10_000_000, 50_000_000, 100_000_000, 250_000_000):
        capacity_drag = aum / 250_000_000 * 0.004
        net = gross - capacity_drag
        rows.append(
            {
                "schema_version": "capacity_frontier.v1",
                "aum_usd": aum,
                "adv_participation": round(min(0.02 + aum / 500_000_000, 0.50), 6),
                "liquidity_bucket": "mvp_liquid",
                "capacity_bottleneck_names": bottleneck,
                "net_alpha_after_capacity": round(net, 8),
                "capacity_status": "capacity_survived" if net >= 0 else "capacity_limited",
            }
        )
    return pd.DataFrame(rows)


def _failure_attribution(
    specs: list[FactorSpec],
    factor_weights: pd.DataFrame,
    cost_stress: pd.DataFrame,
) -> dict[str, dict[str, object]]:
    weight_map = factor_weights.set_index("factor_id").to_dict("index")
    failure: dict[str, dict[str, object]] = {}
    base_cost = cost_stress[cost_stress["cost_scenario"] == "spread_base"].set_index("factor_id")
    for spec in specs:
        if spec.status == "disabled":
            failure[spec.factor_id] = {
                "stop_layer": "pit_unavailable",
                "performance_status": "unavailable",
                "reason": spec.disabled_reason,
            }
            continue
        row = weight_map.get(spec.factor_id)
        if not row:
            failure[spec.factor_id] = {
                "stop_layer": "unavailable",
                "performance_status": "unavailable",
                "reason": "no allocator row",
            }
            continue
        if float(row["weight"]) == 0:
            failure[spec.factor_id] = {
                "stop_layer": "allocator_zero",
                "performance_status": "observed",
                "reason": row["zero_weight_reason"],
            }
            continue
        cost_status = str(base_cost.loc[spec.factor_id, "cost_survival_status"])
        failure[spec.factor_id] = {
            "stop_layer": "cost_capacity_survival" if cost_status == "cost_survived" else "cost",
            "performance_status": "observed",
            "reason": cost_status,
        }
    return failure


def _survival_funnel(
    specs: list[FactorSpec],
    evidence: pd.DataFrame,
    factor_weights: pd.DataFrame,
    cost: pd.DataFrame,
    capacity: pd.DataFrame,
) -> pd.DataFrame:
    active_count = int((factor_weights["weight"] > 0).sum())
    base_cost = cost[cost["cost_scenario"] == "spread_base"]
    return pd.DataFrame(
        [
            {"layer": "spec_pass", "factor_count": len(specs), "status": "observed"},
            {"layer": "pit_pass", "factor_count": sum(spec.status != "disabled" for spec in specs), "status": "observed"},
            {"layer": "q1_pass", "factor_count": int((evidence["q1_decision"] == "q1_pass").sum()), "status": "observed"},
            {"layer": "allocator_positive_weight", "factor_count": active_count, "status": "observed"},
            {"layer": "cost_survived", "factor_count": int((base_cost["cost_survival_status"] == "cost_survived").sum()), "status": "observed"},
            {"layer": "capacity_survived", "factor_count": int((capacity["capacity_status"] == "capacity_survived").sum()), "status": "observed"},
        ]
    )


def _render_summary(
    funnel: pd.DataFrame,
    cost: pd.DataFrame,
    capacity: pd.DataFrame,
    posterior_mu: pd.DataFrame,
) -> str:
    return "\n".join(
        [
            "# Factor Survival Summary",
            "",
            "This report separates raw, benchmark-relative, beta-adjusted, cost-adjusted, and capacity-adjusted readouts.",
            "",
            f"- layers: {len(funnel)}",
            f"- cost_killed_rows: {int((cost['cost_survival_status'] == 'cost_killed').sum())}",
            f"- capacity_limited_rows: {int((capacity['capacity_status'] == 'capacity_limited').sum())}",
            f"- posterior_factor_count: {len(posterior_mu)}",
            "- unavailable_policy: unavailable rows remain unavailable rather than receiving synthetic performance",
            "- non_claims: no production approval, no live trading, no security-level output",
            "",
        ]
    )
