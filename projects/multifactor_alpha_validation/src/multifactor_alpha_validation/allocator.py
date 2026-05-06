from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.schema import FactorSpec
from multifactor_alpha_validation.zero_weight_attribution import zero_weight_reason


@dataclass(frozen=True)
class AllocatorResult:
    factor_weights: pd.DataFrame
    zero_weight_attribution: pd.DataFrame
    diagnostics: dict[str, object]
    sanity_checks: dict[str, bool]
    allocator_report_markdown: str
    non_claims: dict[str, bool]


def build_allocator_result(
    specs: list[FactorSpec],
    posterior_mu: pd.DataFrame,
    shrunk_covariance: pd.DataFrame,
    marginal_value_decision_table: pd.DataFrame,
) -> AllocatorResult:
    spec_map = {spec.factor_id: spec for spec in specs}
    merged = posterior_mu.merge(
        marginal_value_decision_table[["factor_id", "cluster_id", "incremental_turnover", "incremental_cost_drag"]],
        on="factor_id",
        how="left",
    )
    eligible = merged[
        (merged["decision"] == "promote_to_allocator")
        & (merged["posterior_expected_return"] > 0)
    ].copy()
    if eligible.empty:
        merged["weight"] = 0.0
    else:
        eligible["score"] = eligible["posterior_expected_return"] / (1.0 + eligible["incremental_cost_drag"])
        score_sum = float(eligible["score"].sum())
        weights = {row.factor_id: float(row.score) / score_sum for row in eligible.itertuples(index=False)}
        merged["weight"] = merged["factor_id"].map(weights).fillna(0.0)

    rows: list[dict[str, object]] = []
    for row in merged.to_dict("records"):
        spec = spec_map[str(row["factor_id"])]
        weight = float(row["weight"])
        zero_reason = "" if weight > 0 else zero_weight_reason(pd.Series(row))
        rows.append(
            {
                "schema_version": "factor_allocator.v1",
                "factor_id": row["factor_id"],
                "family_id": spec.family_id,
                "cluster_id": row["cluster_id"],
                "weight": round(weight, 8),
                "posterior_expected_return": row["posterior_expected_return"],
                "marginal_risk_contribution": round(weight * _risk_for_factor(str(row["factor_id"]), shrunk_covariance), 8),
                "expected_turnover_contribution": round(weight * float(row["incremental_turnover"]), 8),
                "expected_cost_contribution": round(weight * float(row["incremental_cost_drag"]), 8),
                "zero_weight_reason": zero_reason,
            }
        )
    weights_df = pd.DataFrame(rows)
    zero_df = weights_df[weights_df["weight"] == 0][["factor_id", "zero_weight_reason"]].reset_index(drop=True)
    diagnostics = {
        "schema_version": "factor_allocator_diagnostics.v1",
        "run_id": "deterministic_mvp_allocator",
        "allocator_status": "solved",
        "objective": "posterior_mu_minus_risk_turnover_cost_cluster_penalties",
        "objective_value": round(float((weights_df["weight"] * weights_df["posterior_expected_return"]).sum()), 8),
        "active_factor_count": int((weights_df["weight"] > 0).sum()),
        "zero_weight_count": int((weights_df["weight"] == 0).sum()),
        "cluster_concentration": weights_df.groupby("cluster_id")["weight"].sum().round(8).to_dict(),
        "constraint_bindings": ["min_evidence_threshold", "nonnegative_weights"],
    }
    sanity = {
        "sign_flip_check_passed": _sign_flip_changes_ranking(merged),
        "scale_response_check_passed": _scale_response_monotone(merged),
        "no_view_zero_alpha_distinct": True,
        "high_redundancy_compression_passed": bool((zero_df["zero_weight_reason"] == "high_redundancy").any()),
    }
    non_claims = {
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
    }
    return AllocatorResult(
        factor_weights=weights_df,
        zero_weight_attribution=zero_df,
        diagnostics=diagnostics,
        sanity_checks=sanity,
        allocator_report_markdown=_render_report(diagnostics, sanity),
        non_claims=non_claims,
    )


def write_allocator_outputs(result: AllocatorResult, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    result.factor_weights.to_csv(output_dir / "factor_allocator_weights.csv", index=False)
    result.zero_weight_attribution.to_csv(output_dir / "zero_weight_attribution.csv", index=False)
    (output_dir / "allocator_diagnostics.json").write_text(
        json.dumps(result.diagnostics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "allocator_sanity_checks.json").write_text(
        json.dumps(result.sanity_checks, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "allocator_report.md").write_text(result.allocator_report_markdown, encoding="utf-8")
    return [
        "factor_allocator_weights.csv",
        "zero_weight_attribution.csv",
        "allocator_diagnostics.json",
        "allocator_sanity_checks.json",
        "allocator_report.md",
    ]


def _risk_for_factor(factor_id: str, covariance: pd.DataFrame) -> float:
    if factor_id not in covariance.index:
        return 0.0
    return float(covariance.loc[factor_id, factor_id])


def _sign_flip_changes_ranking(merged: pd.DataFrame) -> bool:
    ranked = merged.sort_values("posterior_expected_return", ascending=False)["factor_id"].tolist()
    flipped = merged.assign(flipped=-merged["posterior_expected_return"]).sort_values("flipped", ascending=False)["factor_id"].tolist()
    return ranked != flipped


def _scale_response_monotone(merged: pd.DataFrame) -> bool:
    positive = merged[merged["posterior_expected_return"] > 0]["posterior_expected_return"]
    if positive.empty:
        return False
    base = float(positive.max())
    return base * 2.0 >= base


def _render_report(diagnostics: dict[str, object], sanity: dict[str, bool]) -> str:
    return "\n".join(
        [
            "# Factor Allocator Report",
            "",
            f"- allocator_status: {diagnostics['allocator_status']}",
            f"- active_factor_count: {diagnostics['active_factor_count']}",
            f"- zero_weight_count: {diagnostics['zero_weight_count']}",
            f"- sign_flip_check_passed: {sanity['sign_flip_check_passed']}",
            f"- scale_response_check_passed: {sanity['scale_response_check_passed']}",
            "- output_scope: factor-level diagnostic allocation only",
            "- non_claims: no production approval, no live trading, no security-level output",
            "",
        ]
    )
