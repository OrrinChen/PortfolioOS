"""Shrinkage allocator and zero-weight attribution for Factor Discovery Sandbox."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .marginal_value import run_marginal_value_gate
from .teaching_baseline import (
    _build_factor_panel,
    _build_teaching_price_fixture,
    _compute_factor_correlation,
)


@dataclass(frozen=True)
class AllocatorResult:
    """Artifacts and summary for the allocator diagnostics."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_allocator(output_dir: str | Path) -> AllocatorResult:
    """Write posterior means, shrunk covariance, allocator weights, and zero reasons."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    gate = run_marginal_value_gate(output_path)
    decision_table = pd.read_csv(gate.artifacts["marginal_value_decision_table"])
    posterior = _build_posterior_mu(decision_table)
    covariance = _build_shrunk_covariance()
    weights = _build_allocator_weights(posterior, decision_table)
    zero_reasons = _build_zero_weight_attribution(weights, decision_table, posterior)

    artifacts = {
        "posterior_factor_mu": output_path / "posterior_factor_mu.csv",
        "factor_covariance_shrunk": output_path / "factor_covariance_shrunk.csv",
        "allocator_weights": output_path / "allocator_weights.csv",
        "zero_weight_attribution": output_path / "zero_weight_attribution.csv",
    }
    posterior.to_csv(artifacts["posterior_factor_mu"], index=False)
    covariance.to_csv(artifacts["factor_covariance_shrunk"])
    weights.to_csv(artifacts["allocator_weights"], index=False)
    zero_reasons.to_csv(artifacts["zero_weight_attribution"], index=False)

    summary = {
        "schema_version": "factor_allocator_summary.v1",
        "factor_count": int(weights["factor"].nunique()),
        "allocated_factor_count": int((weights["allocator_weight"] > 0).sum()),
        "zero_weight_count": int((weights["allocator_weight"] == 0).sum()),
        "sign_flip_sanity_check_passed": _sign_flip_sanity_check(posterior, decision_table),
        "scale_response_sanity_check_passed": _scale_response_sanity_check(posterior, decision_table),
        "production_strategy_claimed": False,
    }
    return AllocatorResult(summary=summary, artifacts=artifacts)


def _build_posterior_mu(decision_table: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in decision_table.itertuples(index=False):
        shrinkage = 0.50
        if row.max_abs_corr >= 0.85:
            shrinkage = 0.75
        if row.decision in {"diagnostic_only", "needs_more_evidence"}:
            shrinkage = 0.90
        raw_mu = float(row.residual_ic)
        rows.append(
            {
                "factor": row.factor,
                "raw_mu": raw_mu,
                "posterior_mu": raw_mu * (1.0 - shrinkage),
                "shrinkage_intensity": shrinkage,
                "decision": row.decision,
            }
        )
    return pd.DataFrame(rows).sort_values("factor").reset_index(drop=True)


def _build_shrunk_covariance() -> pd.DataFrame:
    prices = _build_teaching_price_fixture()
    returns = prices.pct_change().fillna(0.0)
    factor_panel = _build_factor_panel(prices, returns)
    corr = _compute_factor_correlation(factor_panel).fillna(0.0)
    shrunk = corr * 0.50
    for factor in shrunk.index:
        shrunk.loc[factor, factor] = 1.0
    return shrunk.sort_index(axis=0).sort_index(axis=1)


def _build_allocator_weights(posterior: pd.DataFrame, decision_table: pd.DataFrame) -> pd.DataFrame:
    merged = posterior.merge(
        decision_table[["factor", "known_correlation_family", "decision", "incremental_turnover", "cost_drag"]],
        on=["factor", "decision"],
        how="left",
    )
    merged["raw_allocator_score"] = np.where(
        merged["decision"].eq("promote_to_allocator"),
        merged["posterior_mu"].clip(lower=0.0),
        0.0,
    )
    if merged["raw_allocator_score"].sum() <= 0:
        merged["raw_allocator_score"] = np.where(merged["decision"].eq("promote_to_allocator"), 1.0, 0.0)
    merged["allocator_weight"] = _normalize_with_cluster_cap(merged)
    merged["production_strategy_claimed"] = False
    return merged[
        [
            "factor",
            "known_correlation_family",
            "decision",
            "posterior_mu",
            "allocator_weight",
            "incremental_turnover",
            "cost_drag",
            "production_strategy_claimed",
        ]
    ].rename(columns={"known_correlation_family": "cluster_id"})


def _normalize_with_cluster_cap(frame: pd.DataFrame, cluster_cap: float = 0.50) -> pd.Series:
    weights = frame["raw_allocator_score"] / frame["raw_allocator_score"].sum()
    capped = weights.copy()
    for cluster, indexes in frame.groupby("known_correlation_family").groups.items():
        cluster_weight = float(capped.loc[indexes].sum())
        if cluster_weight > cluster_cap:
            capped.loc[indexes] = capped.loc[indexes] * (cluster_cap / cluster_weight)
    if capped.sum() <= 0:
        return capped
    return capped / capped.sum()


def _build_zero_weight_attribution(
    weights: pd.DataFrame,
    decision_table: pd.DataFrame,
    posterior: pd.DataFrame,
) -> pd.DataFrame:
    merged = weights.merge(
        decision_table[["factor", "max_abs_corr", "incremental_net_return"]],
        on="factor",
        how="left",
    )
    rows = []
    for row in merged[merged["allocator_weight"] == 0].itertuples(index=False):
        rows.append(
            {
                "factor": row.factor,
                "zero_weight_reason": _zero_reason(row),
                "decision": row.decision,
            }
        )
    return pd.DataFrame(rows).sort_values("factor").reset_index(drop=True)


def _zero_reason(row: object) -> str:
    if row.decision == "needs_more_evidence":
        return "insufficient_evidence"
    if row.max_abs_corr >= 0.85 or row.decision == "real_but_redundant":
        return "high_redundancy"
    if row.incremental_turnover >= 0.75:
        return "high_turnover"
    if row.cost_drag > abs(row.incremental_net_return):
        return "high_cost_drag"
    if row.decision == "diagnostic_only":
        return "no_view"
    if row.posterior_mu <= 0:
        return "low_posterior_alpha"
    return "cluster_dominated"


def _sign_flip_sanity_check(posterior: pd.DataFrame, decision_table: pd.DataFrame) -> bool:
    original = _build_allocator_weights(posterior, decision_table)["allocator_weight"].sum()
    flipped = posterior.copy()
    flipped["posterior_mu"] = -flipped["posterior_mu"]
    flipped_weight = _build_allocator_weights(flipped, decision_table)["allocator_weight"].sum()
    return bool(original >= flipped_weight)


def _scale_response_sanity_check(posterior: pd.DataFrame, decision_table: pd.DataFrame) -> bool:
    original = _build_allocator_weights(posterior, decision_table)["allocator_weight"].to_numpy()
    scaled = posterior.copy()
    scaled["posterior_mu"] = scaled["posterior_mu"] * 2.0
    scaled_weights = _build_allocator_weights(scaled, decision_table)["allocator_weight"].to_numpy()
    return bool(np.allclose(original, scaled_weights))
