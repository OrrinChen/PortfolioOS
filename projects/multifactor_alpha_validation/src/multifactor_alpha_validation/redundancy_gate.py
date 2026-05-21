from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.marginal_value import choose_marginal_value_decision
from multifactor_alpha_validation.schema import FactorSpec


@dataclass(frozen=True)
class RedundancyGateResult:
    factor_correlation_matrix: pd.DataFrame
    factor_clusters: pd.DataFrame
    marginal_value_report: dict[str, object]
    marginal_value_decision_table: pd.DataFrame
    redundancy_report_markdown: str


def build_redundancy_gate(
    specs: list[FactorSpec],
    signal_panels: dict[str, pd.DataFrame],
    factor_evidence_table: pd.DataFrame,
) -> RedundancyGateResult:
    score_matrix = _score_matrix(signal_panels)
    corr = score_matrix.corr().fillna(0.0).round(6)
    clusters = _cluster_factors(corr)
    decision_table = _decision_table(specs, corr, clusters, factor_evidence_table)
    report = {
        "schema_version": "marginal_value.v1",
        "factor_count": int(len(decision_table)),
        "cluster_count": int(decision_table["cluster_id"].nunique()),
        "promoted_count": int((decision_table["decision"] == "promote_to_allocator").sum()),
        "redundant_count": int((decision_table["decision"] == "real_but_redundant").sum()),
        "archive_count": int((decision_table["decision"] == "archive_no_marginal_value").sum()),
        "decisions": decision_table.to_dict("records"),
    }
    return RedundancyGateResult(
        factor_correlation_matrix=corr,
        factor_clusters=clusters,
        marginal_value_report=report,
        marginal_value_decision_table=decision_table,
        redundancy_report_markdown=_render_report(report),
    )


def write_redundancy_outputs(result: RedundancyGateResult, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    result.factor_correlation_matrix.to_csv(output_dir / "factor_correlation_matrix.csv")
    result.factor_clusters.to_csv(output_dir / "factor_clusters.csv", index=False)
    (output_dir / "marginal_value_report.json").write_text(
        json.dumps(result.marginal_value_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    result.marginal_value_decision_table.to_csv(output_dir / "marginal_value_decision_table.csv", index=False)
    (output_dir / "redundancy_report.md").write_text(result.redundancy_report_markdown, encoding="utf-8")
    return [
        "factor_correlation_matrix.csv",
        "factor_clusters.csv",
        "marginal_value_report.json",
        "marginal_value_decision_table.csv",
        "redundancy_report.md",
    ]


def _score_matrix(signal_panels: dict[str, pd.DataFrame]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for factor_id, panel in signal_panels.items():
        active = panel[panel["coverage_flag"] == True].copy()  # noqa: E712
        active["row_key"] = active["date"].astype(str) + "|" + active["asset_id"].astype(str)
        frames.append(active.set_index("row_key")[["normalized_signal"]].rename(columns={"normalized_signal": factor_id}))
    return pd.concat(frames, axis=1)


def _cluster_factors(corr: pd.DataFrame, threshold: float = 0.95) -> pd.DataFrame:
    parent = {factor_id: factor_id for factor_id in corr.columns}

    def find(item: str) -> str:
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    def union(left: str, right: str) -> None:
        root_left = find(left)
        root_right = find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    for left in corr.columns:
        for right in corr.columns:
            if left < right and abs(float(corr.loc[left, right])) >= threshold:
                union(left, right)

    roots = {factor_id: find(factor_id) for factor_id in corr.columns}
    root_to_cluster = {root: f"cluster_{idx + 1}" for idx, root in enumerate(sorted(set(roots.values())))}
    return pd.DataFrame(
        [
            {"factor_id": factor_id, "cluster_id": root_to_cluster[root]}
            for factor_id, root in sorted(roots.items())
        ]
    )


def _decision_table(
    specs: list[FactorSpec],
    corr: pd.DataFrame,
    clusters: pd.DataFrame,
    factor_evidence_table: pd.DataFrame,
) -> pd.DataFrame:
    spec_map = {spec.factor_id: spec for spec in specs}
    evidence = factor_evidence_table.set_index("factor_id")
    cluster_map = clusters.set_index("factor_id")["cluster_id"].to_dict()
    baseline_by_cluster = _baseline_by_cluster(clusters, evidence)
    rows: list[dict[str, object]] = []

    for factor_id in corr.columns:
        spec = spec_map[factor_id]
        row = evidence.loc[factor_id]
        cluster_id = cluster_map[factor_id]
        baseline = baseline_by_cluster[cluster_id]
        pair_corr = corr.loc[factor_id].drop(labels=[factor_id], errors="ignore").abs()
        max_corr = float(pair_corr.max()) if not pair_corr.empty else 0.0
        baseline_corr = 0.0 if baseline == factor_id else abs(float(corr.loc[factor_id, baseline]))
        neutral_ic = float(row["neutralized_rank_ic_mean"])
        residual_ic = neutral_ic if baseline == factor_id else max(neutral_ic * (1.0 - baseline_corr), 0.0)
        incremental_gross = float(row["top_bottom_spread"]) * (residual_ic / max(abs(neutral_ic), 1e-9))
        incremental_turnover = float(row["turnover_estimate"])
        cost_drag = incremental_turnover * 0.0025
        incremental_net = incremental_gross - cost_drag
        marginal_score = max(incremental_net, 0.0) * 8.0 + residual_ic * 0.25
        decision_input = pd.Series(
            {
                "q1_decision": row["q1_decision"],
                "incremental_net_spread": incremental_net,
                "incremental_turnover": incremental_turnover,
                "max_pairwise_correlation": max_corr,
                "residual_ic_after_baseline": residual_ic,
                "marginal_value_score": marginal_score,
            }
        )
        decision, reason = choose_marginal_value_decision(decision_input)
        rows.append(
            {
                "schema_version": "marginal_value.v1",
                "factor_id": factor_id,
                "family_id": spec.family_id,
                "baseline_set": baseline,
                "cluster_id": cluster_id,
                "correlation_to_baseline": round(baseline_corr, 6),
                "max_pairwise_correlation": round(max_corr, 6),
                "residual_ic_after_baseline": round(residual_ic, 6),
                "incremental_gross_spread": round(incremental_gross, 6),
                "incremental_net_spread": round(incremental_net, 6),
                "incremental_turnover": round(incremental_turnover, 6),
                "incremental_cost_drag": round(cost_drag, 6),
                "marginal_value_score": round(marginal_score, 6),
                "decision": decision,
                "decision_reason": reason,
            }
        )
    return pd.DataFrame(rows)


def _baseline_by_cluster(clusters: pd.DataFrame, evidence: pd.DataFrame) -> dict[str, str]:
    baseline: dict[str, str] = {}
    for cluster_id, group in clusters.groupby("cluster_id"):
        candidates = group["factor_id"].tolist()
        eligible = evidence.loc[candidates]
        eligible = eligible[eligible["q1_decision"] != "q1_diagnostic_only"]
        if eligible.empty:
            eligible = evidence.loc[candidates]
        ranked = eligible.sort_values("neutralized_rank_ic_mean", ascending=False)
        baseline[cluster_id] = str(ranked.index[0])
    return baseline


def _render_report(report: dict[str, object]) -> str:
    return "\n".join(
        [
            "# Redundancy and Marginal-Value Report",
            "",
            f"- factor_count: {report['factor_count']}",
            f"- cluster_count: {report['cluster_count']}",
            f"- promoted_count: {report['promoted_count']}",
            f"- redundant_count: {report['redundant_count']}",
            f"- archive_count: {report['archive_count']}",
            "- rule: raw IC alone cannot route a factor to allocation",
            "- boundary: decisions remain factor-level research decisions only",
            "",
        ]
    )
