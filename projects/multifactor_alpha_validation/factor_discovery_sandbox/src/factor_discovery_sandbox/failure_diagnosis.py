"""FD-R5.1 failure diagnosis for real Factor Discovery candidates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FDRealFailureDiagnosisResult:
    """Artifacts and summary for FD-R5.1."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_real_failure_diagnosis(
    factor_panel_path: str | Path,
    rolling_weights_path: str | Path,
    oos_score_panel_path: str | Path,
    placebo_report_path: str | Path,
    output_dir: str | Path,
) -> FDRealFailureDiagnosisResult:
    """Diagnose why the FD-R3/R4/R5 candidate set failed before allocator entry."""

    factor_panel_file = Path(factor_panel_path)
    weights_file = Path(rolling_weights_path)
    score_file = Path(oos_score_panel_path)
    placebo_file = Path(placebo_report_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    factor_panel = _load_factor_panel(factor_panel_file)
    scores = _load_scores(score_file)
    weights = _load_weights(weights_file)
    placebo = pd.read_csv(placebo_file)

    targets = _target_panel(scores)
    standalone = _standalone_factor_diagnostics(factor_panel, targets)
    family = _family_composite_diagnostics(factor_panel, targets, scores)
    redundancy = _redundancy_clusters(factor_panel)
    attribution = _rolling_weight_failure_attribution(weights, standalone, redundancy)
    recommendations = _candidate_revision_recommendations(
        factor_panel=factor_panel,
        scores=scores,
        placebo=placebo,
        family=family,
        redundancy=redundancy,
        attribution=attribution,
    )
    summary = {
        "schema_version": "fd_real_failure_diagnosis_summary.v1",
        "stage": "FD-R5.1",
        "factor_panel_path": str(factor_panel_file),
        "rolling_weights_path": str(weights_file),
        "oos_score_panel_path": str(score_file),
        "placebo_report_path": str(placebo_file),
        "failure_flags": recommendations["failure_flags"],
        "recommended_next_action": recommendations["recommended_next_action"],
        "allocator_entry_allowed": recommendations["allocator_entry_allowed"],
        "direct_q2_entry_allowed": False,
        "alpha_success_claimed": False,
        "production_approval_claimed": False,
        "not_alpha_evidence": True,
    }

    artifacts = {
        "standalone_factor_oos_diagnostics": output_path / "standalone_factor_oos_diagnostics.csv",
        "family_composite_diagnostics": output_path / "family_composite_diagnostics.csv",
        "rolling_weight_failure_attribution": output_path / "rolling_weight_failure_attribution.csv",
        "real_factor_redundancy_clusters": output_path / "real_factor_redundancy_clusters.csv",
        "candidate_revision_recommendations": output_path / "candidate_revision_recommendations.json",
        "factor_failure_diagnosis_report": output_path / "factor_failure_diagnosis_report.md",
    }
    standalone.to_csv(artifacts["standalone_factor_oos_diagnostics"], index=False)
    family.to_csv(artifacts["family_composite_diagnostics"], index=False)
    attribution.to_csv(artifacts["rolling_weight_failure_attribution"], index=False)
    redundancy.to_csv(artifacts["real_factor_redundancy_clusters"], index=False)
    artifacts["candidate_revision_recommendations"].write_text(
        json.dumps(recommendations, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["factor_failure_diagnosis_report"].write_text(
        _render_report(summary, recommendations, family, redundancy, attribution),
        encoding="utf-8",
    )
    return FDRealFailureDiagnosisResult(summary=summary, artifacts=artifacts)


def _load_factor_panel(path: Path) -> pd.DataFrame:
    usecols = [
        "factor_id",
        "rebalance_date",
        "asset_id",
        "normalized_value",
        "coverage_status",
        "known_correlation_family",
        "signal_timestamp",
        "tradable_timestamp",
    ]
    frame = pd.read_csv(path, usecols=lambda column: column in usecols, low_memory=False)
    frame["rebalance_date"] = pd.to_datetime(frame["rebalance_date"], errors="coerce")
    frame["asset_id"] = frame["asset_id"].astype(str)
    frame["factor_id"] = frame["factor_id"].astype(str)
    frame["normalized_value"] = pd.to_numeric(frame["normalized_value"], errors="coerce")
    if "known_correlation_family" not in frame.columns:
        frame["known_correlation_family"] = "unknown"
    if "signal_timestamp" not in frame.columns:
        frame["signal_timestamp"] = ""
    if "tradable_timestamp" not in frame.columns:
        frame["tradable_timestamp"] = ""
    return frame


def _load_scores(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, low_memory=False)
    frame["rebalance_date"] = pd.to_datetime(frame["rebalance_date"], errors="coerce")
    frame["asset_id"] = frame["asset_id"].astype(str)
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    for column in ("forward_excess_return", "forward_benchmark_return"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["forward_return_available"] = frame["forward_return_available"].astype(str).str.lower().isin(
        {"true", "1", "yes"}
    )
    return frame


def _load_weights(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["rebalance_date"] = pd.to_datetime(frame["rebalance_date"], errors="coerce")
    frame["factor_id"] = frame["factor_id"].astype(str)
    frame["weight"] = pd.to_numeric(frame["weight"], errors="coerce").fillna(0.0)
    frame["rolling_icir"] = pd.to_numeric(frame["rolling_icir"], errors="coerce")
    return frame


def _target_panel(scores: pd.DataFrame) -> pd.DataFrame:
    targets = scores[scores["forward_return_available"]][
        ["rebalance_date", "period", "horizon_months", "asset_id", "forward_excess_return", "forward_benchmark_return"]
    ].drop_duplicates()
    targets["horizon_months"] = targets["horizon_months"].astype(int)
    return targets


def _standalone_factor_diagnostics(factor_panel: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    active = factor_panel[factor_panel["coverage_status"] == "active_view"][
        ["rebalance_date", "asset_id", "factor_id", "known_correlation_family", "normalized_value"]
    ]
    merged = active.merge(targets, on=["rebalance_date", "asset_id"], how="inner")
    rows = []
    for (period, horizon, factor_id), group in merged.groupby(["period", "horizon_months", "factor_id"]):
        rows.append(
            {
                "schema_version": "fd_real_standalone_factor_oos_diagnostics.v1",
                "period": period,
                "horizon_months": int(horizon),
                "factor_id": factor_id,
                "family": str(group["known_correlation_family"].iloc[0]),
                **_score_quality(group, "normalized_value"),
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows).sort_values(["period", "horizon_months", "factor_id"]).reset_index(drop=True)


def _family_composite_diagnostics(
    factor_panel: pd.DataFrame,
    targets: pd.DataFrame,
    scores: pd.DataFrame,
) -> pd.DataFrame:
    active = factor_panel[factor_panel["coverage_status"] == "active_view"][
        ["rebalance_date", "asset_id", "factor_id", "known_correlation_family", "normalized_value"]
    ]
    wide = active.pivot_table(
        index=["rebalance_date", "asset_id"],
        columns="factor_id",
        values="normalized_value",
        aggfunc="last",
    )
    family_map = (
        active[["factor_id", "known_correlation_family"]]
        .drop_duplicates("factor_id")
        .set_index("factor_id")["known_correlation_family"]
        .astype(str)
        .to_dict()
    )
    candidates: dict[str, pd.Series] = {"all_29_equal": wide.mean(axis=1, skipna=True)}
    for family in sorted(set(family_map.values())):
        factors = [factor for factor, factor_family in family_map.items() if factor_family == family and factor in wide.columns]
        if factors:
            candidates[f"{family}_equal"] = wide[factors].mean(axis=1, skipna=True)
    survivor_factors = [
        factor
        for factor, family in family_map.items()
        if family in {"price_momentum", "trend_following", "residual_momentum"} and factor in wide.columns
    ]
    if survivor_factors:
        candidates["momentum_trend_residual_equal"] = wide[survivor_factors].mean(axis=1, skipna=True)

    rows = []
    for candidate, series in candidates.items():
        diagnostic = series.rename("diagnostic_score").reset_index().merge(targets, on=["rebalance_date", "asset_id"], how="inner")
        for (period, horizon), group in diagnostic.groupby(["period", "horizon_months"]):
            rows.append(
                {
                    "schema_version": "fd_real_family_composite_diagnostics.v1",
                    "candidate": candidate,
                    "period": period,
                    "horizon_months": int(horizon),
                    **_score_quality(group, "diagnostic_score"),
                    "not_alpha_evidence": True,
                }
            )

    live = scores[scores["forward_return_available"]].rename(columns={"score": "diagnostic_score"})
    for (period, horizon), group in live.groupby(["period", "horizon_months"]):
        rows.append(
            {
                "schema_version": "fd_real_family_composite_diagnostics.v1",
                "candidate": "rolling_icir_live_composite",
                "period": period,
                "horizon_months": int(horizon),
                **_score_quality(group, "diagnostic_score"),
                "not_alpha_evidence": True,
            }
        )
    frame = pd.DataFrame(rows)
    return frame.sort_values(["period", "horizon_months", "candidate"]).reset_index(drop=True)


def _score_quality(frame: pd.DataFrame, score_column: str) -> dict[str, object]:
    rank_ics: list[float] = []
    spreads: list[float] = []
    positive = 0
    rebalance_count = 0
    for _date, group in frame.groupby("rebalance_date"):
        clean = group[[score_column, "forward_excess_return"]].dropna()
        if len(clean) < 2:
            continue
        rebalance_count += 1
        if clean[score_column].nunique(dropna=True) > 1 and clean["forward_excess_return"].nunique(dropna=True) > 1:
            rank_ics.append(float(clean[score_column].corr(clean["forward_excess_return"], method="spearman")))
        count = max(1, int(np.ceil(len(clean) * 0.1)))
        ordered = clean.sort_values(score_column, ascending=False)
        spread = float(ordered.head(count)["forward_excess_return"].mean() - ordered.tail(count)["forward_excess_return"].mean())
        spreads.append(spread)
        positive += int(spread > 0.0)
    return {
        "rebalance_count": int(rebalance_count),
        "mean_rank_ic": float(np.nanmean(rank_ics)) if rank_ics else np.nan,
        "mean_top_bottom_spread": float(np.nanmean(spreads)) if spreads else np.nan,
        "positive_spread_rate": float(positive / rebalance_count) if rebalance_count else np.nan,
    }


def _redundancy_clusters(factor_panel: pd.DataFrame, threshold: float = 0.95) -> pd.DataFrame:
    columns = [
        "schema_version",
        "cluster_id",
        "factor_id",
        "family",
        "cluster_members",
        "max_abs_corr_in_cluster",
        "relation",
        "recommended_action",
        "not_alpha_evidence",
    ]
    active = factor_panel[factor_panel["coverage_status"] == "active_view"][
        ["rebalance_date", "asset_id", "factor_id", "known_correlation_family", "normalized_value"]
    ]
    family_map = (
        active[["factor_id", "known_correlation_family"]]
        .drop_duplicates("factor_id")
        .set_index("factor_id")["known_correlation_family"]
        .astype(str)
        .to_dict()
    )
    wide = active.pivot_table(
        index=["rebalance_date", "asset_id"],
        columns="factor_id",
        values="normalized_value",
        aggfunc="last",
    )
    corr = wide.corr(method="spearman", min_periods=2)
    factors = list(corr.columns)
    parent = {factor: factor for factor in factors}

    def find(factor: str) -> str:
        while parent[factor] != factor:
            parent[factor] = parent[parent[factor]]
            factor = parent[factor]
        return factor

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    max_corr_by_factor = {factor: 0.0 for factor in factors}
    relation_by_pair: dict[tuple[str, str], str] = {}
    for index, left in enumerate(factors):
        for right in factors[index + 1 :]:
            value = corr.loc[left, right]
            if pd.notna(value):
                max_corr_by_factor[left] = max(max_corr_by_factor[left], abs(float(value)))
                max_corr_by_factor[right] = max(max_corr_by_factor[right], abs(float(value)))
            if pd.notna(value) and abs(float(value)) >= threshold:
                union(left, right)
                relation_by_pair[(left, right)] = _redundancy_relation(left, right, float(value))

    clusters: dict[str, list[str]] = {}
    for factor in factors:
        clusters.setdefault(find(factor), []).append(factor)

    rows = []
    cluster_number = 0
    for members in sorted(clusters.values(), key=lambda values: (len(values) * -1, values[0])):
        if len(members) == 1:
            continue
        cluster_number += 1
        cluster_members = "|".join(sorted(members))
        relation = _cluster_relation(sorted(members), relation_by_pair)
        for factor in sorted(members):
            rows.append(
                {
                    "schema_version": "fd_real_factor_redundancy_clusters.v1",
                    "cluster_id": f"cluster_{cluster_number:02d}",
                    "factor_id": factor,
                    "family": family_map.get(factor, "unknown"),
                    "cluster_members": cluster_members,
                    "max_abs_corr_in_cluster": round(max_corr_by_factor.get(factor, 0.0), 6),
                    "relation": relation,
                    "recommended_action": _redundancy_action(factor, sorted(members), relation),
                    "not_alpha_evidence": True,
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _redundancy_relation(left: str, right: str, corr_value: float) -> str:
    pair = {left, right}
    if {"momentum_6m", "residual_momentum_6m"}.issubset(pair):
        return "not_truly_residual_current_definition"
    if corr_value <= -0.95:
        return "sign_transform_duplicate"
    return "exact_duplicate_or_scale"


def _cluster_relation(members: list[str], relation_by_pair: Mapping[tuple[str, str], str]) -> str:
    if {"momentum_6m", "residual_momentum_6m"}.issubset(set(members)):
        return "not_truly_residual_current_definition"
    relations = set()
    for index, left in enumerate(members):
        for right in members[index + 1 :]:
            relations.add(relation_by_pair.get((left, right)) or relation_by_pair.get((right, left)) or "")
    if "sign_transform_duplicate" in relations:
        return "sign_transform_duplicate"
    return "exact_duplicate_or_scale"


def _redundancy_action(factor: str, members: list[str], relation: str) -> str:
    if factor == "residual_momentum_6m" and relation == "not_truly_residual_current_definition":
        return "rewrite_as_true_residual"
    if relation in {"exact_duplicate_or_scale", "sign_transform_duplicate"}:
        return "requires_formula_mechanism_audit"
    return "cluster_representative_candidate"


def _rolling_weight_failure_attribution(
    weights: pd.DataFrame,
    standalone: pd.DataFrame,
    redundancy: pd.DataFrame,
) -> pd.DataFrame:
    if standalone.empty:
        return pd.DataFrame()
    test_perf = standalone[standalone["period"] == "test"][
        ["factor_id", "horizon_months", "family", "mean_top_bottom_spread", "mean_rank_ic"]
    ].rename(
        columns={
            "mean_top_bottom_spread": "standalone_mean_top_bottom_spread",
            "mean_rank_ic": "standalone_mean_rank_ic",
        }
    )
    grouped = (
        weights[weights["period"] == "test"]
        .groupby(["horizon_months", "factor_id"], as_index=False)
        .agg(
            mean_abs_weight=("weight", lambda series: float(series.abs().mean())),
            total_abs_weight=("weight", lambda series: float(series.abs().sum())),
            positive_weight_rate=("weight", lambda series: float((series > 0.0).mean())),
            mean_rolling_icir=("rolling_icir", "mean"),
        )
    )
    result = grouped.merge(test_perf, on=["horizon_months", "factor_id"], how="left")
    duplicate_actions = redundancy[["factor_id", "recommended_action"]].drop_duplicates("factor_id")
    result = result.merge(duplicate_actions, on="factor_id", how="left")
    result["recommended_action"] = result["recommended_action"].fillna("")
    result["attribution_status"] = result.apply(_attribution_status, axis=1)
    result.insert(0, "schema_version", "fd_real_rolling_weight_failure_attribution.v1")
    result["not_alpha_evidence"] = True
    return result.sort_values(["horizon_months", "attribution_status", "mean_abs_weight"], ascending=[True, True, False])


def _attribution_status(row: pd.Series) -> str:
    spread = row.get("standalone_mean_top_bottom_spread")
    if pd.notna(spread) and float(spread) < 0.0 and float(row.get("mean_abs_weight", 0.0)) > 0.0:
        return "overweighted_negative_or_noise_factor"
    if row.get("recommended_action") in {
        "archive_duplicate",
        "rewrite_as_true_residual",
        "diagnostic_only_sign_transform",
        "requires_formula_mechanism_audit",
    }:
        return "redundant_or_definition_issue"
    if pd.notna(spread) and float(spread) > 0.0:
        return "positive_standalone_signal"
    return "neutral_or_insufficient_signal"


def _candidate_revision_recommendations(
    factor_panel: pd.DataFrame,
    scores: pd.DataFrame,
    placebo: pd.DataFrame,
    family: pd.DataFrame,
    redundancy: pd.DataFrame,
    attribution: pd.DataFrame,
) -> dict[str, object]:
    coverage = factor_panel.groupby("factor_id")["coverage_status"].agg(
        coverage_ratio=lambda series: float((series == "active_view").mean())
    )
    min_coverage = float(coverage["coverage_ratio"].min()) if not coverage.empty else 0.0
    same_close = _same_close_used(factor_panel)
    live_spread = _candidate_spread(family, "rolling_icir_live_composite")
    all_equal_spread = _candidate_spread(family, "all_29_equal")
    price_momentum_spread = _candidate_spread(family, "price_momentum_equal")
    placebo_status, next_action = _placebo_status(placebo)
    residual_flagged = (
        (redundancy["factor_id"] == "residual_momentum_6m")
        & (redundancy["recommended_action"] == "rewrite_as_true_residual")
    ).any() if not redundancy.empty else False
    rolling_failure = bool(
        placebo_status == "failed_placebo_gate"
        or (
            pd.notna(live_spread)
            and pd.notna(all_equal_spread)
            and float(live_spread) < float(all_equal_spread)
        )
    )
    archive_factors = sorted(
        attribution.loc[attribution["recommended_action"] == "archive_duplicate", "factor_id"].dropna().unique().tolist()
    ) if not attribution.empty else []
    recommendations = {
        "schema_version": "fd_real_candidate_revision_recommendations.v1",
        "stage": "FD-R5.1",
        "recommended_next_action": next_action,
        "allocator_entry_allowed": False,
        "direct_q2_entry_allowed": False,
        "not_alpha_evidence": True,
        "failure_flags": {
            "data_timestamp_failure": same_close,
            "coverage_failure": bool(min_coverage < 0.90),
            "factor_definition_failure": bool(residual_flagged),
            "redundancy_failure": bool(not redundancy.empty),
            "rolling_icir_overfit_noise_failure": rolling_failure,
            "sector_regime_contribution": _sector_contribution(placebo),
            "allocator_entry": "blocked",
        },
        "minimum_factor_coverage": round(min_coverage, 6),
        "live_vs_equal_weight": {
            "rolling_icir_live_composite_test_spread": live_spread,
            "all_29_equal_test_spread": all_equal_spread,
            "price_momentum_equal_test_spread": price_momentum_spread,
        },
        "core_families": ["price_momentum", "trend_quality", "sector_neutral_residual_momentum"],
        "diagnostic_only_families": [
            "risk_volatility",
            "overshoot_reversal",
            "liquidity_shock",
            "capacity_level",
            "turnover_shock",
            "turnover_trend",
        ],
        "rewrite_required": ["residual_momentum_6m_v2"] if residual_flagged else [],
        "archive_factors": sorted(set(archive_factors)),
        "do_not_continue_to": ["FD-R6_allocator", "Q1", "Q2", "AlphaRegistry"],
    }
    return recommendations


def _same_close_used(factor_panel: pd.DataFrame) -> bool:
    signal = pd.to_datetime(factor_panel["signal_timestamp"], errors="coerce")
    tradable = pd.to_datetime(factor_panel["tradable_timestamp"], errors="coerce")
    comparable = signal.notna() & tradable.notna()
    if not comparable.any():
        return False
    return bool((tradable[comparable] <= signal[comparable]).any())


def _candidate_spread(family: pd.DataFrame, candidate: str) -> float | None:
    rows = family[(family["candidate"] == candidate) & (family["period"] == "test")]
    if rows.empty:
        return None
    return float(rows["mean_top_bottom_spread"].mean())


def _placebo_status(placebo: pd.DataFrame) -> tuple[str, str]:
    test_rows = placebo[placebo["period"] == "test"]
    live = test_rows[test_rows["test_name"] == "live_oos_score"]
    comparators = test_rows[
        test_rows["test_name"].isin(
            {
                "shuffled_cross_section_placebo",
                "lagged_signal_placebo",
                "random_same_coverage_placebo",
                "rebalance_date_shifted_placebo",
            }
        )
    ]
    if live.empty or comparators.empty:
        return "insufficient_placebo_evidence", "needs_more_evidence_before_allocator"
    live_spread = float(live["mean_top_bottom_spread"].mean())
    live_rank_ic = float(live["mean_rank_ic"].mean())
    comparator_spread = float(comparators["mean_top_bottom_spread"].median())
    if live_spread > max(0.0, comparator_spread) and live_rank_ic > 0.0:
        return "passed_initial_placebo_gate", "needs_more_evidence_before_allocator"
    return "failed_placebo_gate", "stop_before_allocator"


def _sector_contribution(placebo: pd.DataFrame) -> str:
    test_rows = placebo[placebo["period"] == "test"]
    live = test_rows[test_rows["test_name"] == "live_oos_score"]
    sector = test_rows[test_rows["test_name"] == "sector_neutral_placebo"]
    if live.empty or sector.empty:
        return "unknown"
    live_spread = float(live["mean_top_bottom_spread"].mean())
    sector_spread = float(sector["mean_top_bottom_spread"].mean())
    if abs(sector_spread - live_spread) > 0.001:
        return "partial"
    return "limited"


def _render_report(
    summary: Mapping[str, object],
    recommendations: Mapping[str, object],
    family: pd.DataFrame,
    redundancy: pd.DataFrame,
    attribution: pd.DataFrame,
) -> str:
    flags = recommendations["failure_flags"]
    live = recommendations["live_vs_equal_weight"]
    lines = [
        "# FD-R5.1 Factor Failure Diagnosis",
        "",
        "not alpha evidence",
        f"data/timestamp failure: {_yes_no(flags['data_timestamp_failure'])}",
        f"coverage failure: {_yes_no(flags['coverage_failure'])}",
        f"factor definition failure: {_yes_no(flags['factor_definition_failure'])}",
        f"redundancy failure: {_yes_no(flags['redundancy_failure'])}",
        f"rolling ICIR overfit/noise failure: {_yes_no(flags['rolling_icir_overfit_noise_failure'])}",
        f"sector/regime contribution: {flags['sector_regime_contribution']}",
        "allocator entry: blocked",
        "direct Q2 entry: not allowed",
        "",
        "## Live vs Simple Composites",
        f"- rolling_icir_live_composite test spread: {live['rolling_icir_live_composite_test_spread']}",
        f"- all_29_equal test spread: {live['all_29_equal_test_spread']}",
        f"- price_momentum_equal test spread: {live['price_momentum_equal_test_spread']}",
        "",
        "## Redundancy",
        f"- redundant factor rows: {len(redundancy)}",
        f"- rewrite required: {', '.join(recommendations['rewrite_required']) or 'none'}",
        f"- archive factors: {', '.join(recommendations['archive_factors']) or 'none'}",
        "",
        "## Candidate Redefinition",
        "- core: price_momentum / trend_following / true residual_momentum_v2",
        "- diagnostic-only: volatility / reversal / liquidity",
        "- do not proceed to FD-R6 allocator for this candidate set",
        "",
        "## Boundary",
        f"- recommended_next_action: {recommendations['recommended_next_action']}",
        "- no allocator, Q1, Q2, production approval, or Alpha Registry import",
        "",
    ]
    if not attribution.empty:
        bad = attribution[attribution["attribution_status"] == "overweighted_negative_or_noise_factor"].head(5)
        if not bad.empty:
            lines.append("## Weight Failure Examples")
            for row in bad.itertuples(index=False):
                lines.append(
                    f"- {row.factor_id} {int(row.horizon_months)}m: "
                    f"mean_abs_weight={row.mean_abs_weight:.6f}, "
                    f"standalone_spread={row.standalone_mean_top_bottom_spread:.6f}"
                )
            lines.append("")
    return "\n".join(lines)


def _yes_no(value: object) -> str:
    return "yes" if bool(value) else "no"
