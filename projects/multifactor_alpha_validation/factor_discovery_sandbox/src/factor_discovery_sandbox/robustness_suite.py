"""Robustness diagnostics for FD real-data OOS score panels."""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_rebalance_diagnostics(frame: pd.DataFrame, score_column: str, test_name: str) -> pd.DataFrame:
    """Compute rank IC and top-bottom spread by rebalance for one score variant."""

    rows: list[dict[str, object]] = []
    for (period, horizon, rebalance_date), group in frame.groupby(["period", "horizon_months", "rebalance_date"]):
        eligible = group[
            (group["coverage_state"] == "active_score")
            & (group["forward_return_available"])
            & group[score_column].notna()
            & group["forward_excess_return"].notna()
        ][[score_column, "forward_excess_return"]].copy()
        row = {
            "test_name": test_name,
            "period": period,
            "horizon_months": int(horizon),
            "rebalance_date": rebalance_date,
            "eligible_name_count": int(len(eligible)),
            "top_decile_count": 0,
            "bottom_decile_count": 0,
            "rank_ic": np.nan,
            "top_bottom_spread": np.nan,
            "top_decile_excess_return": np.nan,
            "bottom_decile_excess_return": np.nan,
            "not_alpha_evidence": True,
        }
        if len(eligible) >= 2:
            ordered = eligible.sort_values(score_column, ascending=False)
            count = max(1, int(np.ceil(len(ordered) * 0.1)))
            top = ordered.head(count)
            bottom = ordered.tail(count)
            rank_ic = np.nan
            if ordered[score_column].nunique(dropna=True) > 1 and ordered["forward_excess_return"].nunique(dropna=True) > 1:
                rank_ic = ordered[score_column].corr(ordered["forward_excess_return"], method="spearman")
            row.update(
                {
                    "top_decile_count": int(len(top)),
                    "bottom_decile_count": int(len(bottom)),
                    "rank_ic": float(rank_ic) if pd.notna(rank_ic) else np.nan,
                    "top_bottom_spread": float(top["forward_excess_return"].mean() - bottom["forward_excess_return"].mean()),
                    "top_decile_excess_return": float(top["forward_excess_return"].mean()),
                    "bottom_decile_excess_return": float(bottom["forward_excess_return"].mean()),
                }
            )
        rows.append(row)
    return pd.DataFrame(rows)


def aggregate_placebo_report(diagnostics: pd.DataFrame) -> pd.DataFrame:
    """Aggregate rebalance diagnostics into the FD-R5 placebo report."""

    if diagnostics.empty:
        return pd.DataFrame(
            columns=[
                "schema_version",
                "test_name",
                "period",
                "horizon_months",
                "rebalance_count",
                "mean_rank_ic",
                "mean_top_bottom_spread",
                "positive_spread_rate",
                "not_alpha_evidence",
            ]
        )
    report = (
        diagnostics.groupby(["test_name", "period", "horizon_months"], as_index=False)
        .agg(
            rebalance_count=("rebalance_date", "nunique"),
            mean_rank_ic=("rank_ic", "mean"),
            mean_top_bottom_spread=("top_bottom_spread", "mean"),
            positive_spread_rate=("top_bottom_spread", lambda series: float((series > 0.0).mean())),
        )
        .sort_values(["test_name", "period", "horizon_months"])
    )
    report.insert(0, "schema_version", "fd_real_placebo_report.v1")
    report["not_alpha_evidence"] = True
    return report


def build_robustness_by_period(live_diagnostics: pd.DataFrame) -> pd.DataFrame:
    """Summarize live OOS robustness by validation/test period."""

    if live_diagnostics.empty:
        return pd.DataFrame()
    result = (
        live_diagnostics.groupby(["period", "horizon_months"], as_index=False)
        .agg(
            rebalance_count=("rebalance_date", "nunique"),
            mean_rank_ic=("rank_ic", "mean"),
            mean_top_bottom_spread=("top_bottom_spread", "mean"),
            positive_spread_rate=("top_bottom_spread", lambda series: float((series > 0.0).mean())),
        )
        .sort_values(["period", "horizon_months"])
    )
    result.insert(0, "schema_version", "fd_real_robustness_by_period.v1")
    result["not_alpha_evidence"] = True
    return result


def build_robustness_by_regime(score_panel: pd.DataFrame) -> pd.DataFrame:
    """Summarize live score robustness by simple QQQ forward-return regime."""

    frame = score_panel.copy()
    frame["benchmark_regime"] = np.where(frame["forward_benchmark_return"] >= 0.0, "qqq_up", "qqq_down")
    rows: list[pd.DataFrame] = []
    for regime, group in frame.groupby("benchmark_regime"):
        diagnostics = compute_rebalance_diagnostics(group, "score", "live_oos_score")
        if diagnostics.empty:
            continue
        summary = (
            diagnostics.groupby(["period", "horizon_months"], as_index=False)
            .agg(
                rebalance_count=("rebalance_date", "nunique"),
                mean_rank_ic=("rank_ic", "mean"),
                mean_top_bottom_spread=("top_bottom_spread", "mean"),
                positive_spread_rate=("top_bottom_spread", lambda series: float((series > 0.0).mean())),
            )
            .sort_values(["period", "horizon_months"])
        )
        summary["benchmark_regime"] = regime
        rows.append(summary)
    if not rows:
        return pd.DataFrame()
    result = pd.concat(rows, ignore_index=True)
    result.insert(0, "schema_version", "fd_real_robustness_by_regime.v1")
    result["not_alpha_evidence"] = True
    return result[
        [
            "schema_version",
            "period",
            "horizon_months",
            "benchmark_regime",
            "rebalance_count",
            "mean_rank_ic",
            "mean_top_bottom_spread",
            "positive_spread_rate",
            "not_alpha_evidence",
        ]
    ]
