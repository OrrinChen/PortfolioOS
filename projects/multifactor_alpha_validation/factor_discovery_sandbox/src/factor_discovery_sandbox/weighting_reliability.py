"""FD-R4.1 / FD-R5.2 rolling weighting reliability gate."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from .factor_weighting_estimators import (
    GUARD_VALUES,
    estimate_equal_weight_all,
    estimate_family_equal_weight,
    estimate_shrunk_rolling_icir,
)


@dataclass(frozen=True)
class WeightingReliabilityResult:
    """Artifacts and summary for FD-R4.1 / FD-R5.2."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_weighting_reliability_gate(
    factor_panel_path: str | Path,
    rolling_weights_path: str | Path,
    oos_score_panel_path: str | Path,
    placebo_report_path: str | Path,
    output_dir: str | Path,
    report_path: str | Path,
    train_window_months: int = 36,
    shrink_lambdas: Iterable[float] = (6.0, 12.0, 24.0),
    ridge_alphas: Iterable[float] = (1.0, 10.0),
) -> WeightingReliabilityResult:
    """Compare rolling weighting estimators without opening allocator paths."""

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    report = Path(report_path)
    report.parent.mkdir(parents=True, exist_ok=True)

    factor_panel = _normalize_factor_panel(pd.read_csv(factor_panel_path, low_memory=False))
    rolling_weights = _normalize_rolling_weights(pd.read_csv(rolling_weights_path, low_memory=False))
    score_panel = _normalize_score_panel(pd.read_csv(oos_score_panel_path, low_memory=False))
    placebo_report = pd.read_csv(placebo_report_path, low_memory=False)

    metadata = _factor_metadata(factor_panel)
    factor_ids = metadata["factor_id"].tolist()
    feature_wide = _feature_wide(factor_panel, factor_ids)
    members = _members_by_date(factor_panel)
    targets = _targets_from_score_panel(score_panel)
    rebalance_keys = (
        score_panel[["rebalance_date", "period", "horizon_months"]]
        .drop_duplicates()
        .sort_values(["rebalance_date", "horizon_months"])
    )

    all_weight_rows: list[pd.DataFrame] = []
    all_score_rows: list[pd.DataFrame] = []
    for key in rebalance_keys.itertuples(index=False):
        rebalance_date = pd.Timestamp(key.rebalance_date)
        horizon = int(key.horizon_months)
        period = str(key.period)
        current_icir = _current_icir_frame(rolling_weights, metadata, rebalance_date, horizon)
        estimator_frames = _estimator_frames(
            feature_wide=feature_wide,
            targets=targets,
            metadata=metadata,
            current_icir=current_icir,
            rebalance_date=rebalance_date,
            period=period,
            horizon=horizon,
            train_window_months=train_window_months,
            shrink_lambdas=tuple(float(value) for value in shrink_lambdas),
            ridge_alphas=tuple(float(value) for value in ridge_alphas),
        )
        for weights in estimator_frames:
            scored = _score_estimator(feature_wide, members, weights, targets, rebalance_date, period, horizon)
            all_weight_rows.append(weights)
            all_score_rows.append(scored)

    nonempty_weight_rows = [frame for frame in all_weight_rows if not frame.empty]
    nonempty_score_rows = [frame for frame in all_score_rows if not frame.empty]
    weights = pd.concat(nonempty_weight_rows, ignore_index=True) if nonempty_weight_rows else pd.DataFrame()
    scores = pd.concat(nonempty_score_rows, ignore_index=True) if nonempty_score_rows else pd.DataFrame()
    diagnostics = _rebalance_diagnostics(scores)
    placebo_scores, placebo_diag = _placebo_scores_and_diagnostics(feature_wide, members, weights, targets)
    placebo_summary = _placebo_summary(diagnostics, placebo_diag, weights)
    stability = _stability_diagnostics(weights, diagnostics, metadata, rolling_weights)
    comparison = _estimator_comparison(diagnostics, stability, placebo_summary)
    failure = _failure_diagnosis(comparison, placebo_report)

    artifacts = {
        "weighting_estimator_comparison": output / "weighting_estimator_comparison.csv",
        "weight_stability_diagnostics": output / "weight_stability_diagnostics.csv",
        "weighting_placebo_comparison": output / "weighting_placebo_comparison.csv",
        "weighting_failure_diagnosis": output / "weighting_failure_diagnosis.json",
        "weighting_reliability_report": report,
    }
    comparison.to_csv(artifacts["weighting_estimator_comparison"], index=False)
    stability.to_csv(artifacts["weight_stability_diagnostics"], index=False)
    placebo_summary.to_csv(artifacts["weighting_placebo_comparison"], index=False)
    artifacts["weighting_failure_diagnosis"].write_text(
        json.dumps(failure, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["weighting_reliability_report"].write_text(
        _render_report(failure, comparison, stability),
        encoding="utf-8",
    )

    summary = {
        "schema_version": "fd_weighting_reliability_summary.v1",
        "stage": "FD-R4.1/FD-R5.2",
        "factor_panel_path": str(factor_panel_path),
        "rolling_weights_path": str(rolling_weights_path),
        "oos_score_panel_path": str(oos_score_panel_path),
        "placebo_report_path": str(placebo_report_path),
        "estimator_count": int(comparison["estimator_name"].nunique()) if not comparison.empty else 0,
        "decision": failure["decision"],
        "rolling_icir_overfit_noise_failure": failure["rolling_icir_overfit_noise_failure"],
        "allocator_entry_allowed": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "production_approval_claimed": False,
        "direct_q2_entry_allowed": False,
        "not_alpha_evidence": True,
    }
    return WeightingReliabilityResult(summary=summary, artifacts=artifacts)


def _estimator_frames(
    *,
    feature_wide: pd.DataFrame,
    targets: pd.DataFrame,
    metadata: pd.DataFrame,
    current_icir: pd.DataFrame,
    rebalance_date: pd.Timestamp,
    period: str,
    horizon: int,
    train_window_months: int,
    shrink_lambdas: tuple[float, ...],
    ridge_alphas: tuple[float, ...],
) -> list[pd.DataFrame]:
    frames = [
        _decorate_weights(
            current_icir,
            estimator_name="rolling_icir_current",
            estimator_family="learned_icir",
            rebalance_date=rebalance_date,
            period=period,
            horizon=horizon,
        ),
        _decorate_weights(
            estimate_equal_weight_all(metadata["factor_id"].tolist()),
            estimator_name="equal_weight_all",
            estimator_family="baseline",
            rebalance_date=rebalance_date,
            period=period,
            horizon=horizon,
        ),
        _decorate_weights(
            estimate_family_equal_weight(metadata),
            estimator_name="family_equal_weight",
            estimator_family="baseline",
            rebalance_date=rebalance_date,
            period=period,
            horizon=horizon,
        ),
    ]
    for value in shrink_lambdas:
        frames.append(
            _decorate_weights(
                estimate_shrunk_rolling_icir(current_icir, shrink_lambda=value, signed=False),
                estimator_name=f"shrunk_rolling_icir_lambda_{value:g}",
                estimator_family="shrunk_icir",
                rebalance_date=rebalance_date,
                period=period,
                horizon=horizon,
                shrink_lambda=value,
            )
        )
        frames.append(
            _decorate_weights(
                estimate_shrunk_rolling_icir(current_icir, shrink_lambda=value, signed=True),
                estimator_name=f"signed_shrunk_rolling_icir_lambda_{value:g}",
                estimator_family="signed_shrunk_icir",
                rebalance_date=rebalance_date,
                period=period,
                horizon=horizon,
                shrink_lambda=value,
            )
        )
    for alpha in ridge_alphas:
        ridge = _estimate_ridge_weighting_from_wide(
            feature_wide=feature_wide,
            targets=targets,
            rebalance_date=rebalance_date,
            horizon_months=horizon,
            factor_ids=metadata["factor_id"].tolist(),
            train_window_months=train_window_months,
            ridge_alpha=alpha,
        )
        frames.append(
            _decorate_weights(
                ridge,
                estimator_name=f"ridge_weighting_alpha_{alpha:g}",
                estimator_family="ridge",
                rebalance_date=rebalance_date,
                period=period,
                horizon=horizon,
                ridge_alpha=alpha,
            )
        )
    return frames


def _decorate_weights(
    weights: pd.DataFrame,
    *,
    estimator_name: str,
    estimator_family: str,
    rebalance_date: pd.Timestamp,
    period: str,
    horizon: int,
    shrink_lambda: float | None = None,
    ridge_alpha: float | None = None,
) -> pd.DataFrame:
    frame = weights.copy()
    frame["schema_version"] = "fd_weighting_estimator_weights.v1"
    frame["estimator_name"] = estimator_name
    frame["estimator_family"] = estimator_family
    frame["rebalance_date"] = pd.Timestamp(rebalance_date)
    frame["period"] = period
    frame["horizon_months"] = int(horizon)
    frame["shrink_lambda"] = float(shrink_lambda) if shrink_lambda is not None else np.nan
    frame["ridge_alpha"] = float(ridge_alpha) if ridge_alpha is not None else np.nan
    for column in ("estimation_window_start", "estimation_window_end", "return_visibility_cutoff"):
        if column not in frame.columns:
            frame[column] = ""
    for key, value in GUARD_VALUES.items():
        frame[key] = value
    return frame


def _estimate_ridge_weighting_from_wide(
    feature_wide: pd.DataFrame,
    targets: pd.DataFrame,
    rebalance_date: pd.Timestamp,
    horizon_months: int,
    factor_ids: list[str],
    train_window_months: int,
    ridge_alpha: float,
) -> pd.DataFrame:
    factors = sorted(pd.Series(factor_ids).dropna().astype(str).unique())
    rebalance = pd.Timestamp(rebalance_date)
    history_target = targets[
        (targets["horizon_months"] == int(horizon_months))
        & (targets["rebalance_date"] < rebalance)
        & (targets["target_return_visible_timestamp"] < rebalance)
        & (targets["forward_return_available"])
    ].copy()
    history_dates = [pd.Timestamp(value) for value in sorted(history_target["rebalance_date"].dropna().unique())][
        -int(train_window_months) :
    ]
    history_target = history_target[history_target["rebalance_date"].isin(history_dates)]
    if history_target.empty:
        return _zero_weight_frame(factors, "insufficient_visible_history")
    date_index = feature_wide.index.get_level_values("rebalance_date")
    features = feature_wide.loc[date_index.isin(history_dates), factors]
    if features.empty:
        return _zero_weight_frame(factors, "insufficient_training_rows", history_target)
    y = history_target[["rebalance_date", "asset_id", "forward_excess_return"]].copy()
    y["target_rank"] = y.groupby("rebalance_date")["forward_excess_return"].rank(pct=True) - 0.5
    training = features.reset_index().merge(
        y[["rebalance_date", "asset_id", "target_rank"]],
        on=["rebalance_date", "asset_id"],
        how="inner",
    )
    if len(training) < max(3, len(factors)):
        return _zero_weight_frame(factors, "insufficient_training_rows", history_target)
    x = training[factors].fillna(0.0).to_numpy(dtype="float64")
    response = training["target_rank"].fillna(0.0).to_numpy(dtype="float64")
    xtx = x.T @ x
    penalty = float(ridge_alpha) * np.eye(len(factors))
    try:
        raw_weights = np.linalg.solve(xtx + penalty, x.T @ response)
    except np.linalg.LinAlgError:
        raw_weights = np.linalg.pinv(xtx + penalty) @ x.T @ response
    raw = pd.Series(raw_weights, index=factors)
    denominator = float(raw.abs().sum())
    weights = raw / denominator if denominator > 0.0 and np.isfinite(denominator) else raw * 0.0
    rows = []
    for factor_id in factors:
        rows.append(
            {
                "factor_id": factor_id,
                "mechanism_family": "",
                "weight": float(weights.loc[factor_id]),
                "raw_weight": float(raw.loc[factor_id]),
                "raw_shrunk_weight": float(raw.loc[factor_id]),
                "history_observation_count": int(history_target["rebalance_date"].nunique()),
                "rolling_icir": 0.0,
                "weight_status": "active",
                "estimation_window_start": _date_str(history_target["rebalance_date"].min()),
                "estimation_window_end": _date_str(history_target["rebalance_date"].max()),
                "return_visibility_cutoff": _date_str(history_target["target_return_visible_timestamp"].max()),
                **GUARD_VALUES,
            }
        )
    return pd.DataFrame(rows)


def _zero_weight_frame(
    factors: list[str],
    status: str,
    history_target: pd.DataFrame | None = None,
) -> pd.DataFrame:
    history_target = history_target if history_target is not None else pd.DataFrame()
    rows = []
    for factor_id in factors:
        rows.append(
            {
                "factor_id": factor_id,
                "mechanism_family": "",
                "weight": 0.0,
                "raw_weight": 0.0,
                "raw_shrunk_weight": 0.0,
                "history_observation_count": int(history_target["rebalance_date"].nunique()) if not history_target.empty else 0,
                "rolling_icir": 0.0,
                "weight_status": status,
                "estimation_window_start": _date_str(history_target["rebalance_date"].min()) if not history_target.empty else "",
                "estimation_window_end": _date_str(history_target["rebalance_date"].max()) if not history_target.empty else "",
                "return_visibility_cutoff": _date_str(history_target["target_return_visible_timestamp"].max())
                if not history_target.empty
                else "",
                **GUARD_VALUES,
            }
        )
    return pd.DataFrame(rows)


def _score_estimator(
    feature_wide: pd.DataFrame,
    members_by_date: dict[pd.Timestamp, pd.DataFrame],
    weights: pd.DataFrame,
    targets: pd.DataFrame,
    rebalance_date: pd.Timestamp,
    period: str,
    horizon: int,
) -> pd.DataFrame:
    members = members_by_date.get(pd.Timestamp(rebalance_date), pd.DataFrame(columns=["asset_id", "ticker"])).copy()
    if members.empty:
        return pd.DataFrame()
    factor_ids = weights["factor_id"].astype(str).tolist()
    weight_vector = weights.set_index("factor_id")["weight"].reindex(factor_ids).fillna(0.0).astype(float)
    try:
        current_values = feature_wide.xs(pd.Timestamp(rebalance_date), level="rebalance_date").reindex(
            members["asset_id"].astype(str)
        )
    except KeyError:
        current_values = pd.DataFrame(index=members["asset_id"].astype(str), columns=factor_ids, dtype="float64")
    current_values = current_values.reindex(columns=factor_ids)
    available = current_values.notna().astype(float)
    score_numerator = current_values.fillna(0.0).to_numpy(dtype="float64") @ weight_vector.to_numpy(dtype="float64")
    available_weight_abs = available.to_numpy(dtype="float64") @ weight_vector.abs().to_numpy(dtype="float64")
    scored = members.copy()
    scored["score_numerator"] = score_numerator
    scored["available_weight_abs"] = available_weight_abs
    scored["available_weight_abs"] = scored["available_weight_abs"].fillna(0.0)
    scored["score"] = np.where(scored["available_weight_abs"] > 0.0, scored["score_numerator"] / scored["available_weight_abs"], np.nan)
    target = targets[(targets["rebalance_date"] == rebalance_date) & (targets["horizon_months"] == int(horizon))]
    scored = scored.merge(
        target[["rebalance_date", "asset_id", "horizon_months", "forward_excess_return", "forward_return_available"]],
        on="asset_id",
        how="left",
    )
    scored["rebalance_date"] = rebalance_date
    scored["period"] = period
    scored["horizon_months"] = int(horizon)
    scored["coverage_state"] = np.where(scored["available_weight_abs"] > 0.0, "active_score", "explicit_abstain")
    scored["estimator_name"] = str(weights["estimator_name"].iloc[0])
    scored["estimator_family"] = str(weights["estimator_family"].iloc[0])
    scored["shrink_lambda"] = weights["shrink_lambda"].iloc[0]
    scored["ridge_alpha"] = weights["ridge_alpha"].iloc[0]
    scored["not_alpha_evidence"] = True
    scored["direct_q2_entry_allowed"] = False
    scored["forward_return_available"] = scored["forward_return_available"].fillna(False).astype(bool)
    return scored


def _rebalance_diagnostics(scores: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if scores.empty:
        return pd.DataFrame()
    keys = ["estimator_name", "estimator_family", "period", "horizon_months", "rebalance_date"]
    for key, group in scores.groupby(keys, dropna=False):
        estimator_name, estimator_family, period, horizon, rebalance_date = key
        eligible = group[(group["coverage_state"] == "active_score") & (group["forward_return_available"])][
            ["score", "forward_excess_return"]
        ].dropna()
        rank_ic = np.nan
        spread = np.nan
        if len(eligible) >= 2:
            ordered = eligible.sort_values("score", ascending=False)
            count = max(1, int(np.ceil(len(ordered) * 0.1)))
            spread = float(ordered.head(count)["forward_excess_return"].mean() - ordered.tail(count)["forward_excess_return"].mean())
            if ordered["score"].nunique() > 1 and ordered["forward_excess_return"].nunique() > 1:
                rank_ic = float(ordered["score"].corr(ordered["forward_excess_return"], method="spearman"))
        rows.append(
            {
                "estimator_name": estimator_name,
                "estimator_family": estimator_family,
                "period": period,
                "horizon_months": int(horizon),
                "rebalance_date": pd.Timestamp(rebalance_date),
                "rank_ic": rank_ic,
                "top_bottom_spread": spread,
                "eligible_name_count": int(len(eligible)),
                "not_alpha_evidence": True,
                "direct_q2_entry_allowed": False,
            }
        )
    return pd.DataFrame(rows)


def _placebo_scores_and_diagnostics(
    feature_wide: pd.DataFrame,
    members_by_date: dict[pd.Timestamp, pd.DataFrame],
    weights: pd.DataFrame,
    targets: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    placebo_weights = []
    for (_estimator, _date, _horizon), group in weights.groupby(["estimator_name", "rebalance_date", "horizon_months"]):
        shifted = group.sort_values("factor_id").copy()
        shifted["weight"] = shifted["weight"].shift(1).fillna(shifted["weight"].iloc[-1] if len(shifted) else 0.0)
        placebo_weights.append(shifted)
    placebo_weight_frame = pd.concat(placebo_weights, ignore_index=True) if placebo_weights else pd.DataFrame()
    score_rows = []
    for key, group in placebo_weight_frame.groupby(["estimator_name", "rebalance_date", "horizon_months"]):
        estimator, date, horizon = key
        period = str(group["period"].iloc[0])
        scored = _score_estimator(feature_wide, members_by_date, group, targets, pd.Timestamp(date), period, int(horizon))
        scored["estimator_name"] = estimator
        score_rows.append(scored)
    placebo_scores = pd.concat(score_rows, ignore_index=True) if score_rows else pd.DataFrame()
    return placebo_scores, _rebalance_diagnostics(placebo_scores)


def _placebo_summary(diagnostics: pd.DataFrame, placebo_diag: pd.DataFrame, weights: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for estimator, group in diagnostics[diagnostics["period"] == "test"].groupby("estimator_name"):
        placebo = placebo_diag[(placebo_diag["period"] == "test") & (placebo_diag["estimator_name"] == estimator)]
        live_spread = float(group["top_bottom_spread"].mean()) if not group.empty else np.nan
        placebo_spread = float(placebo["top_bottom_spread"].mean()) if not placebo.empty else np.nan
        live_rank_ic = float(group["rank_ic"].mean()) if not group.empty else np.nan
        placebo_rank_ic = float(placebo["rank_ic"].mean()) if not placebo.empty else np.nan
        corr = _mean_placebo_weight_correlation(weights[weights["estimator_name"] == estimator])
        rows.append(
            {
                "schema_version": "fd_weighting_placebo_comparison.v1",
                "estimator_name": estimator,
                "live_mean_rank_ic": live_rank_ic,
                "placebo_mean_rank_ic": placebo_rank_ic,
                "live_mean_spread": live_spread,
                "placebo_mean_spread": placebo_spread,
                "placebo_weight_correlation": corr,
                "beats_placebo": bool(
                    pd.notna(live_spread)
                    and pd.notna(placebo_spread)
                    and live_spread > placebo_spread
                    and pd.notna(live_rank_ic)
                    and pd.notna(placebo_rank_ic)
                    and live_rank_ic > placebo_rank_ic
                ),
                "not_alpha_evidence": True,
                "direct_q2_entry_allowed": False,
            }
        )
    return pd.DataFrame(rows)


def _estimator_comparison(
    diagnostics: pd.DataFrame,
    stability: pd.DataFrame,
    placebo_summary: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    test = diagnostics[diagnostics["period"] == "test"].copy()
    for estimator, group in test.groupby("estimator_name"):
        row = {
            "schema_version": "fd_weighting_estimator_comparison.v1",
            "estimator_name": estimator,
            "estimator_family": str(group["estimator_family"].iloc[0]),
            "mean_rank_ic_1m": _mean_metric(group, 1, "rank_ic"),
            "mean_rank_ic_3m": _mean_metric(group, 3, "rank_ic"),
            "spread_1m": _mean_metric(group, 1, "top_bottom_spread"),
            "spread_3m": _mean_metric(group, 3, "top_bottom_spread"),
            "rank_ic_tstat": _tstat(group["rank_ic"]),
            "spread_tstat": _tstat(group["top_bottom_spread"]),
            "not_alpha_evidence": True,
            "direct_q2_entry_allowed": False,
        }
        rows.append(row)
    comparison = pd.DataFrame(rows)
    if comparison.empty:
        return comparison
    stability_metrics = stability.drop(
        columns=["schema_version", "not_alpha_evidence", "direct_q2_entry_allowed"],
        errors="ignore",
    )
    comparison = comparison.merge(stability_metrics, on="estimator_name", how="left")
    comparison = comparison.merge(
        placebo_summary[["estimator_name", "placebo_weight_correlation", "beats_placebo"]],
        on="estimator_name",
        how="left",
    )
    for column in ["placebo_weight_correlation", "beats_placebo"]:
        if column not in comparison.columns:
            comparison[column] = np.nan if column != "beats_placebo" else False
    comparison["beats_placebo"] = comparison["beats_placebo"].fillna(False).astype(bool)
    comparison["_combined"] = comparison.apply(_combined_score, axis=1)
    comparison["not_alpha_evidence"] = True
    comparison["direct_q2_entry_allowed"] = False
    return comparison


def _stability_diagnostics(
    weights: pd.DataFrame,
    diagnostics: pd.DataFrame,
    metadata: pd.DataFrame,
    rolling_weights: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    family_map = metadata.set_index("factor_id")["mechanism_family"].to_dict()
    for estimator, group in weights[weights["period"] == "test"].groupby("estimator_name"):
        pivot = group.pivot_table(index=["horizon_months", "rebalance_date"], columns="factor_id", values="weight", aggfunc="last").fillna(0.0)
        abs_pivot = pivot.abs()
        turnover = _weight_turnover(pivot)
        entropy = _weight_entropy(abs_pivot)
        max_factor = float(abs_pivot.max(axis=1).max()) if not abs_pivot.empty else np.nan
        max_family = _max_family_weight(group, family_map)
        sign_flip = _sign_flip_rate(pivot)
        entry_exit = _entry_exit_rate(pivot)
        expected_corr = _expected_realized_ic_corr(group, diagnostics, rolling_weights)
        survival = _subperiod_survival_rate(diagnostics[diagnostics["estimator_name"] == estimator])
        bootstrap = _bootstrap_weight_stability(pivot)
        rows.append(
            {
                "schema_version": "fd_weight_stability_diagnostics.v1",
                "estimator_name": estimator,
                "weight_turnover": turnover,
                "weight_entropy": entropy,
                "max_single_factor_weight": max_factor,
                "max_family_weight": max_family,
                "sign_flip_rate": sign_flip,
                "factor_entry_exit_rate": entry_exit,
                "expected_vs_realized_ic_corr": expected_corr,
                "subperiod_survival_rate": survival,
                "bootstrap_weight_stability": bootstrap,
                "not_alpha_evidence": True,
                "direct_q2_entry_allowed": False,
            }
        )
    return pd.DataFrame(rows)


def _failure_diagnosis(comparison: pd.DataFrame, placebo_report: pd.DataFrame) -> dict[str, object]:
    placebo_status = _placebo_status(placebo_report)
    current = comparison[comparison["estimator_name"] == "rolling_icir_current"]
    equal = comparison[comparison["estimator_name"] == "equal_weight_all"]
    family = comparison[comparison["estimator_name"] == "family_equal_weight"]
    baseline_score = max(_combined_score(equal), _combined_score(family))
    learned = comparison[~comparison["estimator_name"].isin(["equal_weight_all", "family_equal_weight"])]
    pass_rows = learned[
        (learned.apply(_combined_score, axis=1) > baseline_score)
        & (learned["beats_placebo"])
        & (learned["sign_flip_rate"].fillna(1.0) <= 0.35)
        & (learned["weight_turnover"].fillna(99.0) <= 1.5)
        & (learned["subperiod_survival_rate"].fillna(0.0) >= 0.5)
    ]
    if current.empty:
        stability_rows = learned.iloc[0:0]
    else:
        stability_rows = learned[learned["weight_turnover"].fillna(99.0) < float(current["weight_turnover"].iloc[0])]
    all_fail_placebo_or_survival = bool(
        not comparison.empty
        and ((~comparison["beats_placebo"]) | (comparison["subperiod_survival_rate"].fillna(0.0) < 0.5)).all()
    )
    if not pass_rows.empty:
        decision = "pass"
    elif all_fail_placebo_or_survival:
        decision = "close"
    elif not stability_rows.empty:
        decision = "weak_pass"
    else:
        decision = "fail"
    rolling_failure = bool(
        placebo_status == "failed_placebo_gate"
        or (_combined_score(current) < max(_combined_score(equal), _combined_score(family)))
    )
    best = comparison.sort_values("_combined", ascending=False).iloc[0]["estimator_name"] if "_combined" in comparison.columns and not comparison.empty else ""
    return {
        "schema_version": "fd_weighting_failure_diagnosis.v1",
        "decision": decision,
        "recommended_next_action": "do_not_enter_allocator",
        "placebo_status": placebo_status,
        "best_estimator": str(best),
        "rolling_icir_overfit_noise_failure": rolling_failure,
        "allocator_entry_allowed": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "production_approval_claimed": False,
        "direct_q2_entry_allowed": False,
        "not_alpha_evidence": True,
    }


def _normalize_factor_panel(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    normalized["normalized_value"] = pd.to_numeric(normalized["normalized_value"], errors="coerce")
    if "mechanism_family" not in normalized.columns:
        normalized["mechanism_family"] = normalized.get("known_correlation_family", "unknown")
    if "known_correlation_family" not in normalized.columns:
        normalized["known_correlation_family"] = normalized["mechanism_family"]
    return normalized


def _feature_wide(factor_panel: pd.DataFrame, factor_ids: list[str]) -> pd.DataFrame:
    active = factor_panel[
        (factor_panel["coverage_status"] == "active_view")
        & (factor_panel["factor_id"].isin(factor_ids))
    ][["rebalance_date", "asset_id", "factor_id", "normalized_value"]].copy()
    wide = active.pivot_table(
        index=["rebalance_date", "asset_id"],
        columns="factor_id",
        values="normalized_value",
        aggfunc="last",
    ).reindex(columns=factor_ids)
    wide.index = wide.index.set_levels(
        [pd.to_datetime(wide.index.levels[0], errors="coerce"), wide.index.levels[1].astype(str)],
        level=["rebalance_date", "asset_id"],
    )
    return wide.sort_index()


def _members_by_date(factor_panel: pd.DataFrame) -> dict[pd.Timestamp, pd.DataFrame]:
    columns = ["rebalance_date", "asset_id"]
    if "ticker" in factor_panel.columns:
        columns.append("ticker")
    members = factor_panel[columns].drop_duplicates(["rebalance_date", "asset_id"]).copy()
    members["rebalance_date"] = pd.to_datetime(members["rebalance_date"], errors="coerce")
    members["asset_id"] = members["asset_id"].astype(str)
    if "ticker" not in members.columns:
        members["ticker"] = members["asset_id"]
    members["ticker"] = members["ticker"].fillna(members["asset_id"]).astype(str)
    return {
        pd.Timestamp(date): group[["asset_id", "ticker"]].sort_values("asset_id").reset_index(drop=True)
        for date, group in members.groupby("rebalance_date")
    }


def _normalize_rolling_weights(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    normalized["horizon_months"] = normalized["horizon_months"].astype(int)
    for column in ["weight", "rolling_icir", "history_observation_count"]:
        normalized[column] = pd.to_numeric(normalized.get(column, 0.0), errors="coerce").fillna(0.0)
    return normalized


def _normalize_score_panel(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["horizon_months"] = normalized["horizon_months"].astype(int)
    normalized["forward_excess_return"] = pd.to_numeric(normalized["forward_excess_return"], errors="coerce")
    normalized["forward_return_available"] = normalized["forward_return_available"].astype(str).str.lower().isin(
        {"true", "1", "yes"}
    )
    if "target_return_visible_timestamp" not in normalized.columns:
        normalized["target_return_visible_timestamp"] = normalized["rebalance_date"] + pd.offsets.MonthEnd(1)
    normalized["target_return_visible_timestamp"] = pd.to_datetime(normalized["target_return_visible_timestamp"], errors="coerce")
    return normalized


def _targets_from_score_panel(score_panel: pd.DataFrame) -> pd.DataFrame:
    return score_panel[
        [
            "rebalance_date",
            "period",
            "horizon_months",
            "asset_id",
            "forward_excess_return",
            "forward_return_available",
            "target_return_visible_timestamp",
        ]
    ].drop_duplicates()


def _factor_metadata(factor_panel: pd.DataFrame) -> pd.DataFrame:
    return (
        factor_panel[["factor_id", "mechanism_family", "known_correlation_family"]]
        .drop_duplicates("factor_id")
        .sort_values("factor_id")
        .reset_index(drop=True)
    )


def _current_icir_frame(
    rolling_weights: pd.DataFrame,
    metadata: pd.DataFrame,
    rebalance_date: pd.Timestamp,
    horizon: int,
) -> pd.DataFrame:
    current = rolling_weights[
        (rolling_weights["rebalance_date"] == pd.Timestamp(rebalance_date))
        & (rolling_weights["horizon_months"] == int(horizon))
    ].copy()
    current = metadata[["factor_id", "mechanism_family"]].merge(current, on="factor_id", how="left")
    for column in ["weight", "rolling_icir", "history_observation_count"]:
        current[column] = pd.to_numeric(current.get(column, 0.0), errors="coerce").fillna(0.0)
    current["raw_weight"] = current["rolling_icir"]
    current["raw_shrunk_weight"] = current["rolling_icir"]
    current["weight_status"] = current.get("weight_status", "active").fillna("missing_current_weight")
    for key, value in GUARD_VALUES.items():
        current[key] = value
    return current


def _mean_metric(frame: pd.DataFrame, horizon: int, column: str) -> float:
    rows = frame[frame["horizon_months"] == horizon]
    return float(rows[column].mean()) if not rows.empty else np.nan


def _tstat(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) < 2:
        return np.nan
    std = float(values.std(ddof=1))
    if std <= 0.0 or not np.isfinite(std):
        return np.nan
    return float(values.mean() / (std / np.sqrt(len(values))))


def _weight_turnover(pivot: pd.DataFrame) -> float:
    values = []
    for _horizon, group in pivot.groupby(level=0):
        values.extend(group.droplevel(0).sort_index().diff().abs().sum(axis=1).dropna().tolist())
    return float(np.mean(values)) if values else 0.0


def _weight_entropy(abs_pivot: pd.DataFrame) -> float:
    entropies = []
    for _idx, row in abs_pivot.iterrows():
        total = float(row.sum())
        if total <= 0.0:
            continue
        p = row[row > 0.0] / total
        denominator = np.log(len(row)) if len(row) > 1 else 1.0
        entropies.append(float(-(p * np.log(p)).sum() / denominator))
    return float(np.mean(entropies)) if entropies else 0.0


def _max_family_weight(group: pd.DataFrame, family_map: Mapping[str, str]) -> float:
    frame = group.copy()
    frame["family"] = frame["factor_id"].map(family_map).fillna("unknown")
    family = frame.groupby(["horizon_months", "rebalance_date", "family"])["weight"].apply(lambda s: s.abs().sum())
    return float(family.max()) if not family.empty else np.nan


def _sign_flip_rate(pivot: pd.DataFrame) -> float:
    flips = []
    for _horizon, group in pivot.groupby(level=0):
        signs = np.sign(group.droplevel(0).sort_index())
        previous = signs.shift(1)
        valid = (signs != 0) & (previous != 0)
        flips.extend(((signs != previous) & valid).to_numpy().ravel().tolist())
    return float(np.mean(flips)) if flips else 0.0


def _entry_exit_rate(pivot: pd.DataFrame) -> float:
    changes = []
    for _horizon, group in pivot.groupby(level=0):
        active = group.droplevel(0).sort_index().abs() > 1e-12
        changes.extend((active != active.shift(1)).iloc[1:].to_numpy().ravel().tolist())
    return float(np.mean(changes)) if changes else 0.0


def _expected_realized_ic_corr(group: pd.DataFrame, diagnostics: pd.DataFrame, rolling_weights: pd.DataFrame) -> float:
    icir = rolling_weights[["rebalance_date", "horizon_months", "factor_id", "rolling_icir"]].rename(
        columns={"rolling_icir": "reference_rolling_icir"}
    )
    merged = group.merge(icir, on=["rebalance_date", "horizon_months", "factor_id"], how="left")
    merged["expected_ic"] = merged["weight"] * pd.to_numeric(merged["reference_rolling_icir"], errors="coerce").fillna(0.0)
    expected = merged.groupby(["estimator_name", "rebalance_date", "horizon_months"], as_index=False)["expected_ic"].sum()
    realized = diagnostics[["estimator_name", "rebalance_date", "horizon_months", "rank_ic"]]
    both = expected.merge(realized, on=["estimator_name", "rebalance_date", "horizon_months"], how="inner").dropna()
    if len(both) < 3 or both["expected_ic"].nunique() <= 1 or both["rank_ic"].nunique() <= 1:
        return np.nan
    return float(both["expected_ic"].corr(both["rank_ic"]))


def _subperiod_survival_rate(frame: pd.DataFrame) -> float:
    test = frame[frame["period"] == "test"].sort_values("rebalance_date")
    if test.empty:
        return 0.0
    results = []
    for _horizon, group in test.groupby("horizon_months"):
        midpoint = max(1, len(group) // 2)
        chunks = [group.iloc[:midpoint], group.iloc[midpoint:]]
        for chunk in chunks:
            if len(chunk):
                results.append(float(chunk["top_bottom_spread"].mean()) > 0.0)
    return float(np.mean(results)) if results else 0.0


def _bootstrap_weight_stability(pivot: pd.DataFrame) -> float:
    if len(pivot) < 4:
        return np.nan
    ordered = pivot.sort_index()
    even = ordered.iloc[::2].mean(axis=0)
    odd = ordered.iloc[1::2].mean(axis=0)
    if even.nunique() <= 1 or odd.nunique() <= 1:
        return np.nan
    value = even.corr(odd)
    return float(value) if pd.notna(value) else np.nan


def _mean_placebo_weight_correlation(group: pd.DataFrame) -> float:
    values = []
    for (_date, _horizon), frame in group.groupby(["rebalance_date", "horizon_months"]):
        ordered = frame.sort_values("factor_id")["weight"].reset_index(drop=True)
        shifted = ordered.shift(1).fillna(ordered.iloc[-1] if len(ordered) else 0.0)
        if ordered.nunique() > 1 and shifted.nunique() > 1:
            values.append(float(ordered.corr(shifted)))
    return float(np.nanmean(values)) if values else np.nan


def _combined_score(row_or_frame: pd.Series | pd.DataFrame) -> float:
    if isinstance(row_or_frame, pd.DataFrame):
        if row_or_frame.empty:
            return -np.inf
        row = row_or_frame.iloc[0]
    else:
        row = row_or_frame
    values = [
        row.get("mean_rank_ic_1m", np.nan),
        row.get("mean_rank_ic_3m", np.nan),
        row.get("spread_1m", np.nan),
        row.get("spread_3m", np.nan),
    ]
    clean = [float(value) for value in values if pd.notna(value)]
    return float(np.mean(clean)) if clean else -np.inf


def _placebo_status(placebo_report: pd.DataFrame) -> str:
    test = placebo_report[placebo_report["period"] == "test"]
    live = test[test["test_name"] == "live_oos_score"]
    comparators = test[test["test_name"].isin(["shuffled_cross_section_placebo", "random_same_coverage_placebo", "rebalance_date_shifted_placebo"])]
    if live.empty or comparators.empty:
        return "insufficient_placebo_evidence"
    live_spread = float(live["mean_top_bottom_spread"].mean())
    live_rank = float(live["mean_rank_ic"].mean())
    comp_spread = float(comparators["mean_top_bottom_spread"].median())
    if live_spread > max(0.0, comp_spread) and live_rank > 0.0:
        return "passed_initial_placebo_gate"
    return "failed_placebo_gate"


def _date_str(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).date().isoformat()


def _render_report(
    failure: Mapping[str, object],
    comparison: pd.DataFrame,
    stability: pd.DataFrame,
) -> str:
    lines = [
        "# FD-R4.1 / FD-R5.2 Weighting Reliability Report",
        "",
        "not alpha evidence",
        "allocator entry: blocked",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "production approval: not claimed",
        "",
        f"- decision: {failure['decision']}",
        f"- placebo status: {failure['placebo_status']}",
        f"- rolling ICIR overfit/noise failure: {str(failure['rolling_icir_overfit_noise_failure']).lower()}",
        f"- best estimator: {failure['best_estimator']}",
        "",
        "## Estimator Comparison",
    ]
    if comparison.empty:
        lines.append("- no estimator comparison rows were produced")
    else:
        for row in comparison.sort_values("_combined", ascending=False).head(12).itertuples(index=False):
            lines.append(
                f"- {row.estimator_name}: 1m_ic={row.mean_rank_ic_1m:.6f}, "
                f"3m_ic={row.mean_rank_ic_3m:.6f}, spread_1m={row.spread_1m:.6f}, "
                f"spread_3m={row.spread_3m:.6f}, survival={row.subperiod_survival_rate:.3f}"
            )
    lines.extend(
        [
            "",
            "## Boundary",
            "- no allocator, Q1, Promotion Gate, Q2, Alpha Registry, broker/order/live workflow, or production approval path is opened",
            "",
        ]
    )
    return "\n".join(lines)
