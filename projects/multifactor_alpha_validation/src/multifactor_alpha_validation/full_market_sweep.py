from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FullMarketSweepResult:
    feature_cache_path: str
    pocket_grid_path: str
    template_grid_path: str
    placebo_top_pockets_path: str
    summary_path: str
    report_path: str
    validation_status: str
    decision_state: str


_WINDOWS = (1, 2, 3, 5, 7, 10, 15, 22, 30, 44)
_QUANTILES = (0.8, 0.9)


def run_full_market_multifactor_sweep(
    returns_panel_path: Path,
    output_dir: Path,
    *,
    top_n: int = 5,
    random_seed: int = 17,
) -> FullMarketSweepResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    feature_cache_path = output_dir / "full_market_feature_cache.csv"
    pocket_grid_path = output_dir / "full_market_pocket_grid.csv"
    template_grid_path = output_dir / "full_market_template_grid.csv"
    placebo_path = output_dir / "full_market_placebo_top_pockets.csv"
    summary_path = output_dir / "full_market_sweep_summary.json"
    report_path = output_dir / "full_market_sweep_report.md"

    raw_returns = _read_returns(returns_panel_path)
    if raw_returns.empty:
        feature_cache = pd.DataFrame(columns=_feature_columns())
        pocket_grid = pd.DataFrame(columns=_pocket_columns())
        template_grid = pd.DataFrame(columns=_template_columns())
        placebo = pd.DataFrame(columns=_placebo_columns())
        summary = _blocked_summary(returns_panel_path, "missing_returns_panel")
        _write_artifacts(
            feature_cache_path,
            pocket_grid_path,
            template_grid_path,
            placebo_path,
            summary_path,
            report_path,
            feature_cache,
            pocket_grid,
            template_grid,
            placebo,
            summary,
        )
        return _result(
            feature_cache_path,
            pocket_grid_path,
            template_grid_path,
            placebo_path,
            summary_path,
            report_path,
            validation_status="blocked",
            decision_state="blocked_data_coverage",
        )

    panel = _build_feature_and_label_panel(raw_returns)
    feature_cache = panel[_feature_columns()].dropna(how="all", subset=_feature_value_columns()).copy()
    pocket_grid = _leaf_pocket_grid(panel)
    template_grid = _template_grid(panel)
    combined = pd.concat(
        [
            pocket_grid.assign(search_kind="leaf", candidate_id=pocket_grid["pocket_id"]),
            template_grid.assign(search_kind="template", candidate_id=template_grid["template_id"]),
        ],
        ignore_index=True,
        sort=False,
    )
    combined = combined.sort_values(
        ["search_profile_score", "t_stat", "hit_rate", "sample_count"],
        ascending=[False, False, False, False],
    )
    top_candidates = combined.head(max(1, top_n)).copy()
    placebo = _placebo_top_pockets(panel, top_candidates, random_seed=random_seed)
    summary = _summary(
        returns_panel_path=returns_panel_path,
        raw_returns=raw_returns,
        feature_cache=feature_cache,
        pocket_grid=pocket_grid,
        template_grid=template_grid,
        top_candidates=top_candidates,
    )

    _write_artifacts(
        feature_cache_path,
        pocket_grid_path,
        template_grid_path,
        placebo_path,
        summary_path,
        report_path,
        feature_cache,
        pocket_grid,
        template_grid,
        placebo,
        summary,
    )
    return _result(
        feature_cache_path,
        pocket_grid_path,
        template_grid_path,
        placebo_path,
        summary_path,
        report_path,
        validation_status="evaluated",
        decision_state="full_market_sweep_diagnostic_only",
    )


def _read_returns(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    date_column = _first_existing(frame, ["date", "trade_date", "rebalance_date"])
    instrument_column = _first_existing(frame, ["instrument_id", "ticker", "permno", "symbol"])
    return_column = _first_existing(frame, ["return", "ret", "daily_return"])
    if date_column is None or instrument_column is None or return_column is None:
        return pd.DataFrame()
    out = frame[[date_column, instrument_column, return_column]].rename(
        columns={date_column: "date", instrument_column: "instrument_id", return_column: "return"}
    )
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["instrument_id"] = out["instrument_id"].astype(str)
    out["return"] = pd.to_numeric(out["return"], errors="coerce")
    out = out.dropna(subset=["date", "instrument_id", "return"])
    return out.sort_values(["instrument_id", "date"]).reset_index(drop=True)


def _first_existing(frame: pd.DataFrame, names: list[str]) -> str | None:
    for name in names:
        if name in frame.columns:
            return name
    return None


def _build_feature_and_label_panel(returns: pd.DataFrame) -> pd.DataFrame:
    panel = returns.sort_values(["instrument_id", "date"]).copy()
    grouped = panel.groupby("instrument_id", group_keys=False)
    panel["lag1_return"] = grouped["return"].shift(1)
    panel["momentum_5d"] = grouped["return"].transform(lambda series: series.rolling(5, min_periods=3).sum().shift(1))
    panel["momentum_10d"] = grouped["return"].transform(lambda series: series.rolling(10, min_periods=5).sum().shift(1))
    panel["momentum_20d"] = grouped["return"].transform(lambda series: series.rolling(20, min_periods=8).sum().shift(1))
    panel["momentum_40d"] = grouped["return"].transform(lambda series: series.rolling(40, min_periods=15).sum().shift(1))
    panel["reversal_1d"] = -panel["lag1_return"]
    panel["reversal_5d"] = -panel["momentum_5d"]
    panel["reversal_10d"] = -panel["momentum_10d"]
    panel["reversal_20d"] = -panel["momentum_20d"]
    panel["volatility_10d"] = grouped["return"].transform(lambda series: series.rolling(10, min_periods=5).std().shift(1))
    panel["volatility_20d"] = grouped["return"].transform(lambda series: series.rolling(20, min_periods=8).std().shift(1))
    panel["low_vol_10d"] = -panel["volatility_10d"]
    panel["low_vol_20d"] = -panel["volatility_20d"]
    panel["high_vol_10d"] = panel["volatility_10d"]
    panel["abs_lag1_return"] = panel["lag1_return"].abs()
    panel["abs_5d_return"] = panel["momentum_5d"].abs()
    panel["up_shock_reversal_score"] = np.where(panel["lag1_return"] > 0, panel["lag1_return"].abs(), np.nan)
    panel["down_shock_rebound_score"] = np.where(panel["lag1_return"] < 0, panel["lag1_return"].abs(), np.nan)
    panel["up_5d_shock_reversal_score"] = np.where(panel["momentum_5d"] > 0, panel["momentum_5d"].abs(), np.nan)
    panel["down_5d_shock_rebound_score"] = np.where(panel["momentum_5d"] < 0, panel["momentum_5d"].abs(), np.nan)
    for window in _WINDOWS:
        panel[f"label_{window}d"] = grouped["return"].transform(lambda series, w=window: _forward_sum(series, w))
    return panel.sort_values(["date", "instrument_id"]).reset_index(drop=True)


def _forward_sum(series: pd.Series, window: int) -> pd.Series:
    values = series.to_numpy(dtype=float)
    out = np.full(len(values), np.nan)
    for index in range(len(values)):
        end = index + window + 1
        if end <= len(values):
            out[index] = float(np.nansum(values[index + 1 : end]))
    return pd.Series(out, index=series.index)


def _leaf_pocket_grid(panel: pd.DataFrame) -> pd.DataFrame:
    feature_specs = [
        ("momentum_5d", "momentum_5d"),
        ("momentum_10d", "momentum_10d"),
        ("momentum_20d", "momentum_20d"),
        ("momentum_40d", "momentum_40d"),
        ("reversal_1d", "reversal_1d"),
        ("reversal_5d", "reversal_5d"),
        ("reversal_10d", "reversal_10d"),
        ("reversal_20d", "reversal_20d"),
        ("low_vol_10d", "low_vol_10d"),
        ("low_vol_20d", "low_vol_20d"),
        ("high_vol_10d", "high_vol_10d"),
        ("up_shock_reversal", "up_shock_reversal_score"),
        ("down_shock_rebound", "down_shock_rebound_score"),
        ("up_5d_shock_reversal", "up_5d_shock_reversal_score"),
        ("down_5d_shock_rebound", "down_5d_shock_rebound_score"),
    ]
    rows: list[dict[str, Any]] = []
    for pocket_id, feature in feature_specs:
        for side in ("top", "bottom"):
            for quantile in _QUANTILES:
                for window in _WINDOWS:
                    label = f"label_{window}d"
                    active = _select_by_date_quantile(panel, feature, side, quantile)
                    metrics = _profile_metrics(panel.loc[active, ["date", "instrument_id", label]].rename(columns={label: "label"}))
                    rows.append(
                        {
                            "schema_version": "full_market_leaf_pocket.v1",
                            "pocket_id": pocket_id,
                            "feature_id": feature,
                            "side": side,
                            "quantile": quantile,
                            "window": f"post_1_{window}",
                            **metrics,
                            "cost_capacity_status": "cost_capacity_inputs_unavailable",
                            "not_alpha_evidence": True,
                        }
                    )
    return pd.DataFrame(rows, columns=_pocket_columns()).sort_values(
        ["search_profile_score", "t_stat", "hit_rate"],
        ascending=[False, False, False],
    )


def _template_grid(panel: pd.DataFrame) -> pd.DataFrame:
    work, templates = _add_template_scores(panel)
    rows: list[dict[str, Any]] = []
    for template_id, components in templates.items():
        available = [component for component in components if component in work.columns]
        if not available:
            continue
        score_column = f"template_{template_id}"
        for side in ("top", "bottom"):
            for quantile in _QUANTILES:
                for window in _WINDOWS:
                    label = f"label_{window}d"
                    active = _select_by_date_quantile(work, score_column, side, quantile)
                    metrics = _profile_metrics(work.loc[active, ["date", "instrument_id", label]].rename(columns={label: "label"}))
                    rows.append(
                        {
                            "schema_version": "full_market_template_pocket.v1",
                            "template_id": template_id,
                            "component_features": "|".join(component.removeprefix("z_") for component in available),
                            "component_count": len(available),
                            "side": side,
                            "quantile": quantile,
                            "window": f"post_1_{window}",
                            **metrics,
                            "cost_capacity_status": "cost_capacity_inputs_unavailable",
                            "not_alpha_evidence": True,
                        }
                    )
    return pd.DataFrame(rows, columns=_template_columns()).sort_values(
        ["search_profile_score", "t_stat", "hit_rate"],
        ascending=[False, False, False],
    )


def _add_template_scores(panel: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    work = panel.copy()
    for column in _feature_value_columns():
        work[f"z_{column}"] = work.groupby("date")[column].transform(_zscore)
    templates = {
        "momentum_reversal_blend": ["z_momentum_5d", "z_reversal_1d"],
        "medium_momentum_reversal_blend": ["z_momentum_20d", "z_reversal_5d"],
        "low_vol_momentum": ["z_momentum_20d", "z_low_vol_10d"],
        "low_vol_long_momentum": ["z_momentum_40d", "z_low_vol_20d"],
        "low_vol_short_reversal": ["z_low_vol_10d", "z_reversal_1d"],
        "high_vol_short_reversal": ["z_high_vol_10d", "z_reversal_1d"],
        "shock_reversal": ["z_abs_lag1_return", "z_reversal_1d"],
        "five_day_shock_reversal": ["z_abs_5d_return", "z_reversal_5d"],
        "short_horizon_reversal": ["z_reversal_1d", "z_reversal_5d"],
        "medium_horizon_reversal": ["z_reversal_5d", "z_reversal_20d"],
        "down_shock_rebound_blend": ["z_down_shock_rebound_score", "z_reversal_1d"],
        "up_shock_reversal_blend": ["z_up_shock_reversal_score", "z_reversal_1d"],
        "down_5d_shock_rebound_blend": ["z_down_5d_shock_rebound_score", "z_reversal_5d"],
        "up_5d_shock_reversal_blend": ["z_up_5d_shock_reversal_score", "z_reversal_5d"],
    }
    for template_id, components in templates.items():
        available = [component for component in components if component in work.columns]
        if available:
            work[f"template_{template_id}"] = work[available].mean(axis=1)
    return work, templates


def _select_by_date_quantile(frame: pd.DataFrame, column: str, side: str, quantile: float) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    values = pd.to_numeric(frame[column], errors="coerce")
    if side == "top":
        thresholds = values.groupby(frame["date"]).transform(lambda series: series.quantile(quantile))
        return values.notna() & thresholds.notna() & (values >= thresholds)
    thresholds = values.groupby(frame["date"]).transform(lambda series: series.quantile(1.0 - quantile))
    return values.notna() & thresholds.notna() & (values <= thresholds)


def _profile_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return _empty_metrics()
    labels = pd.to_numeric(frame["label"], errors="coerce").dropna()
    if labels.empty:
        return _empty_metrics()
    count = int(labels.shape[0])
    mean = float(labels.mean())
    std = float(labels.std(ddof=1)) if count > 1 else 0.0
    t_stat = mean / (std / math.sqrt(count)) if std > 0 and count > 1 else 0.0
    hit_rate = float((labels > 0).mean())
    month_breadth = int(frame.loc[labels.index, "date"].dt.to_period("M").nunique())
    year_breadth = int(frame.loc[labels.index, "date"].dt.year.nunique())
    issuer_breadth = int(frame.loc[labels.index, "instrument_id"].nunique())
    concentration = float(labels.abs().nlargest(min(10, count)).sum() / max(labels.abs().sum(), 1e-12))
    profile_score = mean * math.sqrt(max(month_breadth, 1)) * max(hit_rate, 0.01)
    return {
        "sample_count": count,
        "mean_return": _round(mean),
        "median_return": _round(float(labels.median())),
        "t_stat": _round(t_stat),
        "hit_rate": _round(hit_rate),
        "month_breadth": month_breadth,
        "year_breadth": year_breadth,
        "issuer_breadth": issuer_breadth,
        "top10_abs_return_concentration": _round(concentration),
        "search_profile_score": _round(profile_score),
    }


def _empty_metrics() -> dict[str, Any]:
    return {
        "sample_count": 0,
        "mean_return": 0.0,
        "median_return": 0.0,
        "t_stat": 0.0,
        "hit_rate": 0.0,
        "month_breadth": 0,
        "year_breadth": 0,
        "issuer_breadth": 0,
        "top10_abs_return_concentration": 0.0,
        "search_profile_score": 0.0,
    }


def _placebo_top_pockets(panel: pd.DataFrame, top_candidates: pd.DataFrame, *, random_seed: int) -> pd.DataFrame:
    if top_candidates.empty:
        return pd.DataFrame(columns=_placebo_columns())
    rng = np.random.default_rng(random_seed)
    top = top_candidates.iloc[0]
    work, _templates = _add_template_scores(panel)
    window = int(str(top["window"]).split("_")[-1])
    label = f"label_{window}d"
    feature = _candidate_feature(top)
    side = str(top["side"])
    quantile = float(top["quantile"])
    active = _select_by_date_quantile(work, feature, side, quantile)
    selected = work.loc[active, ["date", "instrument_id", label]].rename(columns={label: "label"}).dropna()
    rows: list[dict[str, Any]] = []
    for placebo_type, values in {
        "same_coverage_random_top": rng.permutation(selected["label"].to_numpy(dtype=float)) if not selected.empty else np.array([]),
        "shifted_date_top": selected["label"].shift(5).to_numpy(dtype=float) if not selected.empty else np.array([]),
    }.items():
        placebo_frame = selected[["date", "instrument_id"]].copy()
        placebo_frame["label"] = values
        metrics = _profile_metrics(placebo_frame)
        rows.append(
            {
                "schema_version": "full_market_sweep_placebo.v1",
                "placebo_type": placebo_type,
                "source_candidate_id": str(top["candidate_id"]),
                **metrics,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_placebo_columns())


def _candidate_feature(row: pd.Series) -> str:
    if str(row.get("search_kind")) == "template":
        return f"template_{row['candidate_id']}"
    feature = str(row.get("feature_id", ""))
    return feature


def _summary(
    *,
    returns_panel_path: Path,
    raw_returns: pd.DataFrame,
    feature_cache: pd.DataFrame,
    pocket_grid: pd.DataFrame,
    template_grid: pd.DataFrame,
    top_candidates: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "schema_version": "full_market_multifactor_sweep_summary.v1",
        "validation_status": "evaluated",
        "decision_state": "full_market_sweep_diagnostic_only",
        "returns_panel_path": str(returns_panel_path),
        "full_market_scope": True,
        "return_row_count": int(len(raw_returns)),
        "instrument_count": int(raw_returns["instrument_id"].nunique()),
        "feature_cache_row_count": int(len(feature_cache)),
        "search_burden": {
            "searched_pocket_count": int(len(pocket_grid)),
            "searched_template_count": int(len(template_grid)),
            "searched_total_count": int(len(pocket_grid) + len(template_grid)),
            "windows": list(_WINDOWS),
            "quantiles": list(_QUANTILES),
        },
        "top_candidates": _top_candidates(top_candidates),
        "cost_capacity_inputs_available": False,
        "fabricated_features": False,
        "fabricated_capacity": False,
        "expected_return_panel_written": False,
        "d3_charter_allowed": False,
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "alpha_registry_update_allowed": False,
        "non_claims": _non_claims(),
    }


def _blocked_summary(returns_panel_path: Path, reason: str) -> dict[str, Any]:
    return {
        "schema_version": "full_market_multifactor_sweep_summary.v1",
        "validation_status": "blocked",
        "decision_state": "blocked_data_coverage",
        "unavailable_reason": reason,
        "returns_panel_path": str(returns_panel_path),
        "full_market_scope": True,
        "search_burden": {"searched_pocket_count": 0, "searched_template_count": 0, "searched_total_count": 0},
        "fabricated_features": False,
        "fabricated_capacity": False,
        "expected_return_panel_written": False,
        "d3_charter_allowed": False,
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "alpha_registry_update_allowed": False,
        "non_claims": _non_claims(),
    }


def _top_candidates(top_candidates: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in top_candidates.itertuples(index=False):
        rows.append(
            {
                "candidate_id": str(row.candidate_id),
                "search_kind": str(row.search_kind),
                "window": str(row.window),
                "mean_return": float(row.mean_return),
                "t_stat": float(row.t_stat),
                "hit_rate": float(row.hit_rate),
                "month_breadth": int(row.month_breadth),
                "issuer_breadth": int(row.issuer_breadth),
            }
        )
    return rows


def _write_artifacts(
    feature_cache_path: Path,
    pocket_grid_path: Path,
    template_grid_path: Path,
    placebo_path: Path,
    summary_path: Path,
    report_path: Path,
    feature_cache: pd.DataFrame,
    pocket_grid: pd.DataFrame,
    template_grid: pd.DataFrame,
    placebo: pd.DataFrame,
    summary: dict[str, Any],
) -> None:
    feature_cache.to_csv(feature_cache_path, index=False)
    pocket_grid.to_csv(pocket_grid_path, index=False)
    template_grid.to_csv(template_grid_path, index=False)
    placebo.to_csv(placebo_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(summary), encoding="utf-8")


def _render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Full-Market Multifactor Sweep",
        "",
        "This is diagnostic only. It is an E0 overfit/discovery lab, not alpha evidence.",
        "",
        "Measurement Spec is not written. D3, Q1, Q2, OR optimization, Alpha Registry, paper/live, broker/order, and production paths remain closed.",
        "",
        "Q2 remains closed. Any candidate from this sweep must be separately frozen and rerun through locked validation before formal review.",
        "",
        f"Decision state: `{summary['decision_state']}`",
        f"Search burden: `{summary.get('search_burden', {})}`",
        "",
    ]
    if summary.get("top_candidates"):
        lines.append("## Top Diagnostic Candidates")
        for candidate in summary["top_candidates"]:
            lines.append(
                f"- `{candidate['candidate_id']}` `{candidate['window']}`: "
                f"mean `{candidate['mean_return']}`, t-stat `{candidate['t_stat']}`, "
                f"hit rate `{candidate['hit_rate']}`."
            )
    return "\n".join(lines) + "\n"


def _result(
    feature_cache_path: Path,
    pocket_grid_path: Path,
    template_grid_path: Path,
    placebo_path: Path,
    summary_path: Path,
    report_path: Path,
    *,
    validation_status: str,
    decision_state: str,
) -> FullMarketSweepResult:
    return FullMarketSweepResult(
        feature_cache_path=str(feature_cache_path),
        pocket_grid_path=str(pocket_grid_path),
        template_grid_path=str(template_grid_path),
        placebo_top_pockets_path=str(placebo_path),
        summary_path=str(summary_path),
        report_path=str(report_path),
        validation_status=validation_status,
        decision_state=decision_state,
    )


def _zscore(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    std = numeric.std(ddof=0)
    if not std or pd.isna(std):
        return pd.Series(np.nan, index=series.index)
    return (numeric - numeric.mean()) / std


def _round(value: float) -> float:
    return round(float(value), 10)


def _non_claims() -> dict[str, bool]:
    return {
        "alpha_evidence": False,
        "d3_approval": False,
        "q1_entry": False,
        "q2_entry": False,
        "or_optimizer": False,
        "alpha_registry": False,
        "paper_canary": False,
        "live_trading": False,
        "broker_order_workflow": False,
        "production_approval": False,
    }


def _feature_value_columns() -> list[str]:
    return [
        "lag1_return",
        "momentum_5d",
        "momentum_10d",
        "momentum_20d",
        "momentum_40d",
        "reversal_1d",
        "reversal_5d",
        "reversal_10d",
        "reversal_20d",
        "volatility_10d",
        "volatility_20d",
        "low_vol_10d",
        "low_vol_20d",
        "high_vol_10d",
        "abs_lag1_return",
        "abs_5d_return",
        "up_shock_reversal_score",
        "down_shock_rebound_score",
        "up_5d_shock_reversal_score",
        "down_5d_shock_rebound_score",
    ]


def _feature_columns() -> list[str]:
    return ["date", "instrument_id", *_feature_value_columns()]


def _pocket_columns() -> list[str]:
    return [
        "schema_version",
        "pocket_id",
        "feature_id",
        "side",
        "quantile",
        "window",
        "sample_count",
        "mean_return",
        "median_return",
        "t_stat",
        "hit_rate",
        "month_breadth",
        "year_breadth",
        "issuer_breadth",
        "top10_abs_return_concentration",
        "search_profile_score",
        "cost_capacity_status",
        "not_alpha_evidence",
    ]


def _template_columns() -> list[str]:
    return [
        "schema_version",
        "template_id",
        "component_features",
        "component_count",
        "side",
        "quantile",
        "window",
        "sample_count",
        "mean_return",
        "median_return",
        "t_stat",
        "hit_rate",
        "month_breadth",
        "year_breadth",
        "issuer_breadth",
        "top10_abs_return_concentration",
        "search_profile_score",
        "cost_capacity_status",
        "not_alpha_evidence",
    ]


def _placebo_columns() -> list[str]:
    return [
        "schema_version",
        "placebo_type",
        "source_candidate_id",
        "sample_count",
        "mean_return",
        "median_return",
        "t_stat",
        "hit_rate",
        "month_breadth",
        "year_breadth",
        "issuer_breadth",
        "top10_abs_return_concentration",
        "search_profile_score",
        "not_alpha_evidence",
    ]
