"""FD-R4 true rolling OOS validation on FD-R3 real factor panels."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from .oos_splitter import build_rolling_oos_splits
from .real_factor_replay import (
    _date_str,
    _detect_frequency,
    _load_manifest,
    _load_section_csv,
    _next_trading_date_map,
    _normalize_benchmark,
    _normalize_prices,
)


@dataclass(frozen=True)
class FDRealRollingOOSResult:
    """Artifacts and summary for FD-R4."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


WEIGHT_COLUMNS = [
    "schema_version",
    "rebalance_date",
    "period",
    "horizon_months",
    "estimation_window_start",
    "estimation_window_end",
    "return_visibility_cutoff",
    "factor_id",
    "history_observation_count",
    "rolling_ic_mean",
    "rolling_ic_std",
    "rolling_icir",
    "weight",
    "weight_status",
    "uses_full_sample_icir",
    "future_universe_used",
    "future_normalization_used",
    "post_period_factor_selection_used",
    "not_alpha_evidence",
]

SCORE_COLUMNS = [
    "schema_version",
    "rebalance_date",
    "period",
    "horizon_months",
    "asset_id",
    "ticker",
    "score",
    "coverage_state",
    "abstain_reason",
    "available_weight_abs",
    "forward_asset_return",
    "forward_benchmark_return",
    "forward_excess_return",
    "forward_return_available",
    "signal_timestamp",
    "visibility_timestamp",
    "tradable_timestamp",
    "target_return_visible_timestamp",
    "uses_full_sample_icir",
    "future_universe_used",
    "future_normalization_used",
    "post_period_factor_selection_used",
    "no_view_is_not_zero_alpha",
    "not_alpha_evidence",
]

DECILE_COLUMNS = [
    "schema_version",
    "rebalance_date",
    "period",
    "horizon_months",
    "eligible_name_count",
    "top_decile_count",
    "bottom_decile_count",
    "top_decile_excess_return",
    "bottom_decile_excess_return",
    "top_bottom_spread",
    "rank_ic",
    "uses_full_sample_icir",
    "future_universe_used",
    "future_normalization_used",
    "post_period_factor_selection_used",
    "not_alpha_evidence",
]


def run_real_rolling_oos(
    manifest_path: str | Path,
    factor_panel_path: str | Path,
    output_dir: str | Path,
    train_window_months: int = 36,
    validation_window_months: int = 12,
    horizons: Iterable[int] = (1, 3),
    min_ic_observations: int = 6,
    min_cross_section: int = 5,
) -> FDRealRollingOOSResult:
    """Run prior-history-only rolling ICIR validation on an FD-R3 panel."""

    manifest_file = Path(manifest_path)
    factor_panel_file = Path(factor_panel_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    horizons_tuple = tuple(int(horizon) for horizon in horizons)
    manifest = _load_manifest(manifest_file)
    prices = _normalize_prices(_load_section_csv(manifest, manifest_file, "prices"))
    benchmark = _normalize_benchmark(_load_section_csv(manifest, manifest_file, "benchmark"))
    frequency = _detect_frequency(prices)
    if frequency != "daily":
        raise ValueError("FD-R4 requires daily price-volume data")

    factor_panel = _normalize_factor_panel(pd.read_csv(factor_panel_file))
    factor_ids = sorted(factor_panel["factor_id"].dropna().astype(str).unique())
    signal_dates = [pd.Timestamp(date) for date in sorted(factor_panel["rebalance_date"].dropna().unique())]
    close = prices.pivot_table(index="date", columns="asset_id", values="adjusted_close", aggfunc="last").sort_index()
    qqq_close = benchmark.drop_duplicates("date").set_index("date")["adjusted_close"].sort_index()
    next_trading_date = _next_trading_date_map(pd.Index(close.index), signal_dates)

    targets = _build_forward_targets(factor_panel, close, qqq_close, signal_dates, next_trading_date, horizons_tuple)
    rank_ic = _build_rank_ic_table(factor_panel, targets, min_cross_section)
    splits = build_rolling_oos_splits(
        signal_dates,
        train_window_months=train_window_months,
        validation_window_months=validation_window_months,
        max_horizon_months=max(horizons_tuple),
    )

    weight_rows: list[dict[str, object]] = []
    score_rows: list[dict[str, object]] = []
    decile_rows: list[dict[str, object]] = []

    for split in splits.itertuples(index=False):
        rebalance_date = pd.Timestamp(split.rebalance_date)
        period = str(split.period)
        current_panel = factor_panel[factor_panel["rebalance_date"] == rebalance_date]
        for horizon in horizons_tuple:
            weights = _estimate_weights(
                rank_ic=rank_ic,
                rebalance_date=rebalance_date,
                horizon=horizon,
                factor_ids=factor_ids,
                train_window_months=train_window_months,
                min_ic_observations=min_ic_observations,
            )
            weight_rows.extend(_weight_output_rows(weights, rebalance_date, period, horizon))

            scores = _score_current_rebalance(current_panel, weights, rebalance_date, period, horizon)
            horizon_targets = targets[
                (targets["rebalance_date"] == rebalance_date) & (targets["horizon_months"] == horizon)
            ]
            scores = scores.merge(
                horizon_targets,
                on=["rebalance_date", "asset_id", "horizon_months"],
                how="left",
                suffixes=("", "_target"),
            )
            scores["forward_return_available"] = scores["forward_return_available"].fillna(False).astype(bool)
            for column in ("forward_asset_return", "forward_benchmark_return", "forward_excess_return"):
                if column not in scores.columns:
                    scores[column] = np.nan
            scores["target_return_visible_timestamp"] = scores["target_return_visible_timestamp"].fillna("")
            score_rows.extend(scores[SCORE_COLUMNS].to_dict(orient="records"))
            decile_rows.append(_decile_spread_row(scores, rebalance_date, period, horizon))

    weights_df = pd.DataFrame(weight_rows, columns=WEIGHT_COLUMNS)
    scores_df = pd.DataFrame(score_rows, columns=SCORE_COLUMNS)
    deciles_df = pd.DataFrame(decile_rows, columns=DECILE_COLUMNS)

    artifacts = {
        "rolling_icir_real": output_path / "rolling_icir_real.csv",
        "oos_factor_score_panel_real": output_path / "oos_factor_score_panel_real.csv",
        "oos_decile_spread_real": output_path / "oos_decile_spread_real.csv",
        "oos_validation_report": output_path / "oos_validation_report.md",
        "oos_validation_summary": output_path / "oos_validation_summary.json",
    }
    weights_df.to_csv(artifacts["rolling_icir_real"], index=False)
    scores_df.to_csv(artifacts["oos_factor_score_panel_real"], index=False)
    deciles_df.to_csv(artifacts["oos_decile_spread_real"], index=False)

    summary = {
        "schema_version": "fd_real_rolling_oos_summary.v1",
        "stage": "FD-R4",
        "dataset_frequency": frequency,
        "manifest_path": str(manifest_file),
        "factor_panel_path": str(factor_panel_file),
        "train_window_months": train_window_months,
        "validation_window_months": validation_window_months,
        "horizons_months": list(horizons_tuple),
        "factor_count": len(factor_ids),
        "rebalance_count": int(len(splits)),
        "validation_rebalance_count": int((splits["period"] == "validation").sum()) if not splits.empty else 0,
        "test_rebalance_count": int((splits["period"] == "test").sum()) if not splits.empty else 0,
        "score_row_count": int(len(scores_df)),
        "decile_row_count": int(len(deciles_df)),
        "uses_full_sample_icir": False,
        "future_universe_used": False,
        "future_normalization_used": False,
        "post_period_factor_selection_used": False,
        "allocator_ran": False,
        "alpha_success_claimed": False,
        "direct_q2_entry_allowed": False,
        "production_approval_claimed": False,
        "no_view_is_not_zero_alpha": True,
        "not_alpha_evidence": True,
    }
    artifacts["oos_validation_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["oos_validation_report"].write_text(_render_report(summary, deciles_df), encoding="utf-8")

    return FDRealRollingOOSResult(summary=summary, artifacts=artifacts)


def _normalize_factor_panel(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    normalized["normalized_value"] = pd.to_numeric(normalized["normalized_value"], errors="coerce")
    for column in ("signal_timestamp", "visibility_timestamp", "tradable_timestamp"):
        normalized[column] = normalized[column].astype(str)
    return normalized


def _build_forward_targets(
    factor_panel: pd.DataFrame,
    close: pd.DataFrame,
    qqq_close: pd.Series,
    signal_dates: list[pd.Timestamp],
    next_trading_date: Mapping[pd.Timestamp, pd.Timestamp],
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    members = factor_panel[["rebalance_date", "asset_id", "ticker"]].drop_duplicates()
    members_by_date = {date: group for date, group in members.groupby("rebalance_date")}
    qqq = qqq_close.reindex(close.index).ffill()
    rows: list[dict[str, object]] = []
    for position, signal_date in enumerate(signal_dates):
        if signal_date not in next_trading_date:
            continue
        entry_date = next_trading_date[signal_date]
        for horizon in horizons:
            exit_position = position + horizon
            if exit_position >= len(signal_dates):
                continue
            exit_signal_date = signal_dates[exit_position]
            if exit_signal_date not in next_trading_date:
                continue
            exit_date = next_trading_date[exit_signal_date]
            benchmark_return = np.nan
            if entry_date in qqq.index and exit_date in qqq.index and pd.notna(qqq.loc[entry_date]) and pd.notna(qqq.loc[exit_date]):
                benchmark_return = float(qqq.loc[exit_date] / qqq.loc[entry_date] - 1.0)
            members_for_date = members_by_date.get(signal_date, pd.DataFrame(columns=["asset_id", "ticker"]))
            entry_prices = close.loc[entry_date] if entry_date in close.index else pd.Series(dtype="float64")
            exit_prices = close.loc[exit_date] if exit_date in close.index else pd.Series(dtype="float64")
            target_visible = f"{_date_str(exit_date + pd.Timedelta(days=1))}T00:00:00"
            for member in members_for_date.itertuples(index=False):
                asset_id = str(member.asset_id)
                asset_return = np.nan
                if (
                    asset_id in entry_prices.index
                    and asset_id in exit_prices.index
                    and pd.notna(entry_prices[asset_id])
                    and pd.notna(exit_prices[asset_id])
                    and float(entry_prices[asset_id]) > 0.0
                ):
                    asset_return = float(exit_prices[asset_id] / entry_prices[asset_id] - 1.0)
                forward_available = bool(pd.notna(asset_return) and pd.notna(benchmark_return))
                rows.append(
                    {
                        "rebalance_date": signal_date,
                        "asset_id": asset_id,
                        "horizon_months": horizon,
                        "forward_asset_return": asset_return,
                        "forward_benchmark_return": benchmark_return,
                        "forward_excess_return": asset_return - benchmark_return if forward_available else np.nan,
                        "forward_return_available": forward_available,
                        "target_return_visible_date": exit_date + pd.Timedelta(days=1),
                        "target_return_visible_timestamp": target_visible,
                    }
                )
    return pd.DataFrame(rows)


def _build_rank_ic_table(factor_panel: pd.DataFrame, targets: pd.DataFrame, min_cross_section: int) -> pd.DataFrame:
    active = factor_panel[factor_panel["coverage_status"] == "active_view"][
        ["rebalance_date", "asset_id", "factor_id", "normalized_value"]
    ]
    merged = active.merge(targets, on=["rebalance_date", "asset_id"], how="inner")
    merged = merged[merged["forward_return_available"]]
    rows: list[dict[str, object]] = []
    for (date, horizon, factor_id), group in merged.groupby(["rebalance_date", "horizon_months", "factor_id"]):
        clean = group[["normalized_value", "forward_excess_return"]].dropna()
        rank_ic = np.nan
        if (
            len(clean) >= min_cross_section
            and clean["normalized_value"].nunique(dropna=True) > 1
            and clean["forward_excess_return"].nunique(dropna=True) > 1
        ):
            rank_ic = clean["normalized_value"].corr(clean["forward_excess_return"], method="spearman")
        rows.append(
            {
                "rebalance_date": pd.Timestamp(date),
                "horizon_months": int(horizon),
                "factor_id": str(factor_id),
                "rank_ic": float(rank_ic) if pd.notna(rank_ic) else np.nan,
                "cross_section_count": int(len(clean)),
                "return_visible_date": pd.to_datetime(group["target_return_visible_date"]).max(),
            }
        )
    return pd.DataFrame(rows)


def _estimate_weights(
    rank_ic: pd.DataFrame,
    rebalance_date: pd.Timestamp,
    horizon: int,
    factor_ids: list[str],
    train_window_months: int,
    min_ic_observations: int,
) -> pd.DataFrame:
    history = rank_ic[
        (rank_ic["horizon_months"] == horizon)
        & (rank_ic["rebalance_date"] < rebalance_date)
        & (pd.to_datetime(rank_ic["return_visible_date"]) < rebalance_date)
    ].copy()
    history_dates = sorted(history["rebalance_date"].dropna().unique())[-train_window_months:]
    history = history[history["rebalance_date"].isin(history_dates)]
    raw_rows: list[dict[str, object]] = []
    for factor_id in factor_ids:
        series = pd.to_numeric(history.loc[history["factor_id"] == factor_id, "rank_ic"], errors="coerce").dropna()
        ic_mean = float(series.mean()) if len(series) else np.nan
        ic_std = float(series.std(ddof=1)) if len(series) > 1 else np.nan
        if len(series) < min_ic_observations:
            icir = 0.0
            weight_status = "insufficient_visible_history"
        elif pd.notna(ic_std) and ic_std > 0.0:
            icir = float(ic_mean / ic_std)
            weight_status = "active"
        else:
            icir = 0.0
            weight_status = "zero_or_undefined_ic_std"
        raw_rows.append(
            {
                "factor_id": factor_id,
                "history_observation_count": int(len(series)),
                "rolling_ic_mean": ic_mean,
                "rolling_ic_std": ic_std,
                "rolling_icir": icir,
                "raw_weight": icir,
                "weight_status": weight_status,
            }
        )
    weights = pd.DataFrame(raw_rows)
    denominator = float(weights["raw_weight"].abs().sum())
    weights["weight"] = weights["raw_weight"] / denominator if denominator > 0.0 else 0.0
    if not history.empty:
        weights["estimation_window_start"] = _date_str(history["rebalance_date"].min())
        weights["estimation_window_end"] = _date_str(history["rebalance_date"].max())
        weights["return_visibility_cutoff"] = _date_str(pd.to_datetime(history["return_visible_date"]).max())
    else:
        weights["estimation_window_start"] = ""
        weights["estimation_window_end"] = ""
        weights["return_visibility_cutoff"] = ""
    return weights


def _weight_output_rows(weights: pd.DataFrame, rebalance_date: pd.Timestamp, period: str, horizon: int) -> list[dict[str, object]]:
    rows = []
    for row in weights.itertuples(index=False):
        rows.append(
            {
                "schema_version": "fd_real_rolling_icir.v1",
                "rebalance_date": _date_str(rebalance_date),
                "period": period,
                "horizon_months": horizon,
                "estimation_window_start": row.estimation_window_start,
                "estimation_window_end": row.estimation_window_end,
                "return_visibility_cutoff": row.return_visibility_cutoff,
                "factor_id": row.factor_id,
                "history_observation_count": row.history_observation_count,
                "rolling_ic_mean": row.rolling_ic_mean,
                "rolling_ic_std": row.rolling_ic_std,
                "rolling_icir": row.rolling_icir,
                "weight": row.weight,
                "weight_status": row.weight_status,
                "uses_full_sample_icir": False,
                "future_universe_used": False,
                "future_normalization_used": False,
                "post_period_factor_selection_used": False,
                "not_alpha_evidence": True,
            }
        )
    return rows


def _score_current_rebalance(
    current_panel: pd.DataFrame,
    weights: pd.DataFrame,
    rebalance_date: pd.Timestamp,
    period: str,
    horizon: int,
) -> pd.DataFrame:
    members = current_panel[["asset_id", "ticker", "signal_timestamp", "visibility_timestamp", "tradable_timestamp"]].drop_duplicates(
        "asset_id"
    )
    active = current_panel[current_panel["coverage_status"] == "active_view"].copy()
    active = active.merge(weights[["factor_id", "weight"]], on="factor_id", how="left")
    active["weight"] = pd.to_numeric(active["weight"], errors="coerce").fillna(0.0)
    active["weighted_value"] = active["normalized_value"] * active["weight"]
    active["available_weight_abs_piece"] = active["weight"].abs()
    grouped = active.groupby("asset_id", as_index=False).agg(
        score_numerator=("weighted_value", "sum"),
        available_weight_abs=("available_weight_abs_piece", "sum"),
    )
    scored = members.merge(grouped, on="asset_id", how="left")
    scored["available_weight_abs"] = scored["available_weight_abs"].fillna(0.0)
    scored["score"] = np.where(
        scored["available_weight_abs"] > 0.0,
        scored["score_numerator"] / scored["available_weight_abs"],
        np.nan,
    )
    scored["coverage_state"] = np.where(scored["available_weight_abs"] > 0.0, "active_score", "explicit_abstain")
    scored["abstain_reason"] = np.where(scored["available_weight_abs"] > 0.0, "", "insufficient_visible_ic_history")
    scored["schema_version"] = "fd_real_oos_factor_score.v1"
    scored["rebalance_date"] = rebalance_date
    scored["period"] = period
    scored["horizon_months"] = horizon
    scored["uses_full_sample_icir"] = False
    scored["future_universe_used"] = False
    scored["future_normalization_used"] = False
    scored["post_period_factor_selection_used"] = False
    scored["no_view_is_not_zero_alpha"] = True
    scored["not_alpha_evidence"] = True
    return scored


def _decile_spread_row(scores: pd.DataFrame, rebalance_date: pd.Timestamp, period: str, horizon: int) -> dict[str, object]:
    eligible = scores[(scores["coverage_state"] == "active_score") & (scores["forward_return_available"])].copy()
    eligible = eligible[["score", "forward_excess_return"]].dropna()
    row = {
        "schema_version": "fd_real_oos_decile_spread.v1",
        "rebalance_date": _date_str(rebalance_date),
        "period": period,
        "horizon_months": horizon,
        "eligible_name_count": int(len(eligible)),
        "top_decile_count": 0,
        "bottom_decile_count": 0,
        "top_decile_excess_return": np.nan,
        "bottom_decile_excess_return": np.nan,
        "top_bottom_spread": np.nan,
        "rank_ic": np.nan,
        "uses_full_sample_icir": False,
        "future_universe_used": False,
        "future_normalization_used": False,
        "post_period_factor_selection_used": False,
        "not_alpha_evidence": True,
    }
    if len(eligible) < 2:
        return row
    count = max(1, int(np.ceil(len(eligible) * 0.1)))
    ordered = eligible.sort_values("score", ascending=False)
    top = ordered.head(count)
    bottom = ordered.tail(count)
    rank_ic = np.nan
    if ordered["score"].nunique(dropna=True) > 1 and ordered["forward_excess_return"].nunique(dropna=True) > 1:
        rank_ic = ordered["score"].corr(ordered["forward_excess_return"], method="spearman")
    row.update(
        {
            "top_decile_count": int(len(top)),
            "bottom_decile_count": int(len(bottom)),
            "top_decile_excess_return": float(top["forward_excess_return"].mean()),
            "bottom_decile_excess_return": float(bottom["forward_excess_return"].mean()),
            "top_bottom_spread": float(top["forward_excess_return"].mean() - bottom["forward_excess_return"].mean()),
            "rank_ic": float(rank_ic) if pd.notna(rank_ic) else np.nan,
        }
    )
    return row


def _render_report(summary: Mapping[str, object], deciles: pd.DataFrame) -> str:
    lines = [
        "# FD-R4 True Rolling OOS Validation",
        "",
        "not alpha evidence",
        "full-sample ICIR: forbidden",
        "future universe: forbidden",
        "future normalization: forbidden",
        "post-period factor selection: forbidden",
        "allocator: not run",
        "direct Q2 entry: not allowed",
        "",
        f"- train window months: {summary['train_window_months']}",
        f"- validation window months: {summary['validation_window_months']}",
        f"- horizons months: {summary['horizons_months']}",
        f"- rebalance count: {summary['rebalance_count']}",
        f"- score rows: {summary['score_row_count']}",
        "",
        "## OOS Diagnostics",
    ]
    if deciles.empty:
        lines.append("- no decile diagnostics were available")
    else:
        diagnostic = (
            deciles.groupby(["period", "horizon_months"], as_index=False)
            .agg(mean_rank_ic=("rank_ic", "mean"), mean_top_bottom_spread=("top_bottom_spread", "mean"))
            .sort_values(["period", "horizon_months"])
        )
        for row in diagnostic.itertuples(index=False):
            lines.append(
                f"- {row.period} {int(row.horizon_months)}m: "
                f"mean_rank_ic={row.mean_rank_ic:.6f}, "
                f"mean_top_bottom_spread={row.mean_top_bottom_spread:.6f}"
            )
    lines.append("")
    return "\n".join(lines)
