"""Aggressive E0 sharpening sweep for small-cap shock reversal pockets."""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .small_emotion_d2 import _data_coverage_report, _prepare_price_panel, _read_csv, _window_return
from .small_emotion_exploratory_sweep import EXPLORATORY_GUARDS


SHARPEN_WINDOWS: dict[str, tuple[int, int]] = {
    "post_1_3": (1, 3),
    "post_1_5": (1, 5),
    "post_1_10": (1, 10),
    "post_6_22": (6, 22),
    "post_1_22": (1, 22),
}


@dataclass(frozen=True)
class SmallEmotionSharpeningSweepResult:
    """Output from aggressive E0 sharpening sweep."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_sharpening_sweep(
    *,
    price_panel_path: str | Path,
    benchmark_panel_path: str | Path,
    delisting_path: str | Path,
    output_dir: str | Path,
    shock_thresholds: Iterable[float] = (0.05, 0.08, 0.10),
    volume_spike_thresholds: Iterable[float] = (1.5, 2.0),
    prior_5d_min_returns: Iterable[float | None] = (None, 0.10, 0.20),
    prior_20d_min_returns: Iterable[float | None] = (None, 0.20),
    close_location_filters: Iterable[str] = ("all", "top_quartile"),
    low_price_filters: Iterable[str] = ("all", "under_10"),
    market_cap_buckets: Iterable[str] = ("all_small_cap", "micro", "small"),
    liquidity_filters: Iterable[str] = ("all", "weak_liquidity"),
    spread_filters: Iterable[str] = ("all", "wide"),
    regime_filters: Iterable[str] = ("all", "market_up_20d", "market_down_20d"),
    windows: Iterable[str] = ("post_1_5", "post_6_22", "post_1_22"),
    mechanisms: Iterable[str] = ("up_shock_reversal",),
    adv_min_dollars: Iterable[float] = (250_000.0,),
    min_history_observations: int = 60,
    min_events: int = 50,
    small_cap_min_market_cap: float = 50_000_000.0,
    small_cap_max_market_cap: float = 5_000_000_000.0,
    top_n: int = 50,
    max_rows: int | None = 750_000,
) -> SmallEmotionSharpeningSweepResult:
    """Run an aggressive in-sample search for sharper gross pockets."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    prices = _read_csv(Path(price_panel_path), nrows=max_rows)
    benchmark = _read_csv(Path(benchmark_panel_path))
    delistings = _read_csv(Path(delisting_path))
    coverage = _data_coverage_report(
        prices=prices,
        benchmark=benchmark,
        delistings=delistings,
        price_panel_path=Path(price_panel_path),
        benchmark_panel_path=Path(benchmark_panel_path),
        delisting_path=Path(delisting_path),
        max_rows=max_rows,
    )
    artifacts["data_coverage_report"].write_text(
        json.dumps({**coverage, **EXPLORATORY_GUARDS}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if coverage["data_status"] != "available":
        grid = pd.DataFrame(columns=_grid_columns())
        best = grid.copy()
        summary = _summary(grid=grid, best=best, coverage=coverage)
        _write_outputs(artifacts, grid, best, summary)
        return SmallEmotionSharpeningSweepResult(summary=summary, artifacts=artifacts)

    prepared = _prepare_price_panel(
        prices,
        benchmark,
        min_history_observations=min_history_observations,
        min_adv_dollars=min(float(value) for value in adv_min_dollars),
        small_cap_min_market_cap=small_cap_min_market_cap,
        small_cap_max_market_cap=small_cap_max_market_cap,
        large_cap_min_market_cap=5_000_000_000.0,
    )
    enriched = _add_sharpening_features(prepared)
    event_labels = _event_label_panel(
        enriched,
        min_shock_threshold=min(float(value) for value in shock_thresholds),
        min_volume_spike=min(float(value) for value in volume_spike_thresholds),
        windows=list(windows),
    )
    grid = _sweep_grid(
        event_labels=event_labels,
        shock_thresholds=list(shock_thresholds),
        volume_spike_thresholds=list(volume_spike_thresholds),
        prior_5d_min_returns=list(prior_5d_min_returns),
        prior_20d_min_returns=list(prior_20d_min_returns),
        close_location_filters=list(close_location_filters),
        low_price_filters=list(low_price_filters),
        market_cap_buckets=list(market_cap_buckets),
        liquidity_filters=list(liquidity_filters),
        spread_filters=list(spread_filters),
        regime_filters=list(regime_filters),
        windows=list(windows),
        mechanisms=list(mechanisms),
        adv_min_dollars=list(adv_min_dollars),
        min_events=min_events,
    )
    best = _best_candidates(grid, top_n=top_n)
    summary = _summary(grid=grid, best=best, coverage=coverage)
    _write_outputs(artifacts, grid, best, summary)
    return SmallEmotionSharpeningSweepResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "data_coverage_report": output_path / "data_coverage_report.json",
        "sharpening_sweep_grid": output_path / "sharpening_sweep_grid.csv",
        "best_explosive_candidates": output_path / "best_explosive_candidates.csv",
        "overfit_disclosure": output_path / "overfit_disclosure.json",
        "candidate_to_freeze_next": output_path / "candidate_to_freeze_next.json",
        "sharpening_sweep_summary": output_path / "sharpening_sweep_summary.json",
        "sharpening_sweep_report": output_path / "sharpening_sweep_report.md",
    }


def _add_sharpening_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["asset_id", "date"]).copy()
    group = out.groupby("asset_id", sort=False)
    log_returns = np.log1p(pd.to_numeric(out["return"], errors="coerce").clip(lower=-0.999999))
    out["prior_5d_return"] = np.expm1(
        log_returns.groupby(out["asset_id"], sort=False).transform(lambda values: values.shift(1).rolling(5, min_periods=3).sum())
    )
    out["prior_20d_return"] = np.expm1(
        log_returns.groupby(out["asset_id"], sort=False).transform(lambda values: values.shift(1).rolling(20, min_periods=10).sum())
    )
    high = pd.to_numeric(out["high"], errors="coerce") if "high" in out.columns else pd.Series(np.nan, index=out.index)
    low = pd.to_numeric(out["low"], errors="coerce") if "low" in out.columns else pd.Series(np.nan, index=out.index)
    close = pd.to_numeric(out["adjusted_close"], errors="coerce")
    if "adjusted_open" in out.columns:
        open_ = pd.to_numeric(out["adjusted_open"], errors="coerce")
    elif "raw_open" in out.columns:
        open_ = pd.to_numeric(out["raw_open"], errors="coerce")
    else:
        open_ = pd.Series(np.nan, index=out.index)
    out["close_location"] = ((close - low) / (high - low).replace(0.0, np.nan)).clip(lower=0.0, upper=1.0)
    out["open_to_close_return"] = close / open_.replace(0.0, np.nan) - 1.0
    out["low_price_bucket"] = np.select(
        [close < 5.0, close < 10.0, close < 20.0],
        ["under_5", "under_10", "under_20"],
        default="above_20",
    )
    bench = out[["date", "benchmark_return"]].drop_duplicates("date").sort_values("date").copy()
    bench["market_prior_20d"] = (1.0 + bench["benchmark_return"].shift(1)).rolling(20, min_periods=10).apply(np.prod, raw=True) - 1.0
    bench["market_vol_20d"] = bench["benchmark_return"].shift(1).rolling(20, min_periods=10).std()
    vol_median = float(bench["market_vol_20d"].median()) if bench["market_vol_20d"].notna().any() else np.nan
    bench["market_regime"] = np.select(
        [
            bench["market_vol_20d"].gt(vol_median) if pd.notna(vol_median) else pd.Series(False, index=bench.index),
            bench["market_prior_20d"].ge(0.0),
            bench["market_prior_20d"].lt(0.0),
        ],
        ["market_high_vol", "market_up_20d", "market_down_20d"],
        default="market_unknown",
    )
    out = out.merge(bench[["date", "market_prior_20d", "market_vol_20d", "market_regime"]], on="date", how="left")
    return out


def _event_label_panel(
    frame: pd.DataFrame,
    *,
    min_shock_threshold: float,
    min_volume_spike: float,
    windows: list[str],
) -> pd.DataFrame:
    active = frame[
        frame["coverage_state"].eq("active_view")
        & frame["shock_abs"].ge(float(min_shock_threshold))
        & frame["abnormal_volume"].ge(float(min_volume_spike))
    ].copy()
    if active.empty:
        return pd.DataFrame(columns=_event_label_columns())
    by_asset = {asset: group.sort_values("date").reset_index(drop=True) for asset, group in frame.groupby("asset_id")}
    rows: list[dict[str, object]] = []
    for _idx, event in active.iterrows():
        asset_panel = by_asset.get(str(event["asset_id"]))
        if asset_panel is None:
            continue
        positions = asset_panel.index[asset_panel["date"].eq(pd.Timestamp(event["date"]))].tolist()
        if not positions:
            continue
        for window in windows:
            if window not in SHARPEN_WINDOWS:
                continue
            asset_return, benchmark_return, status = _window_return(asset_panel, int(positions[0]), SHARPEN_WINDOWS[window])
            abnormal = asset_return - benchmark_return if pd.notna(asset_return) and pd.notna(benchmark_return) else np.nan
            rows.append(
                {
                    "asset_id": str(event["asset_id"]),
                    "ticker": str(event["ticker"]),
                    "date": pd.Timestamp(event["date"]).date().isoformat(),
                    "event_month": pd.Timestamp(event["date"]).strftime("%Y-%m"),
                    "shock_return": float(event["return"]),
                    "abs_shock_return": float(event["shock_abs"]),
                    "abnormal_volume": float(event["abnormal_volume"]),
                    "prior_5d_return": _float_or_nan(event.get("prior_5d_return")),
                    "prior_20d_return": _float_or_nan(event.get("prior_20d_return")),
                    "close_location": _float_or_nan(event.get("close_location")),
                    "open_to_close_return": _float_or_nan(event.get("open_to_close_return")),
                    "low_price_bucket": str(event.get("low_price_bucket", "unavailable")),
                    "market_cap": float(event["market_cap"]),
                    "market_cap_bucket": _market_cap_bucket(float(event["market_cap"])),
                    "adv20": float(event["adv20"]),
                    "liquidity_bucket": str(event["liquidity_bucket"]),
                    "weak_liquidity": bool(event["weak_liquidity"]),
                    "spread_bucket": str(event["spread_bucket"]),
                    "market_regime": str(event.get("market_regime", "market_unknown")),
                    "stale_roll_5": float(event["stale_roll_5"]),
                    "zero_volume": bool(event["zero_volume"]),
                    "window": window,
                    "asset_return": asset_return,
                    "benchmark_return": benchmark_return,
                    "abnormal_return": abnormal,
                    "label_status": status,
                    **EXPLORATORY_GUARDS,
                }
            )
    if not rows:
        return pd.DataFrame(columns=_event_label_columns())
    return pd.DataFrame(rows)[_event_label_columns()]


def _event_label_columns() -> list[str]:
    return [
        "asset_id",
        "ticker",
        "date",
        "event_month",
        "shock_return",
        "abs_shock_return",
        "abnormal_volume",
        "prior_5d_return",
        "prior_20d_return",
        "close_location",
        "open_to_close_return",
        "low_price_bucket",
        "market_cap",
        "market_cap_bucket",
        "adv20",
        "liquidity_bucket",
        "weak_liquidity",
        "spread_bucket",
        "market_regime",
        "stale_roll_5",
        "zero_volume",
        "window",
        "asset_return",
        "benchmark_return",
        "abnormal_return",
        "label_status",
        *EXPLORATORY_GUARDS.keys(),
    ]


def _market_cap_bucket(market_cap: float) -> str:
    if market_cap < 300_000_000.0:
        return "micro"
    if market_cap < 2_000_000_000.0:
        return "small"
    return "lower_mid"


def _sweep_grid(
    *,
    event_labels: pd.DataFrame,
    shock_thresholds: list[float],
    volume_spike_thresholds: list[float],
    prior_5d_min_returns: list[float | None],
    prior_20d_min_returns: list[float | None],
    close_location_filters: list[str],
    low_price_filters: list[str],
    market_cap_buckets: list[str],
    liquidity_filters: list[str],
    spread_filters: list[str],
    regime_filters: list[str],
    windows: list[str],
    mechanisms: list[str],
    adv_min_dollars: list[float],
    min_events: int,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    combos = itertools.product(
        mechanisms,
        shock_thresholds,
        volume_spike_thresholds,
        prior_5d_min_returns,
        prior_20d_min_returns,
        close_location_filters,
        low_price_filters,
        market_cap_buckets,
        liquidity_filters,
        spread_filters,
        regime_filters,
        windows,
        adv_min_dollars,
    )
    for combo in combos:
        row = _evaluate_combo(event_labels, combo, min_events=min_events)
        rows.append(row)
    grid = pd.DataFrame(rows)
    if grid.empty:
        return pd.DataFrame(columns=_grid_columns())
    grid = grid.sort_values(
        ["eligible_for_explosive_review", "mean_directional_return", "gross_explosive_score", "active_event_count"],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)
    grid["gross_rank"] = range(1, len(grid) + 1)
    return grid[_grid_columns()]


def _evaluate_combo(event_labels: pd.DataFrame, combo: tuple[object, ...], *, min_events: int) -> dict[str, object]:
    (
        mechanism,
        shock_threshold,
        volume_threshold,
        prior_5_min,
        prior_20_min,
        close_filter,
        low_price_filter,
        market_cap_bucket,
        liquidity_filter,
        spread_filter,
        regime_filter,
        window,
        adv_min,
    ) = combo
    mechanism = str(mechanism)
    frame = _filter_frame(
        event_labels,
        mechanism=mechanism,
        shock_threshold=float(shock_threshold),
        volume_threshold=float(volume_threshold),
        prior_5_min=prior_5_min,
        prior_20_min=prior_20_min,
        close_filter=str(close_filter),
        low_price_filter=str(low_price_filter),
        market_cap_bucket=str(market_cap_bucket),
        liquidity_filter=str(liquidity_filter),
        spread_filter=str(spread_filter),
        regime_filter=str(regime_filter),
        window=str(window),
        adv_min=float(adv_min),
    )
    directional = _directional_series(frame, mechanism).dropna()
    active_events = int(len(directional))
    event_months = int(frame.loc[directional.index, "event_month"].nunique()) if active_events else 0
    issuer_count = int(frame.loc[directional.index, "asset_id"].nunique()) if active_events else 0
    mean_directional = float(directional.mean()) if active_events else np.nan
    median_directional = float(directional.median()) if active_events else np.nan
    hit_rate = float((directional > 0.0).mean()) if active_events else np.nan
    gross_score = float(mean_directional * np.log1p(active_events)) if active_events and pd.notna(mean_directional) else np.nan
    return {
        "schema_version": "small_emotion_sharpening_sweep_grid.v1",
        "stage": "E0-SMALL-EMOTION-04",
        "mechanism": mechanism,
        "shock_threshold": float(shock_threshold),
        "volume_spike_threshold": float(volume_threshold),
        "prior_5d_min_return": "" if prior_5_min is None else float(prior_5_min),
        "prior_20d_min_return": "" if prior_20_min is None else float(prior_20_min),
        "close_location_filter": str(close_filter),
        "low_price_filter": str(low_price_filter),
        "market_cap_bucket": str(market_cap_bucket),
        "liquidity_filter": str(liquidity_filter),
        "spread_filter": str(spread_filter),
        "regime_filter": str(regime_filter),
        "adv_min_dollars": float(adv_min),
        "window": str(window),
        "active_event_count": active_events,
        "event_month_count": event_months,
        "issuer_count": issuer_count,
        "mean_directional_return": mean_directional,
        "median_directional_return": median_directional,
        "hit_rate": hit_rate,
        "gross_explosive_score": gross_score,
        "eligible_for_explosive_review": bool(active_events >= int(min_events) and pd.notna(mean_directional) and mean_directional > 0.0),
        "selection_status": "aggressive_in_sample_search_only",
        **EXPLORATORY_GUARDS,
    }


def _filter_frame(
    frame: pd.DataFrame,
    *,
    mechanism: str,
    shock_threshold: float,
    volume_threshold: float,
    prior_5_min: float | None,
    prior_20_min: float | None,
    close_filter: str,
    low_price_filter: str,
    market_cap_bucket: str,
    liquidity_filter: str,
    spread_filter: str,
    regime_filter: str,
    window: str,
    adv_min: float,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    filtered = frame[
        frame["window"].eq(window)
        & frame["label_status"].eq("observed")
        & frame["abnormal_volume"].ge(float(volume_threshold))
        & frame["adv20"].ge(float(adv_min))
    ].copy()
    if mechanism in {"up_shock_reversal", "up_shock_continuation"}:
        filtered = filtered[filtered["shock_return"].ge(float(shock_threshold))]
    elif mechanism in {"down_shock_reversal", "down_shock_continuation"}:
        filtered = filtered[filtered["shock_return"].le(-float(shock_threshold))]
    else:
        return filtered.iloc[0:0].copy()
    if prior_5_min is not None:
        filtered = filtered[filtered["prior_5d_return"].ge(float(prior_5_min))]
    if prior_20_min is not None:
        filtered = filtered[filtered["prior_20d_return"].ge(float(prior_20_min))]
    if close_filter == "upper_half":
        filtered = filtered[filtered["close_location"].ge(0.50)]
    elif close_filter == "top_quartile":
        filtered = filtered[filtered["close_location"].ge(0.75)]
    elif close_filter == "lower_half":
        filtered = filtered[filtered["close_location"].lt(0.50)]
    if low_price_filter == "under_20":
        filtered = filtered[filtered["low_price_bucket"].isin(["under_5", "under_10", "under_20"])]
    elif low_price_filter == "under_10":
        filtered = filtered[filtered["low_price_bucket"].isin(["under_5", "under_10"])]
    elif low_price_filter == "under_5":
        filtered = filtered[filtered["low_price_bucket"].eq("under_5")]
    if market_cap_bucket != "all_small_cap":
        filtered = filtered[filtered["market_cap_bucket"].eq(market_cap_bucket)]
    if liquidity_filter == "weak_liquidity":
        filtered = filtered[filtered["weak_liquidity"]]
    elif liquidity_filter != "all":
        filtered = filtered[filtered["liquidity_bucket"].eq(liquidity_filter)]
    if spread_filter != "all":
        filtered = filtered[filtered["spread_bucket"].eq(spread_filter)]
    if regime_filter != "all":
        filtered = filtered[filtered["market_regime"].eq(regime_filter)]
    return filtered


def _directional_series(frame: pd.DataFrame, mechanism: str) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype="float64")
    abnormal = pd.to_numeric(frame["abnormal_return"], errors="coerce")
    if mechanism in {"up_shock_continuation", "down_shock_reversal"}:
        return abnormal
    if mechanism in {"up_shock_reversal", "down_shock_continuation"}:
        return -abnormal
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _grid_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "mechanism",
        "shock_threshold",
        "volume_spike_threshold",
        "prior_5d_min_return",
        "prior_20d_min_return",
        "close_location_filter",
        "low_price_filter",
        "market_cap_bucket",
        "liquidity_filter",
        "spread_filter",
        "regime_filter",
        "adv_min_dollars",
        "window",
        "active_event_count",
        "event_month_count",
        "issuer_count",
        "mean_directional_return",
        "median_directional_return",
        "hit_rate",
        "gross_explosive_score",
        "eligible_for_explosive_review",
        "gross_rank",
        "selection_status",
        *EXPLORATORY_GUARDS.keys(),
    ]


def _best_candidates(grid: pd.DataFrame, *, top_n: int) -> pd.DataFrame:
    if grid.empty:
        return pd.DataFrame(columns=_grid_columns())
    eligible = grid[grid["eligible_for_explosive_review"].astype(bool)].copy()
    if eligible.empty:
        return pd.DataFrame(columns=_grid_columns())
    return eligible.head(int(top_n)).reset_index(drop=True)


def _summary(*, grid: pd.DataFrame, best: pd.DataFrame, coverage: dict[str, object]) -> dict[str, object]:
    top = best.iloc[0].to_dict() if not best.empty else {}
    return {
        "schema_version": "small_emotion_sharpening_sweep_summary.v1",
        "stage": "E0-SMALL-EMOTION-04",
        "candidate_id": "small_cap_emotion_mechanism_sharpening_sweep",
        "purpose": "find_strong_in_sample_pockets_before_freeze",
        "grid_row_count": int(len(grid)),
        "best_candidate_count": int(len(best)),
        "explosive_candidate_found": bool(top),
        "top_candidate": _json_safe_dict(top),
        "data_status": coverage.get("data_status"),
        "exploratory_only": True,
        "overfit_search_allowed": True,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        **EXPLORATORY_GUARDS,
    }


def _write_outputs(
    artifacts: dict[str, Path],
    grid: pd.DataFrame,
    best: pd.DataFrame,
    summary: dict[str, object],
) -> None:
    grid.to_csv(artifacts["sharpening_sweep_grid"], index=False)
    best.to_csv(artifacts["best_explosive_candidates"], index=False)
    artifacts["overfit_disclosure"].write_text(
        json.dumps(_overfit_disclosure(grid, best), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["candidate_to_freeze_next"].write_text(
        json.dumps(_candidate_to_freeze_next(best), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["sharpening_sweep_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["sharpening_sweep_report"].write_text(_report(summary, best), encoding="utf-8")


def _overfit_disclosure(grid: pd.DataFrame, best: pd.DataFrame) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_sharpening_overfit_disclosure.v1",
        "stage": "E0-SMALL-EMOTION-04",
        "purpose": "find_strong_in_sample_pockets_before_freeze",
        "selection_bias_risk": "very_high",
        "parameter_grid_row_count": int(len(grid)),
        "reported_best_candidate_count": int(len(best)),
        "requires_freeze_before_q1": True,
        "exploratory_results_are_not_alpha_evidence": True,
        **EXPLORATORY_GUARDS,
    }


def _candidate_to_freeze_next(best: pd.DataFrame) -> dict[str, object]:
    top = best.iloc[0].to_dict() if not best.empty else {}
    return {
        "schema_version": "small_emotion_sharpening_candidate_to_freeze_next.v1",
        "stage": "E0-SMALL-EMOTION-04",
        "candidate_found": bool(top),
        "candidate": _json_safe_dict(top),
        "recommendation": "manual_review_then_optional_D3_charter" if top else "no_explosive_candidate_found",
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "optimizer_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "production_approval_claimed": False,
        **EXPLORATORY_GUARDS,
    }


def _report(summary: dict[str, object], best: pd.DataFrame) -> str:
    lines = [
        "# E0-SMALL-EMOTION-04 Mechanism Sharpening Sweep",
        "",
        "This is an aggressive in-sample overfit search. The purpose is to find strong gross pockets before freeze; it is not alpha evidence and opens no Q1/Q2/downstream workflow.",
        "",
        f"- grid_row_count: {summary['grid_row_count']}",
        f"- best_candidate_count: {summary['best_candidate_count']}",
        f"- explosive_candidate_found: {summary['explosive_candidate_found']}",
        "",
        "## Best Gross Pockets",
    ]
    if best.empty:
        lines.append("- none")
    else:
        for row in best.head(10).itertuples(index=False):
            lines.append(
                "- "
                f"rank={row.gross_rank}, mechanism={row.mechanism}, "
                f"window={row.window}, shock={row.shock_threshold}, "
                f"vol={row.volume_spike_threshold}, prior5={row.prior_5d_min_return}, "
                f"prior20={row.prior_20d_min_return}, close={row.close_location_filter}, "
                f"price={row.low_price_filter}, mcap={row.market_cap_bucket}, "
                f"liq={row.liquidity_filter}, spread={row.spread_filter}, "
                f"regime={row.regime_filter}, mean={row.mean_directional_return:.6f}, "
                f"events={row.active_event_count}"
            )
    lines.append("")
    return "\n".join(lines)


def _float_or_nan(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _json_safe_dict(values: dict[str, object]) -> dict[str, object]:
    safe: dict[str, object] = {}
    for key, value in values.items():
        if isinstance(value, np.integer):
            safe[key] = int(value)
        elif isinstance(value, np.floating):
            safe[key] = None if pd.isna(value) else float(value)
        elif isinstance(value, float) and pd.isna(value):
            safe[key] = None
        else:
            safe[key] = value
    return safe
