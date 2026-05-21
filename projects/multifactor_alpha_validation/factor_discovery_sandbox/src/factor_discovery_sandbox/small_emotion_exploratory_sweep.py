"""E0 exploratory parameter sweep for small-cap shock mechanisms."""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .small_emotion_d2 import (
    GUARDS,
    _data_coverage_report,
    _prepare_price_panel,
    _read_csv,
    _window_return,
)


POST_WINDOWS: dict[str, tuple[int, int]] = {
    "post_1_3": (1, 3),
    "post_1_5": (1, 5),
    "post_1_10": (1, 10),
    "post_1_22": (1, 22),
}

DEFAULT_MECHANISMS = [
    "up_shock_continuation",
    "up_shock_reversal",
    "down_shock_reversal",
    "down_shock_continuation",
    "liquidity_vacuum_reversal",
]

EXPLORATORY_GUARDS: dict[str, object] = {
    **GUARDS,
    "exploratory_only": True,
    "overfit_search_allowed": True,
    "requires_freeze_before_q1": True,
}


@dataclass(frozen=True)
class SmallEmotionExploratorySweepResult:
    """Output from E0-SMALL-EMOTION exploratory sweep."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_exploratory_sweep(
    *,
    price_panel_path: str | Path,
    benchmark_panel_path: str | Path,
    delisting_path: str | Path,
    output_dir: str | Path,
    shock_thresholds: Iterable[float] = (0.05, 0.08, 0.10, 0.15),
    volume_spike_thresholds: Iterable[float] = (1.5, 2.0, 3.0),
    windows: Iterable[str] = ("post_1_3", "post_1_5", "post_1_10", "post_1_22"),
    mechanisms: Iterable[str] = tuple(DEFAULT_MECHANISMS),
    market_cap_buckets: Iterable[str] = ("micro", "small", "lower_mid", "all_small_cap"),
    liquidity_filters: Iterable[str] = ("all", "low", "mid", "high", "weak_liquidity"),
    stale_filters: Iterable[str] = ("medium", "strict"),
    adv_min_dollars: Iterable[float] = (250_000.0, 500_000.0, 1_000_000.0),
    min_history_observations: int = 60,
    small_cap_min_market_cap: float = 50_000_000.0,
    small_cap_max_market_cap: float = 5_000_000_000.0,
    top_n: int = 25,
    max_rows: int | None = None,
) -> SmallEmotionExploratorySweepResult:
    """Search shock/attention/liquidity parameters in-sample without validation claims."""

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
        summary = _summary(
            grid=grid,
            best=best,
            coverage=coverage,
            candidate_found=False,
            top_candidate={},
        )
        _write_outputs(artifacts, grid, best, coverage, summary, top_candidate={})
        return SmallEmotionExploratorySweepResult(summary=summary, artifacts=artifacts)

    prepared = _prepare_price_panel(
        prices,
        benchmark,
        min_history_observations=min_history_observations,
        min_adv_dollars=min(float(value) for value in adv_min_dollars),
        small_cap_min_market_cap=small_cap_min_market_cap,
        small_cap_max_market_cap=small_cap_max_market_cap,
        large_cap_min_market_cap=5_000_000_000.0,
    )
    event_labels = _event_label_panel(
        prepared,
        min_shock_threshold=min(float(value) for value in shock_thresholds),
        min_volume_spike=min(float(value) for value in volume_spike_thresholds),
        windows=list(windows),
    )
    grid = _parameter_sweep_grid(
        event_labels=event_labels,
        shock_thresholds=list(shock_thresholds),
        volume_spike_thresholds=list(volume_spike_thresholds),
        windows=list(windows),
        mechanisms=list(mechanisms),
        market_cap_buckets=list(market_cap_buckets),
        liquidity_filters=list(liquidity_filters),
        stale_filters=list(stale_filters),
        adv_min_dollars=list(adv_min_dollars),
    )
    best = _best_candidates(grid, top_n=top_n)
    top_candidate = best.iloc[0].to_dict() if not best.empty else {}
    summary = _summary(
        grid=grid,
        best=best,
        coverage=coverage,
        candidate_found=bool(top_candidate),
        top_candidate=top_candidate,
    )
    _write_outputs(artifacts, grid, best, coverage, summary, top_candidate=top_candidate)
    return SmallEmotionExploratorySweepResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "data_coverage_report": output_path / "data_coverage_report.json",
        "parameter_sweep_grid": output_path / "parameter_sweep_grid.csv",
        "best_in_sample_candidates": output_path / "best_in_sample_candidates.csv",
        "overfit_risk_report": output_path / "overfit_risk_report.json",
        "candidate_to_freeze_next": output_path / "candidate_to_freeze_next.json",
        "exploratory_sweep_summary": output_path / "exploratory_sweep_summary.json",
        "exploratory_sweep_report": output_path / "exploratory_sweep_report.md",
    }


def _event_label_panel(
    prepared: pd.DataFrame,
    *,
    min_shock_threshold: float,
    min_volume_spike: float,
    windows: list[str],
) -> pd.DataFrame:
    active = prepared[
        prepared["coverage_state"].eq("active_view")
        & prepared["shock_abs"].ge(float(min_shock_threshold))
        & prepared["abnormal_volume"].ge(float(min_volume_spike))
    ].copy()
    if active.empty:
        return pd.DataFrame(columns=_event_label_columns())

    by_asset = {asset: group.sort_values("date").reset_index(drop=True) for asset, group in prepared.groupby("asset_id")}
    rows: list[dict[str, object]] = []
    for _idx, event in active.iterrows():
        asset_panel = by_asset.get(str(event["asset_id"]))
        if asset_panel is None or asset_panel.empty:
            continue
        positions = asset_panel.index[asset_panel["date"].eq(pd.Timestamp(event["date"]))].tolist()
        if not positions:
            continue
        for window in windows:
            if window not in POST_WINDOWS:
                continue
            asset_return, benchmark_return, label_status = _window_return(asset_panel, int(positions[0]), POST_WINDOWS[window])
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
                    "market_cap": float(event["market_cap"]),
                    "market_cap_bucket": _market_cap_bucket(float(event["market_cap"])),
                    "adv20": float(event["adv20"]),
                    "liquidity_bucket": str(event["liquidity_bucket"]),
                    "weak_liquidity": bool(event["weak_liquidity"]),
                    "stale_roll_5": float(event["stale_roll_5"]),
                    "zero_volume": bool(event["zero_volume"]),
                    "window": window,
                    "asset_return": asset_return,
                    "benchmark_return": benchmark_return,
                    "abnormal_return": abnormal,
                    "label_status": label_status,
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
        "market_cap",
        "market_cap_bucket",
        "adv20",
        "liquidity_bucket",
        "weak_liquidity",
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


def _parameter_sweep_grid(
    *,
    event_labels: pd.DataFrame,
    shock_thresholds: list[float],
    volume_spike_thresholds: list[float],
    windows: list[str],
    mechanisms: list[str],
    market_cap_buckets: list[str],
    liquidity_filters: list[str],
    stale_filters: list[str],
    adv_min_dollars: list[float],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (
        mechanism,
        shock_threshold,
        volume_threshold,
        window,
        market_cap_bucket,
        liquidity_filter,
        stale_filter,
        adv_min,
    ) in itertools.product(
        mechanisms,
        shock_thresholds,
        volume_spike_thresholds,
        windows,
        market_cap_buckets,
        liquidity_filters,
        stale_filters,
        adv_min_dollars,
    ):
        frame = _filter_candidate_frame(
            event_labels,
            mechanism=mechanism,
            shock_threshold=float(shock_threshold),
            volume_threshold=float(volume_threshold),
            window=window,
            market_cap_bucket=market_cap_bucket,
            liquidity_filter=liquidity_filter,
            stale_filter=stale_filter,
            adv_min=float(adv_min),
        )
        directional = _directional_series(frame, mechanism)
        observed = directional.dropna()
        active_event_count = int(len(observed))
        event_month_count = int(frame.loc[observed.index, "event_month"].nunique()) if active_event_count else 0
        issuer_count = int(frame.loc[observed.index, "asset_id"].nunique()) if active_event_count else 0
        mean_directional = float(observed.mean()) if active_event_count else np.nan
        median_directional = float(observed.median()) if active_event_count else np.nan
        hit_rate = float((observed > 0.0).mean()) if active_event_count else np.nan
        exploratory_score = float(mean_directional * np.sqrt(active_event_count)) if active_event_count and pd.notna(mean_directional) else np.nan
        rows.append(
            {
                "schema_version": "small_emotion_exploratory_sweep_grid.v1",
                "stage": "E0-SMALL-EMOTION-02",
                "mechanism": mechanism,
                "shock_threshold": float(shock_threshold),
                "volume_spike_threshold": float(volume_threshold),
                "market_cap_bucket": market_cap_bucket,
                "liquidity_filter": liquidity_filter,
                "stale_filter": stale_filter,
                "adv_min_dollars": float(adv_min),
                "window": window,
                "active_event_count": active_event_count,
                "event_month_count": event_month_count,
                "issuer_count": issuer_count,
                "mean_directional_return": mean_directional,
                "median_directional_return": median_directional,
                "hit_rate": hit_rate,
                "exploratory_score": exploratory_score,
                "selection_status": "in_sample_search_only",
                **EXPLORATORY_GUARDS,
            }
        )
    grid = pd.DataFrame(rows)
    if grid.empty:
        return pd.DataFrame(columns=_grid_columns())
    grid = grid.sort_values(
        ["exploratory_score", "active_event_count", "event_month_count"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)
    grid["in_sample_rank"] = range(1, len(grid) + 1)
    return grid[_grid_columns()]


def _filter_candidate_frame(
    frame: pd.DataFrame,
    *,
    mechanism: str,
    shock_threshold: float,
    volume_threshold: float,
    window: str,
    market_cap_bucket: str,
    liquidity_filter: str,
    stale_filter: str,
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
    if mechanism in {"up_shock_continuation", "up_shock_reversal"}:
        filtered = filtered[filtered["shock_return"].ge(float(shock_threshold))]
    elif mechanism in {"down_shock_reversal", "down_shock_continuation"}:
        filtered = filtered[filtered["shock_return"].le(-float(shock_threshold))]
    elif mechanism == "liquidity_vacuum_reversal":
        filtered = filtered[filtered["abs_shock_return"].ge(float(shock_threshold)) & filtered["weak_liquidity"]]
    else:
        return filtered.iloc[0:0].copy()

    if market_cap_bucket != "all_small_cap":
        filtered = filtered[filtered["market_cap_bucket"].eq(market_cap_bucket)]
    if liquidity_filter == "weak_liquidity":
        filtered = filtered[filtered["weak_liquidity"]]
    elif liquidity_filter != "all":
        filtered = filtered[filtered["liquidity_bucket"].eq(liquidity_filter)]
    if stale_filter == "strict":
        filtered = filtered[filtered["stale_roll_5"].le(0.0) & ~filtered["zero_volume"]]
    return filtered


def _directional_series(frame: pd.DataFrame, mechanism: str) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype="float64")
    abnormal = pd.to_numeric(frame["abnormal_return"], errors="coerce")
    shock = pd.to_numeric(frame["shock_return"], errors="coerce")
    if mechanism in {"up_shock_continuation", "down_shock_reversal"}:
        return abnormal
    if mechanism in {"up_shock_reversal", "down_shock_continuation"}:
        return -abnormal
    if mechanism == "liquidity_vacuum_reversal":
        return -np.sign(shock).replace(0.0, np.nan) * abnormal
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _grid_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "mechanism",
        "shock_threshold",
        "volume_spike_threshold",
        "market_cap_bucket",
        "liquidity_filter",
        "stale_filter",
        "adv_min_dollars",
        "window",
        "active_event_count",
        "event_month_count",
        "issuer_count",
        "mean_directional_return",
        "median_directional_return",
        "hit_rate",
        "exploratory_score",
        "in_sample_rank",
        "selection_status",
        *EXPLORATORY_GUARDS.keys(),
    ]


def _best_candidates(grid: pd.DataFrame, *, top_n: int) -> pd.DataFrame:
    if grid.empty:
        return pd.DataFrame(columns=_grid_columns())
    eligible = grid[grid["active_event_count"].gt(0) & grid["exploratory_score"].notna()].copy()
    if eligible.empty:
        return pd.DataFrame(columns=_grid_columns())
    return eligible.head(int(top_n)).reset_index(drop=True)


def _summary(
    *,
    grid: pd.DataFrame,
    best: pd.DataFrame,
    coverage: dict[str, object],
    candidate_found: bool,
    top_candidate: dict[str, object],
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_exploratory_sweep_summary.v1",
        "stage": "E0-SMALL-EMOTION-02",
        "candidate_id": "small_cap_shock_emotion_liquidity_exploratory_sweep",
        "exploratory_only": True,
        "overfit_search_allowed": True,
        "candidate_found_for_possible_freeze": bool(candidate_found),
        "top_candidate": _json_safe_dict(top_candidate),
        "grid_row_count": int(len(grid)),
        "best_candidate_count": int(len(best)),
        "data_status": coverage.get("data_status"),
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "d3_auto_open_allowed": False,
        **EXPLORATORY_GUARDS,
    }


def _write_outputs(
    artifacts: dict[str, Path],
    grid: pd.DataFrame,
    best: pd.DataFrame,
    coverage: dict[str, object],
    summary: dict[str, object],
    *,
    top_candidate: dict[str, object],
) -> None:
    grid.to_csv(artifacts["parameter_sweep_grid"], index=False)
    best.to_csv(artifacts["best_in_sample_candidates"], index=False)
    artifacts["overfit_risk_report"].write_text(
        json.dumps(_overfit_risk_report(grid, best), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["candidate_to_freeze_next"].write_text(
        json.dumps(_candidate_to_freeze_next(top_candidate), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["exploratory_sweep_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["exploratory_sweep_report"].write_text(
        _report(summary, coverage, best),
        encoding="utf-8",
    )


def _overfit_risk_report(grid: pd.DataFrame, best: pd.DataFrame) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_overfit_risk_report.v1",
        "stage": "E0-SMALL-EMOTION-02",
        "selection_bias_risk": "high",
        "parameter_grid_row_count": int(len(grid)),
        "reported_best_candidate_count": int(len(best)),
        "exploratory_results_are_not_alpha_evidence": True,
        "requires_freeze_before_q1": True,
        "requires_oos_and_placebo_after_freeze": True,
        "allowed_next_step": "manual_review_then_optional_D3_charter_for_one_frozen_candidate",
        **EXPLORATORY_GUARDS,
    }


def _candidate_to_freeze_next(top_candidate: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_candidate_to_freeze_next.v1",
        "stage": "E0-SMALL-EMOTION-02",
        "candidate_found": bool(top_candidate),
        "candidate": _json_safe_dict(top_candidate),
        "recommendation": (
            "manual_review_then_optional_D3_charter"
            if top_candidate
            else "no_in_sample_candidate_found"
        ),
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "optimizer_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "production_approval_claimed": False,
        "exploratory_results_are_not_alpha_evidence": True,
        **EXPLORATORY_GUARDS,
    }


def _report(summary: dict[str, object], coverage: dict[str, object], best: pd.DataFrame) -> str:
    lines = [
        "# E0-SMALL-EMOTION-02 Exploratory Parameter Sweep",
        "",
        "This is an exploratory in-sample search. Parameter tuning and overfit search are allowed here, but the output is not alpha evidence and does not open D3, Q1, Q2, optimizer, portfolio, Alpha Registry, paper, broker, order, live, or production workflows.",
        "",
        f"- data_status: {coverage.get('data_status')}",
        f"- grid_row_count: {summary['grid_row_count']}",
        f"- best_candidate_count: {summary['best_candidate_count']}",
        f"- candidate_found_for_possible_freeze: {summary['candidate_found_for_possible_freeze']}",
        "",
        "## Best In-Sample Candidates",
    ]
    if best.empty:
        lines.append("- none")
    else:
        for row in best.head(10).itertuples(index=False):
            lines.append(
                "- "
                f"rank={row.in_sample_rank}, mechanism={row.mechanism}, "
                f"window={row.window}, shock={row.shock_threshold}, "
                f"volume={row.volume_spike_threshold}, "
                f"bucket={row.market_cap_bucket}, liquidity={row.liquidity_filter}, "
                f"mean_directional={row.mean_directional_return:.6f}, "
                f"events={row.active_event_count}"
            )
    lines.append("")
    return "\n".join(lines)


def _json_safe_dict(values: dict[str, object]) -> dict[str, object]:
    safe: dict[str, object] = {}
    for key, value in values.items():
        if isinstance(value, (np.integer,)):
            safe[key] = int(value)
        elif isinstance(value, (np.floating,)):
            safe[key] = None if pd.isna(value) else float(value)
        elif pd.isna(value) if not isinstance(value, (list, tuple, dict, str, bool)) else False:
            safe[key] = None
        else:
            safe[key] = value
    return safe
