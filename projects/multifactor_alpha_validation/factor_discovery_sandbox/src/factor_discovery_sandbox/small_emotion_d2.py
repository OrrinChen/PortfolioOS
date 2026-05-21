"""D2-SMALL-EMOTION no-formula small-cap shock observability."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError


GUARDS: dict[str, object] = {
    "formula_score_written": False,
    "measurement_spec_written": False,
    "q1_entry_allowed": False,
    "q2_entry_allowed": False,
    "expected_return_panel_written": False,
    "optimizer_entry_allowed": False,
    "portfolio_construction_allowed": False,
    "alpha_registry_update_allowed": False,
    "paper_ready": False,
    "live_ready": False,
    "broker_order_path_opened": False,
    "production_approval_claimed": False,
    "not_alpha_evidence": True,
    "no_view_not_zero_alpha": True,
}

WINDOWS: dict[str, tuple[int, int]] = {
    "pre_20_1": (-20, -1),
    "pre_10_1": (-10, -1),
    "pre_5_1": (-5, -1),
    "event_0_1": (0, 1),
    "post_1_5": (1, 5),
    "post_1_10": (1, 10),
    "post_1_22": (1, 22),
    "post_1_44": (1, 44),
}

DECISIONS = {
    "observable_panic_reversal",
    "observable_fomo_continuation",
    "observable_liquidity_vacuum_reversal",
    "mixed_narrow_scope",
    "blocked_stale_price",
    "blocked_cost_liquidity",
    "blocked_placebo_dominance",
    "blocked_data_coverage",
    "hold_insufficient_sample",
    "not_observable",
}

SUBSET_TO_D3 = {
    "panic_overreaction_candidate": "panic_overreaction_reversal",
    "fomo_continuation_candidate": "fomo_continuation",
    "liquidity_vacuum_reversal_candidate": "liquidity_vacuum_reversal",
}


@dataclass(frozen=True)
class SmallEmotionD2Result:
    """D2 small-cap shock observability output."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_d2_observability(
    *,
    price_panel_path: str | Path,
    benchmark_panel_path: str | Path,
    delisting_path: str | Path,
    output_dir: str | Path,
    minimum_subset_events: int = 50,
    minimum_event_month_count: int = 12,
    minimum_label_coverage_share: float = 0.70,
    min_history_observations: int = 60,
    min_adv_dollars: float = 250_000.0,
    small_cap_min_market_cap: float = 50_000_000.0,
    small_cap_max_market_cap: float = 3_000_000_000.0,
    large_cap_min_market_cap: float = 5_000_000_000.0,
    max_rows: int | None = None,
) -> SmallEmotionD2Result:
    """Run D2 no-formula observability for small-cap shock mechanisms."""

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
        json.dumps(coverage, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if coverage["data_status"] != "available":
        empty = _empty_outputs(artifacts)
        summary = _summary(
            overall_decision="blocked_data_coverage",
            allow_d3_charter_for=[],
            prices=prices,
            event_registry=empty["registry"],
            car=empty["car"],
            placebo=empty["placebo"],
            coverage=coverage,
        )
        _write_summary_and_report(artifacts, summary, coverage, empty["counts"], empty["car"], empty["placebo"])
        return SmallEmotionD2Result(summary=summary, artifacts=artifacts)

    prepared = _prepare_price_panel(
        prices,
        benchmark,
        min_history_observations=min_history_observations,
        min_adv_dollars=min_adv_dollars,
        small_cap_min_market_cap=small_cap_min_market_cap,
        small_cap_max_market_cap=small_cap_max_market_cap,
        large_cap_min_market_cap=large_cap_min_market_cap,
    )
    registry = _build_event_registry(prepared)
    car = _car_window_panel(registry, prepared)
    placebo = _placebo_report(registry, prepared, car)
    counts = _subset_counts(registry, car)
    stale_guard = _stale_price_guard(prepared, registry, placebo)
    capacity_guard = _capacity_guard(prepared, registry)
    cost = _cost_spread_pregate(registry)
    delisting_audit = _delisting_retention_audit(delistings, registry)
    diagnostics = _continuation_reversal_diagnostics(registry, car)
    matched = _matched_control_panel(placebo)
    coverage_no_view = _coverage_no_view_report(registry)
    bucket_report = _bucket_report(registry)

    decision, allow_d3 = _decision(
        counts=counts,
        diagnostics=diagnostics,
        placebo=placebo,
        stale_guard=stale_guard,
        capacity_guard=capacity_guard,
        minimum_subset_events=minimum_subset_events,
        minimum_event_month_count=minimum_event_month_count,
        minimum_label_coverage_share=minimum_label_coverage_share,
    )
    if decision not in DECISIONS:  # pragma: no cover - defensive guard
        raise ValueError(f"unsupported D2-SMALL-EMOTION decision: {decision}")

    _write_frame(registry, artifacts["small_emotion_event_registry"])
    _write_frame(counts, artifacts["subset_counts"])
    _write_frame(car, artifacts["car_window_panel"])
    _write_frame(placebo, artifacts["placebo_report"])
    _write_frame(stale_guard, artifacts["stale_price_guard_report"])
    _write_frame(capacity_guard, artifacts["adv_capacity_guard_report"])
    _write_frame(cost, artifacts["cost_spread_pregate"])
    _write_frame(delisting_audit, artifacts["delisting_retention_audit"])
    _write_frame(diagnostics, artifacts["continuation_reversal_diagnostics"])
    _write_frame(matched, artifacts["matched_control_panel"])
    _write_frame(coverage_no_view, artifacts["coverage_no_view_report"])
    _write_frame(bucket_report, artifacts["sector_size_liquidity_bucket_report"])

    summary = _summary(
        overall_decision=decision,
        allow_d3_charter_for=allow_d3,
        prices=prepared,
        event_registry=registry,
        car=car,
        placebo=placebo,
        coverage=coverage,
    )
    _write_summary_and_report(artifacts, summary, coverage, counts, car, placebo)
    return SmallEmotionD2Result(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "data_coverage_report": output_path / "data_coverage_report.json",
        "small_emotion_event_registry": output_path / "small_emotion_event_registry.csv",
        "subset_counts": output_path / "subset_counts.csv",
        "sector_size_liquidity_bucket_report": output_path / "sector_size_liquidity_bucket_report.csv",
        "stale_price_guard_report": output_path / "stale_price_guard_report.csv",
        "adv_capacity_guard_report": output_path / "adv_capacity_guard_report.csv",
        "cost_spread_pregate": output_path / "cost_spread_pregate.csv",
        "delisting_retention_audit": output_path / "delisting_retention_audit.csv",
        "car_window_panel": output_path / "car_window_panel.csv",
        "continuation_reversal_diagnostics": output_path / "continuation_reversal_diagnostics.csv",
        "matched_control_panel": output_path / "matched_control_panel.csv",
        "placebo_report": output_path / "placebo_report.csv",
        "coverage_no_view_report": output_path / "coverage_no_view_report.csv",
        "d2_small_emotion_summary": output_path / "d2_small_emotion_summary.json",
        "d2_small_emotion_observability_report": output_path / "d2_small_emotion_observability_report.md",
    }


def _read_csv(path: Path, *, nrows: int | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, nrows=nrows)
    except EmptyDataError:
        return pd.DataFrame()


def _data_coverage_report(
    *,
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    delistings: pd.DataFrame,
    price_panel_path: Path,
    benchmark_panel_path: Path,
    delisting_path: Path,
    max_rows: int | None,
) -> dict[str, object]:
    required_price = {"date", "adjusted_close", "volume", "market_cap"}
    if "asset_id" not in prices.columns and "ticker" not in prices.columns and "permno" not in prices.columns:
        required_price.add("asset_id")
    if "return" not in prices.columns and "raw_close" not in prices.columns:
        required_price.add("return")
    if "dollar_volume" not in prices.columns and not {"adjusted_close", "volume"}.issubset(prices.columns):
        required_price.add("dollar_volume")
    missing_price = sorted(column for column in required_price if column not in prices.columns)
    missing_benchmark = sorted(column for column in {"date", "return"}.difference(benchmark.columns))
    missing_delisting = sorted(column for column in {"asset_id", "delisting_date", "delisting_return"}.difference(delistings.columns))
    data_status = (
        "available"
        if not prices.empty and not benchmark.empty and not delistings.empty and not missing_price and not missing_benchmark and not missing_delisting
        else "blocked_data_coverage"
    )
    return {
        "schema_version": "small_emotion_data_coverage_report.v1",
        "stage": "D2-SMALL-EMOTION-01",
        "price_panel_path": str(price_panel_path),
        "benchmark_panel_path": str(benchmark_panel_path),
        "delisting_path": str(delisting_path),
        "source_row_limit": max_rows,
        "price_row_count": int(len(prices)),
        "benchmark_row_count": int(len(benchmark)),
        "delisting_row_count": int(len(delistings)),
        "missing_required_price_columns": missing_price,
        "missing_required_benchmark_columns": missing_benchmark,
        "missing_required_delisting_columns": missing_delisting,
        "data_status": data_status,
        **GUARDS,
    }


def _prepare_price_panel(
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    *,
    min_history_observations: int,
    min_adv_dollars: float,
    small_cap_min_market_cap: float,
    small_cap_max_market_cap: float,
    large_cap_min_market_cap: float,
) -> pd.DataFrame:
    frame = prices.copy()
    if "asset_id" not in frame.columns:
        if "permno" in frame.columns:
            frame["asset_id"] = frame["permno"].astype(str)
        else:
            frame["asset_id"] = frame["ticker"].astype(str)
    frame["asset_id"] = frame["asset_id"].astype(str)
    if "ticker" not in frame.columns:
        frame["ticker"] = frame["asset_id"]
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"]).copy()
    for column in [
        "adjusted_close",
        "raw_close",
        "return",
        "volume",
        "market_cap",
        "dollar_volume",
        "bid_ask_spread",
        "share_code",
        "exchange_code",
    ]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "return" not in frame.columns or frame["return"].isna().all():
        frame = frame.sort_values(["asset_id", "date"]).copy()
        frame["return"] = frame.groupby("asset_id")["adjusted_close"].pct_change()
    if "dollar_volume" not in frame.columns:
        frame["dollar_volume"] = frame["adjusted_close"] * frame["volume"]
    if "bid_ask_spread" not in frame.columns:
        high = pd.to_numeric(frame.get("high", np.nan), errors="coerce")
        low = pd.to_numeric(frame.get("low", np.nan), errors="coerce")
        close = pd.to_numeric(frame["adjusted_close"], errors="coerce")
        frame["bid_ask_spread"] = ((high - low) / close.replace(0, np.nan)).clip(lower=0.0)
    frame = frame.sort_values(["asset_id", "date"]).copy()
    group = frame.groupby("asset_id", sort=False)
    frame["history_observations"] = group.cumcount()
    frame["prev_close"] = group["adjusted_close"].shift(1)
    frame["unchanged_close"] = (frame["adjusted_close"] == frame["prev_close"]).fillna(False)
    frame["zero_volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0) <= 0.0
    frame["stale_roll_5"] = group["unchanged_close"].transform(lambda values: values.rolling(5, min_periods=1).sum())
    frame["adv20"] = group["dollar_volume"].transform(lambda values: values.shift(1).rolling(20, min_periods=5).mean())
    frame["volume20"] = group["volume"].transform(lambda values: values.shift(1).rolling(20, min_periods=5).mean())
    frame["abnormal_volume"] = frame["volume"] / frame["volume20"].replace(0.0, np.nan)

    benchmark_frame = benchmark.copy()
    benchmark_frame["date"] = pd.to_datetime(benchmark_frame["date"], errors="coerce")
    benchmark_frame["benchmark_return"] = pd.to_numeric(benchmark_frame["return"], errors="coerce")
    frame = frame.merge(benchmark_frame[["date", "benchmark_return"]].drop_duplicates("date"), on="date", how="left")

    common_share = pd.Series(True, index=frame.index)
    if "common_share" in frame.columns:
        common_share &= frame["common_share"].astype(str).str.lower().isin({"true", "1", "yes"})
    if "share_code" in frame.columns:
        common_share &= frame["share_code"].astype(str).isin({"10", "11", "10.0", "11.0"})
    frame["common_share_pass"] = common_share
    frame["small_cap_investable_universe"] = (
        frame["common_share_pass"]
        & frame["market_cap"].between(float(small_cap_min_market_cap), float(small_cap_max_market_cap), inclusive="both")
    )
    frame["large_cap_control_universe"] = frame["market_cap"] >= float(large_cap_min_market_cap)
    frame["size_bucket"] = np.where(
        frame["small_cap_investable_universe"],
        "small_cap_investable",
        np.where(frame["large_cap_control_universe"], "large_cap_control", "outside_universe"),
    )
    frame["liquidity_bucket"] = _datewise_bucket(frame, "adv20", labels=["low", "mid", "high"])
    frame["spread_bucket"] = _datewise_bucket(frame, "bid_ask_spread", labels=["tight", "medium", "wide"])
    frame["shock_abs"] = frame["return"].abs()
    frame["weak_liquidity"] = (
        frame["liquidity_bucket"].eq("low")
        | frame["spread_bucket"].eq("wide")
        | (frame["abnormal_volume"] <= 0.75)
    )
    frame["no_view_reason"] = _no_view_reason(frame, min_history_observations, min_adv_dollars)
    frame["coverage_state"] = np.where(frame["no_view_reason"].eq(""), "active_view", "no_view")
    for key, value in GUARDS.items():
        frame[key] = value
    return frame


def _datewise_bucket(frame: pd.DataFrame, column: str, *, labels: list[str]) -> pd.Series:
    output = pd.Series("unavailable", index=frame.index, dtype="object")
    if column not in frame.columns:
        return output
    for _date, group in frame.groupby("date", sort=False):
        values = pd.to_numeric(group[column], errors="coerce")
        if values.notna().sum() < len(labels):
            output.loc[group.index] = "unavailable"
            continue
        try:
            output.loc[group.index] = pd.qcut(values.rank(method="first"), len(labels), labels=labels).astype(str)
        except ValueError:
            output.loc[group.index] = "unavailable"
    return output


def _no_view_reason(frame: pd.DataFrame, min_history_observations: int, min_adv_dollars: float) -> pd.Series:
    reasons = pd.Series("", index=frame.index, dtype="object")
    reasons = reasons.mask(frame["adjusted_close"].isna() | frame["return"].isna() | frame["volume"].isna(), "missing_price_return_or_volume")
    reasons = reasons.mask(reasons.eq("") & ~frame["common_share_pass"], "excluded_non_common_share")
    reasons = reasons.mask(reasons.eq("") & ~frame["small_cap_investable_universe"], "outside_small_cap_investable_universe")
    reasons = reasons.mask(reasons.eq("") & (frame["history_observations"] < int(min_history_observations)), "insufficient_recent_trading_observations")
    reasons = reasons.mask(reasons.eq("") & (frame["zero_volume"] | (frame["stale_roll_5"] >= 4)), "stale_price_or_zero_volume")
    reasons = reasons.mask(reasons.eq("") & frame["adv20"].isna(), "missing_adv_capacity_input")
    reasons = reasons.mask(reasons.eq("") & (frame["adv20"] < float(min_adv_dollars)), "below_min_adv_capacity")
    reasons = reasons.mask(reasons.eq("") & frame["benchmark_return"].isna(), "missing_benchmark_return")
    return reasons


def _build_event_registry(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    active = frame[frame["coverage_state"].eq("active_view")].copy()
    panic = active[(active["return"] <= -0.08) & (active["abnormal_volume"] >= 2.0)].copy()
    fomo = active[(active["return"] >= 0.08) & (active["abnormal_volume"] >= 2.0)].copy()
    vacuum = active[(active["shock_abs"] >= 0.10) & (active["weak_liquidity"])].copy()
    used_index: set[int] = set()
    for subset, subset_frame in [
        ("panic_overreaction_candidate", panic),
        ("fomo_continuation_candidate", fomo),
        ("liquidity_vacuum_reversal_candidate", vacuum),
    ]:
        for index, row in subset_frame.iterrows():
            if int(index) in used_index and subset != "liquidity_vacuum_reversal_candidate":
                continue
            used_index.add(int(index))
            rows.append(_event_row(row, subset, "active_view", ""))
    guard = frame[
        frame["coverage_state"].eq("no_view")
        & (
            frame["return"].abs().fillna(0.0).ge(0.08)
            | frame["zero_volume"]
            | frame["stale_roll_5"].fillna(0.0).ge(4)
        )
    ].head(500)
    for _index, row in guard.iterrows():
        rows.append(_event_row(row, "no_view_guard_row", "no_view", str(row["no_view_reason"])))
    registry = pd.DataFrame(rows)
    if registry.empty:
        return pd.DataFrame(columns=_registry_columns())
    registry = registry.sort_values(["date", "asset_id", "event_subset"]).reset_index(drop=True)
    registry["event_id"] = [f"small_emotion_{idx:08d}" for idx in range(len(registry))]
    return registry[_registry_columns()]


def _event_row(row: object, subset: str, coverage_state: str, no_view_reason: str) -> dict[str, object]:
    shock_return = _row_value(row, "return")
    return {
        "schema_version": "small_emotion_event_registry.v1",
        "event_id": "",
        "asset_id": str(_row_value(row, "asset_id")),
        "ticker": str(_row_value(row, "ticker")),
        "date": pd.Timestamp(_row_value(row, "date")).date().isoformat(),
        "event_month": pd.Timestamp(_row_value(row, "date")).strftime("%Y-%m"),
        "event_subset": subset,
        "coverage_state": coverage_state,
        "no_view_reason": no_view_reason,
        "shock_return": _float_or_nan(shock_return),
        "abs_shock_return": _float_or_nan(_row_value(row, "shock_abs")),
        "abnormal_volume": _float_or_nan(_row_value(row, "abnormal_volume")),
        "market_cap": _float_or_nan(_row_value(row, "market_cap")),
        "adv20": _float_or_nan(_row_value(row, "adv20")),
        "capacity_dollars": _float_or_nan(_row_value(row, "adv20")),
        "bid_ask_spread": _float_or_nan(_row_value(row, "bid_ask_spread")),
        "sector": str(_row_value(row, "sector", "") or ""),
        "industry": str(_row_value(row, "industry", "") or ""),
        "size_bucket": str(_row_value(row, "size_bucket")),
        "liquidity_bucket": str(_row_value(row, "liquidity_bucket")),
        "spread_bucket": str(_row_value(row, "spread_bucket")),
        "stale_price_flag": bool(_row_value(row, "zero_volume") or _row_value(row, "stale_roll_5") >= 4),
        "zero_volume_flag": bool(_row_value(row, "zero_volume")),
        "weak_liquidity_flag": bool(_row_value(row, "weak_liquidity")),
        "diagnostic_only": coverage_state != "active_view",
        **GUARDS,
    }


def _row_value(row: object, key: str, default: object = np.nan) -> object:
    if isinstance(row, pd.Series):
        return row.get(key, default)
    if hasattr(row, key):
        return getattr(row, key)
    return default


def _float_or_nan(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _registry_columns() -> list[str]:
    return [
        "schema_version",
        "event_id",
        "asset_id",
        "ticker",
        "date",
        "event_month",
        "event_subset",
        "coverage_state",
        "no_view_reason",
        "shock_return",
        "abs_shock_return",
        "abnormal_volume",
        "market_cap",
        "adv20",
        "capacity_dollars",
        "bid_ask_spread",
        "sector",
        "industry",
        "size_bucket",
        "liquidity_bucket",
        "spread_bucket",
        "stale_price_flag",
        "zero_volume_flag",
        "weak_liquidity_flag",
        "diagnostic_only",
        *GUARDS.keys(),
    ]


def _car_window_panel(registry: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    active_events = registry[registry["coverage_state"].eq("active_view")].copy()
    if active_events.empty:
        return pd.DataFrame(columns=_car_columns())
    by_asset = {asset: group.sort_values("date").reset_index(drop=True) for asset, group in prices.groupby("asset_id")}
    rows: list[dict[str, object]] = []
    for event in active_events.itertuples(index=False):
        asset_panel = by_asset.get(str(event.asset_id))
        if asset_panel is None or asset_panel.empty:
            continue
        positions = asset_panel.index[asset_panel["date"].eq(pd.Timestamp(event.date))].tolist()
        if not positions:
            continue
        pos = int(positions[0])
        for window, offsets in WINDOWS.items():
            asset_return, benchmark_return, status = _window_return(asset_panel, pos, offsets)
            abnormal = asset_return - benchmark_return if pd.notna(asset_return) and pd.notna(benchmark_return) else np.nan
            rows.append(
                {
                    "schema_version": "small_emotion_car_window_panel.v1",
                    "event_id": event.event_id,
                    "asset_id": event.asset_id,
                    "ticker": event.ticker,
                    "date": event.date,
                    "event_month": event.event_month,
                    "event_subset": event.event_subset,
                    "window": window,
                    "label_status": status,
                    "asset_return": asset_return,
                    "benchmark_return": benchmark_return,
                    "abnormal_return": abnormal,
                    "directional_return": _directional_return(event.event_subset, event.shock_return, abnormal),
                    "coverage_state": "observed" if status == "observed" else "unavailable",
                    **GUARDS,
                }
            )
    car = pd.DataFrame(rows)
    if car.empty:
        return pd.DataFrame(columns=_car_columns())
    grouped = (
        car.groupby(["event_subset", "window", "label_status"], dropna=False)
        .agg(
            observed_event_count=("abnormal_return", "count"),
            mean_asset_return=("asset_return", "mean"),
            mean_benchmark_return=("benchmark_return", "mean"),
            mean_abnormal_return=("abnormal_return", "mean"),
            mean_directional_return=("directional_return", "mean"),
            event_month_count=("event_month", "nunique"),
            issuer_cluster_count=("asset_id", "nunique"),
        )
        .reset_index()
    )
    total = car.groupby(["event_subset", "window"], dropna=False)["event_id"].nunique().rename("window_event_count").reset_index()
    grouped = grouped.merge(total, on=["event_subset", "window"], how="left")
    grouped["schema_version"] = "small_emotion_car_window_panel.v1"
    grouped["label_coverage_share"] = grouped["observed_event_count"] / grouped["window_event_count"].replace(0, np.nan)
    for key, value in GUARDS.items():
        grouped[key] = value
    return grouped[_car_columns()]


def _window_return(asset_panel: pd.DataFrame, pos: int, offsets: tuple[int, int]) -> tuple[float, float, str]:
    start_offset, end_offset = offsets
    start = pos + start_offset
    end = pos + end_offset
    if start < 0 or end >= len(asset_panel) or start > end:
        return np.nan, np.nan, "unavailable_missing_return_window"
    window = asset_panel.iloc[start : end + 1]
    asset_returns = pd.to_numeric(window["return"], errors="coerce")
    benchmark_returns = pd.to_numeric(window["benchmark_return"], errors="coerce")
    if asset_returns.isna().any() or benchmark_returns.isna().any():
        return np.nan, np.nan, "unavailable_missing_return_window"
    return float(np.prod(1.0 + asset_returns) - 1.0), float(np.prod(1.0 + benchmark_returns) - 1.0), "observed"


def _directional_return(event_subset: str, shock_return: float, abnormal_return: float) -> float:
    if pd.isna(abnormal_return):
        return np.nan
    if event_subset == "liquidity_vacuum_reversal_candidate":
        sign = -1.0 if shock_return > 0 else 1.0
        return float(sign * abnormal_return)
    return float(abnormal_return)


def _car_columns() -> list[str]:
    return [
        "schema_version",
        "event_subset",
        "window",
        "label_status",
        "observed_event_count",
        "mean_asset_return",
        "mean_benchmark_return",
        "mean_abnormal_return",
        "mean_directional_return",
        "event_month_count",
        "issuer_cluster_count",
        "window_event_count",
        "label_coverage_share",
        *GUARDS.keys(),
    ]


def _placebo_report(registry: pd.DataFrame, prices: pd.DataFrame, car: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    active_subsets = [
        "panic_overreaction_candidate",
        "fomo_continuation_candidate",
        "liquidity_vacuum_reversal_candidate",
    ]
    for subset in active_subsets:
        live = _live_directional(car, subset)
        subset_events = registry[(registry["event_subset"] == subset) & (registry["coverage_state"] == "active_view")]
        for name, value in {
            "same_coverage_random": _control_directional_return(prices, subset_events, "random"),
            "shift_minus_5": _shifted_directional_return(prices, subset_events, -5, subset),
            "shift_plus_5": _shifted_directional_return(prices, subset_events, 5, subset),
            "shift_minus_10": _shifted_directional_return(prices, subset_events, -10, subset),
            "shift_plus_10": _shifted_directional_return(prices, subset_events, 10, subset),
            "large_cap_matched_shock": _large_cap_matched_directional(prices, subset),
            "sector_size_liquidity_matched_non_shock": _control_directional_return(prices, subset_events, "non_shock"),
            "stale_price_matched": _stale_matched_directional(prices),
            "adv_capacity_matched": _control_directional_return(prices, subset_events, "low_adv"),
        }.items():
            dominates = bool(pd.notna(value) and pd.notna(live) and value >= live and live > 0.0)
            rows.append(
                {
                    "schema_version": "small_emotion_placebo_report.v1",
                    "event_subset": subset,
                    "placebo_name": name,
                    "live_post_1_22_directional_return": live,
                    "placebo_directional_return": value,
                    "placebo_dominates_live": dominates,
                    "status": "fail" if dominates else "pass",
                    **GUARDS,
                }
            )
    return pd.DataFrame(rows)


def _live_directional(car: pd.DataFrame, subset: str) -> float:
    frame = car[(car["event_subset"] == subset) & (car["window"] == "post_1_22") & (car["label_status"] == "observed")]
    if frame.empty:
        return np.nan
    return float(frame["mean_directional_return"].iloc[0])


def _shifted_directional_return(prices: pd.DataFrame, events: pd.DataFrame, shift: int, subset: str) -> float:
    if events.empty:
        return np.nan
    by_asset = {asset: group.sort_values("date").reset_index(drop=True) for asset, group in prices.groupby("asset_id")}
    values = []
    for event in events.itertuples(index=False):
        asset_panel = by_asset.get(str(event.asset_id))
        if asset_panel is None:
            continue
        positions = asset_panel.index[asset_panel["date"].eq(pd.Timestamp(event.date))].tolist()
        if not positions:
            continue
        shifted_pos = int(positions[0]) + int(shift)
        asset_return, benchmark_return, status = _window_return(asset_panel, shifted_pos, WINDOWS["post_1_22"])
        if status != "observed":
            continue
        values.append(_directional_return(subset, event.shock_return, asset_return - benchmark_return))
    return float(np.nanmean(values)) if values else np.nan


def _control_directional_return(prices: pd.DataFrame, events: pd.DataFrame, mode: str) -> float:
    if events.empty:
        return np.nan
    candidates = prices[prices["coverage_state"].eq("active_view")].copy()
    if mode == "non_shock":
        candidates = candidates[candidates["shock_abs"] < 0.03]
    elif mode == "low_adv":
        candidates = candidates[candidates["liquidity_bucket"].eq("low")]
    else:
        candidates = candidates[candidates["shock_abs"] < 0.04]
    candidates = candidates.sort_values(["date", "asset_id"]).head(max(len(events) * 2, len(events)))
    return _mean_future_abnormal(candidates)


def _large_cap_matched_directional(prices: pd.DataFrame, subset: str) -> float:
    frame = prices[prices["large_cap_control_universe"] & prices["return"].notna()].copy()
    if subset == "panic_overreaction_candidate":
        frame = frame[frame["return"] <= -0.08]
    elif subset == "fomo_continuation_candidate":
        frame = frame[frame["return"] >= 0.08]
    else:
        frame = frame[frame["return"].abs() >= 0.10]
    return _mean_future_abnormal(frame)


def _stale_matched_directional(prices: pd.DataFrame) -> float:
    frame = prices[prices["zero_volume"] | (prices["stale_roll_5"] >= 4)].copy()
    return _mean_future_abnormal(frame)


def _mean_future_abnormal(frame: pd.DataFrame) -> float:
    if frame.empty:
        return np.nan
    registry = pd.DataFrame(
        [
            {
                "asset_id": row["asset_id"],
                "date": pd.Timestamp(row["date"]).date().isoformat(),
                "event_subset": "control",
                "shock_return": row["return"],
            }
            for _index, row in frame.head(250).iterrows()
        ]
    )
    if registry.empty:
        return np.nan
    by_asset = {asset: group.sort_values("date").reset_index(drop=True) for asset, group in frame.groupby("asset_id")}
    values = []
    for event in registry.itertuples(index=False):
        asset_panel = by_asset.get(str(event.asset_id))
        if asset_panel is None:
            continue
        positions = asset_panel.index[asset_panel["date"].eq(pd.Timestamp(event.date))].tolist()
        if not positions:
            continue
        asset_return, benchmark_return, status = _window_return(asset_panel, int(positions[0]), WINDOWS["post_1_22"])
        if status == "observed":
            values.append(asset_return - benchmark_return)
    return float(np.nanmean(values)) if values else np.nan


def _subset_counts(registry: pd.DataFrame, car: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for subset, group in registry.groupby("event_subset", dropna=False):
        observed = car[
            (car["event_subset"] == subset)
            & (car["window"] == "post_1_22")
            & (car["label_status"] == "observed")
        ]
        rows.append(
            {
                "schema_version": "small_emotion_subset_counts.v1",
                "event_subset": subset,
                "event_count": int(len(group)),
                "active_event_count": int(group["coverage_state"].eq("active_view").sum()),
                "no_view_count": int(group["coverage_state"].eq("no_view").sum()),
                "event_month_count": int(group.loc[group["coverage_state"].eq("active_view"), "event_month"].nunique()),
                "issuer_cluster_count": int(group.loc[group["coverage_state"].eq("active_view"), "asset_id"].nunique()),
                "observed_post_1_22_count": int(observed["observed_event_count"].sum()) if not observed.empty else 0,
                "label_coverage_share": float(observed["label_coverage_share"].max()) if not observed.empty else 0.0,
                **GUARDS,
            }
        )
    return pd.DataFrame(rows)


def _stale_price_guard(prices: pd.DataFrame, registry: pd.DataFrame, placebo: pd.DataFrame) -> pd.DataFrame:
    stale_rows = int((prices["zero_volume"] | (prices["stale_roll_5"] >= 4)).sum()) if not prices.empty else 0
    active_events = registry[registry["coverage_state"].eq("active_view")]
    stale_placebo_dominates = bool(
        not placebo.empty
        and placebo[placebo["placebo_name"].eq("stale_price_matched")]["placebo_dominates_live"].fillna(False).any()
    )
    return pd.DataFrame(
        [
            {
                "schema_version": "small_emotion_stale_price_guard_report.v1",
                "stale_price_guard_generated": True,
                "stale_or_zero_volume_row_count": stale_rows,
                "active_event_count": int(len(active_events)),
                "stale_placebo_dominates_live": stale_placebo_dominates,
                "stale_rows_marked_no_view": True,
                **GUARDS,
            }
        ]
    )


def _capacity_guard(prices: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    active = registry[registry["coverage_state"].eq("active_view")].copy()
    low_capacity_share = float((pd.to_numeric(active.get("adv20", pd.Series(dtype=float)), errors="coerce") < 250_000.0).mean()) if not active.empty else 0.0
    return pd.DataFrame(
        [
            {
                "schema_version": "small_emotion_adv_capacity_guard_report.v1",
                "capacity_guard_generated": True,
                "active_event_count": int(len(active)),
                "low_capacity_event_share": low_capacity_share,
                "missing_adv_row_count": int(prices["adv20"].isna().sum()) if "adv20" in prices else 0,
                "capacity_guard_fatal": bool(low_capacity_share > 0.50),
                **GUARDS,
            }
        ]
    )


def _cost_spread_pregate(registry: pd.DataFrame) -> pd.DataFrame:
    active = registry[registry["coverage_state"].eq("active_view")].copy()
    spread = pd.to_numeric(active.get("bid_ask_spread", pd.Series(dtype=float)), errors="coerce")
    mean_spread = float(spread.mean()) if spread.notna().any() else np.nan
    return pd.DataFrame(
        [
            {
                "schema_version": "small_emotion_cost_spread_pregate.v1",
                "spread_proxy_available": bool(spread.notna().any()),
                "mean_bid_ask_spread": mean_spread,
                "wide_spread_event_share": float((spread > 0.05).mean()) if spread.notna().any() else np.nan,
                "cost_liquidity_gate_fatal": bool(pd.notna(mean_spread) and mean_spread > 0.05),
                **GUARDS,
            }
        ]
    )


def _delisting_retention_audit(delistings: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    delisted_assets = set(delistings.get("asset_id", pd.Series(dtype=str)).astype(str))
    event_assets = set(registry.get("asset_id", pd.Series(dtype=str)).astype(str))
    return pd.DataFrame(
        [
            {
                "schema_version": "small_emotion_delisting_retention_audit.v1",
                "delisting_file_available": not delistings.empty,
                "delisting_row_count": int(len(delistings)),
                "event_assets_with_delisting_record": int(len(delisted_assets & event_assets)),
                "delisting_bias_guard_generated": True,
                **GUARDS,
            }
        ]
    )


def _continuation_reversal_diagnostics(registry: pd.DataFrame, car: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for subset in [
        "panic_overreaction_candidate",
        "fomo_continuation_candidate",
        "liquidity_vacuum_reversal_candidate",
    ]:
        post = _live_directional(car, subset)
        pre = _car_value(car, subset, "pre_5_1", "mean_directional_return")
        rows.append(
            {
                "schema_version": "small_emotion_continuation_reversal_diagnostics.v1",
                "event_subset": subset,
                "post_1_22_directional_return": post,
                "pre_5_1_directional_return": pre,
                "live_post_direction_matches_preregistered_mechanism": bool(pd.notna(post) and post > 0.0),
                "pre_event_dominates_post": bool(pd.notna(pre) and pd.notna(post) and pre > post),
                **GUARDS,
            }
        )
    return pd.DataFrame(rows)


def _car_value(car: pd.DataFrame, subset: str, window: str, column: str) -> float:
    frame = car[(car["event_subset"] == subset) & (car["window"] == window) & (car["label_status"] == "observed")]
    if frame.empty:
        return np.nan
    return float(frame[column].iloc[0])


def _matched_control_panel(placebo: pd.DataFrame) -> pd.DataFrame:
    frame = placebo.copy()
    if frame.empty:
        return pd.DataFrame(columns=["schema_version", "event_subset", "control_type", "control_directional_return", *GUARDS.keys()])
    frame = frame.rename(columns={"placebo_name": "control_type", "placebo_directional_return": "control_directional_return"})
    frame["schema_version"] = "small_emotion_matched_control_panel.v1"
    return frame[["schema_version", "event_subset", "control_type", "control_directional_return", *GUARDS.keys()]]


def _coverage_no_view_report(registry: pd.DataFrame) -> pd.DataFrame:
    if registry.empty:
        return pd.DataFrame(columns=["schema_version", "no_view_reason", "row_count", *GUARDS.keys()])
    frame = (
        registry[registry["coverage_state"].eq("no_view")]
        .groupby("no_view_reason", dropna=False)
        .size()
        .rename("row_count")
        .reset_index()
    )
    frame["schema_version"] = "small_emotion_coverage_no_view_report.v1"
    for key, value in GUARDS.items():
        frame[key] = value
    return frame[["schema_version", "no_view_reason", "row_count", *GUARDS.keys()]]


def _bucket_report(registry: pd.DataFrame) -> pd.DataFrame:
    if registry.empty:
        return pd.DataFrame(columns=["schema_version", "event_subset", "sector", "size_bucket", "liquidity_bucket", "event_count", *GUARDS.keys()])
    frame = (
        registry.groupby(["event_subset", "sector", "size_bucket", "liquidity_bucket"], dropna=False)
        .size()
        .rename("event_count")
        .reset_index()
    )
    frame["schema_version"] = "small_emotion_sector_size_liquidity_bucket_report.v1"
    for key, value in GUARDS.items():
        frame[key] = value
    return frame[["schema_version", "event_subset", "sector", "size_bucket", "liquidity_bucket", "event_count", *GUARDS.keys()]]


def _decision(
    *,
    counts: pd.DataFrame,
    diagnostics: pd.DataFrame,
    placebo: pd.DataFrame,
    stale_guard: pd.DataFrame,
    capacity_guard: pd.DataFrame,
    minimum_subset_events: int,
    minimum_event_month_count: int,
    minimum_label_coverage_share: float,
) -> tuple[str, list[str]]:
    if counts.empty:
        return "hold_insufficient_sample", []
    if bool(capacity_guard.get("capacity_guard_fatal", pd.Series([False])).iloc[0]):
        return "blocked_cost_liquidity", []
    candidates = []
    for subset in SUBSET_TO_D3:
        count_row = counts[counts["event_subset"] == subset]
        diag_row = diagnostics[diagnostics["event_subset"] == subset]
        if count_row.empty or diag_row.empty:
            continue
        enough_sample = (
            int(count_row["active_event_count"].iloc[0]) >= int(minimum_subset_events)
            and int(count_row["event_month_count"].iloc[0]) >= int(minimum_event_month_count)
            and float(count_row["label_coverage_share"].iloc[0]) >= float(minimum_label_coverage_share)
        )
        if not enough_sample:
            continue
        direction_ok = bool(diag_row["live_post_direction_matches_preregistered_mechanism"].iloc[0])
        pre_dominates = bool(diag_row["pre_event_dominates_post"].iloc[0])
        placebo_dominates = bool(
            placebo[(placebo["event_subset"] == subset)]["placebo_dominates_live"].fillna(False).any()
        )
        if direction_ok and not pre_dominates and not placebo_dominates:
            candidates.append((subset, float(diag_row["post_1_22_directional_return"].iloc[0])))
    if candidates:
        candidates = sorted(candidates, key=lambda item: item[1], reverse=True)
        subset = candidates[0][0]
        if subset == "panic_overreaction_candidate":
            return "observable_panic_reversal", [SUBSET_TO_D3[subset]]
        if subset == "fomo_continuation_candidate":
            return "observable_fomo_continuation", [SUBSET_TO_D3[subset]]
        return "observable_liquidity_vacuum_reversal", [SUBSET_TO_D3[subset]]
    sample_rows = counts[counts["event_subset"].isin(SUBSET_TO_D3)]
    if sample_rows.empty or not (
        (sample_rows["active_event_count"] >= minimum_subset_events)
        & (sample_rows["event_month_count"] >= minimum_event_month_count)
    ).any():
        return "hold_insufficient_sample", []
    stale_rows = placebo[
        placebo["placebo_name"].eq("stale_price_matched")
        & placebo["placebo_dominates_live"].fillna(False)
    ]
    if not stale_rows.empty:
        return "blocked_stale_price", []
    if bool(placebo["placebo_dominates_live"].fillna(False).any()) if not placebo.empty else False:
        return "blocked_placebo_dominance", []
    return "not_observable", []


def _summary(
    *,
    overall_decision: str,
    allow_d3_charter_for: list[str],
    prices: pd.DataFrame,
    event_registry: pd.DataFrame,
    car: pd.DataFrame,
    placebo: pd.DataFrame,
    coverage: Mapping[str, object],
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_d2_summary.v1",
        "stage": "D2-SMALL-EMOTION-01",
        "candidate_id": "small_cap_shock_conditioned_emotion_liquidity_observability",
        "active_mainline": "D2-SMALL-EMOTION-01",
        "d2_insider_02_state": "stopped_before_d3",
        "d2_insider_02_stop_reason": "plan_flag_source_locator_repair_failed",
        "d2_8k_01_state": "hold_pending_data_coverage",
        "d2_8k_01_note": "fixture observable only for research-routing purposes; no D3 open",
        "overall_decision": overall_decision,
        "allow_d3_charter_for": allow_d3_charter_for[:1],
        "price_row_count": int(len(prices)),
        "event_count": int(len(event_registry)),
        "active_event_count": int(event_registry["coverage_state"].eq("active_view").sum()) if "coverage_state" in event_registry else 0,
        "no_view_count": int(event_registry["coverage_state"].eq("no_view").sum()) if "coverage_state" in event_registry else 0,
        "car_row_count": int(len(car)),
        "placebo_row_count": int(len(placebo)),
        "data_status": coverage.get("data_status"),
        **GUARDS,
    }


def _write_summary_and_report(
    artifacts: Mapping[str, Path],
    summary: Mapping[str, object],
    coverage: Mapping[str, object],
    counts: pd.DataFrame,
    car: pd.DataFrame,
    placebo: pd.DataFrame,
) -> None:
    artifacts["d2_small_emotion_summary"].write_text(
        json.dumps(dict(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["d2_small_emotion_observability_report"].write_text(
        _render_report(summary, coverage, counts, car, placebo),
        encoding="utf-8",
    )


def _render_report(
    summary: Mapping[str, object],
    coverage: Mapping[str, object],
    counts: pd.DataFrame,
    car: pd.DataFrame,
    placebo: pd.DataFrame,
) -> str:
    lines = [
        "# D2-SMALL-EMOTION-01 Observability Report",
        "",
        "no-formula observability only",
        "not alpha evidence",
        "",
        f"Decision: `{summary['overall_decision']}`",
        f"Allowed D3 charter for: `{summary['allow_d3_charter_for']}`",
        "",
        "This report tests whether small-cap shock-conditioned emotion / liquidity mechanisms have timestamp-safe observable footprints.",
        "It does not write a formula, MeasurementSpec, Q1, Q2, expected-return panel, optimizer input, portfolio construction, Alpha Registry update, paper workflow, broker/order workflow, live workflow, or production approval.",
        "Missing coverage remains explicit no_view / abstain and is never encoded as zero alpha.",
        "",
        "## Research Routing",
        "",
        "- D2-INSIDER-02 sell contrast: stopped_before_d3 due to plan_flag_source_locator_repair_failed.",
        "- D2-8K-01: hold_pending_data_coverage; no D3 is open from the real archive path.",
        "- Active mainline: D2-SMALL-EMOTION-01.",
        "",
        "## Data Coverage",
        "",
        f"- Data status: `{coverage.get('data_status')}`",
        f"- Price rows: `{coverage.get('price_row_count')}`",
        f"- Benchmark rows: `{coverage.get('benchmark_row_count')}`",
        f"- Delisting rows: `{coverage.get('delisting_row_count')}`",
        "",
        "## Subset Counts",
        "",
    ]
    if counts.empty:
        lines.append("No subset counts were available.")
    else:
        for row in counts.itertuples(index=False):
            lines.append(
                f"- `{row.event_subset}`: events={row.event_count}, active={row.active_event_count}, "
                f"months={row.event_month_count}, label_coverage={row.label_coverage_share:.3f}"
            )
    lines.extend(["", "## CAR And Placebo", ""])
    post = car[(car["window"] == "post_1_22") & (car["label_status"] == "observed")] if not car.empty else pd.DataFrame()
    if post.empty:
        lines.append("No observed post_1_22 CAR rows were available.")
    else:
        for row in post.itertuples(index=False):
            lines.append(
                f"- `{row.event_subset}` post_1_22 directional return: `{row.mean_directional_return:.6f}`"
            )
    if not placebo.empty:
        fail_count = int(placebo["placebo_dominates_live"].fillna(False).sum())
        lines.append(f"- Placebo dominance failures: `{fail_count}`")
    lines.extend(
        [
            "",
            "## Boundary Flags",
            "",
            "- q1_entry_allowed=false",
            "- q2_entry_allowed=false",
            "- optimizer_entry_allowed=false",
            "- alpha_registry_update_allowed=false",
            "- broker_order_path_opened=false",
            "- production_approval_claimed=false",
            "",
        ]
    )
    return "\n".join(lines)


def _empty_outputs(artifacts: Mapping[str, Path]) -> dict[str, pd.DataFrame]:
    registry = pd.DataFrame(columns=_registry_columns())
    counts = pd.DataFrame(columns=["schema_version", "event_subset", "event_count", *GUARDS.keys()])
    car = pd.DataFrame(columns=_car_columns())
    placebo = pd.DataFrame(columns=["schema_version", "event_subset", "placebo_name", "placebo_dominates_live", *GUARDS.keys()])
    for key, frame in {
        "small_emotion_event_registry": registry,
        "subset_counts": counts,
        "car_window_panel": car,
        "placebo_report": placebo,
        "stale_price_guard_report": pd.DataFrame(
            [{"schema_version": "small_emotion_stale_price_guard_report.v1", "stale_price_guard_generated": True, **GUARDS}]
        ),
        "adv_capacity_guard_report": pd.DataFrame(
            [{"schema_version": "small_emotion_adv_capacity_guard_report.v1", "capacity_guard_generated": True, **GUARDS}]
        ),
        "cost_spread_pregate": pd.DataFrame([{"schema_version": "small_emotion_cost_spread_pregate.v1", **GUARDS}]),
        "delisting_retention_audit": pd.DataFrame([{"schema_version": "small_emotion_delisting_retention_audit.v1", **GUARDS}]),
        "continuation_reversal_diagnostics": pd.DataFrame(
            [{"schema_version": "small_emotion_continuation_reversal_diagnostics.v1", **GUARDS}]
        ),
        "matched_control_panel": pd.DataFrame([{"schema_version": "small_emotion_matched_control_panel.v1", **GUARDS}]),
        "coverage_no_view_report": pd.DataFrame([{"schema_version": "small_emotion_coverage_no_view_report.v1", **GUARDS}]),
        "sector_size_liquidity_bucket_report": pd.DataFrame(
            [{"schema_version": "small_emotion_sector_size_liquidity_bucket_report.v1", **GUARDS}]
        ),
    }.items():
        _write_frame(frame, artifacts[key])
    return {"registry": registry, "counts": counts, "car": car, "placebo": placebo}


def _write_frame(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
