"""Q1 falsifier/OOS review for frozen small-cap emotion MeasurementSpecs."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from pandas.errors import EmptyDataError

from .small_emotion_d2 import _data_coverage_report, _read_csv, _window_return
from .small_emotion_exploratory_sweep import EXPLORATORY_GUARDS


STAGE = "Q1-SMALL-EMOTION-01"
WINDOWS: dict[str, tuple[int, int]] = {
    "post_1_5": (1, 5),
    "post_1_10": (1, 10),
    "post_1_22": (1, 22),
    "post_1_44": (1, 44),
    "pre_5_1": (-5, -1),
    "pre_10_1": (-10, -1),
    "pre_20_1": (-20, -1),
}
Q1_PRICE_COLUMNS = {
    "permno",
    "asset_id",
    "ticker",
    "date",
    "raw_open",
    "adjusted_open",
    "adjusted_close",
    "volume",
    "return",
    "market_cap",
    "dollar_volume",
    "bid_ask_spread",
    "high",
    "low",
    "share_code",
    "exchange_code",
    "common_share",
    "sector",
    "industry",
}


@dataclass(frozen=True)
class SmallEmotionQ1OOSResult:
    """Artifacts and summary for Q1 small-emotion review."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_q1_oos_review(
    *,
    measurement_spec_path: str | Path,
    price_panel_path: str | Path,
    benchmark_panel_path: str | Path,
    delisting_path: str | Path,
    output_dir: str | Path,
    min_history_observations: int = 60,
    small_cap_min_market_cap: float = 50_000_000.0,
    small_cap_max_market_cap: float = 5_000_000_000.0,
    minimum_event_count: int = 50,
    minimum_event_month_count: int = 3,
    minimum_oos_event_count: int = 10,
    random_seed: int = 20260514,
    max_rows: int | None = None,
    max_falsifier_events: int = 5_000,
    exclude_stale_price_events: bool = False,
) -> SmallEmotionQ1OOSResult:
    """Run Q1 falsifier/OOS review for a frozen small-emotion MeasurementSpec."""

    spec_path = Path(measurement_spec_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)
    spec = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    filters = dict(spec["signal_definition"]["filters"])  # type: ignore[index]
    universe_scope = _universe_scope_from_filters(filters)

    prices = _read_q1_price_csv(Path(price_panel_path), nrows=max_rows)
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
        empty = pd.DataFrame()
        decision = _decision_summary(
            spec=spec,
            events=empty,
            labels=empty,
            oos=empty,
            falsifier=empty,
            policy=empty,
            q1_decision="blocked_data_coverage",
            universe_scope=universe_scope,
            exclude_stale_price_events=exclude_stale_price_events,
        )
        _write_outputs(artifacts, empty, empty, empty, empty, empty, decision)
        return SmallEmotionQ1OOSResult(summary=decision, artifacts=artifacts)

    min_adv = float(filters.get("adv_min_dollars") or 0.0)
    prepared = _prepare_q1_price_panel(
        prices,
        benchmark,
        min_history_observations=min_history_observations,
        min_adv_dollars=min_adv,
        small_cap_min_market_cap=small_cap_min_market_cap,
        small_cap_max_market_cap=small_cap_max_market_cap,
        large_cap_min_market_cap=5_000_000_000.0,
        universe_scope=universe_scope,
        exclude_stale_price_events=exclude_stale_price_events,
    )
    enriched = _add_q1_sharpening_features(prepared)
    events = _build_event_panel(enriched, spec)
    labels = _window_return_panel(events, enriched, spec)
    primary_window = str(spec["label_contract"]["primary_window"])  # type: ignore[index]
    primary = labels[(labels["window"] == primary_window) & (labels["label_status"] == "observed")].copy()
    oos = _oos_split_report(primary)
    falsifier = _falsifier_report(events, enriched, spec, random_seed=random_seed, max_falsifier_events=max_falsifier_events)
    policy = _policy_guard_report(events, labels, falsifier)
    q1_decision = _q1_decision(
        events=events,
        labels=labels,
        oos=oos,
        falsifier=falsifier,
        primary_window=primary_window,
        minimum_event_count=minimum_event_count,
        minimum_event_month_count=minimum_event_month_count,
        minimum_oos_event_count=minimum_oos_event_count,
    )
    decision = _decision_summary(
        spec=spec,
        events=events,
        labels=labels,
        oos=oos,
        falsifier=falsifier,
        policy=policy,
        q1_decision=q1_decision,
        universe_scope=universe_scope,
        exclude_stale_price_events=exclude_stale_price_events,
    )
    _write_outputs(artifacts, events, labels, oos, falsifier, policy, decision)
    return SmallEmotionQ1OOSResult(summary=decision, artifacts=artifacts)


def _read_q1_price_csv(path: Path, *, nrows: int | None = None) -> pd.DataFrame:
    """Read only the PIT daily columns required by the frozen Q1 review."""

    if not path.exists():
        return pd.DataFrame()
    try:
        available = list(pd.read_csv(path, nrows=0).columns)
        usecols = [column for column in available if column in Q1_PRICE_COLUMNS]
        return pd.read_csv(path, nrows=nrows, usecols=usecols or None)
    except EmptyDataError:
        return pd.DataFrame()


def _prepare_q1_price_panel(
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    *,
    min_history_observations: int,
    min_adv_dollars: float,
    small_cap_min_market_cap: float,
    small_cap_max_market_cap: float,
    large_cap_min_market_cap: float,
    universe_scope: str,
    exclude_stale_price_events: bool,
) -> pd.DataFrame:
    frame = prices.copy()
    if "asset_id" not in frame.columns:
        frame["asset_id"] = frame["permno"].astype(str) if "permno" in frame.columns else frame["ticker"].astype(str)
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
    groups = frame["asset_id"]
    frame["history_observations"] = group.cumcount()
    frame["row_pos"] = frame["history_observations"]
    frame["prev_close"] = group["adjusted_close"].shift(1)
    frame["unchanged_close"] = (frame["adjusted_close"] == frame["prev_close"]).fillna(False)
    frame["zero_volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0) <= 0.0
    frame["stale_roll_5"] = _current_rolling_sum_by_group(frame["unchanged_close"].astype(float), groups, window=5)
    frame["adv20"] = _prior_rolling_mean_by_group(frame["dollar_volume"], groups, window=20, min_periods=5)
    frame["volume20"] = _prior_rolling_mean_by_group(frame["volume"], groups, window=20, min_periods=5)
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
    frame["full_market_investable_universe"] = frame["common_share_pass"] & frame["market_cap"].notna() & frame["market_cap"].gt(0.0)
    frame["large_cap_control_universe"] = frame["market_cap"] >= float(large_cap_min_market_cap)
    frame["liquidity_bucket"] = np.select(
        [frame["adv20"] < 1_000_000.0, frame["adv20"] < 5_000_000.0],
        ["low", "mid"],
        default="high",
    )
    frame["spread_bucket"] = np.select(
        [frame["bid_ask_spread"] <= 0.01, frame["bid_ask_spread"] <= 0.05],
        ["tight", "medium"],
        default="wide",
    )
    frame["shock_abs"] = frame["return"].abs()
    frame["weak_liquidity"] = (
        pd.Series(frame["liquidity_bucket"], index=frame.index).eq("low")
        | pd.Series(frame["spread_bucket"], index=frame.index).eq("wide")
        | (frame["abnormal_volume"] <= 0.75)
    )
    reasons = pd.Series("", index=frame.index, dtype="object")
    reasons = reasons.mask(frame["adjusted_close"].isna() | frame["return"].isna() | frame["volume"].isna(), "missing_price_return_or_volume")
    reasons = reasons.mask(reasons.eq("") & ~frame["common_share_pass"], "excluded_non_common_share")
    if universe_scope == "full_market":
        reasons = reasons.mask(reasons.eq("") & ~frame["full_market_investable_universe"], "outside_full_market_investable_universe")
    else:
        reasons = reasons.mask(reasons.eq("") & ~frame["small_cap_investable_universe"], "outside_small_cap_investable_universe")
    reasons = reasons.mask(reasons.eq("") & (frame["history_observations"] < int(min_history_observations)), "insufficient_recent_trading_observations")
    stale_threshold = 1 if exclude_stale_price_events else 4
    reasons = reasons.mask(reasons.eq("") & (frame["zero_volume"] | (frame["stale_roll_5"] >= stale_threshold)), "stale_price_or_zero_volume")
    reasons = reasons.mask(reasons.eq("") & frame["adv20"].isna(), "missing_adv_capacity_input")
    reasons = reasons.mask(reasons.eq("") & (frame["adv20"] < float(min_adv_dollars)), "below_min_adv_capacity")
    reasons = reasons.mask(reasons.eq("") & frame["benchmark_return"].isna(), "missing_benchmark_return")
    frame["no_view_reason"] = reasons
    frame["coverage_state"] = np.where(frame["no_view_reason"].eq(""), "active_view", "no_view")
    return frame


def _add_q1_sharpening_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add the frozen D4 auxiliary filters without the exploratory sweep overhead."""

    out = frame.sort_values(["asset_id", "date"]).copy()
    groups = out["asset_id"]
    log_returns = np.log1p(pd.to_numeric(out["return"], errors="coerce").clip(lower=-0.999999))
    out["prior_5d_return"] = np.expm1(_prior_rolling_sum_by_group(log_returns, groups, window=5, min_periods=3))
    out["prior_20d_return"] = np.expm1(_prior_rolling_sum_by_group(log_returns, groups, window=20, min_periods=10))

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
    return out.merge(bench[["date", "market_prior_20d", "market_vol_20d", "market_regime"]], on="date", how="left")


def _prior_rolling_mean_by_group(values: pd.Series, groups: pd.Series, *, window: int, min_periods: int) -> pd.Series:
    sums = _prior_rolling_sum_by_group(values, groups, window=window, min_periods=min_periods)
    counts = _prior_rolling_sum_by_group(pd.to_numeric(values, errors="coerce").notna().astype(float), groups, window=window, min_periods=min_periods)
    return sums / counts.replace(0.0, np.nan)


def _prior_rolling_sum_by_group(values: pd.Series, groups: pd.Series, *, window: int, min_periods: int) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    valid = numeric.notna().astype(float)
    filled = numeric.fillna(0.0)
    cumsum = filled.groupby(groups, sort=False).cumsum()
    cumcount = valid.groupby(groups, sort=False).cumsum()
    prev_sum = cumsum.groupby(groups, sort=False).shift(1).fillna(0.0)
    prev_count = cumcount.groupby(groups, sort=False).shift(1).fillna(0.0)
    before_sum = cumsum.groupby(groups, sort=False).shift(window + 1).fillna(0.0)
    before_count = cumcount.groupby(groups, sort=False).shift(window + 1).fillna(0.0)
    sums = prev_sum - before_sum
    counts = prev_count - before_count
    return sums.where(counts >= float(min_periods), np.nan)


def _current_rolling_sum_by_group(values: pd.Series, groups: pd.Series, *, window: int) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").fillna(0.0)
    cumsum = numeric.groupby(groups, sort=False).cumsum()
    before_sum = cumsum.groupby(groups, sort=False).shift(window).fillna(0.0)
    return cumsum - before_sum


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "data_coverage_report": output_path / "data_coverage_report.json",
        "q1_event_panel": output_path / "q1_event_panel.csv",
        "q1_window_return_panel": output_path / "q1_window_return_panel.csv",
        "q1_oos_split_report": output_path / "q1_oos_split_report.csv",
        "q1_falsifier_report": output_path / "q1_falsifier_report.csv",
        "q1_policy_guard_report": output_path / "q1_policy_guard_report.csv",
        "q1_decision_summary": output_path / "q1_decision_summary.json",
        "q1_oos_report": output_path / "q1_oos_report.md",
    }


def _build_event_panel(frame: pd.DataFrame, spec: dict[str, object]) -> pd.DataFrame:
    filters = dict(spec["signal_definition"]["filters"])  # type: ignore[index]
    mechanism = str(spec["signal_definition"]["mechanism"])  # type: ignore[index]
    market_cap_bucket = _market_cap_bucket_series(frame["market_cap"])
    mask = (
        frame["coverage_state"].eq("active_view")
        & frame["abnormal_volume"].ge(float(filters.get("volume_spike_threshold") or 0.0))
        & frame["adv20"].ge(float(filters.get("adv_min_dollars") or 0.0))
    )
    shock = float(filters.get("shock_threshold") or 0.0)
    if mechanism.startswith("up_"):
        mask &= frame["return"].ge(shock)
    elif mechanism.startswith("down_"):
        mask &= frame["return"].le(-shock)
    else:
        return pd.DataFrame(columns=_event_columns())
    mask = _optional_filter_mask(frame, filters, mask, market_cap_bucket)
    active = frame.loc[mask].copy()
    if active.empty:
        return pd.DataFrame(columns=_event_columns())
    active["market_cap_bucket"] = market_cap_bucket.loc[active.index]
    rows: list[dict[str, object]] = []
    for idx, (_, row) in enumerate(active.sort_values(["date", "asset_id"]).iterrows(), start=1):
        rows.append(
            {
                "schema_version": "small_emotion_q1_event_panel.v1",
                "stage": STAGE,
                "event_id": f"small_emotion_q1_{idx:08d}",
                "measurement_spec_id": spec["measurement_spec_id"],
                "asset_id": str(row["asset_id"]),
                "ticker": str(row["ticker"]),
                "date": pd.Timestamp(row["date"]).date().isoformat(),
                "row_pos": int(row["row_pos"]),
                "event_month": pd.Timestamp(row["date"]).strftime("%Y-%m"),
                "mechanism": mechanism,
                "signal_state": "active",
                "shock_return": float(row["return"]),
                "abs_shock_return": float(row["shock_abs"]),
                "abnormal_volume": float(row["abnormal_volume"]),
                "prior_5d_return": _float_or_nan(row.get("prior_5d_return", np.nan)),
                "prior_20d_return": _float_or_nan(row.get("prior_20d_return", np.nan)),
                "market_regime": str(row.get("market_regime", "")),
                "sector": str(row.get("sector", "")),
                "industry": str(row.get("industry", "")),
                "market_cap": float(row["market_cap"]),
                "market_cap_bucket": str(row["market_cap_bucket"]),
                "adjusted_close": _float_or_nan(row.get("adjusted_close", np.nan)),
                "volume": _float_or_nan(row.get("volume", np.nan)),
                "dollar_volume": _float_or_nan(row.get("dollar_volume", np.nan)),
                "bid_ask_spread": _float_or_nan(row.get("bid_ask_spread", np.nan)),
                "zero_volume": bool(row.get("zero_volume", False)),
                "stale_roll_5": _float_or_nan(row.get("stale_roll_5", np.nan)),
                "adv20": float(row["adv20"]),
                "liquidity_bucket": str(row["liquidity_bucket"]),
                "spread_bucket": str(row["spread_bucket"]),
                "weak_liquidity": bool(row["weak_liquidity"]),
                "signal_value": -1.0 if mechanism == "up_shock_reversal" else 1.0,
                "coverage_state": "active",
                "no_view_reason": "",
                **EXPLORATORY_GUARDS,
            }
        )
    return pd.DataFrame(rows, columns=_event_columns())


def _optional_filter_mask(
    frame: pd.DataFrame,
    filters: dict[str, object],
    mask: pd.Series,
    market_cap_bucket: pd.Series,
) -> pd.Series:
    out = mask.copy()
    prior5 = filters.get("prior_5d_min_return")
    if prior5 not in {None, ""}:
        out &= frame["prior_5d_return"].ge(float(prior5))
    prior20 = filters.get("prior_20d_min_return")
    if prior20 not in {None, ""}:
        out &= frame["prior_20d_return"].ge(float(prior20))
    market_cap_filter = filters.get("market_cap_bucket", "all_small_cap")
    if market_cap_filter not in {"all_small_cap", "all_full_market"}:
        out &= market_cap_bucket.eq(market_cap_filter)
    out &= _path_predicate_mask(frame, filters.get("path_predicates", ""), market_cap_bucket)
    if filters.get("liquidity_filter", "all") == "weak_liquidity":
        out &= frame["weak_liquidity"]
    elif filters.get("liquidity_filter", "all") != "all":
        out &= frame["liquidity_bucket"].eq(filters["liquidity_filter"])
    if filters.get("spread_filter", "all") != "all":
        out &= frame["spread_bucket"].eq(filters["spread_filter"])
    if filters.get("regime_filter", "all") != "all":
        out &= frame["market_regime"].eq(filters["regime_filter"])
    low_price = filters.get("low_price_filter", "all")
    if low_price == "under_10":
        out &= frame["low_price_bucket"].isin(["under_5", "under_10"])
    elif low_price == "under_5":
        out &= frame["low_price_bucket"].eq("under_5")
    close_filter = filters.get("close_location_filter", "all")
    if close_filter == "top_quartile":
        out &= frame["close_location"].ge(0.75)
    elif close_filter == "lower_half":
        out &= frame["close_location"].lt(0.50)
    return out


def _universe_scope_from_filters(filters: dict[str, object]) -> str:
    """Return the active universe scope implied by a frozen MeasurementSpec."""

    if str(filters.get("market_cap_bucket", "")).strip() == "all_full_market":
        return "full_market"
    return "small_cap"


def _path_predicate_mask(frame: pd.DataFrame, predicates: object, market_cap_bucket: pd.Series) -> pd.Series:
    """Apply locked overfit-lab path predicates during Q1 replay."""

    out = pd.Series(True, index=frame.index)
    if predicates in {None, ""}:
        return out
    for token in [part.strip() for part in str(predicates).split("&") if part.strip()]:
        if token == "open_to_close_le_minus_5pct":
            out &= pd.to_numeric(frame["open_to_close_return"], errors="coerce").le(-0.05)
        elif token == "open_to_close_ge_5pct":
            out &= pd.to_numeric(frame["open_to_close_return"], errors="coerce").ge(0.05)
        elif token == "prior5_ge_20pct":
            out &= pd.to_numeric(frame["prior_5d_return"], errors="coerce").ge(0.20)
        elif token == "prior5_le_minus_20pct":
            out &= pd.to_numeric(frame["prior_5d_return"], errors="coerce").le(-0.20)
        elif token == "size_micro":
            out &= market_cap_bucket.eq("micro")
        elif token == "size_small":
            out &= market_cap_bucket.eq("small")
        elif token == "spread_wide":
            out &= frame["spread_bucket"].eq("wide")
        elif token == "spread_tight":
            out &= frame["spread_bucket"].eq("tight")
        elif token == "weak_liquidity":
            out &= frame["weak_liquidity"]
        elif token == "liquidity_low":
            out &= frame["liquidity_bucket"].eq("low")
        else:
            shock_match = re.fullmatch(r"shock_ge_(\d+)pct", token)
            if shock_match:
                out &= pd.to_numeric(frame["shock_abs"], errors="coerce").ge(float(shock_match.group(1)) / 100.0)
            else:
                return pd.Series(False, index=frame.index)
    return out


def _window_return_panel(events: pd.DataFrame, prices: pd.DataFrame, spec: dict[str, object]) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=_label_columns())
    by_asset = _by_asset_panel(prices, assets=set(events["asset_id"].astype(str)))
    rows: list[dict[str, object]] = []
    for event in events.itertuples(index=False):
        asset_panel = by_asset.get(str(event.asset_id))
        if asset_panel is None:
            continue
        pos = int(getattr(event, "row_pos", -1))
        if pos < 0 or pos >= len(asset_panel):
            continue
        for window, offsets in WINDOWS.items():
            asset_return, benchmark_return, status = _window_return(asset_panel, pos, offsets)
            abnormal = asset_return - benchmark_return if pd.notna(asset_return) and pd.notna(benchmark_return) else np.nan
            rows.append(
                {
                    "schema_version": "small_emotion_q1_window_return_panel.v1",
                    "stage": STAGE,
                    "event_id": event.event_id,
                    "asset_id": event.asset_id,
                    "ticker": event.ticker,
                    "date": event.date,
                    "event_month": event.event_month,
                    "mechanism": event.mechanism,
                    "window": window,
                    "label_status": status,
                    "asset_return": asset_return,
                    "benchmark_return": benchmark_return,
                    "abnormal_return": abnormal,
                    "directional_return": _directional_return(event.mechanism, abnormal),
                    "measurement_spec_id": spec["measurement_spec_id"],
                    **EXPLORATORY_GUARDS,
                }
            )
    return pd.DataFrame(rows, columns=_label_columns())


def _oos_split_report(primary: pd.DataFrame) -> pd.DataFrame:
    if primary.empty:
        return pd.DataFrame(columns=_oos_columns())
    observed = primary[primary["label_status"].eq("observed")].copy()
    observed["date_ts"] = pd.to_datetime(observed["date"], errors="coerce")
    observed = observed.sort_values("date_ts")
    if observed.empty:
        return pd.DataFrame(columns=_oos_columns())
    midpoint = max(1, len(observed) // 2)
    rows = []
    for split, frame in [("train", observed.iloc[:midpoint]), ("test", observed.iloc[midpoint:])]:
        rows.append(
            {
                "schema_version": "small_emotion_q1_oos_split_report.v1",
                "stage": STAGE,
                "split": split,
                "event_count": int(len(frame)),
                "event_month_count": int(frame["event_month"].nunique()) if not frame.empty else 0,
                "mean_directional_return": float(frame["directional_return"].mean()) if not frame.empty else np.nan,
                "hit_rate": float((frame["directional_return"] > 0.0).mean()) if not frame.empty else np.nan,
                **EXPLORATORY_GUARDS,
            }
        )
    return pd.DataFrame(rows, columns=_oos_columns())


def _falsifier_report(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    spec: dict[str, object],
    *,
    random_seed: int,
    max_falsifier_events: int,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=_falsifier_columns())
    primary_window = str(spec["label_contract"]["primary_window"])  # type: ignore[index]
    offsets = WINDOWS[primary_window]
    same_coverage_events = _same_coverage_random_events(events, prices, random_seed)
    large_cap_events = _matched_pool_events(prices, pool="large_cap", max_events=max_falsifier_events, seed=random_seed + 101)
    stale_events = _matched_pool_events(prices, pool="stale", max_events=max_falsifier_events, seed=random_seed + 202)
    adv_events = _matched_pool_events(prices, pool="low_adv", max_events=max_falsifier_events, seed=random_seed + 303)
    needed_assets = set(events["asset_id"].astype(str))
    for fake in [same_coverage_events, large_cap_events, stale_events, adv_events]:
        if not fake.empty:
            needed_assets.update(fake["asset_id"].astype(str))
    by_asset = _by_asset_panel(prices, assets=needed_assets)
    live = _mean_directional_for_events(events, by_asset, offsets, shift=0)
    rows = []
    for name, shift in {
        "shift_minus_5": -5,
        "shift_plus_5": 5,
        "shift_minus_10": -10,
        "shift_plus_10": 10,
    }.items():
        value = _mean_directional_for_events(events, by_asset, offsets, shift=shift)
        rows.append(_falsifier_row(name, live, value))
    rows.append(
        _falsifier_row(
            "same_coverage_random",
            live,
            _mean_directional_for_events(same_coverage_events, by_asset, offsets, shift=0),
        )
    )
    rows.append(
        _falsifier_row(
            "large_cap_matched_shock",
            live,
            _mean_directional_for_events(large_cap_events, by_asset, offsets, shift=0),
        )
    )
    rows.append(
        _falsifier_row(
            "stale_price_matched",
            live,
            _mean_directional_for_events(stale_events, by_asset, offsets, shift=0),
        )
    )
    rows.append(
        _falsifier_row(
            "adv_capacity_matched",
            live,
            _mean_directional_for_events(adv_events, by_asset, offsets, shift=0),
        )
    )
    return pd.DataFrame(rows, columns=_falsifier_columns())


def _by_asset_panel(prices: pd.DataFrame, *, assets: set[str] | None = None) -> dict[str, pd.DataFrame]:
    frame = prices
    if assets is not None:
        frame = prices[prices["asset_id"].astype(str).isin(assets)]
    return {str(asset): group.sort_values("date").reset_index(drop=True) for asset, group in frame.groupby("asset_id", sort=False)}


def _mean_directional_for_events(
    events: pd.DataFrame,
    by_asset: dict[str, pd.DataFrame],
    offsets: tuple[int, int],
    *,
    shift: int,
) -> float:
    values = []
    for event in events.itertuples(index=False):
        panel = by_asset.get(str(event.asset_id))
        if panel is None:
            continue
        pos = int(getattr(event, "row_pos", -1))
        if pos < 0 or pos >= len(panel):
            continue
        asset_return, benchmark_return, status = _window_return(panel, int(pos) + int(shift), offsets)
        if status != "observed":
            continue
        values.append(_directional_return(str(event.mechanism), asset_return - benchmark_return))
    return float(np.mean(values)) if values else np.nan


def _same_coverage_random_events(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    active = prices[prices["coverage_state"].eq("active_view")]
    if active.empty:
        return pd.DataFrame(columns=["asset_id", "date", "row_pos", "mechanism"])
    sample = active.sample(n=min(len(events), len(active)), random_state=seed, replace=False)
    return pd.DataFrame(
        {
            "asset_id": sample["asset_id"].astype(str).to_list(),
            "date": sample["date"].dt.date.astype(str).to_list(),
            "row_pos": pd.to_numeric(sample["row_pos"], errors="coerce").fillna(-1).astype(int).to_list(),
            "mechanism": ["up_shock_reversal"] * len(sample),
        }
    )


def _matched_pool_events(
    prices: pd.DataFrame,
    *,
    pool: str,
    max_events: int,
    seed: int,
) -> pd.DataFrame:
    frame = prices
    if pool == "large_cap":
        candidates = frame[frame["large_cap_control_universe"] & frame["return"].ge(0.05)]
    elif pool == "stale":
        candidates = frame[(frame["zero_volume"] | frame["stale_roll_5"].ge(4)) & frame["return"].abs().ge(0.0)]
    elif pool == "low_adv":
        threshold = frame["adv20"].quantile(0.25)
        candidates = frame[frame["adv20"].le(threshold) & frame["return"].ge(0.05)]
    else:
        candidates = frame.iloc[0:0]
    if candidates.empty:
        return pd.DataFrame(columns=["asset_id", "date", "row_pos", "mechanism"])
    candidates = candidates.sort_values(["date", "asset_id"])
    if len(candidates) > int(max_events):
        candidates = candidates.sample(n=int(max_events), random_state=seed, replace=False).sort_values(["date", "asset_id"])
    return pd.DataFrame(
        {
            "asset_id": candidates["asset_id"].astype(str).to_list(),
            "date": candidates["date"].dt.date.astype(str).to_list(),
            "row_pos": pd.to_numeric(candidates["row_pos"], errors="coerce").fillna(-1).astype(int).to_list(),
            "mechanism": ["up_shock_reversal"] * len(candidates),
        }
    )


def _falsifier_row(name: str, live: float, value: float) -> dict[str, object]:
    dominates = bool(pd.notna(live) and pd.notna(value) and live > 0.0 and value >= live)
    return {
        "schema_version": "small_emotion_q1_falsifier_report.v1",
        "stage": STAGE,
        "falsifier_name": name,
        "live_mean_directional_return": live,
        "falsifier_mean_directional_return": value,
        "falsifier_dominates_live": dominates,
        **EXPLORATORY_GUARDS,
    }


def _policy_guard_report(events: pd.DataFrame, labels: pd.DataFrame, falsifier: pd.DataFrame) -> pd.DataFrame:
    primary = labels[labels["window"].eq("post_1_22") & labels["label_status"].eq("observed")]
    rows = [
        {
            "schema_version": "small_emotion_q1_policy_guard_report.v1",
            "stage": STAGE,
            "guard_name": "single_month_concentration",
            "guard_breached": _max_share(events, "event_month") > 0.50 if not events.empty else True,
            "observed_value": _max_share(events, "event_month") if not events.empty else np.nan,
            **EXPLORATORY_GUARDS,
        },
        {
            "schema_version": "small_emotion_q1_policy_guard_report.v1",
            "stage": STAGE,
            "guard_name": "single_issuer_concentration",
            "guard_breached": _max_share(events, "asset_id") > 0.25 if not events.empty else True,
            "observed_value": _max_share(events, "asset_id") if not events.empty else np.nan,
            **EXPLORATORY_GUARDS,
        },
        {
            "schema_version": "small_emotion_q1_policy_guard_report.v1",
            "stage": STAGE,
            "guard_name": "pre_event_dominance",
            "guard_breached": _pre_event_dominates(labels),
            "observed_value": _pre_event_mean(labels),
            **EXPLORATORY_GUARDS,
        },
        {
            "schema_version": "small_emotion_q1_policy_guard_report.v1",
            "stage": STAGE,
            "guard_name": "label_observed_count",
            "guard_breached": primary.empty,
            "observed_value": int(len(primary)),
            **EXPLORATORY_GUARDS,
        },
    ]
    return pd.DataFrame(rows)


def _q1_decision(
    *,
    events: pd.DataFrame,
    labels: pd.DataFrame,
    oos: pd.DataFrame,
    falsifier: pd.DataFrame,
    primary_window: str,
    minimum_event_count: int,
    minimum_event_month_count: int,
    minimum_oos_event_count: int,
) -> str:
    primary = labels[(labels["window"] == primary_window) & labels["label_status"].eq("observed")]
    event_count = len(primary)
    month_count = int(primary["event_month"].nunique()) if not primary.empty else 0
    if event_count < int(minimum_event_count) or month_count < int(minimum_event_month_count):
        return "hold_insufficient_sample"
    test = oos[oos["split"].eq("test")]
    if test.empty or int(test["event_count"].iloc[0]) < int(minimum_oos_event_count):
        return "hold_insufficient_sample"
    if float(test["mean_directional_return"].iloc[0]) <= 0.0:
        return "blocked_oos_failure"
    if not falsifier.empty and falsifier["falsifier_dominates_live"].astype(bool).any():
        return "blocked_placebo_dominance"
    live_mean = float(primary["directional_return"].mean()) if not primary.empty else np.nan
    if pd.notna(live_mean) and live_mean > 0.0:
        return "passed_q1_research_review"
    return "failed_q1"


def _decision_summary(
    *,
    spec: dict[str, object],
    events: pd.DataFrame,
    labels: pd.DataFrame,
    oos: pd.DataFrame,
    falsifier: pd.DataFrame,
    policy: pd.DataFrame,
    q1_decision: str,
    universe_scope: str,
    exclude_stale_price_events: bool,
) -> dict[str, object]:
    primary_window = spec.get("label_contract", {}).get("primary_window", "post_1_22")
    primary = labels[(labels["window"] == primary_window) & labels["label_status"].eq("observed")] if not labels.empty else pd.DataFrame()
    passed = q1_decision == "passed_q1_research_review"
    return {
        "schema_version": "small_emotion_q1_oos_summary.v1",
        "stage": STAGE,
        "measurement_spec_id": spec.get("measurement_spec_id"),
        "universe_scope": universe_scope,
        "path_predicates": spec.get("signal_definition", {}).get("filters", {}).get("path_predicates", ""),
        "exclude_stale_price_events": bool(exclude_stale_price_events),
        "q1_decision": q1_decision,
        "promotion_gate_allowed": bool(passed),
        "active_event_count": int(len(events)),
        "observed_primary_label_count": int(len(primary)),
        "event_month_count": int(primary["event_month"].nunique()) if not primary.empty else 0,
        "mean_primary_directional_return": float(primary["directional_return"].mean()) if not primary.empty else np.nan,
        "oos_test_mean_directional_return": _split_value(oos, "test", "mean_directional_return"),
        "falsifier_dominance_count": int(falsifier["falsifier_dominates_live"].sum()) if not falsifier.empty else 0,
        "policy_breach_count": int(policy["guard_breached"].sum()) if not policy.empty else 0,
        "q2_entry_allowed": False,
        "optimizer_entry_allowed": False,
        "expected_return_panel_written": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "not_alpha_evidence": False,
        "no_view_not_zero_alpha": True,
    }


def _write_outputs(
    artifacts: dict[str, Path],
    events: pd.DataFrame,
    labels: pd.DataFrame,
    oos: pd.DataFrame,
    falsifier: pd.DataFrame,
    policy: pd.DataFrame,
    decision: dict[str, object],
) -> None:
    events.to_csv(artifacts["q1_event_panel"], index=False)
    labels.to_csv(artifacts["q1_window_return_panel"], index=False)
    oos.to_csv(artifacts["q1_oos_split_report"], index=False)
    falsifier.to_csv(artifacts["q1_falsifier_report"], index=False)
    policy.to_csv(artifacts["q1_policy_guard_report"], index=False)
    artifacts["q1_decision_summary"].write_text(
        json.dumps(decision, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["q1_oos_report"].write_text(_report(decision), encoding="utf-8")


def _report(decision: dict[str, object]) -> str:
    return "\n".join(
        [
            "# Q1-SMALL-EMOTION-01 Falsifier/OOS Review",
            "",
            "This is a Q1 falsifier/OOS review only. It does not run Q2, optimizer, portfolio construction, Alpha Registry, paper, broker, order, live, or production workflows.",
            "",
            f"- measurement_spec_id: {decision['measurement_spec_id']}",
            f"- q1_decision: {decision['q1_decision']}",
            f"- active_event_count: {decision['active_event_count']}",
            f"- observed_primary_label_count: {decision['observed_primary_label_count']}",
            f"- mean_primary_directional_return: {decision['mean_primary_directional_return']}",
            f"- promotion_gate_allowed: {decision['promotion_gate_allowed']}",
            f"- q2_entry_allowed: {decision['q2_entry_allowed']}",
            "",
        ]
    )


def _directional_return(mechanism: str, abnormal_return: float) -> float:
    if pd.isna(abnormal_return):
        return np.nan
    if mechanism in {"up_shock_reversal", "down_shock_continuation"}:
        return float(-abnormal_return)
    return float(abnormal_return)


def _market_cap_bucket(market_cap: float) -> str:
    if market_cap < 300_000_000.0:
        return "micro"
    if market_cap < 2_000_000_000.0:
        return "small"
    return "lower_mid"


def _market_cap_bucket_series(market_cap: pd.Series) -> pd.Series:
    values = pd.to_numeric(market_cap, errors="coerce")
    return pd.Series(
        np.select(
            [values < 300_000_000.0, values < 2_000_000_000.0],
            ["micro", "small"],
            default="lower_mid",
        ),
        index=market_cap.index,
        dtype="object",
    )


def _float_or_nan(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def _max_share(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame:
        return np.nan
    counts = frame[column].value_counts(normalize=True)
    return float(counts.max()) if not counts.empty else np.nan


def _pre_event_mean(labels: pd.DataFrame) -> float:
    if labels.empty:
        return np.nan
    pre = labels[labels["window"].isin(["pre_5_1", "pre_10_1", "pre_20_1"]) & labels["label_status"].eq("observed")]
    return float(pre["directional_return"].mean()) if not pre.empty else np.nan


def _pre_event_dominates(labels: pd.DataFrame) -> bool:
    if labels.empty:
        return False
    primary = labels[labels["window"].eq("post_1_22") & labels["label_status"].eq("observed")]
    pre_mean = _pre_event_mean(labels)
    post_mean = float(primary["directional_return"].mean()) if not primary.empty else np.nan
    return bool(pd.notna(pre_mean) and pd.notna(post_mean) and pre_mean >= post_mean and post_mean > 0.0)


def _split_value(oos: pd.DataFrame, split: str, column: str) -> float:
    if oos.empty:
        return np.nan
    row = oos[oos["split"].eq(split)]
    if row.empty:
        return np.nan
    return float(row[column].iloc[0])


def _event_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "event_id",
        "measurement_spec_id",
        "asset_id",
        "ticker",
        "date",
        "row_pos",
        "event_month",
        "mechanism",
        "signal_state",
        "shock_return",
        "abs_shock_return",
        "abnormal_volume",
        "prior_5d_return",
        "prior_20d_return",
        "market_regime",
        "sector",
        "industry",
        "market_cap",
        "market_cap_bucket",
        "adjusted_close",
        "volume",
        "dollar_volume",
        "bid_ask_spread",
        "zero_volume",
        "stale_roll_5",
        "adv20",
        "liquidity_bucket",
        "spread_bucket",
        "weak_liquidity",
        "signal_value",
        "coverage_state",
        "no_view_reason",
        *EXPLORATORY_GUARDS.keys(),
    ]


def _label_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "event_id",
        "asset_id",
        "ticker",
        "date",
        "event_month",
        "mechanism",
        "window",
        "label_status",
        "asset_return",
        "benchmark_return",
        "abnormal_return",
        "directional_return",
        "measurement_spec_id",
        *EXPLORATORY_GUARDS.keys(),
    ]


def _oos_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "split",
        "event_count",
        "event_month_count",
        "mean_directional_return",
        "hit_rate",
        *EXPLORATORY_GUARDS.keys(),
    ]


def _falsifier_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "falsifier_name",
        "live_mean_directional_return",
        "falsifier_mean_directional_return",
        "falsifier_dominates_live",
        *EXPLORATORY_GUARDS.keys(),
    ]
