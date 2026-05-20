"""Market-regime filter check for SUE placebo-failure attribution.

This Reopen-H1E.2 diagnostic checks whether the H1E.1 event-date-shift placebo
advantage is explained by March 2020, high-volatility weeks, or low-liquidity
weeks. It reruns filtered score-gate style summaries and placebo curves only;
it does not select a score, run Q2, invoke optimizers, open paper/live/broker
workflows, create orders, or approve production use.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.alpha.sue_historical_event_evidence import (
    SueHistoricalEventEvidenceConfig,
    _load_and_validate_events,
    _load_prices,
    _price_index,
    _safe_events,
)
from portfolio_os.alpha.sue_historical_schema import validate_no_forward_return_feature_columns
from portfolio_os.alpha.sue_placebo_failure_attribution import (
    DEFAULT_CRSP_DAILY_PATH,
    DEFAULT_EVENTS_PATH,
    PRIMARY_WINDOW,
    _best_placebo_row,
    _build_shifted_window_return_frame,
    _helper_gate_config,
    _placebo_shift_curve,
    _primary_curve_row,
)
from portfolio_os.alpha.sue_placebo_failure_attribution import (
    SuePlaceboFailureAttributionConfig as _PlaceboConfig,
)
from portfolio_os.alpha.sue_score_definition_gate import _attach_gate_scores
from portfolio_os.provenance.hashing import canonical_json, hash_payload


SUE_REGIME_FILTER_PLACEBO_SCHEMA_VERSION = "sue_regime_filter_placebo_check.v1"
DEFAULT_OUTPUT_DIR = "outputs/sue_regime_filter_placebo_check"
DEFAULT_REPORT_PATH = "reports/sue_regime_filter_placebo_check_report.md"

MISLEADING_REGIME_FILTER_CLAIMS = (
    "production approved",
    "paper ready",
    "paper-ready",
    "live-ready",
    "live ready",
    "live trading",
    "broker execution",
    "order generation",
    "sue alpha is proven",
    "selected production score",
    "real historical sue alpha proven",
    "historical sue alpha proven",
    "guaranteed tradable alpha",
    "auto trading",
    "investment recommendation",
)


class SueRegimeFilterPlaceboConfig(BaseModel):
    """Config for Reopen-H1E.2 regime-filter placebo check."""

    model_config = ConfigDict(extra="forbid")

    events_path: str = DEFAULT_EVENTS_PATH
    crsp_daily_path: str = DEFAULT_CRSP_DAILY_PATH
    output_dir: str = DEFAULT_OUTPUT_DIR
    report_path: str = DEFAULT_REPORT_PATH
    score_name: str = "surprise_pct_actual_eps"
    shift_trading_days: list[int] = Field(default_factory=lambda: [-10, -5, -2, 0, 2, 5, 10])
    quantiles: int = Field(default=5, gt=1)
    min_rank_ic_names: int = Field(default=3, gt=1)
    min_spread_names: int = Field(default=5, gt=1)
    denominator_abs_min: float = Field(default=0.01, gt=0.0)
    winsorization_scope: str = "month"
    winsor_lower_quantile: float = Field(default=0.01, ge=0.0, lt=0.5)
    winsor_upper_quantile: float = Field(default=0.99, gt=0.5, le=1.0)
    extreme_value_cap: float = Field(default=1000.0, gt=0.0)
    high_volatility_week_quantile: float = Field(default=0.9, gt=0.0, lt=1.0)
    low_liquidity_week_quantile: float = Field(default=0.1, gt=0.0, lt=1.0)
    random_seed: int = 20260508


@dataclass(frozen=True)
class SueRegimeFilterPlaceboResult:
    """In-memory Reopen-H1E.2 regime-filter result."""

    config: SueRegimeFilterPlaceboConfig
    score_gate_summary: pd.DataFrame
    filtered_placebo_shift_curve: pd.DataFrame
    regime_week_classification: pd.DataFrame
    regime_filter_summary: dict[str, Any]
    report_text: str


def load_sue_regime_filter_placebo_config(path: str | Path) -> SueRegimeFilterPlaceboConfig:
    """Load H1E.2 config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    guards = payload.get("guards") or {}
    return SueRegimeFilterPlaceboConfig(
        events_path=str(inputs.get("events_path") or payload.get("events_path") or DEFAULT_EVENTS_PATH),
        crsp_daily_path=str(inputs.get("crsp_daily_path") or payload.get("crsp_daily_path") or DEFAULT_CRSP_DAILY_PATH),
        output_dir=str(outputs.get("output_dir") or payload.get("output_dir") or DEFAULT_OUTPUT_DIR),
        report_path=str(outputs.get("report_path") or payload.get("report_path") or DEFAULT_REPORT_PATH),
        score_name=str(payload.get("score_name", "surprise_pct_actual_eps")),
        shift_trading_days=list(payload.get("shift_trading_days", [-10, -5, -2, 0, 2, 5, 10])),
        quantiles=int(payload.get("quantiles", 5)),
        min_rank_ic_names=int(payload.get("min_rank_ic_names", 3)),
        min_spread_names=int(payload.get("min_spread_names", 5)),
        denominator_abs_min=float(guards.get("denominator_abs_min", payload.get("denominator_abs_min", 0.01))),
        winsorization_scope=str(guards.get("winsorization_scope", payload.get("winsorization_scope", "month"))),
        winsor_lower_quantile=float(
            guards.get("winsor_lower_quantile", payload.get("winsor_lower_quantile", 0.01))
        ),
        winsor_upper_quantile=float(
            guards.get("winsor_upper_quantile", payload.get("winsor_upper_quantile", 0.99))
        ),
        extreme_value_cap=float(guards.get("extreme_value_cap", payload.get("extreme_value_cap", 1000.0))),
        high_volatility_week_quantile=float(
            guards.get("high_volatility_week_quantile", payload.get("high_volatility_week_quantile", 0.9))
        ),
        low_liquidity_week_quantile=float(
            guards.get("low_liquidity_week_quantile", payload.get("low_liquidity_week_quantile", 0.1))
        ),
        random_seed=int(payload.get("random_seed", 20260508)),
    )


def build_sue_regime_filter_placebo_check(
    config: SueRegimeFilterPlaceboConfig | None = None,
) -> SueRegimeFilterPlaceboResult:
    """Build H1E.2 filtered score-gate summaries and placebo curves."""

    resolved = config or SueRegimeFilterPlaceboConfig()
    events = _load_and_validate_events(resolved.events_path)
    validate_no_forward_return_feature_columns(list(events.columns))
    prices = _load_prices(resolved.crsp_daily_path)
    price_index = _price_index(prices)
    safe_events = _safe_events(events)
    week_classification = _regime_week_classification(resolved.crsp_daily_path, resolved)

    score_rows: list[dict[str, Any]] = []
    curve_frames: list[pd.DataFrame] = []
    baseline_advantage: float | None = None
    for filter_name, filtered_events, excluded_reason in _filtered_event_sets(
        safe_events=safe_events,
        regime_week_classification=week_classification,
    ):
        curve = _filtered_placebo_curve(
            filtered_events=filtered_events,
            price_index=price_index,
            config=resolved,
        )
        if not curve.empty:
            curve = curve.copy()
            curve.insert(0, "filter_name", filter_name)
            curve_frames.append(curve)
        summary_row = _filter_summary_row(
            filter_name=filter_name,
            safe_event_count=int(safe_events["event_id"].nunique()),
            filtered_event_count=int(filtered_events["event_id"].nunique()),
            excluded_reason=excluded_reason,
            placebo_shift_curve=curve,
            config=resolved,
        )
        if filter_name == "baseline":
            baseline_advantage = _float_or_none(summary_row.get("placebo_advantage_top_bottom_spread"))
        else:
            current_advantage = _float_or_none(summary_row.get("placebo_advantage_top_bottom_spread"))
            if baseline_advantage is not None and current_advantage is not None:
                summary_row["placebo_advantage_reduction_vs_baseline"] = float(baseline_advantage - current_advantage)
            else:
                summary_row["placebo_advantage_reduction_vs_baseline"] = None
        score_rows.append(summary_row)

    score_summary = pd.DataFrame(score_rows)
    shift_curve = (
        pd.concat([frame.dropna(axis=1, how="all") for frame in curve_frames], ignore_index=True)
        if curve_frames
        else pd.DataFrame()
    )
    regime_summary = _regime_filter_summary(
        config=resolved,
        score_gate_summary=score_summary,
        regime_week_classification=week_classification,
    )
    report_text = render_sue_regime_filter_placebo_report(
        score_gate_summary=score_summary,
        regime_week_classification=week_classification,
        regime_filter_summary=regime_summary,
    )
    validate_sue_regime_filter_placebo_report_language(report_text)
    return SueRegimeFilterPlaceboResult(
        config=resolved,
        score_gate_summary=score_summary,
        filtered_placebo_shift_curve=shift_curve,
        regime_week_classification=week_classification,
        regime_filter_summary=regime_summary,
        report_text=report_text,
    )


def write_sue_regime_filter_placebo_artifacts(result: SueRegimeFilterPlaceboResult) -> dict[str, Path]:
    """Write H1E.2 regime-filter artifacts."""

    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(result.config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    paths = {
        "score_gate_summary": output_dir / "score_gate_summary.csv",
        "filtered_placebo_shift_curve": output_dir / "filtered_placebo_shift_curve.csv",
        "regime_week_classification": output_dir / "regime_week_classification.csv",
        "regime_filter_summary": output_dir / "regime_filter_summary.json",
        "report": report_path,
    }
    result.score_gate_summary.to_csv(paths["score_gate_summary"], index=False)
    result.filtered_placebo_shift_curve.to_csv(paths["filtered_placebo_shift_curve"], index=False)
    result.regime_week_classification.to_csv(paths["regime_week_classification"], index=False)
    _write_json(paths["regime_filter_summary"], result.regime_filter_summary)
    validate_sue_regime_filter_placebo_report_language(result.report_text)
    paths["report"].write_text(result.report_text, encoding="utf-8")
    return paths


def render_sue_regime_filter_placebo_report(
    *,
    score_gate_summary: pd.DataFrame,
    regime_week_classification: pd.DataFrame,
    regime_filter_summary: dict[str, Any],
) -> str:
    """Render H1E.2 report."""

    lines = [
        "# SUE Market-Regime Placebo Filter Check",
        "",
        "H1E.2 validates the market-regime attribution only.",
        "It reruns filtered score-gate summaries and placebo curves after excluding March 2020, high-volatility weeks, and low-liquidity weeks.",
        "It does not select a SUE score, run Q2, run optimizer-path evaluation, promote Alpha Registry state, open paper/live/broker/order workflows, or approve production use.",
        "",
        "## Summary",
        "",
        f"- schema_version: `{regime_filter_summary['schema_version']}`",
        f"- score_name: `{regime_filter_summary['score_name']}`",
        f"- interpretation: `{regime_filter_summary['interpretation']}`",
        f"- selected_score: `{regime_filter_summary['selected_score']}`",
        f"- high_volatility_week_count: `{regime_filter_summary['high_volatility_week_count']}`",
        f"- low_liquidity_week_count: `{regime_filter_summary['low_liquidity_week_count']}`",
        f"- low_liquidity_filter_source: `{regime_filter_summary['low_liquidity_filter_source']}`",
        f"- q2_evaluation_ran: `{regime_filter_summary['q2_evaluation_ran']}`",
        f"- optimizer_path_evaluation_ran: `{regime_filter_summary['optimizer_path_evaluation_ran']}`",
        f"- production_approval_claimed: `{regime_filter_summary['production_approval_claimed']}`",
        "",
        "## Filtered Score-Gate Summary",
        "",
        "| Filter | Events | Excluded | Live Spread | Best Placebo Shift | Best Placebo Spread | Shift Passed |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in score_gate_summary.to_dict(orient="records"):
        lines.append(
            f"| `{row['filter_name']}` | {int(row['surviving_event_count'])} | {int(row['excluded_event_count'])} | "
            f"{_fmt(row.get('live_primary_mean_top_bottom_spread'))} | "
            f"{_fmt(row.get('best_placebo_shift_trading_days'))} | "
            f"{_fmt(row.get('best_placebo_mean_top_bottom_spread'))} | "
            f"`{row['event_date_shift_passed']}` |"
        )
    lines.extend(
        [
            "",
            "## Regime Week Classification",
            "",
            f"- week_count: `{len(regime_week_classification)}`",
            f"- high_volatility_week_count: `{int(regime_week_classification['high_volatility_week'].sum())}`",
            f"- low_liquidity_week_count: `{int(regime_week_classification['low_liquidity_week'].sum())}`",
            "",
            "## Boundaries",
            "",
            "- This phase validates the H1E.1 market-regime attribution only.",
            "- Missing denominator, price, return, or liquidity coverage remains unavailable/no_view and is not encoded as zero alpha.",
            "- Downstream typed projection, Q2, optimizer-path evaluation, and any paper-stage work require a separate explicit reopen.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_sue_regime_filter_placebo_report_language(text: str) -> None:
    """Reject misleading H1E.2 claims while allowing explicit non-claims."""

    scrubbed = str(text).lower()
    allowed_phrases = [
        "h1e.2 validates the market-regime attribution only.",
        "it reruns filtered score-gate summaries and placebo curves after excluding march 2020, high-volatility weeks, and low-liquidity weeks.",
        "it does not select a sue score, run q2, run optimizer-path evaluation, promote alpha registry state, open paper/live/broker/order workflows, or approve production use.",
        "this phase validates the h1e.1 market-regime attribution only.",
        "missing denominator, price, return, or liquidity coverage remains unavailable/no_view and is not encoded as zero alpha.",
        "downstream typed projection, q2, optimizer-path evaluation, and any paper-stage work require a separate explicit reopen.",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in MISLEADING_REGIME_FILTER_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE regime-filter placebo claim detected: {claim}")


def _filtered_event_sets(
    *,
    safe_events: pd.DataFrame,
    regime_week_classification: pd.DataFrame,
) -> list[tuple[str, pd.DataFrame, str]]:
    events = safe_events.copy()
    dates = pd.to_datetime(events["rebalance_date"], errors="coerce")
    events["_event_week"] = dates.dt.to_period("W-FRI").astype(str)
    events["_is_march_2020"] = dates.dt.to_period("M").astype(str).eq("2020-03")
    high_vol_weeks = set(
        regime_week_classification.loc[regime_week_classification["high_volatility_week"], "week"].astype(str)
    )
    low_liq_weeks = set(
        regime_week_classification.loc[regime_week_classification["low_liquidity_week"], "week"].astype(str)
    )
    high_vol = events["_event_week"].isin(high_vol_weeks)
    low_liq = events["_event_week"].isin(low_liq_weeks)
    march = events["_is_march_2020"]
    filters = [
        ("baseline", pd.Series(True, index=events.index), "none"),
        ("exclude_march_2020", ~march, "march_2020"),
        ("exclude_high_volatility_weeks", ~high_vol, "high_volatility_week"),
        ("exclude_low_liquidity_weeks", ~low_liq, "low_liquidity_week"),
        ("exclude_market_regime_weeks", ~(march | high_vol | low_liq), "march_or_high_volatility_or_low_liquidity"),
    ]
    return [(name, events.loc[mask].drop(columns=["_event_week", "_is_march_2020"]).copy(), reason) for name, mask, reason in filters]


def _filtered_placebo_curve(
    *,
    filtered_events: pd.DataFrame,
    price_index: dict[int, pd.DataFrame],
    config: SueRegimeFilterPlaceboConfig,
) -> pd.DataFrame:
    if filtered_events.empty:
        return pd.DataFrame()
    placebo_config = _placebo_config(config)
    gate_config = _helper_gate_config(placebo_config)
    evidence_config = _helper_evidence_config(config)
    shifted_returns = _build_shifted_window_return_frame(
        safe_events=filtered_events,
        price_index=price_index,
        shifts=config.shift_trading_days,
    )
    scored = _attach_gate_scores(
        return_frame=shifted_returns,
        safe_events=filtered_events,
        crsp_daily_path=config.crsp_daily_path,
        config=gate_config,
    )
    return _placebo_shift_curve(scored_frame=scored, score_name=config.score_name, config=evidence_config)


def _filter_summary_row(
    *,
    filter_name: str,
    safe_event_count: int,
    filtered_event_count: int,
    excluded_reason: str,
    placebo_shift_curve: pd.DataFrame,
    config: SueRegimeFilterPlaceboConfig,
) -> dict[str, Any]:
    if placebo_shift_curve.empty or "window_name" not in placebo_shift_curve.columns:
        live = {"window_name": PRIMARY_WINDOW, "shift_trading_days": 0}
        best_placebo = {}
    else:
        live = _primary_curve_row(scored_frame=placebo_shift_curve, shift=0)
        best_placebo = _best_placebo_row(placebo_shift_curve)
    live_rank = _float_or_none(live.get("mean_rank_ic"))
    live_spread = _float_or_none(live.get("mean_top_bottom_spread"))
    placebo_rank = _float_or_none(best_placebo.get("mean_rank_ic"))
    placebo_spread = _float_or_none(best_placebo.get("mean_top_bottom_spread"))
    shift_passed = bool(
        live_rank is not None
        and live_spread is not None
        and (placebo_rank is None or live_rank > placebo_rank)
        and (placebo_spread is None or live_spread > placebo_spread)
    )
    if filtered_event_count == 0:
        status = "unavailable_no_events_after_filter"
    elif shift_passed:
        status = "event_date_shift_passed_after_filter"
    else:
        status = "event_date_shift_still_fails_after_filter"
    return {
        "schema_version": SUE_REGIME_FILTER_PLACEBO_SCHEMA_VERSION,
        "filter_name": filter_name,
        "excluded_reason": excluded_reason,
        "score_name": config.score_name,
        "primary_window": PRIMARY_WINDOW,
        "surviving_event_count": filtered_event_count,
        "excluded_event_count": int(safe_event_count - filtered_event_count),
        "live_primary_mean_rank_ic": live_rank,
        "live_primary_mean_top_bottom_spread": live_spread,
        "best_placebo_shift_trading_days": best_placebo.get("shift_trading_days"),
        "best_placebo_mean_rank_ic": placebo_rank,
        "best_placebo_mean_top_bottom_spread": placebo_spread,
        "placebo_advantage_rank_ic": (placebo_rank - live_rank) if placebo_rank is not None and live_rank is not None else None,
        "placebo_advantage_top_bottom_spread": (
            placebo_spread - live_spread if placebo_spread is not None and live_spread is not None else None
        ),
        "placebo_advantage_reduction_vs_baseline": 0.0 if filter_name == "baseline" else None,
        "event_date_shift_passed": shift_passed,
        "score_gate_status": status,
        "selected_score": None,
        "score_selection_ran": False,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "production_approval_claimed": False,
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
    }


def _regime_week_classification(path: str, config: SueRegimeFilterPlaceboConfig) -> pd.DataFrame:
    prices = pd.read_csv(path)
    required = {"date", "ret", "prc"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError("CRSP daily prices missing regime-filter columns: " + ", ".join(sorted(missing)))
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    frame["ret"] = pd.to_numeric(frame["ret"], errors="coerce")
    frame["week"] = frame["date"].dt.to_period("W-FRI").astype(str)
    daily_market = frame.groupby("date", sort=True)["ret"].mean().reset_index(name="equal_weight_market_return")
    daily_market["week"] = daily_market["date"].dt.to_period("W-FRI").astype(str)
    weekly = daily_market.groupby("week", sort=True).agg(
        weekly_market_std=("equal_weight_market_return", "std"),
        weekly_mean_abs_market_return=("equal_weight_market_return", lambda series: float(series.abs().mean())),
    )
    weekly["weekly_market_volatility"] = weekly["weekly_market_std"].fillna(0.0) + weekly[
        "weekly_mean_abs_market_return"
    ].fillna(0.0)
    if "vol" in frame.columns:
        frame["vol"] = pd.to_numeric(frame["vol"], errors="coerce")
        frame["dollar_volume"] = pd.to_numeric(frame["prc"], errors="coerce").abs() * frame["vol"]
        liquidity = frame.groupby("week", sort=True)["dollar_volume"].median().rename("weekly_liquidity_metric")
        liquidity_source = "dollar_volume"
    else:
        liquidity = frame.groupby("week", sort=True).size().rename("weekly_liquidity_metric")
        liquidity_source = "price_observation_count_proxy_missing_volume"
    weekly = weekly.join(liquidity, how="outer").fillna({"weekly_liquidity_metric": 0.0})
    vol_threshold = float(weekly["weekly_market_volatility"].quantile(config.high_volatility_week_quantile))
    liq_threshold = float(weekly["weekly_liquidity_metric"].quantile(config.low_liquidity_week_quantile))
    weekly = weekly.reset_index()
    weekly["schema_version"] = "sue_regime_week_classification.v1"
    weekly["high_volatility_threshold"] = vol_threshold
    weekly["low_liquidity_threshold"] = liq_threshold
    weekly["high_volatility_week"] = weekly["weekly_market_volatility"].ge(vol_threshold)
    weekly["low_liquidity_week"] = weekly["weekly_liquidity_metric"].le(liq_threshold)
    weekly["low_liquidity_filter_source"] = liquidity_source
    return weekly.loc[
        :,
        [
            "schema_version",
            "week",
            "weekly_market_std",
            "weekly_mean_abs_market_return",
            "weekly_market_volatility",
            "weekly_liquidity_metric",
            "high_volatility_threshold",
            "low_liquidity_threshold",
            "high_volatility_week",
            "low_liquidity_week",
            "low_liquidity_filter_source",
        ],
    ]


def _regime_filter_summary(
    *,
    config: SueRegimeFilterPlaceboConfig,
    score_gate_summary: pd.DataFrame,
    regime_week_classification: pd.DataFrame,
) -> dict[str, Any]:
    baseline = _summary_row(score_gate_summary, "baseline")
    best_filtered = _best_filtered_reduction(score_gate_summary)
    resolved_filters = score_gate_summary.loc[score_gate_summary["event_date_shift_passed"].astype(bool)]
    if not resolved_filters.empty:
        interpretation = "market_regime_filter_resolves_placebo_failure_in_slice"
    elif _float_or_none(best_filtered.get("placebo_advantage_reduction_vs_baseline")) and _float_or_none(
        best_filtered.get("placebo_advantage_reduction_vs_baseline")
    ) > 0:
        interpretation = "market_regime_filter_reduces_but_does_not_resolve_placebo_failure"
    else:
        interpretation = "market_regime_filter_does_not_resolve_placebo_failure"
    payload = {
        "schema_version": SUE_REGIME_FILTER_PLACEBO_SCHEMA_VERSION,
        "score_name": config.score_name,
        "selected_score": None,
        "interpretation": interpretation,
        "baseline_best_placebo_shift_trading_days": baseline.get("best_placebo_shift_trading_days"),
        "baseline_placebo_advantage_top_bottom_spread": baseline.get("placebo_advantage_top_bottom_spread"),
        "best_filter_by_advantage_reduction": best_filtered.get("filter_name"),
        "best_filter_placebo_advantage_reduction": best_filtered.get("placebo_advantage_reduction_vs_baseline"),
        "high_volatility_week_count": int(regime_week_classification["high_volatility_week"].sum()),
        "low_liquidity_week_count": int(regime_week_classification["low_liquidity_week"].sum()),
        "low_liquidity_filter_source": str(regime_week_classification["low_liquidity_filter_source"].iloc[0])
        if not regime_week_classification.empty
        else "unavailable",
        "score_selection_ran": False,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "alpha_registry_promoted": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _summary_row(frame: pd.DataFrame, filter_name: str) -> dict[str, Any]:
    rows = frame.loc[frame["filter_name"].eq(filter_name)]
    if rows.empty:
        return {}
    return rows.iloc[0].where(pd.notna(rows.iloc[0]), None).to_dict()


def _best_filtered_reduction(frame: pd.DataFrame) -> dict[str, Any]:
    rows = frame.loc[~frame["filter_name"].eq("baseline")].copy()
    if rows.empty:
        return {}
    rows["reduction_score"] = pd.to_numeric(rows["placebo_advantage_reduction_vs_baseline"], errors="coerce").fillna(
        -999.0
    )
    return rows.sort_values("reduction_score", ascending=False).iloc[0].drop(labels=["reduction_score"]).where(
        lambda series: pd.notna(series), None
    ).to_dict()


def _placebo_config(config: SueRegimeFilterPlaceboConfig) -> _PlaceboConfig:
    return _PlaceboConfig(
        events_path=config.events_path,
        crsp_daily_path=config.crsp_daily_path,
        output_dir=config.output_dir,
        report_path=config.report_path,
        score_name=config.score_name,
        shift_trading_days=config.shift_trading_days,
        quantiles=config.quantiles,
        min_rank_ic_names=config.min_rank_ic_names,
        min_spread_names=config.min_spread_names,
        denominator_abs_min=config.denominator_abs_min,
        winsorization_scope=config.winsorization_scope,
        winsor_lower_quantile=config.winsor_lower_quantile,
        winsor_upper_quantile=config.winsor_upper_quantile,
        extreme_value_cap=config.extreme_value_cap,
        random_seed=config.random_seed,
    )


def _helper_evidence_config(config: SueRegimeFilterPlaceboConfig) -> SueHistoricalEventEvidenceConfig:
    return SueHistoricalEventEvidenceConfig(
        events_path=config.events_path,
        sue_values_path=config.events_path,
        crsp_daily_path=config.crsp_daily_path,
        output_dir=config.output_dir,
        report_path=config.report_path,
        quantiles=config.quantiles,
        min_rank_ic_names=config.min_rank_ic_names,
        min_spread_names=config.min_spread_names,
        placebo_shift_trading_days=10,
        random_seed=config.random_seed,
        evidence_scope="expanded",
    )


def _float_or_none(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _fmt(value: Any) -> str:
    if value is None or pd.isna(value):
        return "NA"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
