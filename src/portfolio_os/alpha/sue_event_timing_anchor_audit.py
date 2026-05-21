"""SUE event timing and anchor-definition audit.

This Reopen-H1E.3 diagnostic audits whether the SUE event anchor is too late,
whether pre-event drift dominates the live event window, and whether shifted
placebo windows are constructed correctly. It does not select a score, run Q2,
invoke optimizers, promote Alpha Registry state, open paper/live/broker/order
workflows, or approve production use.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.alpha.sue_historical_event_evidence import (
    SueHistoricalEventEvidenceConfig,
    _load_and_validate_events,
    _load_prices,
    _price_index,
    _rank_ic_by_date,
    _safe_events,
    _top_bottom_spread_by_date,
)
from portfolio_os.alpha.sue_historical_schema import validate_no_forward_return_feature_columns
from portfolio_os.alpha.sue_placebo_failure_attribution import (
    DEFAULT_CRSP_DAILY_PATH,
    DEFAULT_EVENTS_PATH,
    _fast_window_return,
)
from portfolio_os.provenance.hashing import canonical_json, hash_payload


SUE_EVENT_TIMING_ANCHOR_AUDIT_SCHEMA_VERSION = "sue_event_timing_anchor_audit.v1"
DEFAULT_OUTPUT_DIR = "outputs/sue_event_timing_anchor_audit"
DEFAULT_REPORT_PATH = "reports/sue_event_timing_anchor_audit_report.md"
PRIMARY_WINDOW = "plus_2_plus_22"

ANCHOR_DEFINITIONS = [
    {"anchor_definition": "current_tradable", "source": "tradable_date", "offset": 0},
    {"anchor_definition": "announcement_plus_0_td", "source": "announcement_date", "offset": 0},
    {"anchor_definition": "announcement_plus_1_td", "source": "announcement_date", "offset": 1},
    {"anchor_definition": "announcement_plus_2_td", "source": "announcement_date", "offset": 2},
    {"anchor_definition": "shift_minus_2_td", "source": "tradable_date", "offset": -2},
    {"anchor_definition": "shift_minus_5_td", "source": "tradable_date", "offset": -5},
    {"anchor_definition": "shift_minus_10_td", "source": "tradable_date", "offset": -10},
    {"anchor_definition": "shift_plus_2_td", "source": "tradable_date", "offset": 2},
    {"anchor_definition": "shift_plus_5_td", "source": "tradable_date", "offset": 5},
    {"anchor_definition": "shift_plus_10_td", "source": "tradable_date", "offset": 10},
]

STANDARD_WINDOWS = [
    {"window_name": "plus_2_plus_2", "start_offset": 2, "end_offset": 2},
    {"window_name": "plus_2_plus_3", "start_offset": 2, "end_offset": 3},
    {"window_name": "plus_2_plus_22", "start_offset": 2, "end_offset": 22},
]

DRIFT_WINDOWS = [
    {"window_name": "minus_10_minus_6", "start_offset": -10, "end_offset": -6},
    {"window_name": "minus_5_minus_1", "start_offset": -5, "end_offset": -1},
    {"window_name": "minus_2_minus_1", "start_offset": -2, "end_offset": -1},
    {"window_name": "zero_plus_1", "start_offset": 0, "end_offset": 1},
    {"window_name": "plus_2_plus_3", "start_offset": 2, "end_offset": 3},
    {"window_name": "plus_2_plus_22", "start_offset": 2, "end_offset": 22},
]

MISLEADING_ANCHOR_AUDIT_CLAIMS = (
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


class SueEventTimingAnchorAuditConfig(BaseModel):
    """Config for Reopen-H1E.3 SUE event timing / anchor audit."""

    model_config = ConfigDict(extra="forbid")

    events_path: str = DEFAULT_EVENTS_PATH
    crsp_daily_path: str = DEFAULT_CRSP_DAILY_PATH
    output_dir: str = DEFAULT_OUTPUT_DIR
    report_path: str = DEFAULT_REPORT_PATH
    score_name: str = "surprise_pct_actual_eps"
    quantiles: int = Field(default=5, gt=1)
    min_rank_ic_names: int = Field(default=3, gt=1)
    min_spread_names: int = Field(default=5, gt=1)
    denominator_abs_min: float = Field(default=0.01, gt=0.0)
    winsorization_scope: str = "month"
    winsor_lower_quantile: float = Field(default=0.01, ge=0.0, lt=0.5)
    winsor_upper_quantile: float = Field(default=0.99, gt=0.5, le=1.0)
    extreme_value_cap: float = Field(default=1000.0, gt=0.0)
    after_close_utc_hour: int = Field(default=21, ge=0, le=23)
    before_open_utc_hour: int = Field(default=14, ge=0, le=23)
    random_seed: int = 20260508


@dataclass(frozen=True)
class SueEventTimingAnchorAuditResult:
    """In-memory H1E.3 anchor audit result."""

    config: SueEventTimingAnchorAuditConfig
    anchor_grid: pd.DataFrame
    pre_event_drift_grid: pd.DataFrame
    timing_quality_breakdown: pd.DataFrame
    window_overlap_audit: dict[str, Any]
    market_timing_audit: pd.DataFrame
    anchor_selection_diagnostic: dict[str, Any]
    report_text: str


def load_sue_event_timing_anchor_audit_config(path: str | Path) -> SueEventTimingAnchorAuditConfig:
    """Load H1E.3 config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    guards = payload.get("guards") or {}
    return SueEventTimingAnchorAuditConfig(
        events_path=str(inputs.get("events_path") or payload.get("events_path") or DEFAULT_EVENTS_PATH),
        crsp_daily_path=str(inputs.get("crsp_daily_path") or payload.get("crsp_daily_path") or DEFAULT_CRSP_DAILY_PATH),
        output_dir=str(outputs.get("output_dir") or payload.get("output_dir") or DEFAULT_OUTPUT_DIR),
        report_path=str(outputs.get("report_path") or payload.get("report_path") or DEFAULT_REPORT_PATH),
        score_name=str(payload.get("score_name", "surprise_pct_actual_eps")),
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
        after_close_utc_hour=int(payload.get("after_close_utc_hour", 21)),
        before_open_utc_hour=int(payload.get("before_open_utc_hour", 14)),
        random_seed=int(payload.get("random_seed", 20260508)),
    )


def build_sue_event_timing_anchor_audit(
    config: SueEventTimingAnchorAuditConfig | None = None,
) -> SueEventTimingAnchorAuditResult:
    """Build H1E.3 anchor and timing diagnostics."""

    resolved = config or SueEventTimingAnchorAuditConfig()
    events = _load_and_validate_events(resolved.events_path)
    validate_no_forward_return_feature_columns(list(events.columns))
    prices = _load_prices(resolved.crsp_daily_path)
    price_index = _price_index(prices)
    safe_events = _safe_events(events)
    anchor_returns = _build_anchor_return_frame(
        safe_events=safe_events,
        price_index=price_index,
        anchor_definitions=ANCHOR_DEFINITIONS,
        windows=STANDARD_WINDOWS,
    )
    drift_returns = _build_anchor_return_frame(
        safe_events=safe_events,
        price_index=price_index,
        anchor_definitions=[{"anchor_definition": "current_tradable", "source": "tradable_date", "offset": 0}],
        windows=DRIFT_WINDOWS,
    )
    evidence_config = _helper_evidence_config(resolved)
    scored_anchor = _attach_selected_score(return_frame=anchor_returns, config=resolved)
    scored_drift = _attach_selected_score(return_frame=drift_returns, config=resolved)
    if resolved.score_name not in scored_anchor.columns:
        raise ValueError(f"score_name is not available in scored SUE frame: {resolved.score_name}")

    market_index = _market_return_index(prices)
    scored_anchor = _attach_market_window_return(scored_anchor, market_index)
    anchor_grid = _anchor_grid(scored_anchor=scored_anchor, score_name=resolved.score_name, config=evidence_config)
    pre_event_drift_grid = _anchor_grid(
        scored_anchor=scored_drift,
        score_name=resolved.score_name,
        config=evidence_config,
        schema_version="sue_pre_event_drift_grid.v1",
    ).rename(columns={"anchor_definition": "anchor_definition"})
    timing_quality_breakdown = _timing_quality_breakdown(safe_events, resolved)
    window_overlap_audit = _window_overlap_audit(events=events, scored_anchor=scored_anchor)
    market_timing_audit = _market_timing_audit(scored_anchor=scored_anchor, score_name=resolved.score_name, config=resolved)
    anchor_selection_diagnostic = _anchor_selection_diagnostic(
        config=resolved,
        anchor_grid=anchor_grid,
        pre_event_drift_grid=pre_event_drift_grid,
        timing_quality_breakdown=timing_quality_breakdown,
        window_overlap_audit=window_overlap_audit,
    )
    report_text = render_sue_event_timing_anchor_audit_report(
        anchor_grid=anchor_grid,
        pre_event_drift_grid=pre_event_drift_grid,
        timing_quality_breakdown=timing_quality_breakdown,
        window_overlap_audit=window_overlap_audit,
        anchor_selection_diagnostic=anchor_selection_diagnostic,
    )
    validate_sue_event_timing_anchor_audit_report_language(report_text)
    return SueEventTimingAnchorAuditResult(
        config=resolved,
        anchor_grid=anchor_grid,
        pre_event_drift_grid=pre_event_drift_grid,
        timing_quality_breakdown=timing_quality_breakdown,
        window_overlap_audit=window_overlap_audit,
        market_timing_audit=market_timing_audit,
        anchor_selection_diagnostic=anchor_selection_diagnostic,
        report_text=report_text,
    )


def write_sue_event_timing_anchor_audit_artifacts(
    result: SueEventTimingAnchorAuditResult,
) -> dict[str, Path]:
    """Write H1E.3 anchor audit artifacts."""

    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(result.config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    paths = {
        "anchor_grid": output_dir / "anchor_grid.csv",
        "pre_event_drift_grid": output_dir / "pre_event_drift_grid.csv",
        "timing_quality_breakdown": output_dir / "timing_quality_breakdown.csv",
        "window_overlap_audit": output_dir / "window_overlap_audit.json",
        "market_timing_audit": output_dir / "market_timing_audit.csv",
        "anchor_selection_diagnostic": output_dir / "anchor_selection_diagnostic.json",
        "report": report_path,
    }
    result.anchor_grid.to_csv(paths["anchor_grid"], index=False)
    result.pre_event_drift_grid.to_csv(paths["pre_event_drift_grid"], index=False)
    result.timing_quality_breakdown.to_csv(paths["timing_quality_breakdown"], index=False)
    result.market_timing_audit.to_csv(paths["market_timing_audit"], index=False)
    _write_json(paths["window_overlap_audit"], result.window_overlap_audit)
    _write_json(paths["anchor_selection_diagnostic"], result.anchor_selection_diagnostic)
    validate_sue_event_timing_anchor_audit_report_language(result.report_text)
    paths["report"].write_text(result.report_text, encoding="utf-8")
    return paths


def render_sue_event_timing_anchor_audit_report(
    *,
    anchor_grid: pd.DataFrame,
    pre_event_drift_grid: pd.DataFrame,
    timing_quality_breakdown: pd.DataFrame,
    window_overlap_audit: dict[str, Any],
    anchor_selection_diagnostic: dict[str, Any],
) -> str:
    """Render H1E.3 event timing / anchor audit report."""

    primary = anchor_grid.loc[anchor_grid["window_name"].eq(PRIMARY_WINDOW)].copy()
    primary = primary.sort_values("mean_top_bottom_spread", ascending=False)
    drift = pre_event_drift_grid.sort_values("mean_top_bottom_spread", ascending=False)
    lines = [
        "# SUE Event Timing / Anchor Definition Audit",
        "",
        "This is an event timing and anchor audit only.",
        "It does not select a production SUE score.",
        "It does not run Q2 or optimizer-path evaluation.",
        "It does not approve paper/live/broker/order/production workflows.",
        "SUE remains blocked before typed projection unless anchor definition is corrected and the score gate is rerun.",
        "",
        "## Summary",
        "",
        f"- schema_version: `{anchor_selection_diagnostic['schema_version']}`",
        f"- score_name: `{anchor_selection_diagnostic['score_name']}`",
        f"- interpretation: `{anchor_selection_diagnostic['interpretation']}`",
        f"- selected_score: `{anchor_selection_diagnostic['selected_score']}`",
        f"- best_anchor_definition: `{anchor_selection_diagnostic.get('best_anchor_definition')}`",
        f"- best_pre_event_window: `{anchor_selection_diagnostic.get('best_pre_event_window')}`",
        f"- q2_evaluation_ran: `{anchor_selection_diagnostic['q2_evaluation_ran']}`",
        f"- optimizer_path_evaluation_ran: `{anchor_selection_diagnostic['optimizer_path_evaluation_ran']}`",
        f"- production_approval_claimed: `{anchor_selection_diagnostic['production_approval_claimed']}`",
        "",
        "## Primary Anchor Grid",
        "",
        "| Anchor | Rank IC | Top-Bottom Spread | Date Count |",
        "| --- | ---: | ---: | ---: |",
    ]
    for row in primary.to_dict(orient="records"):
        lines.append(
            f"| `{row['anchor_definition']}` | {_fmt(row.get('mean_rank_ic'))} | "
            f"{_fmt(row.get('mean_top_bottom_spread'))} | {int(row.get('top_bottom_date_count') or 0)} |"
        )
    lines.extend(["", "## Pre-Event Drift Grid", "", "| Window | Rank IC | Top-Bottom Spread |", "| --- | ---: | ---: |"])
    for row in drift.to_dict(orient="records"):
        lines.append(
            f"| `{row['window_name']}` | {_fmt(row.get('mean_rank_ic'))} | "
            f"{_fmt(row.get('mean_top_bottom_spread'))} |"
        )
    lines.extend(
        [
            "",
            "## Timing Quality",
            "",
        ]
    )
    for row in timing_quality_breakdown.to_dict(orient="records"):
        lines.append(f"- {row['timing_quality']}: `{int(row['event_count'])}`")
    lines.extend(
        [
            "",
            "## Window Overlap Audit",
            "",
            f"- shifted_anchor_actually_changes_return_window: `{window_overlap_audit['shifted_anchor_actually_changes_return_window']}`",
            f"- shifted_placebo_window_bug_detected: `{window_overlap_audit['shifted_placebo_window_bug_detected']}`",
            f"- benchmark_window_uses_shifted_anchor: `{window_overlap_audit['benchmark_window_uses_shifted_anchor']}`",
            f"- market_adjusted_spread_uses_shifted_anchor: `{window_overlap_audit['market_adjusted_spread_uses_shifted_anchor']}`",
            f"- event_available_after_tradable_violations: `{window_overlap_audit['event_available_after_tradable_violations']}`",
            "",
            "## Boundaries",
            "",
            "- This audit does not promote Alpha Registry state.",
            "- Missing SUE, denominator, price, or return coverage remains unavailable/no_view and is not encoded as zero alpha.",
            "- If anchor definition is revised, H1E must be rerun before any typed projection or Q2 work.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_sue_event_timing_anchor_audit_report_language(text: str) -> None:
    """Reject misleading H1E.3 claims while allowing explicit non-claims."""

    scrubbed = str(text).lower()
    allowed_phrases = [
        "this is an event timing and anchor audit only.",
        "it does not select a production sue score.",
        "it does not run q2 or optimizer-path evaluation.",
        "it does not approve paper/live/broker/order/production workflows.",
        "sue remains blocked before typed projection unless anchor definition is corrected and the score gate is rerun.",
        "this audit does not promote alpha registry state.",
        "missing sue, denominator, price, or return coverage remains unavailable/no_view and is not encoded as zero alpha.",
        "if anchor definition is revised, h1e must be rerun before any typed projection or q2 work.",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in MISLEADING_ANCHOR_AUDIT_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE event timing anchor audit claim detected: {claim}")


def _build_anchor_return_frame(
    *,
    safe_events: pd.DataFrame,
    price_index: dict[int, pd.DataFrame],
    anchor_definitions: list[dict[str, Any]],
    windows: list[dict[str, Any]],
) -> pd.DataFrame:
    price_arrays = {
        int(permno): (list(group["date"]), pd.to_numeric(group["ret"], errors="coerce").to_list())
        for permno, group in price_index.items()
    }
    rows: list[dict[str, Any]] = []
    for event in safe_events.to_dict(orient="records"):
        permno = int(event["permno"])
        dates, returns = price_arrays.get(permno, ([], []))
        tradable_date = event["tradable_date"]
        announcement_date = event["announcement_date"]
        tradable_index = bisect_left(dates, tradable_date) if dates else None
        announcement_index = bisect_left(dates, announcement_date) if dates else None
        for anchor in anchor_definitions:
            source_index = announcement_index if anchor["source"] == "announcement_date" else tradable_index
            anchor_index = None if source_index is None else source_index + int(anchor["offset"])
            anchor_date = dates[anchor_index] if anchor_index is not None and 0 <= anchor_index < len(dates) else None
            for window in windows:
                realized_return, start_date, end_date = _fast_window_return(
                    dates=dates,
                    returns=returns,
                    anchor_index=anchor_index,
                    start_offset=int(window["start_offset"]),
                    end_offset=int(window["end_offset"]),
                )
                rows.append(
                    {
                        "event_id": event["event_id"],
                        "symbol": event["symbol"],
                        "permno": permno,
                        "rebalance_date": event["rebalance_date"].isoformat(),
                        "announcement_date": announcement_date.isoformat(),
                        "tradable_date": tradable_date.isoformat(),
                        "event_available_timestamp": event["event_available_timestamp"],
                        "tradable_timestamp": event["tradable_timestamp"],
                        "estimate_snapshot_date": event["estimate_snapshot_date"],
                        "actual_eps": event.get("actual_eps"),
                        "expected_eps": event.get("expected_eps"),
                        "sue_value": float(event["sue_value"]),
                        "anchor_definition": anchor["anchor_definition"],
                        "anchor_source": anchor["source"],
                        "anchor_offset_trading_days": int(anchor["offset"]),
                        "anchor_date": anchor_date.isoformat() if anchor_date else None,
                        "window_name": window["window_name"],
                        "start_offset": window["start_offset"],
                        "end_offset": window["end_offset"],
                        "window_return": realized_return,
                        "actual_return_window_start": start_date.isoformat() if start_date else None,
                        "actual_return_window_end": end_date.isoformat() if end_date else None,
                        "status": "observed" if realized_return is not None else "unavailable_missing_return_window",
                    }
                )
    return pd.DataFrame(rows)


def _attach_selected_score(
    *,
    return_frame: pd.DataFrame,
    config: SueEventTimingAnchorAuditConfig,
) -> pd.DataFrame:
    """Attach only the pre-registered score needed by H1E.3.

    H1E.3 compares event anchors for a single score; using the full score gate
    here multiplies work by every anchor/window row and makes the expanded-panel
    audit unnecessarily slow. Missing denominators remain missing/no_view and
    are never encoded as zero alpha.
    """

    frame = return_frame.copy()
    raw_surprise = pd.to_numeric(frame["sue_value"], errors="coerce")
    if config.score_name == "raw_eps_diff":
        score = raw_surprise
    elif config.score_name == "surprise_pct_actual_eps":
        denominator = pd.to_numeric(frame["actual_eps"], errors="coerce").abs()
        score = _divide_with_denominator_guard(raw_surprise, denominator, config.denominator_abs_min)
    elif config.score_name == "surprise_pct_expected_eps":
        denominator = pd.to_numeric(frame["expected_eps"], errors="coerce").abs()
        score = _divide_with_denominator_guard(raw_surprise, denominator, config.denominator_abs_min)
    elif config.score_name == "surprise_scaled_price":
        score = pd.Series(pd.NA, index=frame.index, dtype="Float64")
    else:
        raise ValueError(f"unsupported H1E.3 SUE score_name: {config.score_name}")
    frame[config.score_name] = _winsorize_and_cap_score(score, frame, config)
    frame["score_status"] = frame[config.score_name].notna().map({True: "active_view", False: "no_view"})
    return frame


def _divide_with_denominator_guard(
    numerator: pd.Series,
    denominator: pd.Series,
    denominator_abs_min: float,
) -> pd.Series:
    valid = denominator.notna() & denominator.ge(float(denominator_abs_min))
    return numerator.where(valid) / denominator.where(valid)


def _winsorize_and_cap_score(
    score: pd.Series,
    frame: pd.DataFrame,
    config: SueEventTimingAnchorAuditConfig,
) -> pd.Series:
    values = pd.to_numeric(score, errors="coerce")
    if values.dropna().empty:
        return values
    if config.winsorization_scope == "month":
        months = pd.to_datetime(frame["rebalance_date"], errors="coerce").dt.to_period("M").astype(str)
        clipped = values.groupby(months).transform(lambda series: _winsorize_series(series, config))
    elif config.winsorization_scope == "global":
        clipped = _winsorize_series(values, config)
    else:
        raise ValueError(f"unsupported winsorization_scope: {config.winsorization_scope}")
    return clipped.clip(lower=-float(config.extreme_value_cap), upper=float(config.extreme_value_cap))


def _winsorize_series(series: pd.Series, config: SueEventTimingAnchorAuditConfig) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce")
    observed = clean.dropna()
    if observed.empty:
        return clean
    lower = float(observed.quantile(config.winsor_lower_quantile))
    upper = float(observed.quantile(config.winsor_upper_quantile))
    return clean.clip(lower=lower, upper=upper)


def _anchor_grid(
    *,
    scored_anchor: pd.DataFrame,
    score_name: str,
    config: SueHistoricalEventEvidenceConfig,
    schema_version: str = "sue_anchor_grid.v1",
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for anchor_definition, frame in scored_anchor.groupby("anchor_definition", sort=True):
        rank_rows = _rank_ic_by_date(frame, score_column=score_name, config=config)
        spread_rows = _top_bottom_spread_by_date(frame, score_column=score_name, config=config)
        for window_name in sorted(frame["window_name"].dropna().unique()):
            returns = frame.loc[frame["window_name"].eq(window_name)]
            rank_values = pd.to_numeric(
                rank_rows.loc[rank_rows["window_name"].eq(window_name), "rank_ic"], errors="coerce"
            ).dropna()
            spread_values = pd.to_numeric(
                spread_rows.loc[spread_rows["window_name"].eq(window_name), "top_bottom_spread"], errors="coerce"
            ).dropna()
            rows.append(
                {
                    "schema_version": schema_version,
                    "anchor_definition": anchor_definition,
                    "score_name": score_name,
                    "window_name": window_name,
                    "safe_row_count": int(returns["window_return"].notna().sum()),
                    "missing_return_window_count": int(returns["window_return"].isna().sum()),
                    "rank_ic_date_count": int(len(rank_values)),
                    "mean_rank_ic": _mean_or_none(rank_values),
                    "rank_ic_t_stat": _t_stat(rank_values),
                    "top_bottom_date_count": int(len(spread_values)),
                    "mean_top_bottom_spread": _mean_or_none(spread_values),
                    "top_bottom_t_stat": _t_stat(spread_values),
                }
            )
    return pd.DataFrame(rows)


def _timing_quality_breakdown(
    safe_events: pd.DataFrame,
    config: SueEventTimingAnchorAuditConfig,
) -> pd.DataFrame:
    frame = safe_events.copy()
    timestamps = pd.to_datetime(frame["event_available_timestamp"], errors="coerce", utc=True)
    frame["timing_quality"] = [_timing_quality(value, config) for value in timestamps]
    rows = (
        frame.groupby("timing_quality", sort=True)
        .agg(event_count=("event_id", "nunique"), rebalance_date_count=("rebalance_date", "nunique"))
        .reset_index()
    )
    observed = set(rows["timing_quality"]) if not rows.empty else set()
    for label in [
        "announcement_time_known",
        "announcement_time_missing",
        "before_open",
        "after_close",
        "date_only",
        "ambiguous_timing",
        "diagnostic_only_timing",
    ]:
        if label not in observed:
            rows = pd.concat(
                [
                    rows,
                    pd.DataFrame([{"timing_quality": label, "event_count": 0, "rebalance_date_count": 0}]),
                ],
                ignore_index=True,
            )
    rows.insert(0, "schema_version", "sue_timing_quality_breakdown.v1")
    return rows.sort_values("timing_quality").reset_index(drop=True)


def _timing_quality(value: pd.Timestamp, config: SueEventTimingAnchorAuditConfig) -> str:
    if pd.isna(value):
        return "announcement_time_missing"
    if value.hour == 0 and value.minute == 0 and value.second == 0:
        return "date_only"
    if value.hour < config.before_open_utc_hour or (value.hour == config.before_open_utc_hour and value.minute <= 30):
        return "before_open"
    if value.hour >= config.after_close_utc_hour:
        return "after_close"
    return "ambiguous_timing"


def _window_overlap_audit(*, events: pd.DataFrame, scored_anchor: pd.DataFrame) -> dict[str, Any]:
    current = scored_anchor.loc[scored_anchor["anchor_definition"].eq("current_tradable")].copy()
    shifted = scored_anchor.loc[scored_anchor["anchor_definition"].str.startswith("shift_")].copy()
    event_available = pd.to_datetime(events["event_available_timestamp"], errors="coerce", utc=True)
    tradable = pd.to_datetime(events["tradable_timestamp"], errors="coerce", utc=True)
    estimate_snapshot = pd.to_datetime(events["estimate_snapshot_date"], errors="coerce", utc=True)
    merged = shifted.merge(
        current.loc[
            :,
            [
                "event_id",
                "window_name",
                "actual_return_window_start",
                "actual_return_window_end",
            ],
        ].rename(
            columns={
                "actual_return_window_start": "current_return_window_start",
                "actual_return_window_end": "current_return_window_end",
            }
        ),
        on=["event_id", "window_name"],
        how="inner",
    )
    for column in [
        "actual_return_window_start",
        "actual_return_window_end",
        "current_return_window_start",
        "current_return_window_end",
    ]:
        merged[column] = pd.to_datetime(merged[column], errors="coerce")
    merged = merged.dropna(
        subset=[
            "actual_return_window_start",
            "actual_return_window_end",
            "current_return_window_start",
            "current_return_window_end",
        ]
    )
    changed = (
        merged["actual_return_window_start"].ne(merged["current_return_window_start"])
        | merged["actual_return_window_end"].ne(merged["current_return_window_end"])
    )
    overlap_start = pd.concat([merged["actual_return_window_start"], merged["current_return_window_start"]], axis=1).max(axis=1)
    overlap_end = pd.concat([merged["actual_return_window_end"], merged["current_return_window_end"]], axis=1).min(axis=1)
    overlap = overlap_start.le(overlap_end)
    shifted_bug = bool(not changed.any()) if not merged.empty else False
    payload = {
        "schema_version": "sue_event_timing_window_overlap_audit.v1",
        "event_available_after_tradable_violations": int((event_available > tradable).sum()),
        "estimate_after_event_available_violations": int((estimate_snapshot > event_available).sum()),
        "shifted_anchor_pair_count": int(len(merged)),
        "shifted_anchor_changed_window_count": int(changed.sum()),
        "shifted_anchor_actually_changes_return_window": bool(changed.any()),
        "live_vs_shifted_window_overlap_count": int(overlap.sum()),
        "live_vs_shifted_window_overlap_rate": float(overlap.mean()) if len(overlap) else 0.0,
        "shifted_placebo_window_bug_detected": shifted_bug,
        "benchmark_window_uses_shifted_anchor": True,
        "market_adjusted_spread_uses_shifted_anchor": True,
        "forward_return_feature_columns_detected": [],
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _market_timing_audit(
    *,
    scored_anchor: pd.DataFrame,
    score_name: str,
    config: SueEventTimingAnchorAuditConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (anchor_definition, window_name), frame in scored_anchor.groupby(["anchor_definition", "window_name"], sort=True):
        raw = _pooled_top_bottom(frame, score_name=score_name, return_column="window_return", config=config)
        market_adjusted = _pooled_top_bottom(
            frame,
            score_name=score_name,
            return_column="market_adjusted_window_return",
            config=config,
        )
        rows.append(
            {
                "schema_version": "sue_event_timing_market_audit.v1",
                "anchor_definition": anchor_definition,
                "window_name": window_name,
                "raw_top_bottom_spread": raw,
                "market_adjusted_top_bottom_spread": market_adjusted,
                "mean_market_window_return": _mean_or_none(pd.to_numeric(frame["market_window_return"], errors="coerce")),
                "sector_adjusted_top_bottom_spread": None,
                "sector_adjusted_status": "unavailable_missing_sector_return",
            }
        )
    return pd.DataFrame(rows)


def _anchor_selection_diagnostic(
    *,
    config: SueEventTimingAnchorAuditConfig,
    anchor_grid: pd.DataFrame,
    pre_event_drift_grid: pd.DataFrame,
    timing_quality_breakdown: pd.DataFrame,
    window_overlap_audit: dict[str, Any],
) -> dict[str, Any]:
    current = _grid_row(anchor_grid, "current_tradable", PRIMARY_WINDOW)
    best_anchor = _best_grid_row(anchor_grid.loc[anchor_grid["window_name"].eq(PRIMARY_WINDOW)])
    best_drift = _best_grid_row(pre_event_drift_grid)
    current_spread = _float_or_none(current.get("mean_top_bottom_spread"))
    best_anchor_spread = _float_or_none(best_anchor.get("mean_top_bottom_spread"))
    best_drift_spread = _float_or_none(best_drift.get("mean_top_bottom_spread"))
    if window_overlap_audit.get("shifted_placebo_window_bug_detected"):
        interpretation = "shifted_placebo_window_bug"
    elif _timing_quality_insufficient(timing_quality_breakdown):
        interpretation = "timing_quality_insufficient"
    elif best_anchor.get("anchor_definition") in {"shift_minus_5_td", "shift_minus_10_td"}:
        interpretation = "anchor_definition_likely_late"
    elif best_drift.get("window_name") in {"minus_10_minus_6", "minus_5_minus_1"} and (
        current_spread is None or (best_drift_spread is not None and best_drift_spread > current_spread)
    ):
        interpretation = "pre_event_drift_dominates"
    elif best_anchor_spread is not None and current_spread is not None and best_anchor_spread > current_spread:
        interpretation = "market_timing_contamination"
    elif best_anchor.get("anchor_definition") == "current_tradable":
        interpretation = "anchor_definition_cleared"
    else:
        interpretation = "anchor_audit_inconclusive"
    payload = {
        "schema_version": SUE_EVENT_TIMING_ANCHOR_AUDIT_SCHEMA_VERSION,
        "score_name": config.score_name,
        "selected_score": None,
        "interpretation": interpretation,
        "primary_window": PRIMARY_WINDOW,
        "current_tradable_mean_rank_ic": current.get("mean_rank_ic"),
        "current_tradable_mean_top_bottom_spread": current.get("mean_top_bottom_spread"),
        "best_anchor_definition": best_anchor.get("anchor_definition"),
        "best_anchor_mean_top_bottom_spread": best_anchor.get("mean_top_bottom_spread"),
        "best_pre_event_window": best_drift.get("window_name"),
        "best_pre_event_mean_top_bottom_spread": best_drift.get("mean_top_bottom_spread"),
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


def _timing_quality_insufficient(frame: pd.DataFrame) -> bool:
    if frame.empty:
        return True
    date_only = int(frame.loc[frame["timing_quality"].eq("date_only"), "event_count"].sum())
    total = int(frame["event_count"].sum())
    return bool(total > 0 and date_only / total > 0.75)


def _market_return_index(prices: pd.DataFrame) -> dict[str, Any]:
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["ret"] = pd.to_numeric(frame["ret"], errors="coerce")
    daily = frame.groupby("date", sort=True)["ret"].mean()
    dates: list[Any] = []
    cumulative: list[float] = []
    product = 1.0
    for date_value, value in daily.dropna().items():
        dates.append(date_value)
        product *= 1.0 + float(value)
        cumulative.append(product)
    return {"dates": dates, "cumulative": cumulative}


def _attach_market_window_return(frame: pd.DataFrame, market_index: dict[str, Any]) -> pd.DataFrame:
    market_dates = market_index.get("dates") or []
    cumulative = market_index.get("cumulative") or []
    output = frame.copy()
    output["market_window_return"] = pd.NA
    output["market_adjusted_window_return"] = pd.NA
    if not market_dates or not cumulative or output.empty:
        return output

    market_datetime = pd.to_datetime(pd.Series(market_dates), errors="coerce").to_numpy(dtype="datetime64[D]")
    cumulative_array = np.asarray(cumulative, dtype=float)
    start = pd.to_datetime(output["actual_return_window_start"], errors="coerce").to_numpy(dtype="datetime64[D]")
    end = pd.to_datetime(output["actual_return_window_end"], errors="coerce").to_numpy(dtype="datetime64[D]")
    valid_dates = ~pd.isna(start) & ~pd.isna(end)
    left = np.searchsorted(market_datetime, start, side="left")
    right = np.searchsorted(market_datetime, end, side="right") - 1
    valid = valid_dates & (left >= 0) & (right >= left) & (right < len(cumulative_array))
    market_return = np.full(len(output), np.nan, dtype=float)
    if valid.any():
        previous = np.ones(len(output), dtype=float)
        previous[valid & (left > 0)] = cumulative_array[left[valid & (left > 0)] - 1]
        market_return[valid] = cumulative_array[right[valid]] / previous[valid] - 1.0
    output["market_window_return"] = market_return
    window_return = pd.to_numeric(output["window_return"], errors="coerce").to_numpy(dtype=float)
    adjusted = window_return - market_return
    adjusted[~np.isfinite(window_return) | ~np.isfinite(market_return)] = np.nan
    output["market_adjusted_window_return"] = adjusted
    return output


def _pooled_top_bottom(
    frame: pd.DataFrame,
    *,
    score_name: str,
    return_column: str,
    config: SueEventTimingAnchorAuditConfig,
) -> float | None:
    observed = frame.dropna(subset=[score_name, return_column]).sort_values(score_name)
    if len(observed) < config.min_spread_names or observed[score_name].nunique() < 2:
        return None
    bucket_size = max(1, len(observed) // config.quantiles)
    bottom = observed.head(bucket_size)
    top = observed.tail(bucket_size)
    return float(top[return_column].mean() - bottom[return_column].mean())


def _grid_row(frame: pd.DataFrame, anchor_definition: str, window_name: str) -> dict[str, Any]:
    rows = frame.loc[frame["anchor_definition"].eq(anchor_definition) & frame["window_name"].eq(window_name)]
    if rows.empty:
        return {}
    return rows.iloc[0].where(pd.notna(rows.iloc[0]), None).to_dict()


def _best_grid_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    scored = frame.copy()
    scored["spread_score"] = pd.to_numeric(scored["mean_top_bottom_spread"], errors="coerce").fillna(-999.0)
    scored["rank_score"] = pd.to_numeric(scored["mean_rank_ic"], errors="coerce").fillna(-999.0)
    return scored.sort_values(["spread_score", "rank_score"], ascending=False).iloc[0].drop(
        labels=["spread_score", "rank_score"]
    ).where(lambda series: pd.notna(series), None).to_dict()


def _helper_evidence_config(config: SueEventTimingAnchorAuditConfig) -> SueHistoricalEventEvidenceConfig:
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


def _mean_or_none(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.mean())


def _t_stat(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if len(clean) < 2:
        return None
    std = float(clean.std(ddof=1))
    if std == 0.0:
        return None
    return float(clean.mean() / (std / (len(clean) ** 0.5)))


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
