"""Event-date-shift placebo failure attribution for scale-aware SUE.

This module diagnoses why the H1E event-date-shift placebo can dominate the
live event-window read. It does not select a score, run Q2, invoke optimizers,
open paper/live/broker/order workflows, or approve production use.
"""

from __future__ import annotations

from dataclasses import dataclass
from bisect import bisect_left
import json
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.alpha.sue_historical_event_evidence import (
    EVENT_WINDOWS,
    SueHistoricalEventEvidenceConfig,
    _event_window_grid,
    _load_and_validate_events,
    _load_prices,
    _price_index,
    _rank_ic_by_date,
    _safe_events,
    _top_bottom_spread_by_date,
)
from portfolio_os.alpha.sue_historical_schema import validate_no_forward_return_feature_columns
from portfolio_os.alpha.sue_score_definition_gate import (
    SueScoreDefinitionGateConfig,
    _attach_gate_scores,
)
from portfolio_os.provenance.hashing import canonical_json, hash_payload


SUE_PLACEBO_FAILURE_ATTRIBUTION_SCHEMA_VERSION = "sue_placebo_failure_attribution.v1"

DEFAULT_EVENTS_PATH = "outputs/sue_historical_event_panel_expanded/events.csv"
DEFAULT_CRSP_DAILY_PATH = "data/cache/wrds_sue_event_panel/crsp_daily.csv"
DEFAULT_OUTPUT_DIR = "outputs/sue_placebo_failure_attribution"
DEFAULT_REPORT_PATH = "reports/sue_placebo_failure_attribution_report.md"
PRIMARY_WINDOW = "plus_2_plus_22"

MISLEADING_ATTRIBUTION_CLAIMS = (
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


class SuePlaceboFailureAttributionConfig(BaseModel):
    """Config for Reopen-H1E.1 placebo-failure attribution."""

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
    high_tail_quantile: float = Field(default=0.99, gt=0.5, lt=1.0)
    random_seed: int = 20260508


@dataclass(frozen=True)
class SuePlaceboFailureAttributionResult:
    """In-memory H1E.1 attribution result."""

    config: SuePlaceboFailureAttributionConfig
    placebo_shift_curve: pd.DataFrame
    live_vs_placebo_by_month: pd.DataFrame
    live_vs_placebo_by_sector: pd.DataFrame
    live_vs_placebo_by_size_liquidity: pd.DataFrame
    return_window_overlap_audit: dict[str, Any]
    market_adjustment_report: pd.DataFrame
    denominator_tail_audit: dict[str, Any]
    regime_concentration_report: dict[str, Any]
    attribution_summary: dict[str, Any]
    report_text: str


def load_sue_placebo_failure_attribution_config(
    path: str | Path,
) -> SuePlaceboFailureAttributionConfig:
    """Load H1E.1 config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    guards = payload.get("guards") or {}
    return SuePlaceboFailureAttributionConfig(
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
        high_tail_quantile=float(guards.get("high_tail_quantile", payload.get("high_tail_quantile", 0.99))),
        random_seed=int(payload.get("random_seed", 20260508)),
    )


def build_sue_placebo_failure_attribution(
    config: SuePlaceboFailureAttributionConfig | None = None,
) -> SuePlaceboFailureAttributionResult:
    """Build Reopen-H1E.1 placebo-failure attribution artifacts."""

    resolved = config or SuePlaceboFailureAttributionConfig()
    events = _load_and_validate_events(resolved.events_path)
    validate_no_forward_return_feature_columns(list(events.columns))
    prices = _load_prices(resolved.crsp_daily_path)
    price_index = _price_index(prices)
    safe_events = _safe_events(events)
    evidence_config = _helper_evidence_config(resolved)
    gate_config = _helper_gate_config(resolved)

    shifted_returns = _build_shifted_window_return_frame(
        safe_events=safe_events,
        price_index=price_index,
        shifts=resolved.shift_trading_days,
    )
    scored = _attach_gate_scores(
        return_frame=shifted_returns,
        safe_events=safe_events,
        crsp_daily_path=resolved.crsp_daily_path,
        config=gate_config,
    )
    if resolved.score_name not in scored.columns:
        raise ValueError(f"score_name is not available in scored SUE frame: {resolved.score_name}")

    placebo_shift_curve = _placebo_shift_curve(scored_frame=scored, score_name=resolved.score_name, config=evidence_config)
    live_vs_month = _bucket_report(
        scored_frame=scored,
        score_name=resolved.score_name,
        bucket_column="calendar_month",
        output_column="calendar_month",
        config=resolved,
    )
    live_vs_sector = _sector_report(scored_frame=scored, score_name=resolved.score_name, config=resolved)
    live_vs_size_liquidity = _size_liquidity_report(scored_frame=scored, score_name=resolved.score_name, config=resolved)
    overlap_audit = _return_window_overlap_audit(events=events, scored_frame=scored)
    market_adjustment = _market_adjustment_report(
        scored_frame=scored,
        score_name=resolved.score_name,
        config=evidence_config,
    )
    denominator_tail = _denominator_tail_audit(scored_frame=scored, score_name=resolved.score_name, config=resolved)
    regime_report = _regime_concentration_report(
        scored_frame=scored,
        score_name=resolved.score_name,
        config=resolved,
        placebo_shift_curve=placebo_shift_curve,
    )
    attribution_summary = _attribution_summary(
        config=resolved,
        placebo_shift_curve=placebo_shift_curve,
        overlap_audit=overlap_audit,
        denominator_tail_audit=denominator_tail,
        regime_concentration_report=regime_report,
    )
    report_text = render_sue_placebo_failure_attribution_report(
        placebo_shift_curve=placebo_shift_curve,
        return_window_overlap_audit=overlap_audit,
        denominator_tail_audit=denominator_tail,
        regime_concentration_report=regime_report,
        attribution_summary=attribution_summary,
    )
    validate_sue_placebo_failure_attribution_report_language(report_text)
    return SuePlaceboFailureAttributionResult(
        config=resolved,
        placebo_shift_curve=placebo_shift_curve,
        live_vs_placebo_by_month=live_vs_month,
        live_vs_placebo_by_sector=live_vs_sector,
        live_vs_placebo_by_size_liquidity=live_vs_size_liquidity,
        return_window_overlap_audit=overlap_audit,
        market_adjustment_report=market_adjustment,
        denominator_tail_audit=denominator_tail,
        regime_concentration_report=regime_report,
        attribution_summary=attribution_summary,
        report_text=report_text,
    )


def write_sue_placebo_failure_attribution_artifacts(
    result: SuePlaceboFailureAttributionResult,
) -> dict[str, Path]:
    """Write Reopen-H1E.1 attribution artifacts."""

    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(result.config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    paths = {
        "placebo_shift_curve": output_dir / "placebo_shift_curve.csv",
        "live_vs_placebo_by_month": output_dir / "live_vs_placebo_by_month.csv",
        "live_vs_placebo_by_sector": output_dir / "live_vs_placebo_by_sector.csv",
        "live_vs_placebo_by_size_liquidity": output_dir / "live_vs_placebo_by_size_liquidity.csv",
        "return_window_overlap_audit": output_dir / "return_window_overlap_audit.json",
        "market_adjustment_report": output_dir / "market_adjustment_report.csv",
        "denominator_tail_audit": output_dir / "denominator_tail_audit.json",
        "regime_concentration_report": output_dir / "regime_concentration_report.json",
        "attribution_summary": output_dir / "attribution_summary.json",
        "report": report_path,
    }
    result.placebo_shift_curve.to_csv(paths["placebo_shift_curve"], index=False)
    result.live_vs_placebo_by_month.to_csv(paths["live_vs_placebo_by_month"], index=False)
    result.live_vs_placebo_by_sector.to_csv(paths["live_vs_placebo_by_sector"], index=False)
    result.live_vs_placebo_by_size_liquidity.to_csv(paths["live_vs_placebo_by_size_liquidity"], index=False)
    result.market_adjustment_report.to_csv(paths["market_adjustment_report"], index=False)
    _write_json(paths["return_window_overlap_audit"], result.return_window_overlap_audit)
    _write_json(paths["denominator_tail_audit"], result.denominator_tail_audit)
    _write_json(paths["regime_concentration_report"], result.regime_concentration_report)
    _write_json(paths["attribution_summary"], result.attribution_summary)
    validate_sue_placebo_failure_attribution_report_language(result.report_text)
    paths["report"].write_text(result.report_text, encoding="utf-8")
    return paths


def render_sue_placebo_failure_attribution_report(
    *,
    placebo_shift_curve: pd.DataFrame,
    return_window_overlap_audit: dict[str, Any],
    denominator_tail_audit: dict[str, Any],
    regime_concentration_report: dict[str, Any],
    attribution_summary: dict[str, Any],
) -> str:
    """Render H1E.1 attribution report."""

    primary_rows = placebo_shift_curve.loc[placebo_shift_curve["window_name"].eq(PRIMARY_WINDOW)].copy()
    primary_rows = primary_rows.sort_values("shift_trading_days")
    lines = [
        "# SUE Event-Date-Shift Placebo Failure Attribution",
        "",
        "H1E did not select a production SUE score.",
        "This phase diagnoses placebo failure only.",
        "No Q2, optimizer-path, paper, live, broker, order, or production workflow is opened.",
        "If placebo failure is unresolved, SUE remains mixed and should not enter typed projection.",
        "",
        "## Summary",
        "",
        f"- schema_version: `{attribution_summary['schema_version']}`",
        f"- score_name: `{attribution_summary['score_name']}`",
        f"- interpretation: `{attribution_summary['interpretation']}`",
        f"- live_primary_mean_rank_ic: `{_fmt(attribution_summary.get('live_primary_mean_rank_ic'))}`",
        f"- live_primary_mean_top_bottom_spread: `{_fmt(attribution_summary.get('live_primary_mean_top_bottom_spread'))}`",
        f"- best_placebo_shift: `{attribution_summary.get('best_placebo_shift_trading_days')}`",
        f"- q2_evaluation_ran: `{attribution_summary['q2_evaluation_ran']}`",
        f"- optimizer_path_evaluation_ran: `{attribution_summary['optimizer_path_evaluation_ran']}`",
        f"- production_approval_claimed: `{attribution_summary['production_approval_claimed']}`",
        "",
        "## Primary Window Timing Curve",
        "",
        "| Shift | Mean Rank IC | Mean Top-Bottom Spread | Date Count |",
        "| ---: | ---: | ---: | ---: |",
    ]
    for row in primary_rows.to_dict(orient="records"):
        lines.append(
            f"| {int(row['shift_trading_days'])} | {_fmt(row.get('mean_rank_ic'))} | "
            f"{_fmt(row.get('mean_top_bottom_spread'))} | {int(row.get('top_bottom_date_count') or 0)} |"
        )
    lines.extend(
        [
            "",
            "## Window Audit",
            "",
            f"- shifted_anchors_used: `{return_window_overlap_audit['shifted_anchors_used']}`",
            "- original_anchor_reused_for_shifted_windows: "
            f"`{return_window_overlap_audit['original_anchor_reused_for_shifted_windows']}`",
            "- event_available_after_tradable_violations: "
            f"`{return_window_overlap_audit['event_available_after_tradable_violations']}`",
            "- live_return_window_start_before_tradable_count: "
            f"`{return_window_overlap_audit['live_return_window_start_before_tradable_count']}`",
            f"- live_shift_window_overlap_rate: `{_fmt(return_window_overlap_audit.get('live_shift_window_overlap_rate'))}`",
            "",
            "## Denominator / Tail Audit",
            "",
            f"- low_denominator_count: `{denominator_tail_audit['low_denominator_count']}`",
            f"- high_tail_event_count: `{denominator_tail_audit['high_tail_event_count']}`",
            "- missing_coverage_encoded_as_zero_alpha: "
            f"`{denominator_tail_audit['missing_coverage_encoded_as_zero_alpha']}`",
            f"- no_view_not_zero_alpha: `{denominator_tail_audit['no_view_not_zero_alpha']}`",
            "",
            "## Regime Audit",
            "",
            f"- sample_start: `{regime_concentration_report.get('sample_start')}`",
            f"- sample_end: `{regime_concentration_report.get('sample_end')}`",
            f"- includes_march_2020: `{regime_concentration_report.get('includes_march_2020')}`",
            f"- short_crash_rebound_window_possible: "
            f"`{regime_concentration_report.get('short_crash_rebound_window_possible')}`",
            "",
            "## Boundaries",
            "",
            "- This diagnostic does not select a production SUE score.",
            "- It does not run Q2 or optimizer-path evaluation.",
            "- It does not promote Alpha Registry state.",
            "- Downstream typed projection and Q2 require a separate explicit reopen.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_sue_placebo_failure_attribution_report_language(text: str) -> None:
    """Reject misleading H1E.1 claims while allowing required non-claims."""

    scrubbed = str(text).lower()
    allowed_phrases = [
        "h1e did not select a production sue score.",
        "this phase diagnoses placebo failure only.",
        "no q2, optimizer-path, paper, live, broker, order, or production workflow is opened.",
        "if placebo failure is unresolved, sue remains mixed and should not enter typed projection.",
        "this diagnostic does not select a production sue score.",
        "it does not run q2 or optimizer-path evaluation.",
        "it does not promote alpha registry state.",
        "downstream typed projection and q2 require a separate explicit reopen.",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in MISLEADING_ATTRIBUTION_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE placebo-failure attribution claim detected: {claim}")


def _build_shifted_window_return_frame(
    *,
    safe_events: pd.DataFrame,
    price_index: dict[int, pd.DataFrame],
    shifts: list[int],
) -> pd.DataFrame:
    price_arrays = {
        int(permno): (list(group["date"]), pd.to_numeric(group["ret"], errors="coerce").to_list())
        for permno, group in price_index.items()
    }
    rows: list[dict[str, Any]] = []
    for event in safe_events.to_dict(orient="records"):
        permno = int(event["permno"])
        tradable_date = event["tradable_date"]
        dates, returns = price_arrays.get(permno, ([], []))
        base_index = bisect_left(dates, tradable_date) if dates else None
        for shift in shifts:
            anchor_index = None if base_index is None else base_index + int(shift)
            anchor_date = dates[anchor_index] if anchor_index is not None and 0 <= anchor_index < len(dates) else None
            for window in EVENT_WINDOWS:
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
                        "announcement_date": event["announcement_date"].isoformat(),
                        "tradable_date": tradable_date.isoformat(),
                        "event_available_timestamp": event["event_available_timestamp"],
                        "tradable_timestamp": event["tradable_timestamp"],
                        "estimate_snapshot_date": event["estimate_snapshot_date"],
                        "sue_value": float(event["sue_value"]),
                        "shift_trading_days": int(shift),
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


def _fast_window_return(
    *,
    dates: list[Any],
    returns: list[Any],
    anchor_index: int | None,
    start_offset: int,
    end_offset: int,
) -> tuple[float | None, Any | None, Any | None]:
    if anchor_index is None or anchor_index < 0 or anchor_index >= len(dates):
        return None, None, None
    start_index = anchor_index + start_offset
    end_index = anchor_index + end_offset
    if start_index < 0 or end_index >= len(dates):
        return None, None, None
    product = 1.0
    for value in returns[start_index : end_index + 1]:
        if pd.isna(value):
            return None, None, None
        product *= 1.0 + float(value)
    realized = float(product - 1.0)
    return realized, dates[start_index], dates[end_index]


def _placebo_shift_curve(
    *,
    scored_frame: pd.DataFrame,
    score_name: str,
    config: SueHistoricalEventEvidenceConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for shift, frame in scored_frame.groupby("shift_trading_days", sort=True):
        rank_rows = _rank_ic_by_date(frame, score_column=score_name, config=config)
        spread_rows = _top_bottom_spread_by_date(frame, score_column=score_name, config=config)
        grid = _event_window_grid(
            live_return_frame=frame,
            rank_ic_by_date=rank_rows,
            top_bottom_spread_by_date=spread_rows,
        )
        for row in grid.to_dict(orient="records"):
            row["schema_version"] = "sue_placebo_shift_curve.v1"
            row["score_name"] = score_name
            row["shift_trading_days"] = int(shift)
            row["is_live_anchor"] = bool(int(shift) == 0)
            rows.append(row)
    return pd.DataFrame(rows).sort_values(["window_name", "shift_trading_days"]).reset_index(drop=True)


def _bucket_report(
    *,
    scored_frame: pd.DataFrame,
    score_name: str,
    bucket_column: str,
    output_column: str,
    config: SuePlaceboFailureAttributionConfig,
) -> pd.DataFrame:
    frame = scored_frame.copy()
    if bucket_column == "calendar_month":
        frame[bucket_column] = pd.to_datetime(frame["rebalance_date"], errors="coerce").dt.to_period("M").astype(str)
    elif bucket_column == "calendar_year":
        frame[bucket_column] = pd.to_datetime(frame["rebalance_date"], errors="coerce").dt.year.astype(str)
    elif bucket_column == "event_week":
        frame[bucket_column] = pd.to_datetime(frame["rebalance_date"], errors="coerce").dt.strftime("%G-W%V")
    rows: list[dict[str, Any]] = []
    for (bucket, shift, window_name), group in frame.groupby([bucket_column, "shift_trading_days", "window_name"], sort=True):
        summary = _pooled_spread_and_rank(group, score_name=score_name, config=config)
        rows.append(
            {
                "schema_version": "sue_placebo_bucket_attribution.v1",
                output_column: bucket,
                "shift_trading_days": int(shift),
                "window_name": window_name,
                **summary,
            }
        )
    return pd.DataFrame(rows)


def _sector_report(
    *,
    scored_frame: pd.DataFrame,
    score_name: str,
    config: SuePlaceboFailureAttributionConfig,
) -> pd.DataFrame:
    sector_columns = [column for column in ["sector", "industry", "siccd", "gics_sector"] if column in scored_frame.columns]
    if not sector_columns:
        return pd.DataFrame(
            [
                {
                    "schema_version": "sue_placebo_sector_attribution.v1",
                    "sector": "unavailable",
                    "industry": "unavailable",
                    "shift_trading_days": None,
                    "window_name": None,
                    "row_count": 0,
                    "status": "unavailable_missing_sector_industry_fields",
                }
            ]
        )
    primary = sector_columns[0]
    rows: list[dict[str, Any]] = []
    for (sector, shift, window_name), group in scored_frame.groupby([primary, "shift_trading_days", "window_name"], sort=True):
        rows.append(
            {
                "schema_version": "sue_placebo_sector_attribution.v1",
                "sector": str(sector),
                "industry": str(sector),
                "shift_trading_days": int(shift),
                "window_name": window_name,
                **_pooled_spread_and_rank(group, score_name=score_name, config=config),
            }
        )
    return pd.DataFrame(rows)


def _size_liquidity_report(
    *,
    scored_frame: pd.DataFrame,
    score_name: str,
    config: SuePlaceboFailureAttributionConfig,
) -> pd.DataFrame:
    frame = scored_frame.copy()
    frame["denominator_bucket"] = _quantile_bucket(pd.to_numeric(frame["actual_eps"], errors="coerce").abs(), 4)
    frame["absolute_eps_bucket"] = _quantile_bucket(pd.to_numeric(frame["actual_eps"], errors="coerce").abs(), 4)
    frame["negative_actual_eps_bucket"] = pd.to_numeric(frame["actual_eps"], errors="coerce").lt(0).map(
        {True: "negative_actual_eps", False: "non_negative_actual_eps"}
    )
    low_denominator = pd.to_numeric(frame["actual_eps"], errors="coerce").abs().lt(config.denominator_abs_min)
    frame["low_denominator_bucket"] = low_denominator.map({True: "low_denominator", False: "denominator_ok"})
    score_abs = pd.to_numeric(frame[score_name], errors="coerce").abs()
    high_tail_threshold = score_abs.quantile(config.high_tail_quantile) if score_abs.notna().any() else None
    high_tail = score_abs.ge(high_tail_threshold) if high_tail_threshold is not None else pd.Series(False, index=frame.index)
    frame["high_tail_concentration_bucket"] = high_tail.map({True: "high_tail", False: "non_high_tail"})
    bucket_specs = [
        ("size", "unavailable_missing_size_field", None),
        ("liquidity", "unavailable_missing_liquidity_field", None),
        ("denominator", "observed", "denominator_bucket"),
        ("absolute_eps", "observed", "absolute_eps_bucket"),
        ("negative_actual_eps", "observed", "negative_actual_eps_bucket"),
        ("low_denominator", "observed", "low_denominator_bucket"),
        ("high_tail_concentration", "observed", "high_tail_concentration_bucket"),
    ]
    rows: list[dict[str, Any]] = []
    for bucket_type, status, column in bucket_specs:
        if column is None:
            rows.append(
                {
                    "schema_version": "sue_placebo_size_liquidity_attribution.v1",
                    "bucket_type": bucket_type,
                    "bucket": "unavailable",
                    "shift_trading_days": None,
                    "window_name": None,
                    "row_count": 0,
                    "status": status,
                }
            )
            continue
        for (bucket, shift, window_name), group in frame.groupby([column, "shift_trading_days", "window_name"], sort=True):
            rows.append(
                {
                    "schema_version": "sue_placebo_size_liquidity_attribution.v1",
                    "bucket_type": bucket_type,
                    "bucket": str(bucket),
                    "shift_trading_days": int(shift),
                    "window_name": window_name,
                    "status": status,
                    **_pooled_spread_and_rank(group, score_name=score_name, config=config),
                }
            )
    return pd.DataFrame(rows)


def _return_window_overlap_audit(*, events: pd.DataFrame, scored_frame: pd.DataFrame) -> dict[str, Any]:
    frame = scored_frame.copy()
    event_available = pd.to_datetime(events["event_available_timestamp"], errors="coerce", utc=True)
    tradable = pd.to_datetime(events["tradable_timestamp"], errors="coerce", utc=True)
    estimate_snapshot = pd.to_datetime(events["estimate_snapshot_date"], errors="coerce", utc=True)
    starts = pd.to_datetime(frame["actual_return_window_start"], errors="coerce")
    tradable_dates = pd.to_datetime(frame["tradable_date"], errors="coerce")
    anchor_dates = pd.to_datetime(frame["anchor_date"], errors="coerce")
    live = frame.loc[frame["shift_trading_days"].eq(0)].copy()
    live_starts = pd.to_datetime(live["actual_return_window_start"], errors="coerce")
    live_tradable = pd.to_datetime(live["tradable_date"], errors="coerce")
    nonzero = frame.loc[~frame["shift_trading_days"].eq(0)].copy()
    if nonzero.empty:
        shifted_anchors_used = False
        reused = False
    else:
        shifted_anchor_dates = pd.to_datetime(nonzero["anchor_date"], errors="coerce")
        shifted_tradable_dates = pd.to_datetime(nonzero["tradable_date"], errors="coerce")
        shifted_anchors_used = bool((shifted_anchor_dates != shifted_tradable_dates).any())
        reused = bool((shifted_anchor_dates == shifted_tradable_dates).any())
    shifted_mask = ~frame["shift_trading_days"].eq(0)
    overlap_stats = _live_shift_overlap_stats(frame)
    payload = {
        "schema_version": "sue_placebo_return_window_overlap_audit.v1",
        "event_available_after_tradable_violations": int((event_available > tradable).sum()),
        "estimate_after_event_available_violations": int((estimate_snapshot > event_available).sum()),
        "live_return_window_start_before_tradable_count": int((live_starts < live_tradable).sum()),
        "return_window_start_before_tradable_count": int((live_starts < live_tradable).sum()),
        "shifted_return_window_start_before_original_tradable_count": int(
            (starts.loc[shifted_mask] < tradable_dates.loc[shifted_mask]).sum()
        ),
        "return_window_start_not_after_shifted_anchor_count": int((starts <= anchor_dates).sum()),
        "shifted_anchors_used": shifted_anchors_used,
        "original_anchor_reused_for_shifted_windows": reused,
        "shifted_windows_use_shifted_anchors": bool(shifted_anchors_used and not reused),
        "benchmark_market_adjustment_uses_shifted_window": True,
        "original_anchor_reused_for_benchmark_adjustment": False,
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
        **overlap_stats,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _live_shift_overlap_stats(frame: pd.DataFrame) -> dict[str, Any]:
    live = frame.loc[
        frame["shift_trading_days"].eq(0),
        ["event_id", "window_name", "actual_return_window_start", "actual_return_window_end"],
    ].copy()
    shifted = frame.loc[
        ~frame["shift_trading_days"].eq(0),
        ["event_id", "window_name", "actual_return_window_start", "actual_return_window_end"],
    ].copy()
    if live.empty or shifted.empty:
        return {"live_shift_window_pair_count": 0, "live_shift_window_overlap_count": 0, "live_shift_window_overlap_rate": 0.0}
    live = live.rename(
        columns={
            "actual_return_window_start": "live_start",
            "actual_return_window_end": "live_end",
        }
    )
    shifted = shifted.rename(
        columns={
            "actual_return_window_start": "shift_start",
            "actual_return_window_end": "shift_end",
        }
    )
    merged = shifted.merge(live, on=["event_id", "window_name"], how="inner")
    for column in ["live_start", "live_end", "shift_start", "shift_end"]:
        merged[column] = pd.to_datetime(merged[column], errors="coerce")
    merged = merged.dropna(subset=["live_start", "live_end", "shift_start", "shift_end"])
    pair_count = int(len(merged))
    if pair_count == 0:
        return {"live_shift_window_pair_count": 0, "live_shift_window_overlap_count": 0, "live_shift_window_overlap_rate": 0.0}
    overlap_start = pd.concat([merged["live_start"], merged["shift_start"]], axis=1).max(axis=1)
    overlap_end = pd.concat([merged["live_end"], merged["shift_end"]], axis=1).min(axis=1)
    overlap_mask = overlap_start.le(overlap_end)
    overlap_count = int(overlap_mask.sum())
    overlap_days = (overlap_end.loc[overlap_mask] - overlap_start.loc[overlap_mask]).dt.days + 1
    max_overlap_days = int(overlap_days.max()) if not overlap_days.empty else 0
    return {
        "live_shift_window_pair_count": int(pair_count),
        "live_shift_window_overlap_count": int(overlap_count),
        "live_shift_window_overlap_rate": float(overlap_count / pair_count) if pair_count else 0.0,
        "max_live_shift_overlap_calendar_days": int(max_overlap_days),
    }


def _market_adjustment_report(
    *,
    scored_frame: pd.DataFrame,
    score_name: str,
    config: SueHistoricalEventEvidenceConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for shift, frame in scored_frame.groupby("shift_trading_days", sort=True):
        raw_rank = _rank_ic_by_date(frame, score_column=score_name, config=config)
        raw_spread = _top_bottom_spread_by_date(frame, score_column=score_name, config=config)
        raw_grid = _event_window_grid(
            live_return_frame=frame,
            rank_ic_by_date=raw_rank,
            top_bottom_spread_by_date=raw_spread,
        )
        demeaned = frame.copy()
        demeaned["within_date_demeaned_return"] = demeaned["window_return"] - demeaned.groupby(
            ["window_name", "rebalance_date"]
        )["window_return"].transform("mean")
        demeaned = demeaned.rename(columns={"window_return": "raw_window_return"})
        demeaned["window_return"] = demeaned["within_date_demeaned_return"]
        demeaned_rank = _rank_ic_by_date(demeaned, score_column=score_name, config=config)
        for row in raw_grid.to_dict(orient="records"):
            window_rank = demeaned_rank.loc[demeaned_rank["window_name"].eq(row["window_name"])]
            rows.append(
                {
                    "schema_version": "sue_placebo_market_adjustment.v1",
                    "shift_trading_days": int(shift),
                    "window_name": row["window_name"],
                    "raw_mean_top_bottom_spread": row.get("mean_top_bottom_spread"),
                    "market_adjusted_spread": None,
                    "market_adjusted_status": "unavailable_missing_market_return",
                    "sector_neutral_spread": None,
                    "sector_neutral_status": "unavailable_missing_sector_industry_fields",
                    "within_date_demeaned_rank_ic": _mean_or_none(window_rank["rank_ic"]),
                    "within_sector_rank_ic": None,
                    "within_sector_status": "unavailable_missing_sector_industry_fields",
                }
            )
    return pd.DataFrame(rows)


def _denominator_tail_audit(
    *,
    scored_frame: pd.DataFrame,
    score_name: str,
    config: SuePlaceboFailureAttributionConfig,
) -> dict[str, Any]:
    unique = scored_frame.drop_duplicates("event_id").copy()
    actual = pd.to_numeric(unique["actual_eps"], errors="coerce")
    expected = pd.to_numeric(unique["expected_eps"], errors="coerce")
    denominator = actual.abs()
    score_abs = pd.to_numeric(unique[score_name], errors="coerce").abs()
    high_tail_threshold = float(score_abs.quantile(config.high_tail_quantile)) if score_abs.notna().any() else None
    high_tail_count = int(score_abs.ge(high_tail_threshold).sum()) if high_tail_threshold is not None else 0
    payload = {
        "schema_version": "sue_placebo_denominator_tail_audit.v1",
        "score_name": score_name,
        "event_count": int(len(unique)),
        "actual_eps_missing_count": int(actual.isna().sum()),
        "expected_eps_missing_count": int(expected.isna().sum()),
        "low_denominator_count": int(denominator.lt(config.denominator_abs_min).sum()),
        "negative_actual_eps_count": int(actual.lt(0).sum()),
        "negative_expected_eps_count": int(expected.lt(0).sum()),
        "high_tail_quantile": config.high_tail_quantile,
        "high_tail_threshold": high_tail_threshold,
        "high_tail_event_count": high_tail_count,
        "high_tail_event_share": float(high_tail_count / len(unique)) if len(unique) else 0.0,
        "missing_denominator_is_no_view": True,
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _regime_concentration_report(
    *,
    scored_frame: pd.DataFrame,
    score_name: str,
    config: SuePlaceboFailureAttributionConfig,
    placebo_shift_curve: pd.DataFrame,
) -> dict[str, Any]:
    frame = scored_frame.copy()
    dates = pd.to_datetime(frame["rebalance_date"], errors="coerce")
    sample_start = dates.min()
    sample_end = dates.max()
    includes_march = bool(dates.dt.strftime("%Y-%m").eq("2020-03").any())
    primary = frame.loc[frame["window_name"].eq(PRIMARY_WINDOW)].copy()
    primary["period"] = pd.to_datetime(primary["rebalance_date"], errors="coerce").lt(pd.Timestamp("2020-03-01")).map(
        {True: "pre_march_2020", False: "post_march_2020"}
    )
    split_rows = []
    for (period, shift), group in primary.groupby(["period", "shift_trading_days"], sort=True):
        split_rows.append({"period": period, "shift_trading_days": int(shift), **_pooled_spread_and_rank(group, score_name=score_name, config=config)})
    live = _primary_curve_row(scored_frame=placebo_shift_curve, shift=0)
    best_placebo = _best_placebo_row(placebo_shift_curve)
    live_spread = _float_or_none(live.get("mean_top_bottom_spread"))
    placebo_spread = _float_or_none(best_placebo.get("mean_top_bottom_spread"))
    payload = {
        "schema_version": "sue_placebo_regime_concentration.v1",
        "sample_start": sample_start.date().isoformat() if pd.notna(sample_start) else None,
        "sample_end": sample_end.date().isoformat() if pd.notna(sample_end) else None,
        "includes_march_2020": includes_march,
        "pre_post_march_2020_split": split_rows,
        "highest_market_volatility_weeks_excluded": False,
        "lowest_liquidity_weeks_excluded": False,
        "short_crash_rebound_window_possible": bool(
            includes_march
            and placebo_spread is not None
            and live_spread is not None
            and placebo_spread > live_spread
        ),
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _attribution_summary(
    *,
    config: SuePlaceboFailureAttributionConfig,
    placebo_shift_curve: pd.DataFrame,
    overlap_audit: dict[str, Any],
    denominator_tail_audit: dict[str, Any],
    regime_concentration_report: dict[str, Any],
) -> dict[str, Any]:
    live = _primary_curve_row(scored_frame=placebo_shift_curve, shift=0)
    best_placebo = _best_placebo_row(placebo_shift_curve)
    live_spread = _float_or_none(live.get("mean_top_bottom_spread"))
    placebo_spread = _float_or_none(best_placebo.get("mean_top_bottom_spread"))
    if overlap_audit["original_anchor_reused_for_shifted_windows"] or overlap_audit["live_return_window_start_before_tradable_count"]:
        interpretation = "placebo_failure_due_to_window_bug"
    elif regime_concentration_report.get("short_crash_rebound_window_possible"):
        interpretation = "placebo_failure_due_to_market_regime"
    elif denominator_tail_audit.get("high_tail_event_share", 0.0) > 0.05:
        interpretation = "placebo_failure_due_to_denominator_tail"
    elif placebo_spread is not None and live_spread is not None and placebo_spread > live_spread:
        interpretation = "placebo_failure_due_to_event_timing_uncertainty"
    else:
        interpretation = "placebo_failure_unexplained"
    payload = {
        "schema_version": SUE_PLACEBO_FAILURE_ATTRIBUTION_SCHEMA_VERSION,
        "score_name": config.score_name,
        "interpretation": interpretation,
        "primary_window": PRIMARY_WINDOW,
        "live_primary_mean_rank_ic": live.get("mean_rank_ic"),
        "live_primary_mean_top_bottom_spread": live.get("mean_top_bottom_spread"),
        "best_placebo_shift_trading_days": best_placebo.get("shift_trading_days"),
        "best_placebo_mean_rank_ic": best_placebo.get("mean_rank_ic"),
        "best_placebo_mean_top_bottom_spread": best_placebo.get("mean_top_bottom_spread"),
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


def _primary_curve_row(*, scored_frame: pd.DataFrame, shift: int) -> dict[str, Any]:
    rows = scored_frame.loc[
        scored_frame["window_name"].eq(PRIMARY_WINDOW) & scored_frame["shift_trading_days"].eq(int(shift))
    ]
    if rows.empty:
        return {"window_name": PRIMARY_WINDOW, "shift_trading_days": shift}
    return rows.iloc[0].where(pd.notna(rows.iloc[0]), None).to_dict()


def _best_placebo_row(placebo_shift_curve: pd.DataFrame) -> dict[str, Any]:
    rows = placebo_shift_curve.loc[
        placebo_shift_curve["window_name"].eq(PRIMARY_WINDOW) & ~placebo_shift_curve["shift_trading_days"].eq(0)
    ].copy()
    if rows.empty:
        return {}
    rows["spread_score"] = pd.to_numeric(rows["mean_top_bottom_spread"], errors="coerce").fillna(-999.0)
    rows["rank_score"] = pd.to_numeric(rows["mean_rank_ic"], errors="coerce").fillna(-999.0)
    return rows.sort_values(["spread_score", "rank_score"], ascending=False).iloc[0].drop(
        labels=["spread_score", "rank_score"]
    ).where(lambda series: pd.notna(series), None).to_dict()


def _pooled_spread_and_rank(
    frame: pd.DataFrame,
    *,
    score_name: str,
    config: SuePlaceboFailureAttributionConfig,
) -> dict[str, Any]:
    observed = frame.dropna(subset=[score_name, "window_return"]).copy()
    rank_ic = None
    top_bottom = None
    top_count = 0
    bottom_count = 0
    status = "observed"
    if len(observed) < config.min_rank_ic_names or observed[score_name].nunique() < 2 or observed["window_return"].nunique() < 2:
        status = "unavailable_insufficient_cross_section"
    else:
        rank_ic = float(observed[score_name].corr(observed["window_return"], method="spearman"))
    if len(observed) >= config.min_spread_names and observed[score_name].nunique() >= 2:
        sorted_frame = observed.sort_values(score_name)
        bucket_size = max(1, len(sorted_frame) // config.quantiles)
        bottom = sorted_frame.head(bucket_size)
        top = sorted_frame.tail(bucket_size)
        bottom_count = int(len(bottom))
        top_count = int(len(top))
        top_bottom = float(top["window_return"].mean() - bottom["window_return"].mean())
    return {
        "row_count": int(len(observed)),
        "rank_ic": rank_ic,
        "top_bottom_spread": top_bottom,
        "top_count": top_count,
        "bottom_count": bottom_count,
        "status": status,
    }


def _quantile_bucket(values: pd.Series, bins: int) -> pd.Series:
    observed = pd.to_numeric(values, errors="coerce")
    if observed.notna().sum() < bins or observed.nunique(dropna=True) < 2:
        return pd.Series(["unavailable"] * len(observed), index=observed.index)
    return pd.qcut(observed, q=bins, labels=[f"q{i + 1}" for i in range(bins)], duplicates="drop").astype(str)


def _helper_evidence_config(config: SuePlaceboFailureAttributionConfig) -> SueHistoricalEventEvidenceConfig:
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


def _helper_gate_config(config: SuePlaceboFailureAttributionConfig) -> SueScoreDefinitionGateConfig:
    return SueScoreDefinitionGateConfig(
        events_path=config.events_path,
        crsp_daily_path=config.crsp_daily_path,
        output_dir=config.output_dir,
        report_path=config.report_path,
        quantiles=config.quantiles,
        min_rank_ic_names=config.min_rank_ic_names,
        min_spread_names=config.min_spread_names,
        denominator_abs_min=config.denominator_abs_min,
        winsorization_scope=config.winsorization_scope,
        winsor_lower_quantile=config.winsor_lower_quantile,
        winsor_upper_quantile=config.winsor_upper_quantile,
        extreme_value_cap=config.extreme_value_cap,
        placebo_shift_trading_days=10,
        random_seed=config.random_seed,
    )


def _mean_or_none(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.mean())


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
