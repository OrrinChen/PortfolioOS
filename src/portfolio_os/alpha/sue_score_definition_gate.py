"""Scale-aware SUE score-definition gate.

This module preregisters and evaluates SUE score-definition variants on an
already-built WRDS/PIT-labeled SUE panel. It does not run Q2, optimizers,
paper workflows, brokers, orders, or production approval paths.
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
    _build_window_return_frame,
    _event_window_grid,
    _load_and_validate_events,
    _load_prices,
    _price_index,
    _rank_ic_by_date,
    _safe_events,
    _top_bottom_spread_by_date,
)
from portfolio_os.alpha.sue_historical_schema import validate_no_forward_return_feature_columns
from portfolio_os.provenance.hashing import hash_payload


SUE_SCORE_DEFINITION_GATE_SCHEMA_VERSION = "sue_score_definition_gate.v1"

DEFAULT_EVENTS_PATH = "outputs/sue_historical_event_panel_expanded/events.csv"
DEFAULT_CRSP_DAILY_PATH = "data/cache/wrds_sue_event_panel/crsp_daily.csv"
DEFAULT_OUTPUT_DIR = "outputs/sue_score_definition_gate"
DEFAULT_REPORT_PATH = "reports/sue_score_definition_gate_report.md"

PRIMARY_WINDOW = "plus_2_plus_22"

SCORE_REGISTRY_TEMPLATE: dict[str, dict[str, Any]] = {
    "raw_eps_diff": {
        "formula": "actual_eps - expected_eps",
        "diagnostic_only": True,
        "denominator": None,
    },
    "surprise_pct_actual_eps": {
        "formula": "(actual_eps - expected_eps) / abs(actual_eps)",
        "diagnostic_only": False,
        "denominator": "actual_eps",
    },
    "surprise_pct_expected_eps": {
        "formula": "(actual_eps - expected_eps) / abs(expected_eps)",
        "diagnostic_only": False,
        "denominator": "expected_eps",
    },
    "surprise_scaled_price": {
        "formula": "(actual_eps - expected_eps) / abs(price_anchor)",
        "diagnostic_only": False,
        "denominator": "price_anchor",
    },
    "surprise_scaled_eps_vol": {
        "formula": "(actual_eps - expected_eps) / trailing_eps_surprise_volatility",
        "diagnostic_only": True,
        "denominator": "trailing_eps_surprise_volatility",
    },
}

MISLEADING_GATE_CLAIMS = (
    "production approved",
    "paper ready",
    "paper-ready",
    "live-ready",
    "live ready",
    "live trading",
    "live alpha orders",
    "broker execution",
    "order generation",
    "real historical sue alpha proven",
    "historical sue alpha proven",
    "sue alpha is proven",
    "guaranteed tradable alpha",
    "auto trading",
    "investment recommendation",
)


class SueScoreDefinitionGateConfig(BaseModel):
    """Config for Reopen-H1E scale-aware SUE score-definition gate."""

    model_config = ConfigDict(extra="forbid")

    events_path: str = DEFAULT_EVENTS_PATH
    crsp_daily_path: str = DEFAULT_CRSP_DAILY_PATH
    output_dir: str = DEFAULT_OUTPUT_DIR
    report_path: str = DEFAULT_REPORT_PATH
    quantiles: int = Field(default=5, gt=1)
    min_rank_ic_names: int = Field(default=3, gt=1)
    min_spread_names: int = Field(default=5, gt=1)
    denominator_abs_min: float = Field(default=0.01, gt=0.0)
    winsorization_scope: str = "month"
    winsor_lower_quantile: float = Field(default=0.01, ge=0.0, lt=0.5)
    winsor_upper_quantile: float = Field(default=0.99, gt=0.5, le=1.0)
    extreme_value_cap: float = Field(default=1000.0, gt=0.0)
    placebo_shift_trading_days: int = Field(default=10, gt=0)
    random_seed: int = 20260508
    max_tail_abs_share: float = Field(default=0.75, gt=0.0, le=1.0)
    min_month_breadth: int = Field(default=2, gt=0)
    min_year_breadth: int = Field(default=1, gt=0)


@dataclass(frozen=True)
class SueScoreDefinitionGateResult:
    """In-memory Reopen-H1E gate result."""

    config: SueScoreDefinitionGateConfig
    score_registry: list[dict[str, Any]]
    score_grid: pd.DataFrame
    window_metrics: pd.DataFrame
    placebo_report: dict[str, Any]
    denominator_guard_report: dict[str, Any]
    tail_concentration_report: dict[str, Any]
    size_liquidity_bucket_report: pd.DataFrame
    sector_exposure_report: pd.DataFrame
    score_selection_summary: dict[str, Any]
    report_text: str


def load_sue_score_definition_gate_config(path: str | Path) -> SueScoreDefinitionGateConfig:
    """Load H1E gate config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    guards = payload.get("guards") or {}
    return SueScoreDefinitionGateConfig(
        events_path=str(inputs.get("events_path") or payload.get("events_path") or DEFAULT_EVENTS_PATH),
        crsp_daily_path=str(inputs.get("crsp_daily_path") or payload.get("crsp_daily_path") or DEFAULT_CRSP_DAILY_PATH),
        output_dir=str(outputs.get("output_dir") or payload.get("output_dir") or DEFAULT_OUTPUT_DIR),
        report_path=str(outputs.get("report_path") or payload.get("report_path") or DEFAULT_REPORT_PATH),
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
        placebo_shift_trading_days=int(payload.get("placebo_shift_trading_days", 10)),
        random_seed=int(payload.get("random_seed", 20260508)),
        max_tail_abs_share=float(guards.get("max_tail_abs_share", payload.get("max_tail_abs_share", 0.75))),
        min_month_breadth=int(guards.get("min_month_breadth", payload.get("min_month_breadth", 2))),
        min_year_breadth=int(guards.get("min_year_breadth", payload.get("min_year_breadth", 1))),
    )


def build_sue_score_definition_gate(
    config: SueScoreDefinitionGateConfig | None = None,
) -> SueScoreDefinitionGateResult:
    """Build Reopen-H1E scale-aware SUE score-definition gate artifacts."""

    resolved = config or SueScoreDefinitionGateConfig()
    events = _load_and_validate_events(resolved.events_path)
    validate_no_forward_return_feature_columns(list(events.columns))
    prices = _load_prices(resolved.crsp_daily_path)
    price_index = _price_index(prices)
    safe_events = _safe_events(events)
    helper_config = _helper_evidence_config(resolved)
    return_frame = _build_window_return_frame(
        safe_events=safe_events,
        price_index=price_index,
        anchor_column="tradable_timestamp",
        config=helper_config,
    )
    scored_frame = _attach_gate_scores(
        return_frame=return_frame,
        safe_events=safe_events,
        crsp_daily_path=resolved.crsp_daily_path,
        config=resolved,
    )
    denominator_guard_report = _denominator_guard_report(scored_frame=scored_frame, config=resolved)
    window_metrics = _window_metrics(scored_frame=scored_frame, config=resolved, helper_config=helper_config)
    tail_report = _tail_concentration_report(scored_frame=scored_frame, window_metrics=window_metrics, config=resolved)
    provisional_score = _provisional_selected_score(
        window_metrics=window_metrics,
        denominator_guard_report=denominator_guard_report,
        tail_concentration_report=tail_report,
        config=resolved,
    )
    placebo_report = _placebo_report(
        safe_events=safe_events,
        price_index=price_index,
        selected_score=provisional_score,
        config=resolved,
        helper_config=helper_config,
    )
    score_registry, score_grid, selection_summary = _selection_outputs(
        safe_events=safe_events,
        window_metrics=window_metrics,
        denominator_guard_report=denominator_guard_report,
        tail_concentration_report=tail_report,
        placebo_report=placebo_report,
        provisional_score=provisional_score,
        config=resolved,
    )
    size_liquidity_report = _size_liquidity_bucket_report()
    sector_report = _sector_exposure_report()
    report_text = render_sue_score_definition_gate_report(
        score_grid=score_grid,
        window_metrics=window_metrics,
        score_selection_summary=selection_summary,
    )
    validate_sue_score_definition_gate_report_language(report_text)
    return SueScoreDefinitionGateResult(
        config=resolved,
        score_registry=score_registry,
        score_grid=score_grid,
        window_metrics=window_metrics,
        placebo_report=placebo_report,
        denominator_guard_report=denominator_guard_report,
        tail_concentration_report=tail_report,
        size_liquidity_bucket_report=size_liquidity_report,
        sector_exposure_report=sector_report,
        score_selection_summary=selection_summary,
        report_text=report_text,
    )


def write_sue_score_definition_gate_artifacts(result: SueScoreDefinitionGateResult) -> dict[str, Path]:
    """Write Reopen-H1E gate artifacts."""

    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(result.config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    paths = {
        "score_registry": output_dir / "score_registry.yaml",
        "score_grid": output_dir / "score_grid.csv",
        "window_metrics": output_dir / "window_metrics.csv",
        "placebo_report": output_dir / "placebo_report.json",
        "denominator_guard_report": output_dir / "denominator_guard_report.json",
        "tail_concentration_report": output_dir / "tail_concentration_report.json",
        "size_liquidity_bucket_report": output_dir / "size_liquidity_bucket_report.csv",
        "sector_exposure_report": output_dir / "sector_exposure_report.csv",
        "score_selection_summary": output_dir / "score_selection_summary.json",
        "report": report_path,
    }
    registry_payload = {
        "schema_version": "sue_score_definition_registry.v1",
        "scores": {row["score_name"]: row for row in result.score_registry},
    }
    paths["score_registry"].write_text(yaml.safe_dump(registry_payload, sort_keys=True), encoding="utf-8")
    result.score_grid.to_csv(paths["score_grid"], index=False)
    result.window_metrics.to_csv(paths["window_metrics"], index=False)
    _write_json(paths["placebo_report"], result.placebo_report)
    _write_json(paths["denominator_guard_report"], result.denominator_guard_report)
    _write_json(paths["tail_concentration_report"], result.tail_concentration_report)
    result.size_liquidity_bucket_report.to_csv(paths["size_liquidity_bucket_report"], index=False)
    result.sector_exposure_report.to_csv(paths["sector_exposure_report"], index=False)
    _write_json(paths["score_selection_summary"], result.score_selection_summary)
    validate_sue_score_definition_gate_report_language(result.report_text)
    paths["report"].write_text(result.report_text, encoding="utf-8")
    return paths


def render_sue_score_definition_gate_report(
    *,
    score_grid: pd.DataFrame,
    window_metrics: pd.DataFrame,
    score_selection_summary: dict[str, Any],
) -> str:
    """Render Reopen-H1E report."""

    selected = score_selection_summary.get("selected_score")
    interpretation = score_selection_summary.get("interpretation")
    primary = window_metrics.loc[window_metrics["window_name"].eq(PRIMARY_WINDOW)].copy()
    lines = [
        "# Scale-Aware SUE Score Definition Gate",
        "",
        "This is Reopen-H1E, a pre-registered scale-aware SUE score definition gate.",
        "raw EPS diff is diagnostic-only after this phase.",
        "Scale-aware SUE is a candidate, not production approval.",
        "This phase does not run Q2 or optimizer-path evaluation.",
        "This phase does not prove paper readiness.",
        "This phase does not create broker/order/live workflows.",
        "Downstream typed projection and Q2 require a separate explicit reopen.",
        "",
        "## Selection Summary",
        "",
        f"- interpretation: `{interpretation}`",
        f"- selected_score: `{selected}`",
        f"- provisional_score: `{score_selection_summary['provisional_score']}`",
        f"- raw_eps_diff_diagnostic_only: `{score_selection_summary['raw_eps_diff_diagnostic_only']}`",
        f"- placebo_passed: `{score_selection_summary['placebo_passed']}`",
        f"- event_date_shift_passed: `{score_selection_summary['event_date_shift_passed']}`",
        f"- provisional_denominator_guard_passed: `{score_selection_summary['provisional_denominator_guard_passed']}`",
        f"- provisional_tail_concentration_passed: `{score_selection_summary['provisional_tail_concentration_passed']}`",
        f"- denominator_guard_passed: `{score_selection_summary['denominator_guard_passed']}`",
        f"- tail_concentration_passed: `{score_selection_summary['tail_concentration_passed']}`",
        f"- month_breadth: `{score_selection_summary['month_breadth']}`",
        f"- year_breadth: `{score_selection_summary['year_breadth']}`",
        "",
        "## Candidate Score Grid",
        "",
        "| Score | Diagnostic Only | Selected | Primary Rank IC | Primary Spread | Denominator Pass | Tail Pass | Placebo Pass |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in score_grid.to_dict(orient="records"):
        lines.append(
            f"| `{row['score_name']}` | {row['diagnostic_only']} | {row['selected']} | "
            f"{_fmt(row['primary_mean_rank_ic'])} | {_fmt(row['primary_mean_top_bottom_spread'])} | "
            f"{row['denominator_guard_passed']} | {row['tail_concentration_passed']} | {row['placebo_passed']} |"
        )
    lines.extend(
        [
            "",
            "## Primary Window Metrics",
            "",
            "| Score | Window | Rows | Mean Rank IC | Rank IC t | Mean Top-Bottom Spread | Spread t | Status |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in primary.to_dict(orient="records"):
        lines.append(
            f"| `{row['score_name']}` | `{row['window_name']}` | {int(row['safe_row_count'])} | "
            f"{_fmt(row['mean_rank_ic'])} | {_fmt(row['rank_ic_t_stat'])} | "
            f"{_fmt(row['mean_top_bottom_spread'])} | {_fmt(row['top_bottom_t_stat'])} | `{row['status']}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Raw EPS difference remains available only as a diagnostic comparator.",
            "- Missing denominators, prices, or return windows remain unavailable/no_view, not zero alpha.",
            "- Failed variants are reported instead of hidden.",
            "- No Alpha Registry promotion is made by this report.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_sue_score_definition_gate_report_language(text: str) -> None:
    """Reject misleading Reopen-H1E claims while allowing explicit non-claims."""

    scrubbed = str(text).lower()
    allowed_phrases = [
        "scale-aware sue is a candidate, not production approval.",
        "this phase does not run q2 or optimizer-path evaluation.",
        "this phase does not prove paper readiness.",
        "this phase does not create broker/order/live workflows.",
        "no broker workflow was added.",
        "no order workflow was added.",
        "no live trading workflow was added.",
        "no production approval is claimed.",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in MISLEADING_GATE_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE score-definition gate claim detected: {claim}")


def _helper_evidence_config(config: SueScoreDefinitionGateConfig) -> SueHistoricalEventEvidenceConfig:
    return SueHistoricalEventEvidenceConfig(
        events_path=config.events_path,
        sue_values_path=config.events_path,
        crsp_daily_path=config.crsp_daily_path,
        output_dir=config.output_dir,
        report_path=config.report_path,
        quantiles=config.quantiles,
        min_rank_ic_names=config.min_rank_ic_names,
        min_spread_names=config.min_spread_names,
        placebo_shift_trading_days=config.placebo_shift_trading_days,
        random_seed=config.random_seed,
        evidence_scope="expanded",
    )


def _attach_gate_scores(
    *,
    return_frame: pd.DataFrame,
    safe_events: pd.DataFrame,
    crsp_daily_path: str,
    config: SueScoreDefinitionGateConfig,
) -> pd.DataFrame:
    events = safe_events.loc[:, ["event_id", "actual_eps", "expected_eps", "price_anchor_date"]].copy()
    if "trailing_eps_surprise_vol" in safe_events.columns:
        events["trailing_eps_surprise_vol"] = safe_events["trailing_eps_surprise_vol"]
    merged = return_frame.merge(events, on="event_id", how="left")
    price_map = _anchor_price_map(crsp_daily_path=crsp_daily_path, safe_events=safe_events)
    merged = merged.merge(price_map, on="event_id", how="left")
    raw = pd.to_numeric(merged["sue_value"], errors="coerce")
    actual_den = pd.to_numeric(merged["actual_eps"], errors="coerce").abs()
    expected_den = pd.to_numeric(merged["expected_eps"], errors="coerce").abs()
    price_den = pd.to_numeric(merged["price_anchor_price"], errors="coerce").abs()
    merged["raw_eps_diff"] = _winsorize_and_cap(raw, merged, config)
    merged["surprise_pct_actual_eps"] = _guarded_ratio(raw, actual_den, merged, config)
    merged["surprise_pct_expected_eps"] = _guarded_ratio(raw, expected_den, merged, config)
    merged["surprise_scaled_price"] = _guarded_ratio(raw, price_den, merged, config)
    if "trailing_eps_surprise_vol" in merged.columns:
        eps_vol_den = pd.to_numeric(merged["trailing_eps_surprise_vol"], errors="coerce").abs()
        merged["surprise_scaled_eps_vol"] = _guarded_ratio(raw, eps_vol_den, merged, config)
    else:
        merged["surprise_scaled_eps_vol"] = pd.NA
    return merged


def _anchor_price_map(*, crsp_daily_path: str, safe_events: pd.DataFrame) -> pd.DataFrame:
    prices = pd.read_csv(crsp_daily_path)
    if "prc" not in prices.columns:
        return pd.DataFrame({"event_id": safe_events["event_id"], "price_anchor_price": pd.NA})
    frame = prices.loc[:, ["permno", "date", "prc"]].copy()
    frame["permno"] = pd.to_numeric(frame["permno"], errors="coerce")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["prc"] = pd.to_numeric(frame["prc"], errors="coerce").abs()
    events = safe_events.loc[:, ["event_id", "permno", "price_anchor_date"]].copy()
    events["permno"] = pd.to_numeric(events["permno"], errors="coerce")
    events["price_anchor_date"] = pd.to_datetime(events["price_anchor_date"], errors="coerce").dt.date
    joined = events.merge(frame, left_on=["permno", "price_anchor_date"], right_on=["permno", "date"], how="left")
    return joined.loc[:, ["event_id", "prc"]].rename(columns={"prc": "price_anchor_price"})


def _guarded_ratio(
    numerator: pd.Series,
    denominator: pd.Series,
    frame: pd.DataFrame,
    config: SueScoreDefinitionGateConfig,
) -> pd.Series:
    valid = denominator.notna() & denominator.ge(config.denominator_abs_min)
    values = numerator.where(valid) / denominator.where(valid)
    return _winsorize_and_cap(values, frame, config)


def _winsorize_and_cap(values: pd.Series, frame: pd.DataFrame, config: SueScoreDefinitionGateConfig) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if config.winsorization_scope == "month":
        month = pd.to_datetime(frame["rebalance_date"], errors="coerce").dt.to_period("M").astype(str)
        clipped = numeric.groupby(month).transform(lambda series: _winsorize_series(series, config))
    elif config.winsorization_scope == "global":
        clipped = _winsorize_series(numeric, config)
    else:
        raise ValueError(f"unsupported winsorization_scope: {config.winsorization_scope}")
    return clipped.clip(lower=-config.extreme_value_cap, upper=config.extreme_value_cap)


def _winsorize_series(values: pd.Series, config: SueScoreDefinitionGateConfig) -> pd.Series:
    observed = pd.to_numeric(values, errors="coerce").dropna()
    if observed.empty:
        return values
    lower = float(observed.quantile(config.winsor_lower_quantile))
    upper = float(observed.quantile(config.winsor_upper_quantile))
    return values.clip(lower=lower, upper=upper)


def _window_metrics(
    *,
    scored_frame: pd.DataFrame,
    config: SueScoreDefinitionGateConfig,
    helper_config: SueHistoricalEventEvidenceConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for score_name in SCORE_REGISTRY_TEMPLATE:
        rank_rows = _rank_ic_by_date(scored_frame, score_column=score_name, config=helper_config)
        spread_rows = _top_bottom_spread_by_date(scored_frame, score_column=score_name, config=helper_config)
        grid = _event_window_grid(
            live_return_frame=scored_frame,
            rank_ic_by_date=rank_rows,
            top_bottom_spread_by_date=spread_rows,
        )
        for row in grid.to_dict(orient="records"):
            score_values = pd.to_numeric(
                scored_frame.loc[scored_frame["window_name"].eq(row["window_name"]), score_name],
                errors="coerce",
            )
            rows.append(
                {
                    "schema_version": SUE_SCORE_DEFINITION_GATE_SCHEMA_VERSION,
                    "score_name": score_name,
                    "window_name": row["window_name"],
                    "start_offset": row["start_offset"],
                    "end_offset": row["end_offset"],
                    "safe_row_count": row["safe_row_count"],
                    "score_available_count": int(score_values.notna().sum()),
                    "missing_return_window_count": row["missing_return_window_count"],
                    "rank_ic_date_count": row["rank_ic_date_count"],
                    "mean_rank_ic": row["mean_rank_ic"],
                    "rank_ic_t_stat": row["rank_ic_t_stat"],
                    "top_bottom_date_count": row["top_bottom_date_count"],
                    "mean_top_bottom_spread": row["mean_top_bottom_spread"],
                    "top_bottom_t_stat": row["top_bottom_t_stat"],
                    "market_adjusted_available": False,
                    "mean_market_adjusted_top_bottom_spread": None,
                    "status": "observed" if int(row["rank_ic_date_count"] or 0) > 0 else "unavailable",
                }
            )
    return pd.DataFrame(rows)


def _denominator_guard_report(*, scored_frame: pd.DataFrame, config: SueScoreDefinitionGateConfig) -> dict[str, Any]:
    unique = scored_frame.drop_duplicates("event_id")
    denominator_sources = {
        "raw_eps_diff": None,
        "surprise_pct_actual_eps": "actual_eps",
        "surprise_pct_expected_eps": "expected_eps",
        "surprise_scaled_price": "price_anchor_price",
        "surprise_scaled_eps_vol": "trailing_eps_surprise_vol",
    }
    score_reports: dict[str, Any] = {}
    for score_name, denominator_column in denominator_sources.items():
        if denominator_column is None:
            report = {
                "score_name": score_name,
                "denominator_column": None,
                "diagnostic_only": True,
                "denominator_missing_count": 0,
                "denominator_below_min_count": 0,
                "score_available_count": int(unique[score_name].notna().sum()) if score_name in unique.columns else 0,
                "denominator_guard_passed": False,
            }
        elif denominator_column not in unique.columns:
            report = {
                "score_name": score_name,
                "denominator_column": denominator_column,
                "diagnostic_only": True,
                "denominator_missing_count": int(len(unique)),
                "denominator_below_min_count": 0,
                "score_available_count": 0,
                "denominator_guard_passed": False,
            }
        else:
            denominator = pd.to_numeric(unique[denominator_column], errors="coerce").abs()
            missing = int(denominator.isna().sum())
            below = int(denominator.notna().mul(denominator.lt(config.denominator_abs_min)).sum())
            available = int(unique[score_name].notna().sum()) if score_name in unique.columns else 0
            report = {
                "score_name": score_name,
                "denominator_column": denominator_column,
                "diagnostic_only": bool(score_name == "surprise_scaled_eps_vol" and available == 0),
                "denominator_missing_count": missing,
                "denominator_below_min_count": below,
                "score_available_count": available,
                "denominator_guard_passed": available > 0 and missing + below < len(unique),
            }
        score_reports[score_name] = report
    payload = {
        "schema_version": "sue_score_definition_denominator_guard.v1",
        "denominator_abs_min": config.denominator_abs_min,
        "score_reports": score_reports,
        "negative_or_near_zero_eps_denominator_audited": True,
        "price_denominator_pit_audited": True,
        "missing_denominator_is_no_view": True,
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _tail_concentration_report(
    *,
    scored_frame: pd.DataFrame,
    window_metrics: pd.DataFrame,
    config: SueScoreDefinitionGateConfig,
) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    primary = scored_frame.loc[scored_frame["window_name"].eq(PRIMARY_WINDOW)].copy()
    for score_name in SCORE_REGISTRY_TEMPLATE:
        values = pd.to_numeric(primary[score_name], errors="coerce").dropna().abs()
        total = float(values.sum()) if not values.empty else 0.0
        max_share = float(values.max() / total) if total > 0 else None
        top_n = max(1, int(len(values) * 0.01)) if not values.empty else 0
        top_share = float(values.sort_values(ascending=False).head(top_n).sum() / total) if total > 0 else None
        reports[score_name] = {
            "score_name": score_name,
            "observed_count": int(len(values)),
            "max_abs_score_share": max_share,
            "top_1pct_abs_score_share": top_share,
            "tail_concentration_passed": bool(top_share is not None and top_share <= config.max_tail_abs_share),
        }
    payload = {
        "schema_version": "sue_score_definition_tail_concentration.v1",
        "primary_window": PRIMARY_WINDOW,
        "max_tail_abs_share": config.max_tail_abs_share,
        "score_reports": reports,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _provisional_selected_score(
    *,
    window_metrics: pd.DataFrame,
    denominator_guard_report: dict[str, Any],
    tail_concentration_report: dict[str, Any],
    config: SueScoreDefinitionGateConfig,
) -> str | None:
    raw = _metric_row(window_metrics, "raw_eps_diff", PRIMARY_WINDOW)
    raw_spread = _float_or_none(raw.get("mean_top_bottom_spread"))
    candidates = ["surprise_pct_actual_eps", "surprise_pct_expected_eps", "surprise_scaled_price", "surprise_scaled_eps_vol"]
    for score_name in candidates:
        row = _metric_row(window_metrics, score_name, PRIMARY_WINDOW)
        rank = _float_or_none(row.get("mean_rank_ic"))
        spread = _float_or_none(row.get("mean_top_bottom_spread"))
        denom_pass = bool(denominator_guard_report["score_reports"][score_name]["denominator_guard_passed"])
        tail_pass = bool(tail_concentration_report["score_reports"][score_name]["tail_concentration_passed"])
        beats_raw_spread = spread is not None and (raw_spread is None or spread > raw_spread)
        if rank is not None and rank > 0.0 and spread is not None and spread > 0.0 and beats_raw_spread and denom_pass and tail_pass:
            return score_name
    return None


def _placebo_report(
    *,
    safe_events: pd.DataFrame,
    price_index: dict[int, pd.DataFrame],
    selected_score: str | None,
    config: SueScoreDefinitionGateConfig,
    helper_config: SueHistoricalEventEvidenceConfig,
) -> dict[str, Any]:
    if selected_score is None:
        payload = {
            "schema_version": "sue_score_definition_gate_placebo.v1",
            "placebo_diagnostics_generated": True,
            "selected_score": None,
            "placebo_passed": False,
            "reason": "no provisional score selected",
        }
        payload["content_hash"] = hash_payload(payload)
        return payload
    live = _build_window_return_frame(
        safe_events=safe_events,
        price_index=price_index,
        anchor_column="tradable_timestamp",
        config=helper_config,
    )
    shifted = _build_window_return_frame(
        safe_events=safe_events,
        price_index=price_index,
        anchor_column="placebo_anchor_date",
        config=helper_config,
    )
    live = _attach_gate_scores(return_frame=live, safe_events=safe_events, crsp_daily_path=config.crsp_daily_path, config=config)
    shifted = _attach_gate_scores(
        return_frame=shifted,
        safe_events=safe_events,
        crsp_daily_path=config.crsp_daily_path,
        config=config,
    )
    sign_flip = live.copy()
    sign_flip[selected_score] = -pd.to_numeric(sign_flip[selected_score], errors="coerce")
    randomized = live.copy()
    randomized[selected_score] = randomized.groupby("rebalance_date", group_keys=False)[selected_score].transform(
        lambda series: series.sample(frac=1.0, random_state=config.random_seed).to_numpy()
    )
    diagnostics = {
        "live": _primary_metric_summary(live, selected_score, helper_config),
        "event_date_shift": _primary_metric_summary(shifted, selected_score, helper_config),
        "sign_flip": _primary_metric_summary(sign_flip, selected_score, helper_config),
        "randomized_sue": _primary_metric_summary(randomized, selected_score, helper_config),
    }
    live_rank = _float_or_none(diagnostics["live"].get("mean_rank_ic")) or -999.0
    live_spread = _float_or_none(diagnostics["live"].get("mean_top_bottom_spread")) or -999.0
    random_rank = _float_or_none(diagnostics["randomized_sue"].get("mean_rank_ic")) or 999.0
    random_spread = _float_or_none(diagnostics["randomized_sue"].get("mean_top_bottom_spread")) or 999.0
    shifted_rank = _float_or_none(diagnostics["event_date_shift"].get("mean_rank_ic"))
    shifted_spread = _float_or_none(diagnostics["event_date_shift"].get("mean_top_bottom_spread"))
    sign_rank = _float_or_none(diagnostics["sign_flip"].get("mean_rank_ic")) or 999.0
    shift_passed = bool(
        (shifted_rank is None or live_rank > shifted_rank)
        and (shifted_spread is None or live_spread > shifted_spread)
    )
    placebo_passed = bool(live_rank > random_rank and live_spread > random_spread and sign_rank < live_rank and shift_passed)
    payload = {
        "schema_version": "sue_score_definition_gate_placebo.v1",
        "placebo_diagnostics_generated": True,
        "selected_score": selected_score,
        "placebo_passed": placebo_passed,
        "event_date_shift_trading_days": config.placebo_shift_trading_days,
        "random_seed": config.random_seed,
        "diagnostics": diagnostics,
        "event_date_shift_passed": shift_passed,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "production_approval_claimed": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _primary_metric_summary(
    frame: pd.DataFrame,
    score_name: str,
    helper_config: SueHistoricalEventEvidenceConfig,
) -> dict[str, Any]:
    rank_rows = _rank_ic_by_date(frame, score_column=score_name, config=helper_config)
    spread_rows = _top_bottom_spread_by_date(frame, score_column=score_name, config=helper_config)
    grid = _event_window_grid(
        live_return_frame=frame,
        rank_ic_by_date=rank_rows,
        top_bottom_spread_by_date=spread_rows,
    )
    row = grid.loc[grid["window_name"].eq(PRIMARY_WINDOW)]
    if row.empty:
        return {"window_name": PRIMARY_WINDOW, "status": "unavailable"}
    return row.iloc[0].where(pd.notna(row.iloc[0]), None).to_dict()


def _selection_outputs(
    *,
    safe_events: pd.DataFrame,
    window_metrics: pd.DataFrame,
    denominator_guard_report: dict[str, Any],
    tail_concentration_report: dict[str, Any],
    placebo_report: dict[str, Any],
    provisional_score: str | None,
    config: SueScoreDefinitionGateConfig,
) -> tuple[list[dict[str, Any]], pd.DataFrame, dict[str, Any]]:
    placebo_passed = bool(placebo_report.get("placebo_passed"))
    selected_score = provisional_score if provisional_score and placebo_passed else None
    month_breadth = int(pd.to_datetime(safe_events["rebalance_date"], errors="coerce").dt.to_period("M").nunique())
    year_breadth = int(pd.to_datetime(safe_events["rebalance_date"], errors="coerce").dt.year.nunique())
    breadth_passed = month_breadth >= config.min_month_breadth and year_breadth >= config.min_year_breadth
    score_grid_rows: list[dict[str, Any]] = []
    registry_rows: list[dict[str, Any]] = []
    for score_name, spec in SCORE_REGISTRY_TEMPLATE.items():
        row = _metric_row(window_metrics, score_name, PRIMARY_WINDOW)
        denom_pass = bool(denominator_guard_report["score_reports"][score_name]["denominator_guard_passed"])
        tail_pass = bool(tail_concentration_report["score_reports"][score_name]["tail_concentration_passed"])
        score_placebo_pass = bool(score_name == provisional_score and placebo_passed)
        unavailable = int(row.get("score_available_count") or 0) == 0
        diagnostic_only = bool(spec["diagnostic_only"] or unavailable)
        selected = bool(score_name == selected_score and breadth_passed)
        registry_rows.append(
            {
                "score_name": score_name,
                "formula": spec["formula"],
                "diagnostic_only": diagnostic_only,
                "denominator": spec["denominator"],
                "denominator_abs_min": config.denominator_abs_min,
                "winsorization_scope": config.winsorization_scope,
                "extreme_value_cap": config.extreme_value_cap,
                "selected": selected,
                "selection_status": _score_selection_status(
                    score_name=score_name,
                    diagnostic_only=diagnostic_only,
                    selected=selected,
                    denom_pass=denom_pass,
                    tail_pass=tail_pass,
                    placebo_pass=score_placebo_pass,
                ),
            }
        )
        score_grid_rows.append(
            {
                "schema_version": SUE_SCORE_DEFINITION_GATE_SCHEMA_VERSION,
                "score_name": score_name,
                "diagnostic_only": diagnostic_only,
                "selected": selected,
                "primary_window": PRIMARY_WINDOW,
                "primary_mean_rank_ic": row.get("mean_rank_ic"),
                "primary_rank_ic_t_stat": row.get("rank_ic_t_stat"),
                "primary_mean_top_bottom_spread": row.get("mean_top_bottom_spread"),
                "primary_top_bottom_t_stat": row.get("top_bottom_t_stat"),
                "score_available_count": row.get("score_available_count"),
                "denominator_guard_passed": denom_pass,
                "tail_concentration_passed": tail_pass,
                "placebo_passed": score_placebo_pass,
                "selection_status": registry_rows[-1]["selection_status"],
            }
        )
    interpretation = _interpret_selection(selected_score=selected_score if breadth_passed else None, provisional_score=provisional_score)
    summary = {
        "schema_version": "sue_score_definition_selection_summary.v1",
        "selected_score": selected_score if breadth_passed else None,
        "provisional_score": provisional_score,
        "interpretation": interpretation,
        "raw_eps_diff_diagnostic_only": True,
        "failed_variants": [
            row["score_name"]
            for row in score_grid_rows
            if row["score_name"] != selected_score and row["selection_status"] != "selected"
        ],
        "primary_window": PRIMARY_WINDOW,
        "month_breadth": month_breadth,
        "year_breadth": year_breadth,
        "breadth_passed": breadth_passed,
        "placebo_passed": placebo_passed,
        "event_date_shift_passed": bool(placebo_report.get("event_date_shift_passed", False)),
        "provisional_denominator_guard_passed": bool(
            provisional_score and denominator_guard_report["score_reports"][provisional_score]["denominator_guard_passed"]
        ),
        "provisional_tail_concentration_passed": bool(
            provisional_score and tail_concentration_report["score_reports"][provisional_score]["tail_concentration_passed"]
        ),
        "denominator_guard_passed": bool(
            selected_score and denominator_guard_report["score_reports"][selected_score]["denominator_guard_passed"]
        ),
        "tail_concentration_passed": bool(
            selected_score and tail_concentration_report["score_reports"][selected_score]["tail_concentration_passed"]
        ),
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "alpha_registry_promoted": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
    }
    summary["content_hash"] = hash_payload(summary)
    return registry_rows, pd.DataFrame(score_grid_rows), summary


def _score_selection_status(
    *,
    score_name: str,
    diagnostic_only: bool,
    selected: bool,
    denom_pass: bool,
    tail_pass: bool,
    placebo_pass: bool,
) -> str:
    if selected:
        return "selected"
    if score_name == "raw_eps_diff":
        return "diagnostic_only_raw_eps_diff"
    if diagnostic_only:
        return "diagnostic_only_or_unavailable"
    if not denom_pass:
        return "failed_denominator_guard"
    if not tail_pass:
        return "failed_tail_concentration_guard"
    if not placebo_pass:
        return "failed_placebo_or_selection_threshold"
    return "not_selected"


def _interpret_selection(*, selected_score: str | None, provisional_score: str | None) -> str:
    if selected_score:
        return "scale_aware_sue_candidate_selected"
    if provisional_score:
        return "scale_aware_sue_mixed"
    return "scale_aware_sue_inconclusive"


def _size_liquidity_bucket_report() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "schema_version": "sue_score_definition_size_liquidity_bucket.v1",
                "bucket_type": "size",
                "bucket": "unavailable",
                "status": "unavailable_missing_size_field",
                "row_count": 0,
            },
            {
                "schema_version": "sue_score_definition_size_liquidity_bucket.v1",
                "bucket_type": "liquidity",
                "bucket": "unavailable",
                "status": "unavailable_missing_liquidity_field",
                "row_count": 0,
            },
        ]
    )


def _sector_exposure_report() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "schema_version": "sue_score_definition_sector_exposure.v1",
                "sector": "unavailable",
                "industry": "unavailable",
                "status": "unavailable_missing_sector_industry_fields",
                "row_count": 0,
            }
        ]
    )


def _metric_row(frame: pd.DataFrame, score_name: str, window_name: str) -> dict[str, Any]:
    rows = frame.loc[frame["score_name"].eq(score_name) & frame["window_name"].eq(window_name)]
    if rows.empty:
        return {}
    return rows.iloc[0].where(pd.notna(rows.iloc[0]), None).to_dict()


def _float_or_none(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _fmt(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):.6f}"


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
