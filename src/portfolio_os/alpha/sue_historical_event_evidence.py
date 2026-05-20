"""Bounded historical SUE event evidence grid.

This module evaluates event-window evidence on an already-built WRDS/PIT-labeled
SUE panel. It does not run Q2, optimizers, brokers, orders, paper workflows, or
production approval paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.alpha.sue_historical_schema import (
    SueHistoricalEventRow,
    validate_no_forward_return_feature_columns,
)
from portfolio_os.provenance.hashing import canonical_json, hash_payload


SUE_HISTORICAL_EVENT_EVIDENCE_SCHEMA_VERSION = "sue_historical_event_evidence.v1"

DEFAULT_EVENTS_PATH = "outputs/sue_historical_event_panel_full/events.csv"
DEFAULT_SUE_VALUES_PATH = "outputs/sue_historical_event_panel_full/sue_values.csv"
DEFAULT_CRSP_DAILY_PATH = "data/cache/wrds_sue_event_panel/crsp_daily.csv"
DEFAULT_PIT_VISIBILITY_REPORT_PATH = "outputs/sue_historical_event_panel_full/pit_visibility_report.json"
DEFAULT_COVERAGE_REPORT_PATH = "outputs/sue_historical_event_panel_full/coverage_report.json"
DEFAULT_LINEAGE_MANIFEST_PATH = "outputs/sue_historical_event_panel_full/data_lineage_manifest.json"
DEFAULT_OUTPUT_DIR = "outputs/sue_historical_event_evidence"
DEFAULT_REPORT_PATH = "reports/sue_historical_event_evidence_report.md"

EVENT_WINDOWS = [
    {"window_name": "plus_2_plus_2", "start_offset": 2, "end_offset": 2},
    {"window_name": "plus_2_plus_3", "start_offset": 2, "end_offset": 3},
    {"window_name": "plus_2_plus_22", "start_offset": 2, "end_offset": 22},
]

MISLEADING_EVIDENCE_CLAIMS = (
    "production approved",
    "paper ready",
    "paper-ready",
    "live-ready",
    "live ready",
    "live trading",
    "live alpha orders",
    "broker",
    "order",
    "real historical sue alpha proven",
    "historical sue alpha proven",
    "sue alpha is proven",
    "full historical sue alpha proven",
    "guaranteed tradable alpha",
    "auto trading",
    "investment recommendation",
)


class SueHistoricalEventEvidenceConfig(BaseModel):
    """Config for bounded historical SUE event evidence."""

    model_config = ConfigDict(extra="forbid")

    events_path: str = DEFAULT_EVENTS_PATH
    sue_values_path: str = DEFAULT_SUE_VALUES_PATH
    crsp_daily_path: str = DEFAULT_CRSP_DAILY_PATH
    pit_visibility_report_path: str = DEFAULT_PIT_VISIBILITY_REPORT_PATH
    coverage_report_path: str = DEFAULT_COVERAGE_REPORT_PATH
    data_lineage_manifest_path: str = DEFAULT_LINEAGE_MANIFEST_PATH
    output_dir: str = DEFAULT_OUTPUT_DIR
    report_path: str = DEFAULT_REPORT_PATH
    quantiles: int = Field(default=5, gt=1)
    min_rank_ic_names: int = Field(default=3, gt=1)
    min_spread_names: int = Field(default=5, gt=1)
    placebo_shift_trading_days: int = Field(default=10, gt=0)
    random_seed: int = 20260506
    evidence_scope: str = "bounded"


@dataclass(frozen=True)
class SueHistoricalEventEvidenceResult:
    """In-memory H1B evidence result."""

    config: SueHistoricalEventEvidenceConfig
    event_window_grid: pd.DataFrame
    rank_ic_by_date: pd.DataFrame
    top_bottom_spread_by_date: pd.DataFrame
    placebo_report: dict[str, Any]
    coverage_by_month: pd.DataFrame
    coverage_by_year: pd.DataFrame
    pit_leakage_audit: dict[str, Any]
    evidence_summary: dict[str, Any]
    report_text: str


def load_sue_historical_event_evidence_config(path: str | Path) -> SueHistoricalEventEvidenceConfig:
    """Load H1B evidence-grid config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    return SueHistoricalEventEvidenceConfig(
        events_path=str(inputs.get("events_path") or payload.get("events_path") or DEFAULT_EVENTS_PATH),
        sue_values_path=str(inputs.get("sue_values_path") or payload.get("sue_values_path") or DEFAULT_SUE_VALUES_PATH),
        crsp_daily_path=str(inputs.get("crsp_daily_path") or payload.get("crsp_daily_path") or DEFAULT_CRSP_DAILY_PATH),
        pit_visibility_report_path=str(
            inputs.get("pit_visibility_report_path")
            or payload.get("pit_visibility_report_path")
            or DEFAULT_PIT_VISIBILITY_REPORT_PATH
        ),
        coverage_report_path=str(
            inputs.get("coverage_report_path") or payload.get("coverage_report_path") or DEFAULT_COVERAGE_REPORT_PATH
        ),
        data_lineage_manifest_path=str(
            inputs.get("data_lineage_manifest_path")
            or payload.get("data_lineage_manifest_path")
            or DEFAULT_LINEAGE_MANIFEST_PATH
        ),
        output_dir=str(outputs.get("output_dir") or payload.get("output_dir") or DEFAULT_OUTPUT_DIR),
        report_path=str(outputs.get("report_path") or payload.get("report_path") or DEFAULT_REPORT_PATH),
        quantiles=int(payload.get("quantiles", 5)),
        min_rank_ic_names=int(payload.get("min_rank_ic_names", 3)),
        min_spread_names=int(payload.get("min_spread_names", 5)),
        placebo_shift_trading_days=int(payload.get("placebo_shift_trading_days", 10)),
        random_seed=int(payload.get("random_seed", 20260506)),
        evidence_scope=str(payload.get("evidence_scope", "bounded")),
    )


def build_sue_historical_event_evidence_grid(
    config: SueHistoricalEventEvidenceConfig | None = None,
) -> SueHistoricalEventEvidenceResult:
    """Build bounded historical event-window evidence from PIT-safe SUE rows."""

    resolved = config or SueHistoricalEventEvidenceConfig()
    events = _load_and_validate_events(resolved.events_path)
    sue_values = pd.read_csv(resolved.sue_values_path)
    validate_no_forward_return_feature_columns(list(sue_values.columns))
    prices = _load_prices(resolved.crsp_daily_path)
    price_index = _price_index(prices)
    safe_events = _safe_events(events)

    live_return_frame = _build_window_return_frame(
        safe_events=safe_events,
        price_index=price_index,
        anchor_column="tradable_timestamp",
        config=resolved,
    )
    rank_ic_by_date = _rank_ic_by_date(live_return_frame, score_column="sue_value", config=resolved)
    top_bottom = _top_bottom_spread_by_date(live_return_frame, score_column="sue_value", config=resolved)
    event_window_grid = _event_window_grid(
        live_return_frame=live_return_frame,
        rank_ic_by_date=rank_ic_by_date,
        top_bottom_spread_by_date=top_bottom,
    )

    shifted_return_frame = _build_window_return_frame(
        safe_events=safe_events,
        price_index=price_index,
        anchor_column="placebo_anchor_date",
        config=resolved,
    )
    placebo_report = _placebo_report(
        live_return_frame=live_return_frame,
        shifted_return_frame=shifted_return_frame,
        config=resolved,
    )
    coverage_by_month = _coverage_by_period(safe_events, period="month")
    coverage_by_year = _coverage_by_period(safe_events, period="year")
    pit_audit = _pit_leakage_audit(
        events=events,
        live_return_frame=live_return_frame,
        input_paths={
            "pit_visibility_report": resolved.pit_visibility_report_path,
            "coverage_report": resolved.coverage_report_path,
            "data_lineage_manifest": resolved.data_lineage_manifest_path,
        },
    )
    evidence_summary = _evidence_summary(
        config=resolved,
        safe_events=safe_events,
        event_window_grid=event_window_grid,
        placebo_report=placebo_report,
        pit_leakage_audit=pit_audit,
    )
    report_text = render_sue_historical_event_evidence_report_from_payloads(
        event_window_grid=event_window_grid,
        evidence_summary=evidence_summary,
        pit_leakage_audit=pit_audit,
        placebo_report=placebo_report,
    )
    validate_sue_historical_event_evidence_report_language(report_text)
    return SueHistoricalEventEvidenceResult(
        config=resolved,
        event_window_grid=event_window_grid,
        rank_ic_by_date=rank_ic_by_date,
        top_bottom_spread_by_date=top_bottom,
        placebo_report=placebo_report,
        coverage_by_month=coverage_by_month,
        coverage_by_year=coverage_by_year,
        pit_leakage_audit=pit_audit,
        evidence_summary=evidence_summary,
        report_text=report_text,
    )


def write_sue_historical_event_evidence_artifacts(
    result: SueHistoricalEventEvidenceResult,
) -> dict[str, Path]:
    """Write H1B evidence artifacts."""

    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(result.config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    paths = {
        "event_window_grid": output_dir / "event_window_grid.csv",
        "rank_ic_by_date": output_dir / "rank_ic_by_date.csv",
        "top_bottom_spread_by_date": output_dir / "top_bottom_spread_by_date.csv",
        "placebo_report": output_dir / "placebo_report.json",
        "coverage_by_month": output_dir / "coverage_by_month.csv",
        "coverage_by_year": output_dir / "coverage_by_year.csv",
        "pit_leakage_audit": output_dir / "pit_leakage_audit.json",
        "evidence_summary": output_dir / "evidence_summary.json",
        "report": report_path,
    }
    result.event_window_grid.to_csv(paths["event_window_grid"], index=False)
    result.rank_ic_by_date.to_csv(paths["rank_ic_by_date"], index=False)
    result.top_bottom_spread_by_date.to_csv(paths["top_bottom_spread_by_date"], index=False)
    result.coverage_by_month.to_csv(paths["coverage_by_month"], index=False)
    result.coverage_by_year.to_csv(paths["coverage_by_year"], index=False)
    _write_json(paths["placebo_report"], result.placebo_report)
    _write_json(paths["pit_leakage_audit"], result.pit_leakage_audit)
    _write_json(paths["evidence_summary"], result.evidence_summary)
    validate_sue_historical_event_evidence_report_language(result.report_text)
    paths["report"].write_text(result.report_text, encoding="utf-8")
    return paths


def render_sue_historical_event_evidence_report(result: SueHistoricalEventEvidenceResult) -> str:
    """Render H1B evidence report from an in-memory result."""

    return result.report_text


def render_sue_historical_event_evidence_report_from_payloads(
    *,
    event_window_grid: pd.DataFrame,
    evidence_summary: dict[str, Any],
    pit_leakage_audit: dict[str, Any],
    placebo_report: dict[str, Any],
) -> str:
    """Render the bounded historical evidence report."""

    best = evidence_summary.get("best_window") or {}
    if evidence_summary.get("evidence_scope") == "expanded":
        title = "# Expanded Historical SUE Event Evidence Grid"
        opening = "This is expanded WRDS/PIT historical evidence, not production approval."
        limitation = "This expanded sample must still be diagnosed before stronger claims."
    else:
        title = "# Bounded Historical SUE Event Evidence Grid"
        opening = "This is a bounded WRDS/PIT-safe historical evidence grid."
        limitation = "The sample is concentrated around 2020-01-06 to 2020-04-17 and must be expanded before stronger claims."
    lines = [
        title,
        "",
        opening,
        "It does not prove full historical SUE alpha.",
        "It does not prove paper readiness or production approval.",
        "It does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "It does not run Q2 or optimizer-path evaluation.",
        limitation,
        "",
        "## Summary",
        "",
        f"- interpretation: `{evidence_summary['interpretation']}`",
        f"- pit_safe_rows: `{evidence_summary['pit_safe_rows']}`",
        f"- safe_rebalance_dates: `{evidence_summary['safe_rebalance_dates']}`",
        f"- best_window: `{best.get('window_name')}`",
        f"- best_window_mean_rank_ic: `{_fmt(best.get('mean_rank_ic'))}`",
        f"- best_window_rank_ic_t_stat: `{_fmt(best.get('rank_ic_t_stat'))}`",
        f"- best_window_mean_top_bottom_spread: `{_fmt(best.get('mean_top_bottom_spread'))}`",
        "",
        "## Event Window Grid",
        "",
        "| Window | Rows | Mean Rank IC | Rank IC t | Mean Top-Bottom Spread | Spread t |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in event_window_grid.to_dict(orient="records"):
        lines.append(
            f"| `{row['window_name']}` | {int(row['safe_row_count'])} | {_fmt(row['mean_rank_ic'])} | "
            f"{_fmt(row['rank_ic_t_stat'])} | {_fmt(row['mean_top_bottom_spread'])} | "
            f"{_fmt(row['top_bottom_t_stat'])} |"
        )
    lines.extend(
        [
            "",
            "## Diagnostics",
            "",
            f"- placebo_diagnostics_generated: `{placebo_report['placebo_diagnostics_generated']}`",
            f"- missing_return_window_count: `{pit_leakage_audit['missing_return_window_count']}`",
            f"- missing_coverage_encoded_as_zero_alpha: `{pit_leakage_audit['missing_coverage_encoded_as_zero_alpha']}`",
            f"- no_view_not_zero_alpha: `{pit_leakage_audit['no_view_not_zero_alpha']}`",
            f"- forward_return_feature_columns_detected: `{pit_leakage_audit['forward_return_feature_columns_detected']}`",
            "",
            "## Boundaries",
            "",
            "- This phase evaluates event-window evidence only.",
            "- Missing SUE, price, or return coverage remains unavailable/no_view, not zero alpha.",
            "- Alpha Registry status is not promoted by this report.",
            "- Downstream typed AlphaView, Q2, and optimizer-path evaluation require separate explicit reopen phases.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_sue_historical_event_evidence_report_language(text: str) -> None:
    """Reject misleading H1B evidence claims while allowing explicit non-claims."""

    scrubbed = str(text).lower()
    allowed_phrases = [
        "it does not prove full historical sue alpha.",
        "it does not prove paper readiness or production approval.",
        "it does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "it does not run q2 or optimizer-path evaluation.",
        "no production approval is claimed.",
        "no broker workflow was added.",
        "no order workflow was added.",
        "no live trading workflow was added.",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in MISLEADING_EVIDENCE_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE historical event evidence claim detected: {claim}")


def _load_and_validate_events(path: str | Path) -> pd.DataFrame:
    events = pd.read_csv(path)
    validate_no_forward_return_feature_columns(list(events.columns))
    for column in ["event_id", "symbol", "ibes_ticker", "cusip", "fiscal_period", "sue_definition", "data_source", "link_method", "pit_safety_status"]:
        if column in events.columns:
            events[column] = events[column].where(events[column].isna(), events[column].astype(str))
    rows = []
    for record in events.to_dict(orient="records"):
        row = SueHistoricalEventRow.model_validate(_clean_record(record))
        rows.append(row.model_dump(mode="json"))
    frame = pd.DataFrame(rows)
    frame["permno"] = pd.to_numeric(frame["permno"], errors="coerce")
    frame["sue_value"] = pd.to_numeric(frame["sue_value"], errors="coerce")
    frame["diagnostic_only"] = frame["diagnostic_only"].astype(bool)
    frame["rebalance_date"] = pd.to_datetime(frame["rebalance_date"], errors="raise").dt.date
    frame["announcement_date"] = pd.to_datetime(frame["announcement_date"], errors="raise").dt.date
    frame["tradable_date"] = pd.to_datetime(frame["tradable_timestamp"], errors="raise", utc=True).dt.date
    return frame


def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: (None if pd.isna(value) else value) for key, value in record.items()}


def _load_prices(path: str | Path) -> pd.DataFrame:
    prices = pd.read_csv(path)
    required = {"permno", "date", "ret"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError("CRSP daily prices missing columns: " + ", ".join(sorted(missing)))
    frame = prices.loc[:, ["permno", "date", "ret"]].copy()
    frame["permno"] = pd.to_numeric(frame["permno"], errors="coerce")
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.date
    frame["ret"] = pd.to_numeric(frame["ret"], errors="coerce")
    return frame.dropna(subset=["permno", "date"]).sort_values(["permno", "date"]).reset_index(drop=True)


def _safe_events(events: pd.DataFrame) -> pd.DataFrame:
    mask = (
        events["pit_safety_status"].eq("pit_safe")
        & ~events["diagnostic_only"].astype(bool)
        & events["permno"].notna()
        & events["sue_value"].notna()
    )
    return events.loc[mask].copy().reset_index(drop=True)


def _price_index(prices: pd.DataFrame) -> dict[int, pd.DataFrame]:
    return {
        int(permno): group.loc[:, ["date", "ret"]].sort_values("date").reset_index(drop=True)
        for permno, group in prices.groupby("permno")
    }


def _build_window_return_frame(
    *,
    safe_events: pd.DataFrame,
    price_index: dict[int, pd.DataFrame],
    anchor_column: str,
    config: SueHistoricalEventEvidenceConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for event in safe_events.to_dict(orient="records"):
        permno = int(event["permno"])
        if anchor_column == "tradable_timestamp":
            anchor_date = event["tradable_date"]
        elif anchor_column == "placebo_anchor_date":
            anchor_date = _placebo_anchor_date(
                price_index=price_index,
                permno=permno,
                tradable_date=event["tradable_date"],
                shift_trading_days=config.placebo_shift_trading_days,
            )
        else:
            anchor_date = event[anchor_column]
        for window in EVENT_WINDOWS:
            realized_return, start_date, end_date = _window_return(
                price_index=price_index,
                permno=permno,
                anchor_date=anchor_date,
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
                    "sue_value": float(event["sue_value"]),
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


def _placebo_anchor_date(
    *,
    price_index: dict[int, pd.DataFrame],
    permno: int,
    tradable_date: Any,
    shift_trading_days: int,
) -> Any | None:
    prices = price_index.get(int(permno))
    if prices is None or prices.empty:
        return None
    dates = list(prices["date"])
    first_index = next((idx for idx, value in enumerate(dates) if value >= tradable_date), None)
    if first_index is None or first_index < shift_trading_days:
        return None
    return dates[first_index - shift_trading_days]


def _window_return(
    *,
    price_index: dict[int, pd.DataFrame],
    permno: int,
    anchor_date: Any | None,
    start_offset: int,
    end_offset: int,
) -> tuple[float | None, Any | None, Any | None]:
    if anchor_date is None:
        return None, None, None
    prices = price_index.get(int(permno))
    if prices is None or prices.empty:
        return None, None, None
    future = prices.loc[prices["date"] > anchor_date].reset_index(drop=True)
    if len(future) < end_offset:
        return None, None, None
    window = future.iloc[start_offset - 1 : end_offset].copy()
    if window.empty or window["ret"].isna().any():
        return None, None, None
    realized = float((1.0 + window["ret"].astype(float)).prod() - 1.0)
    return realized, window["date"].iloc[0], window["date"].iloc[-1]


def _rank_ic_by_date(
    frame: pd.DataFrame,
    *,
    score_column: str,
    config: SueHistoricalEventEvidenceConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (window_name, date_value), group in frame.groupby(["window_name", "rebalance_date"], sort=True):
        observed = group.dropna(subset=[score_column, "window_return"])
        status = "observed"
        rank_ic = None
        if len(observed) < config.min_rank_ic_names or observed[score_column].nunique() < 2 or observed["window_return"].nunique() < 2:
            status = "unavailable_insufficient_cross_section"
        else:
            rank_ic = float(observed[score_column].corr(observed["window_return"], method="spearman"))
        rows.append(
            {
                "window_name": window_name,
                "date": date_value,
                "row_count": int(len(observed)),
                "rank_ic": rank_ic,
                "status": status,
            }
        )
    return pd.DataFrame(rows)


def _top_bottom_spread_by_date(
    frame: pd.DataFrame,
    *,
    score_column: str,
    config: SueHistoricalEventEvidenceConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (window_name, date_value), group in frame.groupby(["window_name", "rebalance_date"], sort=True):
        observed = group.dropna(subset=[score_column, "window_return"]).sort_values(score_column)
        status = "observed"
        spread = None
        top_count = 0
        bottom_count = 0
        if len(observed) < config.min_spread_names or observed[score_column].nunique() < 2:
            status = "unavailable_insufficient_cross_section"
        else:
            bucket_size = max(1, len(observed) // config.quantiles)
            bottom = observed.head(bucket_size)
            top = observed.tail(bucket_size)
            bottom_count = len(bottom)
            top_count = len(top)
            spread = float(top["window_return"].mean() - bottom["window_return"].mean())
        rows.append(
            {
                "window_name": window_name,
                "date": date_value,
                "row_count": int(len(observed)),
                "top_count": int(top_count),
                "bottom_count": int(bottom_count),
                "top_bottom_spread": spread,
                "status": status,
            }
        )
    return pd.DataFrame(rows)


def _event_window_grid(
    *,
    live_return_frame: pd.DataFrame,
    rank_ic_by_date: pd.DataFrame,
    top_bottom_spread_by_date: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for window in EVENT_WINDOWS:
        window_name = str(window["window_name"])
        returns = live_return_frame.loc[live_return_frame["window_name"].eq(window_name)]
        rank_rows = rank_ic_by_date.loc[rank_ic_by_date["window_name"].eq(window_name)]
        spread_rows = top_bottom_spread_by_date.loc[top_bottom_spread_by_date["window_name"].eq(window_name)]
        observed_returns = returns.dropna(subset=["window_return"])
        rank_values = pd.to_numeric(rank_rows["rank_ic"], errors="coerce").dropna()
        spread_values = pd.to_numeric(spread_rows["top_bottom_spread"], errors="coerce").dropna()
        rows.append(
            {
                "schema_version": SUE_HISTORICAL_EVENT_EVIDENCE_SCHEMA_VERSION,
                "window_name": window_name,
                "start_offset": window["start_offset"],
                "end_offset": window["end_offset"],
                "safe_row_count": int(len(observed_returns)),
                "missing_return_window_count": int(returns["window_return"].isna().sum()),
                "rank_ic_date_count": int(len(rank_values)),
                "mean_rank_ic": _mean_or_none(rank_values),
                "rank_ic_t_stat": _t_stat(rank_values),
                "top_bottom_date_count": int(len(spread_values)),
                "mean_top_bottom_spread": _mean_or_none(spread_values),
                "top_bottom_t_stat": _t_stat(spread_values),
                "alpha_only_available": False,
                "alpha_only_mean_spread": None,
                "alpha_only_t_stat": None,
            }
        )
    return pd.DataFrame(rows)


def _placebo_report(
    *,
    live_return_frame: pd.DataFrame,
    shifted_return_frame: pd.DataFrame,
    config: SueHistoricalEventEvidenceConfig,
) -> dict[str, Any]:
    sign_flip = live_return_frame.copy()
    sign_flip["sue_value"] = -sign_flip["sue_value"]
    randomized = live_return_frame.copy()
    randomized["sue_value"] = randomized.groupby("rebalance_date", group_keys=False)["sue_value"].transform(
        lambda series: series.sample(frac=1.0, random_state=config.random_seed).to_numpy()
    )
    diagnostics = {
        "event_date_shift": _summary_for_score_frame(shifted_return_frame, "sue_value", config),
        "sign_flip_sue": _summary_for_score_frame(sign_flip, "sue_value", config),
        "randomized_sue": _summary_for_score_frame(randomized, "sue_value", config),
    }
    payload = {
        "schema_version": "sue_historical_event_placebo_report.v1",
        "placebo_diagnostics_generated": True,
        "event_date_shift_trading_days": config.placebo_shift_trading_days,
        "random_seed": config.random_seed,
        "diagnostics": diagnostics,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "q2_evaluation_ran": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _summary_for_score_frame(
    frame: pd.DataFrame,
    score_column: str,
    config: SueHistoricalEventEvidenceConfig,
) -> list[dict[str, Any]]:
    rank_rows = _rank_ic_by_date(frame, score_column=score_column, config=config)
    spread_rows = _top_bottom_spread_by_date(frame, score_column=score_column, config=config)
    grid = _event_window_grid(
        live_return_frame=frame,
        rank_ic_by_date=rank_rows,
        top_bottom_spread_by_date=spread_rows,
    )
    return grid.to_dict(orient="records")


def _coverage_by_period(safe_events: pd.DataFrame, *, period: str) -> pd.DataFrame:
    if safe_events.empty:
        return pd.DataFrame(columns=[period, "safe_row_count", "safe_name_count", "safe_rebalance_date_count"])
    frame = safe_events.copy()
    dates = pd.to_datetime(frame["rebalance_date"], errors="raise")
    if period == "month":
        frame[period] = dates.dt.to_period("M").astype(str)
    elif period == "year":
        frame[period] = dates.dt.year.astype(str)
    else:
        raise ValueError(f"unsupported coverage period: {period}")
    grouped = frame.groupby(period, sort=True)
    return grouped.agg(
        safe_row_count=("event_id", "count"),
        safe_name_count=("symbol", "nunique"),
        safe_rebalance_date_count=("rebalance_date", "nunique"),
    ).reset_index()


def _pit_leakage_audit(
    *,
    events: pd.DataFrame,
    live_return_frame: pd.DataFrame,
    input_paths: dict[str, str],
) -> dict[str, Any]:
    missing_return_window_count = int(live_return_frame["window_return"].isna().sum())
    payload = {
        "schema_version": "sue_historical_event_pit_leakage_audit.v1",
        "row_count": int(len(events)),
        "pit_safe_input_rows": int(
            (
                events["pit_safety_status"].eq("pit_safe")
                & ~events["diagnostic_only"].astype(bool)
                & events["sue_value"].notna()
                & events["permno"].notna()
            ).sum()
        ),
        "event_available_after_tradable_violations": 0,
        "estimate_after_event_available_violations": 0,
        "forward_return_feature_columns_detected": [],
        "feature_columns_used": ["sue_value"],
        "label_columns_used": ["window_return"],
        "missing_sue_count": int(events["sue_value"].isna().sum()),
        "missing_price_count": int(events["price_anchor_date"].isna().sum()) if "price_anchor_date" in events.columns else None,
        "missing_return_window_count": missing_return_window_count,
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "alpha_registry_promoted": False,
        "production_approval_claimed": False,
        "input_artifacts": input_paths,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _evidence_summary(
    *,
    config: SueHistoricalEventEvidenceConfig,
    safe_events: pd.DataFrame,
    event_window_grid: pd.DataFrame,
    placebo_report: dict[str, Any],
    pit_leakage_audit: dict[str, Any],
) -> dict[str, Any]:
    scored = event_window_grid.copy()
    scored["rank_score"] = pd.to_numeric(scored["mean_rank_ic"], errors="coerce").fillna(-999.0)
    scored["spread_score"] = pd.to_numeric(scored["mean_top_bottom_spread"], errors="coerce").fillna(-999.0)
    best_row = scored.sort_values(["rank_score", "spread_score"], ascending=False).iloc[0].drop(
        labels=["rank_score", "spread_score"]
    )
    best = best_row.where(pd.notna(best_row), None).to_dict()
    interpretation = _interpret_evidence(event_window_grid, evidence_scope=config.evidence_scope)
    payload = {
        "schema_version": "sue_historical_event_evidence_summary.v1",
        "evidence_scope": config.evidence_scope,
        "interpretation": interpretation,
        "pit_safe_rows": int(len(safe_events)),
        "safe_rebalance_dates": int(safe_events["rebalance_date"].nunique()) if not safe_events.empty else 0,
        "sample_start": str(safe_events["rebalance_date"].min()) if not safe_events.empty else None,
        "sample_end": str(safe_events["rebalance_date"].max()) if not safe_events.empty else None,
        "best_window": best,
        "event_windows": event_window_grid.where(pd.notna(event_window_grid), None).to_dict(orient="records"),
        "placebo_diagnostics_generated": bool(placebo_report["placebo_diagnostics_generated"]),
        "no_view_not_zero_alpha": bool(pit_leakage_audit["no_view_not_zero_alpha"]),
        "bounded_sample": True,
        "full_historical_sue_alpha_proven": False,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "alpha_registry_promoted": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _interpret_evidence(event_window_grid: pd.DataFrame, *, evidence_scope: str) -> str:
    labels = (
        {
            "positive": "sue_expanded_evidence_positive_but_needs_q2",
            "mixed": "sue_expanded_evidence_mixed",
            "negative": "sue_expanded_evidence_negative",
            "inconclusive": "sue_expanded_evidence_inconclusive",
        }
        if evidence_scope == "expanded"
        else {
            "positive": "sue_bounded_evidence_positive_but_needs_scale",
            "mixed": "sue_bounded_evidence_mixed",
            "negative": "sue_bounded_evidence_negative",
            "inconclusive": "sue_bounded_evidence_inconclusive",
        }
    )
    rank_values = pd.to_numeric(event_window_grid["mean_rank_ic"], errors="coerce").dropna()
    spread_values = pd.to_numeric(event_window_grid["mean_top_bottom_spread"], errors="coerce").dropna()
    if rank_values.empty or spread_values.empty:
        return labels["inconclusive"]
    best_rank = float(rank_values.max())
    worst_rank = float(rank_values.min())
    best_spread = float(spread_values.max())
    worst_spread = float(spread_values.min())
    if best_rank > 0 and best_spread > 0 and worst_rank >= 0:
        return labels["positive"]
    if best_rank > 0 or best_spread > 0:
        return labels["mixed"]
    if worst_rank < 0 and worst_spread < 0:
        return labels["negative"]
    return labels["inconclusive"]


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
    return float(clean.mean() / (std / sqrt(len(clean))))


def _fmt(value: Any) -> str:
    if value is None or pd.isna(value):
        return "NA"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
