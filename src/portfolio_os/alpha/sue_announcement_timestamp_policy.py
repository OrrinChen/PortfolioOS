"""SUE announcement timestamp source and anchor-policy audit.

Reopen-H1E.4 checks whether an earlier SUE event anchor is supported by an
auditable actual-EPS availability source. It does not infer tradability from a
strong shifted placebo window, select a score, run Q2, invoke optimizers,
promote Alpha Registry state, open paper/live/broker/order workflows, or
approve production use.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.alpha.sue_historical_schema import (
    SUE_HISTORICAL_EVENT_COLUMNS,
    SueHistoricalEventRow,
    validate_no_forward_return_feature_columns,
)
from portfolio_os.alpha.sue_score_definition_gate import (
    SueScoreDefinitionGateConfig,
    build_sue_score_definition_gate,
    write_sue_score_definition_gate_artifacts,
)
from portfolio_os.provenance.hashing import canonical_json, hash_payload


SUE_ANNOUNCEMENT_TIMESTAMP_POLICY_SCHEMA_VERSION = "sue_announcement_timestamp_policy.v1"
DEFAULT_EVENTS_PATH = "outputs/sue_historical_event_panel_expanded/events.csv"
DEFAULT_CRSP_DAILY_PATH = "data/cache/wrds_sue_event_panel/crsp_daily.csv"
DEFAULT_OUTPUT_DIR = "outputs/sue_announcement_timestamp_policy"
DEFAULT_REPORT_PATH = "reports/sue_announcement_timestamp_policy_report.md"

CANDIDATE_ANCHOR_POLICIES = (
    "current_policy",
    "conservative_date_only_next_open",
    "after_close_next_open",
    "before_open_same_day_or_next_open",
    "source_repaired_announcement_timestamp",
    "blocked_if_no_auditable_timestamp",
)

MISLEADING_TIMESTAMP_POLICY_CLAIMS = (
    "production approved",
    "paper ready",
    "paper-ready",
    "live-ready",
    "live ready",
    "live trading",
    "broker execution",
    "order generation",
    "selected production score",
    "real historical sue alpha proven",
    "historical sue alpha proven",
    "sue alpha is proven",
    "guaranteed tradable alpha",
    "auto trading",
    "investment recommendation",
)


class SueAnnouncementTimestampPolicyConfig(BaseModel):
    """Config for Reopen-H1E.4 announcement timestamp policy audit."""

    model_config = ConfigDict(extra="forbid")

    events_path: str = DEFAULT_EVENTS_PATH
    crsp_daily_path: str = DEFAULT_CRSP_DAILY_PATH
    output_dir: str = DEFAULT_OUTPUT_DIR
    report_path: str = DEFAULT_REPORT_PATH
    after_close_utc_hour: int = Field(default=21, ge=0, le=23)
    before_open_utc_hour: int = Field(default=14, ge=0, le=23)
    market_open_utc_hour: int = Field(default=14, ge=0, le=23)
    market_open_utc_minute: int = Field(default=30, ge=0, le=59)
    source_conflict_max_days: int = Field(default=1, ge=0)
    min_repaired_events_for_h1e: int = Field(default=30, ge=1)
    rerun_h1e_if_repairable: bool = True
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


@dataclass(frozen=True)
class SueAnnouncementTimestampPolicyResult:
    """In-memory H1E.4 timestamp policy audit result."""

    config: SueAnnouncementTimestampPolicyConfig
    timestamp_source_comparison: pd.DataFrame
    timing_repair_eligibility: pd.DataFrame
    anchor_policy_grid: pd.DataFrame
    repaired_h1e_summary: dict[str, Any]
    timing_policy_decision: dict[str, Any]
    report_text: str


def load_sue_announcement_timestamp_policy_config(path: str | Path) -> SueAnnouncementTimestampPolicyConfig:
    """Load H1E.4 timestamp policy config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    guards = payload.get("guards") or {}
    return SueAnnouncementTimestampPolicyConfig(
        events_path=str(inputs.get("events_path") or payload.get("events_path") or DEFAULT_EVENTS_PATH),
        crsp_daily_path=str(inputs.get("crsp_daily_path") or payload.get("crsp_daily_path") or DEFAULT_CRSP_DAILY_PATH),
        output_dir=str(outputs.get("output_dir") or payload.get("output_dir") or DEFAULT_OUTPUT_DIR),
        report_path=str(outputs.get("report_path") or payload.get("report_path") or DEFAULT_REPORT_PATH),
        after_close_utc_hour=int(payload.get("after_close_utc_hour", 21)),
        before_open_utc_hour=int(payload.get("before_open_utc_hour", 14)),
        market_open_utc_hour=int(payload.get("market_open_utc_hour", 14)),
        market_open_utc_minute=int(payload.get("market_open_utc_minute", 30)),
        source_conflict_max_days=int(guards.get("source_conflict_max_days", payload.get("source_conflict_max_days", 1))),
        min_repaired_events_for_h1e=int(
            guards.get("min_repaired_events_for_h1e", payload.get("min_repaired_events_for_h1e", 30))
        ),
        rerun_h1e_if_repairable=bool(payload.get("rerun_h1e_if_repairable", True)),
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
    )


def build_sue_announcement_timestamp_policy_audit(
    config: SueAnnouncementTimestampPolicyConfig | None = None,
) -> SueAnnouncementTimestampPolicyResult:
    """Build H1E.4 timestamp-source and anchor-policy diagnostics."""

    resolved = config or SueAnnouncementTimestampPolicyConfig()
    events = _load_events_with_optional_sources(resolved.events_path)
    comparison = _timestamp_source_comparison(events, resolved)
    eligibility = _timing_repair_eligibility(comparison, resolved)
    policy_grid = _anchor_policy_grid(eligibility)
    repaired_summary = _repaired_h1e_summary(events=events, eligibility=eligibility, config=resolved)
    decision = _timing_policy_decision(
        comparison=comparison,
        eligibility=eligibility,
        policy_grid=policy_grid,
        repaired_h1e_summary=repaired_summary,
        config=resolved,
    )
    report_text = render_sue_announcement_timestamp_policy_report(
        timing_policy_decision=decision,
        anchor_policy_grid=policy_grid,
        repaired_h1e_summary=repaired_summary,
    )
    validate_sue_announcement_timestamp_policy_report_language(report_text)
    return SueAnnouncementTimestampPolicyResult(
        config=resolved,
        timestamp_source_comparison=comparison,
        timing_repair_eligibility=eligibility,
        anchor_policy_grid=policy_grid,
        repaired_h1e_summary=repaired_summary,
        timing_policy_decision=decision,
        report_text=report_text,
    )


def write_sue_announcement_timestamp_policy_artifacts(
    result: SueAnnouncementTimestampPolicyResult,
) -> dict[str, Path]:
    """Write H1E.4 timestamp policy artifacts."""

    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(result.config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    paths = {
        "timestamp_source_comparison": output_dir / "timestamp_source_comparison.csv",
        "timing_repair_eligibility": output_dir / "timing_repair_eligibility.csv",
        "anchor_policy_grid": output_dir / "anchor_policy_grid.csv",
        "repaired_h1e_summary": output_dir / "repaired_h1e_summary.json",
        "timing_policy_decision": output_dir / "timing_policy_decision.json",
        "report": report_path,
    }
    result.timestamp_source_comparison.to_csv(paths["timestamp_source_comparison"], index=False)
    result.timing_repair_eligibility.to_csv(paths["timing_repair_eligibility"], index=False)
    result.anchor_policy_grid.to_csv(paths["anchor_policy_grid"], index=False)
    _write_json(paths["repaired_h1e_summary"], result.repaired_h1e_summary)
    _write_json(paths["timing_policy_decision"], result.timing_policy_decision)
    validate_sue_announcement_timestamp_policy_report_language(result.report_text)
    paths["report"].write_text(result.report_text, encoding="utf-8")
    return paths


def render_sue_announcement_timestamp_policy_report(
    *,
    timing_policy_decision: dict[str, Any],
    anchor_policy_grid: pd.DataFrame,
    repaired_h1e_summary: dict[str, Any],
) -> str:
    """Render H1E.4 announcement timestamp policy report."""

    lines = [
        "# SUE Announcement Timestamp Source / Anchor Policy Audit",
        "",
        "-5/-10 windows cannot be used as tradable SUE unless actual EPS availability is proven earlier.",
        "If no earlier timestamp source exists, SUE remains blocked before typed projection/Q2.",
        "This phase does not approve paper/live/broker/order/production workflows.",
        "It does not select a production SUE score.",
        "It does not run Q2 or optimizer-path evaluation.",
        "",
        "## Decision",
        "",
        f"- schema_version: `{timing_policy_decision['schema_version']}`",
        f"- decision_label: `{timing_policy_decision['decision_label']}`",
        f"- event_count: `{timing_policy_decision['event_count']}`",
        f"- repaired_event_count: `{timing_policy_decision['repaired_event_count']}`",
        f"- blind_shift_policy_allowed: `{timing_policy_decision['blind_shift_policy_allowed']}`",
        f"- selected_score: `{timing_policy_decision['selected_score']}`",
        f"- q2_evaluation_ran: `{timing_policy_decision['q2_evaluation_ran']}`",
        f"- optimizer_path_evaluation_ran: `{timing_policy_decision['optimizer_path_evaluation_ran']}`",
        f"- production_approval_claimed: `{timing_policy_decision['production_approval_claimed']}`",
        "",
        "## Anchor Policies",
        "",
        "| Policy | Eligible Events | Description |",
        "| --- | ---: | --- |",
    ]
    for row in anchor_policy_grid.to_dict(orient="records"):
        lines.append(
            f"| `{row['candidate_anchor_policy']}` | {int(row['eligible_event_count'])} | "
            f"{row['policy_description']} |"
        )
    lines.extend(
        [
            "",
            "## H1E Rerun Status",
            "",
            f"- rerun_attempted: `{repaired_h1e_summary['rerun_attempted']}`",
            f"- rerun_required: `{repaired_h1e_summary['rerun_required']}`",
            f"- blocked_reason: `{repaired_h1e_summary.get('blocked_reason')}`",
            f"- h1e_selected_score: `{repaired_h1e_summary.get('selected_score')}`",
            f"- h1e_interpretation: `{repaired_h1e_summary.get('interpretation')}`",
            "",
            "## Boundaries",
            "",
            "- A shifted placebo result is not an auditable timestamp source.",
            "- A repaired policy must keep tradability after actual EPS public availability.",
            "- No Alpha Registry promotion is performed by this audit.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_sue_announcement_timestamp_policy_report_language(text: str) -> None:
    """Reject misleading H1E.4 claims while allowing explicit non-claims."""

    scrubbed = str(text).lower()
    allowed_phrases = [
        "this phase does not approve paper/live/broker/order/production workflows.",
        "it does not select a production sue score.",
        "it does not run q2 or optimizer-path evaluation.",
        "no alpha registry promotion is performed by this audit.",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in MISLEADING_TIMESTAMP_POLICY_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE announcement timestamp policy claim detected: {claim}")


def _load_events_with_optional_sources(path: str | Path) -> pd.DataFrame:
    raw = pd.read_csv(path)
    validate_no_forward_return_feature_columns(list(raw.columns))
    missing = set(SUE_HISTORICAL_EVENT_COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError("SUE events missing required columns: " + ", ".join(sorted(missing)))
    validated = []
    for record in raw.loc[:, SUE_HISTORICAL_EVENT_COLUMNS].to_dict(orient="records"):
        clean = {key: (None if pd.isna(value) else value) for key, value in record.items()}
        for text_field in ["event_id", "symbol", "ibes_ticker", "cusip", "fiscal_period", "sue_definition", "data_source", "link_method", "pit_safety_status"]:
            if clean.get(text_field) is not None:
                clean[text_field] = str(clean[text_field])
        validated.append(SueHistoricalEventRow.model_validate(clean).model_dump(mode="json"))
    frame = pd.DataFrame(validated)
    for column in raw.columns:
        if column not in frame.columns:
            frame[column] = raw[column].values
    return frame


def _timestamp_source_comparison(
    events: pd.DataFrame,
    config: SueAnnouncementTimestampPolicyConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for event in events.to_dict(orient="records"):
        current_available = _to_utc_timestamp(event["event_available_timestamp"])
        current_tradable = _to_utc_timestamp(event["tradable_timestamp"])
        candidates = _source_candidates(event)
        candidates = [candidate for candidate in candidates if candidate["timestamp"] is not None]
        earliest = min(candidates, key=lambda item: item["timestamp"]) if candidates else None
        source_timestamps = [item["timestamp"] for item in candidates]
        conflict_days = _source_conflict_days(source_timestamps)
        rows.append(
            {
                "schema_version": "sue_timestamp_source_comparison.v1",
                "event_id": event["event_id"],
                "symbol": event["symbol"],
                "announcement_date": event["announcement_date"],
                "current_ibes_announcement_date": event["announcement_date"],
                "current_event_available_timestamp": current_available.isoformat(),
                "current_tradable_timestamp": current_tradable.isoformat(),
                "actual_eps_source_timestamp": _source_value(candidates, "actual_eps_source"),
                "report_timestamp": _source_value(candidates, "report_date"),
                "compustat_rdq_timestamp": _source_value(candidates, "compustat_rdq"),
                "announcement_time_timestamp": _source_value(candidates, "announcement_time"),
                "source_count": len(candidates),
                "source_conflict_days": conflict_days,
                "earliest_auditable_source_name": earliest["source_name"] if earliest else None,
                "earliest_auditable_source_timestamp": earliest["timestamp"].isoformat() if earliest else None,
                "earliest_auditable_source_precision": earliest["precision"] if earliest else None,
                "earliest_auditable_source_table": earliest["source_table"] if earliest else None,
                "earliest_source_before_current_available": bool(
                    earliest is not None and earliest["timestamp"] < current_available
                ),
                "source_status": _source_status(candidates, conflict_days, config),
            }
        )
    return pd.DataFrame(rows)


def _timing_repair_eligibility(
    comparison: pd.DataFrame,
    config: SueAnnouncementTimestampPolicyConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in comparison.to_dict(orient="records"):
        current_available = _to_utc_timestamp(row["current_event_available_timestamp"])
        current_tradable = _to_utc_timestamp(row["current_tradable_timestamp"])
        source_timestamp = _optional_utc_timestamp(row.get("earliest_auditable_source_timestamp"))
        precision = row.get("earliest_auditable_source_precision")
        source_status = str(row.get("source_status"))
        repaired_tradable = _tradable_from_source(source_timestamp, precision, config) if source_timestamp else None
        timing_classification = _timing_classification(
            source_timestamp=source_timestamp,
            source_status=source_status,
            precision=precision,
            current_available=current_available,
            current_tradable=current_tradable,
            repaired_tradable=repaired_tradable,
        )
        repair_eligible = bool(
            source_timestamp is not None
            and repaired_tradable is not None
            and source_timestamp < current_available
            and repaired_tradable < current_tradable
            and repaired_tradable >= source_timestamp
            and timing_classification in {"likely_late_vendor_date", "date_only_conservative"}
        )
        rows.append(
            {
                "schema_version": "sue_timing_repair_eligibility.v1",
                "event_id": row["event_id"],
                "symbol": row["symbol"],
                "timing_classification": timing_classification,
                "repair_eligible": repair_eligible,
                "selected_anchor_policy": (
                    "source_repaired_announcement_timestamp" if repair_eligible else "blocked_if_no_auditable_timestamp"
                ),
                "current_event_available_timestamp": row["current_event_available_timestamp"],
                "current_tradable_timestamp": row["current_tradable_timestamp"],
                "earliest_auditable_source_name": row.get("earliest_auditable_source_name"),
                "earliest_auditable_source_timestamp": (
                    source_timestamp.isoformat() if source_timestamp is not None else None
                ),
                "repaired_event_available_timestamp": source_timestamp.isoformat() if repair_eligible else None,
                "repaired_tradable_timestamp": repaired_tradable.isoformat() if repair_eligible else None,
                "tradable_before_actual_eps_public": bool(
                    repaired_tradable is not None and source_timestamp is not None and repaired_tradable < source_timestamp
                ),
                "blind_shift_policy_allowed": False,
            }
        )
    return pd.DataFrame(rows)


def _anchor_policy_grid(eligibility: pd.DataFrame) -> pd.DataFrame:
    counts = {
        "current_policy": len(eligibility),
        "conservative_date_only_next_open": int(eligibility["timing_classification"].eq("date_only_conservative").sum()),
        "after_close_next_open": 0,
        "before_open_same_day_or_next_open": int(eligibility["repair_eligible"].sum()),
        "source_repaired_announcement_timestamp": int(eligibility["repair_eligible"].sum()),
        "blocked_if_no_auditable_timestamp": int((~eligibility["repair_eligible"].astype(bool)).sum()),
    }
    descriptions = {
        "current_policy": "Use the existing IBES/WRDS event_available_timestamp and next tradable timestamp.",
        "conservative_date_only_next_open": "Use date-only source dates only at next market open.",
        "after_close_next_open": "Use after-close source timestamps at next market open.",
        "before_open_same_day_or_next_open": "Use before-open source timestamps at same-day open when still after source visibility.",
        "source_repaired_announcement_timestamp": "Use an earlier auditable source timestamp, never a blind shifted placebo.",
        "blocked_if_no_auditable_timestamp": "Block timing repair when no earlier auditable actual-EPS source exists.",
    }
    return pd.DataFrame(
        [
            {
                "schema_version": "sue_anchor_policy_grid.v1",
                "candidate_anchor_policy": policy,
                "eligible_event_count": counts[policy],
                "policy_description": descriptions[policy],
            }
            for policy in CANDIDATE_ANCHOR_POLICIES
        ]
    )


def _repaired_h1e_summary(
    *,
    events: pd.DataFrame,
    eligibility: pd.DataFrame,
    config: SueAnnouncementTimestampPolicyConfig,
) -> dict[str, Any]:
    repaired_count = int(eligibility["repair_eligible"].sum()) if not eligibility.empty else 0
    base = {
        "schema_version": "sue_repaired_h1e_summary.v1",
        "rerun_attempted": False,
        "rerun_required": False,
        "repaired_event_count": repaired_count,
        "selected_score": None,
        "interpretation": None,
        "score_selection_ran": False,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
    }
    if repaired_count == 0:
        base["blocked_reason"] = "no_auditable_earlier_timestamp_source"
        base["content_hash"] = hash_payload(base)
        return base
    if not config.rerun_h1e_if_repairable or repaired_count < config.min_repaired_events_for_h1e:
        base["rerun_required"] = True
        base["blocked_reason"] = "repaired_event_count_below_h1e_rerun_threshold"
        base["content_hash"] = hash_payload(base)
        return base
    output_dir = Path(config.output_dir)
    repaired_events_path = output_dir / "repaired_h1e_events.csv"
    repaired_events = _build_repaired_events(events=events, eligibility=eligibility)
    repaired_events_path.parent.mkdir(parents=True, exist_ok=True)
    repaired_events.to_csv(repaired_events_path, index=False)
    gate = build_sue_score_definition_gate(
        SueScoreDefinitionGateConfig(
            events_path=str(repaired_events_path),
            crsp_daily_path=config.crsp_daily_path,
            output_dir=str(output_dir / "repaired_h1e"),
            report_path=str(output_dir / "repaired_h1e_report.md"),
            quantiles=config.quantiles,
            min_rank_ic_names=config.min_rank_ic_names,
            min_spread_names=config.min_spread_names,
            denominator_abs_min=config.denominator_abs_min,
            winsorization_scope=config.winsorization_scope,
            winsor_lower_quantile=config.winsor_lower_quantile,
            winsor_upper_quantile=config.winsor_upper_quantile,
            extreme_value_cap=config.extreme_value_cap,
            placebo_shift_trading_days=config.placebo_shift_trading_days,
            random_seed=config.random_seed,
        )
    )
    write_sue_score_definition_gate_artifacts(gate)
    base.update(
        {
            "rerun_attempted": True,
            "score_selection_ran": True,
            "blocked_reason": None,
            "selected_score": gate.score_selection_summary.get("selected_score"),
            "interpretation": gate.score_selection_summary.get("interpretation"),
            "provisional_score": gate.score_selection_summary.get("provisional_score"),
            "repaired_events_path": str(repaired_events_path),
        }
    )
    base["content_hash"] = hash_payload(base)
    return base


def _timing_policy_decision(
    *,
    comparison: pd.DataFrame,
    eligibility: pd.DataFrame,
    policy_grid: pd.DataFrame,
    repaired_h1e_summary: dict[str, Any],
    config: SueAnnouncementTimestampPolicyConfig,
) -> dict[str, Any]:
    repaired_count = int(eligibility["repair_eligible"].sum()) if not eligibility.empty else 0
    if repaired_count == 0:
        decision_label = "no_auditable_earlier_timestamp_sue_blocked"
    elif repaired_h1e_summary.get("rerun_required"):
        decision_label = "anchor_policy_repaired_and_h1e_rerun_required"
    elif repaired_h1e_summary.get("rerun_attempted") and repaired_h1e_summary.get("selected_score") is None:
        decision_label = "anchor_policy_repaired_but_sue_still_mixed"
    elif repaired_h1e_summary.get("rerun_attempted"):
        decision_label = "anchor_policy_repaired_and_h1e_rerun_required"
    else:
        decision_label = "timestamp_policy_inconclusive"
    payload = {
        "schema_version": SUE_ANNOUNCEMENT_TIMESTAMP_POLICY_SCHEMA_VERSION,
        "decision_label": decision_label,
        "event_count": int(len(comparison)),
        "auditable_source_event_count": int(comparison["source_count"].gt(0).sum()) if not comparison.empty else 0,
        "repaired_event_count": repaired_count,
        "candidate_policy_count": int(len(policy_grid)),
        "selected_score": None,
        "score_selection_ran": bool(repaired_h1e_summary.get("score_selection_ran", False)),
        "h1e_rerun_attempted": bool(repaired_h1e_summary.get("rerun_attempted", False)),
        "blind_shift_policy_allowed": False,
        "minus_5_minus_10_windows_allowed_without_source": False,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "alpha_registry_promoted": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
        "factor_discovery_implementation_added": False,
        "min_repaired_events_for_h1e": config.min_repaired_events_for_h1e,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _source_candidates(event: dict[str, Any]) -> list[dict[str, Any]]:
    specs = [
        ("actual_eps_source", ["actual_eps_source_timestamp", "actual_eps_available_timestamp", "actual_eps_public_timestamp"], ["actual_eps_source_date", "actual_eps_date"]),
        ("report_date", ["report_timestamp"], ["report_date"]),
        ("compustat_rdq", ["compustat_rdq_timestamp"], ["compustat_rdq", "rdq"]),
        ("announcement_time", ["announcement_timestamp"], []),
    ]
    candidates: list[dict[str, Any]] = []
    for source_name, timestamp_columns, date_columns in specs:
        for column in timestamp_columns:
            timestamp, precision = _parse_source_value(event.get(column))
            if timestamp is not None:
                candidates.append(_candidate(source_name, column, timestamp, precision, event))
        for column in date_columns:
            timestamp, precision = _parse_source_value(event.get(column), force_date_only=True)
            if timestamp is not None:
                candidates.append(_candidate(source_name, column, timestamp, precision, event))
    time_value = event.get("announcement_time") or event.get("anntims") or event.get("announce_time")
    if pd.notna(time_value) and str(time_value).strip():
        combined = f"{event.get('announcement_date')}T{str(time_value).strip()}"
        timestamp, precision = _parse_source_value(combined)
        if timestamp is not None:
            candidates.append(_candidate("announcement_time", "announcement_time", timestamp, precision, event))
    return candidates


def _candidate(
    source_name: str,
    column: str,
    timestamp: pd.Timestamp,
    precision: str,
    event: dict[str, Any],
) -> dict[str, Any]:
    return {
        "source_name": source_name,
        "column": column,
        "timestamp": timestamp,
        "precision": precision,
        "source_table": event.get(f"{source_name}_table") or event.get("actual_eps_source_table") or event.get("data_source"),
    }


def _parse_source_value(value: Any, *, force_date_only: bool = False) -> tuple[pd.Timestamp | None, str | None]:
    if value is None or pd.isna(value) or not str(value).strip():
        return None, None
    text = str(value).strip()
    timestamp = pd.Timestamp(text)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone.utc)
    else:
        timestamp = timestamp.tz_convert(timezone.utc)
    if force_date_only or len(text) <= 10 or (timestamp.hour == 0 and timestamp.minute == 0 and timestamp.second == 0):
        return timestamp, "date_only"
    return timestamp, "timestamp_precise"


def _source_value(candidates: list[dict[str, Any]], source_name: str) -> str | None:
    values = [item["timestamp"].isoformat() for item in candidates if item["source_name"] == source_name]
    return min(values) if values else None


def _source_conflict_days(timestamps: list[pd.Timestamp]) -> int:
    if len(timestamps) < 2:
        return 0
    dates = [value.date() for value in timestamps]
    return int((max(dates) - min(dates)).days)


def _source_status(
    candidates: list[dict[str, Any]],
    conflict_days: int,
    config: SueAnnouncementTimestampPolicyConfig,
) -> str:
    if not candidates:
        return "no_auditable_earlier_source"
    if conflict_days > config.source_conflict_max_days:
        return "conflicting_event_dates"
    if all(item["precision"] == "date_only" for item in candidates):
        return "date_only_source"
    return "auditable_timestamp_source"


def _timing_classification(
    *,
    source_timestamp: pd.Timestamp | None,
    source_status: str,
    precision: Any,
    current_available: pd.Timestamp,
    current_tradable: pd.Timestamp,
    repaired_tradable: pd.Timestamp | None,
) -> str:
    if source_status == "conflicting_event_dates":
        return "conflicting_event_dates"
    if source_timestamp is None:
        return "unavailable_for_timing_repair"
    if precision == "date_only":
        return "date_only_conservative"
    if repaired_tradable is not None and source_timestamp < current_available and repaired_tradable < current_tradable:
        return "likely_late_vendor_date"
    return "timestamp_precise"


def _tradable_from_source(
    source_timestamp: pd.Timestamp | None,
    precision: Any,
    config: SueAnnouncementTimestampPolicyConfig,
) -> pd.Timestamp | None:
    if source_timestamp is None:
        return None
    open_time = time(config.market_open_utc_hour, config.market_open_utc_minute)
    if precision == "date_only":
        return _market_open(_next_business_day(source_timestamp.date()), open_time)
    if source_timestamp.hour < config.before_open_utc_hour:
        same_day_open = _market_open(_business_day_on_or_after(source_timestamp.date()), open_time)
        return same_day_open if same_day_open >= source_timestamp else _market_open(_next_business_day(source_timestamp.date()), open_time)
    if source_timestamp.hour >= config.after_close_utc_hour:
        return _market_open(_next_business_day(source_timestamp.date()), open_time)
    return _market_open(_next_business_day(source_timestamp.date()), open_time)


def _build_repaired_events(*, events: pd.DataFrame, eligibility: pd.DataFrame) -> pd.DataFrame:
    eligible = eligibility.loc[eligibility["repair_eligible"].astype(bool)]
    repaired_by_event = {row["event_id"]: row for row in eligible.to_dict(orient="records")}
    rows: list[dict[str, Any]] = []
    for event in events.to_dict(orient="records"):
        repair = repaired_by_event.get(event["event_id"])
        if repair is None:
            continue
        row = {column: event.get(column) for column in SUE_HISTORICAL_EVENT_COLUMNS}
        repaired_available = _to_utc_timestamp(repair["repaired_event_available_timestamp"])
        repaired_tradable = _to_utc_timestamp(repair["repaired_tradable_timestamp"])
        row["event_available_timestamp"] = repaired_available.isoformat().replace("+00:00", "Z")
        row["tradable_timestamp"] = repaired_tradable.isoformat().replace("+00:00", "Z")
        row["rebalance_date"] = repaired_tradable.date().isoformat()
        row["price_anchor_date"] = repaired_tradable.date().isoformat()
        row["return_window_start"] = _business_day_offset(repaired_tradable.date(), 2).isoformat()
        row["return_window_end"] = _business_day_offset(repaired_tradable.date(), 22).isoformat()
        rows.append(row)
    return pd.DataFrame(rows, columns=SUE_HISTORICAL_EVENT_COLUMNS)


def _business_day_on_or_after(value: Any) -> Any:
    return pd.bdate_range(pd.Timestamp(value), periods=1)[0].date()


def _next_business_day(value: Any) -> Any:
    return (pd.Timestamp(value) + pd.offsets.BDay(1)).date()


def _business_day_offset(value: Any, offset: int) -> Any:
    return (pd.Timestamp(value) + pd.offsets.BDay(offset)).date()


def _market_open(value: Any, open_time: time) -> pd.Timestamp:
    return pd.Timestamp.combine(pd.Timestamp(value).date(), open_time).tz_localize(timezone.utc)


def _optional_utc_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None or pd.isna(value) or not str(value).strip():
        return None
    return _to_utc_timestamp(value)


def _to_utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize(timezone.utc)
    return timestamp.tz_convert(timezone.utc)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
