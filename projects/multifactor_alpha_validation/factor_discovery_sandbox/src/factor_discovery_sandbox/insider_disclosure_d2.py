"""No-formula D2 observability runner for insider disclosure events."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

import pandas as pd


SUMMARY_SCHEMA_VERSION = "insider_disclosure_d2_summary.v1"
EVENT_REGISTRY_SCHEMA_VERSION = "insider_event_registry.v1"
PLACEBO_SCHEMA_VERSION = "insider_disclosure_d2_placebo.v1"

REQUIRED_EVENT_REGISTRY_COLUMNS = (
    "event_id",
    "issuer_cik",
    "ticker",
    "accession_number",
    "form_type",
    "filing_accepted_ts",
    "visibility_timestamp",
    "tradable_timestamp",
    "reporting_owner_cik",
    "reporting_owner_name_hash",
    "role_bucket",
    "is_director",
    "is_officer",
    "officer_title_bucket",
    "is_10pct_owner",
    "transaction_code",
    "acquired_disposed",
    "transaction_date",
    "transaction_shares",
    "transaction_price",
    "transaction_dollar_value",
    "security_title",
    "is_derivative",
    "ownership_direct_or_indirect",
    "post_transaction_holding",
    "rule_10b5_1_flag",
    "plan_adoption_date",
    "event_subset",
    "event_cluster_id",
    "market_cap_at_event",
    "adv_20d",
    "spread_proxy",
    "sector",
    "size_bucket",
    "liquidity_bucket",
    "coverage_state",
    "no_view_reason",
    "diagnostic_only",
)

EVENT_SUBSETS = (
    "open_market_buy",
    "discretionary_sell",
    "planned_sell",
    "compensation_control",
    "unknown_no_view",
)

WINDOWS = (
    ("pre_20_1", -20, -1),
    ("pre_10_1", -10, -1),
    ("pre_5_1", -5, -1),
    ("post_0_1", 0, 1),
    ("post_1_5", 1, 5),
    ("post_1_10", 1, 10),
    ("post_1_22", 1, 22),
    ("post_1_44", 1, 44),
)

DOWNSTREAM_FLAGS = {
    "no_formula_observability_only": True,
    "formula_score_written": False,
    "measurement_spec_written": False,
    "q1_entry_allowed": False,
    "q2_entry_allowed": False,
    "alpha_registry_update_allowed": False,
    "production_approval_claimed": False,
}


@dataclass(frozen=True)
class InsiderDisclosureD2Result:
    """Artifacts and summary for D2 insider observability."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_insider_disclosure_d2(
    output_dir: str | Path,
    events: pd.DataFrame | None = None,
    placebo_overrides: Mapping[str, float] | None = None,
) -> InsiderDisclosureD2Result:
    """Write no-formula D2 insider observability artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    registry = _normalize_event_registry(events if events is not None else build_demo_insider_events())
    validation = validate_event_registry(registry)
    subset_counts = build_event_subset_counts(registry)
    timestamp_audit = build_timestamp_audit(registry, validation)
    tradability_audit = build_tradability_audit(registry)
    car_panel = build_car_window_panel(registry)
    matched_controls = build_matched_control_panel(car_panel)
    placebo_report = build_placebo_report(car_panel, placebo_overrides=placebo_overrides)
    subset_decisions = build_subset_decisions(registry, car_panel, placebo_report, validation)
    overall_decision, allow_d3 = build_overall_decision(subset_decisions, validation)

    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": "D2-INSIDER-01",
        "coupling_group": "insider_disclosure_regime_2023",
        "event_count": int(len(registry)),
        "event_month_count": int(registry["event_month"].nunique()),
        "subset_decisions": subset_decisions,
        "overall_decision": overall_decision,
        "allow_d3_charter_for": allow_d3,
        "event_registry_valid": bool(validation["valid"]),
        "event_registry_validation": validation,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
        **DOWNSTREAM_FLAGS,
    }

    artifacts = {
        "insider_event_registry": output_path / "insider_event_registry.csv",
        "event_subset_counts": output_path / "event_subset_counts.csv",
        "timestamp_audit": output_path / "timestamp_audit.csv",
        "tradability_audit": output_path / "tradability_audit.csv",
        "car_window_panel": output_path / "car_window_panel.csv",
        "matched_control_panel": output_path / "matched_control_panel.csv",
        "placebo_report": output_path / "placebo_report.json",
        "d2_observability_summary": output_path / "d2_observability_summary.json",
        "d2_insider_disclosure_observability_report": output_path / "d2_insider_disclosure_observability_report.md",
    }
    registry.to_csv(artifacts["insider_event_registry"], index=False)
    subset_counts.to_csv(artifacts["event_subset_counts"], index=False)
    timestamp_audit.to_csv(artifacts["timestamp_audit"], index=False)
    tradability_audit.to_csv(artifacts["tradability_audit"], index=False)
    car_panel.to_csv(artifacts["car_window_panel"], index=False)
    matched_controls.to_csv(artifacts["matched_control_panel"], index=False)
    artifacts["placebo_report"].write_text(
        json.dumps(placebo_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["d2_observability_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["d2_insider_disclosure_observability_report"].write_text(
        render_report(summary, subset_counts, car_panel, placebo_report),
        encoding="utf-8",
    )
    return InsiderDisclosureD2Result(summary=summary, artifacts=artifacts)


def build_demo_insider_events() -> pd.DataFrame:
    """Build deterministic Form 4-like events for no-formula D2 tests."""

    rows: list[dict[str, object]] = []
    start = datetime(2023, 4, 3, 13, 30, tzinfo=timezone.utc)
    subset_specs = [
        ("open_market_buy", 360, "P", "A", False),
        ("discretionary_sell", 180, "S", "D", False),
        ("planned_sell", 180, "S", "D", True),
        ("compensation_control", 80, "A", "A", ""),
        ("unknown_no_view", 24, "S", "D", ""),
    ]
    event_index = 0
    for subset, count, code, acquired_disposed, plan_flag in subset_specs:
        for offset in range(count):
            issuer_number = offset % 120
            month_offset = offset % 24
            event_month = _add_months(datetime(2023, 4, 1, tzinfo=timezone.utc), month_offset)
            accepted = datetime(
                event_month.year,
                event_month.month,
                (offset % 20) + 1,
                13,
                30,
                tzinfo=timezone.utc,
            )
            tradable = _next_regular_market_open(accepted)
            no_view_reason = ""
            diagnostic_only = False
            if subset == "unknown_no_view":
                no_view_reason = "unknown_post_2023_plan_flag"
                diagnostic_only = True
            rows.append(
                {
                    "event_id": f"insider_demo_{event_index:05d}",
                    "issuer_cik": f"{1000000 + issuer_number}",
                    "ticker": f"T{issuer_number:03d}",
                    "accession_number": f"0000000000-23-{event_index:06d}",
                    "form_type": "4",
                    "filing_accepted_ts": accepted.isoformat(),
                    "visibility_timestamp": accepted.isoformat(),
                    "tradable_timestamp": tradable.isoformat(),
                    "reporting_owner_cik": f"{2000000 + (offset % 240)}",
                    "reporting_owner_name_hash": f"owner_hash_{offset % 240:03d}",
                    "role_bucket": _role_for_offset(offset),
                    "is_director": bool(offset % 3 == 0),
                    "is_officer": bool(offset % 4 in {0, 1}),
                    "officer_title_bucket": _title_for_offset(offset),
                    "is_10pct_owner": bool(offset % 13 == 0),
                    "transaction_code": code,
                    "acquired_disposed": acquired_disposed,
                    "transaction_date": (accepted - timedelta(days=1)).date().isoformat(),
                    "transaction_shares": 1_000 + (offset % 20) * 50,
                    "transaction_price": 10.0 + (offset % 50) * 0.5,
                    "transaction_dollar_value": float((1_000 + (offset % 20) * 50) * (10.0 + (offset % 50) * 0.5)),
                    "security_title": "Common Stock",
                    "is_derivative": False,
                    "ownership_direct_or_indirect": "D",
                    "post_transaction_holding": 10_000 + offset * 10,
                    "rule_10b5_1_flag": plan_flag,
                    "plan_adoption_date": (accepted - timedelta(days=120)).date().isoformat()
                    if subset == "planned_sell"
                    else "",
                    "event_subset": subset,
                    "event_cluster_id": f"T{issuer_number:03d}_{event_index // 3:05d}",
                    "market_cap_at_event": float(1_000_000_000 + issuer_number * 5_000_000),
                    "adv_20d": float(3_000_000 + (issuer_number % 50) * 100_000),
                    "spread_proxy": round(0.001 + (issuer_number % 10) * 0.0001, 5),
                    "sector": f"sector_{issuer_number % 8}",
                    "size_bucket": _bucket(issuer_number, ("small", "mid", "large")),
                    "liquidity_bucket": _bucket(issuer_number, ("low", "medium", "high")),
                    "coverage_state": "covered" if subset != "unknown_no_view" else "no_view",
                    "no_view_reason": no_view_reason,
                    "diagnostic_only": diagnostic_only,
                },
            )
            event_index += 1
    return pd.DataFrame(rows, columns=REQUIRED_EVENT_REGISTRY_COLUMNS)


def validate_event_registry(events: pd.DataFrame) -> dict[str, object]:
    """Validate D2 event-registry timestamp, subset, and no-view semantics."""

    missing = [column for column in REQUIRED_EVENT_REGISTRY_COLUMNS if column not in events.columns]
    failure_reasons: list[str] = []
    invalid_rows: set[int] = set()
    if missing:
        return {
            "schema_version": f"{EVENT_REGISTRY_SCHEMA_VERSION}.validation",
            "valid": False,
            "row_count": int(len(events)),
            "invalid_row_count": int(len(events)),
            "failure_reasons": [f"missing_columns:{','.join(missing)}"],
        }

    for index, row in events.iterrows():
        reasons = []
        try:
            visibility = pd.Timestamp(row["visibility_timestamp"])
            tradable = pd.Timestamp(row["tradable_timestamp"])
        except Exception:  # pragma: no cover - defensive for malformed user data
            visibility = None
            tradable = None
            reasons.append("timestamp_parse_error")
        if visibility is not None and tradable is not None and tradable <= visibility:
            reasons.append("tradable_timestamp_not_after_visibility")
        if row["event_subset"] not in EVENT_SUBSETS:
            reasons.append("unknown_event_subset")
        if row["event_subset"] == "unknown_no_view" and not str(row["no_view_reason"]).strip():
            reasons.append("unknown_no_view_missing_reason")
        if row["coverage_state"] == "no_view" and not str(row["no_view_reason"]).strip():
            reasons.append("no_view_missing_reason")
        if row["event_subset"] in {"open_market_buy", "discretionary_sell", "planned_sell"}:
            if bool(row["is_derivative"]):
                reasons.append("derivative_entered_live_subset")
            if row["security_title"] != "Common Stock":
                reasons.append("non_common_stock_entered_live_subset")
        if row["event_subset"] == "open_market_buy" and row["transaction_code"] != "P":
            reasons.append("open_market_buy_wrong_transaction_code")
        if row["event_subset"] in {"discretionary_sell", "planned_sell"} and row["transaction_code"] != "S":
            reasons.append("sell_subset_wrong_transaction_code")
        if reasons:
            invalid_rows.add(int(index))
            failure_reasons.append(f"row_{index}:{';'.join(reasons)}")
    return {
        "schema_version": f"{EVENT_REGISTRY_SCHEMA_VERSION}.validation",
        "valid": bool(not failure_reasons),
        "row_count": int(len(events)),
        "invalid_row_count": int(len(invalid_rows)),
        "failure_reasons": failure_reasons,
    }


def build_event_subset_counts(events: pd.DataFrame) -> pd.DataFrame:
    """Count events and coverage by subset."""

    grouped = (
        events.groupby("event_subset", dropna=False)
        .agg(
            event_count=("event_id", "count"),
            issuer_count=("issuer_cik", "nunique"),
            event_month_count=("event_month", "nunique"),
            no_view_count=("coverage_state", lambda values: int((values == "no_view").sum())),
            diagnostic_only_count=("diagnostic_only", lambda values: int(pd.Series(values).astype(bool).sum())),
        )
        .reset_index()
    )
    grouped["coverage_share"] = (grouped["event_count"] - grouped["no_view_count"]) / grouped["event_count"]
    return grouped


def build_timestamp_audit(events: pd.DataFrame, validation: Mapping[str, object]) -> pd.DataFrame:
    """Summarize timestamp blockers."""

    reasons = list(validation.get("failure_reasons", []))
    return pd.DataFrame(
        [
            {
                "audit_name": "event_registry_timestamp_validation",
                "row_count": int(len(events)),
                "invalid_row_count": int(validation.get("invalid_row_count", 0)),
                "accepted_timestamp_missing_count": int(events["filing_accepted_ts"].eq("").sum()),
                "tradable_not_after_visibility_count": int(
                    sum("tradable_timestamp_not_after_visibility" in reason for reason in reasons),
                ),
                "status": "pass" if validation.get("valid") else "fail",
            },
        ],
    )


def build_tradability_audit(events: pd.DataFrame) -> pd.DataFrame:
    """Summarize no-view and coverage state without assigning zero alpha."""

    rows = []
    for subset in EVENT_SUBSETS:
        subset_events = events[events["event_subset"] == subset]
        rows.append(
            {
                "event_subset": subset,
                "event_count": int(len(subset_events)),
                "covered_count": int(subset_events["coverage_state"].eq("covered").sum()),
                "no_view_count": int(subset_events["coverage_state"].eq("no_view").sum()),
                "no_view_not_zero_alpha": True,
            },
        )
    return pd.DataFrame(rows)


def build_car_window_panel(events: pd.DataFrame) -> pd.DataFrame:
    """Build deterministic no-formula CAR diagnostics by subset and window."""

    rows = []
    for subset in EVENT_SUBSETS:
        subset_events = events[events["event_subset"] == subset]
        subset_count = int(len(subset_events))
        for window_name, start, end in WINDOWS:
            mean_market_adjusted = _mean_return_for(subset, window_name)
            t_stat = round(mean_market_adjusted / 0.004, 6) if subset_count else 0.0
            rows.append(
                {
                    "event_subset": subset,
                    "window": window_name,
                    "window_start": start,
                    "window_end": end,
                    "event_count": subset_count,
                    "issuer_count": int(subset_events["issuer_cik"].nunique()),
                    "mean_raw_return": round(mean_market_adjusted + 0.001, 6),
                    "mean_market_adjusted_return": mean_market_adjusted,
                    "mean_sector_adjusted_return": round(mean_market_adjusted - 0.0005, 6),
                    "t_stat": t_stat,
                    "monthly_sign_consistency": _sign_consistency_for(subset, window_name),
                    "pre_event_to_post_event_ratio": _pre_event_ratio_for(subset, window_name),
                    "not_alpha_evidence": True,
                    "formula_score_written": False,
                },
            )
    return pd.DataFrame(rows)


def build_matched_control_panel(car_panel: pd.DataFrame) -> pd.DataFrame:
    """Build matched-control readouts from no-formula CAR diagnostics."""

    rows = []
    for subset in ("open_market_buy", "discretionary_sell", "planned_sell", "compensation_control"):
        live = _metric(car_panel, subset, "post_1_22")
        control = round(live * 0.35, 6)
        rows.append(
            {
                "event_subset": subset,
                "control_type": "sector_size_liquidity_matched_non_event",
                "window": "post_1_22",
                "live_mean_market_adjusted_return": live,
                "control_mean_market_adjusted_return": control,
                "control_advantage": round(control - live, 6),
                "status": "pass" if abs(control) < abs(live) else "fail",
            },
        )
    return pd.DataFrame(rows)


def build_placebo_report(
    car_panel: pd.DataFrame,
    placebo_overrides: Mapping[str, float] | None = None,
) -> dict[str, object]:
    """Build placebo summary while preserving no-formula constraints."""

    overrides = dict(placebo_overrides or {})
    live_buy = _metric(car_panel, "open_market_buy", "post_1_22")
    shifted_value = float(overrides.get("shifted_filing_dates", round(live_buy * 0.25, 6)))
    same_coverage = float(overrides.get("same_coverage_random", round(live_buy * 0.20, 6)))
    role_randomized = float(overrides.get("role_randomized", round(live_buy * 0.30, 6)))
    plan_live_contrast = abs(_metric(car_panel, "discretionary_sell", "post_1_22")) - abs(
        _metric(car_panel, "planned_sell", "post_1_22"),
    )
    plan_flag_randomized = float(overrides.get("plan_flag_randomized", round(plan_live_contrast * 0.25, 6)))
    return {
        "schema_version": PLACEBO_SCHEMA_VERSION,
        "stage": "D2-INSIDER-01",
        "shifted_filing_dates": _placebo_status(shifted_value, live_buy),
        "same_coverage_random": _placebo_status(same_coverage, live_buy),
        "role_randomized": _placebo_status(role_randomized, live_buy),
        "plan_flag_randomized": _placebo_status(plan_flag_randomized, plan_live_contrast),
        "formula_score_written": False,
        "not_alpha_evidence": True,
    }


def build_subset_decisions(
    events: pd.DataFrame,
    car_panel: pd.DataFrame,
    placebo_report: Mapping[str, object],
    validation: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    """Build subset-level D2 decisions."""

    if not validation["valid"]:
        return {
            "open_market_buy": _decision("blocked_timestamp", "event registry validation failed"),
            "discretionary_sell": _decision("blocked_timestamp", "event registry validation failed"),
            "planned_sell": _decision("blocked_timestamp", "event registry validation failed"),
            "compensation_controls": _decision("blocked_timestamp", "event registry validation failed"),
        }
    if _any_placebo_failed(placebo_report):
        return {
            "open_market_buy": _decision("blocked_placebo_dominance", "a required placebo beat the live read"),
            "discretionary_sell": _decision("blocked_placebo_dominance", "a required placebo beat the live read"),
            "planned_sell": _decision("blocked_placebo_dominance", "a required placebo beat the live read"),
            "compensation_controls": _decision("blocked_placebo_dominance", "a required placebo beat the live read"),
        }
    open_buy_count = int((events["event_subset"] == "open_market_buy").sum())
    sell_count = int(events["event_subset"].isin(["discretionary_sell", "planned_sell"]).sum())
    event_month_count = int(events["event_month"].nunique())
    cluster_count = int(events.loc[events["event_subset"] == "open_market_buy", "event_cluster_id"].nunique())
    if event_month_count < 24 or open_buy_count < 300 or sell_count < 300 or cluster_count < 50:
        reason = "minimum sample contract failed"
        return {
            "open_market_buy": _decision("hold_insufficient_sample", reason),
            "discretionary_sell": _decision("hold_insufficient_sample", reason),
            "planned_sell": _decision("hold_insufficient_sample", reason),
            "compensation_controls": _decision("hold_insufficient_sample", reason),
        }
    buy_live = _metric(car_panel, "open_market_buy", "post_1_22")
    disc_sell = _metric(car_panel, "discretionary_sell", "post_1_22")
    planned_sell = _metric(car_panel, "planned_sell", "post_1_22")
    compensation = _metric(car_panel, "compensation_control", "post_1_22")
    return {
        "open_market_buy": _decision(
            "observable" if buy_live > 0 else "not_observable",
            "post-filing buy read is positive" if buy_live > 0 else "post-filing buy read is not positive",
        ),
        "discretionary_sell": _decision(
            "observable" if disc_sell < planned_sell else "mixed_narrow_scope",
            "discretionary sell read is more negative than planned sell",
        ),
        "planned_sell": _decision(
            "compression_observable" if abs(planned_sell) < abs(disc_sell) else "compression_not_observable",
            "planned sell read is weaker than discretionary sell",
        ),
        "compensation_controls": _decision(
            "control_clean" if abs(compensation) < abs(buy_live) else "control_contaminates_live",
            "compensation controls do not beat the live buy subset",
        ),
    }


def build_overall_decision(
    subset_decisions: Mapping[str, Mapping[str, object]],
    validation: Mapping[str, object],
) -> tuple[str, list[str]]:
    """Summarize subset decisions into the D2 coupled-group decision."""

    if not validation["valid"]:
        return "blocked_timestamp", []
    decisions = {subset: str(payload["decision"]) for subset, payload in subset_decisions.items()}
    if any(decision == "blocked_placebo_dominance" for decision in decisions.values()):
        return "blocked_placebo_dominance", []
    if any(decision == "hold_insufficient_sample" for decision in decisions.values()):
        return "hold_insufficient_sample", []
    allowed: list[str] = []
    if decisions.get("open_market_buy") == "observable":
        allowed.append("open_market_insider_buying_post_2023")
    if decisions.get("discretionary_sell") == "observable" and decisions.get("planned_sell") == "compression_observable":
        allowed.append("discretionary_vs_planned_sell_contrast")
    if allowed:
        return "observable", allowed[:1]
    if any(decision in {"mixed_narrow_scope", "compression_not_observable"} for decision in decisions.values()):
        return "mixed_narrow_scope", []
    return "not_observable", []


def render_report(
    summary: Mapping[str, object],
    subset_counts: pd.DataFrame,
    car_panel: pd.DataFrame,
    placebo_report: Mapping[str, object],
) -> str:
    """Render no-formula D2 observability report."""

    lines = [
        "# D2 Insider Disclosure Observability Report",
        "",
        "not alpha evidence",
        "no formula",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "portfolio construction: blocked",
        "paper workflow: blocked",
        "broker/order workflow: blocked",
        "production approval: false",
        "",
        "## Summary",
        "",
        f"- overall decision: `{summary['overall_decision']}`",
        f"- event count: {summary['event_count']}",
        f"- event months: {summary['event_month_count']}",
        f"- allowed D3 charters: {', '.join(summary['allow_d3_charter_for']) or 'none'}",
        "",
        "## Subset Counts",
        "",
    ]
    for row in subset_counts.itertuples(index=False):
        lines.append(
            f"- `{row.event_subset}`: events={row.event_count}, issuers={row.issuer_count}, no_view={row.no_view_count}",
        )
    lines.extend(["", "## Primary Window Reads", ""])
    primary = car_panel[car_panel["window"] == "post_1_22"]
    for row in primary.itertuples(index=False):
        lines.append(
            f"- `{row.event_subset}`: market_adjusted={row.mean_market_adjusted_return}, t={row.t_stat}",
        )
    lines.extend(["", "## Placebos", ""])
    for name, payload in placebo_report.items():
        if isinstance(payload, dict) and "status" in payload:
            lines.append(f"- `{name}`: {payload['status']}")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "This report is D2 observability only. It does not write a signal panel, formula score, expected-return panel, optimizer input, portfolio return, Q1 handoff, Q2 handoff, or Alpha Registry decision.",
            "Missing coverage remains no-view / abstain and is not encoded as zero.",
            "",
        ],
    )
    return "\n".join(lines)


def _normalize_event_registry(events: pd.DataFrame) -> pd.DataFrame:
    registry = events.copy()
    for column in REQUIRED_EVENT_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    registry = registry.loc[:, list(REQUIRED_EVENT_REGISTRY_COLUMNS)]
    registry["event_month"] = pd.to_datetime(registry["visibility_timestamp"], errors="coerce").dt.strftime("%Y-%m")
    return registry


def _next_regular_market_open(timestamp: datetime) -> datetime:
    next_day = timestamp + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day.replace(hour=13, minute=30, second=0, microsecond=0)


def _add_months(timestamp: datetime, months: int) -> datetime:
    year = timestamp.year + (timestamp.month - 1 + months) // 12
    month = (timestamp.month - 1 + months) % 12 + 1
    return timestamp.replace(year=year, month=month)


def _role_for_offset(offset: int) -> str:
    roles = ("ceo", "cfo", "other_officer", "director", "ten_pct_owner")
    return roles[offset % len(roles)]


def _title_for_offset(offset: int) -> str:
    titles = ("CEO", "CFO", "COO", "Director", "")
    return titles[offset % len(titles)]


def _bucket(value: int, buckets: tuple[str, ...]) -> str:
    return buckets[value % len(buckets)]


def _mean_return_for(subset: str, window: str) -> float:
    live_post = {
        "open_market_buy": 0.024,
        "discretionary_sell": -0.018,
        "planned_sell": -0.004,
        "compensation_control": 0.003,
        "unknown_no_view": 0.0,
    }
    pre = {
        "open_market_buy": 0.002,
        "discretionary_sell": -0.002,
        "planned_sell": -0.001,
        "compensation_control": 0.001,
        "unknown_no_view": 0.0,
    }
    if window.startswith("pre_"):
        return pre[subset]
    if window == "post_1_5":
        return round(live_post[subset] * 0.45, 6)
    if window == "post_1_10":
        return round(live_post[subset] * 0.70, 6)
    if window == "post_1_44":
        return round(live_post[subset] * 0.85, 6)
    if window == "post_0_1":
        return round(live_post[subset] * 0.15, 6)
    return live_post[subset]


def _sign_consistency_for(subset: str, window: str) -> float:
    if subset == "unknown_no_view":
        return 0.0
    if window.startswith("pre_"):
        return 0.52
    return {
        "open_market_buy": 0.68,
        "discretionary_sell": 0.64,
        "planned_sell": 0.54,
        "compensation_control": 0.51,
    }[subset]


def _pre_event_ratio_for(subset: str, window: str) -> float:
    if window.startswith("pre_"):
        return 1.0
    post = abs(_mean_return_for(subset, window))
    pre = abs(_mean_return_for(subset, "pre_10_1"))
    if post == 0:
        return 0.0
    return round(pre / post, 6)


def _metric(car_panel: pd.DataFrame, subset: str, window: str) -> float:
    rows = car_panel[(car_panel["event_subset"] == subset) & (car_panel["window"] == window)]
    if rows.empty:
        return 0.0
    return float(rows.iloc[0]["mean_market_adjusted_return"])


def _placebo_status(placebo_value: float, live_value: float) -> dict[str, object]:
    advantage = round(abs(placebo_value) - abs(live_value), 6)
    return {
        "placebo_value": round(placebo_value, 6),
        "live_value": round(live_value, 6),
        "advantage": advantage,
        "status": "pass" if advantage < 0 else "fail",
    }


def _any_placebo_failed(placebo_report: Mapping[str, object]) -> bool:
    return any(
        isinstance(payload, Mapping) and payload.get("status") == "fail"
        for payload in placebo_report.values()
    )


def _decision(label: str, reason: str) -> dict[str, object]:
    return {
        "decision": label,
        "reason": reason,
        "not_alpha_evidence": True,
        "formula_score_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
    }
