"""FD-S6 revision-confirmed earnings underreaction diagnostic.

This module deliberately stays inside the Factor Discovery Sandbox. It builds
research-mode diagnostics only and does not write Q1, Q2, typed projection,
Alpha Registry, broker, order, paper, or production-approval artifacts.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from .factor_design import write_candidate_design_manifest


CANDIDATE_ID = "revision_confirmed_earnings_underreaction"
SCHEMA_VERSION = "fd_s6_revision_confirmed_earnings_underreaction.v1"
OUTPUT_SCHEMA_VERSION = "fd_s6_revision_confirmed_outputs.v1"
DEFAULT_OUTPUT_DIR = Path("outputs/factor_discovery/research_mode/revision_confirmed_earnings_underreaction")
HORIZONS = ("10d", "20d", "to_next_announcement")
DECAY_DAY_OFFSETS = (1, 2, 3, 5, 10, 20)
COST_BPS_PER_SIDE = 10.0
MIN_MEANINGFUL_TSTAT = 1.0
MIN_CAPACITY_ROW_FRACTION = 0.40
CAPACITY_FILTERS = {
    "none": 0.00,
    "remove_bottom_20pct_adv": 0.20,
    "remove_bottom_40pct_adv": 0.40,
}
VARIANT_COLUMNS = {
    "revision_only_score": "revision_only_score",
    "revision_plus_event_confirmation_score": "revision_plus_event_confirmation_score",
    "industry_neutral_score": "industry_neutral_score",
    "capacity_filtered_industry_neutral_score_remove_bottom_20pct_adv": (
        "capacity_filtered_industry_neutral_score_remove_bottom_20pct_adv"
    ),
    "capacity_filtered_industry_neutral_score_remove_bottom_40pct_adv": (
        "capacity_filtered_industry_neutral_score_remove_bottom_40pct_adv"
    ),
}

RESULT_COLUMNS = [
    "schema_version",
    "candidate_id",
    "candidate_variant",
    "period",
    "horizon",
    "mean_rank_ic",
    "rank_ic_t_stat",
    "mean_top_bottom_spread",
    "mean_top_bottom_spread_after_cost",
    "active_row_count",
    "active_date_count",
    "average_names_per_active_date",
    "turnover",
    "capacity_filter",
    "capacity_coverage_loss",
    "not_alpha_evidence",
    "direct_q2_entry_allowed",
]

PLACEBO_COLUMNS = [
    "schema_version",
    "candidate_id",
    "candidate_variant",
    "horizon",
    "placebo_name",
    "live_mean_rank_ic",
    "placebo_mean_rank_ic",
    "live_mean_top_bottom_spread_after_cost",
    "placebo_mean_top_bottom_spread_after_cost",
    "placebo_beats_live_rank_ic",
    "not_alpha_evidence",
    "direct_q2_entry_allowed",
]

SIGNAL_DECAY_COLUMNS = [
    "schema_version",
    "candidate_id",
    "day_offset",
    "period",
    "mean_rank_ic",
    "rank_ic_t_stat",
    "mean_top_bottom_spread",
    "mean_top_bottom_spread_after_cost",
    "active_row_count",
    "active_date_count",
    "not_alpha_evidence",
    "direct_q2_entry_allowed",
]

CAPACITY_COLUMNS = [
    "schema_version",
    "candidate_id",
    "capacity_filter",
    "minimum_adv_percentile",
    "active_row_count",
    "active_date_count",
    "average_names_per_active_date",
    "capacity_coverage_loss",
    "has_adv_coverage",
    "capacity_filtered_does_not_collapse",
    "not_alpha_evidence",
    "direct_q2_entry_allowed",
]


@dataclass(frozen=True)
class FDRevisionConfirmedAlphaResult:
    """Artifacts and summary for the FD-S6 diagnostic."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_revision_confirmed_earnings_underreaction(
    prices_path: str | Path,
    estimates_path: str | Path,
    events_path: str | Path,
    universe_path: str | Path | None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    min_cross_section: int = 5,
    train_test_split_date: str | None = None,
    cost_bps_per_side: float = COST_BPS_PER_SIDE,
) -> FDRevisionConfirmedAlphaResult:
    """Run the sandbox-only FD-S6 candidate diagnostic and write fixed artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "candidate_design_manifest": output_path / "candidate_design_manifest.json",
        "revision_confirmed_alpha_results": output_path / "revision_confirmed_alpha_results.csv",
        "revision_confirmed_alpha_summary": output_path / "revision_confirmed_alpha_summary.json",
        "revision_confirmed_alpha_report": output_path / "revision_confirmed_alpha_report.md",
        "placebo_comparison": output_path / "placebo_comparison.csv",
        "signal_decay": output_path / "signal_decay.csv",
        "capacity_diagnostics": output_path / "capacity_diagnostics.csv",
        "pit_timestamp_audit": output_path / "pit_timestamp_audit.json",
    }
    design_manifest = write_candidate_design_manifest(
        artifacts["candidate_design_manifest"],
        candidate_id=CANDIDATE_ID,
        family_id="revision_confirmed_earnings_underreaction",
        mechanism_family="revision_confirmed_earnings_underreaction",
    )
    if not design_manifest["candidate_validation_allowed"]:
        raise ValueError("revision-confirmed candidate design contract is invalid")

    prices = pd.read_csv(prices_path)
    estimates = pd.read_csv(estimates_path)
    events = pd.read_csv(events_path)
    universe = pd.read_csv(universe_path) if universe_path is not None and Path(universe_path).exists() else None

    signal_panel, pit_audit = construct_revision_confirmed_signals(
        prices=prices,
        estimates=estimates,
        events=events,
        universe=universe,
        min_cross_section=min_cross_section,
    )
    targets = _build_forward_target_panel(signal_panel, _normalize_prices(prices))
    scored = _attach_targets_and_periods(signal_panel, targets, train_test_split_date=train_test_split_date)
    results = _build_results_table(scored, min_cross_section=min_cross_section, cost_bps_per_side=cost_bps_per_side)
    placebo_comparison = _build_placebo_comparison(
        scored,
        results,
        min_cross_section=min_cross_section,
        cost_bps_per_side=cost_bps_per_side,
    )
    signal_decay = _build_signal_decay(
        signal_panel,
        _normalize_prices(prices),
        train_test_split_date=train_test_split_date,
        min_cross_section=min_cross_section,
        cost_bps_per_side=cost_bps_per_side,
    )
    capacity_diagnostics = _build_capacity_diagnostics(signal_panel, min_cross_section=min_cross_section)
    gate = apply_revision_confirmed_promotion_gate(
        results=results,
        placebo_comparison=placebo_comparison,
        capacity_diagnostics=capacity_diagnostics,
        pit_timestamp_audit=pit_audit,
    )
    exposure_summary = _build_exposure_summary(signal_panel)
    summary = _build_summary(
        prices_path=prices_path,
        estimates_path=estimates_path,
        events_path=events_path,
        universe_path=universe_path,
        signal_panel=signal_panel,
        results=results,
        placebo_comparison=placebo_comparison,
        signal_decay=signal_decay,
        capacity_diagnostics=capacity_diagnostics,
        pit_audit=pit_audit,
        gate=gate,
        exposure_summary=exposure_summary,
        min_cross_section=min_cross_section,
        cost_bps_per_side=cost_bps_per_side,
        design_manifest=design_manifest,
    )

    _write_csv(results, artifacts["revision_confirmed_alpha_results"], RESULT_COLUMNS)
    _write_csv(placebo_comparison, artifacts["placebo_comparison"], PLACEBO_COLUMNS)
    _write_csv(signal_decay, artifacts["signal_decay"], SIGNAL_DECAY_COLUMNS)
    _write_csv(capacity_diagnostics, artifacts["capacity_diagnostics"], CAPACITY_COLUMNS)
    artifacts["pit_timestamp_audit"].write_text(
        json.dumps(pit_audit, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["revision_confirmed_alpha_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["revision_confirmed_alpha_report"].write_text(
        _render_report(summary, results, placebo_comparison, capacity_diagnostics),
        encoding="utf-8",
    )
    return FDRevisionConfirmedAlphaResult(summary=summary, artifacts=artifacts)


def construct_revision_confirmed_signals(
    prices: pd.DataFrame,
    estimates: pd.DataFrame,
    events: pd.DataFrame,
    universe: pd.DataFrame | None = None,
    min_cross_section: int = 5,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Construct FD-S6 event-eligible signal rows using PIT estimate snapshots."""

    price_panel = _normalize_prices(prices)
    estimate_panel = _normalize_estimates(estimates)
    event_panel = _normalize_events(events)
    universe_panel = _normalize_universe(universe)

    calendar = pd.Index(sorted(price_panel["date"].dropna().unique()))
    close = price_panel.pivot_table(index="date", columns="asset_id", values="adjusted_close", aggfunc="last")
    returns = price_panel.pivot_table(index="date", columns="asset_id", values="return", aggfunc="last").reindex(
        close.index,
    )
    volume = (
        price_panel.pivot_table(index="date", columns="asset_id", values="volume", aggfunc="last").reindex(close.index)
        if "volume" in price_panel.columns
        else pd.DataFrame(index=close.index, columns=close.columns, dtype="float64")
    )
    adv_63d = (close * volume).rolling(63, min_periods=20).mean()

    event_panel = event_panel.sort_values(["asset_id", "announcement_date", "event_id"]).copy()
    event_panel["next_announcement_date"] = event_panel.groupby("asset_id")["announcement_date"].shift(-1)
    estimates_by_ticker = {
        ticker: group.sort_values(["estimate_snapshot_date", "fiscal_period"]).reset_index(drop=True)
        for ticker, group in estimate_panel.groupby("ibes_ticker")
    }

    rows: list[dict[str, object]] = []
    timestamp_violations = 0
    broken_pit_rows = 0
    tradability_violations = 0
    missing_event_confirmation = 0
    missing_price_rows = 0
    missing_adv_rows = 0
    missing_revision_rows = 0
    next_earnings_exclusions = 0
    max_lag_days = 0

    for event in event_panel.itertuples(index=False):
        asset_id = str(event.asset_id)
        signal_date = _trading_day_after_offset(calendar, event.announcement_date, 5)
        observable_date = _trading_day_after_offset(calendar, event.announcement_date, 3)
        if signal_date is None or observable_date is None:
            rows.append(_abstain_row(event, "missing_post_announcement_trading_calendar"))
            tradability_violations += 1
            continue

        next_window_date = _trading_day_after_offset(calendar, signal_date, 5)
        if pd.notna(event.next_announcement_date) and next_window_date is not None:
            if pd.Timestamp(event.next_announcement_date) <= pd.Timestamp(next_window_date):
                rows.append(
                    _abstain_row(
                        event,
                        "next_earnings_within_5_trading_days",
                        signal_date=signal_date,
                        observable_date=observable_date,
                    ),
                )
                next_earnings_exclusions += 1
                continue

        event_available_date = _timestamp_to_date(getattr(event, "event_available_timestamp", pd.NaT))
        if event_available_date is not None and event_available_date > pd.Timestamp(signal_date):
            broken_pit_rows += 1
            rows.append(
                _abstain_row(
                    event,
                    "event_timestamp_after_signal_date",
                    signal_date=signal_date,
                    observable_date=observable_date,
                ),
            )
            continue

        if signal_date not in close.index or asset_id not in close.columns or pd.isna(close.at[signal_date, asset_id]):
            missing_price_rows += 1
            rows.append(
                _abstain_row(event, "missing_price", signal_date=signal_date, observable_date=observable_date),
            )
            continue

        adv_value = adv_63d.at[signal_date, asset_id] if asset_id in adv_63d.columns else np.nan
        if pd.isna(adv_value) or not np.isfinite(float(adv_value)):
            missing_adv_rows += 1
            rows.append(
                _abstain_row(event, "missing_adv_63d", signal_date=signal_date, observable_date=observable_date),
            )
            continue

        revision = _revision_inputs(estimates_by_ticker, str(event.ibes_ticker), signal_date, calendar)
        if revision["future_estimate_used"]:
            timestamp_violations += 1
        latest_snapshot = revision["latest_estimate_snapshot_date"]
        if latest_snapshot is not None:
            max_lag_days = max(max_lag_days, int((pd.Timestamp(signal_date) - pd.Timestamp(latest_snapshot)).days))
        if pd.isna(revision["revision_20d_raw"]) or pd.isna(revision["revision_acceleration_raw"]):
            missing_revision_rows += 1
            rows.append(
                _abstain_row(event, "missing_revision_history", signal_date=signal_date, observable_date=observable_date),
            )
            continue

        car3 = _car3_return(returns, calendar, asset_id, event.announcement_date)
        sue_value = _safe_float(getattr(event, "sue_value", np.nan))
        event_missing = pd.isna(sue_value) or pd.isna(car3)
        if event_missing:
            missing_event_confirmation += 1

        recent_5d = _trailing_return(close, calendar, asset_id, signal_date, 5)
        row = {
            "schema_version": SCHEMA_VERSION,
            "candidate_id": CANDIDATE_ID,
            "event_id": str(getattr(event, "event_id", "")),
            "asset_id": asset_id,
            "permno": asset_id,
            "ticker": str(getattr(event, "ticker", "")),
            "ibes_ticker": str(event.ibes_ticker),
            "fiscal_period": str(getattr(event, "fiscal_period", "")),
            "announcement_date": _date_str(event.announcement_date),
            "event_available_timestamp": _timestamp_str(getattr(event, "event_available_timestamp", pd.NaT)),
            "event_observable_date_plus_3td": _date_str(observable_date),
            "eligible_from_date": _date_str(signal_date),
            "signal_date": _date_str(signal_date),
            "signal_trading_day_offset_from_announcement": _trading_day_distance(
                calendar,
                event.announcement_date,
                signal_date,
            ),
            "next_announcement_date": _date_str(getattr(event, "next_announcement_date", pd.NaT)),
            "latest_estimate_snapshot_date": _date_str(revision["latest_estimate_snapshot_date"]),
            "prior_20d_estimate_snapshot_date": _date_str(revision["prior_20d_estimate_snapshot_date"]),
            "prior_60d_estimate_snapshot_date": _date_str(revision["prior_60d_estimate_snapshot_date"]),
            "latest_expected_eps": revision["latest_expected_eps"],
            "prior_20d_expected_eps": revision["prior_20d_expected_eps"],
            "prior_60d_expected_eps": revision["prior_60d_expected_eps"],
            "revision_20d_raw": revision["revision_20d_raw"],
            "revision_60d_raw": revision["revision_60d_raw"],
            "revision_acceleration_raw": revision["revision_acceleration_raw"],
            "revision_scale_source": revision["revision_scale_source"],
            "sue_value": sue_value,
            "car3": car3,
            "event_confirmation_raw": np.nan,
            "event_confirmation_neutralized_missing": bool(event_missing),
            "recent_5d_return_raw": recent_5d,
            "short_reversal_guard_raw": np.nan,
            "adv_63d": float(adv_value),
            "adjusted_close": float(close.at[signal_date, asset_id]),
            "sector": _lookup_classification(universe_panel, asset_id, signal_date, "sector"),
            "industry": _lookup_classification(universe_panel, asset_id, signal_date, "industry"),
            "coverage_status": "active_view",
            "abstain_reason": "",
            "no_view_is_not_zero_alpha": True,
            "not_alpha_evidence": True,
            "direct_q2_entry_allowed": False,
        }
        rows.append(row)

    signal_panel = pd.DataFrame(rows)
    if signal_panel.empty:
        signal_panel = pd.DataFrame(columns=_signal_columns())
    signal_panel = _fill_missing_signal_columns(signal_panel)
    signal_panel = _score_active_signals(signal_panel, min_cross_section=min_cross_section)

    active = signal_panel[signal_panel["coverage_status"] == "active_view"]
    missing_data_ratio = (
        float(
            (
                missing_price_rows
                + missing_adv_rows
                + missing_revision_rows
                + broken_pit_rows
                + next_earnings_exclusions
            )
            / max(len(signal_panel), 1),
        )
        if len(signal_panel)
        else 1.0
    )
    pit_audit = {
        "schema_version": f"{SCHEMA_VERSION}.pit_audit",
        "candidate_id": CANDIDATE_ID,
        "pit_timestamp_audit_passed": bool(timestamp_violations == 0 and broken_pit_rows == 0 and tradability_violations == 0),
        "future_estimate_timestamp_violations": int(timestamp_violations),
        "broken_pit_timestamp_rows": int(broken_pit_rows),
        "tradability_rule_violations": int(tradability_violations),
        "event_confirmation_missing_input_rows": int(missing_event_confirmation),
        "missing_price_rows": int(missing_price_rows),
        "missing_adv_rows": int(missing_adv_rows),
        "missing_revision_history_rows": int(missing_revision_rows),
        "next_earnings_within_5td_exclusions": int(next_earnings_exclusions),
        "max_estimate_snapshot_lag_days": int(max_lag_days),
        "active_row_count": int(len(active)),
        "active_date_count": int(active["signal_date"].nunique()) if not active.empty else 0,
        "explicit_abstain_rows": int((signal_panel["coverage_status"] == "explicit_abstain").sum()),
        "missing_data_explains_effect": bool(missing_data_ratio > 0.50 or active.empty),
        "coverage_diagnostic_block": bool(len(active) < min_cross_section),
        "no_view_is_not_zero_alpha": True,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }
    return signal_panel, pit_audit


def apply_revision_confirmed_promotion_gate(
    results: pd.DataFrame,
    placebo_comparison: pd.DataFrame,
    capacity_diagnostics: pd.DataFrame,
    pit_timestamp_audit: Mapping[str, object],
) -> dict[str, object]:
    """Apply the preregistered conservative FD-S6 promotion gate."""

    result_rows = results.copy() if results is not None else pd.DataFrame(columns=RESULT_COLUMNS)
    placebo_rows = placebo_comparison.copy() if placebo_comparison is not None else pd.DataFrame(columns=PLACEBO_COLUMNS)
    capacity_rows = capacity_diagnostics.copy() if capacity_diagnostics is not None else pd.DataFrame(columns=CAPACITY_COLUMNS)

    primary = result_rows[
        (result_rows.get("candidate_variant") == "industry_neutral_score")
        & (result_rows.get("period") == "test")
    ].copy()
    by_horizon = {str(row.horizon): row for row in primary.itertuples(index=False)}

    test_rank_ic_positive = all(
        _row_float(by_horizon.get(horizon), "mean_rank_ic") > 0.0
        for horizon in ("20d", "to_next_announcement")
    )
    tstat_positive = all(
        _row_float(by_horizon.get(horizon), "rank_ic_t_stat") >= MIN_MEANINGFUL_TSTAT
        for horizon in ("20d", "to_next_announcement")
    )
    spread_after_cost_positive = all(
        _row_float(by_horizon.get(horizon), "mean_top_bottom_spread_after_cost") > 0.0
        for horizon in ("20d", "to_next_announcement")
    )

    shifted = placebo_rows[
        placebo_rows.get("placebo_name", pd.Series(dtype="object")).astype(str).str.startswith("shifted_event")
    ]
    live_beats_shifted = _live_beats_placebo(shifted)
    random_rows = placebo_rows[
        placebo_rows.get("placebo_name", pd.Series(dtype="object")).astype(str) == "random_same_coverage_placebo"
    ]
    live_beats_random = _live_beats_placebo(random_rows)

    if not capacity_rows.empty and "capacity_filtered_does_not_collapse" in capacity_rows.columns:
        capacity_filtered = capacity_rows[capacity_rows["capacity_filter"].astype(str) != "none"]
        capacity_does_not_collapse = bool(
            not capacity_filtered.empty and capacity_filtered["capacity_filtered_does_not_collapse"].fillna(False).all(),
        )
    else:
        capacity_does_not_collapse = False

    horizon_flags = {
        horizon: (
            _row_float(by_horizon.get(horizon), "mean_rank_ic") > 0.0
            and _row_float(by_horizon.get(horizon), "mean_top_bottom_spread_after_cost") > 0.0
        )
        for horizon in HORIZONS
    }
    survives_adjacent_horizons = bool(
        (horizon_flags["10d"] and horizon_flags["20d"])
        or (horizon_flags["20d"] and horizon_flags["to_next_announcement"]),
    )
    non_fragile_variants = _count_nonfragile_variants(result_rows)
    not_isolated_fragile_variant = non_fragile_variants >= 2
    pit_passes = bool(pit_timestamp_audit.get("pit_timestamp_audit_passed", False))
    coverage_ok = not bool(
        pit_timestamp_audit.get("missing_data_explains_effect", True)
        or pit_timestamp_audit.get("coverage_diagnostic_block", True),
    )
    observed_active_rows = int(pit_timestamp_audit.get("active_row_count", 0) or 0)
    if observed_active_rows == 0 and not primary.empty and "active_row_count" in primary.columns:
        observed_active_rows = int(pd.to_numeric(primary["active_row_count"], errors="coerce").fillna(0).sum())
    sufficient_support = bool(not primary.empty and observed_active_rows > 0)

    gate_checks = {
        "test_rank_ic_positive_for_20d_and_to_next_announcement": bool(test_rank_ic_positive),
        "test_rank_ic_t_stat_meaningfully_positive": bool(tstat_positive),
        "top_bottom_spread_after_cost_positive": bool(spread_after_cost_positive),
        "live_beats_strongest_shifted_event_placebo": bool(live_beats_shifted),
        "live_beats_random_same_coverage_placebo": bool(live_beats_random),
        "capacity_filtered_version_does_not_collapse": bool(capacity_does_not_collapse),
        "survives_at_least_two_adjacent_horizons": bool(survives_adjacent_horizons),
        "not_isolated_to_one_fragile_variant": bool(not_isolated_fragile_variant),
        "pit_timestamp_audit_passes": bool(pit_passes),
        "missing_data_coverage_diagnostics_do_not_explain_effect": bool(coverage_ok),
    }
    promotion_gate_passed = bool(all(gate_checks.values()) and sufficient_support)

    if promotion_gate_passed:
        decision_label = "promotable_to_Q1_candidate_review"
    elif not pit_passes:
        decision_label = "timestamp_blocked"
    elif not sufficient_support or not coverage_ok:
        decision_label = "insufficient_support"
    elif not live_beats_shifted or not live_beats_random:
        decision_label = "placebo_blocked"
    elif not capacity_does_not_collapse:
        decision_label = "capacity_blocked"
    else:
        decision_label = "diagnostic_only_mixed"

    return {
        "schema_version": f"{SCHEMA_VERSION}.promotion_gate",
        "candidate_id": CANDIDATE_ID,
        "decision_label": decision_label,
        "promotion_gate_passed": promotion_gate_passed,
        "q1_candidate_review_eligible": bool(decision_label == "promotable_to_Q1_candidate_review"),
        "alpha_success_claimed": False,
        "production_approval_claimed": False,
        "gate_checks": gate_checks,
        "non_fragile_positive_variant_count": int(non_fragile_variants),
    }


def _normalize_prices(prices: pd.DataFrame) -> pd.DataFrame:
    data = prices.copy()
    if "asset_id" not in data.columns:
        if "permno" in data.columns:
            data["asset_id"] = data["permno"]
        elif "ticker" in data.columns:
            data["asset_id"] = data["ticker"]
        else:
            raise ValueError("prices must include asset_id, permno, or ticker")
    data["asset_id"] = data["asset_id"].map(_asset_id)
    if "ticker" not in data.columns:
        data["ticker"] = data["asset_id"]
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.normalize()
    if "adjusted_close" not in data.columns:
        if "raw_close" in data.columns:
            data["adjusted_close"] = data["raw_close"]
        elif "prc" in data.columns:
            data["adjusted_close"] = pd.to_numeric(data["prc"], errors="coerce").abs()
        else:
            raise ValueError("prices must include adjusted_close, raw_close, or prc")
    data["adjusted_close"] = pd.to_numeric(data["adjusted_close"], errors="coerce")
    if "volume" in data.columns:
        data["volume"] = pd.to_numeric(data["volume"], errors="coerce")
    else:
        data["volume"] = np.nan
    if "return" not in data.columns:
        if "ret" in data.columns:
            data["return"] = pd.to_numeric(data["ret"], errors="coerce")
        else:
            data["return"] = (
                data.sort_values(["asset_id", "date"])
                .groupby("asset_id")["adjusted_close"]
                .pct_change(fill_method=None)
            )
    else:
        data["return"] = pd.to_numeric(data["return"], errors="coerce")
    return data.sort_values(["date", "asset_id"]).reset_index(drop=True)


def _normalize_estimates(estimates: pd.DataFrame) -> pd.DataFrame:
    data = estimates.copy()
    if "ibes_ticker" not in data.columns:
        raise ValueError("estimates must include ibes_ticker")
    data["ibes_ticker"] = data["ibes_ticker"].astype(str)
    data["estimate_snapshot_date"] = pd.to_datetime(data["estimate_snapshot_date"], errors="coerce").dt.normalize()
    if "expected_eps" not in data.columns:
        if "medest" in data.columns:
            data["expected_eps"] = data["medest"]
        else:
            raise ValueError("estimates must include expected_eps or medest")
    data["expected_eps"] = pd.to_numeric(data["expected_eps"], errors="coerce")
    if "fiscal_period" not in data.columns:
        data["fiscal_period"] = "UNKNOWN"
    return data.dropna(subset=["ibes_ticker", "estimate_snapshot_date", "expected_eps"]).reset_index(drop=True)


def _normalize_events(events: pd.DataFrame) -> pd.DataFrame:
    data = events.copy()
    if "asset_id" not in data.columns:
        if "permno" in data.columns:
            data["asset_id"] = data["permno"]
        elif "symbol" in data.columns:
            data["asset_id"] = data["symbol"]
        else:
            raise ValueError("events must include asset_id, permno, or symbol")
    data["asset_id"] = data["asset_id"].map(_asset_id)
    if "ticker" not in data.columns:
        data["ticker"] = data["symbol"] if "symbol" in data.columns else data["asset_id"]
    if "event_id" not in data.columns:
        data["event_id"] = [
            f"FD-S6-{row.asset_id}-{idx:05d}" for idx, row in enumerate(data.itertuples(index=False), start=1)
        ]
    data["announcement_date"] = pd.to_datetime(data["announcement_date"], errors="coerce").dt.normalize()
    if "event_available_timestamp" in data.columns:
        data["event_available_timestamp"] = pd.to_datetime(data["event_available_timestamp"], errors="coerce", utc=True)
    else:
        data["event_available_timestamp"] = pd.NaT
    if "ibes_ticker" not in data.columns:
        data["ibes_ticker"] = data["ticker"]
    data["ibes_ticker"] = data["ibes_ticker"].astype(str)
    if "sue_value" not in data.columns:
        data["sue_value"] = np.nan
    data["sue_value"] = pd.to_numeric(data["sue_value"], errors="coerce")
    if "pit_safety_status" in data.columns:
        safe = data["pit_safety_status"].fillna("").astype(str).str.lower().isin({"pit_safe", "safe", "ok"})
        data = data[safe].copy()
    return data.dropna(subset=["asset_id", "announcement_date", "ibes_ticker"]).reset_index(drop=True)


def _normalize_universe(universe: pd.DataFrame | None) -> pd.DataFrame:
    if universe is None or universe.empty:
        return pd.DataFrame(columns=["asset_id", "membership_start", "membership_end", "sector", "industry"])
    data = universe.copy()
    if "asset_id" not in data.columns:
        data["asset_id"] = data["permno"] if "permno" in data.columns else data.get("ticker", "")
    data["asset_id"] = data["asset_id"].map(_asset_id)
    if "membership_start" not in data.columns:
        data["membership_start"] = data["date"] if "date" in data.columns else pd.Timestamp("1900-01-01")
    if "membership_end" not in data.columns:
        data["membership_end"] = pd.Timestamp("2100-01-01")
    data["membership_start"] = pd.to_datetime(data["membership_start"], errors="coerce").dt.normalize()
    data["membership_end"] = pd.to_datetime(data["membership_end"], errors="coerce").dt.normalize()
    for column in ("sector", "industry"):
        if column not in data.columns:
            data[column] = "UNKNOWN"
        data[column] = data[column].fillna("UNKNOWN").astype(str)
    return data[["asset_id", "membership_start", "membership_end", "sector", "industry"]].drop_duplicates()


def _revision_inputs(
    estimates_by_ticker: Mapping[str, pd.DataFrame],
    ibes_ticker: str,
    signal_date: pd.Timestamp,
    calendar: pd.Index,
) -> dict[str, object]:
    empty = {
        "latest_estimate_snapshot_date": None,
        "prior_20d_estimate_snapshot_date": None,
        "prior_60d_estimate_snapshot_date": None,
        "latest_expected_eps": np.nan,
        "prior_20d_expected_eps": np.nan,
        "prior_60d_expected_eps": np.nan,
        "revision_20d_raw": np.nan,
        "revision_60d_raw": np.nan,
        "revision_acceleration_raw": np.nan,
        "revision_scale_source": "prior_consensus_magnitude_floor",
        "future_estimate_used": False,
    }
    group = estimates_by_ticker.get(str(ibes_ticker))
    if group is None or group.empty:
        return empty
    latest = _latest_estimate_asof(group, signal_date)
    anchor_20 = _trading_day_before_offset(calendar, signal_date, 20)
    anchor_60 = _trading_day_before_offset(calendar, signal_date, 60)
    prior_20 = _latest_estimate_asof(group, anchor_20) if anchor_20 is not None else None
    prior_60 = _latest_estimate_asof(group, anchor_60) if anchor_60 is not None else None
    if latest is None or prior_20 is None or prior_60 is None:
        return empty
    latest_eps = float(latest.expected_eps)
    prior_20_eps = float(prior_20.expected_eps)
    prior_60_eps = float(prior_60.expected_eps)
    scale_20 = max(abs(prior_20_eps), 1.0)
    scale_60 = max(abs(prior_60_eps), 1.0)
    revision_20 = (latest_eps - prior_20_eps) / scale_20
    revision_60 = (latest_eps - prior_60_eps) / scale_60
    future_used = bool(pd.Timestamp(latest.estimate_snapshot_date) > pd.Timestamp(signal_date))
    return {
        "latest_estimate_snapshot_date": pd.Timestamp(latest.estimate_snapshot_date),
        "prior_20d_estimate_snapshot_date": pd.Timestamp(prior_20.estimate_snapshot_date),
        "prior_60d_estimate_snapshot_date": pd.Timestamp(prior_60.estimate_snapshot_date),
        "latest_expected_eps": latest_eps,
        "prior_20d_expected_eps": prior_20_eps,
        "prior_60d_expected_eps": prior_60_eps,
        "revision_20d_raw": revision_20,
        "revision_60d_raw": revision_60,
        "revision_acceleration_raw": revision_20 - revision_60,
        "revision_scale_source": "prior_consensus_magnitude_floor",
        "future_estimate_used": future_used,
    }


def _latest_estimate_asof(group: pd.DataFrame, asof_date: pd.Timestamp | None):
    if asof_date is None:
        return None
    visible = group[group["estimate_snapshot_date"] <= pd.Timestamp(asof_date)]
    if visible.empty:
        return None
    return visible.sort_values(["estimate_snapshot_date", "fiscal_period"]).iloc[-1]


def _score_active_signals(signal_panel: pd.DataFrame, min_cross_section: int) -> pd.DataFrame:
    data = signal_panel.copy()
    score_columns = [
        "revision_20d_z",
        "revision_acceleration_z",
        "event_confirmation_z",
        "short_reversal_guard_z",
        "revision_only_score",
        "revision_plus_event_confirmation_score",
        "industry_neutral_score",
        "capacity_rank",
        "capacity_filtered_industry_neutral_score_remove_bottom_20pct_adv",
        "capacity_filtered_industry_neutral_score_remove_bottom_40pct_adv",
    ]
    for column in score_columns:
        if column not in data.columns:
            data[column] = np.nan

    active_mask = data["coverage_status"] == "active_view"
    for signal_date, group in data[active_mask].groupby("signal_date"):
        idx = group.index
        if len(idx) < min_cross_section:
            data.loc[idx, "coverage_status"] = "explicit_abstain"
            data.loc[idx, "abstain_reason"] = "insufficient_cross_section"
            continue
        data.loc[idx, "revision_20d_z"] = _winsorized_zscore(group["revision_20d_raw"])
        data.loc[idx, "revision_acceleration_z"] = _winsorized_zscore(group["revision_acceleration_raw"])

        sue_z = _winsorized_zscore(group["sue_value"])
        car_sign = np.sign(pd.to_numeric(group["car3"], errors="coerce"))
        event_confirmation = sue_z * car_sign
        event_confirmation = event_confirmation.where(~group["event_confirmation_neutralized_missing"].astype(bool), 0.0)
        data.loc[idx, "event_confirmation_raw"] = event_confirmation.fillna(0.0)
        data.loc[idx, "event_confirmation_z"] = _winsorized_zscore(data.loc[idx, "event_confirmation_raw"]).fillna(0.0)

        recent_z = _winsorized_zscore(group["recent_5d_return_raw"])
        data.loc[idx, "short_reversal_guard_raw"] = -recent_z
        data.loc[idx, "short_reversal_guard_z"] = _winsorized_zscore(data.loc[idx, "short_reversal_guard_raw"])
        data.loc[idx, "revision_only_score"] = (
            0.45 * data.loc[idx, "revision_20d_z"] + 0.25 * data.loc[idx, "revision_acceleration_z"]
        )
        data.loc[idx, "revision_plus_event_confirmation_score"] = (
            data.loc[idx, "revision_only_score"]
            + 0.20 * data.loc[idx, "event_confirmation_z"]
            - 0.10 * data.loc[idx, "short_reversal_guard_z"]
        )
        data.loc[idx, "industry_neutral_score"] = _neutralize(
            data.loc[idx, "revision_plus_event_confirmation_score"],
            data.loc[idx, "industry"],
        )
        capacity_rank = data.loc[idx, "adv_63d"].rank(method="average", pct=True)
        data.loc[idx, "capacity_rank"] = capacity_rank
        for filter_name, minimum_rank in CAPACITY_FILTERS.items():
            if filter_name == "none":
                continue
            column = f"capacity_filtered_industry_neutral_score_{filter_name}"
            passed = capacity_rank >= minimum_rank
            data.loc[idx, column] = data.loc[idx, "industry_neutral_score"].where(passed)

    return data[_signal_columns()]


def _build_forward_target_panel(signal_panel: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    close = prices.pivot_table(index="date", columns="asset_id", values="adjusted_close", aggfunc="last").sort_index()
    calendar = pd.Index(close.index)
    rows: list[dict[str, object]] = []
    active = signal_panel[signal_panel["coverage_status"] == "active_view"]
    for row in active.itertuples(index=False):
        signal_date = pd.Timestamp(row.signal_date)
        asset_id = str(row.asset_id)
        for horizon in HORIZONS:
            if horizon == "to_next_announcement":
                end_date = _target_date_before_next_announcement(calendar, row.next_announcement_date, signal_date)
            else:
                end_date = _trading_day_after_offset(calendar, signal_date, int(horizon.removesuffix("d")))
            forward_return = _forward_return(close, asset_id, signal_date, end_date)
            rows.append(
                {
                    "event_id": row.event_id,
                    "asset_id": asset_id,
                    "signal_date": _date_str(signal_date),
                    "horizon": horizon,
                    "target_end_date": _date_str(end_date),
                    "forward_return": forward_return,
                }
            )
    return pd.DataFrame(rows)


def _attach_targets_and_periods(
    signal_panel: pd.DataFrame,
    targets: pd.DataFrame,
    train_test_split_date: str | None,
) -> pd.DataFrame:
    active = signal_panel[signal_panel["coverage_status"] == "active_view"].copy()
    if active.empty or targets.empty:
        return pd.DataFrame()
    scored = active.merge(targets, on=["event_id", "asset_id", "signal_date"], how="left")
    scored["signal_date_dt"] = pd.to_datetime(scored["signal_date"], errors="coerce")
    if train_test_split_date is not None:
        split_date = pd.Timestamp(train_test_split_date)
    else:
        unique_dates = sorted(scored["signal_date_dt"].dropna().unique())
        split_date = pd.Timestamp(unique_dates[max(int(len(unique_dates) * 0.60), 0)]) if unique_dates else pd.NaT
    scored["period"] = np.where(scored["signal_date_dt"] < split_date, "train", "test")
    return scored


def _build_results_table(
    scored: pd.DataFrame,
    min_cross_section: int,
    cost_bps_per_side: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if scored.empty:
        return pd.DataFrame(columns=RESULT_COLUMNS)
    for variant, score_column in VARIANT_COLUMNS.items():
        capacity_filter = _capacity_filter_for_variant(variant)
        variant_scored = scored[pd.notna(scored.get(score_column)) & pd.notna(scored["forward_return"])].copy()
        for period in ("train", "test"):
            for horizon in HORIZONS:
                subset = variant_scored[(variant_scored["period"] == period) & (variant_scored["horizon"] == horizon)]
                metric = _aggregate_metrics(
                    subset,
                    score_column=score_column,
                    min_cross_section=min_cross_section,
                    cost_bps_per_side=cost_bps_per_side,
                )
                rows.append(
                    {
                        "schema_version": OUTPUT_SCHEMA_VERSION,
                        "candidate_id": CANDIDATE_ID,
                        "candidate_variant": variant,
                        "period": period,
                        "horizon": horizon,
                        "mean_rank_ic": metric["mean_rank_ic"],
                        "rank_ic_t_stat": metric["rank_ic_t_stat"],
                        "mean_top_bottom_spread": metric["mean_top_bottom_spread"],
                        "mean_top_bottom_spread_after_cost": metric["mean_top_bottom_spread_after_cost"],
                        "active_row_count": metric["active_row_count"],
                        "active_date_count": metric["active_date_count"],
                        "average_names_per_active_date": metric["average_names_per_active_date"],
                        "turnover": metric["turnover"],
                        "capacity_filter": capacity_filter,
                        "capacity_coverage_loss": metric["capacity_coverage_loss"],
                        "not_alpha_evidence": True,
                        "direct_q2_entry_allowed": False,
                    },
                )
    return pd.DataFrame(rows, columns=RESULT_COLUMNS)


def _build_placebo_comparison(
    scored: pd.DataFrame,
    results: pd.DataFrame,
    min_cross_section: int,
    cost_bps_per_side: float,
) -> pd.DataFrame:
    if scored.empty:
        return pd.DataFrame(columns=PLACEBO_COLUMNS)
    rows: list[dict[str, object]] = []
    primary_variant = "industry_neutral_score"
    live_lookup = {
        str(row.horizon): row
        for row in results[
            (results["candidate_variant"] == primary_variant) & (results["period"] == "test")
        ].itertuples(index=False)
    }
    test_scored = scored[scored["period"] == "test"].copy()
    for placebo_name, placebo_score in _placebo_scores(test_scored).items():
        test_scored[placebo_name] = placebo_score
        for horizon in ("20d", "to_next_announcement"):
            live_row = live_lookup.get(horizon)
            subset = test_scored[(test_scored["horizon"] == horizon) & pd.notna(test_scored[placebo_name])]
            metric = _aggregate_metrics(
                subset,
                score_column=placebo_name,
                min_cross_section=min_cross_section,
                cost_bps_per_side=cost_bps_per_side,
            )
            live_ic = _row_float(live_row, "mean_rank_ic")
            placebo_ic = float(metric["mean_rank_ic"])
            rows.append(
                {
                    "schema_version": OUTPUT_SCHEMA_VERSION,
                    "candidate_id": CANDIDATE_ID,
                    "candidate_variant": primary_variant,
                    "horizon": horizon,
                    "placebo_name": placebo_name,
                    "live_mean_rank_ic": live_ic,
                    "placebo_mean_rank_ic": placebo_ic,
                    "live_mean_top_bottom_spread_after_cost": _row_float(
                        live_row,
                        "mean_top_bottom_spread_after_cost",
                    ),
                    "placebo_mean_top_bottom_spread_after_cost": metric["mean_top_bottom_spread_after_cost"],
                    "placebo_beats_live_rank_ic": bool(pd.notna(placebo_ic) and placebo_ic >= live_ic),
                    "not_alpha_evidence": True,
                    "direct_q2_entry_allowed": False,
                },
            )
    return pd.DataFrame(rows, columns=PLACEBO_COLUMNS)


def _build_signal_decay(
    signal_panel: pd.DataFrame,
    prices: pd.DataFrame,
    train_test_split_date: str | None,
    min_cross_section: int,
    cost_bps_per_side: float,
) -> pd.DataFrame:
    close = prices.pivot_table(index="date", columns="asset_id", values="adjusted_close", aggfunc="last").sort_index()
    calendar = pd.Index(close.index)
    active = signal_panel[signal_panel["coverage_status"] == "active_view"].copy()
    rows: list[dict[str, object]] = []
    for day_offset in DECAY_DAY_OFFSETS:
        target_rows = []
        for row in active.itertuples(index=False):
            signal_date = pd.Timestamp(row.signal_date)
            end_date = _trading_day_after_offset(calendar, signal_date, day_offset)
            target_rows.append(
                {
                    "event_id": row.event_id,
                    "asset_id": row.asset_id,
                    "signal_date": row.signal_date,
                    "horizon": f"{day_offset}d",
                    "forward_return": _forward_return(close, row.asset_id, signal_date, end_date),
                },
            )
        scored = _attach_targets_and_periods(active, pd.DataFrame(target_rows), train_test_split_date)
        for period in ("train", "test"):
            metric = _aggregate_metrics(
                scored[scored["period"] == period] if not scored.empty else scored,
                score_column="industry_neutral_score",
                min_cross_section=min_cross_section,
                cost_bps_per_side=cost_bps_per_side,
            )
            rows.append(
                {
                    "schema_version": OUTPUT_SCHEMA_VERSION,
                    "candidate_id": CANDIDATE_ID,
                    "day_offset": day_offset,
                    "period": period,
                    "mean_rank_ic": metric["mean_rank_ic"],
                    "rank_ic_t_stat": metric["rank_ic_t_stat"],
                    "mean_top_bottom_spread": metric["mean_top_bottom_spread"],
                    "mean_top_bottom_spread_after_cost": metric["mean_top_bottom_spread_after_cost"],
                    "active_row_count": metric["active_row_count"],
                    "active_date_count": metric["active_date_count"],
                    "not_alpha_evidence": True,
                    "direct_q2_entry_allowed": False,
                },
            )
    return pd.DataFrame(rows, columns=SIGNAL_DECAY_COLUMNS)


def _build_capacity_diagnostics(signal_panel: pd.DataFrame, min_cross_section: int) -> pd.DataFrame:
    active = signal_panel[signal_panel["coverage_status"] == "active_view"].copy()
    total = len(active)
    rows: list[dict[str, object]] = []
    for filter_name, minimum_rank in CAPACITY_FILTERS.items():
        if active.empty:
            subset = active
        elif filter_name == "none":
            subset = active[pd.notna(active["adv_63d"])]
        else:
            subset = active[pd.to_numeric(active["capacity_rank"], errors="coerce") >= minimum_rank]
        active_dates = int(subset["signal_date"].nunique()) if not subset.empty else 0
        avg_names = float(subset.groupby("signal_date")["asset_id"].nunique().mean()) if active_dates else 0.0
        loss = 1.0 - (len(subset) / total) if total else 1.0
        has_adv = bool(total > 0 and pd.notna(active["adv_63d"]).all())
        rows.append(
            {
                "schema_version": OUTPUT_SCHEMA_VERSION,
                "candidate_id": CANDIDATE_ID,
                "capacity_filter": filter_name,
                "minimum_adv_percentile": minimum_rank,
                "active_row_count": int(len(subset)),
                "active_date_count": active_dates,
                "average_names_per_active_date": avg_names,
                "capacity_coverage_loss": float(loss),
                "has_adv_coverage": has_adv,
                "capacity_filtered_does_not_collapse": bool(
                    has_adv
                    and (filter_name == "none" or (len(subset) >= max(min_cross_section, int(total * MIN_CAPACITY_ROW_FRACTION))))
                ),
                "not_alpha_evidence": True,
                "direct_q2_entry_allowed": False,
            },
        )
    return pd.DataFrame(rows, columns=CAPACITY_COLUMNS)


def _aggregate_metrics(
    subset: pd.DataFrame,
    score_column: str,
    min_cross_section: int,
    cost_bps_per_side: float,
) -> dict[str, float | int]:
    if subset is None or subset.empty or score_column not in subset.columns:
        return _empty_metric()
    per_date_rows = []
    top_sets: list[set[str]] = []
    for signal_date, group in subset.groupby("signal_date"):
        clean = group[[score_column, "forward_return", "asset_id"]].dropna()
        if len(clean) < min_cross_section:
            continue
        rank_ic = _safe_corr(
            clean[score_column].rank(method="average"),
            clean["forward_return"].rank(method="average"),
        )
        top_count = max(1, int(np.ceil(len(clean) * 0.20)))
        ranked = clean.sort_values(score_column)
        bottom = ranked.head(top_count)
        top = ranked.tail(top_count)
        spread = float(top["forward_return"].mean() - bottom["forward_return"].mean())
        top_assets = set(top["asset_id"].astype(str))
        top_sets.append(top_assets)
        per_date_rows.append(
            {
                "signal_date": signal_date,
                "rank_ic": float(rank_ic) if pd.notna(rank_ic) else np.nan,
                "spread": spread,
                "name_count": len(clean),
            },
        )
    if not per_date_rows:
        return _empty_metric()
    per_date = pd.DataFrame(per_date_rows)
    turnover = _average_top_turnover(top_sets)
    spread_cost = 2.0 * float(cost_bps_per_side) / 10_000.0
    rank_ics = per_date["rank_ic"].dropna()
    t_stat = float(rank_ics.mean() / rank_ics.std(ddof=1) * np.sqrt(len(rank_ics))) if len(rank_ics) > 1 and rank_ics.std(ddof=1) > 0 else np.nan
    return {
        "mean_rank_ic": float(rank_ics.mean()) if not rank_ics.empty else np.nan,
        "rank_ic_t_stat": t_stat,
        "mean_top_bottom_spread": float(per_date["spread"].mean()),
        "mean_top_bottom_spread_after_cost": float(per_date["spread"].mean() - spread_cost),
        "active_row_count": int(per_date["name_count"].sum()),
        "active_date_count": int(len(per_date)),
        "average_names_per_active_date": float(per_date["name_count"].mean()),
        "turnover": float(turnover),
        "capacity_coverage_loss": 0.0,
    }


def _empty_metric() -> dict[str, float | int]:
    return {
        "mean_rank_ic": np.nan,
        "rank_ic_t_stat": np.nan,
        "mean_top_bottom_spread": np.nan,
        "mean_top_bottom_spread_after_cost": np.nan,
        "active_row_count": 0,
        "active_date_count": 0,
        "average_names_per_active_date": 0.0,
        "turnover": np.nan,
        "capacity_coverage_loss": 1.0,
    }


def _placebo_scores(scored: pd.DataFrame) -> dict[str, pd.Series]:
    if scored.empty:
        return {}
    base = scored.copy()
    by_asset = base.sort_values(["asset_id", "signal_date"]).groupby("asset_id")["industry_neutral_score"]
    industry_codes = base["industry"].fillna("UNKNOWN").astype("category").cat.codes.astype(float)
    return {
        "shifted_event_minus_5td": by_asset.shift(-1).reindex(base.index),
        "shifted_event_plus_5td": by_asset.shift(1).reindex(base.index),
        "shifted_event_plus_10td": by_asset.shift(2).reindex(base.index),
        "random_same_coverage_placebo": base.apply(_deterministic_random_score, axis=1),
        "permuted_revision_timestamp_placebo": base.groupby("signal_date")["revision_20d_z"].transform(
            lambda values: values.sample(frac=1.0, random_state=17).to_numpy() if len(values) else values,
        ),
        "industry_only_placebo": _winsorized_zscore(industry_codes),
        "short_term_return_only_placebo": base["short_reversal_guard_z"],
    }


def _build_exposure_summary(signal_panel: pd.DataFrame) -> dict[str, object]:
    active = signal_panel[signal_panel["coverage_status"] == "active_view"]
    if active.empty:
        return {"sector_counts": {}, "industry_counts": {}, "mean_industry_neutral_by_industry": {}}
    return {
        "sector_counts": active["sector"].fillna("UNKNOWN").value_counts().to_dict(),
        "industry_counts": active["industry"].fillna("UNKNOWN").value_counts().to_dict(),
        "mean_industry_neutral_by_industry": (
            active.groupby("industry")["industry_neutral_score"].mean().round(12).fillna(0.0).to_dict()
        ),
    }


def _build_summary(
    prices_path: str | Path,
    estimates_path: str | Path,
    events_path: str | Path,
    universe_path: str | Path | None,
    signal_panel: pd.DataFrame,
    results: pd.DataFrame,
    placebo_comparison: pd.DataFrame,
    signal_decay: pd.DataFrame,
    capacity_diagnostics: pd.DataFrame,
    pit_audit: Mapping[str, object],
    gate: Mapping[str, object],
    exposure_summary: Mapping[str, object],
    min_cross_section: int,
    cost_bps_per_side: float,
    design_manifest: Mapping[str, object],
) -> dict[str, object]:
    active = signal_panel[signal_panel["coverage_status"] == "active_view"]
    return {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "candidate_id": CANDIDATE_ID,
        "alpha_name": CANDIDATE_ID,
        "stage": "FD-S6 sandbox research diagnostic",
        "design_contract_valid": bool(design_manifest["design_contract_valid"]),
        "design_layer_required_before_formula": bool(design_manifest["design_layer_required_before_formula"]),
        "formula_is_measurement_not_thesis": bool(design_manifest["formula_is_measurement_not_thesis"]),
        "candidate_validation_allowed_by_design": bool(design_manifest["candidate_validation_allowed"]),
        "decision_label": gate["decision_label"],
        "promotion_gate_passed": gate["promotion_gate_passed"],
        "q1_candidate_review_eligible": gate["q1_candidate_review_eligible"],
        "economic_thesis": (
            "Markets may underreact after earnings when PIT analyst revisions confirm "
            "the earnings surprise and announcement-window reaction."
        ),
        "signal_definition": {
            "revision_20d": "latest PIT consensus EPS minus consensus as of 20 trading days earlier, scaled by prior consensus magnitude floor",
            "revision_acceleration": "revision_20d minus normalized 60 trading day revision trend",
            "event_confirmation": "cross-sectional z(SUE) multiplied by sign(CAR3), neutral when SUE or CAR3 is missing",
            "short_reversal_guard": "negative cross-sectional z-score of trailing 5 trading day return",
            "final_score": (
                "industry_neutralize(0.45*z(revision_20d)+0.25*z(revision_acceleration)"
                "+0.20*z(event_confirmation)-0.10*z(short_reversal_guard))"
            ),
        },
        "preregistered_parameters": {
            "revision_short_window_trading_days": 20,
            "revision_trend_window_trading_days": 60,
            "event_observable_after_trading_days": 3,
            "tradable_after_earnings_trading_days": 5,
            "next_earnings_exclusion_trading_days": 5,
            "adv_window_trading_days": 63,
            "capacity_filters": list(CAPACITY_FILTERS),
            "evaluation_horizons": list(HORIZONS),
            "cost_bps_per_side": cost_bps_per_side,
            "min_cross_section": min_cross_section,
            "parameters_tuned_after_output": False,
        },
        "input_paths": {
            "prices_path": str(prices_path),
            "estimates_path": str(estimates_path),
            "events_path": str(events_path),
            "universe_path": str(universe_path) if universe_path is not None else None,
        },
        "signal_row_count": int(len(signal_panel)),
        "active_row_count": int(len(active)),
        "active_date_count": int(active["signal_date"].nunique()) if not active.empty else 0,
        "average_names_per_active_date": (
            float(active.groupby("signal_date")["asset_id"].nunique().mean()) if not active.empty else 0.0
        ),
        "explicit_abstain_rows": int((signal_panel["coverage_status"] == "explicit_abstain").sum()),
        "result_row_count": int(len(results)),
        "placebo_row_count": int(len(placebo_comparison)),
        "signal_decay_row_count": int(len(signal_decay)),
        "capacity_diagnostic_row_count": int(len(capacity_diagnostics)),
        "pit_timestamp_audit": dict(pit_audit),
        "gate_checks": gate["gate_checks"],
        "sector_industry_exposure_summary": dict(exposure_summary),
        "sandbox_only": True,
        "allocator_ran": False,
        "q1_entry_written": False,
        "q2_entry_written": False,
        "typed_projection_ran": False,
        "alpha_registry_updated": False,
        "production_approval_claimed": False,
        "alpha_success_claimed": False,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
        "no_view_is_not_zero_alpha": True,
    }


def _render_report(
    summary: Mapping[str, object],
    results: pd.DataFrame,
    placebo_comparison: pd.DataFrame,
    capacity_diagnostics: pd.DataFrame,
) -> str:
    decision = str(summary["decision_label"])
    eligible_text = (
        "Q1 candidate review eligible"
        if decision == "promotable_to_Q1_candidate_review"
        else f"blocked: {decision}"
    )
    lines = [
        "# FD-S6 Revision-Confirmed Earnings Underreaction",
        "",
        f"Decision: {eligible_text}.",
        "",
        "This is not production alpha. It is a Factor Discovery Sandbox research-mode diagnostic only.",
        "Direct Q2 entry: not allowed.",
        "Allocator, Q1 writes, Q2 writes, typed projection, Alpha Registry updates, broker/order/live paths, and production approval were not run.",
        "",
        "## Boundary",
        "",
        "- Sandbox only: true",
        "- Alpha success claimed: false",
        "- Parameters tuned after output: false",
        "- No-view is not zero-alpha: true",
        "",
        "## Gate Checks",
        "",
    ]
    for name, passed in dict(summary["gate_checks"]).items():
        lines.append(f"- {name}: {str(bool(passed)).lower()}")
    lines.extend(["", "## Primary Test Rows", ""])
    primary = results[
        (results.get("candidate_variant") == "industry_neutral_score") & (results.get("period") == "test")
    ]
    if primary.empty:
        lines.append("No primary test rows were available.")
    else:
        lines.append(primary[["horizon", "mean_rank_ic", "rank_ic_t_stat", "mean_top_bottom_spread_after_cost"]].to_markdown(index=False))
    lines.extend(["", "## Placebo Summary", ""])
    if placebo_comparison.empty:
        lines.append("No placebo rows were available.")
    else:
        lines.append(
            placebo_comparison[
                ["horizon", "placebo_name", "live_mean_rank_ic", "placebo_mean_rank_ic", "placebo_beats_live_rank_ic"]
            ].to_markdown(index=False),
        )
    lines.extend(["", "## Capacity Diagnostics", ""])
    if capacity_diagnostics.empty:
        lines.append("No capacity diagnostics were available.")
    else:
        lines.append(
            capacity_diagnostics[
                ["capacity_filter", "active_row_count", "capacity_coverage_loss", "capacity_filtered_does_not_collapse"]
            ].to_markdown(index=False),
        )
    lines.extend(
        [
            "",
            "## Conservative Interpretation",
            "",
            "Do not treat this output as alpha discovery unless every preregistered gate passes. "
            "If the decision is blocked, the blocking label above is the controlling result.",
            "",
        ],
    )
    return "\n".join(lines)


def _write_csv(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    output = frame.copy() if frame is not None else pd.DataFrame(columns=columns)
    for column in columns:
        if column not in output.columns:
            output[column] = np.nan
    output[columns].to_csv(path, index=False)


def _signal_columns() -> list[str]:
    return [
        "schema_version",
        "candidate_id",
        "event_id",
        "asset_id",
        "permno",
        "ticker",
        "ibes_ticker",
        "fiscal_period",
        "announcement_date",
        "event_available_timestamp",
        "event_observable_date_plus_3td",
        "eligible_from_date",
        "signal_date",
        "signal_trading_day_offset_from_announcement",
        "next_announcement_date",
        "latest_estimate_snapshot_date",
        "prior_20d_estimate_snapshot_date",
        "prior_60d_estimate_snapshot_date",
        "latest_expected_eps",
        "prior_20d_expected_eps",
        "prior_60d_expected_eps",
        "revision_20d_raw",
        "revision_60d_raw",
        "revision_acceleration_raw",
        "revision_scale_source",
        "revision_20d_z",
        "revision_acceleration_z",
        "sue_value",
        "car3",
        "event_confirmation_raw",
        "event_confirmation_z",
        "event_confirmation_neutralized_missing",
        "recent_5d_return_raw",
        "short_reversal_guard_raw",
        "short_reversal_guard_z",
        "revision_only_score",
        "revision_plus_event_confirmation_score",
        "industry_neutral_score",
        "capacity_rank",
        "capacity_filtered_industry_neutral_score_remove_bottom_20pct_adv",
        "capacity_filtered_industry_neutral_score_remove_bottom_40pct_adv",
        "adv_63d",
        "adjusted_close",
        "sector",
        "industry",
        "coverage_status",
        "abstain_reason",
        "no_view_is_not_zero_alpha",
        "not_alpha_evidence",
        "direct_q2_entry_allowed",
    ]


def _fill_missing_signal_columns(data: pd.DataFrame) -> pd.DataFrame:
    output = data.copy()
    for column in _signal_columns():
        if column not in output.columns:
            output[column] = np.nan
    for bool_column in ("no_view_is_not_zero_alpha", "not_alpha_evidence"):
        output[bool_column] = output[bool_column].fillna(True).astype(bool)
    output["direct_q2_entry_allowed"] = output["direct_q2_entry_allowed"].fillna(False).astype(bool)
    output["coverage_status"] = output["coverage_status"].fillna("explicit_abstain")
    output["abstain_reason"] = output["abstain_reason"].fillna("unknown")
    output["sector"] = output["sector"].fillna("UNKNOWN")
    output["industry"] = output["industry"].fillna("UNKNOWN")
    return output[_signal_columns()]


def _abstain_row(
    event,
    reason: str,
    signal_date: pd.Timestamp | None = None,
    observable_date: pd.Timestamp | None = None,
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_id": CANDIDATE_ID,
        "event_id": str(getattr(event, "event_id", "")),
        "asset_id": str(getattr(event, "asset_id", "")),
        "permno": str(getattr(event, "asset_id", "")),
        "ticker": str(getattr(event, "ticker", "")),
        "ibes_ticker": str(getattr(event, "ibes_ticker", "")),
        "fiscal_period": str(getattr(event, "fiscal_period", "")),
        "announcement_date": _date_str(getattr(event, "announcement_date", pd.NaT)),
        "event_available_timestamp": _timestamp_str(getattr(event, "event_available_timestamp", pd.NaT)),
        "event_observable_date_plus_3td": _date_str(observable_date),
        "eligible_from_date": _date_str(signal_date),
        "signal_date": _date_str(signal_date),
        "next_announcement_date": _date_str(getattr(event, "next_announcement_date", pd.NaT)),
        "coverage_status": "explicit_abstain",
        "abstain_reason": reason,
        "no_view_is_not_zero_alpha": True,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }


def _asset_id(value: object) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, float) and float(value).is_integer():
        return str(int(value))
    text = str(value)
    if text.endswith(".0"):
        return text[:-2]
    return text


def _safe_float(value: object) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(numeric) if pd.notna(numeric) else np.nan


def _date_str(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).date().isoformat()


def _timestamp_str(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).isoformat()


def _timestamp_to_date(value: object) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).tz_localize(None).normalize()


def _lookup_classification(universe: pd.DataFrame, asset_id: str, signal_date: pd.Timestamp, column: str) -> str:
    if universe.empty:
        return "UNKNOWN"
    rows = universe[
        (universe["asset_id"] == asset_id)
        & (universe["membership_start"] <= pd.Timestamp(signal_date))
        & (universe["membership_end"] >= pd.Timestamp(signal_date))
    ]
    if rows.empty:
        return "UNKNOWN"
    value = rows.iloc[-1][column]
    return str(value) if pd.notna(value) else "UNKNOWN"


def _trading_day_after_offset(calendar: pd.Index, date: object, offset: int) -> pd.Timestamp | None:
    if date is None or pd.isna(date) or calendar.empty:
        return None
    date_value = pd.Timestamp(date).normalize()
    future = calendar[calendar > date_value]
    if len(future) < offset:
        return None
    return pd.Timestamp(future[offset - 1])


def _trading_day_before_offset(calendar: pd.Index, date: object, offset: int) -> pd.Timestamp | None:
    if date is None or pd.isna(date) or calendar.empty:
        return None
    date_value = pd.Timestamp(date).normalize()
    prior = calendar[calendar <= date_value]
    if len(prior) <= offset:
        return None
    return pd.Timestamp(prior[-offset - 1])


def _trading_day_distance(calendar: pd.Index, start: object, end: object) -> int:
    if start is None or end is None or pd.isna(start) or pd.isna(end):
        return 0
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    return int(((calendar > start_ts) & (calendar <= end_ts)).sum())


def _car3_return(returns: pd.DataFrame, calendar: pd.Index, asset_id: str, announcement_date: object) -> float:
    if asset_id not in returns.columns:
        return np.nan
    start_candidates = calendar[calendar >= pd.Timestamp(announcement_date).normalize()]
    if len(start_candidates) == 0:
        return np.nan
    start = pd.Timestamp(start_candidates[0])
    end = _trading_day_after_offset(calendar, start, 3)
    if end is None:
        return np.nan
    window = returns.loc[(returns.index >= start) & (returns.index <= end), asset_id].dropna()
    if len(window) < 4:
        return np.nan
    return float((1.0 + window).prod() - 1.0)


def _trailing_return(close: pd.DataFrame, calendar: pd.Index, asset_id: str, signal_date: object, days: int) -> float:
    if asset_id not in close.columns:
        return np.nan
    start = _trading_day_before_offset(calendar, signal_date, days)
    if start is None or signal_date not in close.index:
        return np.nan
    start_price = close.at[start, asset_id]
    end_price = close.at[pd.Timestamp(signal_date), asset_id]
    if pd.isna(start_price) or pd.isna(end_price) or start_price == 0:
        return np.nan
    return float(end_price / start_price - 1.0)


def _target_date_before_next_announcement(
    calendar: pd.Index,
    next_announcement_date: object,
    signal_date: pd.Timestamp,
) -> pd.Timestamp | None:
    if next_announcement_date is None or pd.isna(next_announcement_date):
        return None
    candidates = calendar[(calendar > pd.Timestamp(signal_date)) & (calendar < pd.Timestamp(next_announcement_date))]
    if len(candidates) == 0:
        return None
    return pd.Timestamp(candidates[-1])


def _forward_return(close: pd.DataFrame, asset_id: str, start_date: object, end_date: object) -> float:
    if end_date is None or asset_id not in close.columns:
        return np.nan
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    if start_ts not in close.index or end_ts not in close.index:
        return np.nan
    start_price = close.at[start_ts, asset_id]
    end_price = close.at[end_ts, asset_id]
    if pd.isna(start_price) or pd.isna(end_price) or start_price == 0:
        return np.nan
    return float(end_price / start_price - 1.0)


def _winsorized_zscore(values: Iterable[object]) -> pd.Series:
    series = pd.to_numeric(pd.Series(values), errors="coerce")
    if series.notna().sum() < 2:
        return pd.Series(np.zeros(len(series)), index=series.index, dtype="float64")
    lower = series.quantile(0.01)
    upper = series.quantile(0.99)
    clipped = series.clip(lower=lower, upper=upper)
    std = clipped.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(np.zeros(len(series)), index=series.index, dtype="float64")
    return (clipped - clipped.mean()) / std


def _neutralize(scores: pd.Series, groups: pd.Series) -> pd.Series:
    frame = pd.DataFrame({"score": pd.to_numeric(scores, errors="coerce"), "group": groups.fillna("UNKNOWN").astype(str)})
    return frame["score"] - frame.groupby("group")["score"].transform("mean")


def _average_top_turnover(top_sets: list[set[str]]) -> float:
    if len(top_sets) < 2:
        return 0.0
    turnovers = []
    for previous, current in zip(top_sets, top_sets[1:]):
        denominator = max(len(previous), 1)
        turnovers.append(1.0 - len(previous & current) / denominator)
    return float(np.mean(turnovers)) if turnovers else 0.0


def _safe_corr(left: pd.Series, right: pd.Series) -> float:
    frame = pd.DataFrame({"left": pd.to_numeric(left, errors="coerce"), "right": pd.to_numeric(right, errors="coerce")})
    frame = frame.dropna()
    if len(frame) < 2:
        return np.nan
    if frame["left"].std(ddof=0) == 0 or frame["right"].std(ddof=0) == 0:
        return np.nan
    return float(frame["left"].corr(frame["right"]))


def _capacity_filter_for_variant(variant: str) -> str:
    if variant.endswith("remove_bottom_20pct_adv"):
        return "remove_bottom_20pct_adv"
    if variant.endswith("remove_bottom_40pct_adv"):
        return "remove_bottom_40pct_adv"
    return "none"


def _row_float(row: object, field: str, default: float = np.nan) -> float:
    if row is None:
        return default
    if isinstance(row, Mapping):
        value = row.get(field, default)
    else:
        value = getattr(row, field, default)
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(numeric) if pd.notna(numeric) else default


def _live_beats_placebo(placebo_rows: pd.DataFrame) -> bool:
    if placebo_rows.empty:
        return False
    clean = placebo_rows.dropna(subset=["live_mean_rank_ic", "placebo_mean_rank_ic"])
    if clean.empty:
        return False
    return bool((clean["live_mean_rank_ic"] > clean["placebo_mean_rank_ic"]).all())


def _count_nonfragile_variants(results: pd.DataFrame) -> int:
    if results.empty:
        return 0
    count = 0
    for variant, group in results[results["period"] == "test"].groupby("candidate_variant"):
        if str(variant).startswith("capacity_filtered"):
            continue
        horizons = {
            str(row.horizon): row
            for row in group[group["horizon"].isin(["20d", "to_next_announcement"])].itertuples(index=False)
        }
        if all(
            _row_float(horizons.get(horizon), "mean_rank_ic") > 0.0
            and _row_float(horizons.get(horizon), "mean_top_bottom_spread_after_cost") > 0.0
            for horizon in ("20d", "to_next_announcement")
        ):
            count += 1
    return count


def _deterministic_random_score(row: pd.Series) -> float:
    key = f"{row.get('asset_id')}|{row.get('signal_date')}|{row.get('event_id')}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    integer = int(digest[:12], 16)
    return (integer / float(16**12 - 1)) * 2.0 - 1.0
