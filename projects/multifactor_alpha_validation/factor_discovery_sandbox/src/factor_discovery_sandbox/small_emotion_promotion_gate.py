"""Promotion Gate audit for the frozen small-cap emotion Q1 candidate."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


STAGE = "PG-SMALL-EMOTION-01"
ALLOWED_DECISIONS = {
    "promote_to_q2_candidate",
    "promising_needs_full_replay_or_breadth",
    "bounded_smoke_only",
    "reject_overfit_or_data_artifact",
}


@dataclass(frozen=True)
class SmallEmotionPromotionGateResult:
    """Promotion Gate artifacts and decision summary."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_promotion_gate(
    *,
    measurement_spec_path: str | Path,
    q1_output_dir: str | Path,
    output_dir: str | Path,
    required_measurement_spec_hash: str,
    search_grid_path: str | Path | None = None,
    stress_notional_usd: float = 25_000.0,
) -> SmallEmotionPromotionGateResult:
    """Run PG-SMALL-EMOTION-01 from a frozen MeasurementSpec and Q1 outputs."""

    spec_path = Path(measurement_spec_path)
    q1_path = Path(q1_output_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    spec_hash = _file_hash(spec_path)
    if spec_hash != required_measurement_spec_hash:
        raise ValueError(
            f"MeasurementSpec hash mismatch: expected {required_measurement_spec_hash}, observed {spec_hash}"
        )

    spec = yaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}
    primary_window = str(spec.get("label_contract", {}).get("primary_window", "post_1_22"))
    q1_summary = _read_json(q1_path / "q1_decision_summary.json")
    coverage = _read_json(q1_path / "data_coverage_report.json")
    events = _read_csv(q1_path / "q1_event_panel.csv")
    labels = _read_csv(q1_path / "q1_window_return_panel.csv")
    falsifier = _read_csv(q1_path / "q1_falsifier_report.csv")
    policy = _read_csv(q1_path / "q1_policy_guard_report.csv")

    primary = labels[
        labels.get("window", pd.Series(dtype=str)).eq(primary_window)
        & labels.get("label_status", pd.Series(dtype=str)).eq("observed")
    ].copy()
    joined = _join_primary_events(events, primary)
    full_no_cap = coverage.get("source_row_limit") in {None, "", "null"}

    search = _search_burden_audit(
        search_grid_path=Path(search_grid_path) if search_grid_path else None,
        q1_summary=q1_summary,
        falsifier=falsifier,
        full_no_cap=full_no_cap,
    )
    tail = _tail_concentration_audit(joined)
    anomaly = _data_anomaly_audit(joined)
    cost = _cost_liquidity_gate(joined, stress_notional_usd=stress_notional_usd)
    time_breadth = _time_breadth_audit(joined)

    decision, stop_reason = _promotion_decision(
        q1_summary=q1_summary,
        full_no_cap=full_no_cap,
        search=search,
        tail=tail,
        anomaly=anomaly,
        cost=cost,
        time_breadth=time_breadth,
    )
    summary = _summary(
        measurement_spec_hash=spec_hash,
        primary_window=primary_window,
        q1_summary=q1_summary,
        full_no_cap=full_no_cap,
        decision=decision,
        stop_reason=stop_reason,
        search=search,
        tail=tail,
        anomaly=anomaly,
        cost=cost,
        time_breadth=time_breadth,
        policy=policy,
        coverage=coverage,
    )
    _write_outputs(artifacts, search, tail, anomaly, cost, time_breadth, summary)
    return SmallEmotionPromotionGateResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "search_burden_audit": output_path / "pg_search_burden_audit.json",
        "tail_concentration_audit": output_path / "pg_tail_concentration_audit.json",
        "data_anomaly_audit": output_path / "pg_data_anomaly_audit.json",
        "cost_liquidity_gate": output_path / "pg_cost_liquidity_gate.csv",
        "time_breadth_audit": output_path / "pg_time_breadth_audit.json",
        "promotion_decision_summary": output_path / "pg_decision_summary.json",
        "promotion_gate_report": output_path / "pg_small_emotion_report.md",
    }


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _join_primary_events(events: pd.DataFrame, primary: pd.DataFrame) -> pd.DataFrame:
    if events.empty or primary.empty:
        return pd.DataFrame()
    return primary.merge(events, on=["event_id", "asset_id", "date", "event_month"], how="left", suffixes=("_label", ""))


def _search_burden_audit(
    *,
    search_grid_path: Path | None,
    q1_summary: dict[str, object],
    falsifier: pd.DataFrame,
    full_no_cap: bool,
) -> dict[str, object]:
    row_count = 0
    if search_grid_path and search_grid_path.exists():
        row_count = int(sum(1 for _ in search_grid_path.open("r", encoding="utf-8")) - 1)
    dominance = int(falsifier.get("falsifier_dominates_live", pd.Series(dtype=bool)).astype(bool).sum()) if not falsifier.empty else 0
    status = "fail" if dominance > 0 else "pass"
    if status == "pass" and row_count >= 10_000:
        status = "warning"
    return {
        "schema_version": "small_emotion_pg_search_burden_audit.v1",
        "stage": STAGE,
        "search_grid_row_count": max(0, row_count),
        "q1_decision": q1_summary.get("q1_decision"),
        "full_no_cap_q1_observed": bool(full_no_cap),
        "falsifier_dominance_count": dominance,
        "sweep_adjusted_placebo_status": status,
        "search_burden_warning": bool(row_count >= 10_000),
        "no_view_not_zero_alpha": True,
    }


def _tail_concentration_audit(joined: pd.DataFrame) -> dict[str, object]:
    if joined.empty:
        return {
            "schema_version": "small_emotion_pg_tail_concentration_audit.v1",
            "stage": STAGE,
            "observed_primary_label_count": 0,
            "tail_status": "fail",
            "sector_status": "unavailable",
        }
    directional = pd.to_numeric(joined["directional_return"], errors="coerce").dropna()
    abs_directional = directional.abs()
    denominator = float(abs_directional.sum()) if not abs_directional.empty else np.nan
    top5_share = float(abs_directional.sort_values(ascending=False).head(5).sum() / denominator) if denominator and denominator > 0 else np.nan
    issuer_share = _max_share(joined, "asset_id")
    month_share = _max_share(joined, "event_month")
    sector_col = "sector" if "sector" in joined.columns else None
    sector_share = _max_share(joined, sector_col) if sector_col else np.nan
    sector_status = "available" if sector_col else "unavailable"
    lower = directional.quantile(0.01) if not directional.empty else np.nan
    upper = directional.quantile(0.99) if not directional.empty else np.nan
    winsorized = directional.clip(lower=lower, upper=upper) if pd.notna(lower) and pd.notna(upper) else directional
    tail_status = "pass"
    if issuer_share > 0.25 or month_share > 0.50 or (pd.notna(top5_share) and top5_share > 0.60):
        tail_status = "warning"
    return {
        "schema_version": "small_emotion_pg_tail_concentration_audit.v1",
        "stage": STAGE,
        "observed_primary_label_count": int(len(joined)),
        "top5_abs_directional_return_share": top5_share,
        "issuer_concentration_max_share": issuer_share,
        "month_concentration_max_share": month_share,
        "sector_concentration_max_share": sector_share,
        "sector_status": sector_status,
        "raw_mean_directional_return": float(directional.mean()) if not directional.empty else np.nan,
        "winsorized_mean_directional_return": float(winsorized.mean()) if not winsorized.empty else np.nan,
        "median_directional_return": float(directional.median()) if not directional.empty else np.nan,
        "hit_rate": float((directional > 0.0).mean()) if not directional.empty else np.nan,
        "tail_status": tail_status,
        "no_view_not_zero_alpha": True,
    }


def _data_anomaly_audit(joined: pd.DataFrame) -> dict[str, object]:
    if joined.empty:
        return {
            "schema_version": "small_emotion_pg_data_anomaly_audit.v1",
            "stage": STAGE,
            "anomaly_status": "fail",
            "stale_event_count": 0,
            "zero_volume_event_count": 0,
            "delisting_event_count": 0,
        }
    stale = _bool_or_threshold_count(joined, "stale_roll_5", threshold=4)
    zero_flag = _boolean_mask(joined, "zero_volume")
    zero_volume_mask = pd.to_numeric(joined.get("volume", pd.Series(dtype=float)), errors="coerce").eq(0)
    zero_volume = int((zero_flag | zero_volume_mask).sum())
    delisting = _bool_or_threshold_count(joined, "delisting_within_label_window", threshold=0)
    bad_prints = int((pd.to_numeric(joined.get("shock_return", pd.Series(dtype=float)), errors="coerce").abs() > 5.0).sum())
    anomaly_status = "pass" if stale == 0 and zero_volume == 0 and delisting == 0 and bad_prints == 0 else "fail"
    return {
        "schema_version": "small_emotion_pg_data_anomaly_audit.v1",
        "stage": STAGE,
        "stale_event_count": stale,
        "zero_volume_event_count": zero_volume,
        "delisting_event_count": delisting,
        "bad_print_event_count": bad_prints,
        "halt_audit_status": "unavailable" if "halted" not in joined.columns else "available",
        "split_or_corporate_action_audit_status": "unavailable" if "split_factor" not in joined.columns else "available",
        "anomaly_status": anomaly_status,
        "no_view_not_zero_alpha": True,
    }


def _cost_liquidity_gate(joined: pd.DataFrame, *, stress_notional_usd: float) -> pd.DataFrame:
    adv = pd.to_numeric(joined.get("adv20", pd.Series(dtype=float)), errors="coerce").replace(0.0, np.nan)
    spread = pd.to_numeric(joined.get("bid_ask_spread", pd.Series(dtype=float)), errors="coerce")
    participation_25k = stress_notional_usd / adv
    participation_100k = 100_000.0 / adv
    rows = [
        _cost_row("adv_participation_25k_p95", _quantile(participation_25k, 0.95), 0.10, "<="),
        _cost_row("adv_participation_100k_p95", _quantile(participation_100k, 0.95), 0.35, "<="),
        _cost_row("spread_proxy_p95", _quantile(spread, 0.95), 0.20, "<="),
        _cost_row("slippage_stress_p95", _quantile(spread.fillna(0.0) * 0.5 + participation_25k.fillna(0.0) * 0.10, 0.95), 0.05, "<="),
        {
            "schema_version": "small_emotion_pg_cost_liquidity_gate.v1",
            "stage": STAGE,
            "metric": "entry_exit_timing",
            "value": "next_trading_day_after_shock_close",
            "threshold": "required",
            "comparison": "==",
            "status": "pass",
            "no_view_not_zero_alpha": True,
        },
    ]
    return pd.DataFrame(rows)


def _time_breadth_audit(joined: pd.DataFrame) -> dict[str, object]:
    if joined.empty:
        return {
            "schema_version": "small_emotion_pg_time_breadth_audit.v1",
            "stage": STAGE,
            "breadth_status": "fail",
        }
    dates = pd.to_datetime(joined["date"], errors="coerce")
    year_count = int(dates.dt.year.nunique())
    regime_count = int(joined["market_regime"].nunique()) if "market_regime" in joined.columns else 0
    observed = int(len(joined))
    months = int(joined["event_month"].nunique()) if "event_month" in joined.columns else 0
    status = "pass" if observed >= 50 and months >= 6 and year_count >= 2 else "warning"
    return {
        "schema_version": "small_emotion_pg_time_breadth_audit.v1",
        "stage": STAGE,
        "observed_primary_label_count": observed,
        "event_month_count": months,
        "year_count": year_count,
        "regime_count": regime_count,
        "first_event_date": dates.min().date().isoformat() if dates.notna().any() else "",
        "last_event_date": dates.max().date().isoformat() if dates.notna().any() else "",
        "breadth_status": status,
        "no_view_not_zero_alpha": True,
    }


def _promotion_decision(
    *,
    q1_summary: dict[str, object],
    full_no_cap: bool,
    search: dict[str, object],
    tail: dict[str, object],
    anomaly: dict[str, object],
    cost: pd.DataFrame,
    time_breadth: dict[str, object],
) -> tuple[str, str]:
    if not full_no_cap:
        return "bounded_smoke_only", "bounded_smoke_only_not_promoted"
    if q1_summary.get("q1_decision") != "passed_q1_research_review":
        return "reject_overfit_or_data_artifact", "q1_failed_full_replay"
    cost_fail = bool((cost["status"] == "fail").any()) if not cost.empty else True
    if search.get("sweep_adjusted_placebo_status") == "fail" or anomaly.get("anomaly_status") == "fail" or cost_fail:
        return "reject_overfit_or_data_artifact", "hard_gate_failed"
    if tail.get("tail_status") != "pass" or time_breadth.get("breadth_status") != "pass":
        return "promising_needs_full_replay_or_breadth", "tail_or_time_breadth_warning"
    return "promote_to_q2_candidate", "promotion_gate_passed_no_q2_run"


def _summary(
    *,
    measurement_spec_hash: str,
    primary_window: str,
    q1_summary: dict[str, object],
    full_no_cap: bool,
    decision: str,
    stop_reason: str,
    search: dict[str, object],
    tail: dict[str, object],
    anomaly: dict[str, object],
    cost: pd.DataFrame,
    time_breadth: dict[str, object],
    policy: pd.DataFrame,
    coverage: dict[str, object],
) -> dict[str, object]:
    if decision not in ALLOWED_DECISIONS:
        raise ValueError(f"unsupported promotion decision: {decision}")
    return {
        "schema_version": "small_emotion_promotion_gate_summary.v1",
        "stage": STAGE,
        "measurement_spec_id": q1_summary.get("measurement_spec_id"),
        "measurement_spec_hash": measurement_spec_hash,
        "required_measurement_spec_hash": measurement_spec_hash,
        "primary_window": primary_window,
        "q1_decision": q1_summary.get("q1_decision"),
        "full_no_cap_q1_required": True,
        "full_no_cap_q1_observed": bool(full_no_cap),
        "q1_price_row_count": coverage.get("price_row_count"),
        "observed_primary_label_count": q1_summary.get("observed_primary_label_count"),
        "mean_primary_directional_return": q1_summary.get("mean_primary_directional_return"),
        "oos_test_mean_directional_return": q1_summary.get("oos_test_mean_directional_return"),
        "falsifier_dominance_count": q1_summary.get("falsifier_dominance_count"),
        "policy_breach_count": int(policy.get("guard_breached", pd.Series(dtype=bool)).astype(bool).sum()) if not policy.empty else 0,
        "search_burden_status": search.get("sweep_adjusted_placebo_status"),
        "tail_status": tail.get("tail_status"),
        "anomaly_status": anomaly.get("anomaly_status"),
        "cost_liquidity_status": "fail" if not cost.empty and (cost["status"] == "fail").any() else "pass",
        "time_breadth_status": time_breadth.get("breadth_status"),
        "promotion_decision": decision,
        "stop_reason": stop_reason,
        "promotion_gate_allowed": decision == "promote_to_q2_candidate",
        "q2_entry_allowed": False,
        "optimizer_entry_allowed": False,
        "expected_return_panel_written": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _write_outputs(
    artifacts: dict[str, Path],
    search: dict[str, object],
    tail: dict[str, object],
    anomaly: dict[str, object],
    cost: pd.DataFrame,
    time_breadth: dict[str, object],
    summary: dict[str, object],
) -> None:
    artifacts["search_burden_audit"].write_text(json.dumps(search, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["tail_concentration_audit"].write_text(json.dumps(tail, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["data_anomaly_audit"].write_text(json.dumps(anomaly, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    cost.to_csv(artifacts["cost_liquidity_gate"], index=False)
    artifacts["time_breadth_audit"].write_text(json.dumps(time_breadth, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["promotion_decision_summary"].write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["promotion_gate_report"].write_text(_report(summary), encoding="utf-8")


def _report(summary: dict[str, object]) -> str:
    return "\n".join(
        [
            "# PG-SMALL-EMOTION-01 Promotion Gate",
            "",
            "This is a Promotion Gate only review. It does not run Q2, optimizer, portfolio construction, Alpha Registry, paper, broker, order, live, or production workflows.",
            "",
            f"- measurement_spec_id: {summary['measurement_spec_id']}",
            f"- measurement_spec_hash: {summary['measurement_spec_hash']}",
            f"- full_no_cap_q1_observed: {summary['full_no_cap_q1_observed']}",
            f"- q1_decision: {summary['q1_decision']}",
            f"- promotion_decision: {summary['promotion_decision']}",
            f"- stop_reason: {summary['stop_reason']}",
            f"- promotion_gate_allowed: {summary['promotion_gate_allowed']}",
            f"- q2_entry_allowed: {summary['q2_entry_allowed']}",
            "",
        ]
    )


def _cost_row(metric: str, value: float, threshold: float, comparison: str) -> dict[str, object]:
    status = "pass" if pd.notna(value) and value <= threshold else "fail"
    return {
        "schema_version": "small_emotion_pg_cost_liquidity_gate.v1",
        "stage": STAGE,
        "metric": metric,
        "value": value,
        "threshold": threshold,
        "comparison": comparison,
        "status": status,
        "no_view_not_zero_alpha": True,
    }


def _quantile(values: pd.Series, q: float) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    return float(clean.quantile(q)) if not clean.empty else np.nan


def _max_share(frame: pd.DataFrame, column: str | None) -> float:
    if not column or column not in frame or frame.empty:
        return np.nan
    counts = frame[column].value_counts(normalize=True)
    return float(counts.max()) if not counts.empty else np.nan


def _bool_or_threshold_count(frame: pd.DataFrame, column: str, *, threshold: float) -> int:
    if column not in frame:
        return 0
    series = frame[column]
    if series.dtype == bool:
        return int(series.sum())
    if series.astype(str).str.lower().isin({"true", "false"}).all():
        return int(series.astype(str).str.lower().eq("true").sum())
    return int((pd.to_numeric(series, errors="coerce") >= threshold).sum())


def _boolean_mask(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(False, index=frame.index)
    series = frame[column]
    if series.dtype == bool:
        return series.fillna(False)
    return series.astype(str).str.lower().eq("true")


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()
