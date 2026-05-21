from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from multifactor_alpha_validation.full_market_locked_validation import (
    _candidate_feature,
    _candidate_label,
    _chronological_thirds,
    _locked_selection,
    _lock_candidate,
)
from multifactor_alpha_validation.full_market_sweep import (
    _add_template_scores,
    _build_feature_and_label_panel,
    _profile_metrics,
    _read_returns,
)


@dataclass(frozen=True)
class FullMarketCandidateAuditResult:
    summary_path: str
    temporal_breadth_path: str
    tail_concentration_path: str
    data_anomaly_audit_path: str
    cost_capacity_audit_path: str
    benchmark_residual_audit_path: str
    report_path: str
    validation_status: str
    decision_label: str


_TEMPORAL_COLUMNS = [
    "schema_version",
    "period_type",
    "split",
    "month",
    "start_date",
    "end_date",
    "sample_count",
    "mean_return",
    "t_stat",
    "hit_rate",
    "issuer_breadth",
    "not_alpha_evidence",
]
_TAIL_COLUMNS = [
    "schema_version",
    "rank",
    "date",
    "instrument_id",
    "label",
    "abs_label",
    "issuer_abs_share",
    "top10_abs_share",
    "not_alpha_evidence",
]
_RESIDUAL_COLUMNS = [
    "schema_version",
    "split",
    "sample_count",
    "mean_return",
    "benchmark_mean_return",
    "benchmark_residual_mean_return",
    "not_alpha_evidence",
]
_DEFAULT_MARKET_REFERENCE = Path("data/universe/us_universe_reference.csv")
_DEFAULT_MARKET_SNAPSHOT = Path("data/universe/us_universe_market_2026-03-27.csv")
_NOTIONAL_SCENARIOS = (25_000.0, 100_000.0, 250_000.0, 1_000_000.0)
_SLIPPAGE_K = 3.498400399110418
_SLIPPAGE_ALPHA = 0.6


def _load_market_capacity_inputs(
    *,
    market_reference_path: Path | None,
    market_snapshot_path: Path | None,
) -> pd.DataFrame:
    reference = _read_market_frame(market_reference_path or _DEFAULT_MARKET_REFERENCE)
    snapshot = _read_market_frame(market_snapshot_path or _DEFAULT_MARKET_SNAPSHOT)
    if reference.empty and snapshot.empty:
        return pd.DataFrame(columns=["instrument_id"])
    if reference.empty:
        merged = snapshot.copy()
    elif snapshot.empty:
        merged = reference.copy()
    else:
        merged = reference.merge(snapshot, on="instrument_id", how="outer", suffixes=("_reference", "_snapshot"))
    return _normalize_market_inputs(merged)


def _read_market_frame(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    key = "ticker" if "ticker" in frame.columns else "instrument_id" if "instrument_id" in frame.columns else ""
    if not key:
        return pd.DataFrame()
    frame = frame.copy()
    frame["instrument_id"] = frame[key].astype(str).str.strip().str.upper()
    return frame


def _first_numeric(frame: pd.DataFrame, candidates: list[str]) -> pd.Series:
    for column in candidates:
        if column in frame.columns:
            values = pd.to_numeric(frame[column], errors="coerce")
            if values.notna().any():
                return values
    return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="Float64")


def _first_text(frame: pd.DataFrame, candidates: list[str], default: str = "") -> pd.Series:
    for column in candidates:
        if column in frame.columns:
            values = frame[column].astype(str).str.strip()
            if values.ne("").any():
                return values
    return pd.Series([default] * len(frame), index=frame.index, dtype="object")


def _normalize_market_inputs(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["instrument_id"])
    normalized = pd.DataFrame({"instrument_id": frame["instrument_id"].astype(str).str.strip().str.upper()})
    normalized["close"] = _first_numeric(frame, ["close_snapshot", "close", "close_2026_03_27", "close_reference"])
    normalized["adv_shares"] = _first_numeric(frame, ["adv_shares", "adv_shares_snapshot", "avg_adv_20d", "avg_adv_20d_reference"])
    normalized["market_cap"] = _first_numeric(frame, ["market_cap", "market_cap_reference"])
    normalized["liquidity_bucket"] = _first_text(frame, ["liquidity_bucket", "liquidity_bucket_reference"], default="unknown").str.lower()
    normalized["adv_dollars"] = normalized["adv_shares"] * normalized["close"]
    normalized = normalized.drop_duplicates(subset=["instrument_id"], keep="first")
    return normalized


def run_full_market_candidate_full_audit(
    returns_panel_path: Path,
    supervisor_dir: Path,
    output_dir: Path,
    market_reference_path: Path | None = None,
    market_snapshot_path: Path | None = None,
) -> FullMarketCandidateAuditResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "candidate_full_audit_summary.json"
    temporal_path = output_dir / "candidate_temporal_breadth.csv"
    tail_path = output_dir / "candidate_tail_concentration.csv"
    anomaly_path = output_dir / "candidate_data_anomaly_audit.json"
    cost_capacity_path = output_dir / "candidate_cost_capacity_audit.json"
    residual_path = output_dir / "candidate_benchmark_residual_audit.csv"
    report_path = output_dir / "candidate_full_audit_report.md"
    market_inputs = _load_market_capacity_inputs(
        market_reference_path=market_reference_path,
        market_snapshot_path=market_snapshot_path,
    )

    manifest_path = supervisor_dir / "frozen_candidate_manifest.json"
    manifest = _read_json(manifest_path)
    candidate = manifest.get("candidate") if isinstance(manifest.get("candidate"), dict) else None
    if candidate is None:
        temporal = pd.DataFrame(columns=_TEMPORAL_COLUMNS)
        tail = pd.DataFrame(columns=_TAIL_COLUMNS)
        residual = pd.DataFrame(columns=_RESIDUAL_COLUMNS)
        anomaly = _data_anomaly_audit(pd.DataFrame(), pd.DataFrame(), market_inputs)
        cost_capacity = _cost_capacity_audit(pd.DataFrame(), market_inputs)
        summary = _summary(
            returns_panel_path=returns_panel_path,
            supervisor_dir=supervisor_dir,
            manifest_path=manifest_path,
            manifest=manifest,
            candidate=None,
            feature_column="",
            label_column="",
            raw_returns=pd.DataFrame(),
            selected=pd.DataFrame(),
            temporal=temporal,
            tail=tail,
            anomaly=anomaly,
            cost_capacity=cost_capacity,
            decision_label="blocked_missing_frozen_candidate",
            unavailable_reason="missing_frozen_candidate_manifest_or_candidate",
        )
        _write_outputs(summary_path, temporal_path, tail_path, anomaly_path, cost_capacity_path, residual_path, report_path, summary, temporal, tail, anomaly, cost_capacity, residual)
        return _result(summary_path, temporal_path, tail_path, anomaly_path, cost_capacity_path, residual_path, report_path, "blocked", "blocked_missing_frozen_candidate")

    locked_candidate = _lock_candidate(candidate)
    raw_returns = _read_returns(returns_panel_path)
    if raw_returns.empty:
        temporal = pd.DataFrame(columns=_TEMPORAL_COLUMNS)
        tail = pd.DataFrame(columns=_TAIL_COLUMNS)
        residual = pd.DataFrame(columns=_RESIDUAL_COLUMNS)
        anomaly = _data_anomaly_audit(raw_returns, pd.DataFrame(), market_inputs)
        cost_capacity = _cost_capacity_audit(pd.DataFrame(), market_inputs)
        summary = _summary(
            returns_panel_path=returns_panel_path,
            supervisor_dir=supervisor_dir,
            manifest_path=manifest_path,
            manifest=manifest,
            candidate=locked_candidate,
            feature_column="",
            label_column="",
            raw_returns=raw_returns,
            selected=pd.DataFrame(),
            temporal=temporal,
            tail=tail,
            anomaly=anomaly,
            cost_capacity=cost_capacity,
            decision_label="blocked_data_coverage",
            unavailable_reason="missing_or_invalid_returns_panel",
        )
        _write_outputs(summary_path, temporal_path, tail_path, anomaly_path, cost_capacity_path, residual_path, report_path, summary, temporal, tail, anomaly, cost_capacity, residual)
        return _result(summary_path, temporal_path, tail_path, anomaly_path, cost_capacity_path, residual_path, report_path, "blocked", "blocked_data_coverage")

    panel = _build_feature_and_label_panel(raw_returns)
    work, _templates = _add_template_scores(panel)
    feature = _candidate_feature(locked_candidate, work)
    label = _candidate_label(locked_candidate)
    if feature is None or label not in work.columns:
        temporal = pd.DataFrame(columns=_TEMPORAL_COLUMNS)
        tail = pd.DataFrame(columns=_TAIL_COLUMNS)
        residual = pd.DataFrame(columns=_RESIDUAL_COLUMNS)
        anomaly = _data_anomaly_audit(raw_returns, pd.DataFrame(), market_inputs)
        cost_capacity = _cost_capacity_audit(pd.DataFrame(), market_inputs)
        summary = _summary(
            returns_panel_path=returns_panel_path,
            supervisor_dir=supervisor_dir,
            manifest_path=manifest_path,
            manifest=manifest,
            candidate=locked_candidate,
            feature_column=feature or "",
            label_column=label,
            raw_returns=raw_returns,
            selected=pd.DataFrame(),
            temporal=temporal,
            tail=tail,
            anomaly=anomaly,
            cost_capacity=cost_capacity,
            decision_label="blocked_data_coverage",
            unavailable_reason="invalid_or_unavailable_frozen_candidate",
        )
        _write_outputs(summary_path, temporal_path, tail_path, anomaly_path, cost_capacity_path, residual_path, report_path, summary, temporal, tail, anomaly, cost_capacity, residual)
        return _result(summary_path, temporal_path, tail_path, anomaly_path, cost_capacity_path, residual_path, report_path, "blocked", "blocked_data_coverage")

    split_dates = _chronological_thirds(work["date"])
    selected = _locked_selection(work, feature, label, locked_candidate)
    benchmark = _same_date_benchmark(work, label)
    selected = selected.merge(benchmark, on="date", how="left")
    selected["benchmark_residual_label"] = selected["label"] - selected["benchmark_label"]
    temporal = _temporal_breadth(selected, split_dates)
    tail = _tail_concentration(selected)
    anomaly = _data_anomaly_audit(raw_returns, selected, market_inputs)
    cost_capacity = _cost_capacity_audit(selected, market_inputs)
    residual = _benchmark_residual_audit(selected, split_dates)
    decision_label = _decision_label(selected, tail, anomaly)
    validation_status = "blocked" if decision_label.startswith("blocked") else "evaluated"
    summary = _summary(
        returns_panel_path=returns_panel_path,
        supervisor_dir=supervisor_dir,
        manifest_path=manifest_path,
        manifest=manifest,
        candidate=locked_candidate,
        feature_column=feature,
        label_column=label,
        raw_returns=raw_returns,
        selected=selected,
        temporal=temporal,
        tail=tail,
        anomaly=anomaly,
        cost_capacity=cost_capacity,
        decision_label=decision_label,
        unavailable_reason="",
    )
    _write_outputs(summary_path, temporal_path, tail_path, anomaly_path, cost_capacity_path, residual_path, report_path, summary, temporal, tail, anomaly, cost_capacity, residual)
    return _result(summary_path, temporal_path, tail_path, anomaly_path, cost_capacity_path, residual_path, report_path, validation_status, decision_label)


def _same_date_benchmark(work: pd.DataFrame, label: str) -> pd.DataFrame:
    benchmark = work[["date", label]].rename(columns={label: "benchmark_label"}).copy()
    benchmark["benchmark_label"] = pd.to_numeric(benchmark["benchmark_label"], errors="coerce")
    return benchmark.groupby("date", as_index=False)["benchmark_label"].mean()


def _temporal_breadth(selected: pd.DataFrame, split_dates: dict[str, pd.Index]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split, dates in split_dates.items():
        frame = selected[selected["date"].isin(dates)].copy()
        rows.append(_temporal_row("split", split, "", frame))
    for month, frame in selected.groupby(selected["date"].dt.to_period("M"), sort=True):
        rows.append(_temporal_row("month", "", str(month), frame))
    return pd.DataFrame(rows, columns=_TEMPORAL_COLUMNS)


def _temporal_row(period_type: str, split: str, month: str, frame: pd.DataFrame) -> dict[str, Any]:
    metrics = _profile_metrics(frame[["date", "instrument_id", "label"]])
    return {
        "schema_version": "full_market_candidate_temporal_breadth.v1",
        "period_type": period_type,
        "split": split,
        "month": month,
        "start_date": _date_bound(frame["date"], "min"),
        "end_date": _date_bound(frame["date"], "max"),
        "sample_count": int(metrics["sample_count"]),
        "mean_return": float(metrics["mean_return"]),
        "t_stat": float(metrics["t_stat"]),
        "hit_rate": float(metrics["hit_rate"]),
        "issuer_breadth": int(metrics["issuer_breadth"]),
        "not_alpha_evidence": True,
    }


def _tail_concentration(selected: pd.DataFrame) -> pd.DataFrame:
    if selected.empty:
        return pd.DataFrame(columns=_TAIL_COLUMNS)
    work = selected[["date", "instrument_id", "label"]].copy()
    work["abs_label"] = work["label"].abs()
    total_abs = max(float(work["abs_label"].sum()), 1e-12)
    issuer_abs = work.groupby("instrument_id")["abs_label"].sum() / total_abs
    top = (
        work.sort_values(["abs_label", "date", "instrument_id"], ascending=[False, True, True])
        .head(10)
        .reset_index(drop=True)
    )
    top10_abs_share = float(top["abs_label"].sum() / total_abs) if not top.empty else 0.0
    top["schema_version"] = "full_market_candidate_tail_concentration.v1"
    top["rank"] = top.index + 1
    top["date"] = top["date"].dt.date.astype(str)
    top["issuer_abs_share"] = top["instrument_id"].map(issuer_abs).fillna(0.0)
    top["top10_abs_share"] = top10_abs_share
    top["not_alpha_evidence"] = True
    return top[_TAIL_COLUMNS]


def _data_anomaly_audit(raw_returns: pd.DataFrame, selected: pd.DataFrame, market_inputs: pd.DataFrame) -> dict[str, Any]:
    returns = pd.to_numeric(raw_returns["return"], errors="coerce") if "return" in raw_returns.columns else pd.Series(dtype=float)
    selected_instruments = set(selected["instrument_id"].astype(str)) if "instrument_id" in selected.columns else set()
    selected_date_min = selected["date"].min() if "date" in selected.columns and not selected.empty else pd.NaT
    selected_date_max = selected["date"].max() if "date" in selected.columns and not selected.empty else pd.NaT
    raw = raw_returns.copy()
    selected_path_extreme_count = 0
    selected_market_coverage_share = _selected_market_coverage_share(selected, market_inputs)
    if not raw.empty and selected_instruments and pd.notna(selected_date_min) and pd.notna(selected_date_max):
        raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
        raw["instrument_id"] = raw["instrument_id"].astype(str)
        selected_path_returns = pd.to_numeric(
            raw.loc[
                raw["instrument_id"].isin(selected_instruments)
                & raw["date"].between(selected_date_min, selected_date_max),
                "return",
            ],
            errors="coerce",
        )
        selected_path_extreme_count = int((selected_path_returns.abs() > 0.35).sum())
    return {
        "schema_version": "full_market_candidate_data_anomaly_audit.v1",
        "returns_row_count": int(len(raw_returns)),
        "selected_row_count": int(len(selected)),
        "extreme_return_row_count": int((returns.abs() > 0.35).sum()),
        "selected_path_extreme_return_row_count": selected_path_extreme_count,
        "zero_return_share": float((returns == 0).mean()) if len(returns) else 0.0,
        "missing_volume_inputs": selected_market_coverage_share <= 0.0,
        "adv_proxy_available": selected_market_coverage_share > 0.0,
        "selected_market_coverage_share": selected_market_coverage_share,
        "stale_price_proxy_available": False,
        "delisting_inputs_available": False,
        "not_alpha_evidence": True,
    }


def _cost_capacity_audit(selected: pd.DataFrame, market_inputs: pd.DataFrame) -> dict[str, Any]:
    if selected.empty or market_inputs.empty:
        return {
            "schema_version": "full_market_candidate_cost_capacity_audit.v1",
            "adv_inputs_available": False,
            "spread_inputs_available": False,
            "real_spread_inputs_available": False,
            "capacity_status": "cost_capacity_inputs_unavailable",
            "fabricated_capacity": False,
            "not_alpha_evidence": True,
        }
    joined = selected[["instrument_id"]].copy()
    joined["instrument_id"] = joined["instrument_id"].astype(str).str.strip().str.upper()
    joined = joined.merge(market_inputs, on="instrument_id", how="left")
    adv = pd.to_numeric(joined.get("adv_shares"), errors="coerce")
    adv_dollars = pd.to_numeric(joined.get("adv_dollars"), errors="coerce")
    close = pd.to_numeric(joined.get("close"), errors="coerce")
    valid = joined[adv.gt(0) & close.gt(0)].copy()
    coverage_share = float(len(valid) / len(joined)) if len(joined) else 0.0
    if valid.empty:
        return {
            "schema_version": "full_market_candidate_cost_capacity_audit.v1",
            "adv_inputs_available": False,
            "spread_inputs_available": False,
            "real_spread_inputs_available": False,
            "selected_row_count": int(len(joined)),
            "market_input_coverage_share": coverage_share,
            "capacity_status": "cost_capacity_inputs_unavailable",
            "fabricated_capacity": False,
            "not_alpha_evidence": True,
        }
    valid["half_spread_bps_proxy"] = valid["liquidity_bucket"].map(_half_spread_proxy_bps).fillna(8.0)
    scenarios = [_capacity_scenario_row(valid, notional) for notional in _NOTIONAL_SCENARIOS]
    return {
        "schema_version": "full_market_candidate_cost_capacity_audit.v1",
        "adv_inputs_available": True,
        "spread_inputs_available": True,
        "real_spread_inputs_available": False,
        "spread_proxy_source": "liquidity_bucket_half_spread_heuristic",
        "adv_source": "data/universe/us_universe_market_2026-03-27.csv plus data/universe/us_universe_reference.csv",
        "slippage_model_source": "config/us_expanded_tca_calibrated.yaml k with default alpha",
        "slippage_k": _SLIPPAGE_K,
        "slippage_alpha": _SLIPPAGE_ALPHA,
        "selected_row_count": int(len(joined)),
        "covered_selected_row_count": int(len(valid)),
        "market_input_coverage_share": round(coverage_share, 10),
        "unique_instrument_count": int(joined["instrument_id"].nunique()),
        "covered_unique_instrument_count": int(valid["instrument_id"].nunique()),
        "adv_dollars_median": _round_float(adv_dollars.dropna().median()),
        "adv_dollars_p10": _round_float(adv_dollars.dropna().quantile(0.10)),
        "adv_dollars_p95": _round_float(adv_dollars.dropna().quantile(0.95)),
        "capacity_scenarios": scenarios,
        "capacity_status": "cost_capacity_proxy_evaluated_actual_spread_pending",
        "fabricated_capacity": False,
        "not_alpha_evidence": True,
    }


def _selected_market_coverage_share(selected: pd.DataFrame, market_inputs: pd.DataFrame) -> float:
    if selected.empty or market_inputs.empty or "instrument_id" not in selected.columns:
        return 0.0
    left = selected[["instrument_id"]].copy()
    left["instrument_id"] = left["instrument_id"].astype(str).str.strip().str.upper()
    joined = left.merge(market_inputs[["instrument_id", "adv_shares", "close"]], on="instrument_id", how="left")
    valid = pd.to_numeric(joined["adv_shares"], errors="coerce").gt(0) & pd.to_numeric(joined["close"], errors="coerce").gt(0)
    return round(float(valid.mean()), 10) if len(valid) else 0.0


def _half_spread_proxy_bps(bucket: str) -> float:
    normalized = str(bucket).strip().lower()
    if normalized == "high":
        return 2.0
    if normalized in {"medium", "mid"}:
        return 5.0
    if normalized == "low":
        return 12.0
    return 8.0


def _capacity_scenario_row(frame: pd.DataFrame, per_name_notional_usd: float) -> dict[str, Any]:
    work = frame.copy()
    work["quantity_proxy"] = per_name_notional_usd / pd.to_numeric(work["close"], errors="coerce").clip(lower=1e-12)
    work["participation"] = work["quantity_proxy"] / pd.to_numeric(work["adv_shares"], errors="coerce").clip(lower=1.0)
    work["slippage_bps_one_way"] = _SLIPPAGE_K * (work["participation"].clip(lower=0.0) ** _SLIPPAGE_ALPHA) * 10000.0
    work["round_trip_cost_bps_proxy"] = 2.0 * (work["half_spread_bps_proxy"] + work["slippage_bps_one_way"])
    return {
        "per_name_notional_usd": per_name_notional_usd,
        "participation_median": _round_float(work["participation"].median()),
        "participation_p95": _round_float(work["participation"].quantile(0.95)),
        "participation_max": _round_float(work["participation"].max()),
        "round_trip_cost_bps_median_proxy": _round_float(work["round_trip_cost_bps_proxy"].median()),
        "round_trip_cost_bps_p95_proxy": _round_float(work["round_trip_cost_bps_proxy"].quantile(0.95)),
        "cost_proxy_status": "watch" if float(work["round_trip_cost_bps_proxy"].quantile(0.95)) > 300.0 else "not_fatal_proxy",
        "not_alpha_evidence": True,
    }


def _round_float(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    return round(float(value), 10)


def _benchmark_residual_audit(selected: pd.DataFrame, split_dates: dict[str, pd.Index]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split, dates in split_dates.items():
        frame = selected[selected["date"].isin(dates)].copy()
        rows.append(
            {
                "schema_version": "full_market_candidate_benchmark_residual_audit.v1",
                "split": split,
                "sample_count": int(len(frame)),
                "mean_return": _mean(frame, "label"),
                "benchmark_mean_return": _mean(frame, "benchmark_label"),
                "benchmark_residual_mean_return": _mean(frame, "benchmark_residual_label"),
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_RESIDUAL_COLUMNS)


def _decision_label(selected: pd.DataFrame, tail: pd.DataFrame, anomaly: dict[str, Any]) -> str:
    if selected.empty:
        return "blocked_data_coverage"
    top10_abs_share = float(tail["top10_abs_share"].max()) if not tail.empty else 0.0
    issuer_abs_share = float(tail["issuer_abs_share"].max()) if not tail.empty else 0.0
    if int(anomaly["selected_path_extreme_return_row_count"]) > 0 and (top10_abs_share > 0.15 or issuer_abs_share > 0.25):
        return "full_audit_blocked_data_anomaly"
    if top10_abs_share > 0.35:
        return "full_audit_blocked_tail_concentration"
    return "full_audit_passed_cost_capacity_pending"


def _summary(
    *,
    returns_panel_path: Path,
    supervisor_dir: Path,
    manifest_path: Path,
    manifest: dict[str, Any],
    candidate: dict[str, Any] | None,
    feature_column: str,
    label_column: str,
    raw_returns: pd.DataFrame,
    selected: pd.DataFrame,
    temporal: pd.DataFrame,
    tail: pd.DataFrame,
    anomaly: dict[str, Any],
    cost_capacity: dict[str, Any],
    decision_label: str,
    unavailable_reason: str,
) -> dict[str, Any]:
    summary = {
        "schema_version": "full_market_candidate_full_audit_summary.v1",
        "validation_status": "blocked" if decision_label.startswith("blocked") or "_blocked_" in decision_label else "evaluated",
        "decision_label": decision_label,
        "unavailable_reason": unavailable_reason,
        "returns_panel_path": str(returns_panel_path),
        "supervisor_dir": str(supervisor_dir),
        "frozen_candidate_manifest_path": str(manifest_path),
        "frozen_candidate_manifest_schema_version": str(manifest.get("schema_version", "")),
        "candidate": candidate,
        "feature_column": feature_column,
        "label_column": label_column,
        "candidate_window_label": label_column,
        "returns_row_count": int(len(raw_returns)),
        "selected_row_count": int(len(selected)),
        "instrument_count": int(raw_returns["instrument_id"].nunique()) if "instrument_id" in raw_returns.columns else 0,
        "temporal_breadth_rows": _records(temporal),
        "top10_abs_share": float(tail["top10_abs_share"].max()) if not tail.empty else 0.0,
        "data_anomaly_audit": anomaly,
        "cost_capacity_status": cost_capacity["capacity_status"],
        "cost_capacity_pending": not bool(cost_capacity.get("real_spread_inputs_available", False)),
        "cost_capacity_market_input_coverage_share": float(cost_capacity.get("market_input_coverage_share", 0.0)),
        "d3_charter_allowed": False,
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "or_optimizer_used": False,
        "expected_return_panel_written": False,
        "alpha_registry_update_allowed": False,
        "production_approval": False,
        "live_trading": False,
        "broker_order_workflow": False,
        "not_alpha_evidence": True,
        "non_claims": _non_claims(),
    }
    if not unavailable_reason:
        summary.pop("unavailable_reason")
    return summary


def _write_outputs(
    summary_path: Path,
    temporal_path: Path,
    tail_path: Path,
    anomaly_path: Path,
    cost_capacity_path: Path,
    residual_path: Path,
    report_path: Path,
    summary: dict[str, Any],
    temporal: pd.DataFrame,
    tail: pd.DataFrame,
    anomaly: dict[str, Any],
    cost_capacity: dict[str, Any],
    residual: pd.DataFrame,
) -> None:
    temporal.to_csv(temporal_path, index=False)
    tail.to_csv(tail_path, index=False)
    anomaly_path.write_text(json.dumps(anomaly, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    cost_capacity_path.write_text(json.dumps(cost_capacity, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    residual.to_csv(residual_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(summary), encoding="utf-8")


def _render_report(summary: dict[str, Any]) -> str:
    cost_note = (
        "Cost/capacity proxy inputs were evaluated, but real bid-ask spread is still unavailable; Q2 remains closed."
        if float(summary.get("cost_capacity_market_input_coverage_share", 0.0)) > 0.0
        else "Q2 remains closed. Cost/capacity pending because ADV and spread inputs are unavailable and no capacity estimate is fabricated."
    )
    lines = [
        "# Full Market Candidate Full Audit",
        "",
        "This full audit is diagnostic only. It audits a freeze-only candidate and does not write D3, MeasurementSpec, Q1, Q2, optimizer, Alpha Registry, expected-return, broker/order, live, or production artifacts.",
        "",
        cost_note,
        "",
        f"Decision label: `{summary['decision_label']}`",
        f"Selected rows: `{summary['selected_row_count']}`",
        f"Top10 absolute contribution share: `{summary['top10_abs_share']}`",
        f"Cost/capacity status: `{summary['cost_capacity_status']}`",
        f"Cost/capacity market input coverage share: `{summary.get('cost_capacity_market_input_coverage_share', 0.0)}`",
        "",
    ]
    return "\n".join(lines)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records"))


def _date_bound(values: pd.Series, bound: str) -> str:
    if values.empty:
        return ""
    dates = pd.to_datetime(values, errors="coerce").dropna()
    if dates.empty:
        return ""
    value = dates.min() if bound == "min" else dates.max()
    return pd.Timestamp(value).date().isoformat()


def _mean(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return round(float(values.mean()), 10) if not values.empty else 0.0


def _non_claims() -> dict[str, bool]:
    return {
        "alpha_evidence": False,
        "d3_approval": False,
        "q1_entry": False,
        "q2_entry": False,
        "or_optimizer": False,
        "expected_return_panel": False,
        "alpha_registry": False,
        "paper_canary": False,
        "live_trading": False,
        "broker_order_workflow": False,
        "production_approval": False,
    }


def _result(
    summary_path: Path,
    temporal_path: Path,
    tail_path: Path,
    anomaly_path: Path,
    cost_capacity_path: Path,
    residual_path: Path,
    report_path: Path,
    validation_status: str,
    decision_label: str,
) -> FullMarketCandidateAuditResult:
    return FullMarketCandidateAuditResult(
        summary_path=str(summary_path),
        temporal_breadth_path=str(temporal_path),
        tail_concentration_path=str(tail_path),
        data_anomaly_audit_path=str(anomaly_path),
        cost_capacity_audit_path=str(cost_capacity_path),
        benchmark_residual_audit_path=str(residual_path),
        report_path=str(report_path),
        validation_status=validation_status,
        decision_label=decision_label,
    )
