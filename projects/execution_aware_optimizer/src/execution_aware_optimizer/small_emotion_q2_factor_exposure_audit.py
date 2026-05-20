"""Small-emotion Q2 factor exposure and beta residual audit.

This module audits whether small-emotion Q2 candidates still have positive
event returns after simple benchmark/beta and factor-proxy controls. It is a
Q2 diagnostic only: it does not modify frozen specs, build portfolios, write
orders, update Alpha Registry, open paper/live/broker/order paths, or claim
production approval.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file


STAGE = "Q2-SMALL-EMOTION-06"
Q2_COMPLETE_DECISION = "completed_q2_execution_survival"
PASS_DECISION = "beta_residual_passed"
FACTOR_COLUMNS = [
    "benchmark_return",
    "trailing_beta_60d",
    "trailing_volatility_60d",
    "log_market_cap",
    "log_dollar_volume",
    "bid_ask_spread",
    "prior_5d_return",
    "prior_20d_return",
    "abs_shock_return",
    "abnormal_volume",
]


@dataclass(frozen=True)
class SmallEmotionQ2FactorExposureAuditResult:
    """Written factor exposure audit artifacts and summary."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_q2_factor_exposure_audit(
    *,
    q2_complete_dir: str | Path,
    q1_event_panels: Mapping[str, str | Path],
    q1_window_panels: Mapping[str, str | Path],
    price_panel_path: str | Path,
    benchmark_panel_path: str | Path,
    output_dir: str | Path,
    minimum_event_count: int = 100,
    beta_lookback_days: int = 60,
) -> SmallEmotionQ2FactorExposureAuditResult:
    """Run Q2 factor exposure / beta residual audit for completed candidates."""

    q2_complete_path = Path(q2_complete_dir)
    price_path = Path(price_panel_path)
    benchmark_path = Path(benchmark_panel_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    complete_matrix = _read_csv(q2_complete_path / "small_emotion_q2_complete_matrix.csv")
    price_panel = _read_csv(price_path)
    benchmark_panel = _read_csv(benchmark_path)

    exposure_frames: list[pd.DataFrame] = []
    residual_rows: list[dict[str, object]] = []
    loading_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    policy_rows: list[dict[str, object]] = []

    for candidate_name in _candidate_order(complete_matrix, q1_event_panels, q1_window_panels):
        complete_row = _first_row(complete_matrix, candidate_name)
        if str(complete_row.get("q2_complete_decision")) != Q2_COMPLETE_DECISION:
            blocked = _blocked_residual_row(candidate_name, complete_row, "blocked_q2_incomplete")
            residual_rows.append(blocked)
            policy_rows.extend(_blocked_policy_rows(candidate_name, "blocked_q2_incomplete"))
            continue

        q1_event_path = Path(q1_event_panels.get(candidate_name, ""))
        q1_window_path = Path(q1_window_panels.get(candidate_name, ""))
        if not q1_event_path.exists() or not q1_window_path.exists() or price_panel.empty or benchmark_panel.empty:
            blocked = _blocked_residual_row(candidate_name, complete_row, "blocked_missing_factor_inputs")
            residual_rows.append(blocked)
            policy_rows.extend(_blocked_policy_rows(candidate_name, "blocked_missing_factor_inputs"))
            continue

        q1_event = _read_csv(q1_event_path)
        q1_window = _read_csv(q1_window_path)
        primary_window = str(
            complete_row.get("primary_window")
            or _primary_window_from_spec(str(complete_row.get("measurement_spec_id", "")))
            or _infer_primary_window(q1_window)
        )
        exposure = _build_exposure_panel(
            candidate_name=candidate_name,
            complete_row=complete_row,
            q1_event=q1_event,
            q1_window=q1_window,
            primary_window=primary_window,
            price_panel=price_panel,
            benchmark_panel=benchmark_panel,
            beta_lookback_days=beta_lookback_days,
        )
        exposure_frames.append(exposure)
        summary_rows.extend(_factor_exposure_summary(candidate_name, exposure))

        regression = _run_factor_regression(candidate_name, exposure, complete_row, minimum_event_count)
        residual_rows.append(regression["residual_row"])
        loading_rows.extend(regression["loading_rows"])
        policy_rows.extend(_policy_rows(candidate_name, regression["residual_row"], minimum_event_count))

    exposure_panel = _concat(exposure_frames, _exposure_columns())
    beta_residual = pd.DataFrame(residual_rows, columns=_residual_columns())
    factor_loadings = pd.DataFrame(loading_rows, columns=_loading_columns())
    exposure_summary = pd.DataFrame(summary_rows, columns=_summary_columns())
    policy_gate = pd.DataFrame(policy_rows, columns=_policy_columns())
    summary = _summary(beta_residual, q2_complete_path, price_path, benchmark_path)
    manifest = _manifest(
        summary,
        artifacts,
        q2_complete_dir=q2_complete_path,
        q1_event_panels=q1_event_panels,
        q1_window_panels=q1_window_panels,
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
    )

    exposure_panel.to_csv(artifacts["exposure_panel"], index=False)
    beta_residual.to_csv(artifacts["beta_residual_matrix"], index=False)
    factor_loadings.to_csv(artifacts["factor_loading_matrix"], index=False)
    exposure_summary.to_csv(artifacts["factor_exposure_summary"], index=False)
    policy_gate.to_csv(artifacts["policy_gate"], index=False)
    artifacts["summary"].write_text(canonical_json(summary) + "\n", encoding="utf-8")
    artifacts["manifest"].write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    artifacts["report"].write_text(_report(summary, beta_residual), encoding="utf-8")

    return SmallEmotionQ2FactorExposureAuditResult(summary=summary, artifacts=artifacts)


def _build_exposure_panel(
    *,
    candidate_name: str,
    complete_row: dict[str, object],
    q1_event: pd.DataFrame,
    q1_window: pd.DataFrame,
    primary_window: str,
    price_panel: pd.DataFrame,
    benchmark_panel: pd.DataFrame,
    beta_lookback_days: int,
) -> pd.DataFrame:
    events = q1_event.copy()
    windows = q1_window.copy()
    if "signal_state" in events.columns:
        events = events[events["signal_state"].astype(str).str.lower().eq("active")].copy()
    if "label_status" in windows.columns:
        windows = windows[windows["label_status"].astype(str).eq("observed")].copy()
    if primary_window and "window" in windows.columns:
        windows = windows[windows["window"].astype(str).eq(primary_window)].copy()
    if events.empty or windows.empty:
        return pd.DataFrame(columns=_exposure_columns())

    for frame in [events, windows]:
        frame["event_id"] = frame["event_id"].astype(str)
        frame["ticker"] = frame["ticker"].astype(str).str.upper()
    merged = events.merge(
        windows.loc[
            :,
            [
                "event_id",
                "ticker",
                "asset_id",
                "date",
                "event_month",
                "window",
                "asset_return",
                "benchmark_return",
                "abnormal_return",
                "directional_return",
            ],
        ],
        on="event_id",
        how="inner",
        suffixes=("", "_label"),
    )
    merged = merged[merged["ticker"].eq(merged["ticker_label"])].copy()
    if merged.empty:
        return pd.DataFrame(columns=_exposure_columns())

    beta_lookup = _build_beta_lookup(price_panel, benchmark_panel, beta_lookback_days)
    trailing_beta: list[float] = []
    trailing_vol: list[float] = []
    for row in merged.to_dict("records"):
        beta, volatility = _lookup_beta_vol(beta_lookup, row.get("asset_id"), row.get("date"))
        trailing_beta.append(beta)
        trailing_vol.append(volatility)

    market_cap = _numeric(merged, "market_cap")
    dollar_volume = _numeric(merged, "dollar_volume")
    result = pd.DataFrame(
        {
            "schema_version": "small_emotion_q2_factor_exposure_panel.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
            "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
            "event_id": merged["event_id"].astype(str),
            "asset_id": merged["asset_id"].astype(str),
            "ticker": merged["ticker"].astype(str),
            "date": pd.to_datetime(merged["date"]).dt.strftime("%Y-%m-%d"),
            "event_month": merged["event_month"].astype(str),
            "primary_window": merged["window"].astype(str),
            "asset_return": _numeric(merged, "asset_return"),
            "benchmark_return": _numeric(merged, "benchmark_return"),
            "abnormal_return": _numeric(merged, "abnormal_return"),
            "directional_return": _numeric(merged, "directional_return"),
            "trailing_beta_60d": trailing_beta,
            "trailing_volatility_60d": trailing_vol,
            "log_market_cap": np.log(market_cap.clip(lower=1.0).astype(float)),
            "log_dollar_volume": np.log(dollar_volume.clip(lower=1.0).astype(float)),
            "bid_ask_spread": _numeric(merged, "bid_ask_spread").fillna(0.0),
            "prior_5d_return": _numeric(merged, "prior_5d_return"),
            "prior_20d_return": _numeric(merged, "prior_20d_return"),
            "abs_shock_return": _numeric(merged, "abs_shock_return"),
            "abnormal_volume": _numeric(merged, "abnormal_volume"),
            "sector": merged.get("sector", pd.Series([""] * len(merged))).astype(str),
            "industry": merged.get("industry", pd.Series([""] * len(merged))).astype(str),
            "market_cap_bucket": merged.get("market_cap_bucket", pd.Series(["unknown"] * len(merged))).astype(str),
            "liquidity_bucket": merged.get("liquidity_bucket", pd.Series(["unknown"] * len(merged))).astype(str),
            "spread_bucket": merged.get("spread_bucket", pd.Series(["unknown"] * len(merged))).astype(str),
            "no_view_not_zero_alpha": True,
        }
    )
    return result.sort_values(["candidate_name", "date", "event_id"]).reset_index(drop=True).loc[:, _exposure_columns()]


def _build_beta_lookup(
    price_panel: pd.DataFrame,
    benchmark_panel: pd.DataFrame,
    beta_lookback_days: int,
) -> dict[str, pd.DataFrame]:
    prices = price_panel.copy()
    bench = benchmark_panel.copy()
    if prices.empty or bench.empty or "return" not in prices.columns or "return" not in bench.columns:
        return {}
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    bench["date"] = pd.to_datetime(bench["date"], errors="coerce")
    prices["asset_id"] = prices["asset_id"].astype(str)
    joined = prices.loc[:, ["asset_id", "date", "return"]].merge(
        bench.loc[:, ["date", "return"]].rename(columns={"return": "benchmark_daily_return"}),
        on="date",
        how="left",
    )
    joined["asset_return"] = pd.to_numeric(joined["return"], errors="coerce")
    joined["benchmark_daily_return"] = pd.to_numeric(joined["benchmark_daily_return"], errors="coerce")
    joined = joined.dropna(subset=["date", "asset_return", "benchmark_daily_return"]).sort_values(["asset_id", "date"])
    return {
        asset_id: frame.loc[:, ["date", "asset_return", "benchmark_daily_return"]].reset_index(drop=True)
        for asset_id, frame in joined.groupby("asset_id", observed=False)
    }


def _lookup_beta_vol(
    lookup: dict[str, pd.DataFrame],
    asset_id: object,
    event_date: object,
) -> tuple[float, float]:
    asset_key = str(asset_id)
    frame = lookup.get(asset_key)
    if frame is None or frame.empty:
        return math.nan, math.nan
    date = pd.to_datetime(event_date, errors="coerce")
    if pd.isna(date):
        return math.nan, math.nan
    prior = frame[frame["date"] < date].tail(60)
    if len(prior) < 3:
        return math.nan, math.nan
    asset = pd.to_numeric(prior["asset_return"], errors="coerce")
    bench = pd.to_numeric(prior["benchmark_daily_return"], errors="coerce")
    valid = pd.DataFrame({"asset": asset, "bench": bench}).dropna()
    if len(valid) < 3:
        return math.nan, math.nan
    bench_var = float(valid["bench"].var(ddof=1))
    beta = float(valid["asset"].cov(valid["bench"]) / bench_var) if bench_var > 0.0 else math.nan
    volatility = float(valid["asset"].std(ddof=1))
    return beta, volatility


def _run_factor_regression(
    candidate_name: str,
    exposure: pd.DataFrame,
    complete_row: dict[str, object],
    minimum_event_count: int,
) -> dict[str, object]:
    if exposure.empty:
        return {
            "residual_row": _blocked_residual_row(candidate_name, complete_row, "blocked_missing_observed_labels"),
            "loading_rows": [],
        }
    model_frame = exposure.loc[:, ["directional_return", *FACTOR_COLUMNS]].copy()
    for column in model_frame.columns:
        model_frame[column] = pd.to_numeric(model_frame[column], errors="coerce")
    model_frame = model_frame.replace([np.inf, -np.inf], np.nan).dropna()
    selected_factors = _selected_factor_columns(model_frame)
    if len(model_frame) < minimum_event_count or not selected_factors:
        row = _base_residual_row(candidate_name, complete_row, exposure, "hold_insufficient_sample")
        row.update(
            {
                "factor_adjusted_alpha": math.nan,
                "factor_adjusted_alpha_t_stat": math.nan,
                "factor_model_r_squared": math.nan,
                "factor_model_observation_count": int(len(model_frame)),
                "factor_model_status": "insufficient_complete_factor_rows",
            }
        )
        return {"residual_row": row, "loading_rows": []}

    y = model_frame["directional_return"].astype(float).to_numpy()
    x_frame = model_frame.loc[:, selected_factors].astype(float)
    x_mean = x_frame.mean(axis=0)
    x_std = x_frame.std(axis=0, ddof=0).replace(0.0, 1.0)
    x_scaled = (x_frame - x_mean) / x_std
    x = np.column_stack([np.ones(len(x_scaled)), x_scaled.to_numpy(dtype=float)])
    beta = np.linalg.pinv(x.T @ x) @ x.T @ y
    fitted = x @ beta
    residuals = y - fitted
    dof = max(len(y) - x.shape[1], 1)
    sigma2 = float((residuals @ residuals) / dof)
    cov = sigma2 * np.linalg.pinv(x.T @ x)
    se = np.sqrt(np.clip(np.diag(cov), 0.0, None))
    t_stats = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se > 0.0)
    ss_total = float(((y - y.mean()) @ (y - y.mean())))
    ss_resid = float(residuals @ residuals)
    r_squared = 1.0 - ss_resid / ss_total if ss_total > 0.0 else math.nan

    alpha = float(beta[0])
    alpha_t = float(t_stats[0])
    decision = PASS_DECISION if alpha > 0.0 and alpha_t > 0.0 else "factor_exposure_explained"
    row = _base_residual_row(candidate_name, complete_row, exposure, decision)
    row.update(
        {
            "factor_adjusted_alpha": alpha,
            "factor_adjusted_alpha_t_stat": alpha_t,
            "factor_model_r_squared": float(r_squared),
            "factor_model_observation_count": int(len(y)),
            "factor_model_status": "observed",
        }
    )
    loading_rows = [
        _loading_row(candidate_name, complete_row, "intercept_alpha", beta[0], t_stats[0], x_mean=None, x_std=None)
    ]
    for idx, factor in enumerate(selected_factors, start=1):
        loading_rows.append(
            _loading_row(
                candidate_name,
                complete_row,
                factor,
                beta[idx],
                t_stats[idx],
                x_mean=float(x_mean[factor]),
                x_std=float(x_std[factor]),
            )
        )
    return {"residual_row": row, "loading_rows": loading_rows}


def _selected_factor_columns(model_frame: pd.DataFrame) -> list[str]:
    candidates = [
        column
        for column in FACTOR_COLUMNS
        if column in model_frame.columns and pd.to_numeric(model_frame[column], errors="coerce").std(ddof=0) > 0.0
    ]
    max_factors = max(min(len(model_frame) - 2, len(candidates)), 0)
    return candidates[:max_factors]


def _base_residual_row(
    candidate_name: str,
    complete_row: dict[str, object],
    exposure: pd.DataFrame,
    decision: str,
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_beta_residual_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
        "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
        "primary_window": _first_nonempty(exposure, "primary_window"),
        "event_count": int(len(exposure)),
        "event_month_count": int(exposure["event_month"].nunique()) if "event_month" in exposure.columns else 0,
        "raw_directional_return_mean": _mean(exposure, "directional_return"),
        "benchmark_return_mean": _mean(exposure, "benchmark_return"),
        "abnormal_return_mean": _mean(exposure, "abnormal_return"),
        "factor_adjusted_alpha": math.nan,
        "factor_adjusted_alpha_t_stat": math.nan,
        "factor_model_r_squared": math.nan,
        "factor_model_observation_count": 0,
        "factor_model_status": "unavailable",
        "audit_decision": decision,
        "factor_exposure_audit_run": True,
        "orders_written": False,
        "portfolio_construction_artifact_written": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _blocked_residual_row(candidate_name: str, complete_row: dict[str, object], decision: str) -> dict[str, object]:
    row = _base_residual_row(candidate_name, complete_row, pd.DataFrame(), decision)
    row["factor_exposure_audit_run"] = False
    row["factor_model_status"] = decision
    return row


def _loading_row(
    candidate_name: str,
    complete_row: dict[str, object],
    factor_name: str,
    loading: float,
    t_stat: float,
    *,
    x_mean: float | None,
    x_std: float | None,
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_factor_loading_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
        "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
        "factor_name": factor_name,
        "loading": float(loading),
        "t_stat": float(t_stat),
        "factor_mean": math.nan if x_mean is None else float(x_mean),
        "factor_std": math.nan if x_std is None else float(x_std),
        "no_view_not_zero_alpha": True,
    }


def _factor_exposure_summary(candidate_name: str, exposure: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for column in ["sector", "market_cap_bucket", "liquidity_bucket", "spread_bucket"]:
        if exposure.empty or column not in exposure.columns:
            continue
        grouped = (
            exposure.groupby(column, observed=False)
            .agg(
                event_count=("event_id", "size"),
                mean_directional_return=("directional_return", "mean"),
                mean_trailing_beta=("trailing_beta_60d", "mean"),
                mean_bid_ask_spread=("bid_ask_spread", "mean"),
            )
            .reset_index()
        )
        for row in grouped.to_dict("records"):
            rows.append(
                {
                    "schema_version": "small_emotion_q2_factor_exposure_summary.v1",
                    "stage": STAGE,
                    "candidate_name": candidate_name,
                    "bucket_type": column,
                    "bucket_value": str(row[column]),
                    "event_count": int(row["event_count"]),
                    "mean_directional_return": float(row["mean_directional_return"]),
                    "mean_trailing_beta": _safe_float(row["mean_trailing_beta"]),
                    "mean_bid_ask_spread": _safe_float(row["mean_bid_ask_spread"]),
                    "no_view_not_zero_alpha": True,
                }
            )
    return rows


def _policy_rows(candidate_name: str, residual_row: dict[str, object], minimum_event_count: int) -> list[dict[str, object]]:
    checks = [
        ("minimum_event_count", residual_row.get("event_count"), minimum_event_count, ">="),
        ("positive_factor_adjusted_alpha", residual_row.get("factor_adjusted_alpha"), 0.0, ">"),
        ("positive_factor_adjusted_alpha_t_stat", residual_row.get("factor_adjusted_alpha_t_stat"), 0.0, ">"),
        ("factor_model_observed", residual_row.get("factor_model_status"), "observed", "=="),
    ]
    return [
        {
            "schema_version": "small_emotion_q2_factor_residual_policy_gate.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "policy_name": name,
            "observed_value": value,
            "limit_value": limit,
            "comparison": comparison,
            "status": _policy_status(value, limit, comparison),
            "factor_exposure_audit_run": True,
            "no_view_not_zero_alpha": True,
        }
        for name, value, limit, comparison in checks
    ]


def _blocked_policy_rows(candidate_name: str, reason: str) -> list[dict[str, object]]:
    return [
        {
            "schema_version": "small_emotion_q2_factor_residual_policy_gate.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "policy_name": reason,
            "observed_value": reason,
            "limit_value": "observed",
            "comparison": "==",
            "status": "fail",
            "factor_exposure_audit_run": False,
            "no_view_not_zero_alpha": True,
        }
    ]


def _summary(beta_residual: pd.DataFrame, q2_complete_dir: Path, price_panel_path: Path, benchmark_panel_path: Path) -> dict[str, object]:
    decisions = (
        beta_residual["audit_decision"].astype(str)
        if "audit_decision" in beta_residual.columns
        else pd.Series(dtype=str)
    )
    return {
        "schema_version": "small_emotion_q2_factor_exposure_audit_summary.v1",
        "stage": STAGE,
        "candidate_count": int(len(beta_residual)),
        "beta_residual_passed_count": int(decisions.eq(PASS_DECISION).sum()),
        "factor_exposure_explained_count": int(decisions.eq("factor_exposure_explained").sum()),
        "blocked_or_hold_count": int((~decisions.isin([PASS_DECISION, "factor_exposure_explained"])).sum()),
        "q2_complete_dir": str(q2_complete_dir),
        "price_panel_path": str(price_panel_path),
        "benchmark_panel_path": str(benchmark_panel_path),
        "factor_exposure_audit_run": bool(decisions.eq(PASS_DECISION).any() or decisions.eq("factor_exposure_explained").any()),
        "orders_written": False,
        "portfolio_construction_artifact_written": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _manifest(
    summary: dict[str, object],
    artifacts: dict[str, Path],
    *,
    q2_complete_dir: Path,
    q1_event_panels: Mapping[str, str | Path],
    q1_window_panels: Mapping[str, str | Path],
    price_panel_path: Path,
    benchmark_panel_path: Path,
) -> dict[str, object]:
    payload = {
        "schema_version": "small_emotion_q2_factor_exposure_audit_manifest.v1",
        "stage": STAGE,
        "summary": summary,
        "input_artifact_hashes": {
            "q2_complete_matrix": _hash_if_exists(q2_complete_dir / "small_emotion_q2_complete_matrix.csv"),
            "q1_event_panels": {name: _hash_if_exists(Path(path)) for name, path in sorted(q1_event_panels.items())},
            "q1_window_panels": {name: _hash_if_exists(Path(path)) for name, path in sorted(q1_window_panels.items())},
            "price_panel": _hash_if_exists(price_panel_path),
            "benchmark_panel": _hash_if_exists(benchmark_panel_path),
        },
        "output_artifacts": {key: str(path) for key, path in artifacts.items()},
        "orders_written": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "exposure_panel": output_path / "small_emotion_q2_factor_exposure_panel.csv",
        "beta_residual_matrix": output_path / "small_emotion_q2_beta_residual_matrix.csv",
        "factor_loading_matrix": output_path / "small_emotion_q2_factor_loading_matrix.csv",
        "factor_exposure_summary": output_path / "small_emotion_q2_factor_exposure_summary.csv",
        "policy_gate": output_path / "small_emotion_q2_factor_residual_policy_gate.csv",
        "summary": output_path / "small_emotion_q2_factor_exposure_audit_summary.json",
        "manifest": output_path / "small_emotion_q2_factor_exposure_audit_manifest.json",
        "report": output_path / "small_emotion_q2_factor_exposure_audit_report.md",
    }


def _report(summary: dict[str, object], matrix: pd.DataFrame) -> str:
    lines = [
        "# Q2-SMALL-EMOTION-06 Factor Exposure / Beta Residual Audit",
        "",
        "This is a Q2 factor exposure / beta residual audit only. It checks whether completed small-emotion candidates retain positive directional returns after benchmark, beta, size, liquidity, volatility, reversal, shock, and attention proxy controls. It does not modify frozen formulas, write orders, update Alpha Registry, open paper/live/broker/order workflows, or claim production approval.",
        "",
        "## Boundary",
        "",
        "- Q2 factor exposure / beta residual audit only",
        "- orders: not written",
        "- broker/order/live paths: closed",
        "- production approval: not claimed",
        "",
        "## Summary",
        "",
        f"- candidate_count: `{summary['candidate_count']}`",
        f"- beta_residual_passed_count: `{summary['beta_residual_passed_count']}`",
        f"- factor_exposure_explained_count: `{summary['factor_exposure_explained_count']}`",
        f"- blocked_or_hold_count: `{summary['blocked_or_hold_count']}`",
        "",
        "| candidate | decision | events | months | raw directional mean | factor-adjusted alpha | alpha t-stat | R2 |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in matrix.to_dict("records"):
        lines.append(
            "| {candidate} | {decision} | {events} | {months} | {raw} | {alpha} | {tstat} | {r2} |".format(
                candidate=row.get("candidate_name", ""),
                decision=row.get("audit_decision", ""),
                events=row.get("event_count", ""),
                months=row.get("event_month_count", ""),
                raw=_fmt(row.get("raw_directional_return_mean")),
                alpha=_fmt(row.get("factor_adjusted_alpha")),
                tstat=_fmt(row.get("factor_adjusted_alpha_t_stat")),
                r2=_fmt(row.get("factor_model_r_squared")),
            )
        )
    return "\n".join(lines) + "\n"


def _candidate_order(
    complete_matrix: pd.DataFrame,
    q1_event_panels: Mapping[str, str | Path],
    q1_window_panels: Mapping[str, str | Path],
) -> list[str]:
    names: list[str] = []
    if not complete_matrix.empty and "candidate_name" in complete_matrix.columns:
        names.extend(str(name) for name in complete_matrix["candidate_name"].dropna().tolist())
    names.extend(str(name) for name in q1_event_panels)
    names.extend(str(name) for name in q1_window_panels)
    ordered: list[str] = []
    seen: set[str] = set()
    for name in names:
        if name and name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def _first_row(frame: pd.DataFrame, candidate_name: str) -> dict[str, object]:
    if frame.empty or "candidate_name" not in frame.columns:
        return {}
    rows = frame[frame["candidate_name"].astype(str).eq(candidate_name)]
    return rows.iloc[0].to_dict() if not rows.empty else {}


def _infer_primary_window(q1_window: pd.DataFrame) -> str:
    if q1_window.empty or "window" not in q1_window.columns:
        return ""
    windows = q1_window["window"].astype(str)
    post = windows[windows.str.startswith("post_")]
    return str(post.mode().iloc[0]) if not post.empty else str(windows.mode().iloc[0])


def _primary_window_from_spec(measurement_spec_id: str) -> str:
    for window in ["post_1_44", "post_1_22", "post_1_10", "post_1_5", "post_6_22"]:
        if window in measurement_spec_id:
            return window
    return ""


def _first_nonempty(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return ""
    values = frame[column].dropna().astype(str)
    return "" if values.empty else str(values.iloc[0])


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce").astype("Float64")


def _mean(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return math.nan
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else math.nan


def _policy_status(value: object, limit: object, comparison: str) -> str:
    if comparison == "==":
        return "pass" if str(value) == str(limit) else "fail"
    if not _is_number(value) or not _is_number(limit):
        return "unavailable"
    observed = float(value)
    threshold = float(limit)
    if comparison == ">=":
        return "pass" if observed >= threshold else "fail"
    if comparison == ">":
        return "pass" if observed > threshold else "fail"
    return "unavailable"


def _safe_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def _is_number(value: object) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def _fmt(value: object) -> str:
    return "" if not _is_number(value) else f"{float(value):.6f}"


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _hash_if_exists(path: Path) -> str:
    return sha256_file(path) if path.exists() else "missing"


def _concat(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    return pd.concat(frames, ignore_index=True).reindex(columns=columns) if frames else pd.DataFrame(columns=columns)


def _exposure_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "event_id",
        "asset_id",
        "ticker",
        "date",
        "event_month",
        "primary_window",
        "asset_return",
        "benchmark_return",
        "abnormal_return",
        "directional_return",
        "trailing_beta_60d",
        "trailing_volatility_60d",
        "log_market_cap",
        "log_dollar_volume",
        "bid_ask_spread",
        "prior_5d_return",
        "prior_20d_return",
        "abs_shock_return",
        "abnormal_volume",
        "sector",
        "industry",
        "market_cap_bucket",
        "liquidity_bucket",
        "spread_bucket",
        "no_view_not_zero_alpha",
    ]


def _residual_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "primary_window",
        "event_count",
        "event_month_count",
        "raw_directional_return_mean",
        "benchmark_return_mean",
        "abnormal_return_mean",
        "factor_adjusted_alpha",
        "factor_adjusted_alpha_t_stat",
        "factor_model_r_squared",
        "factor_model_observation_count",
        "factor_model_status",
        "audit_decision",
        "factor_exposure_audit_run",
        "orders_written",
        "portfolio_construction_artifact_written",
        "alpha_registry_update_allowed",
        "paper_ready",
        "live_ready",
        "broker_order_path_opened",
        "production_approval_claimed",
        "no_view_not_zero_alpha",
    ]


def _loading_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "factor_name",
        "loading",
        "t_stat",
        "factor_mean",
        "factor_std",
        "no_view_not_zero_alpha",
    ]


def _summary_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "bucket_type",
        "bucket_value",
        "event_count",
        "mean_directional_return",
        "mean_trailing_beta",
        "mean_bid_ask_spread",
        "no_view_not_zero_alpha",
    ]


def _policy_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "policy_name",
        "observed_value",
        "limit_value",
        "comparison",
        "status",
        "factor_exposure_audit_run",
        "no_view_not_zero_alpha",
    ]
