"""Offline aggregation helpers for repeated paper-calibration runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from portfolio_os.domain.errors import InputValidationError
from portfolio_os.storage.snapshots import write_text


ET = ZoneInfo("America/New_York")


@dataclass
class PaperCalibrationAggregateResult:
    observations_path: str
    summary_path: str
    run_count: int
    observation_count: int


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _discover_run_dirs(input_root: Path) -> list[Path]:
    run_dirs = {
        path.parent
        for path in input_root.rglob("pretrade_reference_snapshot.csv")
        if (path.parent / "alpaca_fill_orders.csv").exists()
    }
    return sorted(run_dirs)


def _time_of_day_bucket(raw_timestamp: Any) -> str:
    if raw_timestamp is None or (isinstance(raw_timestamp, float) and pd.isna(raw_timestamp)):
        return "unknown"
    ts = pd.Timestamp(raw_timestamp)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    ts = ts.tz_convert(ET)
    minutes = ts.hour * 60 + ts.minute
    if minutes < (10 * 60 + 30):
        return "09:30-10:29"
    if minutes < (12 * 60 + 30):
        return "10:30-12:29"
    if minutes < (14 * 60 + 30):
        return "12:30-14:29"
    return "14:30-16:00"


def _ols_slope(x: pd.Series, y: pd.Series) -> tuple[float | None, float | None, float | None]:
    frame = pd.DataFrame({"x": pd.to_numeric(x, errors="coerce"), "y": pd.to_numeric(y, errors="coerce")}).dropna()
    if len(frame) < 2:
        return None, None, None
    x_values = frame["x"].astype(float)
    y_values = frame["y"].astype(float)
    x_mean = float(x_values.mean())
    y_mean = float(y_values.mean())
    denominator = float(((x_values - x_mean) ** 2).sum())
    if denominator <= 0.0:
        return None, None, None
    slope = float((((x_values - x_mean) * (y_values - y_mean)).sum()) / denominator)
    intercept = float(y_mean - slope * x_mean)
    correlation = float(frame["x"].corr(frame["y"])) if len(frame) >= 2 else None
    return slope, intercept, correlation


def _load_one_run(run_dir: Path) -> pd.DataFrame:
    reference_path = run_dir / "pretrade_reference_snapshot.csv"
    fills_path = run_dir / "alpaca_fill_orders.csv"
    reference_frame = pd.read_csv(reference_path)
    fills_frame = pd.read_csv(fills_path)
    if reference_frame.empty or fills_frame.empty:
        return pd.DataFrame()

    reference_frame["ticker"] = reference_frame["ticker"].astype(str).str.strip().str.upper()
    fills_frame["ticker"] = fills_frame["ticker"].astype(str).str.strip().str.upper()
    fills_frame["status"] = fills_frame["status"].astype(str).str.strip().str.lower()
    fills_frame = fills_frame.loc[fills_frame["status"] == "filled"].copy()
    if fills_frame.empty:
        return pd.DataFrame()

    merged = fills_frame.merge(
        reference_frame,
        on="ticker",
        how="inner",
        suffixes=("_fill", "_reference"),
    )
    if merged.empty:
        return pd.DataFrame()

    merged["run_dir"] = str(run_dir)
    if "reference_price_reference" in merged.columns:
        merged["reference_price"] = merged["reference_price_reference"]
    elif "reference_price_fill" in merged.columns:
        merged["reference_price"] = merged["reference_price_fill"]
    merged["reference_price"] = pd.to_numeric(merged["reference_price"], errors="coerce")
    merged["avg_fill_price"] = pd.to_numeric(merged["avg_fill_price"], errors="coerce")
    merged["spread_bps"] = pd.to_numeric(merged["spread_bps"], errors="coerce")
    merged["half_spread_bps"] = merged["spread_bps"] / 2.0
    captured_at = pd.to_datetime(merged["captured_at_utc"], utc=True, errors="coerce")
    submitted_at = pd.to_datetime(merged["submitted_at_utc"], utc=True, errors="coerce")
    merged["capture_to_submit_latency_seconds"] = (submitted_at - captured_at).dt.total_seconds()
    merged["drift_abs"] = merged["avg_fill_price"] - merged["reference_price"]
    merged["drift_bps"] = (merged["drift_abs"] / merged["reference_price"]) * 10000.0
    merged["drift_vs_half_spread"] = merged["drift_bps"] / merged["half_spread_bps"]
    merged.loc[merged["half_spread_bps"].abs() < 1e-12, "drift_vs_half_spread"] = pd.NA
    merged["time_of_day_bucket"] = merged["captured_at_utc"].map(_time_of_day_bucket)
    keep_columns = [
        "run_dir",
        "ticker",
        "reference_price_source",
        "captured_at_utc",
        "latest_trade_at_utc",
        "submitted_at_utc",
        "terminal_at_utc",
        "latest_trade_price",
        "bid_price",
        "ask_price",
        "mid_price",
        "spread_bps",
        "half_spread_bps",
        "reference_price",
        "requested_qty",
        "filled_qty",
        "avg_fill_price",
        "filled_notional",
        "drift_abs",
        "drift_bps",
        "drift_vs_half_spread",
        "capture_to_submit_latency_seconds",
        "time_of_day_bucket",
    ]
    return merged[keep_columns].copy()


def _build_observations(run_dirs: list[Path]) -> pd.DataFrame:
    frames = [_load_one_run(path) for path in run_dirs]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(
            columns=[
                "run_dir",
                "ticker",
                "reference_price_source",
                "captured_at_utc",
                "latest_trade_at_utc",
                "submitted_at_utc",
                "terminal_at_utc",
                "latest_trade_price",
                "bid_price",
                "ask_price",
                "mid_price",
                "spread_bps",
                "half_spread_bps",
                "reference_price",
                "requested_qty",
                "filled_qty",
                "avg_fill_price",
                "filled_notional",
                "drift_abs",
                "drift_bps",
                "drift_vs_half_spread",
                "capture_to_submit_latency_seconds",
                "time_of_day_bucket",
            ]
        )
    return pd.concat(frames, ignore_index=True)


def _render_summary_markdown(observations: pd.DataFrame, *, run_count: int) -> str:
    drift_series = pd.to_numeric(observations["drift_bps"], errors="coerce").dropna()
    relative_series = pd.to_numeric(observations["drift_vs_half_spread"], errors="coerce").dropna()
    slope, intercept, correlation = _ols_slope(
        observations["capture_to_submit_latency_seconds"],
        observations["drift_bps"],
    )
    bucket_frame = (
        observations.groupby("time_of_day_bucket", dropna=False)
        .agg(
            observation_count=("ticker", "size"),
            median_drift_bps=("drift_bps", "median"),
            median_spread_bps=("spread_bps", "median"),
        )
        .reset_index()
        .sort_values("time_of_day_bucket")
    )
    bucket_lines = ["| Bucket | Count | Median Drift (bps) | Median Spread (bps) |", "|---|---:|---:|---:|"]
    for row in bucket_frame.to_dict(orient="records"):
        bucket_lines.append(
            f"| {row['time_of_day_bucket']} | {int(row['observation_count'])} | "
            f"{float(row['median_drift_bps']):.4f} | {float(row['median_spread_bps']):.4f} |"
        )
    source_mix = observations["reference_price_source"].astype(str).value_counts(dropna=False).to_dict()
    return "\n".join(
        [
            "# Paper Calibration Drift Summary",
            "",
            "## Scope",
            f"- Generated at: {_utc_now_iso()}",
            f"- Run Count: {int(run_count)}",
            f"- Observation Count: {int(len(observations))}",
            f"- Unique tickers: {sorted(observations['ticker'].astype(str).dropna().unique().tolist())}",
            f"- Reference source mix: {source_mix}",
            "",
            "## Drift Distribution",
            f"- Median drift (bps): {float(drift_series.median()):.4f}",
            f"- Mean drift (bps): {float(drift_series.mean()):.4f}",
            f"- Drift IQR (bps): {float(drift_series.quantile(0.75) - drift_series.quantile(0.25)):.4f}",
            f"- Drift p05 / p95 (bps): {float(drift_series.quantile(0.05)):.4f} / {float(drift_series.quantile(0.95)):.4f}",
            "",
            "## Relative To Half-Spread",
            f"- Median drift / half-spread: {float(relative_series.median()):.4f}" if not relative_series.empty else "- Median drift / half-spread: n/a",
            f"- IQR drift / half-spread: {float(relative_series.quantile(0.75) - relative_series.quantile(0.25)):.4f}" if len(relative_series) >= 1 else "- IQR drift / half-spread: n/a",
            "",
            "## Latency Regression",
            f"- Slope (bps / second): {slope:.6f}" if slope is not None else "- Slope (bps / second): n/a",
            f"- Intercept (bps): {intercept:.6f}" if intercept is not None else "- Intercept (bps): n/a",
            f"- Correlation: {correlation:.6f}" if correlation is not None else "- Correlation: n/a",
            "",
            "## Time-Of-Day Buckets",
            *bucket_lines,
        ]
    )


def run_paper_calibration_aggregate(
    *,
    input_root: Path,
    output_dir: Path,
) -> PaperCalibrationAggregateResult:
    """Aggregate repeated paper-calibration runs into drift observations and a short note."""

    run_dirs = _discover_run_dirs(input_root)
    if not run_dirs:
        raise InputValidationError(f"No paper-calibration run directories found under {input_root}.")
    observations = _build_observations(run_dirs)
    if observations.empty:
        raise InputValidationError("No filled paper-calibration observations were found in the discovered runs.")

    output_dir.mkdir(parents=True, exist_ok=True)
    observations_path = output_dir / "drift_observations.csv"
    summary_path = output_dir / "drift_summary.md"
    observations.to_csv(observations_path, index=False)
    write_text(summary_path, _render_summary_markdown(observations, run_count=len(run_dirs)))
    return PaperCalibrationAggregateResult(
        observations_path=str(observations_path),
        summary_path=str(summary_path),
        run_count=len(run_dirs),
        observation_count=int(len(observations)),
    )
