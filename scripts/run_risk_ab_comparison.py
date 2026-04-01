from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "risk_ab_comparison"
DEDUP_KEEP_STRATEGY = "last"
MIN_OVERLAP_RATIO = 0.8
AB_EXPERIMENT_VERSION = "risk_ab_v2.0"
INELIGIBLE_DECISION_TEXT = "Comparison is ineligible; this report is not suitable for strategy decision-making."
REQUIRE_ELIGIBLE_FAILURE_MESSAGE = "Comparison eligibility gate failed (--require-eligible): comparison is ineligible."
NO_OBSERVABLE_EFFECT_THRESHOLD = 0.001
TARGET_DEVIATION_TOLERANCE = 1e-6
POLICY_DETERIORATION_LIMITS: tuple[tuple[str, float], ...] = (
    ("Policy A", 0.01),
    ("Policy B", 0.02),
    ("Policy C", 0.03),
)
DEFAULT_DAILY_TRADEOFF_WEIGHTS: tuple[float, ...] = (1e4, 1.5e4, 2e4)
METRIC_FAMILY_KEYS: tuple[tuple[str, str], ...] = (
    ("override_rate", "override_rate_avg"),
    ("cost_better_ratio", "cost_better_ratio_avg"),
    ("turnover", "turnover_avg"),
    ("solver_time", "solver_time_avg"),
    ("clarabel_convergence_rate", "clarabel_convergence_rate"),
    ("target_deviation_improvement", "target_deviation_improvement_mean"),
)


@dataclass
class ABConfig:
    baseline_dashboard: Path
    start_date: str
    end_date: str
    phase: str
    market: str
    risk_inputs_dir: Path
    w5_values: list[float]
    cool_down: float
    max_failures: int
    output_dir: Path
    risk_integration_mode: str = "replace"
    tracking_error_weight: float = 0.0
    transaction_cost_weight: float = 0.0
    require_eligible: bool = False
    real_sample: bool = False
    replay_require_eligibility_gate: bool = True
    daily_tradeoff_weights: list[float] | None = None


@dataclass
class ReplayRunResult:
    w5_value: float
    status: str
    return_code: int
    tracking_dir: Path
    dashboard_path: Path
    progress_status: str
    metrics: dict[str, Any]
    daily: pd.DataFrame
    stderr: str


def _now_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(message: str) -> None:
    print(f"[{_now_timestamp()}] {message}")


def _parse_w5_values(raw: str) -> list[float]:
    values = [item.strip() for item in str(raw).split(",")]
    parsed: list[float] = []
    for value in values:
        if not value:
            continue
        parsed.append(float(value))
    if not parsed:
        raise ValueError("--w5-values cannot be empty.")
    return parsed


def _parse_risk_term_values(raw: str) -> list[float]:
    parsed = _parse_w5_values(raw)
    if not parsed:
        raise ValueError("--risk-term-values cannot be empty.")
    return parsed


def _parse_iso_date(text: str) -> pd.Timestamp:
    return pd.to_datetime(str(text), format="%Y-%m-%d", errors="raise")


def _business_date_column(frame: pd.DataFrame) -> str:
    if "as_of_date" in frame.columns:
        return "as_of_date"
    if "date" in frame.columns:
        return "date"
    raise ValueError("Dashboard CSV must contain either as_of_date or date column.")


def _is_nightly_row(frame: pd.DataFrame) -> pd.Series:
    if "mode" in frame.columns:
        mode = frame["mode"].astype(str).str.strip().str.lower()
        return mode.eq("nightly")
    release_status = frame.get("release_status", pd.Series([""] * len(frame)))
    return release_status.astype(str).str.strip().eq("")


def _filter_dashboard_window_rows(frame: pd.DataFrame, *, start_date: str, end_date: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    date_column = _business_date_column(frame)
    frame = frame.copy()
    frame["_source_row_number"] = range(len(frame))
    frame["_business_date"] = pd.to_datetime(frame[date_column], errors="coerce")
    frame = frame.dropna(subset=["_business_date"])
    start = _parse_iso_date(start_date)
    end = _parse_iso_date(end_date)
    frame = frame[frame["_business_date"].between(start, end, inclusive="both")]
    frame = frame[_is_nightly_row(frame)].copy()
    if "notes" in frame.columns:
        replay_rows = frame["notes"].astype(str).str.startswith("historical_replay_")
        if replay_rows.any():
            frame = frame[replay_rows].copy()
    return frame


def _count_duplicate_business_dates(path: Path, *, start_date: str, end_date: str) -> int:
    if not path.exists():
        raise FileNotFoundError(f"Dashboard not found: {path}")
    frame = pd.read_csv(path, encoding="utf-8-sig")
    if frame.empty:
        return 0
    filtered = _filter_dashboard_window_rows(frame, start_date=start_date, end_date=end_date)
    if filtered.empty:
        return 0
    return int(filtered.duplicated(subset=["_business_date"]).sum())


def load_dashboard_window(path: Path, *, start_date: str, end_date: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dashboard not found: {path}")
    frame = pd.read_csv(path, encoding="utf-8-sig")
    if frame.empty:
        return frame
    frame = _filter_dashboard_window_rows(frame, start_date=start_date, end_date=end_date)
    frame = frame.sort_values(["_business_date", "_source_row_number"]).drop_duplicates(
        subset=["_business_date"],
        keep=DEDUP_KEEP_STRATEGY,
    )
    frame["date"] = frame["_business_date"].dt.strftime("%Y-%m-%d")
    return frame.reset_index(drop=True)


def _first_present(columns: list[str], frame: pd.DataFrame) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _to_numeric(series: pd.Series | None) -> pd.Series | None:
    if series is None:
        return None
    return pd.to_numeric(series, errors="coerce")


def _success_mask(frame: pd.DataFrame) -> pd.Series:
    if "pipeline_success" in frame.columns:
        text = frame["pipeline_success"].astype(str).str.strip().str.lower()
        return text.isin({"true", "1", "yes", "pass", "passed"})
    if "nightly_status" in frame.columns:
        return frame["nightly_status"].astype(str).str.strip().str.lower().eq("pass")
    return pd.Series([False] * len(frame))


def _collect_main_audit_paths(run_root: Path) -> list[Path]:
    samples_root = run_root / "samples"
    if not samples_root.exists():
        return []
    return sorted(samples_root.glob("*/main/audit.json"))


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return float(parsed)


def _aggregate_audit_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if "run_root" not in frame.columns:
        return {
            "order_count_total": None,
            "gross_traded_notional_total": None,
            "estimated_total_cost_total": None,
            "target_deviation_improvement_mean": None,
            "target_deviation_improvement_missing": True,
            "audit_sample_count": 0,
            "audit_run_root_count": 0,
        }

    run_roots = [
        Path(str(item))
        for item in frame["run_root"].astype(str).dropna().to_list()
        if str(item).strip()
    ]
    if not run_roots:
        return {
            "order_count_total": None,
            "gross_traded_notional_total": None,
            "estimated_total_cost_total": None,
            "target_deviation_improvement_mean": None,
            "target_deviation_improvement_missing": True,
            "audit_sample_count": 0,
            "audit_run_root_count": 0,
        }

    order_count_total = 0
    gross_traded_notional_total = 0.0
    estimated_total_cost_total = 0.0
    target_deviation_values: list[float] = []
    target_deviation_missing = False
    sample_count = 0

    for run_root in run_roots:
        audit_paths = _collect_main_audit_paths(run_root)
        for audit_path in audit_paths:
            sample_count += 1
            try:
                payload = json.loads(audit_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                target_deviation_missing = True
                continue

            orders = payload.get("orders", [])
            if isinstance(orders, list):
                order_count_total += len(orders)

            summary = payload.get("summary", {})
            if not isinstance(summary, dict):
                target_deviation_missing = True
                continue

            gross_value = _safe_float(summary.get("gross_traded_notional"))
            if gross_value is not None:
                gross_traded_notional_total += gross_value

            cost_value = _safe_float(summary.get("estimated_total_cost"))
            if cost_value is not None:
                estimated_total_cost_total += cost_value

            target_value = _safe_float(summary.get("target_deviation_improvement"))
            if target_value is None:
                target_deviation_missing = True
            else:
                target_deviation_values.append(target_value)

    if sample_count == 0:
        return {
            "order_count_total": None,
            "gross_traded_notional_total": None,
            "estimated_total_cost_total": None,
            "target_deviation_improvement_mean": None,
            "target_deviation_improvement_missing": True,
            "audit_sample_count": 0,
            "audit_run_root_count": len(run_roots),
        }

    target_deviation_improvement_mean = (
        float(sum(target_deviation_values) / len(target_deviation_values))
        if target_deviation_values
        else None
    )
    return {
        "order_count_total": int(order_count_total),
        "gross_traded_notional_total": float(gross_traded_notional_total),
        "estimated_total_cost_total": float(estimated_total_cost_total),
        "target_deviation_improvement_mean": target_deviation_improvement_mean,
        "target_deviation_improvement_missing": bool(target_deviation_missing or not target_deviation_values),
        "audit_sample_count": int(sample_count),
        "audit_run_root_count": len(run_roots),
    }


def _aggregate_daily_audit_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    daily_columns = ["date", "estimated_total_cost_daily", "target_deviation_improvement_daily"]
    if frame.empty or "date" not in frame.columns:
        return pd.DataFrame(columns=daily_columns)
    if "run_root" not in frame.columns:
        return pd.DataFrame(
            {
                "date": frame["date"].astype(str).to_list(),
                "estimated_total_cost_daily": [pd.NA] * len(frame),
                "target_deviation_improvement_daily": [pd.NA] * len(frame),
            }
        )

    rows: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        run_root_raw = row.get("run_root")
        run_root_text = str(run_root_raw).strip() if run_root_raw is not None else ""
        if not run_root_text:
            rows.append(
                {
                    "date": str(row.get("date", "")),
                    "estimated_total_cost_daily": pd.NA,
                    "target_deviation_improvement_daily": pd.NA,
                }
            )
            continue

        run_root = Path(run_root_text)
        audit_paths = _collect_main_audit_paths(run_root)
        cost_total = 0.0
        has_cost = False
        target_values: list[float] = []
        for audit_path in audit_paths:
            try:
                payload = json.loads(audit_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            summary = payload.get("summary", {})
            if not isinstance(summary, dict):
                continue

            cost_value = _safe_float(summary.get("estimated_total_cost"))
            if cost_value is not None:
                cost_total += float(cost_value)
                has_cost = True

            target_value = _safe_float(summary.get("target_deviation_improvement"))
            if target_value is not None:
                target_values.append(float(target_value))

        rows.append(
            {
                "date": str(row.get("date", "")),
                "estimated_total_cost_daily": float(cost_total) if has_cost else pd.NA,
                "target_deviation_improvement_daily": (
                    float(sum(target_values) / len(target_values)) if target_values else pd.NA
                ),
            }
        )
    return pd.DataFrame(rows, columns=daily_columns)


def compute_dashboard_metrics(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    if frame.empty:
        metrics = {
            "rows": 0,
            "success_rate": None,
            "override_rate_avg": None,
            "cost_better_ratio_avg": None,
            "turnover_avg": None,
            "solver_time_avg": None,
            "clarabel_convergence_rate": None,
            "order_count_total": None,
            "gross_traded_notional_total": None,
            "estimated_total_cost_total": None,
            "target_deviation_improvement_mean": None,
            "target_deviation_improvement_missing": True,
            "audit_sample_count": 0,
            "audit_run_root_count": 0,
        }
        return metrics, pd.DataFrame(
            columns=[
                "date",
                "cost",
                "turnover",
                "override",
                "target_deviation_improvement",
                "estimated_total_cost_daily",
                "target_deviation_improvement_daily",
            ]
        )

    success = _success_mask(frame)
    override_column = _first_present(["override_used_count", "override_count"], frame)
    cost_column = _first_present(["cost_better_ratio", "cost_better_ratio_static"], frame)
    turnover_column = _first_present(["turnover", "turnover_ratio", "avg_turnover"], frame)
    solver_time_column = _first_present(["solver_time", "solver_time_seconds", "solver_runtime_seconds"], frame)
    solver_name_column = _first_present(["solver_name", "solver_primary"], frame)

    override_series = _to_numeric(frame[override_column]) if override_column else None
    cost_series = _to_numeric(frame[cost_column]) if cost_column else None
    turnover_series = _to_numeric(frame[turnover_column]) if turnover_column else None
    solver_time_series = _to_numeric(frame[solver_time_column]) if solver_time_column else None

    clarabel_rate: float | None = None
    if solver_name_column is not None:
        solver_series = frame[solver_name_column].astype(str).str.strip().str.upper()
        clarabel_rate = float((solver_series == "CLARABEL").mean())
    audit_metrics = _aggregate_audit_metrics(frame)

    metrics = {
        "rows": int(len(frame)),
        "success_rate": float(success.mean()),
        "override_rate_avg": float(override_series.mean()) if override_series is not None else None,
        "cost_better_ratio_avg": float(cost_series.mean()) if cost_series is not None else None,
        "turnover_avg": float(turnover_series.mean()) if turnover_series is not None else None,
        "solver_time_avg": float(solver_time_series.mean()) if solver_time_series is not None else None,
        "clarabel_convergence_rate": clarabel_rate,
        "order_count_total": audit_metrics.get("order_count_total"),
        "gross_traded_notional_total": audit_metrics.get("gross_traded_notional_total"),
        "estimated_total_cost_total": audit_metrics.get("estimated_total_cost_total"),
        "target_deviation_improvement_mean": audit_metrics.get("target_deviation_improvement_mean"),
        "target_deviation_improvement_missing": bool(audit_metrics.get("target_deviation_improvement_missing", True)),
        "audit_sample_count": int(audit_metrics.get("audit_sample_count", 0)),
        "audit_run_root_count": int(audit_metrics.get("audit_run_root_count", 0)),
    }
    daily = pd.DataFrame(
        {
            "date": frame["date"].astype(str),
            "cost": (cost_series if cost_series is not None else pd.Series([pd.NA] * len(frame))).to_list(),
            "turnover": (
                turnover_series if turnover_series is not None else pd.Series([pd.NA] * len(frame))
            ).to_list(),
            "override": (
                override_series if override_series is not None else pd.Series([pd.NA] * len(frame))
            ).to_list(),
            "target_deviation_improvement": pd.Series([pd.NA] * len(frame)).to_list(),
        }
    )
    audit_daily = _aggregate_daily_audit_metrics(frame)
    if not audit_daily.empty:
        daily = daily.merge(audit_daily, on="date", how="left")
    else:
        daily["estimated_total_cost_daily"] = pd.NA
        daily["target_deviation_improvement_daily"] = pd.NA
    return metrics, daily


def build_overlay_payload(
    risk_term_value: float,
    risk_inputs_dir: Path,
    *,
    risk_integration_mode: str = "replace",
    tracking_error_weight: float = 0.0,
    transaction_cost_weight: float = 0.0,
) -> dict[str, Any]:
    returns_path = (risk_inputs_dir / "returns_long.csv").resolve()
    factor_path = (risk_inputs_dir / "factor_exposure.csv").resolve()
    payload: dict[str, Any] = {
        "risk_model": {
            "enabled": True,
            "integration_mode": str(risk_integration_mode).strip().lower(),
            "estimator": "ledoit_wolf",
            "returns_path": str(returns_path),
        },
        "objective_weights": {
            "risk_term": float(risk_term_value),
            "tracking_error": float(tracking_error_weight),
            "transaction_cost": float(transaction_cost_weight),
        },
    }
    if factor_path.exists():
        payload["risk_model"]["factor_exposure_path"] = str(factor_path)
    return payload


def _w5_label(value: float) -> str:
    return str(value).replace(".", "_")


def write_overlay_file(
    output_dir: Path,
    w5_value: float,
    risk_inputs_dir: Path,
    *,
    risk_integration_mode: str = "replace",
    tracking_error_weight: float = 0.0,
    transaction_cost_weight: float = 0.0,
) -> Path:
    target_dir = output_dir / f"config_w5_{_w5_label(w5_value)}"
    target_dir.mkdir(parents=True, exist_ok=True)
    overlay_path = target_dir / "overlay.yaml"
    payload = build_overlay_payload(
        w5_value,
        risk_inputs_dir,
        risk_integration_mode=risk_integration_mode,
        tracking_error_weight=tracking_error_weight,
        transaction_cost_weight=transaction_cost_weight,
    )
    overlay_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return overlay_path


def _run_subprocess(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )


def _load_progress_status(tracking_dir: Path) -> str:
    progress_path = tracking_dir / "replay_progress.json"
    if not progress_path.exists():
        return ""
    payload = json.loads(progress_path.read_text(encoding="utf-8"))
    return str(payload.get("status", "")).strip().lower()


def _metric_text(value: float | None, *, style: str) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    if style == "int":
        return str(int(round(float(value))))
    if style == "percent":
        return f"{float(value):.2%}"
    if style == "float":
        return f"{float(value):.4f}"
    if style == "money":
        return f"{float(value):.2f}"
    if style == "seconds":
        return f"{float(value):.2f}"
    return f"{float(value):.4f}"


def _metric_delta_text(value: float | None, baseline: float | None, *, style: str) -> str:
    if value is None or baseline is None or pd.isna(value) or pd.isna(baseline):
        return "N/A"
    delta = float(value) - float(baseline)
    if style == "int":
        return f"{int(round(delta)):+d}"
    if style == "percent":
        return f"{delta:+.2%}"
    if style == "seconds":
        return f"{delta:+.2f}s"
    if style == "money":
        return f"{delta:+.2f}"
    return f"{delta:+.4f}"


def _metric_delta_pct_text(value: float | None, baseline: float | None) -> str:
    if value is None or baseline is None or pd.isna(value) or pd.isna(baseline):
        return "N/A"
    baseline_value = float(baseline)
    if abs(baseline_value) <= 1e-12:
        return "N/A"
    delta_pct = (float(value) - baseline_value) / abs(baseline_value)
    return f"{delta_pct:+.2%}"


def _daily_detail_markdown(baseline: pd.DataFrame, risk: pd.DataFrame) -> list[str]:
    merged = baseline.merge(risk, on="date", how="outer", suffixes=("_baseline", "_risk")).sort_values("date")
    lines = [
        "| date | baseline_cost | risk_cost | baseline_turnover | risk_turnover | baseline_override | risk_override |",
        "|------|---------------|----------|-------------------|--------------|-------------------|--------------|",
    ]
    for row in merged.to_dict(orient="records"):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("date", "")),
                    _metric_text(row.get("cost_baseline"), style="float"),
                    _metric_text(row.get("cost_risk"), style="float"),
                    _metric_text(row.get("turnover_baseline"), style="percent"),
                    _metric_text(row.get("turnover_risk"), style="percent"),
                    _metric_text(row.get("override_baseline"), style="float"),
                    _metric_text(row.get("override_risk"), style="float"),
                ]
            )
            + " |"
        )
    return lines


def _missing_metric_families(metrics: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for family, key in METRIC_FAMILY_KEYS:
        value = metrics.get(key)
        if value is None or pd.isna(value):
            missing.append(family)
    return missing


def build_comparison_eligibility(
    *,
    baseline_duplicate_dates: int,
    baseline_daily: pd.DataFrame,
    run_results: list[ReplayRunResult],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    def _append_reason(reason: str) -> None:
        if reason not in reasons:
            reasons.append(reason)

    if int(baseline_duplicate_dates) > 0:
        _append_reason("baseline duplicate dates detected")

    baseline_dates = set(baseline_daily["date"].astype(str).to_list()) if not baseline_daily.empty else set()
    for result in run_results:
        run_dates = set(result.daily["date"].astype(str).to_list()) if not result.daily.empty else set()
        if run_dates != baseline_dates:
            _append_reason(f"risk_term_weight={result.w5_value}: date mismatch vs baseline")
        denominator = max(len(baseline_dates), len(run_dates), 1)
        overlap_days = len(baseline_dates.intersection(run_dates))
        overlap_ratio = float(overlap_days) / float(denominator)
        if overlap_ratio < MIN_OVERLAP_RATIO:
            _append_reason(
                "risk_term_weight="
                f"{result.w5_value}: insufficient overlap ({overlap_days}/{denominator}, ratio={overlap_ratio:.2f})"
            )

    return (len(reasons) == 0), reasons


def build_baseline_quality_flags(*, baseline_duplicate_dates: int, baseline_metrics: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if int(baseline_duplicate_dates) > 0:
        flags.append("baseline_duplicate_dates")
    for family in _missing_metric_families(baseline_metrics):
        flags.append(f"baseline_missing_metric_family:{family}")
    return flags


def write_comparison_eligibility_artifact(
    *,
    output_dir: Path,
    eligible: bool,
    reasons: list[str],
    baseline_quality_flags: list[str],
) -> Path:
    payload = {
        "eligible": bool(eligible),
        "reasons": [str(item) for item in reasons],
        "baseline_quality_flags": [str(item) for item in baseline_quality_flags],
    }
    target_path = output_dir / "comparison_eligibility.json"
    target_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return target_path


def build_data_quality_warnings(
    *,
    baseline_duplicate_dates: int,
    baseline_metrics: dict[str, Any],
    baseline_daily: pd.DataFrame,
    run_results: list[ReplayRunResult],
) -> list[str]:
    warnings: list[str] = []
    if int(baseline_duplicate_dates) > 0:
        warnings.append(
            (
                "Baseline contains duplicate business dates: "
                f"{int(baseline_duplicate_dates)} duplicate row(s) were deduplicated by keep-last."
            )
        )

    baseline_missing = _missing_metric_families(baseline_metrics)
    if baseline_missing:
        warnings.append(
            "Baseline missing metric families -> "
            + ", ".join(baseline_missing)
            + " (rendered as N/A)."
        )
    if bool(baseline_metrics.get("target_deviation_improvement_missing", False)):
        warnings.append("Baseline target_deviation_improvement_mean missing from audit summaries (rendered as N/A).")

    baseline_dates = set(baseline_daily["date"].astype(str).to_list()) if not baseline_daily.empty else set()
    for result in run_results:
        run_missing = _missing_metric_families(result.metrics)
        if run_missing:
            warnings.append(
                f"risk_term_weight={result.w5_value}: missing metric families -> "
                + ", ".join(run_missing)
                + " (rendered as N/A)."
            )
        if bool(result.metrics.get("target_deviation_improvement_missing", False)):
            warnings.append(
                "risk_term_weight="
                f"{result.w5_value}: target_deviation_improvement_mean missing from audit summaries (rendered as N/A)."
            )
        run_dates = set(result.daily["date"].astype(str).to_list()) if not result.daily.empty else set()
        if run_dates != baseline_dates:
            missing_in_variant = sorted(baseline_dates - run_dates)
            extra_in_variant = sorted(run_dates - baseline_dates)
            warnings.append(
                (
                    f"risk_term_weight={result.w5_value}: date coverage mismatch vs baseline "
                    f"(missing_in_variant={missing_in_variant or ['none']}, "
                    f"extra_in_variant={extra_in_variant or ['none']})."
                )
            )
    return warnings


def render_ab_report(
    *,
    config: ABConfig,
    risk_input_meta: dict[str, Any],
    baseline_metrics: dict[str, Any],
    baseline_daily: pd.DataFrame,
    run_results: list[ReplayRunResult],
    data_quality_warnings: list[str] | None = None,
    comparison_eligible: bool = True,
    comparison_eligibility_reasons: list[str] | None = None,
    baseline_quality_flags: list[str] | None = None,
) -> str:
    headers = ["Metric", "Baseline"]
    for result in run_results:
        headers.extend(
            [
                f"risk_term_weight={result.w5_value} (abs)",
                f"risk_term_weight={result.w5_value} (delta)",
                f"risk_term_weight={result.w5_value} (delta%)",
            ]
        )
    metrics_config = [
        ("Success rate", "success_rate", "percent"),
        ("Override rate (avg)", "override_rate_avg", "float"),
        ("Cost better ratio (avg)", "cost_better_ratio_avg", "percent"),
        ("Avg turnover per rebalance", "turnover_avg", "percent"),
        ("Avg solver time (s)", "solver_time_avg", "seconds"),
        ("CLARABEL convergence rate", "clarabel_convergence_rate", "percent"),
        ("order_count_total", "order_count_total", "int"),
        ("gross_traded_notional_total", "gross_traded_notional_total", "money"),
        ("estimated_total_cost_total", "estimated_total_cost_total", "money"),
        ("target_deviation_improvement_mean", "target_deviation_improvement_mean", "float"),
    ]

    lines = [
        "# Risk Model A/B Comparison Report",
        "",
        "## Test Configuration",
        (
            f"- Baseline: risk_model.enabled=false "
            f"({baseline_metrics.get('rows', 0)} days, {_metric_text(baseline_metrics.get('success_rate'), style='percent')} success)"
        ),
        f"- Date range: {config.start_date} to {config.end_date}",
        f"- Market: {config.market}",
        (
            f"- Risk inputs: {risk_input_meta.get('ticker_count', 'N/A')} tickers, "
            f"{risk_input_meta.get('actual_trading_days', 'N/A')} days lookback"
        ),
        f"- risk_term_weight values tested: {', '.join(str(value) for value in config.w5_values)}",
        f"- risk_integration_mode: {config.risk_integration_mode}",
        f"- tracking_error_weight (fixed): {config.tracking_error_weight}",
        f"- transaction_cost_weight (fixed): {config.transaction_cost_weight}",
        "",
        "## Summary Comparison",
        "",
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    for title, key, style in metrics_config:
        baseline_value = baseline_metrics.get(key)
        row_values = [title, _metric_text(baseline_value, style=style)]
        for result in run_results:
            current = result.metrics.get(key)
            row_values.extend(
                [
                    _metric_text(current, style=style),
                    _metric_delta_text(current, baseline_value, style=style),
                    _metric_delta_pct_text(current, baseline_value),
                ]
            )
        lines.append("| " + " | ".join(row_values) + " |")

    core_rows = build_core_result_rows(
        baseline_metrics=baseline_metrics,
        run_results=run_results,
    )
    lines.extend(["", "## Highlighted Core KPI Tradeoff"])
    lines.extend(_core_kpi_highlight_markdown(core_rows))

    lines.extend(["", "## Comparison Eligibility", f"- status: {'eligible' if comparison_eligible else 'ineligible'}"])
    if comparison_eligibility_reasons:
        for reason in comparison_eligibility_reasons:
            lines.append(f"- reason: {reason}")
    else:
        lines.append("- reason: none")
    if baseline_quality_flags:
        for flag in baseline_quality_flags:
            lines.append(f"- baseline_quality_flag: {flag}")
    else:
        lines.append("- baseline_quality_flag: none")

    lines.extend(["", "## Data Quality Warnings"])
    if data_quality_warnings:
        for warning in data_quality_warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- none")

    for result in run_results:
        lines.extend(
            [
                "",
                f"## Daily Detail: risk_term_weight={result.w5_value}",
                f"- status: {result.status} (progress={result.progress_status or 'unknown'}, exit={result.return_code})",
            ]
        )
        lines.extend(_daily_detail_markdown(baseline_daily, result.daily))

    lines.extend(["", "## Conclusion"])
    if not comparison_eligible:
        lines.append(f"- {INELIGIBLE_DECISION_TEXT}")
        lines.append("")
        return "\n".join(lines)

    for result in run_results:
        if result.status != "completed":
            lines.append(f"- risk_term_weight={result.w5_value}: replay did not complete ({result.status}).")
            continue
        comparisons: list[str] = []
        for label, key in [
            ("success_rate", "success_rate"),
            ("override_rate", "override_rate_avg"),
            ("cost_better_ratio", "cost_better_ratio_avg"),
            ("clarabel_convergence_rate", "clarabel_convergence_rate"),
            ("estimated_total_cost_total", "estimated_total_cost_total"),
            ("gross_traded_notional_total", "gross_traded_notional_total"),
            ("target_deviation_improvement_mean", "target_deviation_improvement_mean"),
        ]:
            base = baseline_metrics.get(key)
            cur = result.metrics.get(key)
            if base is None or cur is None:
                comparisons.append(f"{label}=N/A")
            else:
                delta = float(cur) - float(base)
                direction = "improved" if delta > 1e-9 else "worsened" if delta < -1e-9 else "unchanged"
                comparisons.append(f"{label} {direction} ({delta:+.4f})")
        lines.append(f"- risk_term_weight={result.w5_value}: " + ", ".join(comparisons))
    lines.append("")
    return "\n".join(lines)


def _risk_inputs_metadata(risk_inputs_dir: Path) -> dict[str, Any]:
    manifest_path = risk_inputs_dir / "risk_inputs_manifest.json"
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    returns_path = risk_inputs_dir / "returns_long.csv"
    if not returns_path.exists():
        return {"ticker_count": "N/A", "actual_trading_days": "N/A"}
    frame = pd.read_csv(returns_path, encoding="utf-8-sig")
    return {
        "ticker_count": int(frame["ticker"].nunique()) if "ticker" in frame.columns else "N/A",
        "actual_trading_days": int(frame["date"].nunique()) if "date" in frame.columns else "N/A",
    }


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _previous_business_day(date_text: str) -> str:
    current = _parse_iso_date(date_text).to_pydatetime().date() - timedelta(days=1)
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current.isoformat()


def _returns_end_date_from_inputs(risk_inputs_dir: Path, risk_input_meta: dict[str, Any]) -> str | None:
    date_range = risk_input_meta.get("date_range")
    if isinstance(date_range, list) and len(date_range) >= 2 and str(date_range[1]).strip():
        return str(date_range[1]).strip()
    returns_path = risk_inputs_dir / "returns_long.csv"
    if not returns_path.exists():
        return None
    frame = pd.read_csv(returns_path, encoding="utf-8-sig")
    if "date" not in frame.columns or frame.empty:
        return None
    parsed = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if parsed.empty:
        return None
    return parsed.max().strftime("%Y-%m-%d")


def build_time_alignment_check(
    *,
    start_date: str,
    risk_inputs_dir: Path,
    risk_input_meta: dict[str, Any],
) -> dict[str, Any]:
    returns_end_date = _returns_end_date_from_inputs(risk_inputs_dir, risk_input_meta)
    required_max_end_date = _previous_business_day(start_date)
    if returns_end_date is None:
        return {
            "aligned": False,
            "returns_end_date": "N/A",
            "required_max_end_date": required_max_end_date,
            "message": "time alignment unavailable: returns_end_date is missing",
        }
    aligned = str(returns_end_date) <= str(required_max_end_date)
    if aligned:
        message = (
            f"time alignment pass: returns_end_date={returns_end_date} <= required_max_end_date={required_max_end_date}"
        )
    else:
        message = (
            f"time alignment violation: returns_end_date={returns_end_date} > required_max_end_date={required_max_end_date}"
        )
    return {
        "aligned": bool(aligned),
        "returns_end_date": str(returns_end_date),
        "required_max_end_date": str(required_max_end_date),
        "message": message,
    }


def _paired_metric_stats(baseline_series: pd.Series, risk_series: pd.Series) -> dict[str, Any]:
    baseline = pd.to_numeric(baseline_series, errors="coerce")
    risk = pd.to_numeric(risk_series, errors="coerce")
    delta = (risk - baseline).dropna()
    n_pairs = int(len(delta))
    if n_pairs == 0:
        return {
            "n_pairs": 0,
            "mean_delta": None,
            "ci95_low": None,
            "ci95_high": None,
            "confidence": "insufficient_data",
        }
    mean_delta = float(delta.mean())
    if n_pairs == 1:
        std = 0.0
    else:
        std = float(delta.std(ddof=1))
    ci_half = 1.96 * (std / (n_pairs**0.5)) if n_pairs > 0 else 0.0
    ci95_low = float(mean_delta - ci_half)
    ci95_high = float(mean_delta + ci_half)
    if ci95_low > 0:
        confidence = "positive_delta_ci_excludes_zero"
    elif ci95_high < 0:
        confidence = "negative_delta_ci_excludes_zero"
    else:
        confidence = "inconclusive_ci_crosses_zero"
    return {
        "n_pairs": n_pairs,
        "mean_delta": mean_delta,
        "ci95_low": ci95_low,
        "ci95_high": ci95_high,
        "confidence": confidence,
    }


def build_paired_daily_delta(
    *,
    baseline_daily: pd.DataFrame,
    run_results: list[ReplayRunResult],
) -> dict[float, dict[str, dict[str, Any]]]:
    output: dict[float, dict[str, dict[str, Any]]] = {}
    for result in run_results:
        merged = baseline_daily.merge(result.daily, on="date", how="inner", suffixes=("_baseline", "_risk"))
        metric_payload: dict[str, dict[str, Any]] = {}
        for metric in ("cost", "turnover", "override"):
            metric_payload[metric] = _paired_metric_stats(
                merged[f"{metric}_baseline"] if f"{metric}_baseline" in merged.columns else pd.Series(dtype=float),
                merged[f"{metric}_risk"] if f"{metric}_risk" in merged.columns else pd.Series(dtype=float),
            )
        output[float(result.w5_value)] = metric_payload
    return output


def _ci_text(low: float | None, high: float | None) -> str:
    if low is None or high is None:
        return "N/A"
    return f"[{low:+.4f}, {high:+.4f}]"


def _mean_text(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:+.4f}"


def _recommendation_from_stats_legacy(
    *,
    comparison_eligible: bool,
    run_results: list[ReplayRunResult],
    paired_delta: dict[float, dict[str, dict[str, Any]]],
) -> dict[str, str]:
    if not comparison_eligible:
        return {
            "default_risk_model": "off",
            "recommended_w5_range": "暂不建议固定默认值",
            "rationale": "comparison ineligible; result is not suitable for strategy decision-making",
        }

    supported_w5: list[float] = []
    for result in run_results:
        stats = paired_delta.get(float(result.w5_value), {})
        cost_stats = stats.get("cost", {})
        turnover_stats = stats.get("turnover", {})
        cost_ci_low = cost_stats.get("ci95_low")
        turnover_ci_high = turnover_stats.get("ci95_high")
        if (
            isinstance(cost_ci_low, float)
            and isinstance(turnover_ci_high, float)
            and cost_ci_low > 0.0
            and turnover_ci_high <= 0.0
        ):
            supported_w5.append(float(result.w5_value))

    if not supported_w5:
        return {
            "default_risk_model": "off",
            "recommended_w5_range": "暂不建议固定默认值",
            "rationale": "no statistically supported w5 under paired-delta CI criteria",
        }

    w5_min = min(supported_w5)
    w5_max = max(supported_w5)
    return {
        "default_risk_model": "on (guarded rollout)",
        "recommended_w5_range": f"{w5_min:.4f} ~ {w5_max:.4f}",
        "rationale": "paired-delta CI supports cost improvement without turnover deterioration",
    }


def _recommendation_from_stats(
    *,
    comparison_eligible: bool,
    run_results: list[ReplayRunResult],
    paired_delta: dict[float, dict[str, dict[str, Any]]],
) -> dict[str, str]:
    if not comparison_eligible:
        return {
            "default_risk_model": "off",
            "recommended_risk_term_range": "not recommended yet",
            "rationale": "comparison ineligible; result is not suitable for strategy decision-making",
        }

    supported_values: list[float] = []
    for result in run_results:
        stats = paired_delta.get(float(result.w5_value), {})
        cost_stats = stats.get("cost", {})
        turnover_stats = stats.get("turnover", {})
        cost_ci_low = cost_stats.get("ci95_low")
        turnover_ci_high = turnover_stats.get("ci95_high")
        if (
            isinstance(cost_ci_low, float)
            and isinstance(turnover_ci_high, float)
            and cost_ci_low > 0.0
            and turnover_ci_high <= 0.0
        ):
            supported_values.append(float(result.w5_value))

    if not supported_values:
        return {
            "default_risk_model": "off",
            "recommended_risk_term_range": "not recommended yet",
            "rationale": "no statistically supported risk_term_weight under paired-delta CI criteria",
        }

    risk_term_min = min(supported_values)
    risk_term_max = max(supported_values)
    return {
        "default_risk_model": "on (guarded rollout)",
        "recommended_risk_term_range": f"{risk_term_min:.4f} ~ {risk_term_max:.4f}",
        "rationale": "paired-delta CI supports cost improvement without turnover deterioration",
    }


def render_ab_v2_report(
    *,
    config: ABConfig,
    comparison_eligible: bool,
    comparison_eligibility_reasons: list[str],
    baseline_quality_flags: list[str],
    data_quality_warnings: list[str],
    time_alignment_check: dict[str, Any],
    baseline_daily: pd.DataFrame,
    run_results: list[ReplayRunResult],
    paired_delta: dict[float, dict[str, dict[str, Any]]],
    recommendation: dict[str, str],
) -> str:
    readiness_text = "Eligible" if comparison_eligible else "Ineligible"
    baseline_dates = set(baseline_daily["date"].astype(str).to_list()) if not baseline_daily.empty else set()
    sample_scope_aligned = True
    for result in run_results:
        run_dates = set(result.daily["date"].astype(str).to_list()) if not result.daily.empty else set()
        if run_dates != baseline_dates:
            sample_scope_aligned = False
            break

    lines = [
        "# Risk Model A/B V2 Report",
        "",
        "## Eligibility & Data Quality Gate",
        f"- Decision Readiness: {readiness_text}",
        f"- market_alignment_check: PASS (baseline/risk market={config.market})",
        f"- date_window_check: PASS ({config.start_date}..{config.end_date})",
        f"- sample_scope_check: {'PASS' if sample_scope_aligned else 'FAIL'}",
        f"- time_alignment_check: {'PASS' if bool(time_alignment_check.get('aligned')) else 'FAIL'}",
        f"- time_alignment_detail: {time_alignment_check.get('message', 'N/A')}",
        "",
        f"- status: {'eligible' if comparison_eligible else 'ineligible'}",
    ]
    if comparison_eligibility_reasons:
        for reason in comparison_eligibility_reasons:
            lines.append(f"- reason: {reason}")
    else:
        lines.append("- reason: none")
    if baseline_quality_flags:
        for flag in baseline_quality_flags:
            lines.append(f"- baseline_quality_flag: {flag}")
    else:
        lines.append("- baseline_quality_flag: none")
    if data_quality_warnings:
        for warning in data_quality_warnings:
            lines.append(f"- data_quality_warning: {warning}")
    else:
        lines.append("- data_quality_warning: none")

    lines.extend(["", "## Paired Daily Delta"])
    for result in run_results:
        stats_by_metric = paired_delta.get(float(result.w5_value), {})
        lines.extend(
            [
                "",
                f"### risk_term_weight={result.w5_value}",
                "| metric | n_pairs | mean_delta (risk-baseline) | 95% CI | confidence |",
                "|---|---:|---:|---|---|",
            ]
        )
        for metric in ("cost", "turnover", "override"):
            stats = stats_by_metric.get(metric, {})
            lines.append(
                "| "
                + " | ".join(
                    [
                        metric,
                        str(int(stats.get("n_pairs", 0) or 0)),
                        _mean_text(stats.get("mean_delta")),
                        _ci_text(stats.get("ci95_low"), stats.get("ci95_high")),
                        str(stats.get("confidence", "insufficient_data")),
                    ]
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## Statistical Confidence",
            "- Policy: do not conclude from single average only; paired daily deltas with CI must be reviewed.",
            "",
            "| risk_term_weight | cost_ci95 | turnover_ci95 | confidence_note |",
            "|---|---|---|---|",
        ]
    )
    for result in run_results:
        stats = paired_delta.get(float(result.w5_value), {})
        cost_stats = stats.get("cost", {})
        turnover_stats = stats.get("turnover", {})
        lines.append(
            "| "
            + " | ".join(
                [
                    str(result.w5_value),
                    _ci_text(cost_stats.get("ci95_low"), cost_stats.get("ci95_high")),
                    _ci_text(turnover_stats.get("ci95_low"), turnover_stats.get("ci95_high")),
                    f"cost={cost_stats.get('confidence', 'insufficient_data')}, turnover={turnover_stats.get('confidence', 'insufficient_data')}",
                ]
            )
            + " |"
        )

    lines.extend(["", "## Decision Readiness", f"- Decision Readiness: {readiness_text}"])
    if not comparison_eligible:
        lines.append(f"- {INELIGIBLE_DECISION_TEXT}")
    else:
        lines.append("- Eligibility and data-quality gate passed; confidence table should drive decision interpretation.")

    lines.extend(
        [
            "",
            "## Recommendation",
            "| item | recommendation | rationale |",
            "|---|---|---|",
            f"| default risk_model | {recommendation.get('default_risk_model', 'off')} | {recommendation.get('rationale', '')} |",
            f"| recommended w5 range | {recommendation.get('recommended_w5_range', '暂不建议固定默认值')} | {recommendation.get('rationale', '')} |",
            "",
        ]
    )
    recommended_range = recommendation.get("recommended_risk_term_range")
    if recommended_range is None:
        recommended_range = recommendation.get("recommended_w5_range")
    if not str(recommended_range or "").strip():
        recommended_range = "not recommended yet"
    for idx, value in enumerate(lines):
        if str(value).startswith("| recommended w5 range |"):
            lines[idx] = (
                f"| recommended risk_term range | {recommended_range} | {recommendation.get('rationale', '')} |"
            )
            break
    return "\n".join(lines)


def write_experiment_manifest(
    *,
    output_dir: Path,
    config: ABConfig,
    risk_input_meta: dict[str, Any],
    time_alignment_check: dict[str, Any],
    run_results: list[ReplayRunResult],
    comparison_eligible: bool,
    comparison_eligibility_reasons: list[str],
    baseline_quality_flags: list[str],
) -> Path:
    payload = {
        "experiment_version": AB_EXPERIMENT_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "script": "scripts/run_risk_ab_comparison.py",
        "baseline_dashboard": str(config.baseline_dashboard),
        "risk_inputs_dir": str(config.risk_inputs_dir),
        "market": config.market,
        "phase": config.phase,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "risk_term_values": [float(value) for value in config.w5_values],
        "risk_integration_mode": str(config.risk_integration_mode).strip().lower(),
        "tracking_error_weight": float(config.tracking_error_weight),
        "transaction_cost_weight": float(config.transaction_cost_weight),
        "daily_tradeoff_weights": [float(value) for value in (config.daily_tradeoff_weights or [])],
        "cool_down": float(config.cool_down),
        "max_failures": int(config.max_failures),
        "require_eligible": bool(config.require_eligible),
        "real_sample": bool(config.real_sample),
        "replay_require_eligibility_gate": bool(config.replay_require_eligibility_gate),
        "risk_inputs_meta": risk_input_meta,
        "time_alignment": time_alignment_check,
        "comparison_eligibility": {
            "eligible": bool(comparison_eligible),
            "reasons": [str(item) for item in comparison_eligibility_reasons],
            "baseline_quality_flags": [str(item) for item in baseline_quality_flags],
        },
        "run_results": [
            {
                "risk_term_weight": float(item.w5_value),
                "status": str(item.status),
                "return_code": int(item.return_code),
                "progress_status": str(item.progress_status),
                "tracking_dir": str(item.tracking_dir),
                "dashboard_path": str(item.dashboard_path),
                "metrics": item.metrics,
            }
            for item in run_results
        ],
    }
    target_path = output_dir / "experiment_manifest.json"
    target_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target_path


def _delta_and_pct(value: float | None, baseline: float | None) -> tuple[float | None, float | None]:
    if value is None or baseline is None or pd.isna(value) or pd.isna(baseline):
        return None, None
    delta = float(value) - float(baseline)
    baseline_value = float(baseline)
    if abs(baseline_value) <= 1e-12:
        return delta, None
    return delta, (delta / abs(baseline_value))


def _format_delta_and_pct(
    *,
    delta: float | None,
    delta_pct: float | None,
    style: str,
) -> str:
    if delta is None or delta_pct is None or pd.isna(delta) or pd.isna(delta_pct):
        return "N/A"
    if style == "int":
        return f"{int(round(float(delta))):+d} ({float(delta_pct):+.2%})"
    if style == "money":
        return f"{float(delta):+.2f} ({float(delta_pct):+.2%})"
    return f"{float(delta):+.4f} ({float(delta_pct):+.2%})"


def _core_kpi_highlight_markdown(core_rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| group | risk_term_weight | status | order_count_total | order_count_delta | gross_traded_notional_total | gross_delta | estimated_total_cost_total | cost_delta | target_deviation_improvement_mean | target_deviation_delta |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in core_rows:
        is_baseline = str(row.get("group", "")).strip().lower() == "baseline"
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("group", "")),
                    f"{float(row.get('risk_term_weight', 0.0)):.4g}",
                    str(row.get("status", "")),
                    _metric_text(row.get("order_count_total"), style="int"),
                    (
                        "baseline"
                        if is_baseline
                        else _format_delta_and_pct(
                            delta=row.get("order_count_total_delta"),
                            delta_pct=row.get("order_count_total_delta_pct"),
                            style="int",
                        )
                    ),
                    _metric_text(row.get("gross_traded_notional_total"), style="money"),
                    (
                        "baseline"
                        if is_baseline
                        else _format_delta_and_pct(
                            delta=row.get("gross_traded_notional_total_delta"),
                            delta_pct=row.get("gross_traded_notional_total_delta_pct"),
                            style="money",
                        )
                    ),
                    _metric_text(row.get("estimated_total_cost_total"), style="money"),
                    (
                        "baseline"
                        if is_baseline
                        else _format_delta_and_pct(
                            delta=row.get("estimated_total_cost_total_delta"),
                            delta_pct=row.get("estimated_total_cost_total_delta_pct"),
                            style="money",
                        )
                    ),
                    _metric_text(row.get("target_deviation_improvement_mean"), style="float"),
                    (
                        "baseline"
                        if is_baseline
                        else _format_delta_and_pct(
                            delta=row.get("target_deviation_improvement_mean_delta"),
                            delta_pct=row.get("target_deviation_improvement_mean_delta_pct"),
                            style="float",
                        )
                    ),
                ]
            )
            + " |"
        )
    return lines


def build_core_result_rows(
    *,
    baseline_metrics: dict[str, Any],
    run_results: list[ReplayRunResult],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.append(
        {
            "group": "baseline",
            "risk_term_weight": 0.0,
            "status": "completed",
            "order_count_total": baseline_metrics.get("order_count_total"),
            "gross_traded_notional_total": baseline_metrics.get("gross_traded_notional_total"),
            "estimated_total_cost_total": baseline_metrics.get("estimated_total_cost_total"),
            "target_deviation_improvement_mean": baseline_metrics.get("target_deviation_improvement_mean"),
            "override_rate_avg": baseline_metrics.get("override_rate_avg"),
            "cost_better_ratio_avg": baseline_metrics.get("cost_better_ratio_avg"),
        }
    )

    base_order = baseline_metrics.get("order_count_total")
    base_gross = baseline_metrics.get("gross_traded_notional_total")
    base_cost = baseline_metrics.get("estimated_total_cost_total")
    base_target_dev = baseline_metrics.get("target_deviation_improvement_mean")
    base_override = baseline_metrics.get("override_rate_avg")

    for result in run_results:
        order_value = result.metrics.get("order_count_total")
        gross_value = result.metrics.get("gross_traded_notional_total")
        cost_value = result.metrics.get("estimated_total_cost_total")
        target_value = result.metrics.get("target_deviation_improvement_mean")
        override_value = result.metrics.get("override_rate_avg")

        order_delta, order_delta_pct = _delta_and_pct(order_value, base_order)
        gross_delta, gross_delta_pct = _delta_and_pct(gross_value, base_gross)
        cost_delta, cost_delta_pct = _delta_and_pct(cost_value, base_cost)
        target_delta, target_delta_pct = _delta_and_pct(target_value, base_target_dev)
        override_delta, override_delta_pct = _delta_and_pct(override_value, base_override)
        rows.append(
            {
                "group": "variant",
                "risk_term_weight": float(result.w5_value),
                "status": str(result.status),
                "order_count_total": order_value,
                "order_count_total_delta": order_delta,
                "order_count_total_delta_pct": order_delta_pct,
                "gross_traded_notional_total": gross_value,
                "gross_traded_notional_total_delta": gross_delta,
                "gross_traded_notional_total_delta_pct": gross_delta_pct,
                "estimated_total_cost_total": cost_value,
                "estimated_total_cost_total_delta": cost_delta,
                "estimated_total_cost_total_delta_pct": cost_delta_pct,
                "target_deviation_improvement_mean": target_value,
                "target_deviation_improvement_mean_delta": target_delta,
                "target_deviation_improvement_mean_delta_pct": target_delta_pct,
                "override_rate_avg": override_value,
                "override_rate_avg_delta": override_delta,
                "override_rate_avg_delta_pct": override_delta_pct,
                "cost_better_ratio_avg": result.metrics.get("cost_better_ratio_avg"),
            }
        )
    return rows


def _metric_delta_pct_points(
    core_rows: list[dict[str, Any]],
    *,
    key: str,
) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    for row in core_rows:
        if str(row.get("group", "")).strip().lower() != "variant":
            continue
        if str(row.get("status", "")).strip().lower() != "completed":
            continue
        value = row.get(key)
        if value is None or pd.isna(value):
            continue
        points.append((float(row.get("risk_term_weight", 0.0)), float(value)))
    return sorted(points, key=lambda item: item[0])


def _monotonicity_diagnostics(points: list[tuple[float, float]]) -> dict[str, Any]:
    values = [value for _, value in points]
    if len(values) < 2:
        return {
            "n_points": len(values),
            "trend": "insufficient_points",
            "reversal_count": 0,
            "local_noise_risk": False,
            "delta_pct_by_weight": [{"risk_term_weight": weight, "delta_pct": value} for weight, value in points],
        }

    diffs = [values[idx] - values[idx - 1] for idx in range(1, len(values))]
    monotonic_non_decreasing = all(diff >= -1e-12 for diff in diffs)
    monotonic_non_increasing = all(diff <= 1e-12 for diff in diffs)
    if monotonic_non_decreasing and not monotonic_non_increasing:
        trend = "non_decreasing"
    elif monotonic_non_increasing and not monotonic_non_decreasing:
        trend = "non_increasing"
    elif monotonic_non_decreasing and monotonic_non_increasing:
        trend = "flat"
    else:
        trend = "non_monotonic"

    directional_signs: list[int] = []
    for diff in diffs:
        if diff > 1e-12:
            directional_signs.append(1)
        elif diff < -1e-12:
            directional_signs.append(-1)
    reversal_count = 0
    for idx in range(1, len(directional_signs)):
        if directional_signs[idx] != directional_signs[idx - 1]:
            reversal_count += 1
    return {
        "n_points": len(values),
        "trend": trend,
        "reversal_count": int(reversal_count),
        "local_noise_risk": bool(reversal_count > 0),
        "delta_pct_by_weight": [{"risk_term_weight": weight, "delta_pct": value} for weight, value in points],
    }


def build_refined_monotonicity_report(core_rows: list[dict[str, Any]]) -> dict[str, Any]:
    cost_points = _metric_delta_pct_points(core_rows, key="estimated_total_cost_total_delta_pct")
    target_points = _metric_delta_pct_points(core_rows, key="target_deviation_improvement_mean_delta_pct")
    return {
        "estimated_total_cost_total_delta_pct": _monotonicity_diagnostics(cost_points),
        "target_deviation_improvement_mean_delta_pct": _monotonicity_diagnostics(target_points),
    }


def _target_deterioration_pct(*, baseline: float | None, variant: float | None) -> float | None:
    if baseline is None or variant is None or pd.isna(baseline) or pd.isna(variant):
        return None
    baseline_value = float(baseline)
    variant_value = float(variant)
    if abs(baseline_value) <= 1e-12:
        return 0.0 if variant_value >= baseline_value - TARGET_DEVIATION_TOLERANCE else float("inf")
    return max(0.0, (baseline_value - variant_value) / abs(baseline_value))


def evaluate_threshold_policies(core_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not core_rows:
        return []
    baseline = core_rows[0]
    baseline_cost = baseline.get("estimated_total_cost_total")
    baseline_target = baseline.get("target_deviation_improvement_mean")
    variant_rows = [
        row
        for row in core_rows
        if str(row.get("group", "")).strip().lower() == "variant"
        and str(row.get("status", "")).strip().lower() == "completed"
    ]

    policy_results: list[dict[str, Any]] = []
    for policy_name, deterioration_limit in POLICY_DETERIORATION_LIMITS:
        candidates: list[dict[str, Any]] = []
        for row in variant_rows:
            target_value = row.get("target_deviation_improvement_mean")
            cost_value = row.get("estimated_total_cost_total")
            if (
                baseline_cost is None
                or cost_value is None
                or pd.isna(baseline_cost)
                or pd.isna(cost_value)
            ):
                continue

            deterioration_pct = _target_deterioration_pct(
                baseline=baseline_target,
                variant=target_value,
            )
            if deterioration_pct is None:
                continue
            if float(deterioration_pct) > float(deterioration_limit) + 1e-12:
                continue

            cost_improvement = float(baseline_cost) - float(cost_value)
            candidates.append(
                {
                    "risk_term_weight": float(row.get("risk_term_weight", 0.0)),
                    "cost_improvement": cost_improvement,
                    "target_deterioration_pct": float(deterioration_pct),
                    "row": row,
                }
            )

        if not candidates:
            policy_results.append(
                {
                    "policy": policy_name,
                    "deterioration_limit_pct": float(deterioration_limit),
                    "status": "NO_FEASIBLE_WEIGHT",
                    "recommended_risk_term_weight": None,
                }
            )
            continue

        best = max(
            candidates,
            key=lambda item: (
                float(item["cost_improvement"]),
                -float(item["target_deterioration_pct"]),
                -float(item["risk_term_weight"]),
            ),
        )
        best_row = best["row"]
        policy_results.append(
            {
                "policy": policy_name,
                "deterioration_limit_pct": float(deterioration_limit),
                "status": "RECOMMENDED",
                "recommended_risk_term_weight": float(best["risk_term_weight"]),
                "target_deterioration_pct": float(best["target_deterioration_pct"]),
                "cost_improvement": float(best["cost_improvement"]),
                "order_count_total": best_row.get("order_count_total"),
                "gross_traded_notional_total": best_row.get("gross_traded_notional_total"),
                "estimated_total_cost_total": best_row.get("estimated_total_cost_total"),
                "target_deviation_improvement_mean": best_row.get("target_deviation_improvement_mean"),
                "order_count_total_delta": best_row.get("order_count_total_delta"),
                "order_count_total_delta_pct": best_row.get("order_count_total_delta_pct"),
                "gross_traded_notional_total_delta": best_row.get("gross_traded_notional_total_delta"),
                "gross_traded_notional_total_delta_pct": best_row.get("gross_traded_notional_total_delta_pct"),
                "estimated_total_cost_total_delta": best_row.get("estimated_total_cost_total_delta"),
                "estimated_total_cost_total_delta_pct": best_row.get("estimated_total_cost_total_delta_pct"),
                "target_deviation_improvement_mean_delta": best_row.get("target_deviation_improvement_mean_delta"),
                "target_deviation_improvement_mean_delta_pct": best_row.get("target_deviation_improvement_mean_delta_pct"),
            }
        )
    return policy_results


def render_threshold_policy_report(
    *,
    config: ABConfig,
    core_rows: list[dict[str, Any]],
    policy_results: list[dict[str, Any]],
    monotonicity: dict[str, Any],
) -> str:
    lines = [
        "# Risk A/B Threshold Policy Evaluation",
        "",
        "## Configuration",
        f"- market: {config.market}",
        f"- date_range: {config.start_date}..{config.end_date}",
        f"- risk_integration_mode: {config.risk_integration_mode}",
        f"- risk_term_weight grid: {', '.join(str(value) for value in config.w5_values)}",
        f"- tracking_error_weight: {config.tracking_error_weight}",
        f"- transaction_cost_weight: {config.transaction_cost_weight}",
        "",
        "## Core KPI Snapshot",
    ]
    lines.extend(_core_kpi_highlight_markdown(core_rows))
    lines.extend(
        [
            "",
            "## Monotonicity & Stability Check",
            "| metric | trend | reversal_count | local_noise_risk |",
            "|---|---|---:|---|",
        ]
    )
    for metric_name, diagnostics in monotonicity.items():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(metric_name),
                    str(diagnostics.get("trend", "N/A")),
                    str(int(diagnostics.get("reversal_count", 0) or 0)),
                    "YES" if bool(diagnostics.get("local_noise_risk", False)) else "NO",
                ]
            )
            + " |"
        )
    if any(bool(item.get("local_noise_risk", False)) for item in monotonicity.values()):
        lines.append("- local noise risk note: non-monotonic reversal detected in refined grid; interpret nearby points cautiously.")
    else:
        lines.append("- local noise risk note: no obvious non-monotonic reversal observed in refined grid.")

    lines.extend(
        [
            "",
            "## Policy A/B/C Recommendation",
            "| policy | target_deviation deterioration cap | status | recommended_weight | order_count_total | gross_traded_notional_total | estimated_total_cost_total | target_deviation_improvement_mean |",
            "|---|---:|---|---:|---:|---:|---:|---:|",
        ]
    )
    for result in policy_results:
        if str(result.get("status")) == "NO_FEASIBLE_WEIGHT":
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(result.get("policy", "")),
                        f"{float(result.get('deterioration_limit_pct', 0.0)):.2%}",
                        "NO_FEASIBLE_WEIGHT",
                        "N/A",
                        "N/A",
                        "N/A",
                        "N/A",
                        "N/A",
                    ]
                )
                + " |"
            )
            continue
        lines.append(
            "| "
            + " | ".join(
                [
                    str(result.get("policy", "")),
                    f"{float(result.get('deterioration_limit_pct', 0.0)):.2%}",
                    str(result.get("status", "")),
                    f"{float(result.get('recommended_risk_term_weight', 0.0)):.4g}",
                    _metric_text(result.get("order_count_total"), style="int"),
                    _metric_text(result.get("gross_traded_notional_total"), style="money"),
                    _metric_text(result.get("estimated_total_cost_total"), style="money"),
                    _metric_text(result.get("target_deviation_improvement_mean"), style="float"),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def build_daily_tradeoff_frame(
    *,
    baseline_daily: pd.DataFrame,
    run_results: list[ReplayRunResult],
    required_weights: list[float],
) -> pd.DataFrame:
    if not required_weights:
        raise ValueError("daily tradeoff export requires at least one weight.")

    result_by_weight = {
        float(result.w5_value): result
        for result in run_results
        if str(result.status).strip().lower() == "completed"
    }
    missing_weights = [weight for weight in required_weights if float(weight) not in result_by_weight]
    if missing_weights:
        raise ValueError(
            "Missing required weights for daily tradeoff export: " + ", ".join(f"{float(item):.4g}" for item in missing_weights)
        )

    rows: list[dict[str, Any]] = []
    for weight in required_weights:
        result = result_by_weight[float(weight)]
        merged = baseline_daily.merge(result.daily, on="date", how="inner", suffixes=("_baseline", "_variant"))
        merged = merged.sort_values("date")
        for row in merged.to_dict(orient="records"):
            baseline_cost = _safe_float(row.get("estimated_total_cost_daily_baseline"))
            variant_cost = _safe_float(row.get("estimated_total_cost_daily_variant"))
            baseline_target = _safe_float(row.get("target_deviation_improvement_daily_baseline"))
            variant_target = _safe_float(row.get("target_deviation_improvement_daily_variant"))
            rows.append(
                {
                    "date": str(row.get("date", "")),
                    "weight": float(weight),
                    "baseline_estimated_total_cost_daily": baseline_cost,
                    "variant_estimated_total_cost_daily": variant_cost,
                    "delta_cost_daily": (
                        float(variant_cost) - float(baseline_cost)
                        if baseline_cost is not None and variant_cost is not None
                        else None
                    ),
                    "baseline_target_deviation_improvement_daily": baseline_target,
                    "variant_target_deviation_improvement_daily": variant_target,
                    "delta_target_deviation_daily": (
                        float(variant_target) - float(baseline_target)
                        if baseline_target is not None and variant_target is not None
                        else None
                    ),
                }
            )
    tradeoff_frame = pd.DataFrame(rows)
    if tradeoff_frame.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "weight",
                "baseline_estimated_total_cost_daily",
                "variant_estimated_total_cost_daily",
                "delta_cost_daily",
                "baseline_target_deviation_improvement_daily",
                "variant_target_deviation_improvement_daily",
                "delta_target_deviation_daily",
            ]
        )
    return tradeoff_frame.sort_values(["weight", "date"]).reset_index(drop=True)


def _has_monotonic_same_sign_triplet(values: list[float]) -> bool:
    if len(values) < 3:
        return False
    for idx in range(len(values) - 2):
        triplet = values[idx : idx + 3]
        if all(item > 0 for item in triplet) or all(item < 0 for item in triplet):
            non_decreasing = triplet[0] <= triplet[1] <= triplet[2]
            non_increasing = triplet[0] >= triplet[1] >= triplet[2]
            if non_decreasing or non_increasing:
                return True
    return False


def evaluate_calibration_decision(
    *,
    baseline_metrics: dict[str, Any],
    run_results: list[ReplayRunResult],
) -> dict[str, Any]:
    baseline_cost = baseline_metrics.get("estimated_total_cost_total")
    baseline_gross = baseline_metrics.get("gross_traded_notional_total")
    baseline_target_dev = baseline_metrics.get("target_deviation_improvement_mean")
    baseline_override = baseline_metrics.get("override_rate_avg")

    valid_variants = [result for result in run_results if str(result.status) == "completed"]
    condition_1 = False
    condition_2 = True
    condition_3 = True
    condition_4 = True
    cost_delta_pcts: list[tuple[float, float]] = []
    target_delta_pcts: list[tuple[float, float]] = []

    for result in sorted(valid_variants, key=lambda item: float(item.w5_value)):
        cost = result.metrics.get("estimated_total_cost_total")
        gross = result.metrics.get("gross_traded_notional_total")
        target_dev = result.metrics.get("target_deviation_improvement_mean")
        override_rate = result.metrics.get("override_rate_avg")

        _, cost_delta_pct = _delta_and_pct(cost, baseline_cost)
        if cost_delta_pct is not None:
            if float(cost_delta_pct) <= -0.02:
                condition_1 = True
            cost_delta_pcts.append((float(result.w5_value), float(cost_delta_pct)))

        _, target_delta_pct = _delta_and_pct(target_dev, baseline_target_dev)
        if target_delta_pct is not None:
            target_delta_pcts.append((float(result.w5_value), float(target_delta_pct)))

        _, gross_delta_pct = _delta_and_pct(gross, baseline_gross)
        if gross_delta_pct is not None and float(gross_delta_pct) < -0.10:
            condition_2 = False

        if (
            baseline_target_dev is not None
            and target_dev is not None
            and float(target_dev) < float(baseline_target_dev) - TARGET_DEVIATION_TOLERANCE
        ):
            condition_3 = False

        if baseline_override is not None and override_rate is not None and float(override_rate) > float(
            baseline_override
        ) + 1e-9:
            condition_4 = False

    ordered_cost_delta_pct = [item[1] for item in sorted(cost_delta_pcts, key=lambda item: item[0])]
    condition_5 = _has_monotonic_same_sign_triplet(ordered_cost_delta_pct)
    monotonicity_report = {
        "estimated_total_cost_total_delta_pct": _monotonicity_diagnostics(
            sorted(cost_delta_pcts, key=lambda item: item[0])
        ),
        "target_deviation_improvement_mean_delta_pct": _monotonicity_diagnostics(
            sorted(target_delta_pcts, key=lambda item: item[0])
        ),
    }

    no_observable_effect = bool(ordered_cost_delta_pct) and all(
        abs(value) <= NO_OBSERVABLE_EFFECT_THRESHOLD for value in ordered_cost_delta_pct
    )
    decision = "ENABLE_GUARDED" if (condition_1 and condition_2 and condition_3 and condition_4 and condition_5) else (
        "DO_NOT_ENABLE_DEFAULT"
    )
    return {
        "decision": decision,
        "conditions": {
            "cost_improvement_ge_2pct_exists": condition_1,
            "gross_notional_not_worse_than_10pct": condition_2,
            "target_deviation_not_worse": condition_3,
            "override_rate_not_up": condition_4,
            "three_adjacent_monotonic_same_sign_cost_delta_pct": condition_5,
        },
        "cost_delta_pct_by_weight": [
            {"risk_term_weight": weight, "delta_pct": delta_pct}
            for weight, delta_pct in sorted(cost_delta_pcts, key=lambda item: item[0])
        ],
        "target_deviation_delta_pct_by_weight": [
            {"risk_term_weight": weight, "delta_pct": delta_pct}
            for weight, delta_pct in sorted(target_delta_pcts, key=lambda item: item[0])
        ],
        "monotonicity": monotonicity_report,
        "no_observable_effect": no_observable_effect,
        "recommended_next_grid": [3e5, 1e6, 3e6] if no_observable_effect else [],
    }


def _core_results_markdown(core_rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| group | risk_term_weight | status | order_count_total | gross_traded_notional_total | estimated_total_cost_total | target_deviation_improvement_mean | override_rate_avg | cost_better_ratio_avg | delta_cost% | delta_gross% |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in core_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("group", "")),
                    f"{float(row.get('risk_term_weight', 0.0)):.4g}",
                    str(row.get("status", "")),
                    _metric_text(row.get("order_count_total"), style="int"),
                    _metric_text(row.get("gross_traded_notional_total"), style="money"),
                    _metric_text(row.get("estimated_total_cost_total"), style="money"),
                    _metric_text(row.get("target_deviation_improvement_mean"), style="float"),
                    _metric_text(row.get("override_rate_avg"), style="float"),
                    _metric_text(row.get("cost_better_ratio_avg"), style="percent"),
                    _metric_delta_pct_text(row.get("estimated_total_cost_total"), core_rows[0].get("estimated_total_cost_total")),
                    _metric_delta_pct_text(
                        row.get("gross_traded_notional_total"), core_rows[0].get("gross_traded_notional_total")
                    ),
                ]
            )
            + " |"
        )
    return lines


def render_calibration_decision_report(
    *,
    config: ABConfig,
    core_rows: list[dict[str, Any]],
    decision_payload: dict[str, Any],
) -> str:
    lines = [
        "# Risk A/B Calibration Decision",
        "",
        "## Configuration",
        f"- market: {config.market}",
        f"- date_range: {config.start_date}..{config.end_date}",
        f"- risk_integration_mode: {config.risk_integration_mode}",
        f"- tracking_error_weight: {config.tracking_error_weight}",
        f"- transaction_cost_weight: {config.transaction_cost_weight}",
        f"- risk_term_weight grid: {', '.join(str(value) for value in config.w5_values)}",
        "",
        "## Core Results",
    ]
    lines.extend(_core_results_markdown(core_rows))
    lines.extend(
        [
            "",
            "## Decision Rules",
            f"- 1) estimated_total_cost_total improvement >=2% exists: {decision_payload['conditions']['cost_improvement_ge_2pct_exists']}",
            f"- 2) gross_traded_notional_total not worse than -10%: {decision_payload['conditions']['gross_notional_not_worse_than_10pct']}",
            f"- 3) target_deviation_improvement_mean not worse vs baseline: {decision_payload['conditions']['target_deviation_not_worse']}",
            f"- 4) override_rate not up: {decision_payload['conditions']['override_rate_not_up']}",
            "- 5) at least one 3-point adjacent monotonic same-sign run on estimated_total_cost_total delta%: "
            + str(decision_payload["conditions"]["three_adjacent_monotonic_same_sign_cost_delta_pct"]),
            "",
            "## Refined Grid Monotonicity Check",
            "| metric | trend | reversal_count | local_noise_risk |",
            "|---|---|---:|---|",
        ]
    )
    monotonicity = decision_payload.get("monotonicity", {})
    for metric_name in (
        "estimated_total_cost_total_delta_pct",
        "target_deviation_improvement_mean_delta_pct",
    ):
        metric_payload = monotonicity.get(metric_name, {})
        lines.append(
            "| "
            + " | ".join(
                [
                    str(metric_name),
                    str(metric_payload.get("trend", "N/A")),
                    str(int(metric_payload.get("reversal_count", 0) or 0)),
                    "YES" if bool(metric_payload.get("local_noise_risk", False)) else "NO",
                ]
            )
            + " |"
        )
    if any(bool(item.get("local_noise_risk", False)) for item in monotonicity.values()):
        lines.append("- local noise risk note: non-monotonic reversal detected in refined grid.")
    else:
        lines.append("- local noise risk note: no obvious non-monotonic reversal in refined grid.")
    lines.extend(
        [
            "",
            "## Hard Conclusion",
            f"- {decision_payload['decision']}",
        ]
    )
    if decision_payload.get("no_observable_effect", False):
        lines.extend(
            [
                "",
                "## Next Grid Recommendation",
                "- No observable effect in 1e2~1e5; recommend next grid: 3e5, 1e6, 3e6",
            ]
        )
    lines.append("")
    return "\n".join(lines)


def write_diagnostic_manifest(
    *,
    output_dir: Path,
    config: ABConfig,
    report_path: Path,
    v2_report_path: Path,
    decision_path: Path,
    threshold_policy_path: Path,
    daily_tradeoff_path: Path,
    experiment_manifest_path: Path,
    comparison_eligibility_path: Path,
    core_rows: list[dict[str, Any]],
    decision_payload: dict[str, Any],
    policy_results: list[dict[str, Any]],
    monotonicity: dict[str, Any],
    daily_tradeoff_weights: list[float] | None,
) -> Path:
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "output_dir": str(output_dir),
        "market": config.market,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "risk_integration_mode": config.risk_integration_mode,
        "tracking_error_weight": float(config.tracking_error_weight),
        "transaction_cost_weight": float(config.transaction_cost_weight),
        "risk_term_values": [float(value) for value in config.w5_values],
        "artifacts": {
            "risk_ab_report": str(report_path),
            "risk_ab_v2_report": str(v2_report_path),
            "risk_ab_calibration_decision": str(decision_path),
            "risk_ab_threshold_policy": str(threshold_policy_path),
            "risk_ab_daily_tradeoff": str(daily_tradeoff_path),
            "comparison_eligibility": str(comparison_eligibility_path),
            "experiment_manifest": str(experiment_manifest_path),
        },
        "core_results": core_rows,
        "decision": decision_payload,
        "policy_results": policy_results,
        "monotonicity": monotonicity,
        "daily_tradeoff_weights": [float(value) for value in (daily_tradeoff_weights or [])],
    }
    target_path = output_dir / "diagnostic_manifest.json"
    target_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target_path


def run_risk_ab_comparison(
    config: ABConfig,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = _run_subprocess,
    logger: Callable[[str], None] = _log,
) -> int:
    if not config.baseline_dashboard.exists():
        raise FileNotFoundError(f"Baseline dashboard not found: {config.baseline_dashboard}")
    returns_path = config.risk_inputs_dir / "returns_long.csv"
    if not returns_path.exists():
        raise FileNotFoundError(f"Missing risk inputs file: {returns_path}")

    baseline_window = load_dashboard_window(
        config.baseline_dashboard,
        start_date=config.start_date,
        end_date=config.end_date,
    )
    baseline_duplicate_dates = _count_duplicate_business_dates(
        config.baseline_dashboard,
        start_date=config.start_date,
        end_date=config.end_date,
    )
    if baseline_window.empty:
        raise ValueError("Baseline dashboard has no nightly rows in the requested date range.")
    baseline_metrics, baseline_daily = compute_dashboard_metrics(baseline_window)
    risk_input_meta = _risk_inputs_metadata(config.risk_inputs_dir)
    time_alignment_check = build_time_alignment_check(
        start_date=config.start_date,
        risk_inputs_dir=config.risk_inputs_dir,
        risk_input_meta=risk_input_meta,
    )
    logger(
        f"Baseline rows={baseline_metrics.get('rows', 0)}, success={_metric_text(baseline_metrics.get('success_rate'), style='percent')}"
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    results: list[ReplayRunResult] = []
    replay_script = ROOT / "scripts" / "pilot_historical_replay.py"
    pilot_ops_script = ROOT / "scripts" / "pilot_ops.py"

    for w5 in config.w5_values:
        label = _w5_label(w5)
        logger(f"Running replay for risk_term_weight={w5} ...")
        overlay_path = write_overlay_file(
            config.output_dir,
            w5,
            config.risk_inputs_dir,
            risk_integration_mode=config.risk_integration_mode,
            tracking_error_weight=config.tracking_error_weight,
            transaction_cost_weight=config.transaction_cost_weight,
        )
        tracking_dir = config.output_dir / f"tracking_w5_{label}"
        tracking_dir.mkdir(parents=True, exist_ok=True)

        init_command = [
            sys.executable,
            str(pilot_ops_script),
            "init",
            "--output-dir",
            str(tracking_dir),
        ]
        init_completed = runner(init_command, cwd=ROOT)
        if int(init_completed.returncode) != 0:
            results.append(
                ReplayRunResult(
                    w5_value=w5,
                    status="failed",
                    return_code=int(init_completed.returncode),
                    tracking_dir=tracking_dir,
                    dashboard_path=tracking_dir / "pilot_dashboard.csv",
                    progress_status="",
                    metrics={},
                    daily=pd.DataFrame(
                        columns=[
                            "date",
                            "cost",
                            "turnover",
                            "override",
                            "target_deviation_improvement",
                            "estimated_total_cost_daily",
                            "target_deviation_improvement_daily",
                        ]
                    ),
                    stderr=str(init_completed.stderr or ""),
                )
            )
            logger(f"Warning: init failed for risk_term_weight={w5} (exit={init_completed.returncode}).")
            continue

        replay_command = [
            sys.executable,
            str(replay_script),
            "--start-date",
            config.start_date,
            "--end-date",
            config.end_date,
            "--phase",
            config.phase,
            "--market",
            config.market,
            "--cool-down",
            str(config.cool_down),
            "--max-failures",
            str(config.max_failures),
            "--output-dir",
            str(tracking_dir),
            "--config-overlay",
            str(overlay_path),
            "--ab-flow",
        ]
        if config.replay_require_eligibility_gate:
            replay_command.append("--require-eligibility-gate")
        if config.real_sample:
            replay_command.append("--real-sample")
        replay_completed = runner(replay_command, cwd=ROOT)
        progress_status = _load_progress_status(tracking_dir)
        dashboard_path = tracking_dir / "pilot_dashboard.csv"

        run_status = "completed"
        if int(replay_completed.returncode) != 0:
            run_status = "failed"
        elif progress_status == "aborted":
            run_status = "aborted"

        run_window = pd.DataFrame()
        if dashboard_path.exists():
            run_window = load_dashboard_window(
                dashboard_path,
                start_date=config.start_date,
                end_date=config.end_date,
            )
        if run_window.empty and run_status == "completed":
            run_status = "failed"

        run_metrics, run_daily = compute_dashboard_metrics(run_window)
        results.append(
            ReplayRunResult(
                w5_value=w5,
                status=run_status,
                return_code=int(replay_completed.returncode),
                tracking_dir=tracking_dir,
                dashboard_path=dashboard_path,
                progress_status=progress_status,
                metrics=run_metrics,
                daily=run_daily,
                stderr=str(replay_completed.stderr or ""),
            )
        )
        logger(
            f"risk_term_weight={w5} finished with status={run_status}, "
            f"success={_metric_text(run_metrics.get('success_rate'), style='percent')}"
        )

    data_quality_warnings = build_data_quality_warnings(
        baseline_duplicate_dates=baseline_duplicate_dates,
        baseline_metrics=baseline_metrics,
        baseline_daily=baseline_daily,
        run_results=results,
    )
    baseline_quality_flags = build_baseline_quality_flags(
        baseline_duplicate_dates=baseline_duplicate_dates,
        baseline_metrics=baseline_metrics,
    )
    comparison_eligible, comparison_eligibility_reasons = build_comparison_eligibility(
        baseline_duplicate_dates=baseline_duplicate_dates,
        baseline_daily=baseline_daily,
        run_results=results,
    )
    if not bool(time_alignment_check.get("aligned", False)):
        _append_unique(
            comparison_eligibility_reasons,
            str(time_alignment_check.get("message", "time alignment violation")),
        )
        comparison_eligible = False
        data_quality_warnings.append(str(time_alignment_check.get("message", "time alignment violation")))

    report = render_ab_report(
        config=config,
        risk_input_meta=risk_input_meta,
        baseline_metrics=baseline_metrics,
        baseline_daily=baseline_daily,
        run_results=results,
        data_quality_warnings=data_quality_warnings,
        comparison_eligible=comparison_eligible,
        comparison_eligibility_reasons=comparison_eligibility_reasons,
        baseline_quality_flags=baseline_quality_flags,
    )
    report_path = config.output_dir / "risk_ab_report.md"
    report_path.write_text(report, encoding="utf-8")
    paired_delta = build_paired_daily_delta(
        baseline_daily=baseline_daily,
        run_results=results,
    )
    recommendation = _recommendation_from_stats(
        comparison_eligible=comparison_eligible,
        run_results=results,
        paired_delta=paired_delta,
    )
    v2_report = render_ab_v2_report(
        config=config,
        comparison_eligible=comparison_eligible,
        comparison_eligibility_reasons=comparison_eligibility_reasons,
        baseline_quality_flags=baseline_quality_flags,
        data_quality_warnings=data_quality_warnings,
        time_alignment_check=time_alignment_check,
        baseline_daily=baseline_daily,
        run_results=results,
        paired_delta=paired_delta,
        recommendation=recommendation,
    )
    v2_report_path = config.output_dir / "risk_ab_v2_report.md"
    v2_report_path.write_text(v2_report, encoding="utf-8")
    eligibility_path = write_comparison_eligibility_artifact(
        output_dir=config.output_dir,
        eligible=comparison_eligible,
        reasons=comparison_eligibility_reasons,
        baseline_quality_flags=baseline_quality_flags,
    )
    manifest_path = write_experiment_manifest(
        output_dir=config.output_dir,
        config=config,
        risk_input_meta=risk_input_meta,
        time_alignment_check=time_alignment_check,
        run_results=results,
        comparison_eligible=comparison_eligible,
        comparison_eligibility_reasons=comparison_eligibility_reasons,
        baseline_quality_flags=baseline_quality_flags,
    )
    core_rows = build_core_result_rows(
        baseline_metrics=baseline_metrics,
        run_results=results,
    )
    decision_payload = evaluate_calibration_decision(
        baseline_metrics=baseline_metrics,
        run_results=results,
    )
    decision_text = render_calibration_decision_report(
        config=config,
        core_rows=core_rows,
        decision_payload=decision_payload,
    )
    decision_path = config.output_dir / "risk_ab_calibration_decision.md"
    decision_path.write_text(decision_text, encoding="utf-8")
    monotonicity = decision_payload.get("monotonicity", build_refined_monotonicity_report(core_rows))
    policy_results = evaluate_threshold_policies(core_rows)
    threshold_policy_text = render_threshold_policy_report(
        config=config,
        core_rows=core_rows,
        policy_results=policy_results,
        monotonicity=monotonicity,
    )
    threshold_policy_path = config.output_dir / "risk_ab_threshold_policy.md"
    threshold_policy_path.write_text(threshold_policy_text, encoding="utf-8")

    completed_weights = sorted(
        {
            float(result.w5_value)
            for result in results
            if str(result.status).strip().lower() == "completed"
        }
    )
    daily_tradeoff_weights = (
        [float(value) for value in config.daily_tradeoff_weights]
        if config.daily_tradeoff_weights is not None
        else completed_weights[:3]
    )
    daily_tradeoff_columns = [
        "date",
        "weight",
        "baseline_estimated_total_cost_daily",
        "variant_estimated_total_cost_daily",
        "delta_cost_daily",
        "baseline_target_deviation_improvement_daily",
        "variant_target_deviation_improvement_daily",
        "delta_target_deviation_daily",
    ]
    if daily_tradeoff_weights:
        daily_tradeoff = build_daily_tradeoff_frame(
            baseline_daily=baseline_daily,
            run_results=results,
            required_weights=daily_tradeoff_weights,
        )
    else:
        daily_tradeoff = pd.DataFrame(columns=daily_tradeoff_columns)
    daily_tradeoff_path = config.output_dir / "risk_ab_daily_tradeoff.csv"
    daily_tradeoff.to_csv(daily_tradeoff_path, index=False, encoding="utf-8")

    diagnostic_manifest_path = write_diagnostic_manifest(
        output_dir=config.output_dir,
        config=config,
        report_path=report_path,
        v2_report_path=v2_report_path,
        decision_path=decision_path,
        threshold_policy_path=threshold_policy_path,
        daily_tradeoff_path=daily_tradeoff_path,
        experiment_manifest_path=manifest_path,
        comparison_eligibility_path=eligibility_path,
        core_rows=core_rows,
        decision_payload=decision_payload,
        policy_results=policy_results,
        monotonicity=monotonicity,
        daily_tradeoff_weights=daily_tradeoff_weights,
    )
    logger(f"risk_ab_report: {report_path}")
    logger(f"risk_ab_v2_report: {v2_report_path}")
    logger(f"risk_ab_calibration_decision: {decision_path}")
    logger(f"risk_ab_threshold_policy: {threshold_policy_path}")
    logger(f"risk_ab_daily_tradeoff: {daily_tradeoff_path}")
    logger(f"comparison_eligibility: {eligibility_path}")
    logger(f"experiment_manifest: {manifest_path}")
    logger(f"diagnostic_manifest: {diagnostic_manifest_path}")
    if config.require_eligible and not comparison_eligible:
        logger(REQUIRE_ELIGIBLE_FAILURE_MESSAGE)
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run risk-model A/B historical replay comparisons.")
    parser.add_argument("--baseline-dashboard", type=Path, required=True, help="Baseline pilot_dashboard.csv path.")
    parser.add_argument("--start-date", required=True, help="Replay start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="Replay end date (YYYY-MM-DD).")
    parser.add_argument("--phase", default="phase_1", help="Pilot phase passed to replay script.")
    parser.add_argument("--market", choices=["cn", "us"], default="cn", help="Market mode for replay.")
    parser.add_argument("--risk-inputs-dir", type=Path, required=True, help="Risk inputs directory containing returns_long.csv.")
    parser.add_argument(
        "--risk-term-values",
        default="0.05,0.1,0.2",
        help="Comma-separated risk_term weights (single-variable sweep).",
    )
    parser.add_argument(
        "--risk-integration-mode",
        choices=["replace", "augment"],
        default="replace",
        help="Risk objective integration mode used in overlay (default: replace).",
    )
    parser.add_argument(
        "--tracking-error-weight",
        type=float,
        default=0.0,
        help="Fixed tracking_error objective weight for all variants (default: 0.0).",
    )
    parser.add_argument(
        "--transaction-cost-weight",
        type=float,
        default=0.0,
        help="Fixed transaction_cost objective weight for all variants (default: 0.0).",
    )
    parser.add_argument(
        "--daily-tradeoff-weights",
        default="",
        help=(
            "Comma-separated risk_term weights for risk_ab_daily_tradeoff.csv export. "
            "When provided, all listed weights must exist among completed variants."
        ),
    )
    parser.add_argument(
        "--w5-values",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--cool-down", type=float, default=3.0, help="Cooldown passed to replay script.")
    parser.add_argument("--max-failures", type=int, default=5, help="Max consecutive nightly failures for replay script.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for A/B artifacts.")
    parser.add_argument(
        "--real-sample",
        action="store_true",
        help="Include real sample in replay runs (default: static-only).",
    )
    parser.add_argument(
        "--skip-replay-eligibility-gate",
        action="store_true",
        help="Do not pass --require-eligibility-gate to replay runs.",
    )
    parser.add_argument(
        "--require-eligible",
        action="store_true",
        help="Return non-zero when comparison eligibility is ineligible.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _parse_iso_date(str(args.start_date))
    _parse_iso_date(str(args.end_date))
    if int(args.max_failures) < 1:
        raise ValueError("--max-failures must be >= 1.")
    if float(args.cool_down) < 0:
        raise ValueError("--cool-down must be >= 0.")
    risk_term_values_raw = str(args.risk_term_values)
    if args.w5_values is not None and str(args.w5_values).strip():
        risk_term_values_raw = str(args.w5_values)

    config = ABConfig(
        baseline_dashboard=Path(args.baseline_dashboard).resolve(),
        start_date=str(args.start_date),
        end_date=str(args.end_date),
        phase=str(args.phase),
        market=str(args.market).strip().lower(),
        risk_inputs_dir=Path(args.risk_inputs_dir).resolve(),
        w5_values=_parse_risk_term_values(risk_term_values_raw),
        cool_down=float(args.cool_down),
        max_failures=int(args.max_failures),
        output_dir=Path(args.output_dir).resolve(),
        risk_integration_mode=str(args.risk_integration_mode).strip().lower(),
        tracking_error_weight=float(args.tracking_error_weight),
        transaction_cost_weight=float(args.transaction_cost_weight),
        require_eligible=bool(args.require_eligible),
        real_sample=bool(args.real_sample),
        replay_require_eligibility_gate=not bool(args.skip_replay_eligibility_gate),
        daily_tradeoff_weights=(
            _parse_risk_term_values(str(args.daily_tradeoff_weights))
            if str(args.daily_tradeoff_weights).strip()
            else None
        ),
    )
    return run_risk_ab_comparison(config)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
