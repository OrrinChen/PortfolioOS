"""Small-emotion Q2 execution-survival diagnostics.

This module consumes the Q2 candidate intake expected-return panel and checks
cost pressure, capacity, participation, holding paths, and whether the rows can
be staged for a PortfolioOS optimizer adapter. It does not run an optimizer,
portfolio construction, broker/order, live, paper, or production workflow.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
import re

import pandas as pd


STAGE = "Q2-SMALL-EMOTION-02"
NOTIONAL_USD = 25_000.0
STRESS_NOTIONAL_USD = 100_000.0
PARTICIPATION_25K_P95_LIMIT = 0.10
PARTICIPATION_100K_P95_LIMIT = 0.35
SPREAD_P95_LIMIT = 0.20


@dataclass(frozen=True)
class SmallEmotionQ2ExecutionSurvivalResult:
    """Written Q2 execution-survival artifacts and summary."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_q2_execution_survival(
    *,
    q2_intake_dir: str | Path,
    output_dir: str | Path,
    notional_usd: float = NOTIONAL_USD,
    stress_notional_usd: float = STRESS_NOTIONAL_USD,
) -> SmallEmotionQ2ExecutionSurvivalResult:
    """Run Q2 execution-survival checks for promoted small-emotion candidates."""

    intake_path = Path(q2_intake_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    expected = _read_csv(intake_path / "small_emotion_q2_expected_return_panel.csv")
    candidate_matrix = _read_csv(intake_path / "small_emotion_q2_candidate_matrix.csv")
    active = _active_rows(expected)
    no_view = _no_view_rows(expected)

    matrix_rows: list[dict[str, object]] = []
    cost_rows: list[dict[str, object]] = []
    holding_frames: list[pd.DataFrame] = []
    probe_rows: list[dict[str, object]] = []

    for candidate_name in _candidate_order(candidate_matrix, active):
        candidate_active = active[active["candidate_name"].astype(str).eq(candidate_name)].copy()
        candidate_no_view = no_view[no_view["candidate_name"].astype(str).eq(candidate_name)].copy()
        candidate_row = _candidate_metadata(candidate_matrix, candidate_name)
        costs, cost_metric_rows = _cost_capacity(candidate_active, candidate_name, notional_usd, stress_notional_usd)
        probe = _optimizer_probe(candidate_active)
        holding = _holding_path(candidate_active, candidate_name, notional_usd)

        required_status = _required_input_status(candidate_active)
        optimizer_probe_status = (
            "staged_optimizer_input_ready"
            if required_status == "pass" and not probe.empty and probe["optimizer_input_probe_status"].eq("staged_optimizer_input_ready").all()
            else "optimizer_input_unavailable"
        )
        survival_decision = _survival_decision(costs, optimizer_probe_status)

        matrix_rows.append(
            {
                "schema_version": "small_emotion_q2_execution_survival_matrix.v1",
                "stage": STAGE,
                "candidate_name": candidate_name,
                "measurement_spec_id": candidate_row.get("measurement_spec_id", ""),
                "measurement_spec_hash": candidate_row.get("measurement_spec_hash", ""),
                "primary_window": candidate_row.get("primary_window", _mode(candidate_active.get("primary_window"))),
                "active_expected_return_rows": int(len(candidate_active)),
                "no_view_rows_excluded": int(len(candidate_no_view)),
                "gross_directional_return_median": costs.get("gross_directional_return_median"),
                "participation_25k_p95": costs.get("participation_25k_p95"),
                "participation_100k_p95": costs.get("participation_100k_p95"),
                "spread_proxy_p95": costs.get("spread_proxy_p95"),
                "net_directional_return_25k_median": costs.get("net_directional_return_25k_median"),
                "cost_capacity_status": costs.get("overall_status"),
                "optimizer_input_probe_status": optimizer_probe_status,
                "survival_decision": survival_decision,
                "actual_optimizer_run": False,
                "portfolio_construction_allowed": False,
                "no_view_not_zero_alpha": True,
            }
        )
        cost_rows.extend(cost_metric_rows)
        if not holding.empty:
            holding_frames.append(holding)
        probe_rows.extend(probe.to_dict("records"))

    survival_matrix = pd.DataFrame(matrix_rows, columns=_survival_matrix_columns())
    cost_capacity_report = pd.DataFrame(cost_rows, columns=_cost_capacity_columns())
    holding_path = (
        pd.concat(holding_frames, ignore_index=True)
        if holding_frames
        else pd.DataFrame(columns=_holding_path_columns())
    )
    optimizer_probe = pd.DataFrame(probe_rows, columns=_optimizer_probe_columns())
    summary = _summary(survival_matrix, expected)

    survival_matrix.to_csv(artifacts["survival_matrix"], index=False)
    cost_capacity_report.to_csv(artifacts["cost_capacity_report"], index=False)
    holding_path.to_csv(artifacts["holding_path"], index=False)
    optimizer_probe.to_csv(artifacts["optimizer_input_probe"], index=False)
    artifacts["summary"].write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["report"].write_text(_report(summary, survival_matrix), encoding="utf-8")

    return SmallEmotionQ2ExecutionSurvivalResult(summary=summary, artifacts=artifacts)


def _active_rows(expected: pd.DataFrame) -> pd.DataFrame:
    if expected.empty:
        return pd.DataFrame(columns=expected.columns)
    q2_status = expected.get("q2_status", pd.Series("", index=expected.index)).astype(str)
    state = expected.get("signal_state", pd.Series("", index=expected.index)).astype(str).str.lower()
    rows = expected[state.eq("active") & q2_status.eq("opened_q2_candidate")].copy()
    rows["expected_return"] = pd.to_numeric(rows.get("expected_return"), errors="coerce")
    rows = rows[rows["expected_return"].notna()].copy()
    for column in ["adv20", "bid_ask_spread", "adjusted_close", "volume", "market_cap", "dollar_volume"]:
        if column in rows.columns:
            rows[column] = pd.to_numeric(rows[column], errors="coerce")
    return rows


def _no_view_rows(expected: pd.DataFrame) -> pd.DataFrame:
    if expected.empty:
        return pd.DataFrame(columns=expected.columns)
    state = expected.get("signal_state", pd.Series("", index=expected.index)).astype(str).str.lower()
    return expected[~state.eq("active")].copy()


def _candidate_order(candidate_matrix: pd.DataFrame, active: pd.DataFrame) -> list[str]:
    names: list[str] = []
    if not candidate_matrix.empty and "candidate_name" in candidate_matrix.columns:
        names.extend([str(name) for name in candidate_matrix["candidate_name"].dropna().tolist()])
    if not active.empty and "candidate_name" in active.columns:
        names.extend([str(name) for name in active["candidate_name"].dropna().tolist()])
    ordered: list[str] = []
    seen: set[str] = set()
    for name in names:
        if name and name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def _candidate_metadata(candidate_matrix: pd.DataFrame, candidate_name: str) -> dict[str, object]:
    if candidate_matrix.empty or "candidate_name" not in candidate_matrix.columns:
        return {}
    rows = candidate_matrix[candidate_matrix["candidate_name"].astype(str).eq(candidate_name)]
    return rows.iloc[0].to_dict() if not rows.empty else {}


def _cost_capacity(
    active: pd.DataFrame,
    candidate_name: str,
    notional_usd: float,
    stress_notional_usd: float,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    if active.empty:
        metrics = {
            "gross_directional_return_median": math.nan,
            "participation_25k_p95": math.nan,
            "participation_100k_p95": math.nan,
            "spread_proxy_p95": math.nan,
            "net_directional_return_25k_median": math.nan,
            "overall_status": "fail",
        }
        return metrics, _metric_rows(candidate_name, metrics)

    effective_adv = _effective_adv_dollars(active)
    spread = pd.to_numeric(active.get("bid_ask_spread"), errors="coerce")
    expected_abs = pd.to_numeric(active.get("expected_return"), errors="coerce").abs()

    participation_25k = notional_usd / effective_adv
    participation_100k = stress_notional_usd / effective_adv
    slippage_stress = spread + (0.20 * participation_25k)
    net_directional = expected_abs - slippage_stress

    metrics = {
        "gross_directional_return_median": _median(expected_abs),
        "participation_25k_p95": _quantile(participation_25k, 0.95),
        "participation_100k_p95": _quantile(participation_100k, 0.95),
        "spread_proxy_p95": _quantile(spread, 0.95),
        "net_directional_return_25k_median": _median(net_directional),
    }
    statuses = {
        "participation_25k_p95": _pass(metrics["participation_25k_p95"], PARTICIPATION_25K_P95_LIMIT, "<="),
        "participation_100k_p95": _pass(metrics["participation_100k_p95"], PARTICIPATION_100K_P95_LIMIT, "<="),
        "spread_proxy_p95": _pass(metrics["spread_proxy_p95"], SPREAD_P95_LIMIT, "<="),
        "net_directional_return_25k_median": _pass(metrics["net_directional_return_25k_median"], 0.0, ">"),
    }
    metrics["overall_status"] = "pass" if set(statuses.values()) == {"pass"} else "fail"
    return metrics, _metric_rows(candidate_name, metrics, statuses)


def _effective_adv_dollars(active: pd.DataFrame) -> pd.Series:
    adv = _numeric_column(active, "adv20")
    dollar_volume = _numeric_column(active, "dollar_volume")
    close = _numeric_column(active, "adjusted_close")
    volume = _numeric_column(active, "volume")
    fallback_dollar_volume = volume * close
    effective = dollar_volume.where(dollar_volume.gt(0), adv)
    effective = effective.where(effective.gt(0), fallback_dollar_volume)
    effective = effective.where(effective.gt(0), pd.NA)
    return effective.astype("Float64")


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _metric_rows(
    candidate_name: str,
    metrics: dict[str, object],
    statuses: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    thresholds = {
        "participation_25k_p95": PARTICIPATION_25K_P95_LIMIT,
        "participation_100k_p95": PARTICIPATION_100K_P95_LIMIT,
        "spread_proxy_p95": SPREAD_P95_LIMIT,
        "net_directional_return_25k_median": 0.0,
        "gross_directional_return_median": "",
    }
    comparisons = {
        "participation_25k_p95": "<=",
        "participation_100k_p95": "<=",
        "spread_proxy_p95": "<=",
        "net_directional_return_25k_median": ">",
        "gross_directional_return_median": "diagnostic",
    }
    rows: list[dict[str, object]] = []
    for metric in [
        "gross_directional_return_median",
        "participation_25k_p95",
        "participation_100k_p95",
        "spread_proxy_p95",
        "net_directional_return_25k_median",
    ]:
        status = "pass" if metric == "gross_directional_return_median" else (statuses or {}).get(metric, "fail")
        rows.append(
            {
                "schema_version": "small_emotion_q2_cost_capacity_report.v1",
                "stage": STAGE,
                "candidate_name": candidate_name,
                "metric": metric,
                "value": metrics.get(metric),
                "threshold": thresholds[metric],
                "comparison": comparisons[metric],
                "status": status,
                "no_view_not_zero_alpha": True,
            }
        )
    return rows


def _required_input_status(active: pd.DataFrame) -> str:
    required = ["date", "symbol", "expected_return", "adjusted_close"]
    if active.empty:
        return "fail"
    for column in required:
        if column not in active.columns or active[column].isna().any():
            return "fail"
    effective_adv = _effective_adv_dollars(active)
    if effective_adv.isna().any():
        return "fail"
    return "pass"


def _optimizer_probe(active: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if active.empty:
        return pd.DataFrame(columns=_optimizer_probe_columns())
    effective_adv = _effective_adv_dollars(active)
    close = pd.to_numeric(active.get("adjusted_close"), errors="coerce")
    adv_shares = effective_adv / close
    for idx, row in active.reset_index(drop=True).iterrows():
        ready = (
            pd.notna(row.get("date"))
            and str(row.get("symbol", "")).strip() != ""
            and pd.notna(row.get("expected_return"))
            and pd.notna(close.iloc[idx])
            and pd.notna(adv_shares.iloc[idx])
        )
        rows.append(
            {
                "schema_version": "small_emotion_q2_optimizer_input_probe.v1",
                "stage": STAGE,
                "candidate_name": row.get("candidate_name", ""),
                "measurement_spec_id": row.get("measurement_spec_id", ""),
                "measurement_spec_hash": row.get("measurement_spec_hash", ""),
                "date": row.get("date", ""),
                "ticker": row.get("symbol", ""),
                "asset_id": row.get("asset_id", ""),
                "event_id": row.get("event_id", ""),
                "expected_return": row.get("expected_return", ""),
                "expected_return_source": "small_emotion_q2_candidate_intake",
                "signal_value": row.get("signal_value", ""),
                "close": close.iloc[idx],
                "adv_dollars": effective_adv.iloc[idx],
                "adv_shares": adv_shares.iloc[idx],
                "bid_ask_spread": row.get("bid_ask_spread", ""),
                "optimizer_input_probe_status": "staged_optimizer_input_ready" if ready else "optimizer_input_unavailable",
                "actual_optimizer_run": False,
                "no_view_not_zero_alpha": True,
            }
        )
    return pd.DataFrame(rows, columns=_optimizer_probe_columns())


def _holding_path(active: pd.DataFrame, candidate_name: str, notional_usd: float) -> pd.DataFrame:
    if active.empty:
        return pd.DataFrame(columns=_holding_path_columns())
    spans: list[dict[str, object]] = []
    for row in active.to_dict("records"):
        start = pd.to_datetime(row.get("date"), errors="coerce")
        if pd.isna(start):
            continue
        holding_days = _primary_window_days(str(row.get("primary_window", "")))
        entry = start + pd.offsets.BDay(1)
        exit_date = entry + pd.offsets.BDay(holding_days)
        spans.append({"entry": entry.normalize(), "exit": exit_date.normalize()})
    if not spans:
        return pd.DataFrame(columns=_holding_path_columns())

    start_date = min(span["entry"] for span in spans)
    end_date = max(span["exit"] for span in spans)
    rows: list[dict[str, object]] = []
    for date in pd.bdate_range(start_date, end_date):
        active_count = sum(1 for span in spans if span["entry"] <= date < span["exit"])
        entries = sum(1 for span in spans if span["entry"] == date)
        exits = sum(1 for span in spans if span["exit"] == date)
        rows.append(
            {
                "schema_version": "small_emotion_q2_holding_path.v1",
                "stage": STAGE,
                "candidate_name": candidate_name,
                "date": date.date().isoformat(),
                "active_positions": active_count,
                "entries": entries,
                "exits": exits,
                "gross_open_notional": active_count * notional_usd,
                "turnover_notional": (entries + exits) * notional_usd,
                "turnover_event_count": entries + exits,
                "actual_portfolio_construction_run": False,
                "no_view_not_zero_alpha": True,
            }
        )
    return pd.DataFrame(rows, columns=_holding_path_columns())


def _primary_window_days(primary_window: str) -> int:
    match = re.search(r"post_\d+_(\d+)", primary_window)
    if match:
        return max(1, int(match.group(1)))
    return 22


def _survival_decision(costs: dict[str, object], optimizer_probe_status: str) -> str:
    if optimizer_probe_status != "staged_optimizer_input_ready":
        return "optimizer_input_unavailable"
    if costs.get("overall_status") != "pass":
        return "cost_capacity_failed"
    return "execution_survival_passed"


def _summary(survival_matrix: pd.DataFrame, expected: pd.DataFrame) -> dict[str, object]:
    passed = int(survival_matrix["survival_decision"].eq("execution_survival_passed").sum()) if not survival_matrix.empty else 0
    cost_failed = int(survival_matrix["survival_decision"].eq("cost_capacity_failed").sum()) if not survival_matrix.empty else 0
    optimizer_unavailable = (
        int(survival_matrix["survival_decision"].eq("optimizer_input_unavailable").sum()) if not survival_matrix.empty else 0
    )
    return {
        "schema_version": "small_emotion_q2_execution_survival_summary.v1",
        "stage": STAGE,
        "candidate_count": int(len(survival_matrix)),
        "survival_passed_count": passed,
        "cost_capacity_failed_count": cost_failed,
        "optimizer_input_unavailable_count": optimizer_unavailable,
        "expected_return_panel_row_count": int(len(expected)),
        "optimizer_input_probe_written": True,
        "actual_optimizer_run": False,
        "q2_execution_survival_ran": True,
        "optimizer_entry_allowed": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "survival_matrix": output_path / "small_emotion_q2_execution_survival_matrix.csv",
        "cost_capacity_report": output_path / "small_emotion_q2_cost_capacity_report.csv",
        "holding_path": output_path / "small_emotion_q2_holding_path.csv",
        "optimizer_input_probe": output_path / "small_emotion_q2_optimizer_input_probe.csv",
        "summary": output_path / "small_emotion_q2_survival_summary.json",
        "report": output_path / "small_emotion_q2_survival_report.md",
    }


def _survival_matrix_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "primary_window",
        "active_expected_return_rows",
        "no_view_rows_excluded",
        "gross_directional_return_median",
        "participation_25k_p95",
        "participation_100k_p95",
        "spread_proxy_p95",
        "net_directional_return_25k_median",
        "cost_capacity_status",
        "optimizer_input_probe_status",
        "survival_decision",
        "actual_optimizer_run",
        "portfolio_construction_allowed",
        "no_view_not_zero_alpha",
    ]


def _cost_capacity_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "metric",
        "value",
        "threshold",
        "comparison",
        "status",
        "no_view_not_zero_alpha",
    ]


def _holding_path_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "date",
        "active_positions",
        "entries",
        "exits",
        "gross_open_notional",
        "turnover_notional",
        "turnover_event_count",
        "actual_portfolio_construction_run",
        "no_view_not_zero_alpha",
    ]


def _optimizer_probe_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "date",
        "ticker",
        "asset_id",
        "event_id",
        "expected_return",
        "expected_return_source",
        "signal_value",
        "close",
        "adv_dollars",
        "adv_shares",
        "bid_ask_spread",
        "optimizer_input_probe_status",
        "actual_optimizer_run",
        "no_view_not_zero_alpha",
    ]


def _report(summary: dict[str, object], survival_matrix: pd.DataFrame) -> str:
    lines = [
        "# Q2-SMALL-EMOTION-02 Execution-Survival",
        "",
        "This is a Q2 execution-survival diagnostic only. It checks cost pressure, capacity, participation, holding paths, and optimizer input staging. It does not run optimizer, portfolio construction, Alpha Registry, paper, live, broker, order, or production workflows.",
        "",
        f"- candidate_count: `{summary['candidate_count']}`",
        f"- survival_passed_count: `{summary['survival_passed_count']}`",
        f"- cost_capacity_failed_count: `{summary['cost_capacity_failed_count']}`",
        f"- optimizer_input_unavailable_count: `{summary['optimizer_input_unavailable_count']}`",
        "",
        "## Candidate Matrix",
        "",
        "| candidate | decision | 25k participation p95 | 100k participation p95 | net directional return median | optimizer probe |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in survival_matrix.to_dict("records"):
        lines.append(
            "| {candidate} | {decision} | {p25} | {p100} | {net} | {probe} |".format(
                candidate=row.get("candidate_name", ""),
                decision=row.get("survival_decision", ""),
                p25=_fmt(row.get("participation_25k_p95")),
                p100=_fmt(row.get("participation_100k_p95")),
                net=_fmt(row.get("net_directional_return_25k_median")),
                probe=row.get("optimizer_input_probe_status", ""),
            )
        )
    lines.extend(
        [
            "",
            "No-view rows are excluded from execution-survival diagnostics and are not encoded as zero alpha.",
        ]
    )
    return "\n".join(lines) + "\n"


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _median(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    return float(clean.median()) if not clean.empty else math.nan


def _quantile(values: pd.Series, q: float) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    return float(clean.quantile(q)) if not clean.empty else math.nan


def _pass(value: object, threshold: float, comparison: str) -> str:
    try:
        observed = float(value)
    except (TypeError, ValueError):
        return "fail"
    if math.isnan(observed):
        return "fail"
    if comparison == "<=":
        return "pass" if observed <= threshold else "fail"
    if comparison == ">":
        return "pass" if observed > threshold else "fail"
    return "fail"


def _mode(values: pd.Series | None) -> str:
    if values is None:
        return ""
    clean = values.dropna().astype(str)
    return str(clean.mode().iloc[0]) if not clean.empty else ""


def _fmt(value: object) -> str:
    try:
        observed = float(value)
    except (TypeError, ValueError):
        return ""
    return "" if math.isnan(observed) else f"{observed:.6f}"
