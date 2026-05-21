"""Small-emotion Q2 candidate intake.

This module opens a Q2 candidate intake only for candidates that already passed
Q1 and Promotion Gate. It builds a diagnostic expected-return panel for Q2
execution-survival work, but it does not run PortfolioOS optimizers, portfolio
construction, broker/order, live, paper, or production workflows.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml


STAGE = "Q2-SMALL-EMOTION-01"
EXPECTED_RETURN_CAP = 0.30


@dataclass(frozen=True)
class SmallEmotionQ2CandidateInput:
    """Input bundle for one promoted small-emotion candidate."""

    candidate_name: str
    measurement_spec_path: str | Path
    q1_output_dir: str | Path
    promotion_gate_dir: str | Path
    required_measurement_spec_hash: str


@dataclass(frozen=True)
class SmallEmotionQ2CandidateIntakeResult:
    """Written Q2 intake artifacts and summary."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_q2_candidate_intake(
    *,
    candidates: Iterable[SmallEmotionQ2CandidateInput],
    output_dir: str | Path,
) -> SmallEmotionQ2CandidateIntakeResult:
    """Open Q2 candidate intake for promoted small-emotion candidates."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    candidate_rows: list[dict[str, object]] = []
    panel_rows: list[dict[str, object]] = []
    risk_rows: list[dict[str, object]] = []
    contract_candidates: list[dict[str, object]] = []

    for candidate in candidates:
        resolved = _resolve_candidate(candidate)
        candidate_rows.append(resolved["candidate_row"])
        risk_rows.extend(resolved["risk_rows"])
        panel_rows.extend(resolved["expected_return_rows"])
        if resolved["candidate_row"]["q2_status"] == "opened_q2_candidate":
            contract_candidates.append(resolved["contract_candidate"])

    candidate_matrix = pd.DataFrame(candidate_rows, columns=_candidate_columns())
    expected_return_panel = pd.DataFrame(panel_rows, columns=_expected_return_columns())
    risk_gate = pd.DataFrame(risk_rows, columns=_risk_columns())
    summary = _build_summary(candidate_matrix, expected_return_panel)
    contract = _build_contract(summary, contract_candidates)

    candidate_matrix.to_csv(artifacts["candidate_matrix"], index=False)
    expected_return_panel.to_csv(artifacts["expected_return_panel"], index=False)
    risk_gate.to_csv(artifacts["execution_risk_gate"], index=False)
    artifacts["q2_input_contract"].write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["summary"].write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["report"].write_text(_report(summary, candidate_matrix), encoding="utf-8")

    return SmallEmotionQ2CandidateIntakeResult(summary=summary, artifacts=artifacts)


def _resolve_candidate(candidate: SmallEmotionQ2CandidateInput) -> dict[str, object]:
    spec_path = Path(candidate.measurement_spec_path)
    q1_dir = Path(candidate.q1_output_dir)
    pg_dir = Path(candidate.promotion_gate_dir)
    spec_hash = _file_hash(spec_path)
    if spec_hash != candidate.required_measurement_spec_hash:
        raise ValueError(
            "MeasurementSpec hash mismatch for "
            f"{candidate.candidate_name}: expected {candidate.required_measurement_spec_hash}, observed {spec_hash}"
        )

    spec = yaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}
    q1 = _read_json(q1_dir / "q1_decision_summary.json")
    pg = _read_json(pg_dir / "pg_decision_summary.json")
    events = _read_csv(q1_dir / "q1_event_panel.csv")

    measurement_spec_id = str(spec.get("measurement_spec_id") or pg.get("measurement_spec_id") or q1.get("measurement_spec_id"))
    primary_window = str(spec.get("label_contract", {}).get("primary_window") or pg.get("primary_window") or "unknown")
    promotion_decision = str(pg.get("promotion_decision"))
    q1_decision = str(q1.get("q1_decision"))
    mean_directional = _float_or_zero(pg.get("mean_primary_directional_return") or q1.get("mean_primary_directional_return"))
    expected_abs = min(abs(mean_directional), EXPECTED_RETURN_CAP)
    expected_return_capped = bool(abs(mean_directional) > EXPECTED_RETURN_CAP)

    block_reasons = _block_reasons(pg=pg, q1=q1)
    q2_status = "opened_q2_candidate" if not block_reasons else "blocked_before_q2"
    active = _active_events(events) if q2_status == "opened_q2_candidate" else pd.DataFrame()
    expected_rows = _expected_return_rows(
        active,
        candidate_name=candidate.candidate_name,
        measurement_spec_id=measurement_spec_id,
        measurement_spec_hash=spec_hash,
        primary_window=primary_window,
        expected_abs=expected_abs,
        expected_return_capped=expected_return_capped,
    )
    risk_rows = _risk_rows(candidate_name=candidate.candidate_name, measurement_spec_id=measurement_spec_id, pg=pg)
    candidate_row = {
        "schema_version": "small_emotion_q2_candidate_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate.candidate_name,
        "measurement_spec_id": measurement_spec_id,
        "measurement_spec_hash": spec_hash,
        "primary_window": primary_window,
        "q1_decision": q1_decision,
        "promotion_decision": promotion_decision,
        "q2_status": q2_status,
        "block_reason": ";".join(block_reasons),
        "active_event_count": int(q1.get("active_event_count") or len(active)),
        "q2_expected_return_rows": int(len(expected_rows)),
        "observed_primary_label_count": int(pg.get("observed_primary_label_count") or q1.get("observed_primary_label_count") or 0),
        "mean_primary_directional_return": mean_directional,
        "oos_test_mean_directional_return": _float_or_zero(
            pg.get("oos_test_mean_directional_return") or q1.get("oos_test_mean_directional_return")
        ),
        "expected_return_abs_calibration": expected_abs,
        "expected_return_cap": EXPECTED_RETURN_CAP,
        "expected_return_capped": expected_return_capped,
        "search_burden_status": pg.get("search_burden_status", ""),
        "tail_status": pg.get("tail_status", ""),
        "anomaly_status": pg.get("anomaly_status", ""),
        "cost_liquidity_status": pg.get("cost_liquidity_status", ""),
        "time_breadth_status": pg.get("time_breadth_status", ""),
        "q2_candidate_intake_opened": q2_status == "opened_q2_candidate",
        "optimizer_entry_allowed": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }
    return {
        "candidate_row": candidate_row,
        "risk_rows": risk_rows,
        "expected_return_rows": expected_rows,
        "contract_candidate": {
            "candidate_name": candidate.candidate_name,
            "measurement_spec_id": measurement_spec_id,
            "measurement_spec_hash": spec_hash,
            "primary_window": primary_window,
            "expected_return_rows": len(expected_rows),
        },
    }


def _block_reasons(*, pg: dict[str, object], q1: dict[str, object]) -> list[str]:
    reasons: list[str] = []
    if q1.get("q1_decision") != "passed_q1_research_review":
        reasons.append("q1_decision_not_passed")
    if pg.get("promotion_decision") != "promote_to_q2_candidate":
        reasons.append("promotion_decision_not_promoted")
    if pg.get("promotion_gate_allowed") is not True:
        reasons.append("promotion_gate_not_allowed")
    for status_key in ["anomaly_status", "cost_liquidity_status", "time_breadth_status", "tail_status"]:
        if pg.get(status_key) not in {"pass", None, ""}:
            reasons.append(f"{status_key}_{pg.get(status_key)}")
    return reasons


def _active_events(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    signal_state = events.get("signal_state", pd.Series(dtype=str)).astype(str).str.lower()
    active = events[signal_state.eq("active")].copy()
    if "signal_value" not in active.columns:
        active["signal_value"] = -1.0
    active["signal_value"] = pd.to_numeric(active["signal_value"], errors="coerce")
    active = active[active["signal_value"].notna()]
    if "ticker" in active.columns:
        active = active[active["ticker"].astype(str).str.strip().ne("")]
    return active


def _expected_return_rows(
    events: pd.DataFrame,
    *,
    candidate_name: str,
    measurement_spec_id: str,
    measurement_spec_hash: str,
    primary_window: str,
    expected_abs: float,
    expected_return_capped: bool,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in events.to_dict("records"):
        signal_value = float(row.get("signal_value") or -1.0)
        expected_return = signal_value * expected_abs
        rows.append(
            {
                "schema_version": "small_emotion_q2_expected_return_panel.v1",
                "stage": STAGE,
                "candidate_name": candidate_name,
                "measurement_spec_id": measurement_spec_id,
                "measurement_spec_hash": measurement_spec_hash,
                "date": str(row.get("date", "")),
                "symbol": str(row.get("ticker", "")).upper(),
                "asset_id": str(row.get("asset_id", "")),
                "event_id": str(row.get("event_id", "")),
                "primary_window": primary_window,
                "signal_state": "active",
                "signal_value": signal_value,
                "expected_return": expected_return,
                "expected_return_calibration": "q1_pg_mean_directional_return_capped_for_q2_diagnostic",
                "expected_return_capped": expected_return_capped,
                "adv20": row.get("adv20", ""),
                "bid_ask_spread": row.get("bid_ask_spread", ""),
                "adjusted_close": row.get("adjusted_close", ""),
                "volume": row.get("volume", ""),
                "market_cap": row.get("market_cap", ""),
                "dollar_volume": row.get("dollar_volume", ""),
                "industry": row.get("industry", ""),
                "sector": row.get("sector", ""),
                "liquidity_bucket": row.get("liquidity_bucket", ""),
                "spread_bucket": row.get("spread_bucket", ""),
                "active_alpha_views": measurement_spec_id,
                "q2_status": "opened_q2_candidate",
                "no_view_not_zero_alpha": True,
            }
        )
    return rows


def _risk_rows(*, candidate_name: str, measurement_spec_id: str, pg: dict[str, object]) -> list[dict[str, object]]:
    return [
        {
            "schema_version": "small_emotion_q2_execution_risk_gate.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "measurement_spec_id": measurement_spec_id,
            "gate": "search_burden",
            "status": pg.get("search_burden_status", "unknown"),
            "q2_blocking": False,
            "no_view_not_zero_alpha": True,
        },
        {
            "schema_version": "small_emotion_q2_execution_risk_gate.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "measurement_spec_id": measurement_spec_id,
            "gate": "tail_concentration",
            "status": pg.get("tail_status", "unknown"),
            "q2_blocking": pg.get("tail_status") not in {"pass", None, ""},
            "no_view_not_zero_alpha": True,
        },
        {
            "schema_version": "small_emotion_q2_execution_risk_gate.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "measurement_spec_id": measurement_spec_id,
            "gate": "data_anomaly",
            "status": pg.get("anomaly_status", "unknown"),
            "q2_blocking": pg.get("anomaly_status") not in {"pass", None, ""},
            "no_view_not_zero_alpha": True,
        },
        {
            "schema_version": "small_emotion_q2_execution_risk_gate.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "measurement_spec_id": measurement_spec_id,
            "gate": "cost_liquidity",
            "status": pg.get("cost_liquidity_status", "unknown"),
            "q2_blocking": pg.get("cost_liquidity_status") not in {"pass", None, ""},
            "no_view_not_zero_alpha": True,
        },
        {
            "schema_version": "small_emotion_q2_execution_risk_gate.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "measurement_spec_id": measurement_spec_id,
            "gate": "time_breadth",
            "status": pg.get("time_breadth_status", "unknown"),
            "q2_blocking": pg.get("time_breadth_status") not in {"pass", None, ""},
            "no_view_not_zero_alpha": True,
        },
    ]


def _build_summary(candidate_matrix: pd.DataFrame, expected_return_panel: pd.DataFrame) -> dict[str, object]:
    opened = int(candidate_matrix["q2_status"].eq("opened_q2_candidate").sum()) if not candidate_matrix.empty else 0
    blocked = int(candidate_matrix["q2_status"].eq("blocked_before_q2").sum()) if not candidate_matrix.empty else 0
    return {
        "schema_version": "small_emotion_q2_candidate_intake_summary.v1",
        "stage": STAGE,
        "candidate_count": int(len(candidate_matrix)),
        "opened_q2_candidate_count": opened,
        "blocked_candidate_count": blocked,
        "expected_return_panel_row_count": int(len(expected_return_panel)),
        "q2_candidate_intake_opened": opened > 0,
        "q2_entry_allowed": opened > 0,
        "optimizer_entry_allowed": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _build_contract(summary: dict[str, object], candidates: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_input_contract.v1",
        "stage": STAGE,
        "input_type": "promoted_small_emotion_expected_return_panel",
        "allowed_consumer": "projects/execution_aware_optimizer",
        "direct_optimizer_execution_allowed": False,
        "portfolio_construction_allowed": False,
        "broker_order_live_production_allowed": False,
        "candidate_count": summary["candidate_count"],
        "opened_q2_candidate_count": summary["opened_q2_candidate_count"],
        "expected_return_panel_artifact": "small_emotion_q2_expected_return_panel.csv",
        "candidates": candidates,
        "no_view_not_zero_alpha": True,
    }


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "candidate_matrix": output_path / "small_emotion_q2_candidate_matrix.csv",
        "expected_return_panel": output_path / "small_emotion_q2_expected_return_panel.csv",
        "execution_risk_gate": output_path / "small_emotion_q2_execution_risk_gate.csv",
        "q2_input_contract": output_path / "small_emotion_q2_input_contract.json",
        "summary": output_path / "small_emotion_q2_candidate_summary.json",
        "report": output_path / "small_emotion_q2_candidate_report.md",
    }


def _candidate_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "primary_window",
        "q1_decision",
        "promotion_decision",
        "q2_status",
        "block_reason",
        "active_event_count",
        "q2_expected_return_rows",
        "observed_primary_label_count",
        "mean_primary_directional_return",
        "oos_test_mean_directional_return",
        "expected_return_abs_calibration",
        "expected_return_cap",
        "expected_return_capped",
        "search_burden_status",
        "tail_status",
        "anomaly_status",
        "cost_liquidity_status",
        "time_breadth_status",
        "q2_candidate_intake_opened",
        "optimizer_entry_allowed",
        "portfolio_construction_allowed",
        "alpha_registry_update_allowed",
        "paper_ready",
        "live_ready",
        "broker_order_path_opened",
        "production_approval_claimed",
        "no_view_not_zero_alpha",
    ]


def _expected_return_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "date",
        "symbol",
        "asset_id",
        "event_id",
        "primary_window",
        "signal_state",
        "signal_value",
        "expected_return",
        "expected_return_calibration",
        "expected_return_capped",
        "adv20",
        "bid_ask_spread",
        "adjusted_close",
        "volume",
        "market_cap",
        "dollar_volume",
        "industry",
        "sector",
        "liquidity_bucket",
        "spread_bucket",
        "active_alpha_views",
        "q2_status",
        "no_view_not_zero_alpha",
    ]


def _risk_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "gate",
        "status",
        "q2_blocking",
        "no_view_not_zero_alpha",
    ]


def _report(summary: dict[str, object], candidate_matrix: pd.DataFrame) -> str:
    lines = [
        "# Q2-SMALL-EMOTION-01 Candidate Intake",
        "",
        "This is a Q2 candidate intake only review. It builds diagnostic expected-return input for execution-survival work and does not run optimizer, portfolio construction, Alpha Registry, paper, live, broker, order, or production workflows.",
        "",
        f"- candidate_count: `{summary['candidate_count']}`",
        f"- opened_q2_candidate_count: `{summary['opened_q2_candidate_count']}`",
        f"- blocked_candidate_count: `{summary['blocked_candidate_count']}`",
        f"- expected_return_panel_row_count: `{summary['expected_return_panel_row_count']}`",
        "",
        "## Candidates",
        "",
        "| candidate | status | primary_window | expected-return rows | block_reason |",
        "|---|---|---|---:|---|",
    ]
    for row in candidate_matrix.to_dict("records"):
        lines.append(
            "| {candidate} | {status} | {window} | {rows} | {reason} |".format(
                candidate=row.get("candidate_name", ""),
                status=row.get("q2_status", ""),
                window=row.get("primary_window", ""),
                rows=row.get("q2_expected_return_rows", 0),
                reason=row.get("block_reason", ""),
            )
        )
    return "\n".join(lines) + "\n"


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _float_or_zero(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
