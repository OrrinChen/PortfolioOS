"""Small-emotion Q2 execution-survival closeout.

This module closes the local Q2 chain for promoted small-emotion candidates by
verifying candidate intake, execution-survival diagnostics, and optimizer
adapter response artifacts. It does not build portfolios, write orders, update
Alpha Registry, open paper/live/broker paths, or claim production approval.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path

import pandas as pd

from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file


STAGE = "Q2-SMALL-EMOTION-04"
PASS_DECISION = "completed_q2_execution_survival"


@dataclass(frozen=True)
class SmallEmotionQ2CompleteResult:
    """Written Q2 closeout artifacts and summary."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_q2_complete(
    *,
    q2_intake_dir: str | Path,
    q2_survival_dir: str | Path,
    optimizer_dry_run_dir: str | Path,
    output_dir: str | Path,
) -> SmallEmotionQ2CompleteResult:
    """Build the Q2 closeout matrix for small-emotion candidates."""

    intake_path = Path(q2_intake_dir)
    survival_path = Path(q2_survival_dir)
    optimizer_path = Path(optimizer_dry_run_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    candidate_matrix = _read_csv(intake_path / "small_emotion_q2_candidate_matrix.csv")
    expected_panel = _read_csv(intake_path / "small_emotion_q2_expected_return_panel.csv")
    survival_matrix = _read_csv(survival_path / "small_emotion_q2_execution_survival_matrix.csv")
    optimizer_response = _read_csv(optimizer_path / "small_emotion_q2_optimizer_response_matrix.csv")
    constraint_response = _read_csv(optimizer_path / "small_emotion_q2_optimizer_constraint_response.csv")

    rows = [
        _candidate_closeout_row(
            candidate_name=candidate_name,
            candidate_matrix=candidate_matrix,
            expected_panel=expected_panel,
            survival_matrix=survival_matrix,
            optimizer_response=optimizer_response,
            constraint_response=constraint_response,
        )
        for candidate_name in _candidate_order(candidate_matrix, survival_matrix, optimizer_response)
    ]
    closeout_matrix = pd.DataFrame(rows, columns=_closeout_columns())
    summary = _summary(
        closeout_matrix,
        q2_intake_dir=intake_path,
        q2_survival_dir=survival_path,
        optimizer_dry_run_dir=optimizer_path,
    )
    manifest = _manifest(
        summary,
        artifacts,
        q2_intake_dir=intake_path,
        q2_survival_dir=survival_path,
        optimizer_dry_run_dir=optimizer_path,
    )

    closeout_matrix.to_csv(artifacts["complete_matrix"], index=False)
    artifacts["summary"].write_text(canonical_json(summary) + "\n", encoding="utf-8")
    artifacts["manifest"].write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    artifacts["report"].write_text(_report(summary, closeout_matrix), encoding="utf-8")

    return SmallEmotionQ2CompleteResult(summary=summary, artifacts=artifacts)


def _candidate_closeout_row(
    *,
    candidate_name: str,
    candidate_matrix: pd.DataFrame,
    expected_panel: pd.DataFrame,
    survival_matrix: pd.DataFrame,
    optimizer_response: pd.DataFrame,
    constraint_response: pd.DataFrame,
) -> dict[str, object]:
    candidate = _first_row(candidate_matrix, candidate_name)
    survival = _first_row(survival_matrix, candidate_name)
    response_rows = _candidate_rows(optimizer_response, candidate_name)
    constraint_rows = _candidate_rows(constraint_response, candidate_name)
    expected_rows = _candidate_rows(expected_panel, candidate_name)

    panel_count = int(response_rows.loc[response_rows.get("optimizer_dry_run_status", "").astype(str).eq("observed"), "panel_name"].nunique()) if not response_rows.empty and "optimizer_dry_run_status" in response_rows.columns else 0
    live_net = _panel_net_change(response_rows, "live_panel")
    flipped_net = _panel_net_change(response_rows, "sign_flipped_panel")
    zero_net = _panel_net_change(response_rows, "zero_alpha_panel")
    sign_status = _signal_sign_response_status(live_net, flipped_net, zero_net)
    constraint_fail_count = _constraint_fail_count(constraint_rows)
    optimizer_status = _optimizer_status(response_rows, constraint_fail_count, sign_status)
    decision = _q2_complete_decision(
        candidate=candidate,
        survival=survival,
        optimizer_status=optimizer_status,
        panel_count=panel_count,
        constraint_fail_count=constraint_fail_count,
        sign_status=sign_status,
    )

    return {
        "schema_version": "small_emotion_q2_complete_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "measurement_spec_id": str(survival.get("measurement_spec_id") or candidate.get("measurement_spec_id") or ""),
        "measurement_spec_hash": str(survival.get("measurement_spec_hash") or candidate.get("measurement_spec_hash") or ""),
        "q2_intake_status": _candidate_intake_status(candidate),
        "active_expected_return_rows": _active_expected_return_rows(expected_rows),
        "survival_decision": str(survival.get("survival_decision", "missing_survival_row")),
        "cost_capacity_status": str(survival.get("cost_capacity_status", "")),
        "optimizer_input_probe_status": str(survival.get("optimizer_input_probe_status", "")),
        "optimizer_panel_count": panel_count,
        "optimizer_observed_panels": ",".join(_observed_panels(response_rows)),
        "optimizer_status": optimizer_status,
        "constraint_fail_count": constraint_fail_count,
        "signal_sign_response_status": sign_status,
        "live_panel_net_weight_change": live_net,
        "sign_flipped_panel_net_weight_change": flipped_net,
        "zero_alpha_panel_net_weight_change": zero_net,
        "q2_complete_decision": decision,
        "orders_written": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _candidate_order(*frames: pd.DataFrame) -> list[str]:
    names: list[str] = []
    for frame in frames:
        if not frame.empty and "candidate_name" in frame.columns:
            names.extend(str(name) for name in frame["candidate_name"].dropna().tolist())
    ordered: list[str] = []
    seen: set[str] = set()
    for name in names:
        if name and name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def _first_row(frame: pd.DataFrame, candidate_name: str) -> dict[str, object]:
    rows = _candidate_rows(frame, candidate_name)
    return rows.iloc[0].to_dict() if not rows.empty else {}


def _candidate_rows(frame: pd.DataFrame, candidate_name: str) -> pd.DataFrame:
    if frame.empty or "candidate_name" not in frame.columns:
        return pd.DataFrame()
    return frame[frame["candidate_name"].astype(str).eq(candidate_name)].copy()


def _candidate_intake_status(candidate: dict[str, object]) -> str:
    for key in ["candidate_intake_status", "q2_status", "status"]:
        value = candidate.get(key)
        if value not in (None, ""):
            return str(value)
    q2_entry_allowed = candidate.get("q2_entry_allowed")
    if str(q2_entry_allowed).lower() == "true" or q2_entry_allowed is True:
        return "opened_q2_candidate"
    return "missing_candidate_intake"


def _active_expected_return_rows(expected_rows: pd.DataFrame) -> int:
    if expected_rows.empty:
        return 0
    if "signal_state" in expected_rows.columns:
        expected_rows = expected_rows[expected_rows["signal_state"].astype(str).str.lower().eq("active")]
    if "expected_return" not in expected_rows.columns:
        return 0
    return int(pd.to_numeric(expected_rows["expected_return"], errors="coerce").notna().sum())


def _observed_panels(response_rows: pd.DataFrame) -> list[str]:
    if response_rows.empty or "panel_name" not in response_rows.columns:
        return []
    rows = response_rows
    if "optimizer_dry_run_status" in rows.columns:
        rows = rows[rows["optimizer_dry_run_status"].astype(str).eq("observed")]
    return sorted(str(panel) for panel in rows["panel_name"].dropna().unique().tolist())


def _panel_net_change(response_rows: pd.DataFrame, panel_name: str) -> float:
    if response_rows.empty or "panel_name" not in response_rows.columns or "net_weight_change" not in response_rows.columns:
        return math.nan
    rows = response_rows[response_rows["panel_name"].astype(str).eq(panel_name)]
    if rows.empty:
        return math.nan
    return _safe_float(rows.iloc[0].get("net_weight_change"))


def _constraint_fail_count(constraint_rows: pd.DataFrame) -> int:
    if constraint_rows.empty or "status" not in constraint_rows.columns:
        return 1
    return int((constraint_rows["status"].astype(str) != "pass").sum())


def _signal_sign_response_status(live_net: float, flipped_net: float, zero_net: float) -> str:
    values = [live_net, flipped_net, zero_net]
    if any(not math.isfinite(value) for value in values):
        return "missing_panel_response"
    if live_net < zero_net and flipped_net > zero_net:
        return "pass"
    return "fail"


def _optimizer_status(response_rows: pd.DataFrame, constraint_fail_count: int, sign_status: str) -> str:
    if response_rows.empty:
        return "missing_optimizer_response"
    observed = response_rows
    if "optimizer_dry_run_status" in observed.columns:
        observed = observed[observed["optimizer_dry_run_status"].astype(str).eq("observed")]
    if observed.empty:
        return "optimizer_not_observed"
    statuses = set(observed.get("optimizer_status", pd.Series(dtype=str)).astype(str).tolist())
    if not statuses <= {"optimal", "optimal_inaccurate"}:
        return "optimizer_status_failed"
    if constraint_fail_count > 0:
        return "optimizer_constraints_failed"
    if sign_status != "pass":
        return "optimizer_signal_response_failed"
    return "optimizer_response_passed"


def _q2_complete_decision(
    *,
    candidate: dict[str, object],
    survival: dict[str, object],
    optimizer_status: str,
    panel_count: int,
    constraint_fail_count: int,
    sign_status: str,
) -> str:
    if _candidate_intake_status(candidate) != "opened_q2_candidate":
        return "failed_candidate_intake"
    if str(survival.get("survival_decision")) != "execution_survival_passed":
        return "failed_execution_survival"
    if panel_count < 3:
        return "failed_optimizer_response_missing_panels"
    if constraint_fail_count > 0:
        return "failed_optimizer_constraints"
    if sign_status != "pass":
        return "failed_optimizer_signal_response"
    if optimizer_status != "optimizer_response_passed":
        return "failed_optimizer_response"
    return PASS_DECISION


def _summary(
    matrix: pd.DataFrame,
    *,
    q2_intake_dir: Path,
    q2_survival_dir: Path,
    optimizer_dry_run_dir: Path,
) -> dict[str, object]:
    decisions = matrix["q2_complete_decision"].astype(str) if "q2_complete_decision" in matrix.columns else pd.Series(dtype=str)
    return {
        "schema_version": "small_emotion_q2_complete_summary.v1",
        "stage": STAGE,
        "candidate_count": int(len(matrix)),
        "q2_complete_passed_count": int(decisions.eq(PASS_DECISION).sum()),
        "q2_complete_failed_count": int((decisions != PASS_DECISION).sum()),
        "q2_intake_dir": str(q2_intake_dir),
        "q2_survival_dir": str(q2_survival_dir),
        "optimizer_dry_run_dir": str(optimizer_dry_run_dir),
        "orders_written": False,
        "portfolio_construction_allowed": False,
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
    q2_intake_dir: Path,
    q2_survival_dir: Path,
    optimizer_dry_run_dir: Path,
) -> dict[str, object]:
    payload = {
        "schema_version": "small_emotion_q2_complete_manifest.v1",
        "stage": STAGE,
        "summary": summary,
        "input_artifact_hashes": {
            "candidate_matrix": _hash_if_exists(q2_intake_dir / "small_emotion_q2_candidate_matrix.csv"),
            "expected_return_panel": _hash_if_exists(q2_intake_dir / "small_emotion_q2_expected_return_panel.csv"),
            "survival_matrix": _hash_if_exists(q2_survival_dir / "small_emotion_q2_execution_survival_matrix.csv"),
            "optimizer_response_matrix": _hash_if_exists(optimizer_dry_run_dir / "small_emotion_q2_optimizer_response_matrix.csv"),
            "optimizer_constraint_response": _hash_if_exists(optimizer_dry_run_dir / "small_emotion_q2_optimizer_constraint_response.csv"),
        },
        "output_artifacts": {key: str(path) for key, path in artifacts.items()},
        "orders_written": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "complete_matrix": output_path / "small_emotion_q2_complete_matrix.csv",
        "summary": output_path / "small_emotion_q2_complete_summary.json",
        "manifest": output_path / "small_emotion_q2_complete_manifest.json",
        "report": output_path / "small_emotion_q2_complete_report.md",
    }


def _closeout_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "q2_intake_status",
        "active_expected_return_rows",
        "survival_decision",
        "cost_capacity_status",
        "optimizer_input_probe_status",
        "optimizer_panel_count",
        "optimizer_observed_panels",
        "optimizer_status",
        "constraint_fail_count",
        "signal_sign_response_status",
        "live_panel_net_weight_change",
        "sign_flipped_panel_net_weight_change",
        "zero_alpha_panel_net_weight_change",
        "q2_complete_decision",
        "orders_written",
        "portfolio_construction_allowed",
        "alpha_registry_update_allowed",
        "paper_ready",
        "live_ready",
        "broker_order_path_opened",
        "production_approval_claimed",
        "no_view_not_zero_alpha",
    ]


def _report(summary: dict[str, object], matrix: pd.DataFrame) -> str:
    lines = [
        "# Q2-SMALL-EMOTION-04 Complete Execution-Survival Closeout",
        "",
        "This is a Q2 execution-survival closeout only. It verifies candidate intake, execution-survival diagnostics, and local optimizer adapter response. It does not build a portfolio construction artifact, write orders, update Alpha Registry, open paper/live/broker/order workflows, or claim production approval.",
        "",
        "## Boundary",
        "",
        "- Q2 execution-survival closeout only",
        "- portfolio construction: not opened",
        "- broker/order/live paths: closed",
        "- production approval: not claimed",
        "",
        "## Summary",
        "",
        f"- candidate_count: `{summary['candidate_count']}`",
        f"- q2_complete_passed_count: `{summary['q2_complete_passed_count']}`",
        f"- q2_complete_failed_count: `{summary['q2_complete_failed_count']}`",
        "",
        "| candidate | decision | survival | optimizer | panels | constraints failed | sign response |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for row in matrix.to_dict("records"):
        lines.append(
            "| {candidate} | {decision} | {survival} | {optimizer} | {panels} | {constraint_fails} | {sign} |".format(
                candidate=row.get("candidate_name", ""),
                decision=row.get("q2_complete_decision", ""),
                survival=row.get("survival_decision", ""),
                optimizer=row.get("optimizer_status", ""),
                panels=row.get("optimizer_panel_count", ""),
                constraint_fails=row.get("constraint_fail_count", ""),
                sign=row.get("signal_sign_response_status", ""),
            )
        )
    return "\n".join(lines) + "\n"


def _safe_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _hash_if_exists(path: Path) -> str:
    return sha256_file(path) if path.exists() else "missing"
