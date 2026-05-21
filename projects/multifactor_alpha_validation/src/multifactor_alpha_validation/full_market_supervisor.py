from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from multifactor_alpha_validation.full_market_locked_validation import run_full_market_locked_validation
from multifactor_alpha_validation.full_market_sweep import run_full_market_multifactor_sweep


@dataclass(frozen=True)
class FullMarketSupervisorResult:
    summary_path: str
    attempts_path: str
    frozen_candidate_manifest_path: str
    report_path: str
    validation_status: str
    decision_state: str


_ATTEMPT_COLUMNS = [
    "schema_version",
    "attempt_index",
    "candidate_id",
    "search_kind",
    "side",
    "quantile",
    "window",
    "locked_validation_decision",
    "locked_validation_status",
    "mean_return_test",
    "t_stat_test",
    "hit_rate_test",
    "month_breadth_test",
    "issuer_breadth_test",
    "retry_action",
    "not_alpha_evidence",
]


def run_full_market_multifactor_supervisor(
    returns_panel_path: Path,
    output_dir: Path,
    *,
    max_attempts: int = 100,
    random_seed: int = 17,
) -> FullMarketSupervisorResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    sweep_dir = output_dir / "e0_sweep"
    locked_dir = output_dir / "locked_validation"
    summary_path = output_dir / "supervisor_run_summary.json"
    attempts_path = output_dir / "supervisor_attempt_log.csv"
    frozen_manifest_path = output_dir / "frozen_candidate_manifest.json"
    report_path = output_dir / "full_market_supervisor_report.md"

    sweep = run_full_market_multifactor_sweep(
        returns_panel_path=returns_panel_path,
        output_dir=sweep_dir,
        top_n=max(max_attempts * 3, 10),
        random_seed=random_seed,
    )
    if sweep.validation_status == "blocked":
        attempts = pd.DataFrame(columns=_ATTEMPT_COLUMNS)
        manifest = _empty_manifest(returns_panel_path, sweep.summary_path)
        summary = _summary(
            returns_panel_path=returns_panel_path,
            sweep_summary_path=sweep.summary_path,
            attempts=attempts,
            manifest=manifest,
            decision_state="blocked_data_coverage",
            final_candidate=None,
        )
        _write_outputs(summary_path, attempts_path, frozen_manifest_path, report_path, summary, attempts, manifest)
        return _result(summary_path, attempts_path, frozen_manifest_path, report_path, "blocked", "blocked_data_coverage")

    candidates = _candidate_queue(Path(sweep.pocket_grid_path), Path(sweep.template_grid_path))
    attempts_rows: list[dict[str, Any]] = []
    final_candidate: dict[str, Any] | None = None
    final_manifest: dict[str, Any] = _empty_manifest(returns_panel_path, sweep.summary_path)
    decision_state = "blocked_no_candidates_available"
    for attempt_index, candidate in enumerate(candidates[: max(max_attempts, 0)], start=1):
        manifest = _freeze_manifest(candidate, returns_panel_path, Path(sweep.summary_path), attempt_index)
        final_manifest = manifest
        attempt_dir = locked_dir / f"attempt_{attempt_index:02d}_{_safe_id(str(candidate['candidate_id']))}"
        locked = run_full_market_locked_validation(
            returns_panel_path=returns_panel_path,
            candidate=candidate,
            output_dir=attempt_dir,
            random_seed=random_seed + attempt_index,
        )
        locked_summary = _read_json(Path(locked.summary_path))
        attempt_row = _attempt_row(attempt_index, candidate, locked, locked_summary)
        attempts_rows.append(attempt_row)
        if locked.decision_label == "locked_validation_passed":
            final_candidate = candidate
            decision_state = "locked_validation_passed_freeze_only"
            break
        decision_state = "retry_next_candidate_locked_validation_failed"
    else:
        if attempts_rows:
            decision_state = "blocked_max_attempts_exhausted"

    attempts = pd.DataFrame(attempts_rows, columns=_ATTEMPT_COLUMNS)
    summary = _summary(
        returns_panel_path=returns_panel_path,
        sweep_summary_path=sweep.summary_path,
        attempts=attempts,
        manifest=final_manifest,
        decision_state=decision_state,
        final_candidate=final_candidate,
    )
    _write_outputs(summary_path, attempts_path, frozen_manifest_path, report_path, summary, attempts, final_manifest)
    validation_status = "evaluated" if decision_state != "blocked_data_coverage" else "blocked"
    return _result(summary_path, attempts_path, frozen_manifest_path, report_path, validation_status, decision_state)


def _candidate_queue(pocket_grid_path: Path, template_grid_path: Path) -> list[dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    if pocket_grid_path.exists() and pocket_grid_path.stat().st_size:
        pockets = pd.read_csv(pocket_grid_path)
        if not pockets.empty:
            frames.append(
                pockets.assign(
                    candidate_id=pockets["pocket_id"],
                    search_kind="leaf",
                )
            )
    if template_grid_path.exists() and template_grid_path.stat().st_size:
        templates = pd.read_csv(template_grid_path)
        if not templates.empty:
            frames.append(
                templates.assign(
                    candidate_id=templates["template_id"],
                    search_kind="template",
                )
            )
    if not frames:
        return []
    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.sort_values(
        ["search_profile_score", "t_stat", "hit_rate", "sample_count"],
        ascending=[False, False, False, False],
    )
    candidates: list[dict[str, Any]] = []
    for row in combined.itertuples(index=False):
        row_dict = row._asdict()
        candidates.append(
            {
                "candidate_id": str(row_dict["candidate_id"]),
                "search_kind": str(row_dict["search_kind"]),
                "window": str(row_dict["window"]),
                "side": str(row_dict["side"]),
                "quantile": float(row_dict["quantile"]),
                "feature_id": _optional_string(row_dict.get("feature_id", "")),
                "search_profile_score": float(row_dict["search_profile_score"]),
                "mean_return": float(row_dict["mean_return"]),
                "t_stat": float(row_dict["t_stat"]),
                "hit_rate": float(row_dict["hit_rate"]),
                "month_breadth": int(row_dict["month_breadth"]),
                "issuer_breadth": int(row_dict["issuer_breadth"]),
            }
        )
    return candidates


def _freeze_manifest(
    candidate: dict[str, Any],
    returns_panel_path: Path,
    sweep_summary_path: Path,
    attempt_index: int,
) -> dict[str, Any]:
    payload = {
        "schema_version": "full_market_supervisor_frozen_candidate.v1",
        "attempt_index": attempt_index,
        "source_returns_panel_path": str(returns_panel_path),
        "source_e0_summary_path": str(sweep_summary_path),
        "candidate": candidate,
        "frozen_selection_hash": _hash(candidate),
        "locked_validation_only": True,
        "formula_modified_after_freeze": False,
        "threshold_modified_after_freeze": False,
        "d3_charter_allowed": False,
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "or_optimizer_used": False,
        "alpha_registry_update_allowed": False,
        "expected_return_panel_written": False,
        "not_alpha_evidence": True,
        "non_claims": _non_claims(),
    }
    return payload


def _empty_manifest(returns_panel_path: Path, sweep_summary_path: str) -> dict[str, Any]:
    return {
        "schema_version": "full_market_supervisor_frozen_candidate.v1",
        "attempt_index": 0,
        "source_returns_panel_path": str(returns_panel_path),
        "source_e0_summary_path": str(sweep_summary_path),
        "candidate": None,
        "frozen_selection_hash": "",
        "locked_validation_only": True,
        "d3_charter_allowed": False,
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "or_optimizer_used": False,
        "alpha_registry_update_allowed": False,
        "expected_return_panel_written": False,
        "not_alpha_evidence": True,
        "non_claims": _non_claims(),
    }


def _attempt_row(
    attempt_index: int,
    candidate: dict[str, Any],
    locked: Any,
    locked_summary: dict[str, Any],
) -> dict[str, Any]:
    test_metrics = _split_metrics(locked_summary, "test")
    retry_action = "stop_freeze_only" if locked.decision_label == "locked_validation_passed" else "retry_next_candidate"
    return {
        "schema_version": "full_market_supervisor_attempt.v1",
        "attempt_index": attempt_index,
        "candidate_id": candidate["candidate_id"],
        "search_kind": candidate["search_kind"],
        "side": candidate["side"],
        "quantile": candidate["quantile"],
        "window": candidate["window"],
        "locked_validation_decision": locked.decision_label,
        "locked_validation_status": locked.validation_status,
        "mean_return_test": test_metrics.get("mean_return", 0.0),
        "t_stat_test": test_metrics.get("t_stat", 0.0),
        "hit_rate_test": test_metrics.get("hit_rate", 0.0),
        "month_breadth_test": test_metrics.get("month_breadth", 0),
        "issuer_breadth_test": test_metrics.get("issuer_breadth", 0),
        "retry_action": retry_action,
        "not_alpha_evidence": True,
    }


def _split_metrics(summary: dict[str, Any], split: str) -> dict[str, Any]:
    for row in summary.get("split_metrics", []):
        if str(row.get("split")) == split:
            return row
    return {}


def _summary(
    *,
    returns_panel_path: Path,
    sweep_summary_path: str,
    attempts: pd.DataFrame,
    manifest: dict[str, Any],
    decision_state: str,
    final_candidate: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema_version": "full_market_supervisor_summary.v1",
        "validation_status": "blocked" if decision_state == "blocked_data_coverage" else "evaluated",
        "decision_state": decision_state,
        "returns_panel_path": str(returns_panel_path),
        "sweep_ran": True,
        "source_e0_summary_path": str(sweep_summary_path),
        "attempt_count": int(len(attempts)),
        "final_candidate": final_candidate,
        "frozen_candidate_manifest": manifest,
        "measurement_spec_written": False,
        "d3_charter_allowed": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "or_optimizer_used": False,
        "security_level_portfolio_construction_used": False,
        "alpha_registry_update_allowed": False,
        "expected_return_panel_written": False,
        "not_alpha_evidence": True,
        "non_claims": _non_claims(),
    }


def _write_outputs(
    summary_path: Path,
    attempts_path: Path,
    frozen_manifest_path: Path,
    report_path: Path,
    summary: dict[str, Any],
    attempts: pd.DataFrame,
    manifest: dict[str, Any],
) -> None:
    attempts.to_csv(attempts_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    frozen_manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(summary, attempts), encoding="utf-8")


def _render_report(summary: dict[str, Any], attempts: pd.DataFrame) -> str:
    lines = [
        "# Full-Market Multifactor Supervisor",
        "",
        "This supervisor retry loop is diagnostic only. It runs E0 sweep candidates through locked validation and does not create alpha evidence.",
        "",
        "Q2 remains closed. D3, MeasurementSpec, Q1, OR optimization, Alpha Registry, paper/live, broker/order, and production paths remain closed.",
        "",
        f"Decision state: `{summary['decision_state']}`",
        f"Attempt count: `{summary['attempt_count']}`",
        "",
    ]
    if attempts.empty:
        lines.append("No locked validation attempts were available.")
    else:
        lines.append("## Attempts")
        for row in attempts.itertuples(index=False):
            lines.append(
                f"- Attempt `{row.attempt_index}` `{row.candidate_id}` `{row.window}`: "
                f"decision `{row.locked_validation_decision}`, action `{row.retry_action}`."
            )
    lines.append("")
    return "\n".join(lines)


def _hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _optional_string(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value)
    return "" if text.lower() == "nan" else text


def _safe_id(value: str) -> str:
    return "".join(character if character.isalnum() or character in {"_", "-"} else "_" for character in value)[:80]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _non_claims() -> dict[str, bool]:
    return {
        "alpha_evidence": False,
        "d3_approval": False,
        "q1_entry": False,
        "q2_entry": False,
        "or_optimizer": False,
        "alpha_registry": False,
        "paper_canary": False,
        "live_trading": False,
        "broker_order_workflow": False,
        "production_approval": False,
    }


def _result(
    summary_path: Path,
    attempts_path: Path,
    frozen_manifest_path: Path,
    report_path: Path,
    validation_status: str,
    decision_state: str,
) -> FullMarketSupervisorResult:
    return FullMarketSupervisorResult(
        summary_path=str(summary_path),
        attempts_path=str(attempts_path),
        frozen_candidate_manifest_path=str(frozen_manifest_path),
        report_path=str(report_path),
        validation_status=validation_status,
        decision_state=decision_state,
    )
