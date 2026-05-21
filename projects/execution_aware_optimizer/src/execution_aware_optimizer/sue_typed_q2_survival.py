"""SUE typed Q2 survival matrix v1.

This Phase 50 adapter consumes SUE typed-alpha artifacts, aligns the projected
expected-return panel to a local PortfolioOS fixture date, reuses the Phase 48
injection path, and maps Q2 rows as observed or unavailable. It does not run
live data, brokers, order workflows, or production approval.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from pydantic import Field

from execution_aware_optimizer.sue_typed_q2_survival_schema import (
    SueTypedQ2SurvivalInput,
    SueTypedQ2SurvivalResult,
    SueTypedQ2SurvivalRow,
    SueTypedQ2SurvivalSummary,
)
from execution_aware_optimizer.typed_expected_return_injection import (
    OPTIMIZER_INPUT_SNAPSHOT_COLUMNS,
    TypedExpectedReturnInjectionRun,
    run_typed_expected_return_injection,
)
from execution_aware_optimizer.typed_injection_schema import TypedExpectedReturnInjectionInput
from portfolio_os.observability.events import TraceEvent
from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file


SUE_SURVIVAL_MATRIX_COLUMNS = [
    "schema_version",
    "scenario_id",
    "alpha_family",
    "projection_policy",
    "abstain_policy",
    "layer",
    "date",
    "status",
    "active_rebalance_count",
    "active_name_count",
    "expected_return_used_share",
    "gross_return",
    "net_return",
    "turnover",
    "cost_drag",
    "gross_to_net_retention",
    "repair_retention",
    "unavailable_reason",
    "source_config_hash",
]


class SueTypedQ2SurvivalRun(SueTypedQ2SurvivalResult):
    """In-memory Phase 50 result plus writeable hidden artifacts."""

    optimizer_input_snapshot: Any = Field(default=None, exclude=True)

    @property
    def summary(self) -> SueTypedQ2SurvivalSummary:
        return _build_summary(self)


def run_sue_typed_q2_survival(survival_input: SueTypedQ2SurvivalInput) -> SueTypedQ2SurvivalRun:
    """Run the local SUE typed Q2 survival matrix."""

    input_hashes = _input_artifact_hashes(survival_input)
    source_config_hash = hash_payload(
        {
            "survival_input": survival_input.model_dump(mode="json"),
            "input_artifact_hashes": input_hashes,
        }
    )
    missing = _validate_required_artifacts(survival_input)
    if missing:
        return _run_result(
            survival_input=survival_input,
            survival_status="rejected",
            injection_status="rejected",
            expected_return_reached_optimizer_input=False,
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rejection_reasons=missing,
        )

    try:
        source_panel = _load_expected_return_panel(survival_input.expected_return_panel_path)
        original_dates = sorted(set(source_panel["date"].astype(str)))
        local_rebalance_date = _local_rebalance_date(survival_input, source_panel)
        with TemporaryDirectory(prefix="portfolioos_sue_survival_") as work_dir_text:
            aligned = _write_aligned_artifacts(
                survival_input=survival_input,
                source_panel=source_panel,
                local_rebalance_date=local_rebalance_date,
                work_dir=Path(work_dir_text),
            )
            injection_run = run_typed_expected_return_injection(
                TypedExpectedReturnInjectionInput.model_validate(
                    {
                        "adapter_config_path": str(survival_input.adapter_config_path),
                        "allow_portfolioos_run": survival_input.allow_portfolioos_run,
                        "expected_return_panel_path": str(aligned["expected_return_panel"]),
                        "local_backtest_manifest_path": str(survival_input.local_backtest_manifest_path),
                        "no_broker": True,
                        "no_network": True,
                        "projection_manifest_path": str(aligned["projection_manifest"]),
                        "q2_input_contract_v2_path": str(aligned["q2_input_contract_v2"]),
                        "rebalance_date": local_rebalance_date,
                        "run_id": survival_input.run_id,
                    }
                )
            )
    except Exception as exc:  # noqa: BLE001 - deterministic unavailable wrapper
        return _run_result(
            survival_input=survival_input,
            survival_status="unavailable",
            injection_status="unavailable",
            expected_return_reached_optimizer_input=False,
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            unavailable_reason=f"SUE typed Q2 survival unavailable: {exc}",
        )

    rows = _map_survival_rows(injection_run)
    return _run_result(
        survival_input=survival_input,
        survival_status=_survival_status(injection_run, rows),
        injection_status=injection_run.result.injection_status,
        expected_return_reached_optimizer_input=injection_run.result.expected_return_reached_optimizer_input,
        source_config_hash=source_config_hash,
        input_hashes=input_hashes,
        rows=rows,
        optimizer_input_snapshot=injection_run.optimizer_input_snapshot,
        optimizer_rebalance_date=injection_run.result.optimizer_rebalance_date,
        original_projection_dates=original_dates,
        local_rebalance_date=local_rebalance_date,
        active_rebalance_count=injection_run.result.active_rebalance_count,
        active_name_count=injection_run.result.active_name_count,
        expected_return_used_share=injection_run.result.expected_return_used_share,
        q2_observed_rows=injection_run.result.q2_observed_rows,
        q2_unavailable_rows=injection_run.result.q2_unavailable_rows,
        rejection_reasons=injection_run.result.rejection_reasons,
        unavailable_reason=injection_run.result.unavailable_reason,
    )


def write_sue_typed_q2_survival_artifacts(
    result: SueTypedQ2SurvivalResult,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write Phase 50 SUE survival artifacts."""

    run = _ensure_run(result)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    matrix_path = output_path / "sue_typed_q2_execution_matrix.csv"
    summary_path = output_path / "sue_typed_q2_survival_summary.json"
    snapshot_path = output_path / "sue_optimizer_input_snapshot.csv"
    manifest_path = output_path / "sue_injection_manifest.json"
    trace_path = output_path / "sue_q2_trace.jsonl"
    result_path = output_path / "sue_typed_q2_survival_result.json"

    pd.DataFrame([row.model_dump(mode="json") for row in run.matrix_rows]).reindex(
        columns=SUE_SURVIVAL_MATRIX_COLUMNS
    ).to_csv(matrix_path, index=False)
    _write_json(summary_path, run.summary.model_dump(mode="json"))
    _write_json(result_path, run.model_dump(mode="json", exclude={"optimizer_input_snapshot"}))
    _snapshot_frame(run).reindex(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS).to_csv(snapshot_path, index=False)
    _write_json(manifest_path, _build_injection_manifest(run, matrix_path, summary_path, snapshot_path, result_path))
    trace_events = [
        TraceEvent.create(event="sue_typed_q2_survival_started", payload={"run_id": run.run_id}),
        TraceEvent.create(
            event="sue_typed_q2_survival_completed",
            payload={
                "run_id": run.run_id,
                "survival_status": run.survival_status,
                "expected_return_reached_optimizer_input": run.expected_return_reached_optimizer_input,
                "q2_observed_rows": run.q2_observed_rows,
                "q2_unavailable_rows": run.q2_unavailable_rows,
            },
        ),
    ]
    trace_path.write_text("".join(event.model_dump_json() + "\n" for event in trace_events), encoding="utf-8")

    return {
        "matrix": matrix_path,
        "summary": summary_path,
        "optimizer_input_snapshot": snapshot_path,
        "injection_manifest": manifest_path,
        "trace": trace_path,
        "result": result_path,
    }


def _write_aligned_artifacts(
    *,
    survival_input: SueTypedQ2SurvivalInput,
    source_panel: pd.DataFrame,
    local_rebalance_date: str,
    work_dir: Path,
) -> dict[str, Path]:
    work_dir.mkdir(parents=True, exist_ok=True)
    aligned_panel = source_panel.copy()
    aligned_panel["date"] = local_rebalance_date
    panel_path = work_dir / "sue_expected_return_panel_aligned.csv"
    aligned_panel.to_csv(panel_path, index=False)

    projection_manifest = _read_json(survival_input.projection_manifest_path)
    projection_manifest["rebalance_dates"] = [local_rebalance_date]
    projection_manifest["panel_row_count"] = int(len(aligned_panel))
    projection_manifest["schema_version"] = "alpha_projection.v2"
    projection_manifest.pop("content_hash", None)
    projection_manifest["content_hash"] = hash_payload(projection_manifest)
    projection_path = work_dir / "sue_projection_manifest_aligned.json"
    _write_json(projection_path, projection_manifest)

    contract = _read_json(survival_input.q2_input_contract_v2_path)
    contract["expected_return_panel_artifact"] = panel_path.name
    contract["projection_manifest_hash"] = projection_manifest["content_hash"]
    q2_path = work_dir / "sue_q2_input_contract_v2_aligned.json"
    _write_json(q2_path, contract)

    return {
        "expected_return_panel": panel_path,
        "projection_manifest": projection_path,
        "q2_input_contract_v2": q2_path,
    }


def _map_survival_rows(injection_run: TypedExpectedReturnInjectionRun) -> list[SueTypedQ2SurvivalRow]:
    rows: list[SueTypedQ2SurvivalRow] = []
    for row in injection_run.q2_matrix_rows:
        scenario_id = str(row.get("scenario_id") or "")
        rows.append(
            SueTypedQ2SurvivalRow(
                scenario_id=scenario_id.replace("synthetic_typed_fixture", "sue_typed_q2_survival"),
                layer=str(row.get("layer")),
                date=row.get("date"),
                status=row.get("status"),
                active_rebalance_count=int(row.get("active_rebalance_count") or 0),
                active_name_count=int(row.get("active_name_count") or 0),
                expected_return_used_share=float(row.get("expected_return_used_share") or 0.0),
                gross_return=row.get("gross_return"),
                net_return=row.get("net_return"),
                turnover=row.get("turnover"),
                cost_drag=row.get("cost_drag"),
                gross_to_net_retention=row.get("gross_to_net_retention"),
                repair_retention=row.get("repair_retention"),
                unavailable_reason=row.get("unavailable_reason"),
                source_config_hash=str(row.get("source_config_hash") or ""),
            )
        )
    return rows


def _survival_status(
    injection_run: TypedExpectedReturnInjectionRun,
    rows: list[SueTypedQ2SurvivalRow],
) -> str:
    if injection_run.result.injection_status == "rejected":
        return "rejected"
    observed = sum(1 for row in rows if row.status == "observed")
    unavailable = sum(1 for row in rows if row.status == "unavailable")
    if observed and unavailable:
        return "partially_observed"
    if observed:
        return "observed"
    return "unavailable"


def _run_result(
    *,
    survival_input: SueTypedQ2SurvivalInput,
    survival_status: str,
    injection_status: str,
    expected_return_reached_optimizer_input: bool,
    source_config_hash: str,
    input_hashes: dict[str, str],
    rows: list[SueTypedQ2SurvivalRow] | None = None,
    optimizer_input_snapshot: Any = None,
    optimizer_rebalance_date: str | None = None,
    original_projection_dates: list[str] | None = None,
    local_rebalance_date: str | None = None,
    active_rebalance_count: int = 0,
    active_name_count: int = 0,
    expected_return_used_share: float = 0.0,
    q2_observed_rows: int = 0,
    q2_unavailable_rows: int = 0,
    rejection_reasons: list[str] | None = None,
    unavailable_reason: str | None = None,
) -> SueTypedQ2SurvivalRun:
    return SueTypedQ2SurvivalRun(
        run_id=survival_input.run_id,
        survival_status=survival_status,
        injection_status=injection_status,
        expected_return_reached_optimizer_input=expected_return_reached_optimizer_input,
        optimizer_rebalance_date=optimizer_rebalance_date,
        original_projection_dates=original_projection_dates or [],
        local_rebalance_date=local_rebalance_date,
        active_rebalance_count=active_rebalance_count,
        active_name_count=active_name_count,
        expected_return_used_share=expected_return_used_share,
        q2_observed_rows=q2_observed_rows,
        q2_unavailable_rows=q2_unavailable_rows,
        matrix_rows=rows or [],
        rejection_reasons=rejection_reasons or [],
        unavailable_reason=unavailable_reason,
        source_config_hash=source_config_hash,
        input_artifact_hashes=input_hashes,
        optimizer_input_snapshot=optimizer_input_snapshot,
    )


def _build_summary(run: SueTypedQ2SurvivalResult) -> SueTypedQ2SurvivalSummary:
    return SueTypedQ2SurvivalSummary(
        run_id=run.run_id,
        survival_status=run.survival_status,
        injection_status=run.injection_status,
        expected_return_reached_optimizer_input=run.expected_return_reached_optimizer_input,
        optimizer_rebalance_date=run.optimizer_rebalance_date,
        active_rebalance_count=run.active_rebalance_count,
        active_name_count=run.active_name_count,
        expected_return_used_share=run.expected_return_used_share,
        q2_observed_rows=run.q2_observed_rows,
        q2_unavailable_rows=run.q2_unavailable_rows,
        unavailable_reason=run.unavailable_reason,
        rejection_reasons=run.rejection_reasons,
    )


def _build_injection_manifest(
    run: SueTypedQ2SurvivalRun,
    matrix_path: Path,
    summary_path: Path,
    snapshot_path: Path,
    result_path: Path,
) -> dict[str, Any]:
    payload = {
        "schema_version": "sue_typed_q2_injection_manifest.v1",
        "run_id": run.run_id,
        "survival_status": run.survival_status,
        "injection_status": run.injection_status,
        "expected_return_reached_optimizer_input": run.expected_return_reached_optimizer_input,
        "optimizer_rebalance_date": run.optimizer_rebalance_date,
        "original_projection_dates": run.original_projection_dates,
        "local_rebalance_date": run.local_rebalance_date,
        "source_config_hash": run.source_config_hash,
        "input_artifact_hashes": run.input_artifact_hashes,
        "output_artifacts": {
            "sue_typed_q2_execution_matrix": sha256_file(matrix_path),
            "sue_typed_q2_survival_summary": sha256_file(summary_path),
            "sue_optimizer_input_snapshot": sha256_file(snapshot_path),
            "sue_typed_q2_survival_result": sha256_file(result_path),
        },
        "no_live_data_confirmed": run.no_live_data_confirmed,
        "no_orders_confirmed": run.no_orders_confirmed,
        "no_broker_confirmed": run.no_broker_confirmed,
        "production_approval_claimed": run.production_approval_claimed,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _snapshot_frame(run: SueTypedQ2SurvivalRun) -> pd.DataFrame:
    if isinstance(run.optimizer_input_snapshot, pd.DataFrame):
        return run.optimizer_input_snapshot.copy()
    return pd.DataFrame(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS)


def _ensure_run(result: SueTypedQ2SurvivalResult) -> SueTypedQ2SurvivalRun:
    if isinstance(result, SueTypedQ2SurvivalRun):
        return result
    return SueTypedQ2SurvivalRun(**result.model_dump(), optimizer_input_snapshot=None)


def _load_expected_return_panel(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {"date", "symbol", "expected_return"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError("expected_return_panel missing required columns: " + ", ".join(sorted(missing)))
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.strftime("%Y-%m-%d")
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["expected_return"] = pd.to_numeric(frame["expected_return"], errors="raise").astype(float)
    return frame


def _local_rebalance_date(survival_input: SueTypedQ2SurvivalInput, source_panel: pd.DataFrame) -> str:
    if survival_input.local_rebalance_date is not None:
        return pd.Timestamp(survival_input.local_rebalance_date).strftime("%Y-%m-%d")
    return str(source_panel["date"].iloc[0])


def _validate_required_artifacts(survival_input: SueTypedQ2SurvivalInput) -> list[str]:
    paths = {
        "adapter_config": survival_input.adapter_config_path,
        "expected_return_panel": survival_input.expected_return_panel_path,
        "local_backtest_manifest": survival_input.local_backtest_manifest_path,
        "projection_manifest": survival_input.projection_manifest_path,
        "q2_input_contract_v2": survival_input.q2_input_contract_v2_path,
    }
    return [f"{name} artifact is missing at {path}" for name, path in sorted(paths.items()) if not path.exists()]


def _input_artifact_hashes(survival_input: SueTypedQ2SurvivalInput) -> dict[str, str]:
    paths = {
        "adapter_config": survival_input.adapter_config_path,
        "expected_return_panel": survival_input.expected_return_panel_path,
        "local_backtest_manifest": survival_input.local_backtest_manifest_path,
        "projection_manifest": survival_input.projection_manifest_path,
        "q2_input_contract_v2": survival_input.q2_input_contract_v2_path,
    }
    return {name: sha256_file(path) if path.exists() else "missing" for name, path in sorted(paths.items())}


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
