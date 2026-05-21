"""Local-only typed Q2 adapter for PortfolioOS fixture outputs."""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any

import pandas as pd

from execution_aware_optimizer.experiment_config import ExperimentConfig, load_experiment_config
from execution_aware_optimizer.ladder import (
    BacktestRunner,
    LadderResultRow,
    build_unavailable_ladder_rows,
    run_alpha_decay_ladder,
)
from execution_aware_optimizer.typed_adapter_schema import (
    TYPED_Q2_ADAPTER_MANIFEST_SCHEMA_VERSION,
    TypedQ2AdapterInput,
    TypedQ2AdapterManifest,
    TypedQ2AdapterMatrixRow,
    TypedQ2AdapterResult,
    TypedQ2AdapterRobustnessSummary,
)
from execution_aware_optimizer.typed_execution_matrix import TypedQ2InputContractV2
from portfolio_os.observability.events import TraceEvent
from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file


FORBIDDEN_OUTPUT_KEY_MARKERS = (
    "account_id",
    "alpaca_order",
    "api_key",
    "broker_output",
    "filled_order",
    "live_performance",
    "production_alpha_approved",
    "recommended_trade",
    "secret",
    "submitted_order",
    "target_order",
    "trade_instruction",
    "trading_instruction",
)

ALLOWED_FORBIDDEN_MARKER_KEYS = {
    "no_broker",
    "no_broker_confirmed",
    "direct_q2_execution_allowed",
}


def run_typed_q2_adapter(
    adapter_input: TypedQ2AdapterInput,
    *,
    backtest_runner: BacktestRunner | None = None,
) -> TypedQ2AdapterResult:
    """Run the typed Q2 adapter against local artifacts.

    The adapter validates typed-alpha inputs and only invokes PortfolioOS when
    explicitly enabled. It returns observed rows only when local fixture metrics
    exist; otherwise it preserves structured unavailable rows.
    """

    input_hashes = _input_artifact_hashes(adapter_input)
    source_config_hash = hash_payload(
        {
            "adapter_input": adapter_input.model_dump(mode="json"),
            "input_artifact_hashes": input_hashes,
        }
    )
    rejection_reasons = _validate_required_artifacts(adapter_input)
    if rejection_reasons:
        return _result(
            adapter_input=adapter_input,
            adapter_status="rejected",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rejection_reasons=rejection_reasons,
            rows=[],
        )

    q2_input_payload = _read_json(adapter_input.q2_input_contract_v2_path)
    projection_manifest = _read_json(adapter_input.projection_manifest_path)
    forbidden_reasons = _forbidden_output_reasons(
        {
            "q2_input_contract_v2": q2_input_payload,
            "projection_manifest": projection_manifest,
            "expected_return_panel_headers": _csv_headers(adapter_input.expected_return_panel_path),
        }
    )
    if forbidden_reasons:
        return _result(
            adapter_input=adapter_input,
            adapter_status="rejected",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rejection_reasons=forbidden_reasons,
            rows=[],
        )

    try:
        contract = TypedQ2InputContractV2.model_validate(q2_input_payload)
        _validate_projection_manifest(contract, projection_manifest)
        expected_return_panel = pd.read_csv(adapter_input.expected_return_panel_path)
    except Exception as exc:  # noqa: BLE001 - deterministic rejection wrapper
        return _result(
            adapter_input=adapter_input,
            adapter_status="rejected",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rejection_reasons=[f"typed adapter input validation failed: {exc}"],
            rows=[],
        )

    config = _load_adapter_experiment_config(adapter_input)
    active_rebalance_count = expected_return_panel["date"].astype(str).nunique() if "date" in expected_return_panel else 0
    active_name_count = expected_return_panel["symbol"].astype(str).str.upper().nunique() if "symbol" in expected_return_panel else 0
    expected_return_used_share = _expected_return_used_share(projection_manifest, expected_return_panel)

    if not adapter_input.allow_portfolioos_run:
        ladder_rows = run_alpha_decay_ladder(config, alpha_panel=expected_return_panel, backtest_runner=backtest_runner)
    elif not adapter_input.local_backtest_manifest_path.exists():
        ladder_rows = build_unavailable_ladder_rows(
            config,
            reason="local_backtest_manifest is missing; typed Q2 adapter cannot observe PortfolioOS fixture rows.",
        )
    else:
        ladder_rows = run_alpha_decay_ladder(config, alpha_panel=expected_return_panel, backtest_runner=backtest_runner)

    rows = [
        _map_ladder_row(
            adapter_input=adapter_input,
            ladder_row=ladder_row,
            active_rebalance_count=active_rebalance_count,
            active_name_count=active_name_count,
            expected_return_used_share=expected_return_used_share,
            source_config_hash=source_config_hash,
        )
        for ladder_row in ladder_rows
    ]
    return _result(
        adapter_input=adapter_input,
        adapter_status=_adapter_status(rows),
        source_config_hash=source_config_hash,
        input_hashes=input_hashes,
        rejection_reasons=[],
        rows=rows,
    )


def write_typed_q2_adapter_artifacts(result: TypedQ2AdapterResult, output_dir: str | Path) -> dict[str, Path]:
    """Write typed Q2 adapter result artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    matrix_path = output_path / "typed_q2_execution_matrix.csv"
    result_path = output_path / "typed_q2_adapter_result.json"
    summary_path = output_path / "typed_q2_robustness_summary.json"
    manifest_path = output_path / "typed_q2_adapter_manifest.json"
    trace_path = output_path / "typed_q2_adapter_trace.jsonl"

    pd.DataFrame([row.model_dump(mode="json") for row in result.matrix_rows]).to_csv(matrix_path, index=False)
    _write_json(result_path, result.model_dump(mode="json"))

    summary = _build_summary(result)
    _write_json(summary_path, summary.model_dump(mode="json"))

    trace_events = [
        TraceEvent.create(
            event="typed_q2_adapter_started",
            payload={"run_id": result.run_id},
        ),
        TraceEvent.create(
            event="typed_q2_adapter_completed",
            payload={
                "run_id": result.run_id,
                "adapter_status": result.adapter_status,
                "observed_rows": result.observed_rows,
                "unavailable_rows": result.unavailable_rows,
            },
        ),
    ]
    trace_path.write_text(
        "".join(event.model_dump_json() + "\n" for event in trace_events),
        encoding="utf-8",
    )

    output_hashes = {
        "typed_q2_execution_matrix": sha256_file(matrix_path),
        "typed_q2_adapter_result": sha256_file(result_path),
        "typed_q2_robustness_summary": sha256_file(summary_path),
        "typed_q2_adapter_trace": sha256_file(trace_path),
    }
    manifest_payload = {
        "schema_version": TYPED_Q2_ADAPTER_MANIFEST_SCHEMA_VERSION,
        "run_id": result.run_id,
        "adapter_status": result.adapter_status,
        "source_config_hash": result.source_config_hash,
        "input_artifact_hashes": result.input_artifact_hashes,
        "output_artifacts": output_hashes,
        "no_live_data_confirmed": result.no_live_data_confirmed,
        "no_orders_confirmed": result.no_orders_confirmed,
        "no_broker_confirmed": result.no_broker_confirmed,
    }
    manifest_payload["content_hash"] = hash_payload(manifest_payload)
    manifest = TypedQ2AdapterManifest.model_validate(manifest_payload)
    _write_json(manifest_path, manifest.model_dump(mode="json"))

    return {
        "matrix": matrix_path,
        "result": result_path,
        "summary": summary_path,
        "manifest": manifest_path,
        "trace": trace_path,
    }


def _load_adapter_experiment_config(adapter_input: TypedQ2AdapterInput) -> ExperimentConfig:
    config = load_experiment_config(adapter_input.adapter_config_path)
    portfolioos = config.portfolioos.model_copy(
        update={
            "allow_portfolioos_run": adapter_input.allow_portfolioos_run,
            "backtest_manifest": str(adapter_input.local_backtest_manifest_path),
        }
    )
    return config.model_copy(update={"portfolioos": portfolioos})


def _validate_required_artifacts(adapter_input: TypedQ2AdapterInput) -> list[str]:
    reasons: list[str] = []
    required_paths = {
        "adapter_config": adapter_input.adapter_config_path,
        "expected_return_panel": adapter_input.expected_return_panel_path,
        "projection_manifest": adapter_input.projection_manifest_path,
        "q2_input_contract_v2": adapter_input.q2_input_contract_v2_path,
    }
    for name, path in required_paths.items():
        if not path.exists():
            reasons.append(f"{name} artifact is missing at {path}")
    return reasons


def _input_artifact_hashes(adapter_input: TypedQ2AdapterInput) -> dict[str, str]:
    paths = {
        "adapter_config": adapter_input.adapter_config_path,
        "expected_return_panel": adapter_input.expected_return_panel_path,
        "local_backtest_manifest": adapter_input.local_backtest_manifest_path,
        "projection_manifest": adapter_input.projection_manifest_path,
        "q2_input_contract_v2": adapter_input.q2_input_contract_v2_path,
    }
    return {
        name: sha256_file(path) if path.exists() else "missing"
        for name, path in sorted(paths.items())
    }


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def _csv_headers(path: str | Path) -> list[str]:
    return list(pd.read_csv(path, nrows=0).columns)


def _validate_projection_manifest(contract: TypedQ2InputContractV2, projection_manifest: dict[str, Any]) -> None:
    if projection_manifest.get("schema_version") != "alpha_projection.v2":
        raise ValueError("projection_manifest must use alpha_projection.v2")
    if projection_manifest.get("content_hash") != contract.projection_manifest_hash:
        raise ValueError("projection manifest hash does not match Q2InputContractV2")
    if contract.alpha_view_id not in set(projection_manifest.get("alpha_view_ids", [])):
        raise ValueError("projection manifest does not include contract alpha_view_id")


def _expected_return_used_share(projection_manifest: dict[str, Any], expected_return_panel: pd.DataFrame) -> float:
    universe_count = len(set(projection_manifest.get("universe_symbols", [])))
    rebalance_count = len(set(projection_manifest.get("rebalance_dates", [])))
    denominator = universe_count * rebalance_count
    if denominator <= 0 or "date" not in expected_return_panel or "symbol" not in expected_return_panel:
        return 0.0
    used = {
        (str(row.date), str(row.symbol).upper())
        for row in expected_return_panel.itertuples(index=False)
        if getattr(row, "expected_return", None) is not None
    }
    return float(len(used) / denominator)


def _map_ladder_row(
    *,
    adapter_input: TypedQ2AdapterInput,
    ladder_row: LadderResultRow,
    active_rebalance_count: int,
    active_name_count: int,
    expected_return_used_share: float,
    source_config_hash: str,
) -> TypedQ2AdapterMatrixRow:
    status = "unavailable" if ladder_row.infeasibility_reason else "observed"
    gross_return = ladder_row.gross_return if status == "observed" else None
    net_return = ladder_row.net_return if status == "observed" else None
    turnover = ladder_row.turnover if status == "observed" else None
    cost_drag = None
    if status == "observed":
        if gross_return is not None and net_return is not None:
            cost_drag = gross_return - net_return
        else:
            cost_drag = ladder_row.estimated_transaction_cost
    gross_to_net_retention = (
        net_return / gross_return
        if status == "observed" and gross_return not in (None, 0.0) and net_return is not None
        else None
    )
    row_date = _serialize_ladder_date(ladder_row)
    row_hash = hash_payload(
        {
            "run_id": adapter_input.run_id,
            "layer": ladder_row.layer_name,
            "date": row_date,
            "source_config_hash": source_config_hash,
        }
    )
    return TypedQ2AdapterMatrixRow(
        scenario_id=f"{adapter_input.run_id}__{ladder_row.layer_name}__{row_date or 'undated'}",
        alpha_family="synthetic_typed_fixture",
        projection_policy="rebalance_period_projection",
        abstain_policy="explicit_abstain",
        layer=ladder_row.layer_name,
        date=row_date,
        status=status,
        active_rebalance_count=active_rebalance_count,
        active_name_count=active_name_count,
        expected_return_used_share=expected_return_used_share,
        gross_return=gross_return,
        net_return=net_return,
        turnover=turnover,
        cost_drag=cost_drag,
        gross_to_net_retention=gross_to_net_retention,
        repair_retention=None,
        unavailable_reason=ladder_row.infeasibility_reason,
        source_config_hash=row_hash,
    )


def _adapter_status(rows: list[TypedQ2AdapterMatrixRow]) -> str:
    observed = sum(1 for row in rows if row.status == "observed")
    unavailable = sum(1 for row in rows if row.status == "unavailable")
    if observed and unavailable:
        return "partially_observed"
    if observed:
        return "observed"
    return "unavailable"


def _serialize_ladder_date(ladder_row: LadderResultRow) -> str | None:
    if ladder_row.date is None:
        return None
    if hasattr(ladder_row.date, "isoformat"):
        return ladder_row.date.isoformat()
    return str(ladder_row.date)


def _result(
    *,
    adapter_input: TypedQ2AdapterInput,
    adapter_status: str,
    source_config_hash: str,
    input_hashes: dict[str, str],
    rejection_reasons: list[str],
    rows: list[TypedQ2AdapterMatrixRow],
) -> TypedQ2AdapterResult:
    return TypedQ2AdapterResult(
        run_id=adapter_input.run_id,
        adapter_status=adapter_status,
        observed_rows=sum(1 for row in rows if row.status == "observed"),
        unavailable_rows=sum(1 for row in rows if row.status == "unavailable"),
        rejection_reasons=rejection_reasons,
        matrix_rows=rows,
        source_config_hash=source_config_hash,
        input_artifact_hashes=input_hashes,
        no_live_data_confirmed=True,
        no_orders_confirmed=True,
        no_broker_confirmed=True,
    )


def _build_summary(result: TypedQ2AdapterResult) -> TypedQ2AdapterRobustnessSummary:
    status_counts = Counter(row.status for row in result.matrix_rows)
    unavailable_reasons = Counter(
        row.unavailable_reason or "Not available"
        for row in result.matrix_rows
        if row.status == "unavailable"
    )
    return TypedQ2AdapterRobustnessSummary(
        run_id=result.run_id,
        adapter_status=result.adapter_status,
        total_rows=len(result.matrix_rows),
        observed_rows=result.observed_rows,
        unavailable_rows=result.unavailable_rows,
        rejected_rows=sum(1 for row in result.matrix_rows if row.status == "rejected"),
        status_counts=dict(sorted(status_counts.items())),
        unavailable_reason_counts=dict(sorted(unavailable_reasons.items())),
    )


def _forbidden_output_reasons(named_payloads: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    for payload_name, payload in named_payloads.items():
        for path, key in _iter_payload_keys(payload):
            normalized = _normalize_key(key)
            if normalized in {_normalize_key(item) for item in ALLOWED_FORBIDDEN_MARKER_KEYS}:
                continue
            if any(marker in normalized for marker in FORBIDDEN_OUTPUT_KEY_MARKERS):
                reasons.append(f"{payload_name} contains forbidden output key {'.'.join(path + [key])}")
    return sorted(set(reasons))


def _iter_payload_keys(payload: Any, path: list[str] | None = None) -> list[tuple[list[str], str]]:
    path = path or []
    if isinstance(payload, dict):
        pairs: list[tuple[list[str], str]] = []
        for key, value in payload.items():
            pairs.append((path, str(key)))
            pairs.extend(_iter_payload_keys(value, path + [str(key)]))
        return pairs
    if isinstance(payload, list):
        pairs = []
        for index, item in enumerate(payload):
            pairs.extend(_iter_payload_keys(item, path + [str(index)]))
        return pairs
    return []


def _normalize_key(key: str) -> str:
    return key.lower().replace("-", "_").replace(" ", "_")
