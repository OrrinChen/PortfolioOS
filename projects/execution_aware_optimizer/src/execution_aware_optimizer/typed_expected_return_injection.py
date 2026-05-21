"""Local typed expected-return injection fixture.

This module proves that a typed expected-return panel can be converted into the
same optimizer-input shape PortfolioOS expects. It does not place orders, call
brokers, use live data, or claim alpha approval.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import Field

from execution_aware_optimizer.typed_adapter_schema import TypedQ2AdapterInput
from execution_aware_optimizer.typed_execution_matrix import TypedQ2InputContractV2
from execution_aware_optimizer.typed_injection_schema import (
    TYPED_EXPECTED_RETURN_INJECTION_MANIFEST_SCHEMA_VERSION,
    TypedExpectedReturnInjectionInput,
    TypedExpectedReturnInjectionManifest,
    TypedExpectedReturnInjectionResult,
    TypedExpectedReturnInjectionSummary,
)
from execution_aware_optimizer.typed_portfolioos_adapter import (
    run_typed_q2_adapter,
)
from portfolio_os.backtest.engine import (
    _build_strategy_universe,
    _load_returns_long,
    build_monthly_rebalance_schedule,
    reconstruct_price_panel,
)
from portfolio_os.backtest.manifest import load_backtest_manifest
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import (
    build_portfolio_frame,
    load_holdings,
    load_portfolio_state,
    load_target_weights,
)
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.observability.events import TraceEvent
from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file
from portfolio_os.utils.config import load_app_config


OPTIMIZER_INPUT_SNAPSHOT_COLUMNS = [
    "date",
    "ticker",
    "expected_return",
    "expected_return_source",
    "target_weight",
    "current_weight",
    "close",
    "adv_shares",
    "industry",
]
INJECTED_PANEL_COLUMNS = [
    "date",
    "symbol",
    "expected_return",
    "active_alpha_views",
    "horizon_conversion",
    "decay_applied",
    "confidence_weight",
]


def run_typed_expected_return_injection(
    injection_input: TypedExpectedReturnInjectionInput,
) -> TypedExpectedReturnInjectionRun:
    """Validate and inject typed expected returns into a local optimizer input snapshot."""

    input_hashes = _input_artifact_hashes(injection_input)
    source_config_hash = hash_payload(
        {
            "injection_input": injection_input.model_dump(mode="json"),
            "input_artifact_hashes": input_hashes,
        }
    )
    q2_result = _run_q2_adapter(injection_input)
    required_reasons = _validate_required_artifacts(injection_input)
    if required_reasons:
        return _build_run(
            injection_input=injection_input,
            result=TypedExpectedReturnInjectionResult(
                run_id=injection_input.run_id,
                injection_status="rejected",
                expected_return_reached_optimizer_input=False,
                q2_adapter_status=q2_result.adapter_status,
                q2_observed_rows=q2_result.observed_rows,
                q2_unavailable_rows=q2_result.unavailable_rows,
                rejection_reasons=required_reasons,
                source_config_hash=source_config_hash,
                input_artifact_hashes=input_hashes,
            ),
            injected_panel=pd.DataFrame(columns=INJECTED_PANEL_COLUMNS),
            optimizer_input_snapshot=pd.DataFrame(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS),
            q2_matrix_rows=[row.model_dump(mode="json") for row in q2_result.matrix_rows],
        )

    try:
        contract = TypedQ2InputContractV2.model_validate(_read_json(injection_input.q2_input_contract_v2_path))
        projection_manifest = _read_json(injection_input.projection_manifest_path)
        _validate_projection_manifest(contract, projection_manifest)
        injected_panel = _load_and_transform_expected_return_panel(injection_input)
    except Exception as exc:  # noqa: BLE001 - convert to deterministic rejection
        return _build_run(
            injection_input=injection_input,
            result=TypedExpectedReturnInjectionResult(
                run_id=injection_input.run_id,
                injection_status="rejected",
                expected_return_reached_optimizer_input=False,
                q2_adapter_status=q2_result.adapter_status,
                q2_observed_rows=q2_result.observed_rows,
                q2_unavailable_rows=q2_result.unavailable_rows,
                rejection_reasons=[f"typed expected-return injection validation failed: {exc}"],
                source_config_hash=source_config_hash,
                input_artifact_hashes=input_hashes,
            ),
            injected_panel=pd.DataFrame(columns=INJECTED_PANEL_COLUMNS),
            optimizer_input_snapshot=pd.DataFrame(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS),
            q2_matrix_rows=[row.model_dump(mode="json") for row in q2_result.matrix_rows],
        )

    active_rebalance_count = injected_panel["date"].astype(str).nunique() if "date" in injected_panel else 0
    active_name_count = injected_panel["symbol"].astype(str).str.upper().nunique() if "symbol" in injected_panel else 0
    used_share = _expected_return_used_share(projection_manifest, injected_panel)

    if not injection_input.allow_portfolioos_run:
        return _build_run(
            injection_input=injection_input,
            result=TypedExpectedReturnInjectionResult(
                run_id=injection_input.run_id,
                injection_status="unavailable",
                expected_return_reached_optimizer_input=False,
                active_rebalance_count=active_rebalance_count,
                active_name_count=active_name_count,
                expected_return_used_share=used_share,
                q2_adapter_status=q2_result.adapter_status,
                q2_observed_rows=q2_result.observed_rows,
                q2_unavailable_rows=q2_result.unavailable_rows,
                unavailable_reason=(
                    "PortfolioOS run disabled; typed expected-return injection snapshot was not built."
                ),
                source_config_hash=source_config_hash,
                input_artifact_hashes=input_hashes,
            ),
            injected_panel=injected_panel,
            optimizer_input_snapshot=pd.DataFrame(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS),
            q2_matrix_rows=[row.model_dump(mode="json") for row in q2_result.matrix_rows],
        )

    try:
        snapshot = build_optimizer_input_snapshot(
            manifest_path=injection_input.local_backtest_manifest_path,
            expected_return_panel=injected_panel,
            rebalance_date=injection_input.rebalance_date,
        )
    except Exception as exc:  # noqa: BLE001 - deterministic unavailable closeout
        return _build_run(
            injection_input=injection_input,
            result=TypedExpectedReturnInjectionResult(
                run_id=injection_input.run_id,
                injection_status="unavailable",
                expected_return_reached_optimizer_input=False,
                active_rebalance_count=active_rebalance_count,
                active_name_count=active_name_count,
                expected_return_used_share=used_share,
                q2_adapter_status=q2_result.adapter_status,
                q2_observed_rows=q2_result.observed_rows,
                q2_unavailable_rows=q2_result.unavailable_rows,
                unavailable_reason=f"typed expected-return injection unavailable: {exc}",
                source_config_hash=source_config_hash,
                input_artifact_hashes=input_hashes,
            ),
            injected_panel=injected_panel,
            optimizer_input_snapshot=pd.DataFrame(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS),
            q2_matrix_rows=[row.model_dump(mode="json") for row in q2_result.matrix_rows],
        )

    injected_count = int((snapshot["expected_return_source"] == "typed_expected_return_panel").sum())
    result = TypedExpectedReturnInjectionResult(
        run_id=injection_input.run_id,
        injection_status="injected",
        expected_return_reached_optimizer_input=bool(injected_count > 0),
        optimizer_input_snapshot_rows=int(len(snapshot)),
        injected_expected_return_count=injected_count,
        optimizer_rebalance_date=str(snapshot["date"].iloc[0]) if not snapshot.empty else None,
        active_rebalance_count=active_rebalance_count,
        active_name_count=active_name_count,
        expected_return_used_share=used_share,
        q2_adapter_status=q2_result.adapter_status,
        q2_observed_rows=q2_result.observed_rows,
        q2_unavailable_rows=q2_result.unavailable_rows,
        source_config_hash=source_config_hash,
        input_artifact_hashes=input_hashes,
    )
    return _build_run(
        injection_input=injection_input,
        result=result,
        injected_panel=injected_panel,
        optimizer_input_snapshot=snapshot,
        q2_matrix_rows=[row.model_dump(mode="json") for row in q2_result.matrix_rows],
    )


class TypedExpectedReturnInjectionRun(TypedExpectedReturnInjectionResult):
    """In-memory bundle of result and writeable local artifacts."""

    injected_panel: Any = Field(exclude=True)
    optimizer_input_snapshot: Any = Field(exclude=True)
    q2_matrix_rows: list[dict[str, Any]] = Field(exclude=True)

    @property
    def result(self) -> TypedExpectedReturnInjectionResult:
        return TypedExpectedReturnInjectionResult.model_validate(
            self.model_dump(exclude={"injected_panel", "optimizer_input_snapshot", "q2_matrix_rows"})
        )


def build_optimizer_input_snapshot(
    *,
    manifest_path: str | Path,
    expected_return_panel: pd.DataFrame,
    rebalance_date: str | None = None,
) -> pd.DataFrame:
    """Build a PortfolioOS optimizer input snapshot with typed expected returns."""

    manifest = load_backtest_manifest(manifest_path)
    holdings = load_holdings(manifest.initial_holdings)
    targets = load_target_weights(manifest.target_weights)
    initial_state = load_portfolio_state(manifest.portfolio_state)
    app_config = load_app_config(
        default_path=manifest.config,
        constraints_path=manifest.constraints,
        execution_path=manifest.execution_profile,
        portfolio_state=initial_state,
    )

    portfolio_frame = build_portfolio_frame(holdings, targets)
    required_tickers = portfolio_frame["ticker"].astype(str).tolist()
    base_market_frame = market_to_frame(load_market_snapshot(manifest.market_snapshot, required_tickers))
    base_reference_frame = reference_to_frame(load_reference_snapshot(manifest.reference, required_tickers))
    anchor_prices = base_market_frame.set_index("ticker")["close"].astype(float).reindex(required_tickers)
    returns_long = _load_returns_long(manifest.returns_file)
    price_panel = reconstruct_price_panel(returns_long, anchor_prices=anchor_prices).reindex(columns=required_tickers)
    schedule = build_monthly_rebalance_schedule(price_panel)
    selected_date = _select_rebalance_date(expected_return_panel, schedule=schedule, rebalance_date=rebalance_date)

    initial_quantities = portfolio_frame.set_index("ticker")["quantity"].reindex(required_tickers).fillna(0).astype(int)
    price_row = price_panel.loc[pd.Timestamp(selected_date)]
    price_row.name = pd.Timestamp(selected_date)
    universe, _config = _build_strategy_universe(
        current_date=pd.Timestamp(selected_date),
        quantities=initial_quantities,
        cash=float(initial_state.available_cash),
        targets=targets,
        base_market_frame=base_market_frame,
        base_reference_frame=base_reference_frame,
        price_row=price_row,
        app_config_template=app_config,
    )
    return inject_expected_returns_into_optimizer_input(
        universe,
        expected_return_panel=expected_return_panel,
        rebalance_date=selected_date,
    )


def inject_expected_returns_into_optimizer_input(
    universe: pd.DataFrame,
    *,
    expected_return_panel: pd.DataFrame,
    rebalance_date: str,
) -> pd.DataFrame:
    """Attach typed expected returns to a universe frame in optimizer-input form."""

    panel = expected_return_panel.copy()
    panel["date"] = pd.to_datetime(panel["date"], errors="raise").dt.strftime("%Y-%m-%d")
    panel["symbol"] = panel["symbol"].astype(str).str.upper()
    panel["expected_return"] = pd.to_numeric(panel["expected_return"], errors="raise").astype(float)
    selected = panel.loc[panel["date"] == str(rebalance_date), ["symbol", "expected_return"]].rename(
        columns={"symbol": "ticker"}
    )
    work = universe.copy()
    work["ticker"] = work["ticker"].astype(str).str.upper()
    work = work.merge(selected, on="ticker", how="left", suffixes=("", "_typed"))
    typed_values = work["expected_return"].copy()
    work["expected_return"] = typed_values.fillna(0.0).astype(float)
    work["expected_return_source"] = "missing_filled_zero"
    work.loc[typed_values.notna(), "expected_return_source"] = "typed_expected_return_panel"
    if "current_weight" not in work.columns:
        work["current_weight"] = 0.0
    snapshot = work.copy()
    snapshot["date"] = str(rebalance_date)
    for column in OPTIMIZER_INPUT_SNAPSHOT_COLUMNS:
        if column not in snapshot.columns:
            snapshot[column] = None
    return (
        snapshot.loc[:, OPTIMIZER_INPUT_SNAPSHOT_COLUMNS]
        .sort_values("ticker")
        .reset_index(drop=True)
    )


def write_typed_expected_return_injection_artifacts(
    run: TypedExpectedReturnInjectionRun | TypedExpectedReturnInjectionResult,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write Phase 48 local injection artifacts."""

    run_bundle = _ensure_run_bundle(run)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    result_path = output_path / "typed_expected_return_injection_result.json"
    snapshot_path = output_path / "optimizer_input_snapshot.csv"
    panel_path = output_path / "injected_expected_return_panel.csv"
    matrix_path = output_path / "typed_q2_execution_matrix_injected.csv"
    summary_path = output_path / "typed_q2_injection_robustness_summary.json"
    manifest_path = output_path / "typed_q2_injection_manifest.json"
    trace_path = output_path / "typed_q2_injection_trace.jsonl"

    _write_json(result_path, run_bundle.result.model_dump(mode="json"))
    run_bundle.optimizer_input_snapshot.reindex(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS).to_csv(snapshot_path, index=False)
    run_bundle.injected_panel.reindex(columns=INJECTED_PANEL_COLUMNS).to_csv(panel_path, index=False)
    pd.DataFrame(run_bundle.q2_matrix_rows).to_csv(matrix_path, index=False)
    summary = TypedExpectedReturnInjectionSummary(
        run_id=run_bundle.result.run_id,
        injection_status=run_bundle.result.injection_status,
        expected_return_reached_optimizer_input=run_bundle.result.expected_return_reached_optimizer_input,
        optimizer_input_snapshot_rows=run_bundle.result.optimizer_input_snapshot_rows,
        injected_expected_return_count=run_bundle.result.injected_expected_return_count,
        q2_adapter_status=run_bundle.result.q2_adapter_status,
        q2_observed_rows=run_bundle.result.q2_observed_rows,
        q2_unavailable_rows=run_bundle.result.q2_unavailable_rows,
    )
    _write_json(summary_path, summary.model_dump(mode="json"))

    trace_events = [
        TraceEvent.create(event="typed_expected_return_injection_started", payload={"run_id": run_bundle.result.run_id}),
        TraceEvent.create(
            event="typed_expected_return_injection_completed",
            payload={
                "run_id": run_bundle.result.run_id,
                "injection_status": run_bundle.result.injection_status,
                "expected_return_reached_optimizer_input": run_bundle.result.expected_return_reached_optimizer_input,
            },
        ),
    ]
    trace_path.write_text("".join(event.model_dump_json() + "\n" for event in trace_events), encoding="utf-8")

    output_hashes = {
        "typed_expected_return_injection_result": sha256_file(result_path),
        "optimizer_input_snapshot": sha256_file(snapshot_path),
        "injected_expected_return_panel": sha256_file(panel_path),
        "typed_q2_execution_matrix_injected": sha256_file(matrix_path),
        "typed_q2_injection_robustness_summary": sha256_file(summary_path),
        "typed_q2_injection_trace": sha256_file(trace_path),
    }
    manifest_payload = {
        "schema_version": TYPED_EXPECTED_RETURN_INJECTION_MANIFEST_SCHEMA_VERSION,
        "run_id": run_bundle.result.run_id,
        "injection_status": run_bundle.result.injection_status,
        "source_config_hash": run_bundle.result.source_config_hash,
        "input_artifact_hashes": run_bundle.result.input_artifact_hashes,
        "output_artifacts": output_hashes,
        "no_live_data_confirmed": run_bundle.result.no_live_data_confirmed,
        "no_orders_confirmed": run_bundle.result.no_orders_confirmed,
        "no_broker_confirmed": run_bundle.result.no_broker_confirmed,
    }
    manifest_payload["content_hash"] = hash_payload(manifest_payload)
    manifest = TypedExpectedReturnInjectionManifest.model_validate(manifest_payload)
    _write_json(manifest_path, manifest.model_dump(mode="json"))

    return {
        "result": result_path,
        "optimizer_input_snapshot": snapshot_path,
        "injected_expected_return_panel": panel_path,
        "matrix": matrix_path,
        "summary": summary_path,
        "manifest": manifest_path,
        "trace": trace_path,
    }


def _run_q2_adapter(injection_input: TypedExpectedReturnInjectionInput):
    return run_typed_q2_adapter(
        TypedQ2AdapterInput.model_validate(
            {
                "adapter_config_path": injection_input.adapter_config_path,
                "allow_portfolioos_run": injection_input.allow_portfolioos_run,
                "expected_return_panel_path": injection_input.expected_return_panel_path,
                "local_backtest_manifest_path": injection_input.local_backtest_manifest_path,
                "no_broker": True,
                "no_network": True,
                "projection_manifest_path": injection_input.projection_manifest_path,
                "q2_input_contract_v2_path": injection_input.q2_input_contract_v2_path,
                "run_id": injection_input.run_id,
            }
        )
    )


def _load_and_transform_expected_return_panel(injection_input: TypedExpectedReturnInjectionInput) -> pd.DataFrame:
    frame = pd.read_csv(injection_input.expected_return_panel_path)
    required = {"date", "symbol", "expected_return"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError("expected_return_panel missing required columns: " + ", ".join(sorted(missing)))
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.strftime("%Y-%m-%d")
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["expected_return"] = (
        pd.to_numeric(frame["expected_return"], errors="raise").astype(float)
        * float(injection_input.expected_return_scale)
        * int(injection_input.expected_return_sign)
    )
    for column in INJECTED_PANEL_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    return frame.loc[:, INJECTED_PANEL_COLUMNS].sort_values(["date", "symbol"]).reset_index(drop=True)


def _select_rebalance_date(
    expected_return_panel: pd.DataFrame,
    *,
    schedule: list[pd.Timestamp],
    rebalance_date: str | None,
) -> str:
    schedule_dates = {pd.Timestamp(item).strftime("%Y-%m-%d") for item in schedule}
    if rebalance_date is not None:
        date_text = pd.Timestamp(rebalance_date).strftime("%Y-%m-%d")
        if date_text not in schedule_dates:
            raise ValueError(f"rebalance_date {date_text} is not in the local backtest schedule")
        return date_text
    panel_dates = sorted(set(pd.to_datetime(expected_return_panel["date"], errors="raise").dt.strftime("%Y-%m-%d")))
    for date_text in panel_dates:
        if date_text in schedule_dates:
            return date_text
    raise ValueError("expected_return_panel does not contain a date in the local backtest schedule")


def _validate_required_artifacts(injection_input: TypedExpectedReturnInjectionInput) -> list[str]:
    paths = {
        "adapter_config": injection_input.adapter_config_path,
        "expected_return_panel": injection_input.expected_return_panel_path,
        "local_backtest_manifest": injection_input.local_backtest_manifest_path,
        "projection_manifest": injection_input.projection_manifest_path,
        "q2_input_contract_v2": injection_input.q2_input_contract_v2_path,
    }
    return [f"{name} artifact is missing at {path}" for name, path in paths.items() if not path.exists()]


def _input_artifact_hashes(injection_input: TypedExpectedReturnInjectionInput) -> dict[str, str]:
    paths = {
        "adapter_config": injection_input.adapter_config_path,
        "expected_return_panel": injection_input.expected_return_panel_path,
        "local_backtest_manifest": injection_input.local_backtest_manifest_path,
        "projection_manifest": injection_input.projection_manifest_path,
        "q2_input_contract_v2": injection_input.q2_input_contract_v2_path,
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
    if denominator <= 0:
        return 0.0
    used = {
        (str(row.date), str(row.symbol).upper())
        for row in expected_return_panel.itertuples(index=False)
        if getattr(row, "expected_return", None) is not None
    }
    return float(len(used) / denominator)


def _build_run(
    *,
    injection_input: TypedExpectedReturnInjectionInput,
    result: TypedExpectedReturnInjectionResult,
    injected_panel: pd.DataFrame,
    optimizer_input_snapshot: pd.DataFrame,
    q2_matrix_rows: list[dict[str, Any]],
) -> TypedExpectedReturnInjectionRun:
    _ = injection_input
    return TypedExpectedReturnInjectionRun(
        **result.model_dump(),
        injected_panel=injected_panel,
        optimizer_input_snapshot=optimizer_input_snapshot,
        q2_matrix_rows=q2_matrix_rows,
    )


def _ensure_run_bundle(
    run: TypedExpectedReturnInjectionRun | TypedExpectedReturnInjectionResult,
) -> TypedExpectedReturnInjectionRun:
    if isinstance(run, TypedExpectedReturnInjectionRun):
        return run
    return TypedExpectedReturnInjectionRun(
        **run.model_dump(),
        injected_panel=pd.DataFrame(columns=INJECTED_PANEL_COLUMNS),
        optimizer_input_snapshot=pd.DataFrame(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS),
        q2_matrix_rows=[],
    )
