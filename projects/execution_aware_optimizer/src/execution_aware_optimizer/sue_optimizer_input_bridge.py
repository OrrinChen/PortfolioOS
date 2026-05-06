"""SUE optimizer input bridge fixture.

This explicit reopen fixture verifies that typed SUE expected returns can enter
the local PortfolioOS optimizer decision path. It is not a broker, paper canary,
live trading, or production approval workflow.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from execution_aware_optimizer.sue_expanded_typed_q2_survival import (
    generate_expanded_sue_event_rows,
    load_expanded_sue_fixture_config,
    project_expanded_sue_events,
)
from portfolio_os.alpha.optimizer_input_bridge import (
    TypedAlphaOptimizerBridgeConfig,
    TypedAlphaOptimizerBridgeRun,
    inject_typed_expected_returns_into_optimizer_universe,
    write_typed_alpha_optimizer_bridge_artifacts,
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
from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file
from portfolio_os.utils.config import AppConfig, load_app_config


SUE_OPTIMIZER_INPUT_BRIDGE_INPUT_SCHEMA_VERSION = "sue_optimizer_input_bridge_input.v1"
SUE_OPTIMIZER_INPUT_BRIDGE_ROW_SCHEMA_VERSION = "sue_optimizer_input_bridge_row.v1"
SUE_OPTIMIZER_INPUT_BRIDGE_SUMMARY_SCHEMA_VERSION = "sue_optimizer_input_bridge_summary.v1"
SUE_OPTIMIZER_INPUT_BRIDGE_RESULT_SCHEMA_VERSION = "sue_optimizer_input_bridge_result.v1"

Q2_ROW_COLUMNS = [
    "schema_version",
    "scenario_id",
    "status",
    "actual_optimizer_output",
    "adapter_hook_only",
    "optimizer_status",
    "expected_return_reached_actual_optimizer_input",
    "optimizer_decision_used_typed_expected_return",
    "rank_weight_alignment",
    "top_minus_bottom_weight_delta",
    "alpha_reward_share",
    "gross_traded_notional",
    "continuous_gross_traded_notional",
    "repair_retention",
    "turnover",
    "cost_drag",
    "expected_return_used_count",
    "active_name_count",
    "unavailable_reason",
]


class SueOptimizerInputBridgeInput(BaseModel):
    """Input contract for the explicit SUE optimizer-path bridge fixture."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_optimizer_input_bridge_input.v1"] = (
        SUE_OPTIMIZER_INPUT_BRIDGE_INPUT_SCHEMA_VERSION
    )
    run_id: str
    fixture_config_path: Path
    local_backtest_manifest_path: Path
    allow_typed_alpha_optimizer_injection: bool = False
    local_rebalance_date: str | None = None
    no_network: bool = True
    no_broker: bool = True

    @model_validator(mode="after")
    def require_local_safety_flags(self) -> "SueOptimizerInputBridgeInput":
        if not self.no_network:
            raise ValueError("SueOptimizerInputBridgeInput requires no_network=true")
        if not self.no_broker:
            raise ValueError("SueOptimizerInputBridgeInput requires no_broker=true")
        return self


class SueOptimizerInputBridgeRow(BaseModel):
    """One actual local optimizer output diagnostic row."""

    schema_version: Literal["sue_optimizer_input_bridge_row.v1"] = SUE_OPTIMIZER_INPUT_BRIDGE_ROW_SCHEMA_VERSION
    scenario_id: str
    status: Literal["observed", "unavailable", "rejected"]
    actual_optimizer_output: bool
    adapter_hook_only: bool = False
    optimizer_status: str
    expected_return_reached_actual_optimizer_input: bool
    optimizer_decision_used_typed_expected_return: bool
    rank_weight_alignment: float
    top_minus_bottom_weight_delta: float
    alpha_reward_share: float
    gross_traded_notional: float
    continuous_gross_traded_notional: float
    repair_retention: float | None
    turnover: float | None
    cost_drag: float | None
    expected_return_used_count: int
    active_name_count: int
    unavailable_reason: str | None = None


class SueOptimizerInputBridgeSummary(BaseModel):
    """Compact optimizer bridge summary for SUE."""

    schema_version: Literal["sue_optimizer_input_bridge_summary.v1"] = (
        SUE_OPTIMIZER_INPUT_BRIDGE_SUMMARY_SCHEMA_VERSION
    )
    run_id: str
    bridge_status: Literal["observed", "unavailable", "rejected"]
    expected_return_reached_actual_optimizer_input: bool
    optimizer_decision_used_typed_expected_return: bool
    sue_rank_weight_alignment_observed: bool
    sign_flip_reversal_observed: bool
    scaled_alpha_monotonicity_observed: bool
    no_view_not_encoded_as_zero: bool
    actual_optimizer_output_rows: int
    adapter_hook_only: bool = False
    production_approval_claimed: bool = False


class SueOptimizerInputBridgeResult(BaseModel):
    """Top-level result for the SUE optimizer input bridge fixture."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_optimizer_input_bridge_result.v1"] = (
        SUE_OPTIMIZER_INPUT_BRIDGE_RESULT_SCHEMA_VERSION
    )
    run_id: str
    bridge_status: Literal["observed", "unavailable", "rejected"]
    expected_return_reached_actual_optimizer_input: bool
    optimizer_decision_used_typed_expected_return: bool
    sue_rank_weight_alignment_observed: bool
    sign_flip_reversal_observed: bool
    scaled_alpha_monotonicity_observed: bool
    no_view_not_encoded_as_zero: bool
    actual_optimizer_output_rows: int
    q2_rows: list[SueOptimizerInputBridgeRow] = Field(default_factory=list)
    rejection_reasons: list[str] = Field(default_factory=list)
    unavailable_reason: str | None = None
    source_config_hash: str
    input_artifact_hashes: dict[str, str] = Field(default_factory=dict)
    adapter_hook_only: bool = False
    production_approval_claimed: bool = False
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True

    @property
    def summary(self) -> SueOptimizerInputBridgeSummary:
        return SueOptimizerInputBridgeSummary(
            run_id=self.run_id,
            bridge_status=self.bridge_status,
            expected_return_reached_actual_optimizer_input=self.expected_return_reached_actual_optimizer_input,
            optimizer_decision_used_typed_expected_return=self.optimizer_decision_used_typed_expected_return,
            sue_rank_weight_alignment_observed=self.sue_rank_weight_alignment_observed,
            sign_flip_reversal_observed=self.sign_flip_reversal_observed,
            scaled_alpha_monotonicity_observed=self.scaled_alpha_monotonicity_observed,
            no_view_not_encoded_as_zero=self.no_view_not_encoded_as_zero,
            actual_optimizer_output_rows=self.actual_optimizer_output_rows,
            adapter_hook_only=self.adapter_hook_only,
            production_approval_claimed=self.production_approval_claimed,
        )


class SueOptimizerInputBridgeRun(SueOptimizerInputBridgeResult):
    """SUE result plus writeable local artifacts."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    optimizer_input_with_typed_alpha: Any = Field(default=None, exclude=True)
    bridge_manifest: dict[str, Any] = Field(default_factory=dict, exclude=True)
    bridge_coverage_report: dict[str, Any] = Field(default_factory=dict, exclude=True)
    response_diagnostics: dict[str, Any] = Field(default_factory=dict, exclude=True)
    trace_events: list[TraceEvent] = Field(default_factory=list, exclude=True)


def run_sue_optimizer_input_bridge_fixture(
    bridge_input: SueOptimizerInputBridgeInput,
) -> SueOptimizerInputBridgeRun:
    """Run the expanded SUE fixture through the actual local optimizer path."""

    input_hashes = _input_artifact_hashes(bridge_input)
    source_config_hash = hash_payload(
        {
            "bridge_input": bridge_input.model_dump(mode="json"),
            "input_artifact_hashes": input_hashes,
        }
    )
    missing = _validate_required_artifacts(bridge_input)
    if missing:
        return _result(
            bridge_input=bridge_input,
            bridge_status="rejected",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rows=[],
            rejection_reasons=missing,
        )
    if not bridge_input.allow_typed_alpha_optimizer_injection:
        return _result(
            bridge_input=bridge_input,
            bridge_status="unavailable",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rows=[],
            unavailable_reason="Typed alpha optimizer injection disabled; no optimizer decision path was evaluated.",
        )

    try:
        fixture_config = load_expanded_sue_fixture_config(bridge_input.fixture_config_path)
        event_rows = generate_expanded_sue_event_rows(fixture_config)
        projection = project_expanded_sue_events(event_rows, fixture_config)
        panel, abstain_report, projection_manifest, q2_contract = _local_representative_artifacts(
            projection=projection,
            local_rebalance_date=bridge_input.local_rebalance_date,
            event_rows=event_rows,
        )
        universe, config, selected_date = _build_local_optimizer_context(
            manifest_path=bridge_input.local_backtest_manifest_path,
            expected_return_panel=panel,
            rebalance_date=bridge_input.local_rebalance_date,
        )
        variants = _build_sue_panel_variants(
            base_panel=panel,
            universe=universe,
            rebalance_date=selected_date,
        )
        evaluations = [
            _evaluate_variant(
                universe=universe,
                config=config,
                panel=variant["panel"],
                scenario_id=variant["scenario_id"],
                projection_manifest=projection_manifest,
                q2_contract=q2_contract,
                abstain_report=abstain_report,
                rebalance_date=selected_date,
            )
            for variant in variants
        ]
    except Exception as exc:  # noqa: BLE001 - deterministic unavailable wrapper
        return _result(
            bridge_input=bridge_input,
            bridge_status="unavailable",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rows=[],
            unavailable_reason=f"SUE optimizer input bridge unavailable: {exc}",
        )

    rows = [row for row, _bridge_run, _rebalance in evaluations]
    bridge_runs = {row.scenario_id: bridge_run for row, bridge_run, _rebalance in evaluations}
    base_bridge = bridge_runs.get("sue_positive_panel")
    diagnostics = _response_diagnostics(rows, bridge_runs)
    return _result(
        bridge_input=bridge_input,
        bridge_status="observed" if rows else "unavailable",
        source_config_hash=source_config_hash,
        input_hashes=input_hashes,
        rows=rows,
        optimizer_input=(
            base_bridge.optimizer_input_with_typed_alpha.copy()
            if base_bridge is not None
            else pd.DataFrame()
        ),
        bridge_manifest={
            "projection_manifest_hash": projection_manifest.get("content_hash"),
            "source_config_hash": source_config_hash,
        },
        bridge_coverage_report=base_bridge.coverage_report if base_bridge is not None else {},
        response_diagnostics=diagnostics,
    )


def write_sue_optimizer_input_bridge_artifacts(
    result: SueOptimizerInputBridgeRun | SueOptimizerInputBridgeResult,
    output_dir: str | Path,
    *,
    report_path: str | Path | None = None,
) -> dict[str, Path]:
    """Write SUE optimizer bridge artifacts."""

    run = _ensure_run(result)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_destination = (
        Path(report_path)
        if report_path is not None
        else output_path / "sue_optimizer_input_bridge_report.md"
    )
    report_destination.parent.mkdir(parents=True, exist_ok=True)

    generic_run = TypedAlphaOptimizerBridgeRun(
        run_id=run.run_id,
        bridge_status="injected" if run.bridge_status == "observed" else "disabled",
        expected_return_reached_actual_optimizer_input=run.expected_return_reached_actual_optimizer_input,
        optimizer_decision_used_typed_expected_return=run.optimizer_decision_used_typed_expected_return,
        source_config_hash=run.source_config_hash,
        projection_manifest_hash=str(run.bridge_manifest.get("projection_manifest_hash") or ""),
        optimizer_input_with_typed_alpha=_optimizer_input_frame(run),
        coverage_report=run.bridge_coverage_report,
    )
    bridge_artifacts = write_typed_alpha_optimizer_bridge_artifacts(generic_run, output_path)

    summary_path = output_path / "sue_optimizer_input_bridge_summary.json"
    diagnostics_path = output_path / "typed_alpha_optimizer_response_diagnostics.json"
    q2_rows_path = output_path / "typed_alpha_optimizer_q2_survival_rows.csv"
    result_path = output_path / "sue_optimizer_input_bridge_result.json"
    trace_path = output_path / "sue_optimizer_input_bridge_trace.jsonl"

    _write_json(summary_path, run.summary.model_dump(mode="json"))
    _write_json(diagnostics_path, run.response_diagnostics)
    _write_json(result_path, run.model_dump(mode="json"))
    pd.DataFrame([row.model_dump(mode="json") for row in run.q2_rows]).reindex(columns=Q2_ROW_COLUMNS).to_csv(
        q2_rows_path,
        index=False,
    )
    report_destination.write_text(render_sue_optimizer_input_bridge_report(run), encoding="utf-8")
    trace_path.write_text("".join(event.model_dump_json() + "\n" for event in run.trace_events), encoding="utf-8")
    return {
        **bridge_artifacts,
        "summary": summary_path,
        "response_diagnostics": diagnostics_path,
        "q2_rows": q2_rows_path,
        "result": result_path,
        "trace": trace_path,
        "report": report_destination,
    }


def render_sue_optimizer_input_bridge_report(run: SueOptimizerInputBridgeResult) -> str:
    """Render the SUE optimizer input bridge report."""

    return "\n".join(
        [
            "# SUE Optimizer Input Bridge Report",
            "",
            "This proves local optimizer-path integration only.",
            "This does not prove real historical SUE alpha.",
            "This does not prove paper readiness or production approval.",
            "This does not create broker/order/live workflows.",
            "Q2 rows in this report are based on actual local optimizer outputs, not adapter-hook mapping.",
            "",
            "## Summary",
            "",
            f"- bridge_status: `{run.bridge_status}`",
            f"- expected_return_reached_actual_optimizer_input: `{str(run.expected_return_reached_actual_optimizer_input).lower()}`",
            f"- optimizer_decision_used_typed_expected_return: `{str(run.optimizer_decision_used_typed_expected_return).lower()}`",
            f"- sue_rank_weight_alignment_observed: `{str(run.sue_rank_weight_alignment_observed).lower()}`",
            f"- sign_flip_reversal_observed: `{str(run.sign_flip_reversal_observed).lower()}`",
            f"- scaled_alpha_monotonicity_observed: `{str(run.scaled_alpha_monotonicity_observed).lower()}`",
            f"- no_view_not_encoded_as_zero: `{str(run.no_view_not_encoded_as_zero).lower()}`",
            f"- actual_optimizer_output_rows: `{run.actual_optimizer_output_rows}`",
            f"- adapter_hook_only: `{str(run.adapter_hook_only).lower()}`",
            f"- production_approval_claimed: `{str(run.production_approval_claimed).lower()}`",
            "",
            "The unscaled deterministic SUE row can still be dominated by existing risk, target, cost, and repair constraints. Directional optimizer response is therefore reported through the local sign-flip and scale-grid diagnostics, not as a real SUE alpha success claim.",
            "",
            "## Boundaries",
            "",
            "- no live data workflow was added",
            "- no broker workflow was added",
            "- no order workflow was added",
            "- no paper canary was approved",
            "- no production alpha approval is claimed",
            "",
        ]
    )


def _evaluate_variant(
    *,
    universe: pd.DataFrame,
    config: AppConfig,
    panel: pd.DataFrame,
    scenario_id: str,
    projection_manifest: dict[str, Any],
    q2_contract: dict[str, Any],
    abstain_report: dict[str, Any],
    rebalance_date: str,
) -> tuple[SueOptimizerInputBridgeRow, TypedAlphaOptimizerBridgeRun, Any]:
    bridge_run = inject_typed_expected_returns_into_optimizer_universe(
        universe=universe,
        expected_return_panel=panel,
        projection_manifest=projection_manifest,
        q2_input_contract=q2_contract,
        alpha_abstain_report=abstain_report,
        rebalance_date=rebalance_date,
        config=TypedAlphaOptimizerBridgeConfig(allow_typed_alpha_optimizer_injection=True),
        run_id=f"sue-optimizer-input-bridge-{scenario_id}",
    )
    if bridge_run.bridge_status != "injected":
        row = SueOptimizerInputBridgeRow(
            scenario_id=scenario_id,
            status="unavailable",
            actual_optimizer_output=False,
            optimizer_status=bridge_run.bridge_status,
            expected_return_reached_actual_optimizer_input=False,
            optimizer_decision_used_typed_expected_return=False,
            rank_weight_alignment=0.0,
            top_minus_bottom_weight_delta=0.0,
            alpha_reward_share=0.0,
            gross_traded_notional=0.0,
            continuous_gross_traded_notional=0.0,
            repair_retention=None,
            turnover=None,
            cost_drag=None,
            expected_return_used_count=0,
            active_name_count=0,
            unavailable_reason="typed optimizer bridge did not inject",
        )
        return row, bridge_run, None

    work = bridge_run.optimizer_input_with_typed_alpha.copy()
    rebalance_run = run_rebalance(work, config)
    optimization_result = rebalance_run.optimization_result
    current_weights = pd.Series(optimization_result.current_weights, dtype=float).sort_index()
    post_trade_weights = pd.Series(optimization_result.post_trade_weights, dtype=float).reindex(
        current_weights.index
    ).fillna(0.0)
    weight_change = post_trade_weights - current_weights
    diagnostic_axis = _diagnostic_axis(work, current_weights.index)
    continuous_gross = float(optimization_result.gross_traded_notional)
    repaired_gross = float(rebalance_run.basket.gross_traded_notional)
    pre_trade_nav = float(optimization_result.pre_trade_nav)
    total_cost = float(rebalance_run.basket.total_fee + rebalance_run.basket.total_slippage)
    row = SueOptimizerInputBridgeRow(
        scenario_id=scenario_id,
        status="observed",
        actual_optimizer_output=True,
        adapter_hook_only=False,
        optimizer_status=str(optimization_result.status),
        expected_return_reached_actual_optimizer_input=bridge_run.expected_return_reached_actual_optimizer_input,
        optimizer_decision_used_typed_expected_return=bool(bridge_run.expected_return_used_count > 0),
        rank_weight_alignment=_safe_spearman(diagnostic_axis, weight_change),
        top_minus_bottom_weight_delta=_top_minus_bottom_weight_delta(diagnostic_axis, weight_change),
        alpha_reward_share=_alpha_reward_contribution(optimization_result.objective_decomposition),
        gross_traded_notional=repaired_gross,
        continuous_gross_traded_notional=continuous_gross,
        repair_retention=float(repaired_gross / continuous_gross) if continuous_gross > 0.0 else 0.0,
        turnover=float(repaired_gross / pre_trade_nav) if pre_trade_nav > 0.0 else None,
        cost_drag=float(total_cost / pre_trade_nav) if pre_trade_nav > 0.0 else None,
        expected_return_used_count=bridge_run.expected_return_used_count,
        active_name_count=bridge_run.active_name_count,
    )
    return row, bridge_run, rebalance_run


def _local_representative_artifacts(
    *,
    projection: Any,
    local_rebalance_date: str | None,
    event_rows: list[Any],
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    frame = pd.DataFrame(projection.expected_return_panel)
    if frame.empty:
        raise ValueError("expanded SUE projection produced no expected-return rows")
    first_date = sorted(frame["date"].astype(str).unique())[0]
    local_date = pd.Timestamp(local_rebalance_date or first_date).strftime("%Y-%m-%d")
    panel = frame.loc[frame["date"].astype(str) == first_date].copy()
    panel["date"] = local_date
    panel["diagnostic_score"] = pd.to_numeric(panel["expected_return"], errors="raise").astype(float)
    local_ts = pd.Timestamp(local_date)
    panel["event_timestamp"] = datetime.combine(
        (local_ts - pd.Timedelta(days=2)).date(),
        time(21, 5),
        tzinfo=timezone.utc,
    ).isoformat().replace("+00:00", "Z")
    panel["event_available_timestamp"] = datetime.combine(
        (local_ts - pd.Timedelta(days=2)).date(),
        time(21, 15),
        tzinfo=timezone.utc,
    ).isoformat().replace("+00:00", "Z")
    panel["tradable_timestamp"] = datetime.combine(
        local_ts.date(),
        time(14, 30),
        tzinfo=timezone.utc,
    ).isoformat().replace("+00:00", "Z")
    panel["projection_policy"] = "event_window_decay"

    abstain_rows = []
    first_abstain_date = first_date
    for row in projection.alpha_abstain_report:
        if str(row.get("date")) == first_abstain_date:
            copied = dict(row)
            copied["date"] = local_date
            abstain_rows.append(copied)
    projection_manifest = dict(projection.alpha_projection_manifest)
    projection_manifest["rebalance_dates"] = [local_date]
    projection_manifest["panel_row_count"] = int(len(panel))
    projection_manifest.pop("content_hash", None)
    projection_manifest["content_hash"] = hash_payload(projection_manifest)
    q2_contract = {
        "schema_version": "q2_input_contract.v2",
        "alpha_view_id": list(projection_manifest.get("alpha_view_ids", ["AV-US-SUE-EXPANDED"]))[0],
        "projection_manifest_hash": projection_manifest["content_hash"],
        "allowed_consumer": "projects/execution_aware_optimizer",
        "direct_q2_execution_allowed": False,
    }
    _ = event_rows
    return panel, {"abstain_report": abstain_rows}, projection_manifest, q2_contract


def _build_sue_panel_variants(
    *,
    base_panel: pd.DataFrame,
    universe: pd.DataFrame,
    rebalance_date: str,
) -> list[dict[str, Any]]:
    variants = [
        ("sue_positive_panel", 1.0, 1, "active_view"),
        ("sue_scaled_0_5x_panel", 0.5, 1, "active_view"),
        ("sue_scaled_1_0x_panel", 1.0, 1, "active_view"),
        ("sue_scaled_2_0x_panel", 2.0, 1, "active_view"),
        ("sue_sign_flipped_panel", 1.0, -1, "active_view"),
        ("sue_zero_panel", 0.0, 1, "active_view"),
        ("sue_abstain_panel", 0.0, 1, "no_view"),
    ]
    outputs: list[dict[str, Any]] = []
    for scenario_id, scale, sign, view_state in variants:
        if view_state == "no_view":
            panel = base_panel.iloc[0:0].copy()
        elif scenario_id == "sue_zero_panel":
            panel = base_panel.copy()
            panel["expected_return"] = 0.0
        else:
            panel = base_panel.copy()
            panel["expected_return"] = (
                pd.to_numeric(panel["expected_return"], errors="raise").astype(float) * float(scale) * int(sign)
            )
        if "diagnostic_score" not in panel.columns:
            panel["diagnostic_score"] = pd.to_numeric(panel.get("expected_return", 0.0), errors="coerce").fillna(0.0)
        panel["date"] = str(rebalance_date)
        outputs.append({"scenario_id": scenario_id, "panel": panel})
    _ = universe
    return outputs


def _build_local_optimizer_context(
    *,
    manifest_path: str | Path,
    expected_return_panel: pd.DataFrame,
    rebalance_date: str | None,
) -> tuple[pd.DataFrame, AppConfig, str]:
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
    universe, config = _build_strategy_universe(
        current_date=pd.Timestamp(selected_date),
        quantities=initial_quantities,
        cash=float(initial_state.available_cash),
        targets=targets,
        base_market_frame=base_market_frame,
        base_reference_frame=base_reference_frame,
        price_row=price_row,
        app_config_template=app_config,
    )
    return universe, config, selected_date


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


def _diagnostic_axis(work: pd.DataFrame, index: pd.Index) -> pd.Series:
    source_column = "diagnostic_score" if "diagnostic_score" in work.columns else "typed_alpha_expected_return"
    return (
        work.set_index("ticker")[source_column]
        .pipe(pd.to_numeric, errors="coerce")
        .reindex(index)
        .fillna(0.0)
    )


def _top_minus_bottom_weight_delta(expected_return: pd.Series, weight_change: pd.Series) -> float:
    if expected_return.nunique() <= 1:
        return 0.0
    rank_pct = expected_return.rank(method="first", pct=True)
    top_mask = rank_pct >= 0.8
    bottom_mask = rank_pct <= 0.2
    return float(weight_change.loc[top_mask].mean() - weight_change.loc[bottom_mask].mean())


def _safe_spearman(left: pd.Series, right: pd.Series) -> float:
    pair = pd.concat([left.rename("left"), right.rename("right")], axis=1).dropna()
    if len(pair) < 2 or pair["left"].nunique() <= 1 or pair["right"].nunique() <= 1:
        return 0.0
    value = float(pair["left"].corr(pair["right"], method="spearman"))
    return value if np.isfinite(value) else 0.0


def _alpha_reward_contribution(objective_decomposition: dict[str, Any]) -> float:
    alpha_component = objective_decomposition.get("components", {}).get("alpha_reward", {})
    if "weighted_value" in alpha_component:
        return float(abs(alpha_component.get("weighted_value") or 0.0))
    if "raw_value" in alpha_component:
        return float(abs(alpha_component.get("raw_value") or 0.0))
    return float(abs(alpha_component.get("share_abs_weighted", 0.0)))


def _response_diagnostics(
    rows: list[SueOptimizerInputBridgeRow],
    bridge_runs: dict[str, TypedAlphaOptimizerBridgeRun],
) -> dict[str, Any]:
    by_id = {row.scenario_id: row for row in rows}
    scaled = [
        by_id[name].alpha_reward_share
        for name in ("sue_scaled_0_5x_panel", "sue_scaled_1_0x_panel", "sue_scaled_2_0x_panel")
        if name in by_id
    ]
    positive_candidates = [
        by_id[name]
        for name in ("sue_positive_panel", "sue_scaled_1_0x_panel", "sue_scaled_2_0x_panel")
        if name in by_id
    ]
    sign_flipped = by_id.get("sue_sign_flipped_panel")
    no_view_bridge = bridge_runs.get("sue_abstain_panel")
    return {
        "schema_version": "sue_optimizer_input_bridge_diagnostics.v1",
        "sue_rank_weight_alignment_observed": bool(
            any(row.rank_weight_alignment > 0.0 and row.top_minus_bottom_weight_delta > 0.0 for row in positive_candidates)
        ),
        "sign_flip_reversal_observed": bool(
            sign_flipped is not None
            and sign_flipped.rank_weight_alignment < 0.0
            and sign_flipped.top_minus_bottom_weight_delta < 0.0
        ),
        "scaled_alpha_monotonicity_observed": bool(
            len(scaled) == 3
            and all(later >= earlier - 1e-12 for earlier, later in zip(scaled, scaled[1:], strict=False))
            and scaled[0] < scaled[-1]
        ),
        "no_view_not_encoded_as_zero": bool(
            no_view_bridge is not None
            and no_view_bridge.coverage_report.get("no_view_not_encoded_as_zero") is True
        ),
        "actual_optimizer_output_rows": sum(1 for row in rows if row.actual_optimizer_output),
        "adapter_hook_only": False,
        "production_approval_claimed": False,
    }


def _result(
    *,
    bridge_input: SueOptimizerInputBridgeInput,
    bridge_status: Literal["observed", "unavailable", "rejected"],
    source_config_hash: str,
    input_hashes: dict[str, str],
    rows: list[SueOptimizerInputBridgeRow],
    optimizer_input: pd.DataFrame | None = None,
    bridge_manifest: dict[str, Any] | None = None,
    bridge_coverage_report: dict[str, Any] | None = None,
    response_diagnostics: dict[str, Any] | None = None,
    rejection_reasons: list[str] | None = None,
    unavailable_reason: str | None = None,
) -> SueOptimizerInputBridgeRun:
    diagnostics = response_diagnostics or {}
    actual_rows = sum(1 for row in rows if row.actual_optimizer_output)
    expected_reached = bool(any(row.expected_return_reached_actual_optimizer_input for row in rows))
    decision_used = bool(any(row.optimizer_decision_used_typed_expected_return for row in rows))
    trace_events = [
        TraceEvent.create(event="sue_optimizer_input_bridge_started", payload={"run_id": bridge_input.run_id}),
        TraceEvent.create(
            event="sue_optimizer_input_bridge_completed",
            payload={
                "run_id": bridge_input.run_id,
                "bridge_status": bridge_status,
                "actual_optimizer_output_rows": actual_rows,
            },
        ),
    ]
    return SueOptimizerInputBridgeRun(
        run_id=bridge_input.run_id,
        bridge_status=bridge_status,
        expected_return_reached_actual_optimizer_input=expected_reached,
        optimizer_decision_used_typed_expected_return=decision_used,
        sue_rank_weight_alignment_observed=bool(diagnostics.get("sue_rank_weight_alignment_observed", False)),
        sign_flip_reversal_observed=bool(diagnostics.get("sign_flip_reversal_observed", False)),
        scaled_alpha_monotonicity_observed=bool(diagnostics.get("scaled_alpha_monotonicity_observed", False)),
        no_view_not_encoded_as_zero=bool(diagnostics.get("no_view_not_encoded_as_zero", False)),
        actual_optimizer_output_rows=actual_rows,
        q2_rows=rows,
        rejection_reasons=rejection_reasons or [],
        unavailable_reason=unavailable_reason,
        source_config_hash=source_config_hash,
        input_artifact_hashes=input_hashes,
        optimizer_input_with_typed_alpha=optimizer_input if optimizer_input is not None else pd.DataFrame(),
        bridge_manifest=bridge_manifest or {},
        bridge_coverage_report=bridge_coverage_report or {},
        response_diagnostics=diagnostics,
        trace_events=trace_events,
    )


def _ensure_run(result: SueOptimizerInputBridgeRun | SueOptimizerInputBridgeResult) -> SueOptimizerInputBridgeRun:
    if isinstance(result, SueOptimizerInputBridgeRun):
        return result
    return SueOptimizerInputBridgeRun(
        **result.model_dump(),
        optimizer_input_with_typed_alpha=pd.DataFrame(),
        bridge_manifest={},
        bridge_coverage_report={},
        response_diagnostics={},
        trace_events=[],
    )


def _optimizer_input_frame(run: SueOptimizerInputBridgeRun) -> pd.DataFrame:
    if isinstance(run.optimizer_input_with_typed_alpha, pd.DataFrame):
        return run.optimizer_input_with_typed_alpha.copy()
    return pd.DataFrame()


def _validate_required_artifacts(bridge_input: SueOptimizerInputBridgeInput) -> list[str]:
    paths = {
        "fixture_config": bridge_input.fixture_config_path,
        "local_backtest_manifest": bridge_input.local_backtest_manifest_path,
    }
    return [f"{name} artifact is missing at {path}" for name, path in sorted(paths.items()) if not path.exists()]


def _input_artifact_hashes(bridge_input: SueOptimizerInputBridgeInput) -> dict[str, str]:
    paths = {
        "fixture_config": bridge_input.fixture_config_path,
        "local_backtest_manifest": bridge_input.local_backtest_manifest_path,
    }
    return {name: sha256_file(path) if path.exists() else "missing" for name, path in sorted(paths.items())}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
