"""Typed expected-return optimizer response acceptance suite.

This module evaluates deterministic typed expected-return panel variants
against a local PortfolioOS optimizer fixture. It writes only aggregate
diagnostics: no orders, broker payloads, live data, or production approval.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from execution_aware_optimizer.typed_execution_matrix import TypedQ2InputContractV2
from execution_aware_optimizer.typed_optimizer_response_schema import (
    TypedOptimizerResponseInput,
    TypedOptimizerResponseResult,
    TypedOptimizerResponseRow,
    TypedOptimizerResponseSummary,
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
from portfolio_os.optimizer.acceptance_proof import build_deterministic_synthetic_alpha_frame
from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file
from portfolio_os.utils.config import AppConfig, load_app_config


RESPONSE_GRID_COLUMNS = [
    "schema_version",
    "panel_name",
    "view_state",
    "expected_return_scale",
    "expected_return_sign",
    "optimizer_status",
    "alpha_reward_share",
    "rank_alignment",
    "top_minus_bottom_weight_delta",
    "gross_traded_notional",
    "continuous_gross_traded_notional",
    "repair_retention",
    "expected_return_used_share",
    "active_name_count",
    "zero_alpha_distinct_from_no_view",
]


def run_typed_optimizer_response_acceptance(
    response_input: TypedOptimizerResponseInput,
) -> TypedOptimizerResponseResult:
    """Run Phase 49 optimizer response diagnostics on local typed panels."""

    input_hashes = _input_artifact_hashes(response_input)
    source_config_hash = hash_payload(
        {
            "response_input": response_input.model_dump(mode="json"),
            "input_artifact_hashes": input_hashes,
        }
    )
    missing = _validate_required_artifacts(response_input)
    if missing:
        return _result(
            response_input=response_input,
            response_status="rejected",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rows=[],
            rejection_reasons=missing,
        )

    try:
        contract = TypedQ2InputContractV2.model_validate(_read_json(response_input.q2_input_contract_v2_path))
        projection_manifest = _read_json(response_input.projection_manifest_path)
        _validate_projection_manifest(contract, projection_manifest)
        expected_panel = _load_expected_return_panel(response_input.expected_return_panel_path)
    except Exception as exc:  # noqa: BLE001 - deterministic rejection wrapper
        return _result(
            response_input=response_input,
            response_status="rejected",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rows=[],
            rejection_reasons=[f"typed optimizer response input validation failed: {exc}"],
        )

    if not response_input.allow_portfolioos_run:
        return _result(
            response_input=response_input,
            response_status="unavailable",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rows=[],
            unavailable_reason="PortfolioOS run disabled; optimizer response suite was not evaluated.",
        )

    try:
        universe, config, rebalance_date = _build_local_optimizer_context(
            manifest_path=response_input.local_backtest_manifest_path,
            expected_return_panel=expected_panel,
            rebalance_date=response_input.rebalance_date,
        )
        panels = _build_response_panels(
            universe=universe,
            rebalance_date=rebalance_date,
            base_expected_return_unit=response_input.base_expected_return_unit,
        )
        rows = [
            _evaluate_response_panel(universe=universe, config=config, panel=panel)
            for panel in panels
        ]
    except Exception as exc:  # noqa: BLE001 - deterministic unavailable wrapper
        return _result(
            response_input=response_input,
            response_status="unavailable",
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rows=[],
            unavailable_reason=f"typed optimizer response unavailable: {exc}",
        )

    return _result(
        response_input=response_input,
        response_status="observed",
        source_config_hash=source_config_hash,
        input_hashes=input_hashes,
        rows=rows,
    )


def write_typed_optimizer_response_artifacts(
    result: TypedOptimizerResponseResult,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write Phase 49 optimizer response artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    summary_path = output_path / "optimizer_response_summary.json"
    grid_path = output_path / "optimizer_response_grid.csv"
    sign_flip_path = output_path / "sign_flip_diagnostics.json"
    abstain_path = output_path / "abstain_vs_zero_report.json"

    _write_json(summary_path, result.summary.model_dump(mode="json"))
    pd.DataFrame([row.model_dump(mode="json") for row in result.response_rows]).reindex(
        columns=RESPONSE_GRID_COLUMNS
    ).to_csv(grid_path, index=False)
    _write_json(sign_flip_path, _build_sign_flip_diagnostics(result))
    _write_json(abstain_path, _build_abstain_vs_zero_report(result))

    return {
        "summary": summary_path,
        "grid": grid_path,
        "sign_flip": sign_flip_path,
        "abstain_vs_zero": abstain_path,
    }


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
    app_config = _alpha_response_config(app_config)

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


def _alpha_response_config(config: AppConfig) -> AppConfig:
    objective = config.objective_weights.model_copy(
        update={
            "alpha_weight": 1.0,
            "risk_term": 0.0,
            "tracking_error": 0.0,
            "transaction_cost": 0.0,
            "target_deviation": 0.0,
            "transaction_fee": 0.0,
            "turnover_penalty": 0.0,
            "slippage_penalty": 0.0,
        }
    )
    risk_model = config.risk_model.model_copy(update={"enabled": False})
    return config.model_copy(update={"objective_weights": objective, "risk_model": risk_model})


def _build_response_panels(
    *,
    universe: pd.DataFrame,
    rebalance_date: str,
    base_expected_return_unit: float,
) -> list[dict[str, Any]]:
    scores = build_deterministic_synthetic_alpha_frame(universe)
    scores["date"] = str(rebalance_date)
    variants = [
        ("positive_panel", "active_view", 1.0, 1),
        ("scaled_0_5x_panel", "active_view", 0.5, 1),
        ("scaled_1_0x_panel", "active_view", 1.0, 1),
        ("scaled_2_0x_panel", "active_view", 2.0, 1),
        ("sign_flipped_panel", "active_view", 1.0, -1),
        ("zero_panel", "zero_alpha", 0.0, 1),
        ("abstain_panel", "no_view", 0.0, 1),
    ]
    panels = []
    for name, view_state, scale, sign in variants:
        panel = scores.loc[:, ["date", "ticker", "synthetic_alpha_score"]].rename(columns={"ticker": "symbol"})
        if view_state == "no_view":
            panel = panel.iloc[0:0].copy()
            panel["expected_return"] = pd.Series(dtype=float)
        elif view_state == "zero_alpha":
            panel = panel.copy()
            panel["expected_return"] = 0.0
        else:
            panel = panel.copy()
            panel["expected_return"] = (
                float(base_expected_return_unit)
                * float(scale)
                * int(sign)
                * panel["synthetic_alpha_score"].astype(float)
            )
        panel["diagnostic_score"] = panel.get("synthetic_alpha_score", 0.0)
        panels.append(
            {
                "panel_name": name,
                "view_state": view_state,
                "expected_return_scale": float(scale),
                "expected_return_sign": int(sign),
                "panel": panel.loc[:, ["date", "symbol", "expected_return", "diagnostic_score"]],
            }
        )
    return panels


def _evaluate_response_panel(
    *,
    universe: pd.DataFrame,
    config: AppConfig,
    panel: dict[str, Any],
) -> TypedOptimizerResponseRow:
    work = _merge_expected_return_panel(universe, panel["panel"], view_state=panel["view_state"])
    rebalance_run = run_rebalance(work, config)
    optimization_result = rebalance_run.optimization_result

    current_weights = pd.Series(optimization_result.current_weights, dtype=float).sort_index()
    post_trade_weights = pd.Series(optimization_result.post_trade_weights, dtype=float).reindex(
        current_weights.index
    ).fillna(0.0)
    expected_return = (
        work.set_index("ticker")["expected_return"]
        .astype(float)
        .reindex(current_weights.index)
        .fillna(0.0)
    )
    diagnostic_axis = _diagnostic_axis(work, panel["view_state"], current_weights.index)
    weight_change = post_trade_weights - current_weights
    continuous_gross = float(optimization_result.gross_traded_notional)
    repaired_gross = float(rebalance_run.basket.gross_traded_notional)
    used_share = _expected_return_used_share_for_panel(universe, panel["panel"], panel["view_state"])
    return TypedOptimizerResponseRow(
        panel_name=str(panel["panel_name"]),
        view_state=panel["view_state"],
        expected_return_scale=float(panel["expected_return_scale"]),
        expected_return_sign=int(panel["expected_return_sign"]),
        optimizer_status=str(optimization_result.status),
        alpha_reward_share=_alpha_reward_contribution(optimization_result.objective_decomposition),
        rank_alignment=_safe_spearman(diagnostic_axis, weight_change),
        top_minus_bottom_weight_delta=_top_minus_bottom_weight_delta(diagnostic_axis, weight_change),
        gross_traded_notional=repaired_gross,
        continuous_gross_traded_notional=continuous_gross,
        repair_retention=float(repaired_gross / continuous_gross) if continuous_gross > 0.0 else 0.0,
        expected_return_used_share=used_share,
        active_name_count=int(_active_name_count(panel["panel"])),
        zero_alpha_distinct_from_no_view=str(panel["view_state"]) == "no_view",
    )


def _merge_expected_return_panel(
    universe: pd.DataFrame,
    panel: pd.DataFrame,
    *,
    view_state: str,
) -> pd.DataFrame:
    work = universe.copy()
    work["ticker"] = work["ticker"].astype(str).str.upper()
    if view_state == "no_view":
        work["expected_return"] = 0.0
        return work
    expected = panel.copy()
    expected["symbol"] = expected["symbol"].astype(str).str.upper()
    expected["expected_return"] = pd.to_numeric(expected["expected_return"], errors="raise").astype(float)
    selected_columns = ["symbol", "expected_return"]
    if "diagnostic_score" in expected.columns:
        expected["diagnostic_score"] = pd.to_numeric(expected["diagnostic_score"], errors="coerce").astype(float)
        selected_columns.append("diagnostic_score")
    selected = expected.loc[:, selected_columns].rename(columns={"symbol": "ticker"})
    work = work.merge(selected, on="ticker", how="left")
    if work["expected_return"].isna().any():
        missing = sorted(work.loc[work["expected_return"].isna(), "ticker"].astype(str).unique())
        raise ValueError("typed response panel does not cover optimizer universe: " + ", ".join(missing))
    return work


def _alpha_reward_contribution(objective_decomposition: dict[str, Any]) -> float:
    alpha_component = objective_decomposition.get("components", {}).get("alpha_reward", {})
    if "weighted_value" in alpha_component:
        return float(abs(alpha_component.get("weighted_value") or 0.0))
    if "raw_value" in alpha_component:
        return float(abs(alpha_component.get("raw_value") or 0.0))
    return float(abs(alpha_component.get("share_abs_weighted", 0.0)))


def _diagnostic_axis(work: pd.DataFrame, view_state: str, index: pd.Index) -> pd.Series:
    if view_state != "active_view":
        return (
            work.set_index("ticker")["expected_return"]
            .astype(float)
            .reindex(index)
            .fillna(0.0)
        )
    if "diagnostic_score" not in work.columns:
        return (
            work.set_index("ticker")["expected_return"]
            .astype(float)
            .reindex(index)
            .fillna(0.0)
        )
    return (
        work.set_index("ticker")["diagnostic_score"]
        .astype(float)
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
    pair = pd.concat([left, right], axis=1).dropna()
    if len(pair) < 2:
        return 0.0
    if pair.iloc[:, 0].nunique() <= 1 or pair.iloc[:, 1].nunique() <= 1:
        return 0.0
    value = float(pair.iloc[:, 0].corr(pair.iloc[:, 1], method="spearman"))
    if not np.isfinite(value):
        return 0.0
    return value


def _result(
    *,
    response_input: TypedOptimizerResponseInput,
    response_status: str,
    source_config_hash: str,
    input_hashes: dict[str, str],
    rows: list[TypedOptimizerResponseRow],
    unavailable_reason: str | None = None,
    rejection_reasons: list[str] | None = None,
) -> TypedOptimizerResponseResult:
    summary = _build_summary(
        run_id=response_input.run_id,
        response_status=response_status,
        rows=rows,
        unavailable_reason=unavailable_reason,
        rejection_reasons=rejection_reasons or [],
    )
    return TypedOptimizerResponseResult(
        run_id=response_input.run_id,
        response_status=response_status,
        response_rows=rows,
        summary=summary,
        source_config_hash=source_config_hash,
        input_artifact_hashes=input_hashes,
        no_live_data_confirmed=True,
        no_orders_confirmed=True,
        no_broker_confirmed=True,
    )


def _build_summary(
    *,
    run_id: str,
    response_status: str,
    rows: list[TypedOptimizerResponseRow],
    unavailable_reason: str | None,
    rejection_reasons: list[str],
) -> TypedOptimizerResponseSummary:
    by_name = {row.panel_name: row for row in rows}
    scaled = [
        by_name[name].alpha_reward_share
        for name in ("scaled_0_5x_panel", "scaled_1_0x_panel", "scaled_2_0x_panel")
        if name in by_name
    ]
    positive = by_name.get("positive_panel")
    sign_flipped = by_name.get("sign_flipped_panel")
    zero = by_name.get("zero_panel")
    abstain = by_name.get("abstain_panel")
    return TypedOptimizerResponseSummary(
        run_id=run_id,
        response_status=response_status,
        optimizer_status=_summary_optimizer_status(rows, response_status),
        panel_count=len(rows),
        positive_rank_alignment_passed=bool(
            positive is not None and positive.rank_alignment > 0.0 and positive.top_minus_bottom_weight_delta > 0.0
        ),
        scaled_alpha_reward_monotone=bool(
            len(scaled) == 3
            and all(later >= earlier - 1e-12 for earlier, later in zip(scaled, scaled[1:], strict=False))
            and scaled[0] < scaled[-1]
        ),
        sign_flip_reverses_ordering=bool(
            sign_flipped is not None
            and sign_flipped.rank_alignment < 0.0
            and sign_flipped.top_minus_bottom_weight_delta < 0.0
        ),
        no_view_distinct_from_zero_alpha=bool(
            zero is not None
            and abstain is not None
            and zero.view_state == "zero_alpha"
            and abstain.view_state == "no_view"
            and zero.expected_return_used_share > abstain.expected_return_used_share
        ),
        repair_retention_reported=bool(rows and all(row.repair_retention is not None for row in rows)),
        unavailable_reason=unavailable_reason,
        rejection_reasons=rejection_reasons,
    )


def _summary_optimizer_status(rows: list[TypedOptimizerResponseRow], response_status: str) -> str:
    if not rows:
        return "not_run" if response_status == "unavailable" else response_status
    statuses = sorted({row.optimizer_status for row in rows})
    return statuses[0] if len(statuses) == 1 else ",".join(statuses)


def _build_sign_flip_diagnostics(result: TypedOptimizerResponseResult) -> dict[str, Any]:
    by_name = {row.panel_name: row for row in result.response_rows}
    positive = by_name.get("positive_panel")
    sign_flipped = by_name.get("sign_flipped_panel")
    return {
        "run_id": result.run_id,
        "schema_version": "typed_optimizer_sign_flip_diagnostics.v1",
        "sign_flip_reverses_ordering": result.summary.sign_flip_reverses_ordering,
        "positive_rank_alignment": positive.rank_alignment if positive else None,
        "sign_flipped_rank_alignment": sign_flipped.rank_alignment if sign_flipped else None,
        "positive_top_minus_bottom_weight_delta": positive.top_minus_bottom_weight_delta if positive else None,
        "sign_flipped_top_minus_bottom_weight_delta": (
            sign_flipped.top_minus_bottom_weight_delta if sign_flipped else None
        ),
    }


def _build_abstain_vs_zero_report(result: TypedOptimizerResponseResult) -> dict[str, Any]:
    by_name = {row.panel_name: row for row in result.response_rows}
    zero = by_name.get("zero_panel")
    abstain = by_name.get("abstain_panel")
    return {
        "run_id": result.run_id,
        "schema_version": "typed_optimizer_abstain_vs_zero.v1",
        "no_view_distinct_from_zero_alpha": result.summary.no_view_distinct_from_zero_alpha,
        "zero_panel": zero.model_dump(mode="json") if zero else None,
        "abstain_panel": abstain.model_dump(mode="json") if abstain else None,
    }


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


def _validate_required_artifacts(response_input: TypedOptimizerResponseInput) -> list[str]:
    paths = {
        "expected_return_panel": response_input.expected_return_panel_path,
        "local_backtest_manifest": response_input.local_backtest_manifest_path,
        "projection_manifest": response_input.projection_manifest_path,
        "q2_input_contract_v2": response_input.q2_input_contract_v2_path,
    }
    return [f"{name} artifact is missing at {path}" for name, path in sorted(paths.items()) if not path.exists()]


def _input_artifact_hashes(response_input: TypedOptimizerResponseInput) -> dict[str, str]:
    paths = {
        "expected_return_panel": response_input.expected_return_panel_path,
        "local_backtest_manifest": response_input.local_backtest_manifest_path,
        "projection_manifest": response_input.projection_manifest_path,
        "q2_input_contract_v2": response_input.q2_input_contract_v2_path,
    }
    return {name: sha256_file(path) if path.exists() else "missing" for name, path in sorted(paths.items())}


def _validate_projection_manifest(contract: TypedQ2InputContractV2, projection_manifest: dict[str, Any]) -> None:
    if projection_manifest.get("schema_version") != "alpha_projection.v2":
        raise ValueError("projection_manifest must use alpha_projection.v2")
    if projection_manifest.get("content_hash") != contract.projection_manifest_hash:
        raise ValueError("projection manifest hash does not match Q2InputContractV2")
    if contract.alpha_view_id not in set(projection_manifest.get("alpha_view_ids", [])):
        raise ValueError("projection manifest does not include contract alpha_view_id")


def _expected_return_used_share_for_panel(universe: pd.DataFrame, panel: pd.DataFrame, view_state: str) -> float:
    if view_state == "no_view":
        return 0.0
    denominator = int(universe["ticker"].astype(str).str.upper().nunique())
    if denominator <= 0:
        return 0.0
    numerator = int(panel["symbol"].astype(str).str.upper().nunique()) if "symbol" in panel else 0
    return float(numerator / denominator)


def _active_name_count(panel: pd.DataFrame) -> int:
    if panel.empty or "symbol" not in panel:
        return 0
    return int(panel["symbol"].astype(str).str.upper().nunique())


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
