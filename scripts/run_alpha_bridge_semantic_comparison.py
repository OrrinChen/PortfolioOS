from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.alpha.backtest_bridge import build_alpha_snapshot_for_rebalance_protocol
from portfolio_os.alpha.bridge_semantics import (
    AlphaNegativeSpreadProtocol,
    compute_hold_through_metrics,
    recommend_guard_protocol,
    render_guard_protocol_comparison_note,
    summarize_guard_protocol_results,
)
from portfolio_os.backtest.engine import (
    _StrategyState,
    _apply_pending_orders,
    _build_strategy_universe,
    _decision_horizon_days_for_rebalance,
    _inject_expected_returns,
    _load_returns_long,
    build_monthly_rebalance_schedule,
    reconstruct_price_panel,
)
from portfolio_os.backtest.manifest import load_backtest_manifest
from portfolio_os.compliance.pretrade import collect_data_quality_findings
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import (
    build_portfolio_frame,
    load_holdings,
    load_portfolio_state,
    load_target_weights,
)
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.utils.config import load_app_config


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "alpha_bridge_semantic_comparison_2026-04-15"
_PROTOCOLS: tuple[AlphaNegativeSpreadProtocol, ...] = (
    "floor_to_zero",
    "signed_spread",
    "explicit_abstain",
)


@dataclass(frozen=True)
class GuardEventContext:
    rebalance_date: pd.Timestamp
    next_date: pd.Timestamp
    period_end_date: pd.Timestamp
    decision_horizon_days: int
    pre_trade_quantities: pd.Series
    pre_trade_cash: float
    price_row: pd.Series
    history_length: int
    confidence: float
    raw_mean_top_bottom_spread: float


def _prepare_backtest_context(manifest_path: Path) -> dict[str, Any]:
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
    price_panel = reconstruct_price_panel(
        returns_long,
        anchor_prices=anchor_prices,
    ).reindex(columns=required_tickers)
    schedule = build_monthly_rebalance_schedule(price_panel)
    dates = [pd.Timestamp(item).normalize() for item in price_panel.index.tolist()]

    initial_quantities = (
        portfolio_frame.set_index("ticker")["quantity"].reindex(required_tickers).fillna(0).astype(int)
    )
    state = _StrategyState(
        name="optimizer",
        quantities=initial_quantities.copy(),
        cash=float(initial_state.available_cash),
    )

    return {
        "manifest": manifest,
        "app_config": app_config,
        "targets": targets,
        "base_market_frame": base_market_frame,
        "base_reference_frame": base_reference_frame,
        "price_panel": price_panel,
        "schedule": schedule,
        "dates": dates,
        "state": state,
    }


def _collect_guard_event_contexts(manifest_path: Path) -> list[GuardEventContext]:
    context = _prepare_backtest_context(manifest_path)
    manifest = context["manifest"]
    app_config = context["app_config"]
    targets = context["targets"]
    base_market_frame = context["base_market_frame"]
    base_reference_frame = context["base_reference_frame"]
    price_panel = context["price_panel"]
    schedule = context["schedule"]
    dates = context["dates"]
    state = context["state"]

    commission_rate = float(app_config.fees.commission_rate)
    half_spread_bps = float(app_config.execution.backtest_fixed_half_spread_bps)
    schedule_index_map = {pd.Timestamp(item).normalize(): idx for idx, item in enumerate(schedule, start=1)}
    date_position_map = {pd.Timestamp(item).normalize(): idx for idx, item in enumerate(dates)}
    guard_contexts: list[GuardEventContext] = []
    min_eval = int(manifest.alpha_model.min_evaluation_dates) if manifest.alpha_model is not None else 0

    for idx, current_date in enumerate(dates):
        price_row = price_panel.loc[current_date]
        price_row.name = pd.Timestamp(current_date).normalize()
        _apply_pending_orders(
            state,
            current_date=current_date,
            price_row=price_row,
            commission_rate=commission_rate,
            half_spread_bps=half_spread_bps,
        )

        if current_date not in schedule:
            continue
        next_date = dates[idx + 1]
        period_index = schedule_index_map[current_date]
        schedule_position = period_index - 1
        period_end_date = schedule[schedule_position + 1] if schedule_position + 1 < len(schedule) else dates[-1]
        decision_horizon_days = _decision_horizon_days_for_rebalance(
            current_index=idx,
            period_end_date=period_end_date,
            date_position_map=date_position_map,
        )

        try:
            protocol_result = build_alpha_snapshot_for_rebalance_protocol(
                returns_file=manifest.returns_file,
                rebalance_date=current_date,
                quantiles=manifest.alpha_model.quantiles,
                min_evaluation_dates=manifest.alpha_model.min_evaluation_dates,
                zscore_winsor_limit=manifest.alpha_model.zscore_winsor_limit,
                t_stat_full_confidence=manifest.alpha_model.t_stat_full_confidence,
                max_abs_expected_return=manifest.alpha_model.max_abs_expected_return,
                decision_horizon_days=decision_horizon_days,
                negative_spread_protocol="floor_to_zero",
            )
        except InputValidationError:
            protocol_result = None

        if (
            protocol_result is not None
            and protocol_result.history_length >= min_eval
            and protocol_result.protocol_decision.status == "spread_floor_to_zero"
        ):
            guard_contexts.append(
                GuardEventContext(
                    rebalance_date=pd.Timestamp(current_date).normalize(),
                    next_date=pd.Timestamp(next_date).normalize(),
                    period_end_date=pd.Timestamp(period_end_date).normalize(),
                    decision_horizon_days=int(decision_horizon_days),
                    pre_trade_quantities=state.quantities.copy(),
                    pre_trade_cash=float(state.cash),
                    price_row=price_row.copy(),
                    history_length=int(protocol_result.history_length),
                    confidence=float(protocol_result.confidence),
                    raw_mean_top_bottom_spread=float(protocol_result.protocol_decision.raw_mean_top_bottom_spread),
                )
            )

        optimizer_universe, optimizer_config = _build_strategy_universe(
            current_date=current_date,
            quantities=state.quantities.copy(),
            cash=float(state.cash),
            targets=targets,
            base_market_frame=base_market_frame,
            base_reference_frame=base_reference_frame,
            price_row=price_row,
            app_config_template=app_config,
        )
        optimizer_universe = _inject_expected_returns(
            optimizer_universe,
            alpha_snapshot=(protocol_result.snapshot if protocol_result is not None else None),
        )
        optimizer_findings = collect_data_quality_findings(optimizer_universe, optimizer_config)
        optimizer_run = run_rebalance(
            optimizer_universe,
            optimizer_config,
            input_findings=optimizer_findings,
        )
        state.pending_orders = list(optimizer_run.orders)
        state.pending_fill_date = pd.Timestamp(next_date).normalize()

    return guard_contexts


def _evaluate_protocol_for_guard_event(
    guard_context: GuardEventContext,
    *,
    manifest_path: Path,
    protocol: AlphaNegativeSpreadProtocol,
) -> dict[str, Any]:
    context = _prepare_backtest_context(manifest_path)
    manifest = context["manifest"]
    app_config = context["app_config"]
    targets = context["targets"]
    base_market_frame = context["base_market_frame"]
    base_reference_frame = context["base_reference_frame"]

    optimizer_universe, optimizer_config = _build_strategy_universe(
        current_date=guard_context.rebalance_date,
        quantities=guard_context.pre_trade_quantities.copy(),
        cash=float(guard_context.pre_trade_cash),
        targets=targets,
        base_market_frame=base_market_frame,
        base_reference_frame=base_reference_frame,
        price_row=guard_context.price_row,
        app_config_template=app_config,
    )

    protocol_result = build_alpha_snapshot_for_rebalance_protocol(
        returns_file=manifest.returns_file,
        rebalance_date=guard_context.rebalance_date,
        quantiles=manifest.alpha_model.quantiles,
        min_evaluation_dates=manifest.alpha_model.min_evaluation_dates,
        zscore_winsor_limit=manifest.alpha_model.zscore_winsor_limit,
        t_stat_full_confidence=manifest.alpha_model.t_stat_full_confidence,
        max_abs_expected_return=manifest.alpha_model.max_abs_expected_return,
        decision_horizon_days=guard_context.decision_horizon_days,
        negative_spread_protocol=protocol,
    )
    optimizer_universe = _inject_expected_returns(
        optimizer_universe,
        alpha_snapshot=protocol_result.snapshot,
    )

    optimizer_findings = collect_data_quality_findings(optimizer_universe, optimizer_config)
    optimizer_run = run_rebalance(
        optimizer_universe,
        optimizer_config,
        input_findings=optimizer_findings,
    )
    hold_metrics = compute_hold_through_metrics(
        tickers=optimizer_universe["ticker"],
        pre_trade_quantities=guard_context.pre_trade_quantities,
        post_trade_quantities=optimizer_run.post_trade_quantities,
        price_row=guard_context.price_row,
    )

    positive_expected_return_count = 0
    negative_expected_return_count = 0
    zero_expected_return_count = 0
    if protocol_result.snapshot is not None:
        expected_returns = protocol_result.snapshot.current_cross_section["expected_return"].astype(float)
        positive_expected_return_count = int((expected_returns > 0.0).sum())
        negative_expected_return_count = int((expected_returns < 0.0).sum())
        zero_expected_return_count = int((expected_returns == 0.0).sum())

    return {
        "rebalance_date": guard_context.rebalance_date.strftime("%Y-%m-%d"),
        "next_date": guard_context.next_date.strftime("%Y-%m-%d"),
        "period_end_date": guard_context.period_end_date.strftime("%Y-%m-%d"),
        "protocol": str(protocol),
        "protocol_status": str(protocol_result.protocol_decision.status),
        "history_length": int(protocol_result.history_length),
        "confidence": float(protocol_result.confidence),
        "raw_mean_top_bottom_spread": float(protocol_result.protocol_decision.raw_mean_top_bottom_spread),
        "annualized_top_bottom_spread": protocol_result.protocol_decision.annualized_top_bottom_spread,
        "period_top_bottom_spread": protocol_result.protocol_decision.period_top_bottom_spread,
        "alpha_snapshot_present": bool(protocol_result.snapshot is not None),
        "positive_expected_return_count": int(positive_expected_return_count),
        "negative_expected_return_count": int(negative_expected_return_count),
        "zero_expected_return_count": int(zero_expected_return_count),
        "pre_trade_nav": float(optimizer_run.pre_trade_nav),
        "gross_traded_notional": float(optimizer_run.basket.gross_traded_notional),
        "turnover": (
            float(optimizer_run.basket.gross_traded_notional / optimizer_run.pre_trade_nav)
            if optimizer_run.pre_trade_nav
            else 0.0
        ),
        "post_trade_cash_weight": (
            float(optimizer_run.cash_after / optimizer_run.pre_trade_nav)
            if optimizer_run.pre_trade_nav
            else 0.0
        ),
        "pre_held_position_count": int(hold_metrics.pre_held_position_count),
        "retained_position_count": int(hold_metrics.retained_position_count),
        "liquidated_preheld_count": int(hold_metrics.liquidated_preheld_count),
        "hold_through_rate_count": float(hold_metrics.hold_through_rate_count),
        "hold_through_rate_value": float(hold_metrics.hold_through_rate_value),
        "liquidated_preheld_value": float(hold_metrics.liquidated_preheld_value),
    }


def run_alpha_bridge_semantic_comparison(
    *,
    manifest_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    guard_contexts = _collect_guard_event_contexts(manifest_path)
    detail_rows: list[dict[str, Any]] = []
    for guard_context in guard_contexts:
        for protocol in _PROTOCOLS:
            detail_rows.append(
                _evaluate_protocol_for_guard_event(
                    guard_context,
                    manifest_path=manifest_path,
                    protocol=protocol,
                )
            )

    detail_frame = pd.DataFrame(detail_rows).sort_values(["rebalance_date", "protocol"]).reset_index(drop=True)
    summary_frame = summarize_guard_protocol_results(detail_frame, baseline_protocol="explicit_abstain")
    recommended_protocol = recommend_guard_protocol(summary_frame) if not summary_frame.empty else None
    note_text = render_guard_protocol_comparison_note(
        detail_frame=detail_frame,
        summary_frame=summary_frame,
        baseline_protocol="explicit_abstain",
        recommended_protocol=recommended_protocol,
    )

    detail_path = output_dir / "guard_protocol_detail.csv"
    summary_path = output_dir / "guard_protocol_summary.csv"
    note_path = output_dir / "alpha_bridge_semantic_comparison.md"
    manifest_out_path = output_dir / "alpha_bridge_semantic_comparison_manifest.json"

    detail_frame.to_csv(detail_path, index=False)
    summary_frame.to_csv(summary_path, index=False)
    note_path.write_text(note_text, encoding="utf-8")
    manifest_out_path.write_text(
        json.dumps(
            {
                "manifest_path": str(manifest_path),
                "guard_event_count": int(detail_frame["rebalance_date"].nunique()) if not detail_frame.empty else 0,
                "protocols": list(_PROTOCOLS),
                "recommended_protocol": recommended_protocol,
                "artifacts": {
                    "detail_csv": str(detail_path),
                    "summary_csv": str(summary_path),
                    "note_markdown": str(note_path),
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "detail_path": detail_path,
        "summary_path": summary_path,
        "note_path": note_path,
        "manifest_path": manifest_out_path,
        "recommended_protocol": recommended_protocol,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare alpha-bridge guard semantics on shared pre-trade states.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="Backtest manifest used for the official alpha path.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for comparison artifacts.")
    return parser


def main(argv: list[str] | None = None) -> int:
    os.chdir(ROOT)
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run_alpha_bridge_semantic_comparison(
        manifest_path=Path(args.manifest).resolve(),
        output_dir=Path(args.output_dir).resolve(),
    )
    print(f"guard_protocol_detail.csv: {result['detail_path']}")
    print(f"guard_protocol_summary.csv: {result['summary_path']}")
    print(f"alpha_bridge_semantic_comparison.md: {result['note_path']}")
    print(f"alpha_bridge_semantic_comparison_manifest.json: {result['manifest_path']}")
    print(f"recommended_protocol: {result['recommended_protocol']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
