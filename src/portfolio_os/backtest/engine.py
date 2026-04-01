"""Minimal monthly backtest engine built on top of the optimizer library API."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.alpha.backtest_bridge import AlphaRebalanceSnapshot, build_alpha_snapshot_for_rebalance
from portfolio_os.backtest.attribution import build_backtest_summary, build_period_attribution_frame
from portfolio_os.backtest.baseline import SUPPORTED_BASELINES, run_alpha_only_top_quintile, run_naive_pro_rata
from portfolio_os.backtest.manifest import load_backtest_manifest
from portfolio_os.backtest.report import render_backtest_report
from portfolio_os.compliance.pretrade import collect_data_quality_findings
from portfolio_os.data.loaders import ensure_columns, normalize_ticker, read_csv
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import (
    build_portfolio_frame,
    load_holdings,
    load_portfolio_state,
    load_target_weights,
)
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.data.universe import build_universe_frame
from portfolio_os.domain.enums import OrderSide
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.domain.models import Holding, Order, PortfolioState, TargetWeight
from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.utils.config import AppConfig, load_app_config


@dataclass
class _StrategyState:
    """Mutable state tracked for one strategy across the backtest."""

    name: str
    quantities: pd.Series
    cash: float
    pending_orders: list[Order] = field(default_factory=list)
    pending_fill_date: pd.Timestamp | None = None
    rebalance_count: int = 0
    total_turnover: float = 0.0
    total_filled_notional: float = 0.0
    total_commission: float = 0.0
    total_spread_cost: float = 0.0


@dataclass
class BacktestResult:
    """Serializable backtest output plus the daily NAV panel."""

    manifest_path: Path
    nav_series: pd.DataFrame
    period_attribution: pd.DataFrame
    summary: dict[str, Any]
    report_markdown: str
    alpha_panel: pd.DataFrame | None = None

    def to_payload(self, *, alpha_panel_path: str | Path | None = None) -> dict[str, Any]:
        """Build the JSON payload written by the CLI."""

        payload = {
            "manifest_path": str(self.manifest_path),
            "summary": self.summary,
        }
        if alpha_panel_path is not None:
            payload["artifacts"] = {
                "alpha_panel": str(Path(alpha_panel_path)),
            }
        return payload


_ALPHA_PANEL_COLUMNS = [
    "date",
    "ticker",
    "alpha_score",
    "alpha_rank_pct",
    "alpha_zscore",
    "expected_return",
    "quantile",
    "signal_strength_confidence",
    "annualized_top_bottom_spread",
]


def _load_returns_long(path: Path) -> pd.DataFrame:
    """Load normalized long-form daily returns."""

    frame = read_csv(path)
    ensure_columns(frame, ["date", "ticker", "return"], str(path))
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.normalize()
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    frame["return"] = pd.to_numeric(frame["return"], errors="raise").astype(float)
    return frame.sort_values(["date", "ticker"]).reset_index(drop=True)


def reconstruct_price_panel(
    returns_long: pd.DataFrame,
    *,
    anchor_prices: pd.Series,
) -> pd.DataFrame:
    """Reconstruct daily close prices from returns and a terminal close snapshot."""

    returns_panel = (
        returns_long.pivot_table(index="date", columns="ticker", values="return", aggfunc="last")
        .sort_index()
        .fillna(0.0)
    )
    returns_panel = returns_panel.reindex(columns=list(anchor_prices.index), fill_value=0.0)
    if returns_panel.empty:
        raise InputValidationError("Backtest returns_file produced an empty date panel.")

    relative_prices = (1.0 + returns_panel).cumprod()
    terminal_scale = anchor_prices.astype(float) / relative_prices.iloc[-1].astype(float)
    price_panel = relative_prices.mul(terminal_scale, axis=1)
    price_panel.index = pd.to_datetime(price_panel.index).normalize()
    price_panel.index.name = "date"
    return price_panel


def build_monthly_rebalance_schedule(price_panel: pd.DataFrame) -> list[pd.Timestamp]:
    """Return last-trading-day monthly rebalance dates, excluding the terminal date."""

    if price_panel.empty:
        return []
    month_ends = (
        price_panel.index.to_series()
        .groupby(price_panel.index.to_period("M"))
        .max()
        .sort_values()
        .tolist()
    )
    if month_ends and month_ends[-1] == price_panel.index.max():
        month_ends = month_ends[:-1]
    return [pd.Timestamp(item).normalize() for item in month_ends]


def _build_strategy_universe(
    *,
    current_date: pd.Timestamp,
    quantities: pd.Series,
    cash: float,
    targets: list[TargetWeight],
    base_market_frame: pd.DataFrame,
    base_reference_frame: pd.DataFrame,
    price_row: pd.Series,
    app_config_template: AppConfig,
) -> tuple[pd.DataFrame, AppConfig]:
    """Build one in-memory universe snapshot for a rebalance decision."""

    holdings = [
        Holding(ticker=str(ticker), quantity=int(quantity))
        for ticker, quantity in quantities.items()
    ]
    portfolio_frame = build_portfolio_frame(holdings, targets)
    market_frame = base_market_frame.copy()
    market_frame["close"] = market_frame["ticker"].map(lambda ticker: float(price_row[str(ticker)]))
    market_frame["vwap"] = market_frame["close"]
    portfolio_state = app_config_template.portfolio_state.model_copy(
        update={
            "as_of_date": current_date.strftime("%Y-%m-%d"),
            "available_cash": float(cash),
        }
    )
    universe = build_universe_frame(
        portfolio_frame,
        market_frame,
        base_reference_frame,
        portfolio_state,
    )
    config = app_config_template.model_copy(
        deep=True,
        update={"portfolio_state": portfolio_state},
    )
    return universe, config


def _apply_pending_orders(
    state: _StrategyState,
    *,
    current_date: pd.Timestamp,
    price_row: pd.Series,
    commission_rate: float,
    half_spread_bps: float,
) -> None:
    """Fill any orders scheduled for the current T+1 date."""

    if state.pending_fill_date != current_date or not state.pending_orders:
        return
    for order in state.pending_orders:
        close_price = float(price_row[str(order.ticker)])
        side_sign = 1.0 if order.side == OrderSide.BUY else -1.0
        filled_quantity = int(order.quantity)
        fill_price = close_price * (1.0 + side_sign * half_spread_bps / 10000.0)
        fill_notional = float(abs(filled_quantity) * fill_price)
        commission = float(fill_notional * commission_rate)
        signed_quantity = filled_quantity if order.side == OrderSide.BUY else -filled_quantity
        state.quantities.loc[str(order.ticker)] = int(state.quantities.loc[str(order.ticker)] + signed_quantity)
        state.cash -= float(signed_quantity * fill_price + commission)
        state.total_filled_notional += fill_notional
        state.total_commission += commission
        state.total_spread_cost += float(abs(fill_price - close_price) * abs(filled_quantity))
    state.pending_orders = []
    state.pending_fill_date = None


def _record_nav_rows(states: dict[str, _StrategyState], price_row: pd.Series) -> list[dict[str, Any]]:
    """Capture one NAV row per strategy for the current date."""

    rows: list[dict[str, Any]] = []
    for strategy_name, state in states.items():
        gross_market_value = float((state.quantities.astype(float) * price_row.reindex(state.quantities.index)).sum())
        nav = gross_market_value + float(state.cash)
        rows.append(
            {
                "date": price_row.name.strftime("%Y-%m-%d"),
                "strategy": strategy_name,
                "nav": nav,
                "cash": float(state.cash),
                "gross_market_value": gross_market_value,
            }
        )
    return rows


def _inject_expected_returns(
    universe: pd.DataFrame,
    *,
    alpha_snapshot: AlphaRebalanceSnapshot | None,
) -> pd.DataFrame:
    """Attach walk-forward expected returns when an alpha snapshot is available."""

    if alpha_snapshot is None:
        return universe
    expected_return_frame = alpha_snapshot.current_cross_section.loc[:, ["ticker", "expected_return"]].copy()
    merged = universe.merge(expected_return_frame, on="ticker", how="left")
    merged["expected_return"] = merged["expected_return"].fillna(0.0).astype(float)
    return merged


def _build_summary(
    *,
    states: dict[str, _StrategyState],
) -> dict[str, dict[str, float | int]]:
    """Extract per-strategy totals from mutable state."""

    return {
        strategy_name: {
            "rebalance_count": int(state.rebalance_count),
            "total_turnover": float(state.total_turnover),
            "total_filled_notional": float(state.total_filled_notional),
            "total_commission": float(state.total_commission),
            "total_spread_cost": float(state.total_spread_cost),
            "total_transaction_cost": float(state.total_commission + state.total_spread_cost),
        }
        for strategy_name, state in states.items()
    }


def _build_period_attribution_row(
    *,
    strategy_name: str,
    period_index: int,
    start_date: pd.Timestamp,
    fill_date: pd.Timestamp,
    end_date: pd.Timestamp,
    start_quantities: pd.Series,
    start_cash: float,
    start_price_row: pd.Series,
    fill_price_row: pd.Series,
    end_price_row: pd.Series,
    orders: list[Order],
    gross_traded_notional: float,
    turnover: float,
    commission_rate: float,
    half_spread_bps: float,
) -> dict[str, Any]:
    """Build one period attribution row from a rebalance decision."""

    start_nav = float(start_cash + (start_quantities.astype(float) * start_price_row.reindex(start_quantities.index)).sum())
    holding_pnl = float((start_quantities.astype(float) * (end_price_row - start_price_row).reindex(start_quantities.index)).sum())
    active_trading_pnl = 0.0
    commission_cost = 0.0
    spread_cost = 0.0
    filled_notional = 0.0
    for order in orders:
        ticker = str(order.ticker)
        signed_qty = float(order.quantity if order.side == OrderSide.BUY else -order.quantity)
        fill_mid = float(fill_price_row[ticker])
        end_price = float(end_price_row[ticker])
        side_sign = 1.0 if signed_qty > 0 else -1.0
        fill_price = fill_mid * (1.0 + side_sign * half_spread_bps / 10000.0)
        trade_notional = float(abs(signed_qty) * fill_price)
        active_trading_pnl += float(signed_qty * (end_price - fill_mid))
        commission_cost += float(trade_notional * commission_rate)
        spread_cost += float(signed_qty * (fill_price - fill_mid))
        filled_notional += trade_notional
    trading_cost_pnl = float(-(commission_cost + spread_cost))
    period_pnl = float(holding_pnl + active_trading_pnl + trading_cost_pnl)
    end_nav = float(start_nav + period_pnl)
    period_return = float(period_pnl / start_nav) if start_nav else 0.0
    return {
        "strategy": strategy_name,
        "period_index": int(period_index),
        "start_date": start_date.strftime("%Y-%m-%d"),
        "fill_date": fill_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "start_nav": start_nav,
        "end_nav": end_nav,
        "filled_notional": float(filled_notional),
        "gross_traded_notional": float(gross_traded_notional),
        "turnover": float(turnover),
        "commission_cost": float(commission_cost),
        "spread_cost": float(spread_cost),
        "holding_pnl": float(holding_pnl),
        "active_trading_pnl": float(active_trading_pnl),
        "trading_cost_pnl": float(trading_cost_pnl),
        "period_pnl": float(period_pnl),
        "period_return": float(period_return),
    }


def run_backtest(manifest_path: str | Path) -> BacktestResult:
    """Run the minimal monthly backtest loop and return NAV plus summary data."""

    manifest = load_backtest_manifest(manifest_path)
    unknown_baselines = sorted(set(manifest.baselines) - SUPPORTED_BASELINES)
    if unknown_baselines:
        raise InputValidationError(
            "Unsupported backtest baseline(s): " + ", ".join(unknown_baselines)
        )
    if "alpha_only_top_quintile" in manifest.baselines and not (
        manifest.alpha_model is not None and manifest.alpha_model.enabled
    ):
        raise InputValidationError("alpha_only_top_quintile baseline requires alpha_model.enabled = true.")

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

    anchor_prices = (
        base_market_frame.set_index("ticker")["close"].astype(float).reindex(required_tickers)
    )
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

    alpha_only_enabled = bool(
        manifest.alpha_model is not None
        and manifest.alpha_model.enabled
        and manifest.alpha_model.add_alpha_only_baseline
    )
    strategy_order = ["optimizer", *manifest.baselines]
    if alpha_only_enabled and "alpha_only_top_quintile" not in strategy_order:
        strategy_order.append("alpha_only_top_quintile")
    states: dict[str, _StrategyState] = {
        "optimizer": _StrategyState(
            name="optimizer",
            quantities=initial_quantities.copy(),
            cash=float(initial_state.available_cash),
        )
    }
    if "naive_pro_rata" in manifest.baselines:
        states["naive_pro_rata"] = _StrategyState(
            name="naive_pro_rata",
            quantities=initial_quantities.copy(),
            cash=float(initial_state.available_cash),
        )
    if "buy_and_hold" in manifest.baselines:
        states["buy_and_hold"] = _StrategyState(
            name="buy_and_hold",
            quantities=initial_quantities.copy(),
            cash=float(initial_state.available_cash),
        )
    if "alpha_only_top_quintile" in strategy_order:
        states["alpha_only_top_quintile"] = _StrategyState(
            name="alpha_only_top_quintile",
            quantities=initial_quantities.copy(),
            cash=float(initial_state.available_cash),
        )

    nav_rows: list[dict[str, Any]] = []
    period_rows: list[dict[str, Any]] = []
    alpha_panel_frames: list[pd.DataFrame] = []
    alpha_ready_rebalance_count = 0
    commission_rate = float(app_config.fees.commission_rate)
    half_spread_bps = float(app_config.execution.backtest_fixed_half_spread_bps)
    schedule_index_map = {pd.Timestamp(item).normalize(): idx for idx, item in enumerate(schedule, start=1)}

    for idx, current_date in enumerate(dates):
        price_row = price_panel.loc[current_date]
        price_row.name = pd.Timestamp(current_date).normalize()

        for state in states.values():
            _apply_pending_orders(
                state,
                current_date=current_date,
                price_row=price_row,
                commission_rate=commission_rate,
                half_spread_bps=half_spread_bps,
            )

        nav_rows.extend(_record_nav_rows(states, price_row))

        if current_date not in schedule:
            continue
        next_date = dates[idx + 1]
        period_index = schedule_index_map[current_date]
        schedule_position = period_index - 1
        period_end_date = schedule[schedule_position + 1] if schedule_position + 1 < len(schedule) else dates[-1]
        fill_price_row = price_panel.loc[next_date]
        end_price_row = price_panel.loc[period_end_date]
        alpha_snapshot = None
        if manifest.alpha_model is not None and manifest.alpha_model.enabled:
            try:
                alpha_snapshot = build_alpha_snapshot_for_rebalance(
                    returns_file=manifest.returns_file,
                    rebalance_date=current_date,
                    quantiles=manifest.alpha_model.quantiles,
                    min_evaluation_dates=manifest.alpha_model.min_evaluation_dates,
                    zscore_winsor_limit=manifest.alpha_model.zscore_winsor_limit,
                    t_stat_full_confidence=manifest.alpha_model.t_stat_full_confidence,
                    max_abs_expected_return=manifest.alpha_model.max_abs_expected_return,
                )
            except InputValidationError:
                # Early rebalance dates can legitimately lack enough history for the accepted recipe.
                alpha_snapshot = None
        if alpha_snapshot is not None:
            alpha_ready_rebalance_count += 1
            alpha_panel_frame = alpha_snapshot.current_cross_section.copy()
            if "date" not in alpha_panel_frame.columns:
                alpha_panel_frame["date"] = current_date.strftime("%Y-%m-%d")
            alpha_panel_frames.append(alpha_panel_frame.reindex(columns=_ALPHA_PANEL_COLUMNS).copy())

        optimizer_start_quantities = states["optimizer"].quantities.copy()
        optimizer_start_cash = float(states["optimizer"].cash)
        optimizer_universe, optimizer_config = _build_strategy_universe(
            current_date=current_date,
            quantities=optimizer_start_quantities,
            cash=optimizer_start_cash,
            targets=targets,
            base_market_frame=base_market_frame,
            base_reference_frame=base_reference_frame,
            price_row=price_row,
            app_config_template=app_config,
        )
        optimizer_universe = _inject_expected_returns(
            optimizer_universe,
            alpha_snapshot=alpha_snapshot,
        )
        optimizer_findings = collect_data_quality_findings(optimizer_universe, optimizer_config)
        optimizer_run = run_rebalance(
            optimizer_universe,
            optimizer_config,
            input_findings=optimizer_findings,
        )
        states["optimizer"].rebalance_count += 1
        states["optimizer"].total_turnover += (
            optimizer_run.basket.gross_traded_notional / optimizer_run.pre_trade_nav
            if optimizer_run.pre_trade_nav
            else 0.0
        )
        period_rows.append(
            _build_period_attribution_row(
                strategy_name="optimizer",
                period_index=period_index,
                start_date=current_date,
                fill_date=next_date,
                end_date=period_end_date,
                start_quantities=optimizer_start_quantities,
                start_cash=optimizer_start_cash,
                start_price_row=price_row,
                fill_price_row=fill_price_row,
                end_price_row=end_price_row,
                orders=list(optimizer_run.orders),
                gross_traded_notional=float(optimizer_run.basket.gross_traded_notional),
                turnover=(
                    optimizer_run.basket.gross_traded_notional / optimizer_run.pre_trade_nav
                    if optimizer_run.pre_trade_nav
                    else 0.0
                ),
                commission_rate=commission_rate,
                half_spread_bps=half_spread_bps,
            )
        )
        states["optimizer"].pending_orders = list(optimizer_run.orders)
        states["optimizer"].pending_fill_date = next_date

        if "naive_pro_rata" in states:
            naive_start_quantities = states["naive_pro_rata"].quantities.copy()
            naive_start_cash = float(states["naive_pro_rata"].cash)
            naive_universe, naive_config = _build_strategy_universe(
                current_date=current_date,
                quantities=naive_start_quantities,
                cash=naive_start_cash,
                targets=targets,
                base_market_frame=base_market_frame,
                base_reference_frame=base_reference_frame,
                price_row=price_row,
                app_config_template=app_config,
            )
            naive_findings = collect_data_quality_findings(naive_universe, naive_config)
            naive_run = run_naive_pro_rata(
                naive_universe,
                naive_config,
                input_findings=naive_findings,
            )
            states["naive_pro_rata"].rebalance_count += 1
            states["naive_pro_rata"].total_turnover += (
                naive_run.basket.gross_traded_notional / naive_run.pre_trade_nav
                if naive_run.pre_trade_nav
                else 0.0
            )
            period_rows.append(
                _build_period_attribution_row(
                    strategy_name="naive_pro_rata",
                    period_index=period_index,
                    start_date=current_date,
                    fill_date=next_date,
                    end_date=period_end_date,
                    start_quantities=naive_start_quantities,
                    start_cash=naive_start_cash,
                    start_price_row=price_row,
                    fill_price_row=fill_price_row,
                    end_price_row=end_price_row,
                    orders=list(naive_run.orders),
                    gross_traded_notional=float(naive_run.basket.gross_traded_notional),
                    turnover=(
                        naive_run.basket.gross_traded_notional / naive_run.pre_trade_nav
                        if naive_run.pre_trade_nav
                        else 0.0
                    ),
                    commission_rate=commission_rate,
                    half_spread_bps=half_spread_bps,
                )
            )
            states["naive_pro_rata"].pending_orders = list(naive_run.orders)
            states["naive_pro_rata"].pending_fill_date = next_date

        if "buy_and_hold" in states:
            buy_hold_start_quantities = states["buy_and_hold"].quantities.copy()
            buy_hold_start_cash = float(states["buy_and_hold"].cash)
            period_rows.append(
                _build_period_attribution_row(
                    strategy_name="buy_and_hold",
                    period_index=period_index,
                    start_date=current_date,
                    fill_date=next_date,
                    end_date=period_end_date,
                    start_quantities=buy_hold_start_quantities,
                    start_cash=buy_hold_start_cash,
                    start_price_row=price_row,
                    fill_price_row=fill_price_row,
                    end_price_row=end_price_row,
                    orders=[],
                    gross_traded_notional=0.0,
                    turnover=0.0,
                    commission_rate=commission_rate,
                    half_spread_bps=half_spread_bps,
                )
            )

        if "alpha_only_top_quintile" in states and alpha_snapshot is not None:
            alpha_start_quantities = states["alpha_only_top_quintile"].quantities.copy()
            alpha_start_cash = float(states["alpha_only_top_quintile"].cash)
            alpha_universe, alpha_config = _build_strategy_universe(
                current_date=current_date,
                quantities=alpha_start_quantities,
                cash=alpha_start_cash,
                targets=targets,
                base_market_frame=base_market_frame,
                base_reference_frame=base_reference_frame,
                price_row=price_row,
                app_config_template=app_config,
            )
            alpha_findings = collect_data_quality_findings(alpha_universe, alpha_config)
            alpha_run = run_alpha_only_top_quintile(
                alpha_universe,
                alpha_config,
                alpha_target_weights=alpha_snapshot.alpha_only_target_weights,
                input_findings=alpha_findings,
            )
            states["alpha_only_top_quintile"].rebalance_count += 1
            states["alpha_only_top_quintile"].total_turnover += (
                alpha_run.basket.gross_traded_notional / alpha_run.pre_trade_nav
                if alpha_run.pre_trade_nav
                else 0.0
            )
            period_rows.append(
                _build_period_attribution_row(
                    strategy_name="alpha_only_top_quintile",
                    period_index=period_index,
                    start_date=current_date,
                    fill_date=next_date,
                    end_date=period_end_date,
                    start_quantities=alpha_start_quantities,
                    start_cash=alpha_start_cash,
                    start_price_row=price_row,
                    fill_price_row=fill_price_row,
                    end_price_row=end_price_row,
                    orders=list(alpha_run.orders),
                    gross_traded_notional=float(alpha_run.basket.gross_traded_notional),
                    turnover=(
                        alpha_run.basket.gross_traded_notional / alpha_run.pre_trade_nav
                        if alpha_run.pre_trade_nav
                        else 0.0
                    ),
                    commission_rate=commission_rate,
                    half_spread_bps=half_spread_bps,
                )
            )
            states["alpha_only_top_quintile"].pending_orders = list(alpha_run.orders)
            states["alpha_only_top_quintile"].pending_fill_date = next_date

    nav_series = pd.DataFrame(nav_rows)
    nav_series["strategy"] = pd.Categorical(
        nav_series["strategy"],
        categories=[name for name in strategy_order if name in states],
        ordered=True,
    )
    nav_series = nav_series.sort_values(["date", "strategy"]).reset_index(drop=True)
    alpha_panel = None
    if manifest.alpha_model is not None and manifest.alpha_model.enabled:
        alpha_panel = (
            pd.concat(alpha_panel_frames, ignore_index=True)
            if alpha_panel_frames
            else pd.DataFrame(columns=_ALPHA_PANEL_COLUMNS)
        )
    period_attribution = build_period_attribution_frame(period_rows)
    state_summary = _build_summary(states=states)
    summary = build_backtest_summary(
        schedule=schedule,
        nav_series=nav_series,
        period_attribution=period_attribution,
        strategy_state_summary=state_summary,
    )
    summary["alpha_model"] = {
        "enabled": bool(manifest.alpha_model is not None and manifest.alpha_model.enabled),
        "recipe_name": (
            manifest.alpha_model.recipe_name
            if manifest.alpha_model is not None and manifest.alpha_model.enabled
            else None
        ),
        "alpha_weight": float(app_config.objective_weights.alpha_weight or 0.0),
        "write_alpha_panel": bool(manifest.alpha_model.write_alpha_panel) if manifest.alpha_model is not None else False,
        "add_alpha_only_baseline": (
            bool(manifest.alpha_model.add_alpha_only_baseline)
            if manifest.alpha_model is not None
            else False
        ),
        "panel_row_count": int(len(alpha_panel)) if alpha_panel is not None else 0,
        "rebalance_dates_with_alpha_signal": int(alpha_ready_rebalance_count),
        "rebalance_dates_without_alpha_signal": int(len(schedule) - alpha_ready_rebalance_count),
    }
    result = BacktestResult(
        manifest_path=manifest.manifest_path,
        nav_series=nav_series,
        period_attribution=period_attribution,
        summary=summary,
        report_markdown="",
        alpha_panel=alpha_panel,
    )
    result.report_markdown = render_backtest_report(result)
    return result
