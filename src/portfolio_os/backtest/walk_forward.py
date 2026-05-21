"""Historical walk-forward portfolio evaluation for PortfolioOS."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from portfolio_os.backtest.engine import _load_returns_long, reconstruct_price_panel
from portfolio_os.backtest.manifest import load_backtest_manifest
from portfolio_os.cost.slippage import estimate_slippage_array
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import build_portfolio_frame, load_holdings, load_portfolio_state, load_target_weights
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.utils.config import load_app_config


WalkForwardFrequency = Literal["monthly", "weekly"]


@dataclass(frozen=True)
class WalkForwardResult:
    """Result bundle for a walk-forward portfolio evaluation."""

    output_dir: Path
    nav_curve: pd.DataFrame
    drawdown_curve: pd.DataFrame
    turnover_distribution: pd.DataFrame
    cost_attribution: pd.DataFrame
    multi_snapshot_replay: pd.DataFrame
    strategy_comparison: pd.DataFrame
    policy_breaches: pd.DataFrame
    no_lookahead_report: pd.DataFrame
    summary: dict[str, Any]
    report_markdown: str


_STRATEGIES = [
    "equal_weight",
    "mean_variance",
    "risk_parity",
    "cost_unaware_rebalance",
    "portfolio_os_cost_aware_rebalance",
]

_DOWNSTREAM_FLAGS = {
    "q1_entry_allowed": False,
    "q2_entry_allowed": False,
    "optimizer_alpha_input_opened": False,
    "alpha_registry_update_allowed": False,
    "paper_ready": False,
    "live_ready": False,
    "broker_order_path_opened": False,
    "production_approval_claimed": False,
}


def build_rebalance_schedule(price_panel: pd.DataFrame, *, frequency: WalkForwardFrequency) -> list[pd.Timestamp]:
    """Build weekly or monthly rebalance dates, excluding terminal date."""

    if price_panel.empty:
        return []
    if frequency == "monthly":
        periods = price_panel.index.to_period("M")
    elif frequency == "weekly":
        periods = price_panel.index.to_period("W-FRI")
    else:
        raise ValueError(f"unsupported walk-forward frequency: {frequency}")
    schedule = price_panel.index.to_series().groupby(periods).max().sort_values().tolist()
    terminal = pd.Timestamp(price_panel.index.max()).normalize()
    return [pd.Timestamp(item).normalize() for item in schedule if pd.Timestamp(item).normalize() != terminal]


def _normalize_weights(weights: pd.Series, tickers: list[str]) -> pd.Series:
    """Clip to long-only and normalize weights over the walk-forward universe."""

    result = weights.reindex(tickers).fillna(0.0).astype(float).clip(lower=0.0)
    total = float(result.sum())
    if total <= 0.0:
        return pd.Series(1.0 / len(tickers), index=tickers, dtype=float)
    return (result / total).astype(float)


def _estimate_mean_variance_weights(history: pd.DataFrame, tickers: list[str]) -> pd.Series:
    """Estimate a simple long-only diagonal mean-variance portfolio from past returns."""

    if len(history) < 5:
        return pd.Series(1.0 / len(tickers), index=tickers, dtype=float)
    means = history.reindex(columns=tickers).mean().fillna(0.0).astype(float)
    variances = history.reindex(columns=tickers).var(ddof=0).replace(0.0, np.nan).fillna(history.var(ddof=0).median())
    raw = means.clip(lower=0.0) / variances.replace(0.0, np.nan)
    raw = raw.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return _normalize_weights(raw, tickers)


def _estimate_risk_parity_weights(history: pd.DataFrame, tickers: list[str]) -> pd.Series:
    """Estimate an inverse-volatility risk-parity approximation from past returns."""

    if len(history) < 5:
        return pd.Series(1.0 / len(tickers), index=tickers, dtype=float)
    vol = history.reindex(columns=tickers).std(ddof=0).replace(0.0, np.nan)
    raw = 1.0 / vol
    raw = raw.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return _normalize_weights(raw, tickers)


def _target_sector_weights(target_weights: pd.Series, reference_frame: pd.DataFrame) -> dict[str, float]:
    """Compute target industry weights when industry data is available."""

    if "industry" not in reference_frame.columns:
        return {}
    sector_by_ticker = reference_frame.set_index("ticker")["industry"].astype(str).to_dict()
    totals: dict[str, float] = {}
    for ticker, weight in target_weights.items():
        sector = sector_by_ticker.get(str(ticker))
        if sector:
            totals[sector] = totals.get(sector, 0.0) + float(weight)
    return totals


def _max_exposure_drift(
    weights: pd.Series,
    *,
    reference_frame: pd.DataFrame,
    target_sector_weights: dict[str, float],
) -> float:
    """Compute max absolute industry exposure drift versus target sectors."""

    if not target_sector_weights or "industry" not in reference_frame.columns:
        return 0.0
    sector_by_ticker = reference_frame.set_index("ticker")["industry"].astype(str).to_dict()
    observed: dict[str, float] = {}
    for ticker, weight in weights.items():
        sector = sector_by_ticker.get(str(ticker))
        if sector:
            observed[sector] = observed.get(sector, 0.0) + float(weight)
    sectors = set(target_sector_weights) | set(observed)
    if not sectors:
        return 0.0
    return float(max(abs(observed.get(sector, 0.0) - target_sector_weights.get(sector, 0.0)) for sector in sectors))


def _historical_cvar(returns: pd.Series, alpha: float = 0.05) -> float:
    """Historical lower-tail expected shortfall."""

    cleaned = pd.to_numeric(returns, errors="coerce").dropna().astype(float)
    if cleaned.empty:
        return 0.0
    cutoff = float(cleaned.quantile(alpha))
    tail = cleaned.loc[cleaned <= cutoff]
    return float(tail.mean()) if not tail.empty else 0.0


def _metrics(
    nav: pd.Series,
    daily_returns: pd.Series,
    *,
    turnover: float,
    cost: float,
    cvar_alpha: float,
    exposure: float,
) -> dict[str, float]:
    """Compute portfolio risk and return metrics."""

    nav = nav.astype(float).reset_index(drop=True)
    daily_returns = daily_returns.astype(float).reset_index(drop=True)
    starting_nav = float(nav.iloc[0]) if not nav.empty else 0.0
    ending_nav = float(nav.iloc[-1]) if not nav.empty else 0.0
    total_return = float(ending_nav / starting_nav - 1.0) if starting_nav > 0.0 else 0.0
    annualized_return = (
        float((ending_nav / starting_nav) ** (252.0 / max(len(nav) - 1, 1)) - 1.0)
        if starting_nav > 0.0 and ending_nav > 0.0 and len(nav) > 1
        else 0.0
    )
    annualized_vol = float(daily_returns.std(ddof=0) * np.sqrt(252.0)) if len(daily_returns) > 1 else 0.0
    sharpe = float(daily_returns.mean() / daily_returns.std(ddof=0) * np.sqrt(252.0)) if daily_returns.std(ddof=0) > 0 else 0.0
    running_peak = nav.cummax()
    drawdown = nav / running_peak - 1.0
    return {
        "starting_nav": starting_nav,
        "ending_nav": ending_nav,
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": annualized_vol,
        "sharpe": sharpe,
        "max_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "turnover": float(turnover),
        "transaction_cost": float(cost),
        f"cvar_{int(round(cvar_alpha * 100))}": _historical_cvar(daily_returns, alpha=cvar_alpha),
        "max_exposure_drift": float(exposure),
    }


def _transaction_cost_components(
    *,
    nav: float,
    current_weights: pd.Series,
    target_weights: pd.Series,
    price_row: pd.Series,
    adv_shares: pd.Series,
    commission_rate: float,
    transfer_fee_rate: float,
    stamp_duty_rate: float,
    half_spread_bps: float,
    slippage_config: Any,
) -> dict[str, float]:
    """Estimate rebalance cost components from target-weight changes."""

    weights_delta = (target_weights - current_weights).reindex(current_weights.index).fillna(0.0).astype(float)
    prices = price_row.reindex(weights_delta.index).astype(float).replace(0.0, np.nan)
    signed_notional = float(nav) * weights_delta
    signed_quantities = (signed_notional / prices).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    abs_notional = signed_notional.abs()
    sell_notional = (-signed_notional).clip(lower=0.0)
    adv = adv_shares.reindex(weights_delta.index).fillna(1.0).astype(float).clip(lower=1.0)
    price_values = prices.fillna(0.0).to_numpy(dtype=float)
    quantity_values = signed_quantities.to_numpy(dtype=float)
    adv_values = adv.to_numpy(dtype=float)

    commission_cost = float(abs_notional.sum() * float(commission_rate))
    transfer_fee_cost = float(abs_notional.sum() * float(transfer_fee_rate))
    stamp_duty_cost = float(sell_notional.sum() * float(stamp_duty_rate))
    spread_cost = float(abs_notional.sum() * float(half_spread_bps) / 10000.0)
    slippage_cost = float(np.sum(estimate_slippage_array(quantity_values, price_values, adv_values, slippage_config)))
    total_transaction_cost = commission_cost + transfer_fee_cost + stamp_duty_cost + spread_cost + slippage_cost
    return {
        "commission_cost": commission_cost,
        "transfer_fee_cost": transfer_fee_cost,
        "stamp_duty_cost": stamp_duty_cost,
        "spread_cost": spread_cost,
        "slippage_cost": slippage_cost,
        "total_transaction_cost": total_transaction_cost,
    }


def _render_report(summary: dict[str, Any]) -> str:
    """Render a concise markdown report."""

    cvar_key = f"cvar_{int(round(summary['policy']['cvar_alpha'] * 100))}"
    lines = [
        "# Portfolio Quant Walk-Forward Report",
        "",
        "> Historical portfolio evaluation only. Not investment advice.",
        "",
        "## Boundary",
        "- This is not HFT alpha research.",
        "- This does not open Q1, Q2, optimizer alpha input, Alpha Registry, paper, live, broker/order, or production workflows.",
        "",
        "## Strategy Metrics",
        "",
        "| Strategy | Ann. Return | Volatility | Sharpe | Max DD | CVaR | Turnover | Cost | Exposure Drift |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for strategy, values in summary["strategies"].items():
        lines.append(
            f"| {strategy} | {values['annualized_return']:.2%} | {values['annualized_volatility']:.2%} | "
            f"{values['sharpe']:.2f} | {values['max_drawdown']:.2%} | {values[cvar_key]:.2%} | "
            f"{values['turnover']:.4f} | {values['transaction_cost']:.2f} | {values['max_exposure_drift']:.4f} |"
        )
    lines.extend(
        [
            "",
            "## No-Lookahead",
            f"- Validated: {summary['metadata']['no_lookahead_validated']}",
            f"- Frequency: {summary['metadata']['frequency']}",
            "",
        ]
    )
    return "\n".join(lines)


def run_walk_forward(
    *,
    manifest_path: str | Path,
    output_dir: str | Path,
    frequency: WalkForwardFrequency = "monthly",
    estimation_window_days: int = 63,
    min_estimation_days: int = 20,
) -> WalkForwardResult:
    """Run a historical weekly/monthly walk-forward portfolio evaluation."""

    manifest = load_backtest_manifest(manifest_path)
    holdings = load_holdings(manifest.initial_holdings)
    targets = load_target_weights(manifest.target_weights)
    state = load_portfolio_state(manifest.portfolio_state)
    config = load_app_config(
        default_path=manifest.config,
        constraints_path=manifest.constraints,
        execution_path=manifest.execution_profile,
        portfolio_state=state,
    )
    portfolio_frame = build_portfolio_frame(holdings, targets)
    tickers = portfolio_frame["ticker"].astype(str).tolist()
    market_frame = market_to_frame(load_market_snapshot(manifest.market_snapshot, tickers))
    reference_frame = reference_to_frame(load_reference_snapshot(manifest.reference, tickers))
    anchor_prices = market_frame.set_index("ticker")["close"].astype(float).reindex(tickers)
    adv_shares = market_frame.set_index("ticker")["adv_shares"].astype(float).reindex(tickers).fillna(1.0)
    returns_long = _load_returns_long(manifest.returns_file)
    returns_panel = (
        returns_long.pivot_table(index="date", columns="ticker", values="return", aggfunc="last")
        .sort_index()
        .reindex(columns=tickers)
        .fillna(0.0)
    )
    price_panel = reconstruct_price_panel(returns_long, anchor_prices=anchor_prices).reindex(columns=tickers)
    schedule = build_rebalance_schedule(price_panel, frequency=frequency)
    date_positions = {pd.Timestamp(date).normalize(): idx for idx, date in enumerate(price_panel.index)}
    target_weights = portfolio_frame.set_index("ticker")["target_weight"].reindex(tickers).fillna(0.0).astype(float)
    target_weights = _normalize_weights(target_weights, tickers)
    equal_weights = pd.Series(1.0 / len(tickers), index=tickers, dtype=float)
    target_sector_weights = _target_sector_weights(target_weights, reference_frame)

    first_prices = price_panel.iloc[0].astype(float)
    initial_quantities = portfolio_frame.set_index("ticker")["quantity"].reindex(tickers).fillna(0.0).astype(float)
    initial_cash = float(state.available_cash)
    initial_nav = float((initial_quantities * first_prices).sum() + initial_cash)
    initial_weights = (initial_quantities * first_prices / initial_nav).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    weights = {strategy: initial_weights.copy() for strategy in _STRATEGIES}
    nav = {strategy: initial_nav for strategy in _STRATEGIES}
    total_turnover = {strategy: 0.0 for strategy in _STRATEGIES}
    total_cost = {strategy: 0.0 for strategy in _STRATEGIES}
    max_exposure = {strategy: _max_exposure_drift(initial_weights, reference_frame=reference_frame, target_sector_weights=target_sector_weights) for strategy in _STRATEGIES}
    last_returns = {strategy: 0.0 for strategy in _STRATEGIES}
    nav_rows: list[dict[str, Any]] = []
    turnover_rows: list[dict[str, Any]] = []
    no_lookahead_rows: list[dict[str, Any]] = []

    portfolio_quant_config = manifest.portfolio_quant
    policy = portfolio_quant_config.policy if portfolio_quant_config is not None else None
    cvar_alpha = float(policy.cvar_alpha) if policy is not None else 0.05
    cvar_key = f"cvar_{int(round(cvar_alpha * 100))}"
    turnover_cap = (
        float(policy.turnover_cap)
        if policy is not None and policy.turnover_cap is not None
        else float(config.constraints.max_turnover or 1.0)
    )
    max_drawdown_limit = float(policy.max_drawdown_limit) if policy is not None and policy.max_drawdown_limit is not None else None
    min_cvar_limit = float(policy.min_cvar_limit) if policy is not None and policy.min_cvar_limit is not None else None

    for date in price_panel.index:
        normalized_date = pd.Timestamp(date).normalize()
        day_returns = returns_panel.loc[normalized_date].reindex(tickers).fillna(0.0).astype(float)
        for strategy in _STRATEGIES:
            period_return = float((weights[strategy] * day_returns).sum())
            nav[strategy] *= 1.0 + period_return
            last_returns[strategy] = period_return
            drifted = weights[strategy] * (1.0 + day_returns)
            weights[strategy] = _normalize_weights(drifted, tickers)
            max_exposure[strategy] = max(
                max_exposure[strategy],
                _max_exposure_drift(
                    weights[strategy],
                    reference_frame=reference_frame,
                    target_sector_weights=target_sector_weights,
                ),
            )
            nav_rows.append(
                {
                    "date": normalized_date.strftime("%Y-%m-%d"),
                    "strategy": strategy,
                    "nav": float(nav[strategy]),
                    "daily_return": period_return,
                }
            )

        if normalized_date not in schedule:
            continue

        position = date_positions[normalized_date]
        history_end_position = position - 1
        history_start_position = max(0, history_end_position - estimation_window_days + 1)
        history = returns_panel.iloc[history_start_position : history_end_position + 1].copy()
        estimation_start = returns_panel.index[history_start_position] if not history.empty else pd.NaT
        estimation_end = returns_panel.index[history_end_position] if history_end_position >= 0 else pd.NaT
        no_lookahead_passed = bool(not pd.isna(estimation_end) and pd.Timestamp(estimation_end) < normalized_date)
        no_lookahead_rows.append(
            {
                "rebalance_date": normalized_date.strftime("%Y-%m-%d"),
                "frequency": frequency,
                "estimation_start_date": pd.Timestamp(estimation_start).strftime("%Y-%m-%d") if not pd.isna(estimation_start) else "",
                "estimation_end_date": pd.Timestamp(estimation_end).strftime("%Y-%m-%d") if not pd.isna(estimation_end) else "",
                "estimation_observation_count": int(len(history)),
                "min_estimation_days": int(min_estimation_days),
                "no_lookahead_passed": no_lookahead_passed,
            }
        )
        usable_history = history if len(history) >= min_estimation_days else pd.DataFrame(columns=tickers)
        rebalance_targets = {
            "equal_weight": equal_weights,
            "mean_variance": _estimate_mean_variance_weights(usable_history, tickers),
            "risk_parity": _estimate_risk_parity_weights(usable_history, tickers),
            "cost_unaware_rebalance": target_weights,
            "portfolio_os_cost_aware_rebalance": target_weights,
        }
        for strategy, target in rebalance_targets.items():
            target = _normalize_weights(target, tickers)
            current = weights[strategy]
            desired_delta = target - current
            gross_turnover = float(desired_delta.abs().sum())
            applied_turnover = gross_turnover
            applied_target = target
            if strategy == "portfolio_os_cost_aware_rebalance" and gross_turnover > turnover_cap:
                scale = turnover_cap / gross_turnover if gross_turnover > 0.0 else 0.0
                applied_target = _normalize_weights(current + desired_delta * scale, tickers)
                applied_turnover = float((applied_target - current).abs().sum())
            cost_components = _transaction_cost_components(
                nav=float(nav[strategy]),
                current_weights=current,
                target_weights=applied_target,
                price_row=price_panel.loc[normalized_date],
                adv_shares=adv_shares,
                commission_rate=float(config.fees.commission_rate),
                transfer_fee_rate=float(config.fees.transfer_fee_rate),
                stamp_duty_rate=float(config.fees.stamp_duty_rate),
                half_spread_bps=float(config.execution.backtest_fixed_half_spread_bps),
                slippage_config=config.slippage,
            )
            transaction_cost = float(cost_components["total_transaction_cost"])
            nav[strategy] = max(float(nav[strategy] - transaction_cost), 0.0)
            weights[strategy] = applied_target
            total_turnover[strategy] += applied_turnover
            total_cost[strategy] += transaction_cost
            turnover_rows.append(
                {
                    "date": normalized_date.strftime("%Y-%m-%d"),
                    "strategy": strategy,
                    "turnover": applied_turnover,
                    **cost_components,
                    "transaction_cost": transaction_cost,
                    "post_cost_nav": float(nav[strategy]),
                    "gross_turnover_requested": gross_turnover,
                    "turnover_cap": turnover_cap,
                    "turnover_capped": bool(strategy == "portfolio_os_cost_aware_rebalance" and gross_turnover > turnover_cap),
                }
            )

    nav_curve = pd.DataFrame(nav_rows)
    no_lookahead_report = pd.DataFrame(no_lookahead_rows)
    drawdown_curve = nav_curve.copy()
    drawdown_curve["running_peak"] = drawdown_curve.groupby("strategy", observed=False)["nav"].cummax()
    drawdown_curve["drawdown"] = drawdown_curve["nav"] / drawdown_curve["running_peak"] - 1.0
    turnover_frame = pd.DataFrame(turnover_rows)
    turnover_distribution = (
        turnover_frame.groupby("strategy", observed=False)["turnover"]
        .agg(
            period_count="count",
            turnover_mean="mean",
            turnover_median="median",
            turnover_p95=lambda series: float(series.quantile(0.95)),
            turnover_max="max",
        )
        .reset_index()
        if not turnover_frame.empty
        else pd.DataFrame(columns=["strategy", "period_count", "turnover_mean", "turnover_median", "turnover_p95", "turnover_max"])
    )
    cost_attribution = (
        turnover_frame.groupby("strategy", observed=False)[
            [
                "commission_cost",
                "transfer_fee_cost",
                "stamp_duty_cost",
                "spread_cost",
                "slippage_cost",
                "total_transaction_cost",
            ]
        ]
        .sum()
        .reset_index()
        if not turnover_frame.empty
        else pd.DataFrame(
            columns=[
                "strategy",
                "commission_cost",
                "transfer_fee_cost",
                "stamp_duty_cost",
                "spread_cost",
                "slippage_cost",
                "total_transaction_cost",
            ]
        )
    )
    multi_snapshot_replay = turnover_frame.copy()

    strategy_metrics = {
        strategy: _metrics(
            nav_curve.loc[nav_curve["strategy"] == strategy, "nav"],
            nav_curve.loc[nav_curve["strategy"] == strategy, "daily_return"],
            turnover=total_turnover[strategy],
            cost=total_cost[strategy],
            cvar_alpha=cvar_alpha,
            exposure=max_exposure[strategy],
        )
        for strategy in _STRATEGIES
    }
    comparison_rows = []
    cost_aware = strategy_metrics["portfolio_os_cost_aware_rebalance"]
    for baseline in ("equal_weight", "mean_variance", "risk_parity", "cost_unaware_rebalance"):
        base = strategy_metrics[baseline]
        comparison_label = (
            "portfolio_os_cost_aware_vs_cost_unaware"
            if baseline == "cost_unaware_rebalance"
            else f"portfolio_os_cost_aware_vs_{baseline}"
        )
        comparison_rows.append(
            {
                "comparison": comparison_label,
                "left_strategy": "portfolio_os_cost_aware_rebalance",
                "right_strategy": baseline,
                "ending_nav_delta": float(cost_aware["ending_nav"] - base["ending_nav"]),
                "total_return_delta": float(cost_aware["total_return"] - base["total_return"]),
                "total_cost_delta": float(cost_aware["transaction_cost"] - base["transaction_cost"]),
                "total_turnover_delta": float(cost_aware["turnover"] - base["turnover"]),
            }
        )
    strategy_comparison = pd.DataFrame(comparison_rows)

    policy_rows = []
    for strategy, values in strategy_metrics.items():
        policy_rows.append(
            {
                "policy_name": "turnover_cap",
                "strategy": strategy,
                "observed_value": float(values["turnover"]),
                "limit_value": turnover_cap * max(len(schedule), 1),
                "breached": bool(values["turnover"] > turnover_cap * max(len(schedule), 1)),
            }
        )
        policy_rows.append(
            {
                "policy_name": "cvar_observed",
                "strategy": strategy,
                "observed_value": float(values[cvar_key]),
                "limit_value": min_cvar_limit if min_cvar_limit is not None else np.nan,
                "breached": bool(min_cvar_limit is not None and float(values[cvar_key]) < min_cvar_limit),
            }
        )
        policy_rows.append(
            {
                "policy_name": "min_cvar_limit",
                "strategy": strategy,
                "observed_value": float(values[cvar_key]),
                "limit_value": min_cvar_limit if min_cvar_limit is not None else np.nan,
                "breached": bool(min_cvar_limit is not None and float(values[cvar_key]) < min_cvar_limit),
            }
        )
        policy_rows.append(
            {
                "policy_name": "max_drawdown_limit",
                "strategy": strategy,
                "observed_value": float(values["max_drawdown"]),
                "limit_value": max_drawdown_limit if max_drawdown_limit is not None else np.nan,
                "breached": bool(max_drawdown_limit is not None and float(values["max_drawdown"]) < max_drawdown_limit),
            }
        )
        policy_rows.append(
            {
                "policy_name": "exposure_drift",
                "strategy": strategy,
                "observed_value": float(values["max_exposure_drift"]),
                "limit_value": np.nan,
                "breached": False,
            }
        )
    policy_breaches = pd.DataFrame(policy_rows)

    summary = {
        "metadata": {
            "evaluation_type": "historical_walk_forward_portfolio_quant",
            "frequency": frequency,
            "rebalance_frequency": frequency,
            "rebalance_count": int(len(schedule)),
            "estimation_window_days": int(estimation_window_days),
            "min_estimation_days": int(min_estimation_days),
            "no_lookahead_validated": bool(no_lookahead_report["no_lookahead_passed"].all())
            if not no_lookahead_report.empty
            else False,
            "not_alpha_research": True,
        },
        "policy": {
            "turnover_cap": turnover_cap,
            "cvar_alpha": cvar_alpha,
            "max_drawdown_limit": max_drawdown_limit,
            "min_cvar_limit": min_cvar_limit,
            "exposure_status": "evaluated_from_industry" if target_sector_weights else "unavailable",
            "exposure_result_fabricated": False,
        },
        "strategies": strategy_metrics,
        "comparison": comparison_rows,
        "downstream_flags": dict(_DOWNSTREAM_FLAGS),
    }
    report_markdown = _render_report(summary)

    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "walk_forward_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    nav_curve.to_csv(output / "walk_forward_nav_curve.csv", index=False)
    drawdown_curve.to_csv(output / "walk_forward_drawdown_curve.csv", index=False)
    turnover_distribution.to_csv(output / "walk_forward_turnover_distribution.csv", index=False)
    cost_attribution.to_csv(output / "walk_forward_cost_attribution.csv", index=False)
    multi_snapshot_replay.to_csv(output / "walk_forward_multi_snapshot_replay.csv", index=False)
    strategy_comparison.to_csv(output / "walk_forward_strategy_comparison.csv", index=False)
    policy_breaches.to_csv(output / "walk_forward_policy_breaches.csv", index=False)
    no_lookahead_report.to_csv(output / "walk_forward_no_lookahead_report.csv", index=False)
    (output / "walk_forward_report.md").write_text(report_markdown, encoding="utf-8")

    return WalkForwardResult(
        output_dir=output,
        nav_curve=nav_curve,
        drawdown_curve=drawdown_curve,
        turnover_distribution=turnover_distribution,
        cost_attribution=cost_attribution,
        multi_snapshot_replay=multi_snapshot_replay,
        strategy_comparison=strategy_comparison,
        policy_breaches=policy_breaches,
        no_lookahead_report=no_lookahead_report,
        summary=summary,
        report_markdown=report_markdown,
    )
