"""Hard optimizer constraints."""

from __future__ import annotations

import cvxpy as cp
import numpy as np
import pandas as pd

from portfolio_os.constraints.base import effective_single_name_limit
from portfolio_os.cost.fee import fee_expression
from portfolio_os.cost.slippage import slippage_expression
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.risk.model import RiskModelContext
from portfolio_os.utils.config import AppConfig


def _is_us_market(config: AppConfig) -> bool:
    """Return whether current run uses US market semantics."""

    return bool(config.trading.is_us_market)


def build_hard_constraints(
    trades: cp.Variable,
    universe: pd.DataFrame,
    config: AppConfig,
    pre_trade_nav: float,
    risk_context: RiskModelContext | None = None,
) -> list[cp.Constraint]:
    """Build the hard constraint set for the rebalance problem."""

    current_quantities = universe["quantity"].to_numpy(dtype=float)
    prices = universe["estimated_price"].to_numpy(dtype=float)
    adv_shares = universe["adv_shares"].to_numpy(dtype=float)
    post_trade_quantities = current_quantities + trades
    participation_limit_shares = config.constraints.participation_limit * adv_shares

    constraints: list[cp.Constraint] = [
        post_trade_quantities >= 0,
        trades >= -current_quantities,
        trades <= participation_limit_shares,
        trades >= -participation_limit_shares,
    ]

    if _is_us_market(config):
        blocked_mask = (~universe["tradable"]).to_numpy(dtype=bool)
    else:
        blocked_mask = (
            (~universe["tradable"])
            | universe["upper_limit_hit"]
            | universe["lower_limit_hit"]
        ).to_numpy(dtype=bool)
    buy_blacklist_mask = universe["blacklist_buy"].to_numpy(dtype=bool)
    sell_blacklist_mask = universe["blacklist_sell"].to_numpy(dtype=bool)
    locked_trade_mask = blocked_mask | (buy_blacklist_mask & sell_blacklist_mask)
    if blocked_mask.any():
        constraints.append(trades[blocked_mask] == 0)
    if buy_blacklist_mask.any():
        constraints.append(trades[buy_blacklist_mask] <= 0)
    if sell_blacklist_mask.any():
        constraints.append(trades[sell_blacklist_mask] >= 0)

    single_name_limit = effective_single_name_limit(config)
    current_weights = np.divide(
        prices * current_quantities,
        pre_trade_nav,
        out=np.zeros_like(prices, dtype=float),
        where=pre_trade_nav > 0,
    )
    per_name_limits = np.full(len(universe), single_name_limit, dtype=float)
    guardrail = config.constraints.single_name_guardrail
    if bool(guardrail.enabled):
        effective_lot_size = max(1.0, float(config.trading.lot_size))
        lot_weight_step = np.divide(
            effective_lot_size * prices,
            pre_trade_nav,
            out=np.zeros_like(prices, dtype=float),
            where=pre_trade_nav > 0,
        )
        guardrail_buffer = np.maximum(
            float(guardrail.buffer_min_weight),
            lot_weight_step * float(guardrail.buffer_lot_multiplier),
        )
        tradable_for_guardrail = ~locked_trade_mask
        per_name_limits[tradable_for_guardrail] = np.maximum(
            0.0,
            per_name_limits[tradable_for_guardrail] - guardrail_buffer[tradable_for_guardrail],
        )
    if locked_trade_mask.any():
        per_name_limits[locked_trade_mask] = np.maximum(
            per_name_limits[locked_trade_mask],
            current_weights[locked_trade_mask],
        )
    post_trade_weights = cp.multiply(prices, post_trade_quantities) / pre_trade_nav
    constraints.append(post_trade_weights <= per_name_limits)

    if bool(config.constraints.no_trade_zone.enabled):
        target_weights = universe["target_weight"].to_numpy(dtype=float)
        dead_zone_mask = np.abs(current_weights - target_weights) < float(
            config.constraints.no_trade_zone.weight_threshold
        )
        if dead_zone_mask.any():
            constraints.append(trades[dead_zone_mask] == 0)

    for industry, bound in config.constraints.industry_bounds.items():
        industry_mask = (universe["industry"] == industry).to_numpy(dtype=bool)
        if not industry_mask.any():
            continue
        industry_exposure = cp.sum(cp.multiply(prices[industry_mask], post_trade_quantities[industry_mask])) / pre_trade_nav
        current_industry_exposure = float(np.sum(prices[industry_mask] * current_quantities[industry_mask]) / pre_trade_nav)
        industry_no_trade = locked_trade_mask[industry_mask]
        effective_min = bound.min
        effective_max = bound.max
        if bool(industry_no_trade.all()):
            if effective_min is not None:
                effective_min = min(effective_min, current_industry_exposure)
            if effective_max is not None:
                effective_max = max(effective_max, current_industry_exposure)
        if bound.min is not None:
            constraints.append(industry_exposure >= effective_min)
        if bound.max is not None:
            constraints.append(industry_exposure <= effective_max)

    if config.constraints.factor_bounds:
        if risk_context is None:
            raise InputValidationError(
                "factor_bounds configured but risk model context is unavailable."
            )
        factor_name_to_index = {
            str(name): idx for idx, name in enumerate(risk_context.factor_names)
        }
        factor_exposure = cp.matmul(risk_context.factor_matrix.T, post_trade_weights)
        active_exposure = factor_exposure - risk_context.target_factor_exposure
        for factor_name, bounds in config.constraints.factor_bounds.items():
            if factor_name not in factor_name_to_index:
                raise InputValidationError(
                    f"factor_bounds includes unknown factor {factor_name!r}."
                )
            idx = factor_name_to_index[factor_name]
            if bounds.abs_min is not None:
                constraints.append(factor_exposure[idx] >= float(bounds.abs_min))
            if bounds.abs_max is not None:
                constraints.append(factor_exposure[idx] <= float(bounds.abs_max))
            if bounds.active_min is not None:
                constraints.append(active_exposure[idx] >= float(bounds.active_min))
            if bounds.active_max is not None:
                constraints.append(active_exposure[idx] <= float(bounds.active_max))

    gross_notional = cp.sum(cp.multiply(prices, cp.abs(trades)))
    constraints.append(gross_notional / pre_trade_nav <= config.constraints.max_turnover)

    if config.constraints.cash_non_negative:
        fees = fee_expression(trades, prices, config.fees)
        slippage = slippage_expression(trades, prices, adv_shares, config.slippage)
        cash_after = config.portfolio_state.available_cash - cp.sum(cp.multiply(prices, trades)) - fees - slippage
        constraints.append(cash_after >= config.portfolio_state.min_cash_buffer)

    return constraints
