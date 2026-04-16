"""Objective construction for the rebalance problem."""

from __future__ import annotations

import cvxpy as cp
import pandas as pd

from portfolio_os.constraints.soft import turnover_penalty_expression
from portfolio_os.cost.fee import fee_expression
from portfolio_os.cost.slippage import slippage_expression
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.risk.model import RiskModelContext
from portfolio_os.utils.config import AppConfig


def _resolve_decision_horizon_scale(universe: pd.DataFrame, config: AppConfig) -> float:
    """Return the annualized-to-period scaling factor implied by the universe."""

    if "decision_horizon_days" not in universe.columns:
        return 1.0
    horizon_series = pd.to_numeric(universe["decision_horizon_days"], errors="coerce").dropna()
    if horizon_series.empty:
        return 1.0
    unique_values = {float(value) for value in horizon_series.to_list()}
    if len(unique_values) != 1:
        raise InputValidationError("decision_horizon_days must resolve to one unique positive value.")
    horizon_days = unique_values.pop()
    if horizon_days <= 0.0:
        raise InputValidationError("decision_horizon_days must be positive.")
    annualization_factor = float(config.risk_model.annualization_factor)
    if annualization_factor <= 0.0:
        raise InputValidationError("risk_model.annualization_factor must be positive.")
    return float(horizon_days / annualization_factor)


def build_objective(
    trades: cp.Variable,
    universe: pd.DataFrame,
    config: AppConfig,
    pre_trade_nav: float,
    risk_context: RiskModelContext | None = None,
) -> tuple[cp.Expression, dict[str, cp.Expression]]:
    """Build the weighted objective and its component expressions."""

    current_quantities = universe["quantity"].to_numpy(dtype=float)
    prices = universe["estimated_price"].to_numpy(dtype=float)
    adv_shares = universe["adv_shares"].to_numpy(dtype=float)
    target_weights = universe["target_weight"].to_numpy(dtype=float)

    post_trade_quantities = current_quantities + trades
    post_trade_weights = cp.multiply(prices, post_trade_quantities) / pre_trade_nav
    expected_return = (
        universe["expected_return"].to_numpy(dtype=float)
        if "expected_return" in universe.columns
        else None
    )

    target_deviation = cp.sum_squares(post_trade_weights - target_weights)
    transaction_fee = fee_expression(trades, prices, config.fees)
    turnover_penalty = turnover_penalty_expression(trades, prices, pre_trade_nav)
    slippage_penalty = slippage_expression(trades, prices, adv_shares, config.slippage)
    transaction_cost_currency = transaction_fee + slippage_penalty
    transaction_cost_fraction = transaction_cost_currency / pre_trade_nav
    transaction_cost_mode = str(config.objective_weights.transaction_cost_objective_mode).strip().lower()
    transaction_cost = (
        transaction_cost_fraction
        if transaction_cost_mode == "nav_fraction"
        else transaction_cost_currency
    )
    alpha_reward = (
        cp.sum(cp.multiply(expected_return, post_trade_weights))
        if expected_return is not None
        else cp.Constant(0.0)
    )
    alpha_penalty = float(config.objective_weights.alpha_weight or 0.0) * alpha_reward

    legacy_objective = (
        float(config.objective_weights.target_deviation or 0.0) * target_deviation
        + float(config.objective_weights.transaction_fee or 0.0) * transaction_fee
        + float(config.objective_weights.turnover_penalty or 0.0) * turnover_penalty
        + float(config.objective_weights.slippage_penalty or 0.0) * slippage_penalty
    )

    if config.risk_model.enabled:
        if risk_context is None:
            raise InputValidationError("Risk mode is enabled but risk context is missing.")
        sigma_scale = _resolve_decision_horizon_scale(universe, config)
        sigma_psd = cp.psd_wrap(risk_context.sigma * sigma_scale)
        risk_term = cp.quad_form(post_trade_weights, sigma_psd)
        tracking_error = cp.quad_form(post_trade_weights - target_weights, sigma_psd)
        economic_objective = (
            config.objective_weights.risk_term * risk_term
            + config.objective_weights.tracking_error * tracking_error
            + config.objective_weights.transaction_cost * transaction_cost
        )
        return economic_objective - alpha_penalty, {
            "risk_term": risk_term,
            "tracking_error": tracking_error,
            "transaction_cost": transaction_cost,
            "transaction_cost_currency": transaction_cost_currency,
            "transaction_cost_fraction": transaction_cost_fraction,
            "alpha_reward": alpha_reward,
        }

    return legacy_objective - alpha_penalty, {
        "target_deviation": target_deviation,
        "transaction_fee": transaction_fee,
        "turnover_penalty": turnover_penalty,
        "slippage_penalty": slippage_penalty,
        "alpha_reward": alpha_reward,
    }
