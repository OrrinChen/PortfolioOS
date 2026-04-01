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

    target_deviation = cp.sum_squares(post_trade_weights - target_weights)
    transaction_fee = fee_expression(trades, prices, config.fees)
    turnover_penalty = turnover_penalty_expression(trades, prices, pre_trade_nav)
    slippage_penalty = slippage_expression(trades, prices, adv_shares, config.slippage)
    transaction_cost = transaction_fee + slippage_penalty

    legacy_objective = (
        float(config.objective_weights.target_deviation or 0.0) * target_deviation
        + float(config.objective_weights.transaction_fee or 0.0) * transaction_fee
        + float(config.objective_weights.turnover_penalty or 0.0) * turnover_penalty
        + float(config.objective_weights.slippage_penalty or 0.0) * slippage_penalty
    )

    if config.risk_model.enabled:
        if risk_context is None:
            raise InputValidationError("Risk mode is enabled but risk context is missing.")
        sigma_psd = cp.psd_wrap(risk_context.sigma)
        risk_term = cp.quad_form(post_trade_weights, sigma_psd)
        tracking_error = cp.quad_form(post_trade_weights - target_weights, sigma_psd)
        risk_objective = (
            config.objective_weights.risk_term * risk_term
            + config.objective_weights.tracking_error * tracking_error
            + config.objective_weights.transaction_cost * transaction_cost
        )
        if config.risk_model.integration_mode == "augment":
            objective = legacy_objective + risk_objective
            return objective, {
                "risk_term": risk_term,
                "tracking_error": tracking_error,
                "transaction_cost": transaction_cost,
                "target_deviation": target_deviation,
                "transaction_fee": transaction_fee,
                "turnover_penalty": turnover_penalty,
                "slippage_penalty": slippage_penalty,
            }
        return risk_objective, {
            "risk_term": risk_term,
            "tracking_error": tracking_error,
            "transaction_cost": transaction_cost,
        }

    return legacy_objective, {
        "target_deviation": target_deviation,
        "transaction_fee": transaction_fee,
        "turnover_penalty": turnover_penalty,
        "slippage_penalty": slippage_penalty,
    }
