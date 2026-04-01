"""CVXPY solver wrapper."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any

import cvxpy as cp
import numpy as np
import pandas as pd

from portfolio_os.constraints.base import compute_weights_from_quantities, gross_traded_notional
from portfolio_os.constraints.hard import build_hard_constraints
from portfolio_os.cost.fee import estimate_fee_array
from portfolio_os.cost.slippage import estimate_slippage_array
from portfolio_os.domain.errors import OptimizationError
from portfolio_os.domain.models import OptimizationResult, TradeInstruction
from portfolio_os.optimizer.objective import build_objective
from portfolio_os.risk.model import RiskModelContext
from portfolio_os.utils.config import AppConfig


@dataclass
class _SolveAttempt:
    solver_name: str
    solver_kwargs: dict[str, float | int]
    status: str
    objective_value: float | None
    trade_values: np.ndarray
    constraint_residual_max: float


def _build_failure_hint(universe: pd.DataFrame, config: AppConfig, pre_trade_nav: float) -> str:
    """Build a concise hint when the solver returns an infeasible status."""

    if pre_trade_nav <= 0:
        return ""
    prices = universe["estimated_price"].to_numpy(dtype=float)
    current_quantities = universe["quantity"].to_numpy(dtype=float)
    current_weights = np.divide(
        prices * current_quantities,
        pre_trade_nav,
        out=np.zeros_like(prices, dtype=float),
        where=pre_trade_nav > 0,
    )
    no_trade_mask = (
        (~universe["tradable"].astype(bool))
        | universe["upper_limit_hit"].astype(bool)
        | universe["lower_limit_hit"].astype(bool)
        | (universe["blacklist_buy"].astype(bool) & universe["blacklist_sell"].astype(bool))
    ).to_numpy(dtype=bool)
    limit = config.effective_single_name_limit
    locked_over_limit = universe.loc[no_trade_mask & (current_weights > limit + 1e-9), "ticker"].astype(str).tolist()
    if locked_over_limit:
        return (
            " Precheck hint: locked tickers already above the single-name limit may make strict constraints infeasible. "
            f"Tickers: {', '.join(locked_over_limit)}."
        )
    return ""


def _max_constraint_violation(constraints: list[cp.Constraint]) -> float:
    """Return the maximum absolute constraint residual for the solved problem."""

    max_violation = 0.0
    for constraint in constraints:
        try:
            raw = constraint.violation()
        except Exception:
            continue
        if raw is None:
            continue
        violation = float(np.max(np.abs(np.asarray(raw, dtype=float))))
        if violation > max_violation:
            max_violation = violation
    return max_violation


def _solver_chain(config: AppConfig) -> list[tuple[str, dict[str, float | int]]]:
    """Build the solver candidate chain based on config."""

    normalized_name = str(config.solver.name).strip().upper()
    scs_kwargs: dict[str, float | int] = {
        "max_iters": int(config.solver.max_iters),
        "eps": float(config.solver.eps),
    }
    clarabel_kwargs: dict[str, float | int] = {
        "max_iter": int(config.solver.max_iters),
        "tol_gap_abs": float(config.solver.eps),
        "tol_gap_rel": float(config.solver.eps),
        "tol_feas": float(config.solver.eps),
    }
    if normalized_name == "SCS":
        return [("SCS", scs_kwargs)]
    if normalized_name == "CLARABEL":
        return [("CLARABEL", clarabel_kwargs), ("SCS", scs_kwargs)]
    return [(normalized_name, {}), ("SCS", scs_kwargs)]


def _solve_once(
    *,
    problem: cp.Problem,
    constraints: list[cp.Constraint],
    trades: cp.Variable,
    solver_name: str,
    solver_kwargs: dict[str, float | int],
) -> _SolveAttempt:
    """Run one solver attempt and return normalized diagnostics."""

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Solution may be inaccurate.*",
            category=UserWarning,
        )
        try:
            problem.solve(solver=solver_name, verbose=False, **solver_kwargs)
        except TypeError:
            # Best-effort fallback for solver-specific keyword mismatches.
            problem.solve(solver=solver_name, verbose=False)
    status = str(problem.status)
    if status not in {cp.OPTIMAL, cp.OPTIMAL_INACCURATE}:
        raise OptimizationError(f"status={status}")
    if trades.value is None:
        raise OptimizationError("solver returned empty trade vector")
    return _SolveAttempt(
        solver_name=solver_name,
        solver_kwargs=solver_kwargs,
        status=status,
        objective_value=float(problem.value) if problem.value is not None else None,
        trade_values=np.asarray(trades.value, dtype=float),
        constraint_residual_max=_max_constraint_violation(constraints),
    )


def _safe_scalar(value: Any) -> float:
    """Convert expression values into finite scalar floats."""

    if value is None:
        return 0.0
    array = np.asarray(value, dtype=float)
    if array.size == 0:
        return 0.0
    scalar = float(array.reshape(-1)[0])
    if not np.isfinite(scalar):
        return 0.0
    return scalar


def _component_names_for_mode(config: AppConfig) -> tuple[str, ...]:
    """Return canonical objective component order for the active mode."""

    if config.risk_model.enabled:
        if str(config.risk_model.integration_mode).strip().lower() == "augment":
            return (
                "risk_term",
                "tracking_error",
                "transaction_cost",
                "target_deviation",
                "transaction_fee",
                "turnover_penalty",
                "slippage_penalty",
            )
        return ("risk_term", "tracking_error", "transaction_cost")
    return ("target_deviation", "transaction_fee", "turnover_penalty", "slippage_penalty")


def _component_weight(config: AppConfig, component_name: str) -> float:
    """Resolve configured weight for one objective component."""

    weights = config.objective_weights
    if component_name == "risk_term":
        return float(weights.risk_term or 0.0)
    if component_name == "tracking_error":
        return float(weights.tracking_error or 0.0)
    if component_name == "transaction_cost":
        return float(weights.transaction_cost or 0.0)
    if component_name == "target_deviation":
        return float(weights.target_deviation or 0.0)
    if component_name == "transaction_fee":
        return float(weights.transaction_fee or 0.0)
    if component_name == "turnover_penalty":
        return float(weights.turnover_penalty or 0.0)
    if component_name == "slippage_penalty":
        return float(weights.slippage_penalty or 0.0)
    return 0.0


def _build_objective_decomposition(
    *,
    config: AppConfig,
    component_exprs: dict[str, cp.Expression],
    objective_value: float | None,
) -> dict[str, Any]:
    """Build objective decomposition payload for audit diagnostics."""

    mode = "risk" if config.risk_model.enabled else "legacy"
    component_payload: dict[str, dict[str, float]] = {}
    abs_weighted_sum = 0.0
    for component_name in _component_names_for_mode(config):
        expression = component_exprs.get(component_name)
        raw_value = _safe_scalar(getattr(expression, "value", None) if expression is not None else None)
        weight = _component_weight(config, component_name)
        weighted_value = raw_value * weight
        abs_weighted_sum += abs(weighted_value)
        component_payload[component_name] = {
            "raw_value": raw_value,
            "weight": weight,
            "weighted_value": weighted_value,
            "share_abs_weighted": 0.0,
        }
    if abs_weighted_sum > 0.0:
        for payload in component_payload.values():
            payload["share_abs_weighted"] = abs(float(payload["weighted_value"])) / abs_weighted_sum
    return {
        "mode": mode,
        "integration_mode": (
            str(config.risk_model.integration_mode).strip().lower() if config.risk_model.enabled else "legacy"
        ),
        "objective_value": _safe_scalar(objective_value),
        "abs_weighted_sum": float(abs_weighted_sum),
        "components": component_payload,
    }


def solve_rebalance_problem(
    universe: pd.DataFrame,
    config: AppConfig,
    *,
    risk_context: RiskModelContext | None = None,
) -> OptimizationResult:
    """Solve the convex single-period rebalance problem."""

    prices = universe["estimated_price"].to_numpy(dtype=float)
    current_quantities = universe["quantity"].to_numpy(dtype=float)
    target_weights = universe["target_weight"].to_numpy(dtype=float)
    pre_trade_nav = float(np.sum(current_quantities * prices) + config.portfolio_state.available_cash)
    if pre_trade_nav <= 0:
        raise OptimizationError("Pre-trade NAV must be positive.")

    trades = cp.Variable(len(universe))
    objective, component_exprs = build_objective(
        trades,
        universe,
        config,
        pre_trade_nav,
        risk_context=risk_context,
    )
    constraints = build_hard_constraints(
        trades,
        universe,
        config,
        pre_trade_nav,
        risk_context=risk_context,
    )
    problem = cp.Problem(cp.Minimize(objective), constraints)
    attempts = _solver_chain(config)
    acceptable_residual = max(float(config.solver.eps) * 5.0, 0.1)
    selected_attempt: _SolveAttempt | None = None
    last_error: str | None = None
    for solver_name, solver_kwargs in attempts:
        try:
            attempt = _solve_once(
                problem=problem,
                constraints=constraints,
                trades=trades,
                solver_name=solver_name,
                solver_kwargs=solver_kwargs,
            )
        except OptimizationError as exc:
            last_error = f"{solver_name}: {exc}"
            continue
        except Exception as exc:  # pragma: no cover
            last_error = f"{solver_name}: {exc}"
            continue
        if attempt.constraint_residual_max > acceptable_residual:
            last_error = (
                f"{solver_name}: residual {attempt.constraint_residual_max:.6g} "
                f"exceeds tolerance {acceptable_residual:.6g}"
            )
            continue
        selected_attempt = attempt
        break

    if selected_attempt is None:
        hint = _build_failure_hint(universe, config, pre_trade_nav)
        reason = last_error or "all solver attempts failed"
        raise OptimizationError(f"Optimization failed ({reason}).{hint}")

    quantities = selected_attempt.trade_values
    fees = estimate_fee_array(quantities, prices, config.fees)
    slippage = estimate_slippage_array(
        quantities,
        prices,
        universe["adv_shares"].to_numpy(dtype=float),
        config.slippage,
    )
    post_trade_quantities = current_quantities + quantities
    cash_after = (
        config.portfolio_state.available_cash
        - float(np.sum(prices * quantities))
        - float(np.sum(fees))
        - float(np.sum(slippage))
    )
    current_weights = compute_weights_from_quantities(current_quantities, prices, pre_trade_nav)
    post_trade_weights = compute_weights_from_quantities(post_trade_quantities, prices, pre_trade_nav)

    instructions = [
        TradeInstruction(
            ticker=str(ticker),
            quantity=float(quantity),
            estimated_price=float(price),
            current_weight=float(current_weight),
            target_weight=float(target_weight),
            reason_tags=["optimized"],
        )
        for ticker, quantity, price, current_weight, target_weight in zip(
            universe["ticker"],
            quantities,
            prices,
            current_weights,
            target_weights,
            strict=True,
        )
    ]
    return OptimizationResult(
        status=selected_attempt.status,
        objective_value=(
            float(selected_attempt.objective_value)
            if selected_attempt.objective_value is not None
            else 0.0
        ),
        instructions=instructions,
        solver_used=selected_attempt.solver_name,
        solver_fallback_used=len(attempts) > 1 and selected_attempt.solver_name != attempts[0][0],
        constraint_residual_max=float(selected_attempt.constraint_residual_max),
        gross_traded_notional=gross_traded_notional(quantities, prices),
        estimated_total_fee=float(np.sum(fees)),
        estimated_total_slippage=float(np.sum(slippage)),
        post_trade_cash_estimate=float(cash_after),
        pre_trade_nav=pre_trade_nav,
        current_weights={
            str(ticker): float(weight)
            for ticker, weight in zip(universe["ticker"], current_weights, strict=True)
        },
        target_weights={
            str(ticker): float(weight)
            for ticker, weight in zip(universe["ticker"], target_weights, strict=True)
        },
        post_trade_weights={
            str(ticker): float(weight)
            for ticker, weight in zip(universe["ticker"], post_trade_weights, strict=True)
        },
        objective_decomposition=_build_objective_decomposition(
            config=config,
            component_exprs=component_exprs,
            objective_value=selected_attempt.objective_value,
        ),
    )
