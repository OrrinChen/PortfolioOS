"""Constraint diagnostics for execution-aware optimizer outputs."""

from __future__ import annotations

from pydantic import BaseModel, Field

from execution_aware_optimizer.ladder import LadderResultRow


class ConstraintDiagnostics(BaseModel):
    """Serializable diagnostics summary for a ladder run."""

    binding_constraints: list[str] = Field(default_factory=list)
    rejected_symbols: list[str] = Field(default_factory=list)
    alpha_lost_due_to_constraints: float | None = None
    turnover_budget_usage: float | None = None
    liquidity_budget_usage: float | None = None
    cost_drag: float | None = None
    sector_exposure_drift: dict[str, float] = Field(default_factory=dict)
    infeasible_rebalance_dates: list[str] = Field(default_factory=list)
    available_metrics: dict[str, float | int | str] = Field(default_factory=dict)
    todos: list[str] = Field(default_factory=list)


def _unique_preserve_order(values: list[str]) -> list[str]:
    """Return unique strings without sorting away severity/order signal."""

    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def build_constraint_diagnostics(
    rows: list[LadderResultRow],
    *,
    turnover_budget: float | None = None,
    liquidity_budget: float | None = None,
) -> ConstraintDiagnostics:
    """Summarize available constraint and cost diagnostics from ladder rows."""

    binding_constraints = _unique_preserve_order(
        [constraint for row in rows for constraint in row.binding_constraints]
    )
    rejected_symbols = _unique_preserve_order([symbol for row in rows for symbol in row.rejected_symbols])
    infeasible_dates = _unique_preserve_order(
        [
            row.date.isoformat()
            for row in rows
            if row.date is not None and row.infeasibility_reason
        ]
    )

    cost_drags = [
        float(row.gross_return) - float(row.net_return)
        for row in rows
        if row.gross_return is not None and row.net_return is not None
    ]
    turnovers = [float(row.turnover) for row in rows if row.turnover is not None]
    max_turnover = max(turnovers) if turnovers else None
    max_liquidity_usage = None
    if liquidity_budget is not None:
        liquidity_cost_rows = [
            float(row.estimated_transaction_cost)
            for row in rows
            if row.estimated_transaction_cost is not None and row.layer_name == "liquidity_constrained"
        ]
        max_liquidity_usage = max(liquidity_cost_rows) / liquidity_budget if liquidity_cost_rows and liquidity_budget else None

    todos = [
        "PortfolioOS optimizer dual values / shadow prices are not exposed yet; report slack/usage metrics instead.",
        "Add explicit liquidity constraint usage once PortfolioOS exports per-name participation slack.",
        "Add risk exposure attribution once PortfolioOS exports rebalance-level factor exposures in a stable schema.",
    ]

    return ConstraintDiagnostics(
        binding_constraints=binding_constraints,
        rejected_symbols=rejected_symbols,
        alpha_lost_due_to_constraints=None,
        turnover_budget_usage=(
            max_turnover / float(turnover_budget)
            if max_turnover is not None and turnover_budget not in {None, 0}
            else None
        ),
        liquidity_budget_usage=max_liquidity_usage,
        cost_drag=(sum(cost_drags) / float(len(cost_drags)) if cost_drags else None),
        sector_exposure_drift={},
        infeasible_rebalance_dates=infeasible_dates,
        available_metrics={
            "row_count": int(len(rows)),
            "infeasible_row_count": int(sum(1 for row in rows if row.infeasibility_reason)),
            **({"max_turnover": float(max_turnover)} if max_turnover is not None else {}),
        },
        todos=todos,
    )
