"""Execution evaluation matrix for Q2 scenarios."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from portfolio_os.explain import explain_q2_unavailable

from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.ladder import BacktestRunner, LadderResultRow, run_alpha_decay_ladder
from execution_aware_optimizer.scenario_grid import (
    ConstraintLevel,
    ExecutionMode,
    ExecutionScenario,
    LiquidityBucket,
    build_scenario_grid,
)


MatrixRowStatus = Literal["observed", "unavailable"]


class ExecutionMatrixRow(LadderResultRow):
    """One scenario/layer row in the execution evaluation matrix."""

    scenario_id: str
    source_config_hash: str
    cost_bps: int
    participation_rate: float
    liquidity_bucket: LiquidityBucket
    constraint_level: ConstraintLevel
    execution_mode: ExecutionMode
    status: MatrixRowStatus
    unavailable_reason: str | None = None
    explanation: dict[str, str] | None = None


def run_execution_matrix(
    config: ExperimentConfig,
    *,
    alpha_panel: pd.DataFrame | None = None,
    backtest_runner: BacktestRunner | None = None,
) -> list[ExecutionMatrixRow]:
    """Run or plan the execution evaluation matrix.

    This function delegates portfolio construction to the existing ladder
    adapter. In the default non-execution config it records structured
    unavailable rows instead of fabricating scenario results.
    """

    rows: list[ExecutionMatrixRow] = []
    for scenario in build_scenario_grid(config):
        scenario_config = config.model_copy(deep=True)
        scenario_config.portfolioos.cost_assumption_bps = float(scenario.cost_bps)
        ladder_rows = run_alpha_decay_ladder(
            scenario_config,
            alpha_panel=alpha_panel,
            backtest_runner=backtest_runner,
        )
        rows.extend(_matrix_row_from_ladder(scenario, row) for row in ladder_rows)
    return rows


def execution_matrix_rows_to_frame(rows: list[ExecutionMatrixRow]) -> pd.DataFrame:
    """Serialize execution matrix rows to a DataFrame."""

    return pd.DataFrame([row.model_dump(mode="json") for row in rows])


def _matrix_row_from_ladder(
    scenario: ExecutionScenario,
    row: LadderResultRow,
) -> ExecutionMatrixRow:
    status: MatrixRowStatus = "observed" if _has_observed_values(row) else "unavailable"
    explanation = None
    if status == "unavailable" and row.infeasibility_reason:
        explanation = explain_q2_unavailable(row.infeasibility_reason).model_dump(mode="json")
    return ExecutionMatrixRow(
        **row.model_dump(mode="json"),
        scenario_id=scenario.scenario_id,
        source_config_hash=scenario.source_config_hash,
        cost_bps=scenario.cost_bps,
        participation_rate=scenario.participation_rate,
        liquidity_bucket=scenario.liquidity_bucket,
        constraint_level=scenario.constraint_level,
        execution_mode=scenario.execution_mode,
        status=status,
        unavailable_reason=row.infeasibility_reason if status == "unavailable" else None,
        explanation=explanation,
    )


def _has_observed_values(row: LadderResultRow) -> bool:
    return any(
        value is not None
        for value in (
            row.gross_return,
            row.net_return,
            row.turnover,
            row.estimated_transaction_cost,
            row.realized_transaction_cost,
        )
    )
