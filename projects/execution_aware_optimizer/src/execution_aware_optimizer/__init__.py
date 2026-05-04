"""Standalone Execution-Aware Portfolio Optimizer project."""

from execution_aware_optimizer.alpha_input import (
    AlphaInputReport,
    AlphaInputResult,
    clean_alpha_scores,
    load_alpha_scores,
)
from execution_aware_optimizer.cost_sensitivity import (
    CostSensitivityResultRow,
    CostSensitivityScenario,
    build_cost_sensitivity_scenarios,
    build_portfolioos_cost_overrides,
    load_cost_sensitivity_results,
)
from execution_aware_optimizer.diagnostics import (
    ConstraintDiagnostics,
    build_constraint_diagnostics,
)
from execution_aware_optimizer.execution_matrix import (
    ExecutionMatrixRow,
    execution_matrix_rows_to_frame,
    run_execution_matrix,
)
from execution_aware_optimizer.experiment_config import ExperimentConfig, load_experiment_config
from execution_aware_optimizer.ladder import LadderResultRow, run_alpha_decay_ladder
from execution_aware_optimizer.robustness_summary import (
    RobustnessSummary,
    render_execution_matrix_report,
    summarize_execution_matrix,
)
from execution_aware_optimizer.scenario_grid import (
    ExecutionScenario,
    ScenarioGridConfig,
    build_scenario_grid,
)
from execution_aware_optimizer.typed_execution_matrix import (
    TypedExecutionMatrixRow,
    TypedExecutionMatrixSummary,
    TypedQ2InputContractV2,
    render_typed_execution_matrix_report,
    run_typed_alpha_execution_matrix,
    summarize_typed_execution_matrix,
)

__all__ = [
    "AlphaInputReport",
    "AlphaInputResult",
    "CostSensitivityResultRow",
    "CostSensitivityScenario",
    "ConstraintDiagnostics",
    "ExecutionMatrixRow",
    "ExecutionScenario",
    "ExperimentConfig",
    "LadderResultRow",
    "RobustnessSummary",
    "ScenarioGridConfig",
    "TypedExecutionMatrixRow",
    "TypedExecutionMatrixSummary",
    "TypedQ2InputContractV2",
    "build_constraint_diagnostics",
    "build_cost_sensitivity_scenarios",
    "build_portfolioos_cost_overrides",
    "build_scenario_grid",
    "clean_alpha_scores",
    "execution_matrix_rows_to_frame",
    "load_alpha_scores",
    "load_cost_sensitivity_results",
    "load_experiment_config",
    "render_execution_matrix_report",
    "render_typed_execution_matrix_report",
    "run_alpha_decay_ladder",
    "run_execution_matrix",
    "run_typed_alpha_execution_matrix",
    "summarize_execution_matrix",
    "summarize_typed_execution_matrix",
]
