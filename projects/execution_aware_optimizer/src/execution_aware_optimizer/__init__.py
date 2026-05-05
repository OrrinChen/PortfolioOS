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
from execution_aware_optimizer.revision_marginal_value_gate import (
    render_revision_marginal_value_report,
    run_revision_marginal_value_gate,
    write_revision_marginal_value_artifacts,
)
from execution_aware_optimizer.revision_marginal_value_schema import (
    RevisionMarginalValueInput,
    RevisionMarginalValueResult,
    RevisionMarginalValueSummary,
)
from execution_aware_optimizer.scenario_grid import (
    ExecutionScenario,
    ScenarioGridConfig,
    build_scenario_grid,
)
from execution_aware_optimizer.sue_execution_survival_attribution import (
    build_sue_execution_survival_attribution,
    render_sue_execution_survival_attribution_report,
    write_sue_execution_survival_attribution_artifacts,
)
from execution_aware_optimizer.sue_execution_survival_attribution_schema import (
    SueExecutionSurvivalAttribution,
    SueLayerAttribution,
)
from execution_aware_optimizer.sue_typed_q2_survival import (
    run_sue_typed_q2_survival,
    write_sue_typed_q2_survival_artifacts,
)
from execution_aware_optimizer.sue_typed_q2_survival_schema import (
    SueTypedQ2SurvivalInput,
    SueTypedQ2SurvivalResult,
)
from execution_aware_optimizer.typed_execution_matrix import (
    TypedExecutionMatrixRow,
    TypedExecutionMatrixSummary,
    TypedQ2InputContractV2,
    render_typed_execution_matrix_report,
    run_typed_alpha_execution_matrix,
    summarize_typed_execution_matrix,
)
from execution_aware_optimizer.typed_adapter_schema import (
    TypedQ2AdapterInput,
    TypedQ2AdapterMatrixRow,
    TypedQ2AdapterResult,
)
from execution_aware_optimizer.typed_portfolioos_adapter import (
    run_typed_q2_adapter,
    write_typed_q2_adapter_artifacts,
)
from execution_aware_optimizer.typed_expected_return_injection import (
    run_typed_expected_return_injection,
    write_typed_expected_return_injection_artifacts,
)
from execution_aware_optimizer.typed_injection_schema import (
    TypedExpectedReturnInjectionInput,
    TypedExpectedReturnInjectionResult,
)
from execution_aware_optimizer.typed_optimizer_response import (
    run_typed_optimizer_response_acceptance,
    write_typed_optimizer_response_artifacts,
)
from execution_aware_optimizer.typed_optimizer_response_schema import (
    TypedOptimizerResponseInput,
    TypedOptimizerResponseResult,
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
    "RevisionMarginalValueInput",
    "RevisionMarginalValueResult",
    "RevisionMarginalValueSummary",
    "ScenarioGridConfig",
    "SueExecutionSurvivalAttribution",
    "SueLayerAttribution",
    "SueTypedQ2SurvivalInput",
    "SueTypedQ2SurvivalResult",
    "TypedExecutionMatrixRow",
    "TypedExecutionMatrixSummary",
    "TypedQ2AdapterInput",
    "TypedQ2AdapterMatrixRow",
    "TypedQ2AdapterResult",
    "TypedQ2InputContractV2",
    "TypedExpectedReturnInjectionInput",
    "TypedExpectedReturnInjectionResult",
    "TypedOptimizerResponseInput",
    "TypedOptimizerResponseResult",
    "build_constraint_diagnostics",
    "build_cost_sensitivity_scenarios",
    "build_portfolioos_cost_overrides",
    "build_scenario_grid",
    "build_sue_execution_survival_attribution",
    "clean_alpha_scores",
    "execution_matrix_rows_to_frame",
    "load_alpha_scores",
    "load_cost_sensitivity_results",
    "load_experiment_config",
    "render_execution_matrix_report",
    "render_revision_marginal_value_report",
    "render_sue_execution_survival_attribution_report",
    "render_typed_execution_matrix_report",
    "run_alpha_decay_ladder",
    "run_execution_matrix",
    "run_revision_marginal_value_gate",
    "run_typed_alpha_execution_matrix",
    "run_typed_expected_return_injection",
    "run_typed_optimizer_response_acceptance",
    "run_sue_typed_q2_survival",
    "run_typed_q2_adapter",
    "summarize_execution_matrix",
    "summarize_typed_execution_matrix",
    "write_typed_q2_adapter_artifacts",
    "write_typed_expected_return_injection_artifacts",
    "write_typed_optimizer_response_artifacts",
    "write_sue_typed_q2_survival_artifacts",
    "write_sue_execution_survival_attribution_artifacts",
    "write_revision_marginal_value_artifacts",
]
