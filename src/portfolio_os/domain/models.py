"""Core domain models for the PortfolioOS MVP."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from portfolio_os.domain.enums import FindingCategory, FindingSeverity, OrderSide, RepairStatus, Urgency


class Holding(BaseModel):
    """Current position in one security."""

    ticker: str
    quantity: int
    avg_cost: float | None = None


class TargetWeight(BaseModel):
    """Target portfolio weight for one security."""

    ticker: str
    target_weight: float


class MarketRow(BaseModel):
    """Market data row used by the optimizer and compliance checks."""

    ticker: str
    close: float
    vwap: float | None = None
    adv_shares: float
    tradable: bool
    upper_limit_hit: bool
    lower_limit_hit: bool


class MarketSnapshot(BaseModel):
    """Collection of market rows."""

    rows: list[MarketRow]


class ReferenceRow(BaseModel):
    """Reference data and static flags for one security."""

    ticker: str
    industry: str
    blacklist_buy: bool = False
    blacklist_sell: bool = False
    benchmark_weight: float | None = None
    manager_aggregate_qty: float | None = None
    issuer_total_shares: float | None = None


class ReferenceSnapshot(BaseModel):
    """Collection of reference rows."""

    rows: list[ReferenceRow]


class PortfolioState(BaseModel):
    """Account-level portfolio state."""

    account_id: str
    as_of_date: str
    available_cash: float
    min_cash_buffer: float = 0.0
    account_type: str


class TradeInstruction(BaseModel):
    """Continuous or repaired trade instruction."""

    ticker: str
    quantity: float
    estimated_price: float
    current_weight: float
    target_weight: float
    reason_tags: list[str] = Field(default_factory=list)


class Order(BaseModel):
    """Executable order row."""

    ticker: str
    side: OrderSide
    quantity: int
    estimated_price: float
    estimated_notional: float
    estimated_fee: float
    estimated_slippage: float
    urgency: str
    reason: str


class Basket(BaseModel):
    """Exportable order basket with aggregate cost metrics."""

    orders: list[Order] = Field(default_factory=list)
    gross_traded_notional: float = 0.0
    total_fee: float = 0.0
    total_slippage: float = 0.0
    total_cost: float = 0.0


class ComplianceFinding(BaseModel):
    """Structured finding emitted by compliance checks."""

    code: str
    category: FindingCategory
    severity: FindingSeverity
    ticker: str | None = None
    message: str
    rule_source: str
    blocking: bool = False
    repair_status: RepairStatus = RepairStatus.NOT_NEEDED
    details: dict[str, Any] = Field(default_factory=dict)

    @property
    def rule_code(self) -> str:
        """Backward-compatible alias used by existing code paths."""

        return self.code


class ConstraintSnapshot(BaseModel):
    """Resolved constraint template included in the audit payload."""

    source_path: str | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    effective_single_name_limit: float | None = None


class OptimizationResult(BaseModel):
    """Result of the convex rebalance optimization."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    status: str
    objective_value: float
    instructions: list[TradeInstruction]
    solver_used: str | None = None
    solver_fallback_used: bool = False
    constraint_residual_max: float | None = None
    gross_traded_notional: float
    estimated_total_fee: float
    estimated_total_slippage: float
    post_trade_cash_estimate: float
    pre_trade_nav: float
    current_weights: dict[str, float]
    target_weights: dict[str, float]
    post_trade_weights: dict[str, float]
    objective_decomposition: dict[str, Any] = Field(default_factory=dict)


class RunArtifacts(BaseModel):
    """Files produced by one CLI run."""

    run_id: str
    output_dir: str
    orders_path: str
    orders_oms_path: str
    audit_path: str
    summary_path: str
    benchmark_json_path: str
    benchmark_markdown_path: str
    handoff_checklist_path: str
    manifest_path: str
    created_at: str


class BenchmarkMetrics(BaseModel):
    """Unified static benchmark metrics for one rebalance strategy."""

    strategy_name: str
    pre_trade_nav: float
    cash_before: float
    cash_after: float
    target_deviation_before: float
    target_deviation_after: float
    target_deviation_improvement: float
    portfolio_variance_before: float = 0.0
    portfolio_variance_after: float = 0.0
    tracking_error_variance_before: float = 0.0
    tracking_error_variance_after: float = 0.0
    gross_traded_notional: float
    turnover: float
    estimated_fee_total: float
    estimated_slippage_total: float
    estimated_total_cost: float
    buy_order_count: int
    sell_order_count: int
    blocked_trade_count: int
    compliance_finding_count: int


class BenchmarkComparison(BaseModel):
    """Serialized comparison across benchmark strategies."""

    strategies: list[BenchmarkMetrics]
    comparison_summary: dict[str, Any] = Field(default_factory=dict)


class ReplayArtifacts(BaseModel):
    """Files produced by one replay-suite run."""

    run_id: str
    output_dir: str
    sample_results_dir: str
    suite_results_path: str
    suite_summary_path: str
    manifest_path: str
    created_at: str


class ScenarioArtifacts(BaseModel):
    """Files produced by one scenario-suite run."""

    run_id: str
    output_dir: str
    scenario_results_dir: str
    scenario_comparison_json_path: str
    scenario_comparison_markdown_path: str
    decision_pack_path: str
    manifest_path: str
    created_at: str


class ApprovalArtifacts(BaseModel):
    """Files produced by one approval/freeze run."""

    run_id: str
    output_dir: str
    approval_record_path: str
    approval_summary_path: str
    freeze_manifest_path: str
    final_orders_path: str
    final_orders_oms_path: str
    final_audit_path: str
    final_summary_path: str
    handoff_checklist_path: str
    manifest_path: str
    created_at: str


class ExecutionArtifacts(BaseModel):
    """Files produced by one execution-simulation run."""

    run_id: str
    output_dir: str
    execution_report_json_path: str
    execution_report_markdown_path: str
    execution_fills_path: str
    execution_child_orders_path: str
    handoff_checklist_path: str
    manifest_path: str
    created_at: str


class ScenarioMetrics(BaseModel):
    """Unified scenario metrics for decision-pack comparison."""

    scenario_id: str
    scenario_label: str
    constraints_template: str
    execution_profile: str
    pre_trade_nav: float
    cash_before: float
    cash_after: float
    target_deviation_after: float
    gross_traded_notional: float
    turnover: float
    estimated_fee_total: float
    estimated_slippage_total: float
    estimated_total_cost: float
    buy_order_count: int
    sell_order_count: int
    blocked_trade_count: int
    blocking_finding_count: int
    warning_finding_count: int
    data_quality_finding_count: int = 0
    regulatory_finding_count: int = 0
    tradability_finding_count: int = 0
