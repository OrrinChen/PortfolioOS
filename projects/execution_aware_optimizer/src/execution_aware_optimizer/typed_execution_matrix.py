"""Typed AlphaView execution matrix for Q2.

This module consumes Promotion Gate v2 / Alpha Projection Bridge artifacts and
builds auditable Q2 scenario rows. It does not run PortfolioOS optimizers or
fabricate returns when typed execution hooks are unavailable.
"""

from __future__ import annotations

from collections import Counter
import hashlib
import json
from typing import Any, Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator

from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.scenario_grid import (
    ConstraintLevel,
    ExecutionMode,
    LiquidityBucket,
    build_scenario_grid,
)


ProjectionPolicy = Literal[
    "event_window_only",
    "event_window_decay",
    "to_next_event",
    "rebalance_period_projection",
]
TypedAbstainPolicy = Literal["explicit_abstain", "coverage_threshold", "stale_view_abstain"]
AlphaFamily = Literal["SUE", "revision", "SUE_plus_revision"]
TypedMatrixStatus = Literal["observed", "unavailable"]


class TypedQ2InputContractV2(BaseModel):
    """Local Q2 view of Promotion Gate v2 input contracts."""

    model_config = ConfigDict(extra="forbid")

    bundle_id: str
    alpha_view_id: str
    input_type: Literal["projected_expected_return_panel"] = "projected_expected_return_panel"
    expected_return_panel_artifact: str
    projection_manifest_hash: str
    alpha_projection_diagnostics_artifact: str
    alpha_abstain_report_artifact: str
    allowed_consumer: str = "projects/execution_aware_optimizer"
    direct_q2_execution_allowed: bool = False

    @model_validator(mode="after")
    def require_read_only_q2_contract(self) -> "TypedQ2InputContractV2":
        if self.allowed_consumer != "projects/execution_aware_optimizer":
            raise ValueError("Q2InputContractV2 is not addressed to execution_aware_optimizer")
        if self.direct_q2_execution_allowed is not False:
            raise ValueError("Q2 typed matrix cannot consume contracts that allow direct Q2 execution")
        return self


class TypedExecutionMatrixRow(BaseModel):
    """One typed alpha scenario row in Q2."""

    scenario_id: str
    source_config_hash: str
    cost_bps: int
    participation_rate: float
    liquidity_bucket: LiquidityBucket
    constraint_level: ConstraintLevel
    execution_mode: ExecutionMode
    projection_policy: ProjectionPolicy
    abstain_policy: TypedAbstainPolicy
    alpha_family: AlphaFamily
    q2_input_contract_version: Literal["v2"] = "v2"
    status: TypedMatrixStatus
    unavailable_reason: str | None = None
    active_rebalance_count: int
    active_name_count: int
    gross_to_net_retention: float | None = None
    turnover: float | None = None
    expected_return_used_share: float
    cost_drag: float | None = None
    constraint_repair_retention: float | None = None
    abstain_count: int
    sign_consistency: str
    view_overlap: str


class TypedExecutionMatrixSummary(BaseModel):
    """Aggregate status for typed Q2 execution matrix rows."""

    total_rows: int
    observed_rows: int
    unavailable_rows: int
    unique_source_config_hashes: int
    unavailable_reason_counts: dict[str, int] = Field(default_factory=dict)


def run_typed_alpha_execution_matrix(
    *,
    config: ExperimentConfig,
    q2_input_contract_v2: dict[str, Any] | TypedQ2InputContractV2,
    projection_manifest: dict[str, Any],
    expected_return_panel: Sequence[dict[str, Any]],
    projection_diagnostics: Sequence[dict[str, Any]],
    alpha_abstain_report: Sequence[dict[str, Any]],
    projection_policies: Sequence[ProjectionPolicy] | None = None,
    abstain_policies: Sequence[TypedAbstainPolicy] | None = None,
    alpha_families: Sequence[AlphaFamily] | None = None,
) -> list[TypedExecutionMatrixRow]:
    """Build typed Q2 scenario rows from projected AlphaView artifacts."""

    contract = (
        q2_input_contract_v2
        if isinstance(q2_input_contract_v2, TypedQ2InputContractV2)
        else TypedQ2InputContractV2.model_validate(q2_input_contract_v2)
    )
    _validate_projection_manifest(contract, projection_manifest)
    projection_policies = list(projection_policies or ["event_window_only", "event_window_decay", "to_next_event"])
    abstain_policies = list(abstain_policies or ["explicit_abstain"])
    alpha_families = list(alpha_families or ["SUE", "revision", "SUE_plus_revision"])

    active_rebalance_count = len({str(row.get("date")) for row in expected_return_panel})
    active_name_count = len({str(row.get("symbol")).upper() for row in expected_return_panel})
    abstain_count = len(alpha_abstain_report)
    used_share = _expected_return_used_share(projection_manifest, expected_return_panel)
    sign_consistency = _sign_consistency(expected_return_panel)
    view_overlap = _view_overlap(projection_diagnostics)
    unavailable_reason = (
        "typed Q2 execution adapter is not implemented; projected alpha input was consumed for diagnostics only."
    )

    rows: list[TypedExecutionMatrixRow] = []
    for base_scenario in build_scenario_grid(config):
        for projection_policy in projection_policies:
            for abstain_policy in abstain_policies:
                for alpha_family in alpha_families:
                    source_hash = _typed_source_config_hash(
                        base_hash=base_scenario.source_config_hash,
                        projection_manifest_hash=str(projection_manifest["content_hash"]),
                        projection_policy=projection_policy,
                        abstain_policy=abstain_policy,
                        alpha_family=alpha_family,
                    )
                    rows.append(
                        TypedExecutionMatrixRow(
                            scenario_id="__".join(
                                [
                                    base_scenario.scenario_id,
                                    f"projection_{projection_policy}",
                                    f"abstain_{abstain_policy}",
                                    f"alpha_{alpha_family}",
                                ]
                            ),
                            source_config_hash=source_hash,
                            cost_bps=base_scenario.cost_bps,
                            participation_rate=base_scenario.participation_rate,
                            liquidity_bucket=base_scenario.liquidity_bucket,
                            constraint_level=base_scenario.constraint_level,
                            execution_mode=base_scenario.execution_mode,
                            projection_policy=projection_policy,
                            abstain_policy=abstain_policy,
                            alpha_family=alpha_family,
                            status="unavailable",
                            unavailable_reason=unavailable_reason,
                            active_rebalance_count=active_rebalance_count,
                            active_name_count=active_name_count,
                            expected_return_used_share=used_share,
                            abstain_count=abstain_count,
                            sign_consistency=sign_consistency,
                            view_overlap=view_overlap,
                        )
                    )
    return rows


def summarize_typed_execution_matrix(rows: Sequence[TypedExecutionMatrixRow]) -> TypedExecutionMatrixSummary:
    """Summarize typed Q2 execution matrix rows."""

    unavailable_reasons = Counter(
        row.unavailable_reason or "Not available"
        for row in rows
        if row.status == "unavailable"
    )
    return TypedExecutionMatrixSummary(
        total_rows=len(rows),
        observed_rows=sum(1 for row in rows if row.status == "observed"),
        unavailable_rows=sum(1 for row in rows if row.status == "unavailable"),
        unique_source_config_hashes=len({row.source_config_hash for row in rows}),
        unavailable_reason_counts=dict(sorted(unavailable_reasons.items())),
    )


def render_typed_execution_matrix_report(
    rows: Sequence[TypedExecutionMatrixRow],
    *,
    summary: TypedExecutionMatrixSummary | None = None,
) -> str:
    """Render a Q2 typed alpha matrix report without fabricating returns."""

    summary = summary or summarize_typed_execution_matrix(rows)
    lines = [
        "# Q2 Typed Alpha Execution Matrix",
        "",
        "This report consumes projected expected-return artifacts. It does not run Q2 or fabricate execution results when typed execution hooks are unavailable.",
        "",
        "## Scenario Coverage",
        "",
        "| total_rows | observed_rows | unavailable_rows | unique_source_config_hashes |",
        "|---:|---:|---:|---:|",
        f"| {summary.total_rows} | {summary.observed_rows} | {summary.unavailable_rows} | {summary.unique_source_config_hashes} |",
        "",
        "## Typed Alpha Consumption",
        "",
        "The matrix explains alpha consumption through cost assumptions, constraint level, coverage, and abstain diagnostics.",
        "",
        "| scenario_id | cost assumptions | constraint level | projection_policy | active names | expected-return used share | abstain count | gross_to_net_retention | turnover | cost_drag | status |",
        "|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| {scenario} | {cost} bps | {constraint} | {projection} | {active_names} | "
            "{used_share:.6f} | {abstain_count} | {retention} | {turnover} | {cost_drag} | {status} |".format(
                scenario=row.scenario_id,
                cost=row.cost_bps,
                constraint=row.constraint_level,
                projection=row.projection_policy,
                active_names=row.active_name_count,
                used_share=row.expected_return_used_share,
                abstain_count=row.abstain_count,
                retention=_fmt_optional(row.gross_to_net_retention),
                turnover=_fmt_optional(row.turnover),
                cost_drag=_fmt_optional(row.cost_drag),
                status=row.status,
            )
        )
    lines.extend(["", "## Unavailable Rows", ""])
    if summary.unavailable_reason_counts:
        for reason, count in summary.unavailable_reason_counts.items():
            lines.append(f"- `{count}` rows: {reason}")
    else:
        lines.append("- Not available")
    return "\n".join(lines) + "\n"


def _validate_projection_manifest(
    contract: TypedQ2InputContractV2,
    projection_manifest: dict[str, Any],
) -> None:
    if projection_manifest.get("schema_version") != "alpha_projection.v2":
        raise ValueError("projection_manifest must use alpha_projection.v2")
    if projection_manifest.get("content_hash") != contract.projection_manifest_hash:
        raise ValueError("projection manifest hash does not match Q2InputContractV2")
    if contract.alpha_view_id not in set(projection_manifest.get("alpha_view_ids", [])):
        raise ValueError("projection manifest does not include contract alpha_view_id")


def _expected_return_used_share(
    projection_manifest: dict[str, Any],
    expected_return_panel: Sequence[dict[str, Any]],
) -> float:
    universe_count = len(set(projection_manifest.get("universe_symbols", [])))
    rebalance_count = len(set(projection_manifest.get("rebalance_dates", [])))
    denominator = universe_count * rebalance_count
    if denominator <= 0:
        return 0.0
    used_symbols = {
        (str(row.get("date")), str(row.get("symbol")).upper())
        for row in expected_return_panel
        if row.get("expected_return") is not None
    }
    return float(len(used_symbols) / denominator)


def _sign_consistency(expected_return_panel: Sequence[dict[str, Any]]) -> str:
    values = [float(row["expected_return"]) for row in expected_return_panel if row.get("expected_return") is not None]
    if not values:
        return "not_available"
    has_positive = any(value > 0.0 for value in values)
    has_negative = any(value < 0.0 for value in values)
    if has_positive and has_negative:
        return "mixed_sign"
    if has_positive:
        return "positive_only"
    if has_negative:
        return "negative_only"
    return "zero_only"


def _view_overlap(projection_diagnostics: Sequence[dict[str, Any]]) -> str:
    active_view_sets = [
        set(row.get("active_views", []))
        for row in projection_diagnostics
        if row.get("active_views")
    ]
    if not active_view_sets:
        return "not_available"
    if any(len(view_set) > 1 for view_set in active_view_sets):
        return "multiple_active_views"
    return "single_active_view"


def _typed_source_config_hash(
    *,
    base_hash: str,
    projection_manifest_hash: str,
    projection_policy: ProjectionPolicy,
    abstain_policy: TypedAbstainPolicy,
    alpha_family: AlphaFamily,
) -> str:
    payload = {
        "abstain_policy": abstain_policy,
        "alpha_family": alpha_family,
        "base_source_config_hash": base_hash,
        "projection_manifest_hash": projection_manifest_hash,
        "projection_policy": projection_policy,
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _fmt_optional(value: object) -> str:
    if value is None:
        return "Not available"
    return str(value)
