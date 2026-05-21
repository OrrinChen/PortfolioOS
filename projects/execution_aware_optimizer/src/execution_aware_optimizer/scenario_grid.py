"""Scenario grid construction for Q2 execution evaluation."""

from __future__ import annotations

import hashlib
import json
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from execution_aware_optimizer.experiment_config import ExperimentConfig


LiquidityBucket = Literal["high", "medium", "low"]
ConstraintLevel = Literal["raw", "risk_aware", "full_execution_aware"]
ExecutionMode = Literal["impact_aware", "participation_twap"]


class ScenarioGridConfig(BaseModel):
    """Configurable scenario dimensions for the execution matrix."""

    cost_bps: list[int] | None = None
    participation_rates: list[float] = Field(default_factory=lambda: [0.001, 0.005, 0.01])
    liquidity_buckets: list[LiquidityBucket] = Field(
        default_factory=lambda: ["high", "medium", "low"]
    )
    constraint_levels: list[ConstraintLevel] = Field(
        default_factory=lambda: ["raw", "risk_aware", "full_execution_aware"]
    )
    execution_modes: list[ExecutionMode] = Field(
        default_factory=lambda: ["impact_aware", "participation_twap"]
    )

    @field_validator("cost_bps")
    @classmethod
    def validate_cost_bps(cls, values: list[int] | None) -> list[int] | None:
        if values is None:
            return None
        if not values:
            raise ValueError("execution_matrix.cost_bps cannot be empty")
        invalid = [value for value in values if int(value) < 0]
        if invalid:
            raise ValueError("execution_matrix.cost_bps cannot include negative values")
        return [int(value) for value in values]

    @field_validator("participation_rates")
    @classmethod
    def validate_participation_rates(cls, values: list[float]) -> list[float]:
        if not values:
            raise ValueError("participation_rates cannot be empty")
        invalid = [value for value in values if float(value) <= 0.0]
        if invalid:
            raise ValueError("participation_rates must be positive")
        return [float(value) for value in values]

    @field_validator("liquidity_buckets", "constraint_levels", "execution_modes")
    @classmethod
    def validate_non_empty_dimensions(cls, values: list[str]) -> list[str]:
        if not values:
            raise ValueError("scenario dimensions cannot be empty")
        return values


class ExecutionScenario(BaseModel):
    """One deterministic execution-evaluation scenario."""

    scenario_id: str
    cost_bps: int
    participation_rate: float
    liquidity_bucket: LiquidityBucket
    constraint_level: ConstraintLevel
    execution_mode: ExecutionMode
    source_config_hash: str


def build_scenario_grid(config: ExperimentConfig) -> list[ExecutionScenario]:
    """Build the full deterministic scenario grid for one Q2 config."""

    grid_config = ScenarioGridConfig.model_validate(config.execution_matrix)
    cost_values = grid_config.cost_bps or config.cost_sensitivity_bps
    scenarios: list[ExecutionScenario] = []
    for cost_bps in cost_values:
        for participation_rate in grid_config.participation_rates:
            for liquidity_bucket in grid_config.liquidity_buckets:
                for constraint_level in grid_config.constraint_levels:
                    for execution_mode in grid_config.execution_modes:
                        payload = {
                            "cost_bps": int(cost_bps),
                            "participation_rate": float(participation_rate),
                            "liquidity_bucket": liquidity_bucket,
                            "constraint_level": constraint_level,
                            "execution_mode": execution_mode,
                        }
                        scenarios.append(
                            ExecutionScenario(
                                scenario_id=_scenario_id(payload),
                                source_config_hash=_source_config_hash(config, payload),
                                **payload,
                            )
                        )
    return scenarios


def _scenario_id(payload: dict[str, object]) -> str:
    return "__".join(
        [
            f"cost_{payload['cost_bps']}bps",
            f"participation_{_format_participation(float(payload['participation_rate']))}",
            f"liquidity_{payload['liquidity_bucket']}",
            f"constraint_{payload['constraint_level']}",
            f"execution_{payload['execution_mode']}",
        ]
    )


def _format_participation(value: float) -> str:
    return f"{value:g}".replace(".", "p")


def _source_config_hash(config: ExperimentConfig, scenario_payload: dict[str, object]) -> str:
    serialized = json.dumps(
        {
            "experiment_config": config.model_dump(mode="json"),
            "scenario": scenario_payload,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
