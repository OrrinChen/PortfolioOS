"""Typed Q2 adapter contracts.

These contracts sit outside the existing Q2 typed matrix. They describe a
local-only adapter that can observe PortfolioOS fixture outputs when explicitly
enabled, while preserving unavailable rows where PortfolioOS hooks do not exist.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


TYPED_Q2_ADAPTER_INPUT_SCHEMA_VERSION = "typed_q2_adapter_input.v1"
TYPED_Q2_ADAPTER_MATRIX_SCHEMA_VERSION = "typed_q2_adapter_matrix.v1"
TYPED_Q2_ADAPTER_RESULT_SCHEMA_VERSION = "typed_q2_adapter_result.v1"
TYPED_Q2_ADAPTER_MANIFEST_SCHEMA_VERSION = "typed_q2_adapter_manifest.v1"

AdapterStatus = Literal["observed", "partially_observed", "unavailable", "rejected"]
AdapterRowStatus = Literal["observed", "unavailable", "rejected"]


class TypedQ2AdapterInput(BaseModel):
    """Input contract for local typed Q2 adapter runs."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["typed_q2_adapter_input.v1"] = TYPED_Q2_ADAPTER_INPUT_SCHEMA_VERSION
    run_id: str
    q2_input_contract_v2_path: Path
    expected_return_panel_path: Path
    projection_manifest_path: Path
    local_backtest_manifest_path: Path
    adapter_config_path: Path
    allow_portfolioos_run: bool = False
    no_network: bool = True
    no_broker: bool = True

    @model_validator(mode="after")
    def require_local_safety_flags(self) -> "TypedQ2AdapterInput":
        """Require explicit local-only safety flags."""

        if not self.no_network:
            raise ValueError("TypedQ2AdapterInput requires no_network=true")
        if not self.no_broker:
            raise ValueError("TypedQ2AdapterInput requires no_broker=true")
        return self


class TypedQ2AdapterMatrixRow(BaseModel):
    """One observed/unavailable typed Q2 adapter matrix row."""

    schema_version: Literal["typed_q2_adapter_matrix.v1"] = TYPED_Q2_ADAPTER_MATRIX_SCHEMA_VERSION
    scenario_id: str
    alpha_family: str
    projection_policy: str
    abstain_policy: str
    layer: str
    date: str | None = None
    status: AdapterRowStatus
    active_rebalance_count: int
    active_name_count: int
    expected_return_used_share: float
    gross_return: float | None = None
    net_return: float | None = None
    turnover: float | None = None
    cost_drag: float | None = None
    gross_to_net_retention: float | None = None
    repair_retention: float | None = None
    unavailable_reason: str | None = None
    source_config_hash: str


class TypedQ2AdapterResult(BaseModel):
    """Top-level local typed Q2 adapter result."""

    schema_version: Literal["typed_q2_adapter_result.v1"] = TYPED_Q2_ADAPTER_RESULT_SCHEMA_VERSION
    run_id: str
    adapter_status: AdapterStatus
    observed_rows: int
    unavailable_rows: int
    rejection_reasons: list[str] = Field(default_factory=list)
    matrix_rows: list[TypedQ2AdapterMatrixRow] = Field(default_factory=list)
    source_config_hash: str
    input_artifact_hashes: dict[str, str] = Field(default_factory=dict)
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True


class TypedQ2AdapterRobustnessSummary(BaseModel):
    """Compact status summary for local adapter artifacts."""

    schema_version: Literal["typed_q2_adapter_result.v1"] = TYPED_Q2_ADAPTER_RESULT_SCHEMA_VERSION
    run_id: str
    adapter_status: AdapterStatus
    total_rows: int
    observed_rows: int
    unavailable_rows: int
    rejected_rows: int
    status_counts: dict[str, int]
    unavailable_reason_counts: dict[str, int]


class TypedQ2AdapterManifest(BaseModel):
    """Reproducibility manifest for adapter artifacts."""

    schema_version: Literal["typed_q2_adapter_manifest.v1"] = TYPED_Q2_ADAPTER_MANIFEST_SCHEMA_VERSION
    run_id: str
    adapter_status: AdapterStatus
    source_config_hash: str
    input_artifact_hashes: dict[str, str]
    output_artifacts: dict[str, str]
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True
    content_hash: str
