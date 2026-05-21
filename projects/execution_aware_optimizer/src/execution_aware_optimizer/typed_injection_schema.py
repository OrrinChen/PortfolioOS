"""Typed expected-return injection contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


TYPED_EXPECTED_RETURN_INJECTION_INPUT_SCHEMA_VERSION = "typed_expected_return_injection_input.v1"
TYPED_EXPECTED_RETURN_INJECTION_RESULT_SCHEMA_VERSION = "typed_expected_return_injection_result.v1"
TYPED_EXPECTED_RETURN_INJECTION_MANIFEST_SCHEMA_VERSION = "typed_expected_return_injection_manifest.v1"

InjectionStatus = Literal["injected", "unavailable", "rejected"]


class TypedExpectedReturnInjectionInput(BaseModel):
    """Input contract for a local typed expected-return injection fixture."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["typed_expected_return_injection_input.v1"] = (
        TYPED_EXPECTED_RETURN_INJECTION_INPUT_SCHEMA_VERSION
    )
    run_id: str
    q2_input_contract_v2_path: Path
    expected_return_panel_path: Path
    projection_manifest_path: Path
    local_backtest_manifest_path: Path
    adapter_config_path: Path
    allow_portfolioos_run: bool = False
    expected_return_scale: float = 1.0
    expected_return_sign: Literal[-1, 1] = 1
    rebalance_date: str | None = None
    no_network: bool = True
    no_broker: bool = True

    @model_validator(mode="after")
    def require_local_safety_flags(self) -> "TypedExpectedReturnInjectionInput":
        if not self.no_network:
            raise ValueError("TypedExpectedReturnInjectionInput requires no_network=true")
        if not self.no_broker:
            raise ValueError("TypedExpectedReturnInjectionInput requires no_broker=true")
        return self


class TypedExpectedReturnInjectionResult(BaseModel):
    """Top-level result for a typed expected-return injection fixture."""

    schema_version: Literal["typed_expected_return_injection_result.v1"] = (
        TYPED_EXPECTED_RETURN_INJECTION_RESULT_SCHEMA_VERSION
    )
    run_id: str
    injection_status: InjectionStatus
    expected_return_reached_optimizer_input: bool
    optimizer_input_snapshot_rows: int = 0
    injected_expected_return_count: int = 0
    optimizer_rebalance_date: str | None = None
    active_rebalance_count: int = 0
    active_name_count: int = 0
    expected_return_used_share: float = 0.0
    q2_adapter_status: str = "unavailable"
    q2_observed_rows: int = 0
    q2_unavailable_rows: int = 0
    rejection_reasons: list[str] = Field(default_factory=list)
    unavailable_reason: str | None = None
    source_config_hash: str
    input_artifact_hashes: dict[str, str] = Field(default_factory=dict)
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True


class TypedExpectedReturnInjectionSummary(BaseModel):
    """Compact summary for injection fixture artifacts."""

    schema_version: Literal["typed_expected_return_injection_result.v1"] = (
        TYPED_EXPECTED_RETURN_INJECTION_RESULT_SCHEMA_VERSION
    )
    run_id: str
    injection_status: InjectionStatus
    expected_return_reached_optimizer_input: bool
    optimizer_input_snapshot_rows: int
    injected_expected_return_count: int
    q2_adapter_status: str
    q2_observed_rows: int
    q2_unavailable_rows: int


class TypedExpectedReturnInjectionManifest(BaseModel):
    """Reproducibility manifest for injection fixture artifacts."""

    schema_version: Literal["typed_expected_return_injection_manifest.v1"] = (
        TYPED_EXPECTED_RETURN_INJECTION_MANIFEST_SCHEMA_VERSION
    )
    run_id: str
    injection_status: InjectionStatus
    source_config_hash: str
    input_artifact_hashes: dict[str, str]
    output_artifacts: dict[str, str]
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True
    content_hash: str
