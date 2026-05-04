"""Promotion gate schemas for safe Q1-to-Q2 handoff."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from portfolio_os.alpha.schema_versions import (
    PROMOTION_DECISION_V2_SCHEMA_VERSION,
    Q2_INPUT_CONTRACT_V2_SCHEMA_VERSION,
)


PromotionDecisionType = Literal["promote_to_execution_eval", "reject", "needs_more_evidence"]


class Q2InputContract(BaseModel):
    """A narrow input contract Q2 may consume later as a plain artifact."""

    bundle_id: str
    alpha_score_columns: list[str] = Field(min_length=3)
    allowed_consumer: str = "projects/execution_aware_optimizer"
    direct_q2_execution_allowed: bool = False

    @field_validator("bundle_id", "allowed_consumer")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @field_validator("alpha_score_columns")
    @classmethod
    def require_alpha_score_columns(cls, values: list[str]) -> list[str]:
        cleaned = [str(value).strip() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("alpha_score_columns cannot contain blank entries")
        required = {"date", "symbol", "alpha_score"}
        missing = sorted(required.difference(cleaned))
        if missing:
            raise ValueError("alpha_score_columns missing required columns: " + ", ".join(missing))
        return cleaned

    @model_validator(mode="after")
    def forbid_direct_execution(self) -> "Q2InputContract":
        if self.direct_q2_execution_allowed is not False:
            raise ValueError("promotion gate cannot allow direct Q2 execution")
        return self


class PromotionDecision(BaseModel):
    """Decision record separating research validation from execution evaluation."""

    bundle_id: str
    decision: PromotionDecisionType
    reasons: list[str] = Field(min_length=1)
    q2_allowed_inputs: Q2InputContract | None = None
    forbidden_outputs_checked: bool

    @field_validator("bundle_id")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @field_validator("reasons")
    @classmethod
    def require_reasons(cls, values: list[str]) -> list[str]:
        cleaned = [str(value).strip() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("reasons cannot contain blank entries")
        return cleaned

    @model_validator(mode="after")
    def require_decision_contract(self) -> "PromotionDecision":
        if self.forbidden_outputs_checked is not True:
            raise ValueError("forbidden_outputs_checked must be true")
        if self.decision == "promote_to_execution_eval" and self.q2_allowed_inputs is None:
            raise ValueError("promoted decisions require q2_allowed_inputs")
        if self.decision != "promote_to_execution_eval" and self.q2_allowed_inputs is not None:
            raise ValueError("non-promoted decisions cannot include q2_allowed_inputs")
        return self


class Q2InputContractV2(BaseModel):
    """Typed expected-return-panel input contract for Q2."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["q2_input_contract.v2"] = Q2_INPUT_CONTRACT_V2_SCHEMA_VERSION
    bundle_id: str
    alpha_view_id: str
    input_type: Literal["projected_expected_return_panel"] = "projected_expected_return_panel"
    expected_return_panel_artifact: str = "expected_return_panel.csv"
    projection_manifest_hash: str
    alpha_projection_diagnostics_artifact: str = "alpha_projection_diagnostics.json"
    alpha_abstain_report_artifact: str = "alpha_abstain_report.json"
    allowed_consumer: str = "projects/execution_aware_optimizer"
    direct_q2_execution_allowed: bool = False

    @field_validator(
        "bundle_id",
        "alpha_view_id",
        "expected_return_panel_artifact",
        "projection_manifest_hash",
        "alpha_projection_diagnostics_artifact",
        "alpha_abstain_report_artifact",
        "allowed_consumer",
    )
    @classmethod
    def require_non_empty_v2_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @model_validator(mode="after")
    def forbid_direct_execution(self) -> "Q2InputContractV2":
        if self.direct_q2_execution_allowed is not False:
            raise ValueError("promotion gate v2 cannot allow direct Q2 execution")
        return self


class PromotionDecisionV2(BaseModel):
    """Typed promotion decision separating AlphaView projection from Q2 execution."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["promotion_decision.v2"] = PROMOTION_DECISION_V2_SCHEMA_VERSION
    bundle_id: str
    alpha_view_id: str | None = None
    decision: PromotionDecisionType
    reasons: list[str] = Field(min_length=1)
    q2_allowed_inputs: Q2InputContractV2 | None = None
    forbidden_outputs_checked: bool
    typed_alpha_view_checked: bool
    projection_manifest_checked: bool
    marginal_value_disclosure_required: bool = False

    @field_validator("bundle_id", "alpha_view_id")
    @classmethod
    def clean_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @field_validator("reasons")
    @classmethod
    def require_v2_reasons(cls, values: list[str]) -> list[str]:
        cleaned = [str(value).strip() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("reasons cannot contain blank entries")
        return cleaned

    @model_validator(mode="after")
    def require_v2_decision_contract(self) -> "PromotionDecisionV2":
        if self.forbidden_outputs_checked is not True:
            raise ValueError("forbidden_outputs_checked must be true")
        if self.decision == "promote_to_execution_eval" and self.q2_allowed_inputs is None:
            raise ValueError("promoted v2 decisions require q2_allowed_inputs")
        if self.decision != "promote_to_execution_eval" and self.q2_allowed_inputs is not None:
            raise ValueError("non-promoted v2 decisions cannot include q2_allowed_inputs")
        return self
