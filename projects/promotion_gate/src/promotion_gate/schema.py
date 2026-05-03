"""Promotion gate schemas for safe Q1-to-Q2 handoff."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


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
