"""Deterministic local batch scheduling."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class LocalBatchCandidate(BaseModel):
    """One local candidate artifact to evaluate."""

    candidate_id: str
    bundle_path: str
    description: str | None = None

    @field_validator("candidate_id", "bundle_path")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text


class BatchSchedule(BaseModel):
    """Deterministic candidate schedule plus missing partial-rerun ids."""

    candidates: list[LocalBatchCandidate] = Field(default_factory=list)
    missing_candidate_ids: list[str] = Field(default_factory=list)


def build_batch_schedule(
    candidates: list[LocalBatchCandidate],
    *,
    only_candidate_ids: set[str] | None = None,
) -> BatchSchedule:
    """Sort candidates and optionally select a deterministic partial rerun."""

    by_id = {candidate.candidate_id: candidate for candidate in candidates}
    if only_candidate_ids is None:
        selected_ids = sorted(by_id)
        missing_ids: list[str] = []
    else:
        selected_ids = sorted(
            candidate_id for candidate_id in only_candidate_ids if candidate_id in by_id
        )
        missing_ids = sorted(
            candidate_id for candidate_id in only_candidate_ids if candidate_id not in by_id
        )
    return BatchSchedule(
        candidates=[by_id[candidate_id] for candidate_id in selected_ids],
        missing_candidate_ids=missing_ids,
    )
