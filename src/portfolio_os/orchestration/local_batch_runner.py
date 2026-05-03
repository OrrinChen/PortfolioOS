"""Deterministic local batch orchestration."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from portfolio_os.orchestration.result_store import LocalResultStore
from portfolio_os.orchestration.retry_policy import RetryPolicy
from portfolio_os.orchestration.scheduler import LocalBatchCandidate, build_batch_schedule
from portfolio_os.provenance import build_provenance_manifest, write_provenance_manifest


CandidateStatus = Literal["promoted", "rejected", "unavailable", "failed"]
CandidateRunner = Callable[[LocalBatchCandidate], "CandidateEvaluation"]


class CandidateEvaluation(BaseModel):
    """Result returned by an injected local candidate evaluator."""

    status: CandidateStatus
    reasons: list[str] = Field(min_length=1)
    artifacts: dict[str, str] = Field(default_factory=dict)

    @field_validator("reasons")
    @classmethod
    def require_reasons(cls, values: list[str]) -> list[str]:
        cleaned = [str(value).strip() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("reasons cannot contain blank entries")
        return cleaned


class CandidateRunResult(BaseModel):
    """Stored local batch result for one candidate."""

    candidate_id: str
    status: CandidateStatus
    reasons: list[str]
    attempts: int
    artifacts: dict[str, str] = Field(default_factory=dict)
    result_path: str
    provenance_path: str | None = None


class LocalBatchSummary(BaseModel):
    """Aggregated local batch run summary."""

    total_candidates: int
    counts_by_status: dict[str, int]
    missing_candidate_ids: list[str] = Field(default_factory=list)
    results: list[CandidateRunResult] = Field(default_factory=list)


def run_local_batch(
    candidates: list[LocalBatchCandidate],
    *,
    output_dir: str | Path,
    repo_root: str | Path,
    runner: CandidateRunner,
    retry_policy: RetryPolicy | None = None,
    only_candidate_ids: set[str] | None = None,
    created_at: str | None = None,
    git_sha: str | None = None,
    git_dirty: bool | None = None,
) -> LocalBatchSummary:
    """Run local candidates in deterministic order with failure isolation."""

    store = LocalResultStore(output_dir)
    schedule = build_batch_schedule(candidates, only_candidate_ids=only_candidate_ids)
    policy = retry_policy or RetryPolicy()
    results = [
        _run_candidate(
            candidate,
            store=store,
            repo_root=Path(repo_root),
            runner=runner,
            retry_policy=policy,
            created_at=created_at,
            git_sha=git_sha,
            git_dirty=git_dirty,
        )
        for candidate in schedule.candidates
    ]
    counts = Counter(result.status for result in results)
    summary = LocalBatchSummary(
        total_candidates=len(results),
        counts_by_status={status: counts[status] for status in sorted(counts)},
        missing_candidate_ids=schedule.missing_candidate_ids,
        results=results,
    )
    store.write_model(store.output_dir / "batch_summary.json", summary)
    return summary


def _run_candidate(
    candidate: LocalBatchCandidate,
    *,
    store: LocalResultStore,
    repo_root: Path,
    runner: CandidateRunner,
    retry_policy: RetryPolicy,
    created_at: str | None,
    git_sha: str | None,
    git_dirty: bool | None,
) -> CandidateRunResult:
    attempts = 0
    try:
        evaluation, attempts = retry_policy.run(lambda: runner(candidate))
    except Exception as exc:  # noqa: BLE001 - batch runs must isolate candidate failures.
        attempts = retry_policy.max_attempts
        evaluation = CandidateEvaluation(status="failed", reasons=[str(exc)])

    result_path = store.result_path(candidate.candidate_id)
    provenance_path = store.provenance_path(candidate.candidate_id)
    result = CandidateRunResult(
        candidate_id=candidate.candidate_id,
        status=evaluation.status,
        reasons=evaluation.reasons,
        attempts=attempts,
        artifacts=evaluation.artifacts,
        result_path=store.relative_to_output(result_path),
        provenance_path=store.relative_to_output(provenance_path),
    )
    store.write_model(result_path, result)
    provenance = build_provenance_manifest(
        repo_root=repo_root,
        run_id=f"local_batch:{candidate.candidate_id}",
        command=["portfolioos-local-batch", "--candidate-id", candidate.candidate_id],
        config_path=None,
        input_paths={"bundle": repo_root / candidate.bundle_path},
        output_paths={"candidate_result": result_path},
        created_at=created_at,
        runner_version="portfolioos-local-batch-v1",
        git_sha=git_sha,
        git_dirty=git_dirty,
    )
    write_provenance_manifest(provenance_path, provenance)
    return result
