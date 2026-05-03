"""Deterministic local batch orchestration."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from portfolio_os.cache import ContentAddressedStore, build_file_cache_key
from portfolio_os.orchestration.result_store import LocalResultStore
from portfolio_os.orchestration.retry_policy import RetryPolicy
from portfolio_os.orchestration.scheduler import LocalBatchCandidate, build_batch_schedule
from portfolio_os.provenance import build_provenance_manifest, write_provenance_manifest


CandidateStatus = Literal["promoted", "rejected", "unavailable", "failed"]
CacheStatus = Literal["disabled", "hit", "miss"]
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
    cache_status: CacheStatus = "disabled"
    cache_key: str | None = None


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
    cache_store: ContentAddressedStore | None = None,
    code_version: str | None = None,
    runner_version: str = "portfolioos-local-batch-v1",
    cache_config_path: str | Path | None = None,
    seed: int | None = None,
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
            cache_store=cache_store,
            code_version=code_version,
            runner_version=runner_version,
            cache_config_path=cache_config_path,
            seed=seed,
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
    cache_store: ContentAddressedStore | None,
    code_version: str | None,
    runner_version: str,
    cache_config_path: str | Path | None,
    seed: int | None,
    created_at: str | None,
    git_sha: str | None,
    git_dirty: bool | None,
) -> CandidateRunResult:
    cache_key_digest: str | None = None
    cache_status: CacheStatus = "disabled"
    cached_payload = None
    if cache_store is not None:
        key = build_file_cache_key(
            input_path=_resolve_candidate_path(repo_root, candidate.bundle_path),
            config_path=_resolve_optional_path(repo_root, cache_config_path),
            code_version=code_version or "unknown",
            runner_version=runner_version,
            seed=seed,
        )
        cache_key_digest = key.digest
        cached_payload = cache_store.get_json(key)
        cache_status = "hit" if cached_payload is not None else "miss"

    if cached_payload is not None:
        attempts = 0
        evaluation = CandidateEvaluation.model_validate(cached_payload)
    else:
        attempts = 0
        try:
            evaluation, attempts = retry_policy.run(lambda: runner(candidate))
        except Exception as exc:  # noqa: BLE001 - batch runs must isolate candidate failures.
            attempts = retry_policy.max_attempts
            evaluation = CandidateEvaluation(status="failed", reasons=[str(exc)])
        if cache_store is not None and evaluation.status != "failed":
            cache_store.put_json(key, evaluation.model_dump(mode="json"))

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
        cache_status=cache_status,
        cache_key=cache_key_digest,
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
        runner_version=runner_version,
        random_seed=seed,
        git_sha=git_sha,
        git_dirty=git_dirty,
    )
    write_provenance_manifest(provenance_path, provenance)
    return result


def _resolve_candidate_path(repo_root: Path, path: str | Path) -> Path:
    raw_path = Path(path)
    return raw_path if raw_path.is_absolute() else repo_root / raw_path


def _resolve_optional_path(repo_root: Path, path: str | Path | None) -> Path | None:
    if path is None:
        return None
    return _resolve_candidate_path(repo_root, path)
