from __future__ import annotations

from pathlib import Path

from portfolio_os.orchestration import (
    CandidateEvaluation,
    LocalBatchCandidate,
    RetryPolicy,
    run_local_batch,
)


def test_local_batch_runner_sorts_candidates_and_isolates_failures(tmp_path: Path) -> None:
    candidates = [
        _candidate(tmp_path, "candidate_c"),
        _candidate(tmp_path, "candidate_a"),
        _candidate(tmp_path, "candidate_b"),
    ]

    def runner(candidate: LocalBatchCandidate) -> CandidateEvaluation:
        if candidate.candidate_id == "candidate_b":
            raise RuntimeError("fixture failure")
        if candidate.candidate_id == "candidate_c":
            return CandidateEvaluation(status="unavailable", reasons=["q2 adapter unavailable"])
        return CandidateEvaluation(status="promoted", reasons=["ready for execution evaluation"])

    summary = run_local_batch(
        candidates,
        output_dir=tmp_path,
        repo_root=Path.cwd(),
        runner=runner,
        created_at="2026-05-03T00:00:00+00:00",
        git_sha="abc123",
        git_dirty=False,
    )

    assert [result.candidate_id for result in summary.results] == [
        "candidate_a",
        "candidate_b",
        "candidate_c",
    ]
    assert summary.counts_by_status == {
        "failed": 1,
        "promoted": 1,
        "unavailable": 1,
    }
    assert summary.results[1].status == "failed"
    assert summary.results[1].reasons == ["fixture failure"]
    for result in summary.results:
        assert (tmp_path / "results" / f"{result.candidate_id}.json").exists()
        assert result.provenance_path is not None
        assert (tmp_path / result.provenance_path).exists()
    assert (tmp_path / "batch_summary.json").exists()


def test_local_batch_runner_supports_deterministic_partial_rerun(tmp_path: Path) -> None:
    candidates = [
        _candidate(tmp_path, "candidate_b"),
        _candidate(tmp_path, "candidate_a"),
        _candidate(tmp_path, "candidate_c"),
    ]

    summary = run_local_batch(
        candidates,
        output_dir=tmp_path,
        repo_root=Path.cwd(),
        runner=lambda candidate: CandidateEvaluation(
            status="rejected",
            reasons=[f"{candidate.candidate_id} rejected"],
        ),
        only_candidate_ids={"candidate_c", "candidate_a"},
        created_at="2026-05-03T00:00:00+00:00",
        git_sha="abc123",
        git_dirty=False,
    )

    assert [result.candidate_id for result in summary.results] == ["candidate_a", "candidate_c"]
    assert summary.total_candidates == 2
    assert summary.counts_by_status == {"rejected": 2}


def test_local_batch_runner_retries_candidate_failures(tmp_path: Path) -> None:
    attempts = {"candidate_a": 0}

    def flaky_runner(candidate: LocalBatchCandidate) -> CandidateEvaluation:
        attempts[candidate.candidate_id] += 1
        if attempts[candidate.candidate_id] == 1:
            raise RuntimeError("transient failure")
        return CandidateEvaluation(status="promoted", reasons=["second attempt passed"])

    summary = run_local_batch(
        [_candidate(tmp_path, "candidate_a")],
        output_dir=tmp_path,
        repo_root=Path.cwd(),
        runner=flaky_runner,
        retry_policy=RetryPolicy(max_attempts=2),
        created_at="2026-05-03T00:00:00+00:00",
        git_sha="abc123",
        git_dirty=False,
    )

    assert attempts == {"candidate_a": 2}
    assert summary.results[0].status == "promoted"
    assert summary.results[0].attempts == 2


def test_local_batch_runner_records_missing_partial_rerun_ids(tmp_path: Path) -> None:
    summary = run_local_batch(
        [_candidate(tmp_path, "candidate_a")],
        output_dir=tmp_path,
        repo_root=Path.cwd(),
        runner=lambda _candidate: CandidateEvaluation(status="promoted", reasons=["ok"]),
        only_candidate_ids={"candidate_missing"},
        created_at="2026-05-03T00:00:00+00:00",
        git_sha="abc123",
        git_dirty=False,
    )

    assert summary.results == []
    assert summary.missing_candidate_ids == ["candidate_missing"]
    assert summary.counts_by_status == {}


def _candidate(tmp_path: Path, candidate_id: str) -> LocalBatchCandidate:
    input_dir = tmp_path / "inputs"
    input_dir.mkdir(exist_ok=True)
    bundle_path = input_dir / f"{candidate_id}.yaml"
    bundle_path.write_text(f"candidate_id: {candidate_id}\n", encoding="utf-8")
    return LocalBatchCandidate(candidate_id=candidate_id, bundle_path=str(bundle_path))
