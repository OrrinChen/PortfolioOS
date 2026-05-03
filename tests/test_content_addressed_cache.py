from __future__ import annotations

from pathlib import Path

from portfolio_os.cache import (
    ContentAddressedStore,
    build_file_cache_key,
    explain_cache_invalidation,
)
from portfolio_os.orchestration import CandidateEvaluation, LocalBatchCandidate, run_local_batch


def test_content_addressed_store_misses_then_hits(tmp_path: Path) -> None:
    bundle_path = _write(tmp_path / "bundle.yaml", "candidate: one\n")
    config_path = _write(tmp_path / "config.yaml", "cost_bps: 5\n")
    cache_key = build_file_cache_key(
        input_path=bundle_path,
        config_path=config_path,
        code_version="abc123",
        runner_version="runner-v1",
        seed=7,
    )
    store = ContentAddressedStore(tmp_path / "cache")

    assert store.get_json(cache_key) is None

    store.put_json(cache_key, {"status": "promoted"})

    assert store.get_json(cache_key) == {"status": "promoted"}


def test_cache_key_changes_when_config_changes(tmp_path: Path) -> None:
    bundle_path = _write(tmp_path / "bundle.yaml", "candidate: one\n")
    config_path = _write(tmp_path / "config.yaml", "cost_bps: 5\n")
    first = build_file_cache_key(
        input_path=bundle_path,
        config_path=config_path,
        code_version="abc123",
        runner_version="runner-v1",
    )

    config_path.write_text("cost_bps: 25\n", encoding="utf-8")
    second = build_file_cache_key(
        input_path=bundle_path,
        config_path=config_path,
        code_version="abc123",
        runner_version="runner-v1",
    )

    assert first.digest != second.digest
    assert explain_cache_invalidation(first, second).changed_fields == ["config_hash"]


def test_cache_key_changes_when_bundle_changes(tmp_path: Path) -> None:
    bundle_path = _write(tmp_path / "bundle.yaml", "candidate: one\n")
    first = build_file_cache_key(
        input_path=bundle_path,
        config_path=None,
        code_version="abc123",
        runner_version="runner-v1",
    )

    bundle_path.write_text("candidate: two\n", encoding="utf-8")
    second = build_file_cache_key(
        input_path=bundle_path,
        config_path=None,
        code_version="abc123",
        runner_version="runner-v1",
    )

    assert first.digest != second.digest
    assert explain_cache_invalidation(first, second).changed_fields == ["input_hash"]


def test_cache_key_changes_when_seed_changes(tmp_path: Path) -> None:
    bundle_path = _write(tmp_path / "bundle.yaml", "candidate: one\n")
    first = build_file_cache_key(
        input_path=bundle_path,
        config_path=None,
        code_version="abc123",
        runner_version="runner-v1",
        seed=1,
    )
    second = build_file_cache_key(
        input_path=bundle_path,
        config_path=None,
        code_version="abc123",
        runner_version="runner-v1",
        seed=2,
    )

    assert first.digest != second.digest
    assert explain_cache_invalidation(first, second).changed_fields == ["seed"]


def test_local_batch_surfaces_cache_miss_then_hit(tmp_path: Path) -> None:
    bundle_path = _write(tmp_path / "bundle.yaml", "candidate: one\n")
    candidate = LocalBatchCandidate(candidate_id="candidate_a", bundle_path=str(bundle_path))
    cache_store = ContentAddressedStore(tmp_path / "cache")
    calls = {"count": 0}

    def runner(_candidate: LocalBatchCandidate) -> CandidateEvaluation:
        calls["count"] += 1
        return CandidateEvaluation(status="promoted", reasons=["computed"])

    first = run_local_batch(
        [candidate],
        output_dir=tmp_path / "run1",
        repo_root=Path.cwd(),
        runner=runner,
        cache_store=cache_store,
        code_version="abc123",
        runner_version="runner-v1",
        created_at="2026-05-03T00:00:00+00:00",
        git_sha="abc123",
        git_dirty=False,
    )
    second = run_local_batch(
        [candidate],
        output_dir=tmp_path / "run2",
        repo_root=Path.cwd(),
        runner=runner,
        cache_store=cache_store,
        code_version="abc123",
        runner_version="runner-v1",
        created_at="2026-05-03T00:00:00+00:00",
        git_sha="abc123",
        git_dirty=False,
    )

    assert calls == {"count": 1}
    assert first.results[0].cache_status == "miss"
    assert second.results[0].cache_status == "hit"
    assert second.results[0].attempts == 0


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path
