from __future__ import annotations

import json
from pathlib import Path

from portfolio_os.provenance import (
    build_provenance_manifest,
    sanitize_command,
    write_provenance_manifest,
)


def test_same_fixture_manifest_hash_is_stable_across_timestamps(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "report.md"
    config_path.write_text("alpha: 1\n", encoding="utf-8")
    input_path.write_text("date,symbol,alpha_score\n2026-01-01,ABC,1.0\n", encoding="utf-8")
    output_path.write_text("# report\n", encoding="utf-8")

    first = build_provenance_manifest(
        repo_root=tmp_path,
        run_id="demo",
        command="portfolio-os-demo --api-key secret-value",
        config_path=config_path,
        input_paths={"alpha": input_path},
        output_paths={"report": output_path},
        created_at="2026-05-03T00:00:00Z",
        git_sha="abc123",
        git_dirty=False,
    )
    second = build_provenance_manifest(
        repo_root=tmp_path,
        run_id="demo",
        command="portfolio-os-demo --api-key other-secret",
        config_path=config_path,
        input_paths={"alpha": input_path},
        output_paths={"report": output_path},
        created_at="2026-05-03T01:00:00Z",
        git_sha="abc123",
        git_dirty=False,
    )

    assert first.content_hash == second.content_hash
    assert first.created_at != second.created_at
    assert "secret-value" not in first.model_dump_json()
    assert "other-secret" not in second.model_dump_json()


def test_manifest_hash_changes_when_config_or_input_changes(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "report.md"
    config_path.write_text("cost_bps: 5\n", encoding="utf-8")
    input_path.write_text("symbol,alpha_score\nABC,1.0\n", encoding="utf-8")
    output_path.write_text("# report\n", encoding="utf-8")

    base = build_provenance_manifest(
        repo_root=tmp_path,
        run_id="demo",
        command="demo",
        config_path=config_path,
        input_paths={"alpha": input_path},
        output_paths={"report": output_path},
        created_at="2026-05-03T00:00:00Z",
        git_sha="abc123",
        git_dirty=False,
    )
    config_path.write_text("cost_bps: 25\n", encoding="utf-8")
    changed_config = build_provenance_manifest(
        repo_root=tmp_path,
        run_id="demo",
        command="demo",
        config_path=config_path,
        input_paths={"alpha": input_path},
        output_paths={"report": output_path},
        created_at="2026-05-03T00:00:00Z",
        git_sha="abc123",
        git_dirty=False,
    )
    config_path.write_text("cost_bps: 5\n", encoding="utf-8")
    input_path.write_text("symbol,alpha_score\nABC,2.0\n", encoding="utf-8")
    changed_input = build_provenance_manifest(
        repo_root=tmp_path,
        run_id="demo",
        command="demo",
        config_path=config_path,
        input_paths={"alpha": input_path},
        output_paths={"report": output_path},
        created_at="2026-05-03T00:00:00Z",
        git_sha="abc123",
        git_dirty=False,
    )

    assert changed_config.content_hash != base.content_hash
    assert changed_input.content_hash != base.content_hash


def test_write_provenance_manifest_records_outputs_and_sorts_json(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    output_path = tmp_path / "report.md"
    manifest_path = tmp_path / "run_manifest.json"
    config_path.write_text("demo: true\n", encoding="utf-8")
    output_path.write_text("# report\n", encoding="utf-8")
    manifest = build_provenance_manifest(
        repo_root=tmp_path,
        run_id="demo",
        command=["demo", "--token", "secret"],
        config_path=config_path,
        input_paths={},
        output_paths={"report": output_path},
        created_at="2026-05-03T00:00:00Z",
        git_sha="abc123",
        git_dirty=False,
        random_seed=7,
    )

    written = write_provenance_manifest(manifest_path, manifest)
    payload = json.loads(written.read_text(encoding="utf-8"))

    assert written == manifest_path
    assert payload["outputs"]["report"]["sha256"]
    assert payload["random_seed"] == 7
    assert payload["command"] == ["demo", "--token", "<redacted>"]
    assert list(payload) == sorted(payload)


def test_sanitize_command_redacts_secret_like_values() -> None:
    assert sanitize_command("demo --password abc --plain ok") == [
        "demo",
        "--password",
        "<redacted>",
        "--plain",
        "ok",
    ]
