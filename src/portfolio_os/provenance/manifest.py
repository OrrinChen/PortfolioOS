"""Run provenance manifest construction and writing."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import shlex
import subprocess

from pydantic import BaseModel, Field

from portfolio_os.provenance.artifact_index import ArtifactRecord, index_artifact, index_artifacts
from portfolio_os.provenance.environment import EnvironmentSnapshot, capture_environment
from portfolio_os.provenance.hashing import hash_payload


SCHEMA_VERSION = "portfolioos.provenance.v1"
SECRET_MARKERS = ("api-key", "apikey", "token", "secret", "password", "credential")


class GitState(BaseModel):
    """Git code-state metadata."""

    sha: str | None
    dirty: bool


class ProvenanceManifest(BaseModel):
    """Reproducibility manifest for one local evaluation run."""

    schema_version: str = SCHEMA_VERSION
    run_id: str
    runner_version: str = "portfolioos-demo-runner-v1"
    created_at: str
    command: list[str]
    git: GitState
    environment: EnvironmentSnapshot
    random_seed: int | None = None
    config: ArtifactRecord | None = None
    inputs: dict[str, ArtifactRecord] = Field(default_factory=dict)
    outputs: dict[str, ArtifactRecord] = Field(default_factory=dict)
    content_hash: str


def build_provenance_manifest(
    *,
    repo_root: str | Path,
    run_id: str,
    command: str | list[str],
    config_path: str | Path | None,
    input_paths: dict[str, str | Path],
    output_paths: dict[str, str | Path],
    created_at: str | None = None,
    runner_version: str = "portfolioos-demo-runner-v1",
    random_seed: int | None = None,
    dependency_snapshot: dict[str, str] | None = None,
    git_sha: str | None = None,
    git_dirty: bool | None = None,
) -> ProvenanceManifest:
    """Build a provenance manifest with stable content hashing."""

    root = Path(repo_root)
    sanitized_command = sanitize_command(command)
    git_state = GitState(
        sha=git_sha if git_sha is not None else _read_git_sha(root),
        dirty=git_dirty if git_dirty is not None else _read_git_dirty(root),
    )
    config = index_artifact(root, config_path) if config_path is not None else None
    inputs = index_artifacts(root, input_paths)
    outputs = index_artifacts(root, output_paths)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "runner_version": runner_version,
        "command": sanitized_command,
        "git": git_state.model_dump(mode="json"),
        "environment": capture_environment(dependency_snapshot).model_dump(mode="json"),
        "random_seed": random_seed,
        "config": config.model_dump(mode="json") if config is not None else None,
        "inputs": {name: record.model_dump(mode="json") for name, record in inputs.items()},
        "outputs": {name: record.model_dump(mode="json") for name, record in outputs.items()},
    }
    return ProvenanceManifest(
        **payload,
        created_at=created_at or datetime.now(UTC).replace(microsecond=0).isoformat(),
        content_hash=hash_payload(payload),
    )


def write_provenance_manifest(
    path: str | Path,
    manifest: ProvenanceManifest,
) -> Path:
    """Write a provenance manifest as stable sorted JSON."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        manifest.model_dump_json(indent=2, by_alias=False),
        encoding="utf-8",
    )
    # Pydantic preserves model field order; rewrite with hash_payload-compatible
    # sorted keys so diffs remain stable across Python versions.
    import json

    payload = manifest.model_dump(mode="json")
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def sanitize_command(command: str | list[str]) -> list[str]:
    """Redact secret-like command values while preserving replay shape."""

    tokens = shlex.split(command) if isinstance(command, str) else [str(token) for token in command]
    sanitized: list[str] = []
    redact_next = False
    for token in tokens:
        if redact_next:
            sanitized.append("<redacted>")
            redact_next = False
            continue
        if _is_secret_assignment(token):
            sanitized.append(token.split("=", maxsplit=1)[0] + "=<redacted>")
            continue
        sanitized.append(token)
        if _is_secret_flag(token):
            redact_next = True
    return sanitized


def _is_secret_assignment(token: str) -> bool:
    if "=" not in token:
        return False
    key = token.split("=", maxsplit=1)[0].strip("-").lower().replace("_", "-")
    return any(marker in key for marker in SECRET_MARKERS)


def _is_secret_flag(token: str) -> bool:
    if not token.startswith("-"):
        return False
    key = token.strip("-").lower().replace("_", "-")
    return any(marker in key for marker in SECRET_MARKERS)


def _read_git_sha(repo_root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def _read_git_dirty(repo_root: Path) -> bool:
    try:
        result = subprocess.run(
            ["git", "status", "--short"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return True
    return bool(result.stdout.strip())
