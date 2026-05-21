"""Artifact metadata indexing for provenance manifests."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from portfolio_os.provenance.hashing import sha256_file


class ArtifactRecord(BaseModel):
    """Content-addressed metadata for one local artifact."""

    path: str
    sha256: str
    size_bytes: int


def index_artifact(repo_root: str | Path, path: str | Path) -> ArtifactRecord:
    """Index one local artifact without reading it into the manifest."""

    root = Path(repo_root).resolve()
    artifact_path = Path(path).resolve()
    relative_path = _relative_path(root, artifact_path)
    return ArtifactRecord(
        path=relative_path,
        sha256=sha256_file(artifact_path),
        size_bytes=artifact_path.stat().st_size,
    )


def index_artifacts(
    repo_root: str | Path,
    paths: dict[str, str | Path],
) -> dict[str, ArtifactRecord]:
    """Index a named artifact mapping."""

    return {
        str(name): index_artifact(repo_root, path)
        for name, path in sorted(paths.items())
    }


def _relative_path(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()
