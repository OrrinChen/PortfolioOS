"""Simple local run catalog helpers."""

from __future__ import annotations

from pathlib import Path


def list_run_manifests(root: str | Path) -> list[Path]:
    """List run manifests under a root directory."""

    return sorted(Path(root).glob("**/run_manifest.json"))

