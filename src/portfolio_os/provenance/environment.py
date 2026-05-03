"""Environment capture for provenance manifests."""

from __future__ import annotations

import platform
import sys

from pydantic import BaseModel, Field


class EnvironmentSnapshot(BaseModel):
    """Minimal local runtime metadata for reproducibility."""

    python_version: str
    platform: str
    dependency_snapshot: dict[str, str] = Field(default_factory=dict)


def capture_environment(
    dependency_snapshot: dict[str, str] | None = None,
) -> EnvironmentSnapshot:
    """Capture deterministic-enough runtime metadata."""

    return EnvironmentSnapshot(
        python_version=sys.version.split()[0],
        platform=platform.platform(),
        dependency_snapshot=dependency_snapshot or {},
    )
