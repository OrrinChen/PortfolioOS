"""Content-addressed cache key construction."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from portfolio_os.provenance.hashing import hash_payload, sha256_file


CACHE_KEY_SCHEMA_VERSION = "portfolioos.cache_key.v1"


class CacheKeyParts(BaseModel):
    """Stable fields that determine cache identity."""

    schema_version: str = CACHE_KEY_SCHEMA_VERSION
    code_version: str
    input_hash: str
    config_hash: str | None = None
    runner_version: str
    seed: int | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class CacheKey(BaseModel):
    """Content-addressed cache key plus its source parts."""

    digest: str
    parts: CacheKeyParts


def build_cache_key(parts: CacheKeyParts) -> CacheKey:
    """Build a deterministic digest from cache key parts."""

    payload = parts.model_dump(mode="json")
    return CacheKey(digest=hash_payload(payload), parts=parts)


def build_file_cache_key(
    *,
    input_path: str | Path,
    config_path: str | Path | None,
    code_version: str,
    runner_version: str,
    seed: int | None = None,
    extra: dict[str, Any] | None = None,
) -> CacheKey:
    """Build a cache key from local input/config file content."""

    parts = CacheKeyParts(
        code_version=code_version,
        input_hash=sha256_file(input_path),
        config_hash=sha256_file(config_path) if config_path is not None else None,
        runner_version=runner_version,
        seed=seed,
        extra=extra or {},
    )
    return build_cache_key(parts)
