"""Hashing helpers for provenance manifests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_file(path: str | Path) -> str:
    """Compute a SHA256 hash for one file."""

    hasher = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def canonical_json(payload: Any) -> str:
    """Serialize a JSON-compatible payload deterministically."""

    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_text(value: str) -> str:
    """Compute a SHA256 hash for text."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def hash_payload(payload: Any) -> str:
    """Hash a JSON-compatible payload deterministically."""

    return sha256_text(canonical_json(payload))
