"""Persistence helpers for text and JSON artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_file(path: str | Path) -> str:
    """Compute the SHA256 hash of a file."""

    hasher = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def file_metadata(path: str | Path) -> dict[str, Any]:
    """Collect metadata for an input or output file."""

    file_path = Path(path)
    return {
        "path": str(file_path),
        "size": file_path.stat().st_size,
        "sha256": sha256_file(file_path),
    }


def write_json(path: str | Path, payload: Any) -> None:
    """Write JSON to disk with UTF-8 encoding."""

    with Path(path).open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def write_text(path: str | Path, text: str) -> None:
    """Write text to disk with UTF-8 encoding."""

    with Path(path).open("w", encoding="utf-8") as handle:
        handle.write(text)

