"""Simple JSON content-addressed cache store."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from portfolio_os.cache.cache_key import CacheKey


class ContentAddressedStore:
    """Persist JSON payloads by cache-key digest."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, key: CacheKey) -> Path:
        return self.root / f"{key.digest}.json"

    def get_json(self, key: CacheKey) -> dict[str, Any] | None:
        path = self.path_for(key)
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Cache payload must be a JSON object: {path}")
        return payload

    def put_json(self, key: CacheKey, payload: dict[str, Any]) -> Path:
        path = self.path_for(key)
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return path
