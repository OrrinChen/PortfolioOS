"""Cache invalidation explanations."""

from __future__ import annotations

from pydantic import BaseModel, Field

from portfolio_os.cache.cache_key import CacheKey


class CacheInvalidation(BaseModel):
    """Field-level comparison between two cache keys."""

    changed_fields: list[str] = Field(default_factory=list)


def explain_cache_invalidation(previous: CacheKey, current: CacheKey) -> CacheInvalidation:
    """Explain which key parts changed between two cache keys."""

    previous_parts = previous.parts.model_dump(mode="json")
    current_parts = current.parts.model_dump(mode="json")
    changed = [
        field_name
        for field_name in sorted(previous_parts)
        if previous_parts[field_name] != current_parts.get(field_name)
    ]
    return CacheInvalidation(changed_fields=changed)
