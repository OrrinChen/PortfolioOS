"""Read-only artifact service contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel


READ_ONLY_ROUTES = (
    "GET /health",
    "GET /runs",
    "GET /runs/{run_id}",
    "GET /bundles/{bundle_id}",
    "GET /reports/{run_id}",
    "GET /decisions/{bundle_id}",
)


class ServiceResponse(BaseModel):
    """Framework-neutral service response."""

    status_code: int
    body: Any
    content_type: str = "application/json"


class ReadOnlyArtifactService:
    """Read local evaluation artifacts without triggering workflows."""

    def __init__(self, artifact_root: str | Path):
        self.artifact_root = Path(artifact_root)

    def route_contract(self) -> list[str]:
        """Return the supported read-only route contract."""

        return list(READ_ONLY_ROUTES)

    def handle(self, method: str, path: str) -> ServiceResponse:
        """Handle a read-only route request."""

        normalized_method = method.upper()
        if normalized_method != "GET":
            return ServiceResponse(
                status_code=405,
                body={"error": "read_only_service_allows_get_only"},
            )

        parts = [part for part in path.strip("/").split("/") if part]
        if parts == ["health"]:
            return ServiceResponse(status_code=200, body={"status": "ok", "mode": "read_only"})
        if parts == ["runs"]:
            return ServiceResponse(status_code=200, body={"runs": self._list_ids("runs", ".json")})
        if len(parts) == 2 and parts[0] == "runs":
            return self._read_json(f"runs/{parts[1]}.json")
        if len(parts) == 2 and parts[0] == "bundles":
            return self._read_json(f"bundles/{parts[1]}.json")
        if len(parts) == 2 and parts[0] == "decisions":
            return self._read_json(f"decisions/{parts[1]}.json")
        if len(parts) == 2 and parts[0] == "reports":
            return self._read_text(f"reports/{parts[1]}.md", content_type="text/markdown")
        return ServiceResponse(status_code=404, body={"error": "route_not_found"})

    def _list_ids(self, directory: str, suffix: str) -> list[str]:
        path = self.artifact_root / directory
        if not path.exists():
            return []
        return sorted(item.name[: -len(suffix)] for item in path.glob(f"*{suffix}") if item.is_file())

    def _read_json(self, relative_path: str) -> ServiceResponse:
        path = self.artifact_root / relative_path
        if not path.exists():
            return _missing_artifact(relative_path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        return ServiceResponse(status_code=200, body=payload)

    def _read_text(self, relative_path: str, *, content_type: str) -> ServiceResponse:
        path = self.artifact_root / relative_path
        if not path.exists():
            return _missing_artifact(relative_path)
        return ServiceResponse(
            status_code=200,
            body=path.read_text(encoding="utf-8"),
            content_type=content_type,
        )


def _missing_artifact(relative_path: str) -> ServiceResponse:
    return ServiceResponse(
        status_code=404,
        body={"error": "artifact_not_found", "artifact": relative_path},
    )
