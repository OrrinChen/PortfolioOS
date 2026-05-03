"""Result storage for local batch orchestration."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel


class LocalResultStore:
    """Write local batch result artifacts as stable JSON."""

    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.results_dir = self.output_dir / "results"
        self.provenance_dir = self.output_dir / "provenance"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.provenance_dir.mkdir(parents=True, exist_ok=True)

    def result_path(self, candidate_id: str) -> Path:
        return self.results_dir / f"{candidate_id}.json"

    def provenance_path(self, candidate_id: str) -> Path:
        return self.provenance_dir / f"{candidate_id}_manifest.json"

    def relative_to_output(self, path: str | Path) -> str:
        return Path(path).relative_to(self.output_dir).as_posix()

    def write_model(self, path: str | Path, model: BaseModel) -> Path:
        return self.write_payload(path, model.model_dump(mode="json"))

    def write_payload(self, path: str | Path, payload: object) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return output_path
