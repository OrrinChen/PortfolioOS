"""Validation helpers for evidence bundle artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from evidence_bundle.schema import EvidenceBundle


def load_evidence_bundle(path: str | Path) -> EvidenceBundle:
    """Load and validate one evidence bundle YAML file."""

    resolved_path = Path(path)
    with resolved_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"{resolved_path} must contain a YAML mapping")
    return EvidenceBundle.model_validate(payload)


def deterministic_bundle_json(bundle: EvidenceBundle) -> str:
    """Serialize an evidence bundle with deterministic key ordering."""

    return json.dumps(bundle.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
