"""Validation helpers for research-to-platform promotion bundles."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portfolio_os.domain.errors import InputValidationError


PROMOTION_CONTRACT_TYPE = "portfolio_os_research_promotion_bundle"
PROMOTION_CONTRACT_VERSION = "1.0"
PROMOTION_CONTRACT_FILENAME = "promotion_bundle.json"


@dataclass(frozen=True)
class PromotionSignalContract:
    """One signal carried inside a promotion bundle."""

    name: str
    stage_bucket: str
    audit_summary_path: Path


@dataclass(frozen=True)
class PromotionComboContract:
    """Shared Stage 3 or later combination evidence."""

    summary_path: Path
    eligible_for_stage4: bool
    blocking_reason: str
    full_sample_ir: float
    second_half_ir: float


@dataclass(frozen=True)
class PromotionContract:
    """Validated promotion bundle metadata plus resolved artifact paths."""

    bundle_dir: Path
    manifest_path: Path
    contract_type: str
    contract_version: str
    bundle_id: str
    created_at: str
    research_line: str
    candidate_status: str
    thesis_summary: str
    universe_name: str
    signals: list[PromotionSignalContract]
    combo: PromotionComboContract
    memory_path: Path
    ledger_path: Path


def _require_mapping(payload: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise InputValidationError(f"Promotion contract field `{field_name}` must be an object.")
    return payload


def _require_list(payload: Any, field_name: str) -> list[Any]:
    if not isinstance(payload, list):
        raise InputValidationError(f"Promotion contract field `{field_name}` must be a list.")
    return payload


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise InputValidationError(f"Promotion contract field `{key}` must be a non-empty string.")
    return value.strip()


def _require_bool(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise InputValidationError(f"Promotion contract field `{key}` must be a boolean.")
    return value


def _require_float(payload: dict[str, Any], key: str) -> float:
    value = payload.get(key)
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise InputValidationError(f"Promotion contract field `{key}` must be numeric.") from exc


def _resolve_artifact_path(bundle_dir: Path, relative_path: str) -> Path:
    artifact_path = Path(relative_path)
    if artifact_path.is_absolute():
        raise InputValidationError(
            f"Promotion contract artifact paths must be relative to the bundle directory, got `{relative_path}`."
        )
    resolved = (bundle_dir / artifact_path).resolve()
    if not resolved.exists():
        raise InputValidationError(f"Missing promotion contract artifact: `{relative_path}`.")
    return resolved


def load_promotion_contract(bundle_dir: str | Path) -> PromotionContract:
    """Load and validate one promotion bundle directory."""

    bundle_root = Path(bundle_dir).resolve()
    manifest_path = bundle_root / PROMOTION_CONTRACT_FILENAME
    if not manifest_path.exists():
        raise InputValidationError(
            f"Promotion bundle is missing `{PROMOTION_CONTRACT_FILENAME}`: `{bundle_root}`."
        )

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise InputValidationError(f"Promotion bundle manifest is not valid JSON: `{manifest_path}`.") from exc

    manifest = _require_mapping(payload, "root")
    contract_type = _require_string(manifest, "contract_type")
    contract_version = _require_string(manifest, "contract_version")
    if contract_type != PROMOTION_CONTRACT_TYPE:
        raise InputValidationError(
            f"Unsupported promotion contract type `{contract_type}`; expected `{PROMOTION_CONTRACT_TYPE}`."
        )
    if contract_version != PROMOTION_CONTRACT_VERSION:
        raise InputValidationError(
            f"Unsupported promotion contract version `{contract_version}`; expected `{PROMOTION_CONTRACT_VERSION}`."
        )

    thesis = _require_mapping(manifest.get("thesis"), "thesis")
    combo_payload = _require_mapping(manifest.get("combo"), "combo")
    artifacts_payload = _require_mapping(manifest.get("artifacts"), "artifacts")

    signal_payloads = _require_list(manifest.get("signals"), "signals")
    if not signal_payloads:
        raise InputValidationError("Promotion contract must include at least one signal.")

    signals: list[PromotionSignalContract] = []
    for index, signal_value in enumerate(signal_payloads):
        signal_payload = _require_mapping(signal_value, f"signals[{index}]")
        signals.append(
            PromotionSignalContract(
                name=_require_string(signal_payload, "name"),
                stage_bucket=_require_string(signal_payload, "stage_bucket"),
                audit_summary_path=_resolve_artifact_path(
                    bundle_root,
                    _require_string(signal_payload, "audit_summary_path"),
                ),
            )
        )

    combo = PromotionComboContract(
        summary_path=_resolve_artifact_path(bundle_root, _require_string(combo_payload, "summary_path")),
        eligible_for_stage4=_require_bool(combo_payload, "eligible_for_stage4"),
        blocking_reason=_require_string(combo_payload, "blocking_reason"),
        full_sample_ir=_require_float(combo_payload, "full_sample_ir"),
        second_half_ir=_require_float(combo_payload, "second_half_ir"),
    )

    return PromotionContract(
        bundle_dir=bundle_root,
        manifest_path=manifest_path,
        contract_type=contract_type,
        contract_version=contract_version,
        bundle_id=_require_string(manifest, "bundle_id"),
        created_at=_require_string(manifest, "created_at"),
        research_line=_require_string(manifest, "research_line"),
        candidate_status=_require_string(manifest, "candidate_status"),
        thesis_summary=_require_string(thesis, "summary"),
        universe_name=_require_string(thesis, "universe_name"),
        signals=signals,
        combo=combo,
        memory_path=_resolve_artifact_path(bundle_root, _require_string(artifacts_payload, "memory_path")),
        ledger_path=_resolve_artifact_path(bundle_root, _require_string(artifacts_payload, "ledger_path")),
    )


def validate_promotion_contract(bundle_dir: str | Path) -> PromotionContract:
    """Alias kept for call sites that want validation semantics explicitly."""

    return load_promotion_contract(bundle_dir)
