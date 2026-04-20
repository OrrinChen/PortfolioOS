"""Shared helpers for data builder manifests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from portfolio_os.domain.errors import InputValidationError, ProviderPermissionError, ProviderRuntimeError
from portfolio_os.storage.snapshots import file_metadata


def builder_manifest_path(output_path: str | Path) -> Path:
    """Return the sidecar manifest path for one builder output."""

    path = Path(output_path)
    return path.with_name(f"{path.stem}_manifest.json")


def build_builder_manifest(
    *,
    provider_name: str,
    provider_metadata: dict[str, Any] | None,
    as_of_date: str,
    request_parameters: dict[str, Any],
    output_path: str | Path,
    row_count: int,
    approximation_notes: list[str],
    build_status: str = "success",
    provider_capability_status: str = "available",
    fallback_notes: list[str] | None = None,
    fallback_chain_used: list[str] | None = None,
    data_source_mix: list[str] | None = None,
    permission_notes: list[str] | None = None,
    recommended_alternative_path: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build a standard builder artifact manifest."""

    output_file = Path(output_path)
    output_metadata = file_metadata(output_file) if output_file.exists() else None
    metadata = provider_metadata or {}
    return {
        "provider": provider_name,
        "provider_token_source": metadata.get("provider_token_source"),
        "as_of_date": as_of_date,
        "request_parameters": request_parameters,
        "output_path": str(output_path),
        "output_sha256": output_metadata["sha256"] if output_metadata is not None else None,
        "row_count": row_count,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "approximation_notes": approximation_notes,
        "provider_capability_status": provider_capability_status,
        "fallback_notes": fallback_notes or [],
        "fallback_chain_used": fallback_chain_used or [],
        "data_source_mix": data_source_mix or [provider_name],
        "permission_notes": permission_notes or [],
        "recommended_alternative_path": recommended_alternative_path,
        "build_status": build_status,
        "error_message": error_message,
    }


def get_provider_report(provider: Any, feed_name: str) -> dict[str, Any]:
    """Return a provider capability report for one feed, with safe defaults."""

    if hasattr(provider, "get_capability_report"):
        report = provider.get_capability_report(feed_name)
        if isinstance(report, dict):
            return report
    return {
        "provider_capability_status": "available",
        "fallback_notes": [],
        "fallback_chain_used": [],
        "data_source_mix": [],
        "permission_notes": [],
        "recommended_alternative_path": None,
    }


def classify_builder_error(error: Exception) -> str:
    """Map an exception to a builder build_status value."""

    if isinstance(error, ProviderPermissionError):
        return "failed_permission"
    if isinstance(error, InputValidationError):
        return "failed_data"
    if isinstance(error, ProviderRuntimeError):
        return "failed_runtime"
    return "failed_runtime"
