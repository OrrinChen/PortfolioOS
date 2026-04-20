"""Builder for standard PortfolioOS `target.csv` files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.data.builders.common import build_builder_manifest, builder_manifest_path, get_provider_report
from portfolio_os.data.providers.base import DataProvider
from portfolio_os.data.loaders import ensure_unique_tickers, normalize_ticker
from portfolio_os.domain.errors import InputValidationError


TARGET_COLUMNS = ["ticker", "target_weight"]


def build_target_frame(
    *,
    provider: DataProvider,
    index_code: str,
    as_of_date: str,
    normalization_tolerance: float = 0.02,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build a validated standard `target.csv` frame."""

    rows = provider.get_index_weights(index_code, as_of_date)
    frame = pd.DataFrame([row.model_dump(mode="json") for row in rows], columns=TARGET_COLUMNS)
    if frame.empty:
        raise InputValidationError("Provider returned no index-weight rows.")
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    ensure_unique_tickers(frame, "provider index weights")
    frame["target_weight"] = pd.to_numeric(frame["target_weight"], errors="raise")
    if (frame["target_weight"] < 0).any():
        raise InputValidationError("Provider index weights contain negative values.")

    input_weight_sum = float(frame["target_weight"].sum())
    normalized = False
    if input_weight_sum <= 0:
        raise InputValidationError("Provider index weights sum to zero.")
    if input_weight_sum > 1.0 + normalization_tolerance:
        raise InputValidationError(
            f"Provider index weights sum to {input_weight_sum:.6f}, which is above the allowed tolerance."
        )
    if abs(input_weight_sum - 1.0) <= normalization_tolerance and abs(input_weight_sum - 1.0) > 1e-9:
        frame["target_weight"] = frame["target_weight"] / input_weight_sum
        normalized = True

    output_weight_sum = float(frame["target_weight"].sum())
    manifest = {
        "index_code": index_code,
        "input_weight_sum": input_weight_sum,
        "output_weight_sum": output_weight_sum,
        "normalized": normalized,
        "normalization_tolerance": normalization_tolerance,
    }
    return frame[TARGET_COLUMNS].sort_values("ticker").reset_index(drop=True), manifest


def write_target_csv(frame: pd.DataFrame, path: str | Path) -> None:
    """Write a standard `target.csv` file."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)


def target_manifest_path(output_path: str | Path) -> Path:
    """Return the companion manifest path for a target builder output."""

    return builder_manifest_path(output_path)


def build_target_manifest(
    *,
    provider: DataProvider,
    as_of_date: str,
    index_code: str,
    output_path: str | Path,
    details: dict[str, Any],
    frame: pd.DataFrame,
    build_status: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build the sidecar manifest for one `target.csv` output."""

    approximation_notes = list(
        getattr(provider, "provider_metadata", {}).get("approximation_notes", {}).get("target", [])
    )
    provider_report = get_provider_report(provider, "target")
    resolved_build_status = build_status or (
        "success_with_degradation"
        if provider_report["provider_capability_status"] == "degraded"
        else "success"
    )
    manifest = build_builder_manifest(
        provider_name=getattr(provider, "provider_name", "unknown"),
        provider_metadata=getattr(provider, "provider_metadata", {}),
        as_of_date=as_of_date,
        request_parameters={
            "index_code": index_code,
            "normalization_tolerance": details["normalization_tolerance"],
        },
        output_path=output_path,
        row_count=len(frame),
        approximation_notes=approximation_notes,
        build_status=resolved_build_status,
        provider_capability_status=provider_report["provider_capability_status"],
        fallback_notes=list(provider_report["fallback_notes"]),
        fallback_chain_used=list(provider_report.get("fallback_chain_used", [])),
        data_source_mix=list(provider_report.get("data_source_mix", [])),
        permission_notes=list(provider_report["permission_notes"]),
        recommended_alternative_path=provider_report["recommended_alternative_path"],
        error_message=error_message,
    )
    manifest.update(details)
    return manifest
