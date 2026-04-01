"""Snapshot bundle builder for market/reference/target preparation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.data.builders.common import builder_manifest_path, classify_builder_error, get_provider_report
from portfolio_os.data.builders.market_builder import (
    build_market_frame,
    build_market_manifest,
    load_tickers_file,
    write_market_csv,
)
from portfolio_os.data.builders.reference_builder import (
    build_reference_frame,
    build_reference_manifest,
    write_reference_csv,
)
from portfolio_os.data.builders.target_builder import (
    build_target_frame,
    build_target_manifest,
    target_manifest_path,
    write_target_csv,
)
from portfolio_os.data.providers.base import DataProvider
from portfolio_os.domain.errors import InputValidationError, PortfolioOSError
from portfolio_os.storage.snapshots import file_metadata, write_json


def _build_step_manifest_metadata(manifest_path: Path, output_path: Path) -> dict[str, Any]:
    """Return step-level metadata for one built artifact."""

    return {
        "manifest": file_metadata(manifest_path),
        "output": file_metadata(output_path) if output_path.exists() else None,
    }


def _humanize_alternative_path(path_code: str | None) -> str | None:
    """Return a user-facing message for a recommended alternative path code."""

    if path_code == "provide_target_csv_and_continue":
        return "Provide target.csv from the client side and continue."
    if path_code == "run_market_and_reference_builders_only":
        return "Continue with market and reference builders only."
    if path_code == "provide_reference_csv_and_continue":
        return "Provide reference.csv from the client side and continue."
    return path_code


def build_snapshot_bundle(
    *,
    provider: DataProvider,
    tickers_file: str | Path,
    index_code: str,
    as_of_date: str,
    output_dir: str | Path,
    reference_overlay: str | Path | None = None,
    allow_partial_build: bool = False,
) -> dict[str, Any]:
    """Build a full standard static snapshot bundle and its manifests."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    tickers = load_tickers_file(tickers_file)

    market_path = output_path / "market.csv"
    reference_path = output_path / "reference.csv"
    target_path = output_path / "target.csv"
    market_manifest_path = builder_manifest_path(market_path)
    reference_manifest_path = builder_manifest_path(reference_path)
    target_manifest_file = target_manifest_path(target_path)

    steps: dict[str, dict[str, Any]] = {}
    failures: list[tuple[str, str, str | None]] = []

    try:
        market_frame = build_market_frame(
            provider=provider,
            tickers=tickers,
            as_of_date=as_of_date,
        )
        write_market_csv(market_frame, market_path)
        market_manifest = build_market_manifest(
            provider=provider,
            as_of_date=as_of_date,
            tickers_file=tickers_file,
            output_path=market_path,
            tickers=tickers,
        )
        write_json(market_manifest_path, market_manifest)
        steps["market"] = {
            "build_status": market_manifest["build_status"],
            "provider_capability_status": market_manifest["provider_capability_status"],
            "fallback_notes": market_manifest["fallback_notes"],
            "permission_notes": market_manifest["permission_notes"],
            "recommended_alternative_path": market_manifest["recommended_alternative_path"],
            **_build_step_manifest_metadata(market_manifest_path, market_path),
        }
    except Exception as exc:
        build_status = classify_builder_error(exc)
        market_manifest = build_market_manifest(
            provider=provider,
            as_of_date=as_of_date,
            tickers_file=tickers_file,
            output_path=market_path,
            tickers=tickers,
            build_status=build_status,
            error_message=str(exc),
        )
        write_json(market_manifest_path, market_manifest)
        steps["market"] = {
            "build_status": market_manifest["build_status"],
            "provider_capability_status": market_manifest["provider_capability_status"],
            "fallback_notes": market_manifest["fallback_notes"],
            "permission_notes": market_manifest["permission_notes"],
            "recommended_alternative_path": market_manifest["recommended_alternative_path"],
            "error_message": str(exc),
            "manifest": file_metadata(market_manifest_path),
            "output": None,
        }
        failures.append(("market", build_status, market_manifest["recommended_alternative_path"]))

    try:
        reference_frame = build_reference_frame(
            provider=provider,
            tickers=tickers,
            as_of_date=as_of_date,
            overlay_path=reference_overlay,
        )
        write_reference_csv(reference_frame, reference_path)
        reference_manifest = build_reference_manifest(
            provider=provider,
            as_of_date=as_of_date,
            tickers_file=tickers_file,
            overlay_path=reference_overlay,
            output_path=reference_path,
            frame=reference_frame,
        )
        write_json(reference_manifest_path, reference_manifest)
        steps["reference"] = {
            "build_status": reference_manifest["build_status"],
            "provider_capability_status": reference_manifest["provider_capability_status"],
            "fallback_notes": reference_manifest["fallback_notes"],
            "permission_notes": reference_manifest["permission_notes"],
            "recommended_alternative_path": reference_manifest["recommended_alternative_path"],
            **_build_step_manifest_metadata(reference_manifest_path, reference_path),
        }
    except Exception as exc:
        build_status = classify_builder_error(exc)
        reference_manifest = build_reference_manifest(
            provider=provider,
            as_of_date=as_of_date,
            tickers_file=tickers_file,
            overlay_path=reference_overlay,
            output_path=reference_path,
            frame=pd.DataFrame(columns=["ticker"]),
            build_status=build_status,
            error_message=str(exc),
        )
        write_json(reference_manifest_path, reference_manifest)
        steps["reference"] = {
            "build_status": reference_manifest["build_status"],
            "provider_capability_status": reference_manifest["provider_capability_status"],
            "fallback_notes": reference_manifest["fallback_notes"],
            "permission_notes": reference_manifest["permission_notes"],
            "recommended_alternative_path": reference_manifest["recommended_alternative_path"],
            "error_message": str(exc),
            "manifest": file_metadata(reference_manifest_path),
            "output": None,
        }
        failures.append(("reference", build_status, reference_manifest["recommended_alternative_path"]))

    try:
        target_frame, target_details = build_target_frame(
            provider=provider,
            index_code=index_code,
            as_of_date=as_of_date,
        )
        write_target_csv(target_frame, target_path)
        target_manifest = build_target_manifest(
            provider=provider,
            as_of_date=as_of_date,
            index_code=index_code,
            output_path=target_path,
            details=target_details,
            frame=target_frame,
        )
        write_json(target_manifest_file, target_manifest)
        steps["target"] = {
            "build_status": target_manifest["build_status"],
            "provider_capability_status": target_manifest["provider_capability_status"],
            "fallback_notes": target_manifest["fallback_notes"],
            "permission_notes": target_manifest["permission_notes"],
            "recommended_alternative_path": target_manifest["recommended_alternative_path"],
            **_build_step_manifest_metadata(target_manifest_file, target_path),
        }
    except Exception as exc:
        build_status = classify_builder_error(exc)
        target_manifest = build_target_manifest(
            provider=provider,
            as_of_date=as_of_date,
            index_code=index_code,
            output_path=target_path,
            details={
                "index_code": index_code,
                "input_weight_sum": None,
                "output_weight_sum": None,
                "normalized": False,
                "normalization_tolerance": 0.02,
            },
            frame=pd.DataFrame(columns=["ticker", "target_weight"]),
            build_status=build_status,
            error_message=str(exc),
        )
        write_json(target_manifest_file, target_manifest)
        steps["target"] = {
            "build_status": target_manifest["build_status"],
            "provider_capability_status": target_manifest["provider_capability_status"],
            "fallback_notes": target_manifest["fallback_notes"],
            "permission_notes": target_manifest["permission_notes"],
            "recommended_alternative_path": target_manifest["recommended_alternative_path"],
            "error_message": str(exc),
            "manifest": file_metadata(target_manifest_file),
            "output": None,
        }
        failures.append(("target", build_status, target_manifest["recommended_alternative_path"]))

    snapshot_manifest_path = output_path / "snapshot_manifest.json"
    snapshot_manifest = {
        "provider": getattr(provider, "provider_name", "unknown"),
        "provider_token_source": getattr(provider, "provider_metadata", {}).get("provider_token_source"),
        "as_of_date": as_of_date,
        "tickers_input_path": str(tickers_file),
        "index_code": index_code,
        "overlay_path": str(reference_overlay) if reference_overlay is not None else None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "build_status": (
            "success_with_degradation"
            if (not failures and any(step["provider_capability_status"] == "degraded" for step in steps.values()))
            else (
                "success"
                if not failures
                else (
                    "success_with_degradation"
                    if allow_partial_build
                    else (
                        "failed_permission"
                        if any(item[1] == "failed_permission" for item in failures)
                        else ("failed_runtime" if any(item[1] == "failed_runtime" for item in failures) else "failed_data")
                    )
                )
            )
        ),
        "provider_capability_status": (
            "unavailable"
            if any(step["provider_capability_status"] == "unavailable" for step in steps.values())
            else ("degraded" if any(step["provider_capability_status"] == "degraded" for step in steps.values()) else "available")
        ),
        "fallback_notes": [note for step in steps.values() for note in step.get("fallback_notes", [])],
        "permission_notes": [note for step in steps.values() for note in step.get("permission_notes", [])],
        "recommended_alternative_path": (
            next(
                (
                    recommendation
                    for _, _, recommendation in failures
                    if recommendation is not None
                ),
                None,
            )
        ),
        "steps": steps,
        "child_manifests": {
            key: value["manifest"]
            for key, value in steps.items()
        },
        "output_files": {
            key: value["output"]
            for key, value in steps.items()
            if value["output"] is not None
        },
        "notes": [
            "Snapshot bundle locks a static cross-sectional input set for downstream PortfolioOS workflows.",
            "Provider token values are never written; only provider_token_source is recorded.",
        ],
    }
    write_json(snapshot_manifest_path, snapshot_manifest)

    if failures and not allow_partial_build:
        first_recommendation = next((item[2] for item in failures if item[2] is not None), None)
        recommendation_text = (
            f" Recommended alternative: {_humanize_alternative_path(first_recommendation)}"
            if first_recommendation is not None
            else ""
        )
        failed_steps = ", ".join(step for step, _, _ in failures)
        raise InputValidationError(
            f"Snapshot bundle completed only partially. Failed step(s): {failed_steps}."
            f"{recommendation_text}"
        )

    return {
        "market_path": str(market_path) if market_path.exists() else None,
        "reference_path": str(reference_path) if reference_path.exists() else None,
        "target_path": str(target_path) if target_path.exists() else None,
        "market_manifest_path": str(market_manifest_path),
        "reference_manifest_path": str(reference_manifest_path),
        "target_manifest_path": str(target_manifest_file),
        "snapshot_manifest_path": str(snapshot_manifest_path),
        "build_status": snapshot_manifest["build_status"],
        "recommended_alternative_path": snapshot_manifest["recommended_alternative_path"],
    }
