"""Builder for standard PortfolioOS `reference.csv` files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.data.builders.common import build_builder_manifest, get_provider_report
from portfolio_os.data.loaders import ensure_unique_tickers, normalize_ticker, parse_bool, read_csv
from portfolio_os.data.providers.base import DataProvider
from portfolio_os.domain.errors import InputValidationError


REFERENCE_COLUMNS = [
    "ticker",
    "industry",
    "blacklist_buy",
    "blacklist_sell",
    "benchmark_weight",
    "manager_aggregate_qty",
    "issuer_total_shares",
]

OVERLAY_COLUMNS = {
    "blacklist_buy",
    "blacklist_sell",
    "manager_aggregate_qty",
}


def build_reference_frame(
    *,
    provider: DataProvider,
    tickers: list[str],
    as_of_date: str,
    overlay_path: str | Path | None = None,
) -> pd.DataFrame:
    """Build a validated standard `reference.csv` frame."""

    rows = provider.get_reference_snapshot(tickers, as_of_date)
    frame = pd.DataFrame([row.model_dump(mode="json") for row in rows])
    if frame.empty:
        raise InputValidationError("Provider returned no reference rows.")
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    ensure_unique_tickers(frame, "provider reference snapshot")
    missing = sorted(set(tickers) - set(frame["ticker"].tolist()))
    if missing:
        raise InputValidationError(
            f"Provider reference snapshot is missing ticker(s): {', '.join(missing)}"
        )
    if frame["industry"].isna().any() or (frame["industry"].astype(str).str.strip() == "").any():
        raise InputValidationError("Provider reference snapshot contains missing industry values.")
    frame["blacklist_buy"] = False
    frame["blacklist_sell"] = False
    frame["manager_aggregate_qty"] = 0.0
    frame["benchmark_weight"] = frame["benchmark_weight"].astype(float)
    frame["issuer_total_shares"] = frame["issuer_total_shares"].astype(float)

    if overlay_path is not None:
        overlay = read_csv(overlay_path)
        if "ticker" not in overlay.columns:
            raise InputValidationError("reference overlay must contain a ticker column.")
        overlay["ticker"] = overlay["ticker"].map(normalize_ticker)
        ensure_unique_tickers(overlay, "reference overlay")
        overlay = overlay[overlay["ticker"].isin(frame["ticker"])].copy()
        for column in OVERLAY_COLUMNS:
            if column not in overlay.columns:
                continue
            if column in {"blacklist_buy", "blacklist_sell"}:
                overlay[column] = overlay[column].map(
                    lambda value, field=column: parse_bool(value, field)
                )
            else:
                overlay[column] = pd.to_numeric(overlay[column], errors="raise")
            frame = frame.merge(
                overlay[["ticker", column]],
                on="ticker",
                how="left",
                suffixes=("", "_overlay"),
            )
            frame[column] = frame[f"{column}_overlay"].combine_first(frame[column])
            frame = frame.drop(columns=[f"{column}_overlay"])

    return frame[REFERENCE_COLUMNS].sort_values("ticker").reset_index(drop=True)


def write_reference_csv(frame: pd.DataFrame, path: str | Path) -> None:
    """Write a standard `reference.csv` file."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)


def build_reference_manifest(
    *,
    provider: DataProvider,
    as_of_date: str,
    tickers_file: str | Path,
    overlay_path: str | Path | None,
    output_path: str | Path,
    frame: pd.DataFrame,
    build_status: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build the sidecar manifest for one `reference.csv` output."""

    approximation_notes = list(
        getattr(provider, "provider_metadata", {}).get("approximation_notes", {}).get("reference", [])
    )
    provider_report = get_provider_report(provider, "reference")
    resolved_build_status = build_status or (
        "success_with_degradation"
        if provider_report["provider_capability_status"] == "degraded"
        else "success"
    )
    return build_builder_manifest(
        provider_name=getattr(provider, "provider_name", "unknown"),
        provider_metadata=getattr(provider, "provider_metadata", {}),
        as_of_date=as_of_date,
        request_parameters={
            "tickers_file": str(tickers_file),
            "overlay_path": str(overlay_path) if overlay_path is not None else None,
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
