"""Builder for standard PortfolioOS `market.csv` files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.data.builders.common import build_builder_manifest, get_provider_report
from portfolio_os.data.loaders import ensure_positive, ensure_unique_tickers, normalize_ticker
from portfolio_os.data.providers.base import DataProvider
from portfolio_os.domain.errors import InputValidationError


MARKET_COLUMNS = [
    "ticker",
    "close",
    "vwap",
    "adv_shares",
    "tradable",
    "upper_limit_hit",
    "lower_limit_hit",
]


def load_tickers_file(path: str | Path) -> list[str]:
    """Load ticker symbols from a `.txt` file or a simple CSV."""

    input_path = Path(path)
    if input_path.suffix.lower() == ".txt":
        tickers = [
            normalize_ticker(line)
            for line in input_path.read_text(encoding="utf-8").splitlines()
            if str(line).strip()
        ]
    else:
        frame = pd.read_csv(input_path, dtype=str)
        if frame.empty:
            raise InputValidationError(f"Ticker file is empty: {input_path}")
        if "ticker" in frame.columns:
            values = frame["ticker"].tolist()
        else:
            values = frame.iloc[:, 0].tolist()
        tickers = [normalize_ticker(value) for value in values if str(value).strip()]
    deduped = list(dict.fromkeys(tickers))
    if not deduped:
        raise InputValidationError(f"No tickers were found in {input_path}.")
    return deduped


def build_market_frame(
    *,
    provider: DataProvider,
    tickers: list[str],
    as_of_date: str,
) -> pd.DataFrame:
    """Build a validated standard `market.csv` frame."""

    rows = provider.get_daily_market_snapshot(tickers, as_of_date)
    frame = pd.DataFrame([row.model_dump(mode="json") for row in rows], columns=MARKET_COLUMNS)
    if frame.empty:
        raise InputValidationError("Provider returned no market rows.")
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    ensure_unique_tickers(frame, "provider market snapshot")
    missing = sorted(set(tickers) - set(frame["ticker"].tolist()))
    if missing:
        raise InputValidationError(
            f"Provider market snapshot is missing ticker(s): {', '.join(missing)}"
        )
    ensure_positive(frame, "close", "provider market snapshot")
    ensure_positive(frame, "vwap", "provider market snapshot")
    ensure_positive(frame, "adv_shares", "provider market snapshot")
    return frame[MARKET_COLUMNS].sort_values("ticker").reset_index(drop=True)


def write_market_csv(frame: pd.DataFrame, path: str | Path) -> None:
    """Write a standard `market.csv` file."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)


def build_market_manifest(
    *,
    provider: DataProvider,
    as_of_date: str,
    tickers_file: str | Path,
    output_path: str | Path,
    tickers: list[str],
    build_status: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build the sidecar manifest for one `market.csv` output."""

    approximation_notes = list(
        getattr(provider, "provider_metadata", {}).get("approximation_notes", {}).get("market", [])
    )
    provider_report = get_provider_report(provider, "market")
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
            "tickers_count": len(tickers),
        },
        output_path=output_path,
        row_count=len(tickers),
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
