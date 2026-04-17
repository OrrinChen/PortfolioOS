"""Builder for contract-shaped state-transition daily panels."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.data.builders.common import build_builder_manifest, get_provider_report
from portfolio_os.data.loaders import (
    ensure_columns,
    ensure_non_negative,
    ensure_positive,
    ensure_unique_tickers,
    normalize_ticker,
    parse_bool,
)
from portfolio_os.domain.errors import InputValidationError


STATE_TRANSITION_DAILY_PANEL_COLUMNS = [
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "upper_limit_price",
    "lower_limit_price",
    "tradable",
    "industry",
    "issuer_total_shares",
]


def build_state_transition_daily_panel_frame(
    *,
    provider: Any,
    tickers: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Build one validated state-transition daily panel frame."""

    loader = getattr(provider, "get_state_transition_daily_panel", None)
    if loader is None or not callable(loader):
        raise InputValidationError(
            "Provider does not support state-transition daily panel history."
        )

    frame = loader(tickers, start_date, end_date)
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise InputValidationError("Provider returned no state-transition daily panel rows.")

    ensure_columns(frame, STATE_TRANSITION_DAILY_PANEL_COLUMNS, "state-transition daily panel")
    work = frame.loc[:, STATE_TRANSITION_DAILY_PANEL_COLUMNS].copy()
    work["ticker"] = work["ticker"].map(normalize_ticker)
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    if work["date"].isna().any():
        raise InputValidationError("state-transition daily panel contains invalid dates.")
    work["date"] = work["date"].dt.strftime("%Y-%m-%d")
    if work.duplicated(subset=["date", "ticker"]).any():
        raise InputValidationError(
            "state-transition daily panel contains duplicate (date, ticker) rows."
        )

    for column in ("tradable",):
        work[column] = work[column].map(lambda value, field=column: parse_bool(value, field))
    work["industry"] = work["industry"].astype(str).str.strip()
    if work["industry"].eq("").any():
        raise InputValidationError("state-transition daily panel contains blank industries.")

    for column in (
        "open",
        "high",
        "low",
        "close",
        "upper_limit_price",
        "lower_limit_price",
        "issuer_total_shares",
    ):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    for column in ("volume", "amount"):
        work[column] = pd.to_numeric(work[column], errors="coerce")

    ensure_positive(work, "open", "state-transition daily panel")
    ensure_positive(work, "high", "state-transition daily panel")
    ensure_positive(work, "low", "state-transition daily panel")
    ensure_positive(work, "close", "state-transition daily panel")
    ensure_positive(work, "upper_limit_price", "state-transition daily panel")
    ensure_positive(work, "lower_limit_price", "state-transition daily panel")
    ensure_positive(work, "issuer_total_shares", "state-transition daily panel")
    ensure_non_negative(work, "volume", "state-transition daily panel")
    ensure_non_negative(work, "amount", "state-transition daily panel")
    ensure_unique_tickers(
        work.loc[work["date"] == work["date"].min(), ["ticker"]].copy(),
        "state-transition daily panel start-date slice",
    )
    return work.sort_values(["date", "ticker"]).reset_index(drop=True)


def write_state_transition_daily_panel_csv(frame: pd.DataFrame, path: str | Path) -> None:
    """Write a contract-shaped state-transition daily panel CSV."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)


def build_state_transition_daily_panel_manifest(
    *,
    provider: Any,
    start_date: str,
    end_date: str,
    tickers_file: str | Path,
    output_path: str | Path,
    tickers: list[str],
    frame: pd.DataFrame,
    build_status: str = "success",
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build the sidecar manifest for one state-transition daily panel output."""

    provider_report = get_provider_report(provider, "state_transition_daily_panel")
    approximation_notes = list(
        getattr(provider, "provider_metadata", {})
        .get("approximation_notes", {})
        .get("state_transition_daily_panel", [])
    )
    return build_builder_manifest(
        provider_name=getattr(provider, "provider_name", "unknown"),
        provider_metadata=getattr(provider, "provider_metadata", {}),
        as_of_date=end_date,
        request_parameters={
            "tickers_file": str(tickers_file),
            "tickers_count": len(tickers),
            "start_date": start_date,
            "end_date": end_date,
        },
        output_path=output_path,
        row_count=len(frame),
        approximation_notes=approximation_notes,
        build_status=build_status,
        provider_capability_status=provider_report["provider_capability_status"],
        fallback_notes=list(provider_report["fallback_notes"]),
        fallback_chain_used=list(provider_report.get("fallback_chain_used", [])),
        data_source_mix=list(provider_report.get("data_source_mix", [])),
        permission_notes=list(provider_report["permission_notes"]),
        recommended_alternative_path=provider_report["recommended_alternative_path"],
        error_message=error_message,
    )
