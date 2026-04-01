"""Reference data loaders."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from portfolio_os.compliance.findings import build_finding
from portfolio_os.data.import_profiles import ImportProfile
from portfolio_os.data.loaders import (
    ensure_columns,
    ensure_unique_tickers,
    normalize_ticker,
    parse_bool,
    read_input_frame,
)
from portfolio_os.domain.enums import FindingCategory, FindingSeverity, RepairStatus
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.domain.models import ComplianceFinding, ReferenceRow, ReferenceSnapshot


def load_reference_snapshot(
    path: str | Path,
    required_tickers: Iterable[str],
    *,
    import_profile: ImportProfile | None = None,
) -> ReferenceSnapshot:
    """Load reference data and validate required industry coverage."""

    frame = read_input_frame(path, input_type="reference", import_profile=import_profile)
    ensure_columns(frame, ["ticker", "industry", "blacklist_buy", "blacklist_sell"], str(path))
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    ensure_unique_tickers(frame, str(path))
    required = sorted(set(required_tickers))
    available = set(frame["ticker"].tolist())
    missing = sorted(set(required) - available)
    if missing:
        raise InputValidationError(
            f"reference.csv is missing required ticker(s): {', '.join(missing)}"
        )
    frame = frame[frame["ticker"].isin(required)].copy()
    if frame["industry"].isna().any() or (frame["industry"].astype(str).str.strip() == "").any():
        raise InputValidationError(
            "reference.csv contains missing industry values for holdings or target tickers."
        )
    for column in ("blacklist_buy", "blacklist_sell"):
        frame[column] = frame[column].map(lambda value, field=column: parse_bool(value, field))
    if "benchmark_weight" not in frame.columns:
        frame["benchmark_weight"] = None
    if "manager_aggregate_qty" not in frame.columns:
        frame["manager_aggregate_qty"] = None
    if "issuer_total_shares" not in frame.columns:
        frame["issuer_total_shares"] = None
    if "benchmark_weight" in frame.columns and frame["benchmark_weight"].notna().any():
        if (pd.to_numeric(frame.loc[frame["benchmark_weight"].notna(), "benchmark_weight"], errors="raise") < 0).any():
            raise InputValidationError("reference.csv contains negative benchmark_weight values.")
    rows = [
        ReferenceRow(
            ticker=row["ticker"],
            industry=str(row["industry"]).strip(),
            blacklist_buy=bool(row["blacklist_buy"]),
            blacklist_sell=bool(row["blacklist_sell"]),
            benchmark_weight=(
                float(row["benchmark_weight"]) if pd.notna(row["benchmark_weight"]) else None
            ),
            manager_aggregate_qty=(
                float(row["manager_aggregate_qty"])
                if pd.notna(row["manager_aggregate_qty"])
                else None
            ),
            issuer_total_shares=(
                float(row["issuer_total_shares"])
                if pd.notna(row["issuer_total_shares"])
                else None
            ),
        )
        for row in frame.to_dict(orient="records")
    ]
    return ReferenceSnapshot(rows=rows)


def reference_to_frame(snapshot: ReferenceSnapshot) -> pd.DataFrame:
    """Convert a reference snapshot to a DataFrame."""

    frame = pd.DataFrame([row.model_dump(mode="json") for row in snapshot.rows])
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "ticker",
                "industry",
                "blacklist_buy",
                "blacklist_sell",
                "benchmark_weight",
                "manager_aggregate_qty",
                "issuer_total_shares",
            ]
        )
    frame["manager_aggregate_qty"] = frame["manager_aggregate_qty"].fillna(0.0)
    frame["issuer_total_shares"] = frame["issuer_total_shares"].fillna(0.0)
    return frame.sort_values("ticker").reset_index(drop=True)


def build_reference_data_quality_findings(reference_frame: pd.DataFrame) -> list[ComplianceFinding]:
    """Return non-blocking reference-data quality findings."""

    findings: list[ComplianceFinding] = []
    if "benchmark_weight" not in reference_frame.columns:
        return findings
    available_benchmark_weights = reference_frame["benchmark_weight"].dropna()
    if available_benchmark_weights.empty:
        return findings
    benchmark_weight_total = float(available_benchmark_weights.sum())
    if benchmark_weight_total > 1.10 or (
        len(available_benchmark_weights) == len(reference_frame) and benchmark_weight_total < 0.05
    ):
        findings.append(
            build_finding(
                "benchmark_weight_total_anomaly",
                FindingCategory.DATA_QUALITY,
                FindingSeverity.WARNING,
                "Reference benchmark weights look abnormal for the covered universe and should be reviewed before using benchmark-relative interpretation.",
                rule_source="reference.csv",
                blocking=False,
                repair_status=RepairStatus.NOT_NEEDED,
                details={
                    "benchmark_weight_total": benchmark_weight_total,
                    "covered_ticker_count": int(len(reference_frame)),
                    "benchmark_weight_count": int(len(available_benchmark_weights)),
                },
            )
        )
    return findings
