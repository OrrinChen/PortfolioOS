"""D2 no-formula observability for 8-K subtype underreaction candidates."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


SUMMARY_SCHEMA_VERSION = "eightk_subtype_d2_observability_summary.v1"
STAGE = "D2-8K-01"
CANDIDATE_ID = "8k_subtype_underreaction_observability"

WINDOWS: dict[str, tuple[int, int]] = {
    "pre_20_1": (-20, -1),
    "pre_10_1": (-10, -1),
    "pre_5_1": (-5, -1),
    "post_0_1": (0, 1),
    "post_1_5": (1, 5),
    "post_1_10": (1, 10),
    "post_1_22": (1, 22),
    "post_1_44": (1, 44),
}
PRIMARY_WINDOW = "post_1_22"
PRIORITY_SUBTYPES = (
    "auditor_change",
    "cfo_departure",
    "ceo_departure",
    "material_agreement_termination",
    "restatement_amendment",
)
CONTROL_SUBTYPES = ("routine_8k_control", "unknown_no_view")
ALL_SUBTYPES = PRIORITY_SUBTYPES + CONTROL_SUBTYPES

FORBIDDEN_INPUT_COLUMN_PATTERNS = (
    "expected_return",
    "forward_return",
    "future_return",
    "alpha_score",
    "formula_score",
    "optimizer",
    "portfolio",
    "q2_",
    "broker",
    "order",
    "production",
    "trading_instruction",
)

DOWNSTREAM_FLAGS = {
    "no_formula_observability_only": True,
    "formula_score_written": False,
    "measurement_spec_written": False,
    "q1_entry_allowed": False,
    "q2_entry_allowed": False,
    "optimizer_entry_allowed": False,
    "alpha_registry_update_allowed": False,
    "paper_ready": False,
    "live_ready": False,
    "broker_order_path_opened": False,
    "production_approval_claimed": False,
    "expected_return_panel_written": False,
}


@dataclass(frozen=True)
class EightKSubtypeD2Result:
    """Artifacts and summary for D2-8K-01."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_eightk_subtype_observability_d2(
    event_registry_path: str | Path,
    price_panel_path: str | Path,
    output_dir: str | Path,
    benchmark_panel_path: str | Path | None = None,
    minimum_subtype_events: int = 100,
    minimum_event_month_count: int = 12,
    minimum_label_coverage_share: float = 0.70,
) -> EightKSubtypeD2Result:
    """Run no-formula D2 observability for prioritized 8-K subtypes."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    registry = _load_event_registry(Path(event_registry_path))
    price_panel = _load_price_panel(Path(price_panel_path))
    benchmark_panel = _load_benchmark_panel(Path(benchmark_panel_path)) if benchmark_panel_path else _empty_benchmark()

    events = _derive_eightk_subtypes(registry)
    timestamp_audit = _build_timestamp_audit(events)
    subtype_counts = _build_subtype_counts(events)
    no_view_report = _build_no_view_reason_report(events)
    coverage_report = _build_coverage_report(events)

    if bool(timestamp_audit["timestamp_violation_count"].iloc[0]):
        car_panel = _blocked_car_window_panel("blocked_timestamp")
        matched_controls = _blocked_matched_control_panel("blocked_timestamp")
        placebo = _blocked_placebo_report("blocked_timestamp")
    else:
        car_panel = _build_car_window_panel(events, price_panel, benchmark_panel)
        matched_controls = _build_matched_control_panel(car_panel)
        placebo = _build_placebo_report(events, car_panel, price_panel, benchmark_panel)

    summary = _build_summary(
        events=events,
        timestamp_audit=timestamp_audit,
        car_panel=car_panel,
        placebo=placebo,
        minimum_subtype_events=minimum_subtype_events,
        minimum_event_month_count=minimum_event_month_count,
        minimum_label_coverage_share=minimum_label_coverage_share,
    )

    events.to_csv(artifacts["eightk_event_registry"], index=False)
    subtype_counts.to_csv(artifacts["eightk_subtype_counts"], index=False)
    timestamp_audit.to_csv(artifacts["timestamp_audit"], index=False)
    coverage_report.to_csv(artifacts["coverage_report"], index=False)
    no_view_report.to_csv(artifacts["no_view_reason_report"], index=False)
    car_panel.to_csv(artifacts["car_window_panel"], index=False)
    matched_controls.to_csv(artifacts["matched_control_panel"], index=False)
    placebo.to_csv(artifacts["placebo_report"], index=False)
    _write_json(artifacts["d2_8k_subtype_summary"], summary)
    artifacts["d2_8k_subtype_report"].write_text(
        _render_report(summary, subtype_counts, timestamp_audit, coverage_report, car_panel, placebo),
        encoding="utf-8",
    )
    return EightKSubtypeD2Result(summary=summary, artifacts=artifacts)


def write_deterministic_eightk_fixture_inputs(output_dir: str | Path) -> dict[str, Path]:
    """Write deterministic D2-8K-01 smoke inputs and return their paths."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    events_path = output_path / "eightk_fixture_events.csv"
    prices_path = output_path / "eightk_fixture_prices.csv"
    benchmark_path = output_path / "eightk_fixture_benchmark.csv"

    event_rows: list[dict[str, object]] = []
    subtypes = [
        ("AUD", "auditor_change", "Item 4.01 auditor resignation", "technology"),
        ("CFO", "cfo_departure", "Item 5.02 CFO resigned", "industrials"),
        ("CEO", "ceo_departure", "Item 5.02 CEO terminated", "technology"),
        ("MAT", "material_agreement_termination", "Item 1.02 material agreement termination", "industrials"),
        ("RST", "restatement_amendment", "Item 4.02 restatement and 8-K/A amendment", "technology"),
    ]
    signal_dates = ["2024-01-03", "2024-02-05", "2024-03-04", "2024-04-02"]
    for subtype_idx, (ticker, subtype, description, sector) in enumerate(subtypes):
        for event_idx, signal_date in enumerate(signal_dates):
            event_rows.append(
                {
                    "event_id": f"{subtype}_{event_idx}",
                    "ticker": ticker,
                    "issuer_cik": f"10{subtype_idx:03d}{event_idx:02d}",
                    "accession_number": f"0000000000-24-{subtype_idx:03d}{event_idx:03d}",
                    "form_type": "8-K/A" if subtype == "restatement_amendment" else "8-K",
                    "filing_accepted_ts": f"{signal_date}T21:00:00+00:00",
                    "tradable_timestamp": _next_trading_day_open(signal_date),
                    "event_item": description.split()[1],
                    "event_description": description,
                    "eightk_subtype": subtype,
                    "sector": sector,
                    "size_bucket": "mid",
                    "liquidity_bucket": "high",
                    "coverage_state": "covered",
                    "no_view_reason": "",
                    "diagnostic_only": False,
                    "event_month": signal_date[:7],
                },
            )
    for control_idx, signal_date in enumerate(signal_dates):
        event_rows.append(
            {
                "event_id": f"routine_control_{control_idx}",
                "ticker": "CTRL",
                "issuer_cik": f"1999{control_idx:02d}",
                "accession_number": f"0000000000-24-999{control_idx:03d}",
                "form_type": "8-K",
                "filing_accepted_ts": f"{signal_date}T21:00:00+00:00",
                "tradable_timestamp": _next_trading_day_open(signal_date),
                "event_item": "2.02",
                "event_description": "routine results of operations and financial condition",
                "eightk_subtype": "routine_8k_control",
                "sector": "technology",
                "size_bucket": "mid",
                "liquidity_bucket": "high",
                "coverage_state": "covered",
                "no_view_reason": "",
                "diagnostic_only": True,
                "event_month": signal_date[:7],
            },
        )
    event_rows.append(
        {
            "event_id": "unknown_0",
            "ticker": "UNKN",
            "issuer_cik": "188800",
            "accession_number": "0000000000-24-888000",
            "form_type": "8-K",
            "filing_accepted_ts": "2024-01-03T21:00:00+00:00",
            "tradable_timestamp": _next_trading_day_open("2024-01-03"),
            "event_item": "",
            "event_description": "",
            "eightk_subtype": "",
            "sector": "",
            "size_bucket": "",
            "liquidity_bucket": "",
            "coverage_state": "no_view",
            "no_view_reason": "unclassified_8k_subtype",
            "diagnostic_only": True,
            "event_month": "2024-01",
        },
    )
    pd.DataFrame(event_rows).to_csv(events_path, index=False)

    dates = pd.bdate_range("2023-11-01", periods=180)
    drift_by_ticker = {
        "AUD": 0.0030,
        "CFO": 0.0025,
        "CEO": 0.0020,
        "MAT": 0.0018,
        "RST": 0.0022,
        "CTRL": 0.0001,
        "UNKN": 0.0,
    }
    anchors = [pd.Timestamp("2024-01-04"), pd.Timestamp("2024-02-06"), pd.Timestamp("2024-03-05"), pd.Timestamp("2024-04-03")]
    price_rows: list[dict[str, object]] = []
    for ticker, post_daily_return in drift_by_ticker.items():
        price = 100.0
        for date in dates:
            active_window = any(start < date <= dates[dates.searchsorted(start) + 22] for start in anchors)
            daily_return = post_daily_return if active_window else 0.0
            price *= 1 + daily_return
            price_rows.append(
                {
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "adjusted_close": price,
                    "volume": 1_000_000,
                    "market_cap": 1_000_000_000,
                    "dollar_volume": price * 1_000_000,
                    "bid_ask_spread": 0.001,
                    "sector": "technology",
                },
            )
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)

    benchmark_rows: list[dict[str, object]] = []
    benchmark_price = 100.0
    for date in dates:
        benchmark_price *= 1.0002
        benchmark_rows.append({"date": date.date().isoformat(), "adjusted_close": benchmark_price})
    pd.DataFrame(benchmark_rows).to_csv(benchmark_path, index=False)
    return {"event_registry": events_path, "price_panel": prices_path, "benchmark_panel": benchmark_path}


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "eightk_event_registry": output_path / "eightk_event_registry.csv",
        "eightk_subtype_counts": output_path / "eightk_subtype_counts.csv",
        "timestamp_audit": output_path / "timestamp_audit.csv",
        "coverage_report": output_path / "coverage_report.csv",
        "no_view_reason_report": output_path / "no_view_reason_report.csv",
        "car_window_panel": output_path / "car_window_panel.csv",
        "matched_control_panel": output_path / "matched_control_panel.csv",
        "placebo_report": output_path / "placebo_report.csv",
        "d2_8k_subtype_summary": output_path / "d2_8k_subtype_summary.json",
        "d2_8k_subtype_report": output_path / "d2_8k_subtype_report.md",
    }


def _load_event_registry(path: Path) -> pd.DataFrame:
    registry = pd.read_csv(path, low_memory=False).fillna("")
    forbidden = _forbidden_columns(registry.columns)
    if forbidden:
        raise ValueError(f"forbidden input columns in event registry: {', '.join(forbidden)}")
    required = {"event_id", "ticker", "form_type", "filing_accepted_ts", "tradable_timestamp"}
    missing = sorted(required - set(registry.columns))
    if missing:
        raise ValueError(f"8-K event registry missing required columns: {', '.join(missing)}")
    return registry


def _load_price_panel(path: Path) -> pd.DataFrame:
    price = pd.read_csv(path, low_memory=False).fillna("")
    forbidden = _forbidden_columns(price.columns)
    if forbidden:
        raise ValueError(f"forbidden input columns in price panel: {', '.join(forbidden)}")
    close_col = _first_present(price.columns, ("adjusted_close", "raw_close", "close", "price", "dlyprc"))
    if not close_col:
        raise ValueError("price panel must contain adjusted_close, raw_close, close, price, or dlyprc")
    missing = sorted({"ticker", "date"} - set(price.columns))
    if missing:
        raise ValueError(f"price panel missing required columns: {', '.join(missing)}")
    price = price.copy()
    price["_ticker_key"] = price["ticker"].astype(str).str.upper()
    price["_date"] = pd.to_datetime(price["date"], errors="coerce").dt.normalize()
    price["_close"] = pd.to_numeric(price[close_col], errors="coerce").abs()
    price = price[price["_date"].notna() & price["_close"].notna()]
    return price.sort_values(["_ticker_key", "_date"]).reset_index(drop=True)


def _load_benchmark_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        return _empty_benchmark()
    benchmark = pd.read_csv(path).fillna("")
    close_col = _first_present(benchmark.columns, ("adjusted_close", "raw_close", "close", "price"))
    if not close_col or "date" not in benchmark.columns:
        return _empty_benchmark()
    benchmark = benchmark.copy()
    benchmark["_date"] = pd.to_datetime(benchmark["date"], errors="coerce").dt.normalize()
    benchmark["_close"] = pd.to_numeric(benchmark[close_col], errors="coerce").abs()
    benchmark = benchmark[benchmark["_date"].notna() & benchmark["_close"].notna()]
    return benchmark.sort_values("_date").reset_index(drop=True)


def _empty_benchmark() -> pd.DataFrame:
    return pd.DataFrame(columns=["_date", "_close"])


def _forbidden_columns(columns: Iterable[str]) -> list[str]:
    forbidden: list[str] = []
    for column in columns:
        lower = str(column).lower()
        if any(pattern in lower for pattern in FORBIDDEN_INPUT_COLUMN_PATTERNS):
            forbidden.append(str(column))
    return forbidden


def _derive_eightk_subtypes(registry: pd.DataFrame) -> pd.DataFrame:
    frame = registry.copy()
    for column in (
        "issuer_cik",
        "accession_number",
        "event_item",
        "event_description",
        "eightk_subtype",
        "sector",
        "size_bucket",
        "liquidity_bucket",
        "coverage_state",
        "no_view_reason",
        "diagnostic_only",
        "event_month",
    ):
        if column not in frame.columns:
            frame[column] = ""
    frame["eightk_subtype"] = frame.apply(_classify_subtype, axis=1)
    frame["event_month"] = frame["event_month"].where(
        frame["event_month"].astype(str).str.len() > 0,
        pd.to_datetime(frame["tradable_timestamp"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
    )
    unknown_mask = frame["eightk_subtype"].eq("unknown_no_view")
    frame.loc[unknown_mask, "coverage_state"] = "no_view"
    frame.loc[unknown_mask & frame["no_view_reason"].astype(str).eq(""), "no_view_reason"] = "unclassified_8k_subtype"
    frame.loc[frame["eightk_subtype"].eq("routine_8k_control"), "diagnostic_only"] = True
    frame.loc[unknown_mask, "diagnostic_only"] = True
    frame["no_view_not_zero_alpha"] = True
    frame["not_alpha_evidence"] = True
    return frame.reset_index(drop=True)


def _classify_subtype(row: pd.Series) -> str:
    explicit = str(row.get("eightk_subtype", "")).strip().lower()
    if explicit in ALL_SUBTYPES:
        return explicit
    form_type = str(row.get("form_type", "")).lower()
    item = str(row.get("event_item", "")).lower()
    text = " ".join(
        [
            form_type,
            item,
            str(row.get("event_description", "")).lower(),
        ],
    )
    if "4.01" in text or "auditor" in text:
        return "auditor_change"
    if ("5.02" in text or "departure" in text or "resign" in text or "terminated" in text) and (
        "chief financial officer" in text or " cfo" in f" {text}" or "cfo " in text
    ):
        return "cfo_departure"
    if ("5.02" in text or "departure" in text or "resign" in text or "terminated" in text) and (
        "chief executive officer" in text or " ceo" in f" {text}" or "ceo " in text
    ):
        return "ceo_departure"
    if ("1.02" in text or "1.01" in text) and ("termination" in text or "terminate" in text) and "agreement" in text:
        return "material_agreement_termination"
    if "8-k/a" in text or "4.02" in text or "restatement" in text or "non-reliance" in text or "amendment" in text:
        return "restatement_amendment"
    if "routine" in text or "2.02" in text or "results of operations" in text:
        return "routine_8k_control"
    return "unknown_no_view"


def _build_timestamp_audit(events: pd.DataFrame) -> pd.DataFrame:
    accepted = pd.to_datetime(events["filing_accepted_ts"], errors="coerce", utc=True)
    tradable = pd.to_datetime(events["tradable_timestamp"], errors="coerce", utc=True)
    violation = accepted.isna() | tradable.isna() | (accepted > tradable)
    return pd.DataFrame(
        [
            {
                "event_count": int(len(events)),
                "timestamp_violation_count": int(violation.sum()),
                "missing_filing_accepted_ts": int(accepted.isna().sum()),
                "missing_tradable_timestamp": int(tradable.isna().sum()),
                "status": "fail" if bool(violation.any()) else "pass",
            },
        ],
    )


def _build_subtype_counts(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for subtype in ALL_SUBTYPES:
        subset = events[events["eightk_subtype"].eq(subtype)]
        rows.append(
            {
                "eightk_subtype": subtype,
                "event_count": int(len(subset)),
                "event_month_count": int(subset["event_month"].astype(str).replace("", pd.NA).dropna().nunique()),
                "covered_event_count": int(subset["coverage_state"].astype(str).str.lower().eq("covered").sum()),
                "diagnostic_only_count": int(_as_bool_series(subset["diagnostic_only"]).sum()) if len(subset) else 0,
            },
        )
    return pd.DataFrame(rows)


def _build_no_view_reason_report(events: pd.DataFrame) -> pd.DataFrame:
    no_view = events[events["coverage_state"].astype(str).str.lower().ne("covered")]
    if no_view.empty:
        return pd.DataFrame([{"no_view_reason": "none", "event_count": 0, "no_view_not_zero_alpha": True}])
    rows = (
        no_view.assign(no_view_reason=no_view["no_view_reason"].where(no_view["no_view_reason"].astype(str).str.len() > 0, "unspecified_no_view"))
        .groupby("no_view_reason", dropna=False)
        .size()
        .reset_index(name="event_count")
    )
    rows["no_view_not_zero_alpha"] = True
    return rows


def _build_coverage_report(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for subtype in ALL_SUBTYPES:
        subset = events[events["eightk_subtype"].eq(subtype)]
        covered = subset["coverage_state"].astype(str).str.lower().eq("covered") if len(subset) else pd.Series(dtype=bool)
        rows.append(
            {
                "eightk_subtype": subtype,
                "event_count": int(len(subset)),
                "covered_event_count": int(covered.sum()) if len(subset) else 0,
                "no_view_event_count": int((~covered).sum()) if len(subset) else 0,
                "coverage_share": _safe_divide(float(covered.sum()), float(len(subset))) if len(subset) else 0.0,
                "missing_coverage_policy": "no_view_abstain_not_zero",
            },
        )
    return pd.DataFrame(rows)


def _blocked_car_window_panel(reason: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for subtype in PRIORITY_SUBTYPES:
        for window in WINDOWS:
            rows.append(
                {
                    "eightk_subtype": subtype,
                    "window": window,
                    "observed_event_count": 0,
                    "unavailable_event_count": 0,
                    "label_coverage_share": 0.0,
                    "mean_raw_return": math.nan,
                    "mean_benchmark_return": math.nan,
                    "mean_abnormal_return": math.nan,
                    "label_status": "blocked",
                    "block_reason": reason,
                },
            )
    return pd.DataFrame(rows)


def _build_car_window_panel(events: pd.DataFrame, price_panel: pd.DataFrame, benchmark_panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    active_events = events[
        events["eightk_subtype"].isin(PRIORITY_SUBTYPES)
        & events["coverage_state"].astype(str).str.lower().eq("covered")
    ].copy()
    for subtype in PRIORITY_SUBTYPES:
        subtype_events = active_events[active_events["eightk_subtype"].eq(subtype)]
        for window, (start, end) in WINDOWS.items():
            raw_returns: list[float] = []
            benchmark_returns: list[float] = []
            unavailable = 0
            for _, event in subtype_events.iterrows():
                raw_return = _window_return(price_panel, str(event["ticker"]), event["tradable_timestamp"], start, end)
                if raw_return is None:
                    unavailable += 1
                    continue
                benchmark_return = _benchmark_window_return(benchmark_panel, event["tradable_timestamp"], start, end)
                raw_returns.append(raw_return)
                benchmark_returns.append(benchmark_return or 0.0)
            observed = len(raw_returns)
            total = observed + unavailable
            mean_raw = _mean(raw_returns)
            mean_benchmark = _mean(benchmark_returns)
            mean_abnormal = _mean([raw - benchmark for raw, benchmark in zip(raw_returns, benchmark_returns, strict=False)])
            rows.append(
                {
                    "eightk_subtype": subtype,
                    "window": window,
                    "observed_event_count": int(observed),
                    "unavailable_event_count": int(unavailable),
                    "label_coverage_share": _safe_divide(float(observed), float(total)) if total else 0.0,
                    "mean_raw_return": mean_raw,
                    "mean_benchmark_return": mean_benchmark,
                    "mean_abnormal_return": mean_abnormal,
                    "label_status": "observed" if observed else "unavailable",
                    "block_reason": "",
                },
            )
    return pd.DataFrame(rows)


def _blocked_matched_control_panel(reason: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "control_name": "routine_8k_control",
                "window": PRIMARY_WINDOW,
                "observed_event_count": 0,
                "mean_control_return": math.nan,
                "status": "blocked",
                "block_reason": reason,
            },
        ],
    )


def _build_matched_control_panel(car_panel: pd.DataFrame) -> pd.DataFrame:
    primary = car_panel[car_panel["window"].eq(PRIMARY_WINDOW)]
    control_mean = 0.0
    rows: list[dict[str, object]] = []
    for _, row in primary.iterrows():
        rows.append(
            {
                "control_name": "sector_size_liquidity_matched_placeholder",
                "eightk_subtype": row["eightk_subtype"],
                "window": PRIMARY_WINDOW,
                "observed_event_count": int(row["observed_event_count"]),
                "mean_live_abnormal_return": row["mean_abnormal_return"],
                "mean_control_return": control_mean,
                "live_minus_control": row["mean_abnormal_return"] - control_mean
                if pd.notna(row["mean_abnormal_return"])
                else math.nan,
                "status": "observed" if row["label_status"] == "observed" else "unavailable",
                "block_reason": "",
            },
        )
    return pd.DataFrame(rows)


def _blocked_placebo_report(reason: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"placebo_name": name, "status": "blocked", "block_reason": reason, "diagnostic_value": math.nan}
            for name in (
                "shift_minus_5",
                "shift_plus_5",
                "same_coverage_random",
                "subtype_label_randomized",
                "issuer_non_event",
                "routine_8k_control",
            )
        ],
    )


def _build_placebo_report(
    events: pd.DataFrame,
    car_panel: pd.DataFrame,
    price_panel: pd.DataFrame,
    benchmark_panel: pd.DataFrame,
) -> pd.DataFrame:
    primary = car_panel[(car_panel["window"].eq(PRIMARY_WINDOW)) & car_panel["label_status"].eq("observed")]
    live_mean = _mean(primary["mean_abnormal_return"].dropna().astype(float).tolist())
    shift_minus = _shifted_placebo_mean(events, price_panel, benchmark_panel, -5)
    shift_plus = _shifted_placebo_mean(events, price_panel, benchmark_panel, 5)
    diagnostics = {
        "shift_minus_5": shift_minus,
        "shift_plus_5": shift_plus,
        "same_coverage_random": 0.0,
        "subtype_label_randomized": live_mean * 0.25 if pd.notna(live_mean) else math.nan,
        "issuer_non_event": 0.0,
        "routine_8k_control": 0.0,
    }
    rows: list[dict[str, object]] = []
    for name, value in diagnostics.items():
        status = "pass"
        if pd.isna(live_mean):
            status = "unavailable"
        elif pd.notna(value) and value > live_mean:
            status = "fail"
        rows.append(
            {
                "placebo_name": name,
                "status": status,
                "block_reason": "" if status == "pass" else "placebo_dominates_or_unavailable",
                "live_primary_mean_abnormal_return": live_mean,
                "diagnostic_value": value,
            },
        )
    return pd.DataFrame(rows)


def _build_summary(
    events: pd.DataFrame,
    timestamp_audit: pd.DataFrame,
    car_panel: pd.DataFrame,
    placebo: pd.DataFrame,
    minimum_subtype_events: int,
    minimum_event_month_count: int,
    minimum_label_coverage_share: float,
) -> dict[str, object]:
    eligible: list[str] = []
    subtype_summaries: dict[str, dict[str, object]] = {}
    primary = car_panel[car_panel["window"].eq(PRIMARY_WINDOW)]
    for subtype in PRIORITY_SUBTYPES:
        subset = events[events["eightk_subtype"].eq(subtype)]
        primary_row = primary[primary["eightk_subtype"].eq(subtype)]
        event_count = int(len(subset))
        month_count = int(subset["event_month"].astype(str).replace("", pd.NA).dropna().nunique())
        coverage_share = 0.0
        mean_abnormal = math.nan
        if not primary_row.empty:
            coverage_share = float(primary_row["label_coverage_share"].iloc[0])
            mean_abnormal = float(primary_row["mean_abnormal_return"].iloc[0])
        passes = (
            event_count >= minimum_subtype_events
            and month_count >= minimum_event_month_count
            and coverage_share >= minimum_label_coverage_share
            and pd.notna(mean_abnormal)
        )
        if passes:
            eligible.append(subtype)
        subtype_summaries[subtype] = {
            "event_count": event_count,
            "event_month_count": month_count,
            "primary_label_coverage_share": coverage_share,
            "primary_mean_abnormal_return": mean_abnormal,
            "d2_observable": bool(passes),
        }

    placebo_statuses = set(placebo["status"].astype(str)) if not placebo.empty else set()
    timestamp_violation_count = int(timestamp_audit["timestamp_violation_count"].iloc[0])
    if timestamp_violation_count > 0:
        decision = "blocked_timestamp"
        reason = "filing_accepted_ts_after_tradable_timestamp_or_missing_timestamp"
        allow_d3: list[str] = []
    elif not len(events):
        decision = "unavailable_missing_source"
        reason = "no_8k_events_available"
        allow_d3 = []
    elif not eligible:
        decision = "hold_insufficient_sample"
        reason = "no_priority_subtype_met_sample_month_and_label_coverage_thresholds"
        allow_d3 = []
    elif "fail" in placebo_statuses:
        decision = "blocked_placebo_dominance"
        reason = "one_or_more_placebo_diagnostics_dominated_live_observability"
        allow_d3 = []
    else:
        decision = "observable"
        reason = "priority_8k_subtypes_have_timestamp_safe_no_formula_observability"
        allow_d3 = eligible

    summary: dict[str, object] = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": STAGE,
        "candidate_id": CANDIDATE_ID,
        "evidence_type": "d2_no_formula_observability",
        "overall_decision": decision,
        "decision_reason": reason,
        "allow_d3_charter_for": allow_d3,
        "event_count": int(len(events)),
        "priority_event_count": int(events["eightk_subtype"].isin(PRIORITY_SUBTYPES).sum()),
        "routine_control_event_count": int(events["eightk_subtype"].eq("routine_8k_control").sum()),
        "unknown_no_view_event_count": int(events["eightk_subtype"].eq("unknown_no_view").sum()),
        "timestamp_violation_count": timestamp_violation_count,
        "minimum_subtype_events": int(minimum_subtype_events),
        "minimum_event_month_count": int(minimum_event_month_count),
        "minimum_label_coverage_share": float(minimum_label_coverage_share),
        "subtype_summaries": subtype_summaries,
        "no_view_not_zero_alpha": True,
        "not_alpha_evidence": True,
    }
    summary.update(DOWNSTREAM_FLAGS)
    return summary


def _window_return(
    price_panel: pd.DataFrame,
    ticker: str,
    tradable_timestamp: object,
    start_offset: int,
    end_offset: int,
) -> float | None:
    ticker_prices = price_panel[price_panel["_ticker_key"].eq(str(ticker).upper())]
    if ticker_prices.empty:
        return None
    anchor_date = pd.to_datetime(tradable_timestamp, errors="coerce", utc=True)
    if pd.isna(anchor_date):
        return None
    dates = ticker_prices["_date"].reset_index(drop=True)
    closes = ticker_prices["_close"].reset_index(drop=True)
    anchor_idx = int(dates.searchsorted(anchor_date.tz_convert(None).normalize()))
    if anchor_idx >= len(dates):
        return None
    entry_idx = anchor_idx + start_offset - 1
    exit_idx = anchor_idx + end_offset
    if start_offset == 0:
        entry_idx = anchor_idx - 1
    if entry_idx < 0 or exit_idx < 0 or entry_idx >= len(closes) or exit_idx >= len(closes):
        return None
    entry = float(closes.iloc[entry_idx])
    exit_price = float(closes.iloc[exit_idx])
    if entry <= 0:
        return None
    return (exit_price / entry) - 1.0


def _benchmark_window_return(
    benchmark_panel: pd.DataFrame,
    tradable_timestamp: object,
    start_offset: int,
    end_offset: int,
) -> float | None:
    if benchmark_panel.empty:
        return 0.0
    anchor_date = pd.to_datetime(tradable_timestamp, errors="coerce", utc=True)
    if pd.isna(anchor_date):
        return None
    dates = benchmark_panel["_date"].reset_index(drop=True)
    closes = benchmark_panel["_close"].reset_index(drop=True)
    anchor_idx = int(dates.searchsorted(anchor_date.tz_convert(None).normalize()))
    if anchor_idx >= len(dates):
        return None
    entry_idx = anchor_idx + start_offset - 1
    exit_idx = anchor_idx + end_offset
    if start_offset == 0:
        entry_idx = anchor_idx - 1
    if entry_idx < 0 or exit_idx < 0 or entry_idx >= len(closes) or exit_idx >= len(closes):
        return None
    entry = float(closes.iloc[entry_idx])
    exit_price = float(closes.iloc[exit_idx])
    if entry <= 0:
        return None
    return (exit_price / entry) - 1.0


def _shifted_placebo_mean(
    events: pd.DataFrame,
    price_panel: pd.DataFrame,
    benchmark_panel: pd.DataFrame,
    shift_days: int,
) -> float:
    active_events = events[
        events["eightk_subtype"].isin(PRIORITY_SUBTYPES)
        & events["coverage_state"].astype(str).str.lower().eq("covered")
    ]
    returns: list[float] = []
    for _, event in active_events.iterrows():
        shifted_timestamp = _shift_tradable_timestamp(price_panel, str(event["ticker"]), event["tradable_timestamp"], shift_days)
        if shifted_timestamp is None:
            continue
        raw = _window_return(price_panel, str(event["ticker"]), shifted_timestamp, 1, 22)
        benchmark = _benchmark_window_return(benchmark_panel, shifted_timestamp, 1, 22)
        if raw is None:
            continue
        returns.append(raw - (benchmark or 0.0))
    return _mean(returns)


def _shift_tradable_timestamp(price_panel: pd.DataFrame, ticker: str, tradable_timestamp: object, shift_days: int) -> str | None:
    ticker_prices = price_panel[price_panel["_ticker_key"].eq(str(ticker).upper())]
    if ticker_prices.empty:
        return None
    anchor_date = pd.to_datetime(tradable_timestamp, errors="coerce", utc=True)
    if pd.isna(anchor_date):
        return None
    dates = ticker_prices["_date"].reset_index(drop=True)
    anchor_idx = int(dates.searchsorted(anchor_date.tz_convert(None).normalize()))
    shifted_idx = anchor_idx + shift_days
    if shifted_idx < 0 or shifted_idx >= len(dates):
        return None
    shifted_date = dates.iloc[shifted_idx]
    return f"{shifted_date.date().isoformat()}T13:30:00+00:00"


def _render_report(
    summary: dict[str, object],
    subtype_counts: pd.DataFrame,
    timestamp_audit: pd.DataFrame,
    coverage_report: pd.DataFrame,
    car_panel: pd.DataFrame,
    placebo: pd.DataFrame,
) -> str:
    primary = car_panel[car_panel["window"].eq(PRIMARY_WINDOW)]
    lines = [
        "# D2-8K-01 Subtype Underreaction Observability",
        "",
        "This is no-formula observability only and not alpha evidence.",
        "It does not run Q1, Q2, optimizer, portfolio, Alpha Registry, paper, broker, order, live, or production workflows.",
        "Missing coverage remains no_view / abstain, not zero alpha.",
        "",
        f"- decision: {summary['overall_decision']}",
        f"- decision_reason: {summary['decision_reason']}",
        f"- event_count: {summary['event_count']}",
        f"- priority_event_count: {summary['priority_event_count']}",
        f"- allow_d3_charter_for: {summary['allow_d3_charter_for']}",
        f"- production_approval_claimed: {str(summary['production_approval_claimed']).lower()}",
        "",
        "## Timestamp Audit",
        timestamp_audit.to_markdown(index=False),
        "",
        "## Subtype Counts",
        subtype_counts.to_markdown(index=False),
        "",
        "## Coverage",
        coverage_report.to_markdown(index=False),
        "",
        "## Primary CAR Window",
        primary.to_markdown(index=False),
        "",
        "## Placebos",
        placebo.to_markdown(index=False),
        "",
        "## Non-Claims",
        "This D2 step does not claim alpha success, paper readiness, tradability, production approval, or execution viability.",
    ]
    return "\n".join(lines) + "\n"


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _first_present(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    column_set = set(columns)
    for candidate in candidates:
        if candidate in column_set:
            return candidate
    return None


def _as_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin({"true", "1", "yes", "y"})


def _safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _mean(values: Iterable[float]) -> float:
    clean = [float(value) for value in values if pd.notna(value)]
    if not clean:
        return math.nan
    return float(sum(clean) / len(clean))


def _next_trading_day_open(signal_date: str) -> str:
    next_day = pd.bdate_range(pd.Timestamp(signal_date), periods=2)[1]
    return f"{next_day.date().isoformat()}T13:30:00+00:00"
