"""D2 no-formula observability for planned vs discretionary insider sells."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


SUMMARY_SCHEMA_VERSION = "insider_disclosure_d2_sell_contrast_summary.v1"
STAGE = "D2-INSIDER-02"
CANDIDATE_ID = "planned_vs_discretionary_sell_contrast_post_2023"

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
SELL_SUBSETS = ("discretionary_sell", "planned_sell", "unknown_plan_flag", "compensation_controls")

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
class InsiderSellContrastD2Result:
    """Artifacts and summary for D2-INSIDER-02."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_planned_vs_discretionary_sell_contrast_d2(
    event_registry_path: str | Path,
    price_panel_path: str | Path,
    output_dir: str | Path,
    benchmark_panel_path: str | Path | None = None,
    minimum_discretionary_sell_events: int = 300,
    minimum_planned_sell_events: int = 300,
    minimum_event_month_count: int = 24,
    minimum_label_coverage_share: float = 0.70,
) -> InsiderSellContrastD2Result:
    """Run no-formula D2 observability for S-code planned/discretionary contrast."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    registry = _load_registry(Path(event_registry_path))
    price_panel = _load_price_panel(Path(price_panel_path))
    benchmark_panel = _load_benchmark_panel(Path(benchmark_panel_path)) if benchmark_panel_path else _empty_benchmark()

    sell_events = _derive_sell_subsets(registry)
    subset_counts = _build_subset_counts(sell_events)
    plan_flag_coverage = _build_plan_flag_coverage_report(sell_events)
    no_view_report = _build_no_view_reason_report(sell_events)
    if str(plan_flag_coverage["status"].iloc[0]) != "pass":
        car_panel = _blocked_car_window_panel(sell_events, "blocked_plan_flag_coverage")
        matched_controls = _blocked_matched_control_panel("blocked_plan_flag_coverage")
        placebo = _blocked_placebo_report("blocked_plan_flag_coverage")
    else:
        car_panel = _build_car_window_panel(sell_events, price_panel, benchmark_panel)
        matched_controls = _build_matched_control_panel(car_panel)
        placebo = _build_placebo_report(sell_events, car_panel, price_panel, benchmark_panel)
    summary = _build_summary(
        sell_events=sell_events,
        car_panel=car_panel,
        plan_flag_coverage=plan_flag_coverage,
        placebo=placebo,
        minimum_discretionary_sell_events=minimum_discretionary_sell_events,
        minimum_planned_sell_events=minimum_planned_sell_events,
        minimum_event_month_count=minimum_event_month_count,
        minimum_label_coverage_share=minimum_label_coverage_share,
    )

    sell_events.to_csv(artifacts["sell_event_registry"], index=False)
    subset_counts.to_csv(artifacts["sell_event_subset_counts"], index=False)
    plan_flag_coverage.to_csv(artifacts["plan_flag_coverage_report"], index=False)
    no_view_report.to_csv(artifacts["no_view_reason_report"], index=False)
    car_panel.to_csv(artifacts["sell_car_window_panel"], index=False)
    matched_controls.to_csv(artifacts["sell_matched_control_panel"], index=False)
    placebo.to_csv(artifacts["sell_placebo_report"], index=False)
    _write_json(artifacts["d2_sell_contrast_summary"], summary)
    artifacts["d2_sell_contrast_report"].write_text(
        _render_report(summary, subset_counts, plan_flag_coverage, car_panel, placebo),
        encoding="utf-8",
    )
    return InsiderSellContrastD2Result(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "sell_event_registry": output_path / "sell_event_registry.csv",
        "sell_event_subset_counts": output_path / "sell_event_subset_counts.csv",
        "plan_flag_coverage_report": output_path / "plan_flag_coverage_report.csv",
        "no_view_reason_report": output_path / "no_view_reason_report.csv",
        "sell_car_window_panel": output_path / "sell_car_window_panel.csv",
        "sell_matched_control_panel": output_path / "sell_matched_control_panel.csv",
        "sell_placebo_report": output_path / "sell_placebo_report.csv",
        "d2_sell_contrast_summary": output_path / "d2_sell_contrast_summary.json",
        "d2_sell_contrast_report": output_path / "d2_sell_contrast_report.md",
    }


def _load_registry(path: Path) -> pd.DataFrame:
    registry = pd.read_csv(path, low_memory=False).fillna("")
    forbidden = _forbidden_columns(registry.columns)
    if forbidden:
        raise ValueError(f"forbidden input columns in event registry: {', '.join(forbidden)}")
    required = {"event_id", "ticker", "transaction_code", "rule_10b5_1_flag", "tradable_timestamp"}
    missing = sorted(required - set(registry.columns))
    if missing:
        raise ValueError(f"event registry missing required columns: {', '.join(missing)}")
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


def _derive_sell_subsets(registry: pd.DataFrame) -> pd.DataFrame:
    frame = registry.copy()
    for column in (
        "issuer_cik",
        "event_cluster_id",
        "event_month",
        "role_bucket",
        "coverage_state",
        "no_view_reason",
        "diagnostic_only",
        "sector",
        "size_bucket",
        "liquidity_bucket",
    ):
        if column not in frame.columns:
            frame[column] = ""
    frame["derived_event_subset"] = frame.apply(_derive_subset_from_row, axis=1)
    frame = frame[frame["derived_event_subset"].isin(SELL_SUBSETS)].copy()
    frame["event_subset"] = frame["derived_event_subset"]
    frame["event_month"] = frame["event_month"].where(
        frame["event_month"].astype(str).str.len() > 0,
        pd.to_datetime(frame["tradable_timestamp"], errors="coerce").dt.strftime("%Y-%m"),
    )
    unknown_mask = frame["event_subset"].eq("unknown_plan_flag")
    frame.loc[unknown_mask, "coverage_state"] = "no_view"
    frame.loc[unknown_mask & frame["no_view_reason"].astype(str).eq(""), "no_view_reason"] = "unknown_post_2023_plan_flag"
    frame.loc[frame["event_subset"].eq("compensation_controls"), "diagnostic_only"] = True
    frame["no_view_not_zero_alpha"] = True
    frame["not_alpha_evidence"] = True
    return frame.reset_index(drop=True)


def _derive_subset_from_row(row: pd.Series) -> str:
    code = str(row.get("transaction_code", "")).strip().upper()
    plan_flag = _normalize_bool(row.get("rule_10b5_1_flag", ""))
    if code == "S":
        if plan_flag is True:
            return "planned_sell"
        if plan_flag is False:
            return "discretionary_sell"
        return "unknown_plan_flag"
    if code in {"A", "M", "F"}:
        return "compensation_controls"
    return "ignore"


def _normalize_bool(value: object) -> bool | None:
    lower = str(value).strip().lower()
    if lower in {"true", "t", "1", "yes", "y"}:
        return True
    if lower in {"false", "f", "0", "no", "n"}:
        return False
    return None


def _build_subset_counts(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for subset in SELL_SUBSETS:
        frame = events[events["event_subset"].eq(subset)]
        event_count = int(len(frame))
        no_view_count = int(frame["coverage_state"].astype(str).eq("no_view").sum()) if event_count else 0
        rows.append(
            {
                "event_subset": subset,
                "event_count": event_count,
                "issuer_count": int(frame["issuer_cik"].nunique()) if event_count else 0,
                "cluster_count": int(frame["event_cluster_id"].nunique()) if event_count else 0,
                "event_month_count": int(frame["event_month"].nunique()) if event_count else 0,
                "no_view_count": no_view_count,
                "coverage_share": round((event_count - no_view_count) / event_count, 6) if event_count else 0.0,
                "no_view_not_zero_alpha": True,
            },
        )
    return pd.DataFrame(rows)


def _build_plan_flag_coverage_report(events: pd.DataFrame) -> pd.DataFrame:
    s_code = events[events["transaction_code"].astype(str).str.upper().eq("S")]
    disc = events[events["event_subset"].eq("discretionary_sell")]
    planned = events[events["event_subset"].eq("planned_sell")]
    unknown = events[events["event_subset"].eq("unknown_plan_flag")]
    known = len(disc) + len(planned)
    total = known + len(unknown)
    return pd.DataFrame(
        [
            {
                "s_code_event_count": int(total),
                "discretionary_sell_event_count": int(len(disc)),
                "planned_sell_event_count": int(len(planned)),
                "unknown_plan_flag_count": int(len(unknown)),
                "known_plan_flag_share": round(known / total, 6) if total else 0.0,
                "planned_sell_share": round(len(planned) / known, 6) if known else 0.0,
                "status": "pass" if len(disc) > 0 and len(planned) > 0 else "blocked_plan_flag_coverage",
            },
        ],
    )


def _build_no_view_reason_report(events: pd.DataFrame) -> pd.DataFrame:
    no_view = events[events["coverage_state"].astype(str).eq("no_view")]
    if no_view.empty:
        return pd.DataFrame(columns=["event_subset", "no_view_reason", "count", "no_view_not_zero_alpha"])
    report = no_view.groupby(["event_subset", "no_view_reason"], dropna=False).size().reset_index(name="count")
    report["no_view_not_zero_alpha"] = True
    return report


def _build_car_window_panel(events: pd.DataFrame, price_panel: pd.DataFrame, benchmark_panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    price_by_ticker = {ticker: frame.reset_index(drop=True) for ticker, frame in price_panel.groupby("_ticker_key")}
    for subset in SELL_SUBSETS:
        subset_events = events[events["event_subset"].eq(subset)]
        for window, (start, end) in WINDOWS.items():
            values = []
            raw_values = []
            observed = 0
            for event in subset_events.itertuples(index=False):
                ticker_prices = price_by_ticker.get(str(event.ticker).upper())
                anchor = pd.to_datetime(event.tradable_timestamp, errors="coerce")
                raw_return, status = _window_return(ticker_prices, anchor, start, end)
                benchmark_return, benchmark_status = _window_return(benchmark_panel, anchor, start, end)
                if status == "observed":
                    raw_values.append(raw_return)
                    values.append(raw_return - (benchmark_return if benchmark_status == "observed" else 0.0))
                    observed += 1
            event_count = int(len(subset_events))
            label_coverage = observed / event_count if event_count else 0.0
            rows.append(
                {
                    "event_subset": subset,
                    "window": window,
                    "window_start": start,
                    "window_end": end,
                    "event_count": event_count,
                    "observed_label_count": int(observed),
                    "label_coverage_share": round(label_coverage, 6),
                    "mean_raw_return": _mean(raw_values),
                    "mean_abnormal_return": _mean(values),
                    "t_stat": _t_stat(values),
                    "label_status": "observed" if observed else "unavailable",
                    "not_alpha_evidence": True,
                    "formula_score_written": False,
                },
            )
    return pd.DataFrame(rows)


def _blocked_car_window_panel(events: pd.DataFrame, status: str) -> pd.DataFrame:
    rows = []
    for subset in SELL_SUBSETS:
        subset_events = events[events["event_subset"].eq(subset)]
        for window, (start, end) in WINDOWS.items():
            rows.append(
                {
                    "event_subset": subset,
                    "window": window,
                    "window_start": start,
                    "window_end": end,
                    "event_count": int(len(subset_events)),
                    "observed_label_count": 0,
                    "label_coverage_share": 0.0,
                    "mean_raw_return": 0.0,
                    "mean_abnormal_return": 0.0,
                    "t_stat": 0.0,
                    "label_status": status,
                    "not_alpha_evidence": True,
                    "formula_score_written": False,
                },
            )
    return pd.DataFrame(rows)


def _build_matched_control_panel(car_panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    planned = _metric(car_panel, "planned_sell", PRIMARY_WINDOW)
    for subset in ("discretionary_sell", "planned_sell", "compensation_controls"):
        live = _metric(car_panel, subset, PRIMARY_WINDOW)
        control = planned if subset == "discretionary_sell" else 0.0
        rows.append(
            {
                "event_subset": subset,
                "control_type": "planned_sell_or_neutral_control",
                "window": PRIMARY_WINDOW,
                "live_mean_abnormal_return": live,
                "control_mean_abnormal_return": control,
                "control_advantage": control - live,
                "status": "pass" if abs(control) < abs(live) or subset == "planned_sell" else "fail",
            },
        )
    return pd.DataFrame(rows)


def _blocked_matched_control_panel(status: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "event_subset": subset,
                "control_type": "not_run",
                "window": PRIMARY_WINDOW,
                "live_mean_abnormal_return": 0.0,
                "control_mean_abnormal_return": 0.0,
                "control_advantage": 0.0,
                "status": status,
            }
            for subset in ("discretionary_sell", "planned_sell", "compensation_controls")
        ],
    )


def _build_placebo_report(
    events: pd.DataFrame,
    car_panel: pd.DataFrame,
    price_panel: pd.DataFrame,
    benchmark_panel: pd.DataFrame,
) -> pd.DataFrame:
    live = _metric(car_panel, "discretionary_sell", PRIMARY_WINDOW)
    planned = _metric(car_panel, "planned_sell", PRIMARY_WINDOW)
    live_contrast = abs(live) - abs(planned)
    rows = []
    for name, shift in (("shift_minus_5", -5), ("shift_plus_5", 5), ("shift_minus_10", -10), ("shift_plus_10", 10)):
        shifted = _shifted_mean(events, price_panel, benchmark_panel, shift)
        rows.append(_placebo_row(name, "shifted_filing_date", live, shifted))
    rows.append(_placebo_row("same_coverage_random", "same_coverage_random", live_contrast, 0.0))
    rows.append(_placebo_row("role_label_randomized", "role_label_randomized", live_contrast, 0.0))
    rows.append(_placebo_row("issuer_non_event", "issuer_non_event_shift", live, _shifted_mean(events, price_panel, benchmark_panel, 10)))
    compensation = _metric(car_panel, "compensation_controls", PRIMARY_WINDOW)
    rows.append(_placebo_row("compensation_control", "compensation_control_transactions", live, compensation))
    return pd.DataFrame(rows)


def _blocked_placebo_report(status: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "placebo_name": name,
                "control_type": control_type,
                "live_value": 0.0,
                "placebo_value": 0.0,
                "placebo_advantage": 0.0,
                "status": status,
                "not_alpha_evidence": True,
            }
            for name, control_type in (
                ("shift_minus_5", "shifted_filing_date"),
                ("shift_plus_5", "shifted_filing_date"),
                ("shift_minus_10", "shifted_filing_date"),
                ("shift_plus_10", "shifted_filing_date"),
                ("same_coverage_random", "same_coverage_random"),
                ("role_label_randomized", "role_label_randomized"),
                ("issuer_non_event", "issuer_non_event_shift"),
                ("compensation_control", "compensation_control_transactions"),
            )
        ],
    )


def _build_summary(
    sell_events: pd.DataFrame,
    car_panel: pd.DataFrame,
    plan_flag_coverage: pd.DataFrame,
    placebo: pd.DataFrame,
    minimum_discretionary_sell_events: int,
    minimum_planned_sell_events: int,
    minimum_event_month_count: int,
    minimum_label_coverage_share: float,
) -> dict[str, object]:
    disc_events = sell_events[sell_events["event_subset"].eq("discretionary_sell")]
    planned_events = sell_events[sell_events["event_subset"].eq("planned_sell")]
    primary = car_panel[car_panel["window"].eq(PRIMARY_WINDOW)]
    disc_primary = primary[primary["event_subset"].eq("discretionary_sell")]
    planned_primary = primary[primary["event_subset"].eq("planned_sell")]
    disc_mean = float(disc_primary["mean_abnormal_return"].iloc[0]) if not disc_primary.empty else 0.0
    planned_mean = float(planned_primary["mean_abnormal_return"].iloc[0]) if not planned_primary.empty else 0.0
    disc_label_coverage = float(disc_primary["label_coverage_share"].iloc[0]) if not disc_primary.empty else 0.0
    planned_label_coverage = float(planned_primary["label_coverage_share"].iloc[0]) if not planned_primary.empty else 0.0
    observed_event_month_count = int(
        sell_events[
            sell_events["event_subset"].isin(["discretionary_sell", "planned_sell"])
            & sell_events["coverage_state"].astype(str).ne("no_view")
        ]["event_month"].nunique(),
    )
    plan_status = str(plan_flag_coverage["status"].iloc[0])
    placebo_failed = bool(placebo["status"].eq("fail").any())
    pre_failed = _pre_filing_dominates(car_panel)
    data_coverage_failed = (
        len(disc_events) < minimum_discretionary_sell_events
        or len(planned_events) < minimum_planned_sell_events
        or observed_event_month_count < minimum_event_month_count
        or disc_label_coverage < minimum_label_coverage_share
        or planned_label_coverage < minimum_label_coverage_share
    )
    if plan_status != "pass":
        decision = "blocked_plan_flag_coverage"
        allow_d3: list[str] = []
        reason = "planned/discretionary S-code split is not observable"
    elif data_coverage_failed:
        decision = "hold_insufficient_sample"
        allow_d3 = []
        reason = "minimum sell contrast sample or label coverage contract failed"
    elif pre_failed:
        decision = "mixed_narrow_scope"
        allow_d3 = []
        reason = "pre-filing drift dominates post-filing discretionary sell window"
    elif placebo_failed:
        decision = "blocked_placebo_dominance"
        allow_d3 = []
        reason = "required placebo dominates live discretionary sell contrast"
    elif disc_mean < planned_mean:
        decision = "observable"
        allow_d3 = [CANDIDATE_ID]
        reason = "discretionary sells are more negative than planned sells after filing"
    else:
        decision = "not_observable"
        allow_d3 = []
        reason = "discretionary sell footprint is not more negative than planned sell footprint"
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": STAGE,
        "candidate_id": CANDIDATE_ID,
        "overall_decision": decision,
        "decision_reason": reason,
        "allow_d3_charter_for": allow_d3,
        "event_count": int(len(sell_events)),
        "discretionary_sell_event_count": int(len(disc_events)),
        "planned_sell_event_count": int(len(planned_events)),
        "unknown_plan_flag_event_count": int(sell_events["event_subset"].eq("unknown_plan_flag").sum()),
        "compensation_control_event_count": int(sell_events["event_subset"].eq("compensation_controls").sum()),
        "observed_event_month_count": observed_event_month_count,
        "discretionary_sell_label_coverage_share": disc_label_coverage,
        "planned_sell_label_coverage_share": planned_label_coverage,
        "discretionary_sell_primary_mean_abnormal_return": disc_mean,
        "planned_sell_primary_mean_abnormal_return": planned_mean,
        "discretionary_minus_planned_primary_mean_abnormal_return": disc_mean - planned_mean,
        "plan_flag_coverage_status": plan_status,
        "placebo_failed": placebo_failed,
        "pre_filing_dominance_failed": pre_failed,
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
        **DOWNSTREAM_FLAGS,
    }


def _window_return(frame: pd.DataFrame | None, anchor: pd.Timestamp, start: int, end: int) -> tuple[float, str]:
    if frame is None or frame.empty or pd.isna(anchor):
        return math.nan, "missing_price_window"
    anchor = pd.Timestamp(anchor)
    if anchor.tzinfo is not None:
        anchor = anchor.tz_convert(None)
    anchor = anchor.normalize()
    dates = frame["_date"]
    anchor_position = int(dates.searchsorted(anchor, side="left"))
    start_position = anchor_position + start
    end_position = anchor_position + end
    if start_position < 0 or end_position < 0 or start_position >= len(frame) or end_position >= len(frame):
        return math.nan, "missing_price_window"
    start_close = float(frame.iloc[start_position]["_close"])
    end_close = float(frame.iloc[end_position]["_close"])
    if start_close <= 0 or end_close <= 0:
        return math.nan, "invalid_price_window"
    return end_close / start_close - 1.0, "observed"


def _shifted_mean(events: pd.DataFrame, price_panel: pd.DataFrame, benchmark_panel: pd.DataFrame, shift: int) -> float:
    disc = events[events["event_subset"].eq("discretionary_sell")]
    if disc.empty:
        return 0.0
    price_by_ticker = {ticker: frame.reset_index(drop=True) for ticker, frame in price_panel.groupby("_ticker_key")}
    values = []
    for event in disc.itertuples(index=False):
        anchor = pd.to_datetime(event.tradable_timestamp, errors="coerce") + pd.tseries.offsets.BDay(shift)
        raw_return, status = _window_return(price_by_ticker.get(str(event.ticker).upper()), anchor, *WINDOWS[PRIMARY_WINDOW])
        benchmark_return, benchmark_status = _window_return(benchmark_panel, anchor, *WINDOWS[PRIMARY_WINDOW])
        if status == "observed":
            values.append(raw_return - (benchmark_return if benchmark_status == "observed" else 0.0))
    return _mean(values)


def _placebo_row(name: str, control_type: str, live_value: float, placebo_value: float) -> dict[str, object]:
    advantage = abs(placebo_value) - abs(live_value)
    return {
        "placebo_name": name,
        "control_type": control_type,
        "live_value": live_value,
        "placebo_value": placebo_value,
        "placebo_advantage": advantage,
        "status": "pass" if advantage < 0 else "fail",
        "not_alpha_evidence": True,
    }


def _pre_filing_dominates(car_panel: pd.DataFrame) -> bool:
    disc_post = abs(_metric(car_panel, "discretionary_sell", PRIMARY_WINDOW))
    if disc_post == 0:
        return False
    for window in ("pre_5_1", "pre_10_1", "pre_20_1"):
        if abs(_metric(car_panel, "discretionary_sell", window)) > disc_post:
            return True
    return False


def _metric(car_panel: pd.DataFrame, subset: str, window: str) -> float:
    rows = car_panel[(car_panel["event_subset"].eq(subset)) & (car_panel["window"].eq(window))]
    if rows.empty:
        return 0.0
    value = rows.iloc[0].get("mean_abnormal_return", 0.0)
    return float(value) if not pd.isna(value) else 0.0


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(pd.Series(values, dtype="float64").mean())


def _t_stat(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    series = pd.Series(values, dtype="float64")
    std = float(series.std(ddof=1))
    if std == 0:
        return 0.0
    return float(series.mean() / (std / math.sqrt(len(series))))


def _first_present(columns: Iterable[str], candidates: tuple[str, ...]) -> str | None:
    column_set = set(columns)
    for candidate in candidates:
        if candidate in column_set:
            return candidate
    return None


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _render_report(
    summary: dict[str, object],
    subset_counts: pd.DataFrame,
    plan_flag_coverage: pd.DataFrame,
    car_panel: pd.DataFrame,
    placebo: pd.DataFrame,
) -> str:
    primary = car_panel[car_panel["window"].eq(PRIMARY_WINDOW)]
    lines = [
        "# D2-INSIDER-02 Planned vs Discretionary Sell Contrast",
        "",
        "no-formula observability only",
        "not alpha evidence",
        "",
        "## Decision",
        "",
        f"- overall_decision: `{summary['overall_decision']}`",
        f"- decision_reason: `{summary['decision_reason']}`",
        f"- allowed_d3_charters: `{', '.join(summary['allow_d3_charter_for']) or 'none'}`",
        "",
        "## Plan Flag Coverage",
        "",
    ]
    coverage_row = plan_flag_coverage.iloc[0].to_dict()
    for key, value in coverage_row.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Subset Counts", ""])
    for row in subset_counts.itertuples(index=False):
        lines.append(f"- `{row.event_subset}`: events={row.event_count}, no_view={row.no_view_count}")
    lines.extend(["", "## Primary Window", ""])
    for row in primary.itertuples(index=False):
        lines.append(
            f"- `{row.event_subset}`: observed={row.observed_label_count}, mean_abnormal_return={row.mean_abnormal_return}",
        )
    lines.extend(["", "## Placebos", ""])
    for row in placebo.itertuples(index=False):
        lines.append(f"- `{row.placebo_name}`: {row.status}")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "This D2 step does not write a MeasurementSpec, formula score, signal panel, expected-return panel, Q1 handoff, Q2 handoff, optimizer input, portfolio result, Alpha Registry update, paper workflow, broker/order workflow, live workflow, or production approval.",
            "Missing plan flags and missing coverage remain no-view / abstain and are not encoded as zero alpha.",
            "",
        ],
    )
    return "\n".join(lines)
