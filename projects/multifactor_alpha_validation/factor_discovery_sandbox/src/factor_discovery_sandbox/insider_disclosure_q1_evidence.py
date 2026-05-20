"""Q1 evidence review for D3 open-market/P-code insider buying signals."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pandas as pd


SUMMARY_SCHEMA_VERSION = "insider_open_market_buying_q1_evidence_summary.v1"
STAGE = "Q1-INSIDER-01"
MEASUREMENT_SPEC_ID = "open_market_insider_buying_post_2023_v0"
TRANSACTION_CODE_SCOPE = "open_market_or_private_purchase"
P_CODE_SCOPE_WARNING = (
    "Form 4 code P means open-market or private purchase; this Q1 review "
    "does not prove every code-P event is exchange open-market."
)

FORBIDDEN_SIGNAL_PATTERNS = (
    "forward_return",
    "future_return",
    "expected_return",
    "optimizer",
    "portfolio",
    "alpha_registry",
    "paper_ready",
    "live_ready",
    "broker",
    "order",
    "production",
)
FORBIDDEN_SIGNAL_PREFIXES = ("q2_",)

WINDOWS: dict[str, tuple[int, int]] = {
    "post_0_1": (0, 1),
    "post_1_5": (1, 5),
    "post_1_10": (1, 10),
    "post_1_22": (1, 22),
    "post_1_44": (1, 44),
    "pre_5_1": (-5, -1),
    "pre_10_1": (-10, -1),
    "pre_20_1": (-20, -1),
}
PRIMARY_WINDOW = "post_1_22"
POST_WINDOWS = ("post_1_5", "post_1_10", "post_1_22", "post_1_44")
PRE_WINDOWS = ("pre_5_1", "pre_10_1", "pre_20_1")

DOWNSTREAM_FLAGS = {
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
class InsiderOpenMarketBuyingQ1Result:
    """Artifacts and summary for Q1-INSIDER-01."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_open_market_buying_q1_evidence_review(
    signal_panel_path: str | Path,
    price_panel_path: str | Path,
    output_dir: str | Path,
    benchmark_panel_path: str | Path | None = None,
    minimum_active_event_clusters: int = 500,
    minimum_event_month_count: int = 24,
    minimum_label_coverage_share: float = 0.75,
    random_seed: int = 20260513,
) -> InsiderOpenMarketBuyingQ1Result:
    """Run a narrow Q1 evidence review from D3 signal artifacts and local prices."""

    signal_path = Path(signal_panel_path)
    price_path = Path(price_panel_path)
    benchmark_path = Path(benchmark_panel_path) if benchmark_panel_path else None
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    signal_header = pd.read_csv(signal_path, nrows=0)
    forbidden_columns = _forbidden_signal_columns(signal_header.columns)
    if forbidden_columns:
        raise ValueError(f"forbidden signal panel columns: {', '.join(forbidden_columns)}")

    signal_panel = pd.read_csv(signal_path).fillna("")
    price_panel = _load_price_panel(price_path)
    benchmark_panel = _load_benchmark_panel(benchmark_path)

    event_clusters = _enrich_cluster_controls(_build_event_clusters(signal_panel), price_panel)
    labels = _build_forward_return_labels(event_clusters, price_panel, benchmark_panel)
    primary_labels = labels[labels["window"] == PRIMARY_WINDOW].copy()
    cluster_panel = _attach_primary_labels(event_clusters, primary_labels)
    signal_label_panel = _build_signal_label_panel(signal_panel, cluster_panel)
    rank_ic = _build_rank_ic_by_month(cluster_panel)
    treatment = _build_event_treatment_effect(cluster_panel)
    top_bottom = _build_top_bottom_spread(cluster_panel)
    pre_filing = _build_pre_filing_dominance_audit(labels)
    concentration = _build_cluster_concentration_report(cluster_panel)
    liquidity_cost = _build_liquidity_cost_pregate(cluster_panel)
    placebo = _build_placebo_report(
        cluster_panel=cluster_panel,
        signal_panel=signal_panel,
        price_panel=price_panel,
        benchmark_panel=benchmark_panel,
        random_seed=random_seed,
    )

    decision = _build_decision_summary(
        signal_panel=signal_panel,
        cluster_panel=cluster_panel,
        rank_ic=rank_ic,
        treatment=treatment,
        top_bottom=top_bottom,
        pre_filing=pre_filing,
        placebo=placebo,
        liquidity_cost=liquidity_cost,
        minimum_active_event_clusters=minimum_active_event_clusters,
        minimum_event_month_count=minimum_event_month_count,
        minimum_label_coverage_share=minimum_label_coverage_share,
    )

    cluster_panel.to_csv(artifacts["q1_event_cluster_panel"], index=False)
    signal_label_panel.to_csv(artifacts["q1_signal_label_panel"], index=False)
    labels.to_csv(artifacts["q1_forward_return_labels"], index=False)
    rank_ic.to_csv(artifacts["q1_rank_ic_by_month"], index=False)
    treatment.to_csv(artifacts["q1_event_treatment_effect"], index=False)
    top_bottom.to_csv(artifacts["q1_top_bottom_spread"], index=False)
    placebo.to_csv(artifacts["q1_placebo_report"], index=False)
    pre_filing.to_csv(artifacts["q1_pre_filing_dominance_audit"], index=False)
    concentration.to_csv(artifacts["q1_cluster_concentration_report"], index=False)
    liquidity_cost.to_csv(artifacts["q1_liquidity_cost_pregate"], index=False)
    _write_json(artifacts["q1_decision_summary"], decision)
    artifacts["q1_open_market_buying_evidence_report"].write_text(
        _render_report(decision, artifacts),
        encoding="utf-8",
    )
    return InsiderOpenMarketBuyingQ1Result(summary=decision, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "q1_event_cluster_panel": output_path / "q1_event_cluster_panel.csv",
        "q1_signal_label_panel": output_path / "q1_signal_label_panel.csv",
        "q1_forward_return_labels": output_path / "q1_forward_return_labels.csv",
        "q1_rank_ic_by_month": output_path / "q1_rank_ic_by_month.csv",
        "q1_event_treatment_effect": output_path / "q1_event_treatment_effect.csv",
        "q1_top_bottom_spread": output_path / "q1_top_bottom_spread.csv",
        "q1_placebo_report": output_path / "q1_placebo_report.csv",
        "q1_pre_filing_dominance_audit": output_path / "q1_pre_filing_dominance_audit.csv",
        "q1_cluster_concentration_report": output_path / "q1_cluster_concentration_report.csv",
        "q1_liquidity_cost_pregate": output_path / "q1_liquidity_cost_pregate.csv",
        "q1_decision_summary": output_path / "q1_decision_summary.json",
        "q1_open_market_buying_evidence_report": output_path / "q1_open_market_buying_evidence_report.md",
    }


def _forbidden_signal_columns(columns: pd.Index) -> list[str]:
    forbidden: list[str] = []
    for column in columns:
        lower = str(column).lower()
        if lower.startswith(FORBIDDEN_SIGNAL_PREFIXES) or any(pattern in lower for pattern in FORBIDDEN_SIGNAL_PATTERNS):
            forbidden.append(str(column))
    return forbidden


def _load_price_panel(path: Path) -> pd.DataFrame:
    price = pd.read_csv(path).fillna("")
    close_col = _first_present(price.columns, ("adjusted_close", "raw_close", "close", "price"))
    if not close_col:
        raise ValueError("price panel must contain adjusted_close, raw_close, close, or price")
    required = {"ticker", "date"}
    missing = sorted(required - set(price.columns))
    if missing:
        raise ValueError(f"price panel missing columns: {', '.join(missing)}")
    price = price.copy()
    price["_ticker_key"] = price["ticker"].astype(str).str.upper()
    price["_date"] = pd.to_datetime(price["date"], errors="coerce").dt.normalize()
    price["_close"] = pd.to_numeric(price[close_col], errors="coerce")
    dollar_volume_col = _first_present(price.columns, ("dollar_volume", "dlyprcvol", "dollar_volume_20d"))
    volume_col = _first_present(price.columns, ("volume", "vol"))
    if dollar_volume_col:
        price["_dollar_volume"] = pd.to_numeric(price[dollar_volume_col], errors="coerce")
    elif volume_col:
        price["_dollar_volume"] = price["_close"] * pd.to_numeric(price[volume_col], errors="coerce")
    else:
        price["_dollar_volume"] = pd.NA
    spread_col = _first_present(price.columns, ("spread_proxy", "bid_ask_spread", "spread"))
    price["_spread_proxy"] = pd.to_numeric(price[spread_col], errors="coerce").fillna(0.0) if spread_col else 0.0
    if "sector" not in price.columns:
        price["sector"] = ""
    price = price[price["_date"].notna() & price["_close"].notna()].sort_values(["_ticker_key", "_date"])
    return price.reset_index(drop=True)


def _load_benchmark_panel(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame(columns=["_date", "_close"])
    benchmark = pd.read_csv(path).fillna("")
    close_col = _first_present(benchmark.columns, ("adjusted_close", "raw_close", "close", "price"))
    if not close_col or "date" not in benchmark.columns:
        return pd.DataFrame(columns=["_date", "_close"])
    benchmark = benchmark.copy()
    benchmark["_date"] = pd.to_datetime(benchmark["date"], errors="coerce").dt.normalize()
    benchmark["_close"] = pd.to_numeric(benchmark[close_col], errors="coerce")
    benchmark = benchmark[benchmark["_date"].notna() & benchmark["_close"].notna()].sort_values("_date")
    return benchmark.reset_index(drop=True)


def _build_event_clusters(signal_panel: pd.DataFrame) -> pd.DataFrame:
    active = signal_panel[signal_panel["coverage_state"].eq("active")].copy()
    if active.empty:
        return _empty_cluster_panel()
    active["issuer_event_cluster_id"] = active.apply(_cluster_id_from_row, axis=1)
    active["_normalized_signal"] = pd.to_numeric(active["normalized_signal"], errors="coerce")
    active["_raw_buy_conviction"] = pd.to_numeric(active["raw_buy_conviction"], errors="coerce")
    active["_buy_dollar_value"] = pd.to_numeric(active["buy_dollar_value"], errors="coerce")
    active["_buy_value_pct"] = pd.to_numeric(active["buy_value_pct"], errors="coerce")
    active["_role_weight"] = pd.to_numeric(active["role_weight"], errors="coerce")
    active["_adv_20d"] = pd.to_numeric(active.get("adv_20d", ""), errors="coerce")
    active["_spread_proxy"] = pd.to_numeric(active.get("spread_proxy", ""), errors="coerce")
    group = active.groupby("issuer_event_cluster_id", sort=True, dropna=False)
    rows = []
    for cluster_id, frame in group:
        first = frame.iloc[0]
        rows.append(
            {
                "issuer_event_cluster_id": cluster_id,
                "ticker": first.get("ticker", ""),
                "cik": first.get("cik", ""),
                "signal_date": first.get("signal_date", ""),
                "tradable_ts": first.get("tradable_ts", ""),
                "event_subset": "open_market_buy",
                "event_month": _month(first.get("signal_date", "")),
                "source_signal_row_count": int(len(frame)),
                "source_event_ids": ";".join(frame["event_id"].astype(str).tolist()),
                "source_accession_numbers": ";".join(sorted(set(frame["accession_number"].astype(str).tolist()))),
                "cluster_distinct_insiders": int(_max_numeric(frame.get("distinct_buying_insider_count", pd.Series(dtype=object)))),
                "cluster_buy_dollar_value": float(frame["_buy_dollar_value"].max(skipna=True)),
                "cluster_buy_value_pct": float(frame["_buy_value_pct"].max(skipna=True)),
                "cluster_max_role_weight": float(frame["_role_weight"].max(skipna=True)),
                "cluster_raw_buy_conviction": float(frame["_raw_buy_conviction"].max(skipna=True)),
                "cluster_normalized_signal": float(frame["_normalized_signal"].mean(skipna=True)),
                "adv_20d": float(frame["_adv_20d"].max(skipna=True)) if "_adv_20d" in frame else 0.0,
                "spread_proxy": float(frame["_spread_proxy"].max(skipna=True)) if "_spread_proxy" in frame else 0.0,
                "sector": _first_non_empty(frame.get("sector", pd.Series(dtype=object))),
                "size_bucket": _first_non_empty(frame.get("size_bucket", pd.Series(dtype=object))),
                "liquidity_bucket": _first_non_empty(frame.get("liquidity_bucket", pd.Series(dtype=object))),
                "coverage_state": "active",
                "transaction_code_scope": TRANSACTION_CODE_SCOPE,
                "private_purchase_filter_status": "unavailable_from_form4_code_only",
                "no_view_not_zero_alpha": True,
            },
        )
    return pd.DataFrame(rows)


def _empty_cluster_panel() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "issuer_event_cluster_id",
            "ticker",
            "cik",
            "signal_date",
            "tradable_ts",
            "event_subset",
            "event_month",
            "source_signal_row_count",
            "source_event_ids",
            "cluster_normalized_signal",
            "coverage_state",
        ],
    )


def _build_forward_return_labels(
    clusters: pd.DataFrame,
    price_panel: pd.DataFrame,
    benchmark_panel: pd.DataFrame,
) -> pd.DataFrame:
    price_by_ticker = {ticker: frame.reset_index(drop=True) for ticker, frame in price_panel.groupby("_ticker_key", sort=False)}
    rows: list[dict[str, object]] = []
    for cluster in clusters.itertuples(index=False):
        ticker_key = str(cluster.ticker).upper()
        ticker_prices = price_by_ticker.get(ticker_key)
        anchor = pd.to_datetime(cluster.signal_date, errors="coerce")
        for window_name, (start, end) in WINDOWS.items():
            raw_return, status = _window_return(ticker_prices, anchor, start, end)
            benchmark_return, benchmark_status = _window_return(benchmark_panel, anchor, start, end)
            if status != "observed":
                abnormal_return = math.nan
                label_status = status
            else:
                abnormal_return = raw_return - (benchmark_return if benchmark_status == "observed" else 0.0)
                label_status = "observed"
            rows.append(
                {
                    "issuer_event_cluster_id": cluster.issuer_event_cluster_id,
                    "ticker": cluster.ticker,
                    "signal_date": cluster.signal_date,
                    "event_month": cluster.event_month,
                    "window": window_name,
                    "window_start": start,
                    "window_end": end,
                    "raw_return": raw_return if status == "observed" else "",
                    "benchmark_return": benchmark_return if benchmark_status == "observed" else "",
                    "abnormal_return": abnormal_return if label_status == "observed" else "",
                    "label_status": label_status,
                    "benchmark_status": benchmark_status,
                    "return_anchor": "tradable_signal_date",
                    "no_forward_return_in_signal": True,
                },
            )
    return pd.DataFrame(rows)


def _enrich_cluster_controls(clusters: pd.DataFrame, price_panel: pd.DataFrame) -> pd.DataFrame:
    if clusters.empty:
        return clusters
    enriched = clusters.copy()
    price_by_ticker = {ticker: frame.reset_index(drop=True) for ticker, frame in price_panel.groupby("_ticker_key", sort=False)}
    for index, row in enriched.iterrows():
        ticker_prices = price_by_ticker.get(str(row.get("ticker", "")).upper())
        anchor = pd.to_datetime(row.get("signal_date", ""), errors="coerce")
        if ticker_prices is None or pd.isna(anchor):
            continue
        position = int(ticker_prices["_date"].searchsorted(pd.Timestamp(anchor).normalize(), side="right")) - 1
        if position < 0 or position >= len(ticker_prices):
            continue
        payload = ticker_prices.iloc[position]
        if not _has_positive_number(row.get("adv_20d", 0.0)):
            enriched.loc[index, "adv_20d"] = float(payload.get("_dollar_volume", 0.0) or 0.0)
        if not _has_positive_number(row.get("spread_proxy", 0.0)):
            enriched.loc[index, "spread_proxy"] = float(payload.get("_spread_proxy", 0.0) or 0.0)
        if not str(row.get("sector", "")).strip():
            enriched.loc[index, "sector"] = str(payload.get("sector", ""))
    return enriched


def _attach_primary_labels(clusters: pd.DataFrame, primary_labels: pd.DataFrame) -> pd.DataFrame:
    if clusters.empty:
        clusters = clusters.copy()
        clusters["primary_label_status"] = ""
        clusters["primary_raw_return"] = ""
        clusters["primary_benchmark_return"] = ""
        clusters["primary_abnormal_return"] = ""
        return clusters
    primary = primary_labels[
        [
            "issuer_event_cluster_id",
            "raw_return",
            "benchmark_return",
            "abnormal_return",
            "label_status",
        ]
    ].rename(
        columns={
            "raw_return": "primary_raw_return",
            "benchmark_return": "primary_benchmark_return",
            "abnormal_return": "primary_abnormal_return",
            "label_status": "primary_label_status",
        },
    )
    merged = clusters.merge(primary, on="issuer_event_cluster_id", how="left")
    merged["primary_label_status"] = merged["primary_label_status"].fillna("missing_price_window")
    return merged.fillna("")


def _build_signal_label_panel(signal_panel: pd.DataFrame, cluster_panel: pd.DataFrame) -> pd.DataFrame:
    rows = signal_panel.copy()
    rows["issuer_event_cluster_id"] = rows.apply(_cluster_id_from_row, axis=1)
    label_cols = cluster_panel[
        [
            "issuer_event_cluster_id",
            "primary_raw_return",
            "primary_benchmark_return",
            "primary_abnormal_return",
            "primary_label_status",
        ]
    ] if not cluster_panel.empty else pd.DataFrame(
        columns=[
            "issuer_event_cluster_id",
            "primary_raw_return",
            "primary_benchmark_return",
            "primary_abnormal_return",
            "primary_label_status",
        ],
    )
    rows = rows.merge(label_cols, on="issuer_event_cluster_id", how="left")
    no_view_mask = rows["coverage_state"].eq("no_view")
    for column in ("primary_raw_return", "primary_benchmark_return", "primary_abnormal_return"):
        rows[column] = rows[column].astype(object)
    rows.loc[no_view_mask, "primary_raw_return"] = ""
    rows.loc[no_view_mask, "primary_benchmark_return"] = ""
    rows.loc[no_view_mask, "primary_abnormal_return"] = ""
    rows.loc[no_view_mask, "primary_label_status"] = "no_view_signal_excluded"
    rows["q1_label_status"] = rows["primary_label_status"].fillna("missing_price_window")
    rows["q1_no_view_not_zero_alpha"] = True
    rows["q2_entry_allowed"] = False
    return rows.fillna("")


def _build_rank_ic_by_month(cluster_panel: pd.DataFrame) -> pd.DataFrame:
    observed = _observed_primary(cluster_panel)
    rows = []
    for month, frame in observed.groupby("event_month", sort=True):
        if len(frame) < 2:
            rank_ic = math.nan
            status = "insufficient_monthly_cross_section"
        else:
            rank_ic = frame["cluster_normalized_signal"].astype(float).corr(
                frame["primary_abnormal_return"].astype(float),
                method="spearman",
            )
            status = "observed" if not pd.isna(rank_ic) else "insufficient_monthly_cross_section"
        rows.append(
            {
                "event_month": month,
                "cluster_count": int(len(frame)),
                "rank_ic": rank_ic if status == "observed" else "",
                "status": status,
            },
        )
    return pd.DataFrame(rows, columns=["event_month", "cluster_count", "rank_ic", "status"])


def _build_event_treatment_effect(cluster_panel: pd.DataFrame) -> pd.DataFrame:
    observed = _observed_primary(cluster_panel)
    rows = []
    for month, frame in observed.groupby("event_month", sort=True):
        values = frame["primary_abnormal_return"].astype(float)
        rows.append(
            {
                "event_month": month,
                "cluster_count": int(len(frame)),
                "mean_abnormal_return": float(values.mean()),
                "median_abnormal_return": float(values.median()),
                "win_rate": float((values > 0).mean()),
                "t_stat": _t_stat(values),
                "status": "observed",
            },
        )
    return pd.DataFrame(
        rows,
        columns=[
            "event_month",
            "cluster_count",
            "mean_abnormal_return",
            "median_abnormal_return",
            "win_rate",
            "t_stat",
            "status",
        ],
    )


def _build_top_bottom_spread(cluster_panel: pd.DataFrame) -> pd.DataFrame:
    observed = _observed_primary(cluster_panel)
    rows = []
    for month, frame in observed.groupby("event_month", sort=True):
        ordered = frame.sort_values("cluster_normalized_signal")
        if len(ordered) < 2:
            rows.append(
                {
                    "event_month": month,
                    "cluster_count": int(len(ordered)),
                    "top_mean_abnormal_return": "",
                    "bottom_mean_abnormal_return": "",
                    "top_bottom_spread": "",
                    "status": "insufficient_monthly_cross_section",
                },
            )
            continue
        bucket_size = max(1, len(ordered) // 3)
        bottom = ordered.head(bucket_size)["primary_abnormal_return"].astype(float)
        top = ordered.tail(bucket_size)["primary_abnormal_return"].astype(float)
        rows.append(
            {
                "event_month": month,
                "cluster_count": int(len(ordered)),
                "top_mean_abnormal_return": float(top.mean()),
                "bottom_mean_abnormal_return": float(bottom.mean()),
                "top_bottom_spread": float(top.mean() - bottom.mean()),
                "status": "observed",
            },
        )
    return pd.DataFrame(rows)


def _build_pre_filing_dominance_audit(labels: pd.DataFrame) -> pd.DataFrame:
    rows = []
    primary = _mean_label(labels, PRIMARY_WINDOW)
    for window in PRE_WINDOWS:
        pre = _mean_label(labels, window)
        rows.append(
            {
                "pre_window": window,
                "primary_post_window": PRIMARY_WINDOW,
                "pre_mean_abnormal_return": pre,
                "primary_mean_abnormal_return": primary,
                "pre_to_post_abs_ratio": abs(pre) / abs(primary) if primary else 0.0,
                "status": "fail" if primary and abs(pre) > abs(primary) else "pass",
            },
        )
    return pd.DataFrame(rows)


def _build_cluster_concentration_report(cluster_panel: pd.DataFrame) -> pd.DataFrame:
    observed = _observed_primary(cluster_panel)
    if observed.empty:
        return pd.DataFrame(
            [
                {
                    "observed_cluster_count": 0,
                    "issuer_count": 0,
                    "top_issuer_cluster_share": 0.0,
                    "top_month_cluster_share": 0.0,
                    "status": "insufficient_observed_labels",
                },
            ],
        )
    issuer_counts = observed.groupby("ticker").size()
    month_counts = observed.groupby("event_month").size()
    top_issuer_share = float(issuer_counts.max() / len(observed))
    top_month_share = float(month_counts.max() / len(observed))
    return pd.DataFrame(
        [
            {
                "observed_cluster_count": int(len(observed)),
                "issuer_count": int(observed["ticker"].nunique()),
                "top_issuer_cluster_share": top_issuer_share,
                "top_month_cluster_share": top_month_share,
                "status": "fail" if top_issuer_share > 0.20 or top_month_share > 0.35 else "pass",
            },
        ],
    )


def _build_liquidity_cost_pregate(cluster_panel: pd.DataFrame) -> pd.DataFrame:
    observed = _observed_primary(cluster_panel)
    if observed.empty:
        return pd.DataFrame(
            [
                {
                    "observed_cluster_count": 0,
                    "mean_primary_abnormal_return": 0.0,
                    "estimated_roundtrip_spread_cost": 0.0,
                    "mean_after_spread_cost": 0.0,
                    "low_liquidity_cluster_share": 0.0,
                    "status": "insufficient_observed_labels",
                },
            ],
        )
    spreads = pd.to_numeric(observed.get("spread_proxy", 0.0), errors="coerce").fillna(0.0)
    abnormal = observed["primary_abnormal_return"].astype(float)
    roundtrip = float((2.0 * spreads).mean())
    low_liquidity_share = float(observed.get("liquidity_bucket", pd.Series(dtype=object)).astype(str).eq("low").mean())
    mean_abnormal = float(abnormal.mean())
    net = mean_abnormal - roundtrip
    status = "fail" if net <= 0 or low_liquidity_share > 0.70 else "pass"
    return pd.DataFrame(
        [
            {
                "observed_cluster_count": int(len(observed)),
                "mean_primary_abnormal_return": mean_abnormal,
                "estimated_roundtrip_spread_cost": roundtrip,
                "mean_after_spread_cost": net,
                "low_liquidity_cluster_share": low_liquidity_share,
                "status": status,
            },
        ],
    )


def _build_placebo_report(
    cluster_panel: pd.DataFrame,
    signal_panel: pd.DataFrame,
    price_panel: pd.DataFrame,
    benchmark_panel: pd.DataFrame,
    random_seed: int,
) -> pd.DataFrame:
    observed = _observed_primary(cluster_panel)
    live_value = float(observed["primary_abnormal_return"].astype(float).mean()) if not observed.empty else 0.0
    rows = []
    for name, shift in (
        ("shift_minus_5", -5),
        ("shift_plus_5", 5),
        ("shift_minus_10", -10),
        ("shift_plus_10", 10),
    ):
        value = _mean_shifted_return(observed, price_panel, benchmark_panel, shift)
        rows.append(_placebo_row(name, live_value, value, "shifted_filing_date"))

    random_value = _randomized_top_bottom_value(observed, random_seed)
    rows.append(_placebo_row("same_coverage_random", _mean_top_bottom(cluster_panel), random_value, "same_coverage_random"))

    role_value = _randomized_top_bottom_value(observed, random_seed + 17)
    rows.append(_placebo_row("role_label_randomized", _mean_top_bottom(cluster_panel), role_value, "role_label_randomized"))

    rows.append(_placebo_row("issuer_non_event", live_value, _mean_shifted_return(observed, price_panel, benchmark_panel, 10), "issuer_non_event_shift"))

    compensation = _compensation_control_value(signal_panel, price_panel, benchmark_panel)
    rows.append(_placebo_row("compensation_control", live_value, compensation, "compensation_control_transactions"))
    return pd.DataFrame(rows)


def _build_decision_summary(
    signal_panel: pd.DataFrame,
    cluster_panel: pd.DataFrame,
    rank_ic: pd.DataFrame,
    treatment: pd.DataFrame,
    top_bottom: pd.DataFrame,
    pre_filing: pd.DataFrame,
    placebo: pd.DataFrame,
    liquidity_cost: pd.DataFrame,
    minimum_active_event_clusters: int,
    minimum_event_month_count: int,
    minimum_label_coverage_share: float,
) -> dict[str, object]:
    active_signal_rows = int(signal_panel["coverage_state"].eq("active").sum())
    no_view_rows = int(signal_panel["coverage_state"].eq("no_view").sum())
    active_clusters = int(len(cluster_panel))
    observed = _observed_primary(cluster_panel)
    observed_count = int(len(observed))
    event_month_count = int(cluster_panel["event_month"].nunique()) if not cluster_panel.empty else 0
    observed_event_month_count = int(observed["event_month"].nunique()) if not observed.empty else 0
    label_coverage_share = round(observed_count / active_clusters, 6) if active_clusters else 0.0
    mean_abnormal = float(observed["primary_abnormal_return"].astype(float).mean()) if not observed.empty else 0.0
    rank_ic_mean = _mean_observed_column(rank_ic, "rank_ic")
    top_bottom_mean = _mean_observed_column(top_bottom, "top_bottom_spread")
    treatment_positive_rate = (
        float((treatment["mean_abnormal_return"].astype(float) > 0).mean()) if not treatment.empty else 0.0
    )
    placebo_failed = bool(not placebo.empty and placebo["status"].eq("fail").any())
    pre_failed = bool(not pre_filing.empty and pre_filing["status"].eq("fail").any())
    concentration_status = _single_status(_build_cluster_concentration_report(cluster_panel))
    cost_status = _single_status(liquidity_cost)

    if (
        active_clusters < minimum_active_event_clusters
        or observed_event_month_count < minimum_event_month_count
        or label_coverage_share < minimum_label_coverage_share
    ):
        decision = "hold_insufficient_sample"
        interpretation = "insufficient_q1_label_sample_or_price_coverage"
    elif pre_failed:
        decision = "blocked_timestamp"
        interpretation = "pre_filing_drift_dominates_post_tradable_window"
    elif placebo_failed:
        decision = "blocked_placebo"
        interpretation = "required_placebo_or_control_dominates_live_read"
    elif cost_status == "fail":
        decision = "blocked_cost_liquidity"
        interpretation = "liquidity_or_spread_pregate_is_fatal"
    elif mean_abnormal > 0 and rank_ic_mean > 0 and top_bottom_mean > 0 and treatment_positive_rate >= 0.60:
        decision = "passed_q1"
        interpretation = "event_footprint_and_score_ranking_observed"
    elif mean_abnormal > 0 and (rank_ic_mean <= 0 or top_bottom_mean <= 0):
        decision = "mixed_narrow_scope"
        interpretation = "event_footprint_observable_but_score_not_validated"
    elif rank_ic_mean > 0 and top_bottom_mean > 0:
        decision = "mixed_narrow_scope"
        interpretation = "relative_ranking_signal_only"
    else:
        decision = "failed_q1"
        interpretation = "event_footprint_and_score_ranking_not_observed"

    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": STAGE,
        "measurement_spec_id": MEASUREMENT_SPEC_ID,
        "q1_decision": decision,
        "q1_result_interpretation": interpretation,
        "active_signal_rows": active_signal_rows,
        "no_view_rows": no_view_rows,
        "active_event_clusters": active_clusters,
        "observed_primary_label_clusters": observed_count,
        "event_month_count": event_month_count,
        "observed_event_month_count": observed_event_month_count,
        "label_coverage_share": label_coverage_share,
        "primary_window": PRIMARY_WINDOW,
        "primary_mean_abnormal_return": mean_abnormal,
        "rank_ic_mean": rank_ic_mean,
        "top_bottom_spread_mean": top_bottom_mean,
        "positive_event_month_share": treatment_positive_rate,
        "placebo_failed": placebo_failed,
        "pre_filing_dominance_failed": pre_failed,
        "cluster_concentration_status": concentration_status,
        "liquidity_cost_status": cost_status,
        "transaction_code_scope": TRANSACTION_CODE_SCOPE,
        "p_code_purchase_scope_warning": P_CODE_SCOPE_WARNING,
        "no_view_not_zero_alpha": True,
        "promotion_gate_allowed": decision == "passed_q1",
        "not_q2_evidence": True,
        **DOWNSTREAM_FLAGS,
    }


def _render_report(summary: Mapping[str, object], artifacts: Mapping[str, Path]) -> str:
    return "\n".join(
        [
            "# Q1 Open-Market Insider Buying Evidence Report",
            "",
            "Q1 evidence review only.",
            "This is not Promotion Gate, Q2, optimizer, portfolio construction, Alpha Registry, paper workflow, broker/order workflow, live workflow, or production approval.",
            "Form 4 code P means open-market or private purchase.",
            P_CODE_SCOPE_WARNING,
            "",
            "## Summary",
            "",
            f"- decision: `{summary['q1_decision']}`",
            f"- interpretation: `{summary['q1_result_interpretation']}`",
            f"- active event clusters: {summary['active_event_clusters']}",
            f"- observed primary labels: {summary['observed_primary_label_clusters']}",
            f"- label coverage share: {summary['label_coverage_share']}",
            f"- primary mean abnormal return: {summary['primary_mean_abnormal_return']}",
            f"- rank IC mean: {summary['rank_ic_mean']}",
            f"- top-bottom spread mean: {summary['top_bottom_spread_mean']}",
            "",
            "## Artifacts",
            "",
            f"- event clusters: `{artifacts['q1_event_cluster_panel']}`",
            f"- signal labels: `{artifacts['q1_signal_label_panel']}`",
            f"- placebos: `{artifacts['q1_placebo_report']}`",
            f"- decision summary: `{artifacts['q1_decision_summary']}`",
            "",
            "## Boundary",
            "",
            "Missing or no-view rows remain abstain and are not encoded as zero.",
            "The review writes no expected-return panel and opens no downstream execution path.",
            "",
        ],
    )


def _cluster_id_from_row(row: Mapping[str, object]) -> str:
    return "|".join(
        [
            str(row.get("ticker", "")),
            str(row.get("cik", "")),
            str(row.get("signal_date", "")),
            str(row.get("event_subset", "")),
        ],
    )


def _window_return(frame: pd.DataFrame | None, anchor: pd.Timestamp, start: int, end: int) -> tuple[float, str]:
    if frame is None or frame.empty or pd.isna(anchor):
        return 0.0, "missing_price_window"
    anchor = pd.Timestamp(anchor).normalize()
    dates = frame["_date"]
    anchor_pos = int(dates.searchsorted(anchor, side="left"))
    if anchor_pos >= len(frame):
        return 0.0, "missing_price_window"
    if start >= 0:
        base_pos = anchor_pos + max(0, start - 1)
    else:
        base_pos = anchor_pos + start
    end_pos = anchor_pos + end
    if base_pos < 0 or end_pos < 0 or base_pos >= len(frame) or end_pos >= len(frame):
        return 0.0, "missing_price_window"
    base = float(frame.iloc[base_pos]["_close"])
    terminal = float(frame.iloc[end_pos]["_close"])
    if base <= 0:
        return 0.0, "missing_price_window"
    return terminal / base - 1.0, "observed"


def _shifted_anchor(frame: pd.DataFrame | None, anchor: pd.Timestamp, shift: int) -> pd.Timestamp | None:
    if frame is None or frame.empty or pd.isna(anchor):
        return None
    anchor = pd.Timestamp(anchor).normalize()
    anchor_pos = int(frame["_date"].searchsorted(anchor, side="left"))
    shifted_pos = anchor_pos + shift
    if shifted_pos < 0 or shifted_pos >= len(frame):
        return None
    return pd.Timestamp(frame.iloc[shifted_pos]["_date"])


def _mean_shifted_return(
    observed: pd.DataFrame,
    price_panel: pd.DataFrame,
    benchmark_panel: pd.DataFrame,
    shift: int,
) -> float:
    if observed.empty:
        return 0.0
    price_by_ticker = {ticker: frame.reset_index(drop=True) for ticker, frame in price_panel.groupby("_ticker_key", sort=False)}
    values = []
    for row in observed.itertuples(index=False):
        ticker_prices = price_by_ticker.get(str(row.ticker).upper())
        anchor = pd.to_datetime(row.signal_date, errors="coerce")
        shifted_anchor = _shifted_anchor(ticker_prices, anchor, shift)
        if shifted_anchor is None:
            continue
        raw_return, raw_status = _window_return(ticker_prices, shifted_anchor, 1, 22)
        bench_return, bench_status = _window_return(benchmark_panel, shifted_anchor, 1, 22)
        if raw_status == "observed":
            values.append(raw_return - (bench_return if bench_status == "observed" else 0.0))
    return float(pd.Series(values).mean()) if values else 0.0


def _randomized_top_bottom_value(observed: pd.DataFrame, seed: int) -> float:
    if len(observed) < 3:
        return 0.0
    shuffled = observed.copy()
    shuffled["cluster_normalized_signal"] = shuffled["cluster_normalized_signal"].sample(
        frac=1.0,
        random_state=seed,
    ).to_numpy()
    return _top_bottom_value(shuffled)


def _compensation_control_value(signal_panel: pd.DataFrame, price_panel: pd.DataFrame, benchmark_panel: pd.DataFrame) -> float:
    controls = signal_panel[signal_panel["event_subset"].eq("compensation_control")].copy()
    if controls.empty:
        return 0.0
    controls["issuer_event_cluster_id"] = controls.apply(_cluster_id_from_row, axis=1)
    controls["event_month"] = pd.to_datetime(controls["signal_date"], errors="coerce").dt.strftime("%Y-%m")
    controls = controls.drop_duplicates("issuer_event_cluster_id")
    price_by_ticker = {ticker: frame.reset_index(drop=True) for ticker, frame in price_panel.groupby("_ticker_key", sort=False)}
    values = []
    for row in controls.itertuples(index=False):
        ticker_prices = price_by_ticker.get(str(row.ticker).upper())
        anchor = pd.to_datetime(row.signal_date, errors="coerce")
        raw_return, raw_status = _window_return(ticker_prices, anchor, 1, 22)
        bench_return, bench_status = _window_return(benchmark_panel, anchor, 1, 22)
        if raw_status == "observed":
            values.append(raw_return - (bench_return if bench_status == "observed" else 0.0))
    return float(pd.Series(values).mean()) if values else 0.0


def _placebo_row(name: str, live_value: float, placebo_value: float, control_type: str) -> dict[str, object]:
    advantage = abs(placebo_value) - abs(live_value)
    return {
        "placebo_name": name,
        "control_type": control_type,
        "live_value": live_value,
        "placebo_value": placebo_value,
        "placebo_advantage": advantage,
        "status": "pass" if advantage < 0 else "fail",
    }


def _observed_primary(cluster_panel: pd.DataFrame) -> pd.DataFrame:
    if cluster_panel.empty or "primary_label_status" not in cluster_panel.columns:
        return cluster_panel.iloc[0:0].copy()
    observed = cluster_panel[cluster_panel["primary_label_status"].eq("observed")].copy()
    if observed.empty:
        return observed
    observed["primary_abnormal_return"] = pd.to_numeric(observed["primary_abnormal_return"], errors="coerce")
    observed["cluster_normalized_signal"] = pd.to_numeric(observed["cluster_normalized_signal"], errors="coerce")
    return observed[observed["primary_abnormal_return"].notna() & observed["cluster_normalized_signal"].notna()]


def _mean_label(labels: pd.DataFrame, window: str) -> float:
    frame = labels[(labels["window"] == window) & (labels["label_status"] == "observed")]
    if frame.empty:
        return 0.0
    return float(pd.to_numeric(frame["abnormal_return"], errors="coerce").mean())


def _mean_top_bottom(cluster_panel: pd.DataFrame) -> float:
    observed = _observed_primary(cluster_panel)
    return _top_bottom_value(observed)


def _top_bottom_value(observed: pd.DataFrame) -> float:
    if len(observed) < 2:
        return 0.0
    ordered = observed.sort_values("cluster_normalized_signal")
    bucket_size = max(1, len(ordered) // 3)
    bottom = ordered.head(bucket_size)["primary_abnormal_return"].astype(float)
    top = ordered.tail(bucket_size)["primary_abnormal_return"].astype(float)
    return float(top.mean() - bottom.mean())


def _mean_observed_column(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else 0.0


def _single_status(frame: pd.DataFrame) -> str:
    if frame.empty or "status" not in frame.columns:
        return "unknown"
    return str(frame.iloc[0]["status"])


def _t_stat(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if len(clean) < 2:
        return 0.0
    std = clean.std(ddof=1)
    if std == 0 or pd.isna(std):
        return 0.0
    return float(clean.mean() / (std / math.sqrt(len(clean))))


def _first_present(columns: pd.Index, candidates: tuple[str, ...]) -> str:
    column_set = set(columns)
    for candidate in candidates:
        if candidate in column_set:
            return candidate
    return ""


def _first_non_empty(values: pd.Series) -> str:
    for value in values.astype(str):
        if value:
            return value
    return ""


def _max_numeric(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float(numeric.max()) if not numeric.empty else 0.0


def _has_positive_number(value: object) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def _month(value: object) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(timestamp) else timestamp.strftime("%Y-%m")


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
