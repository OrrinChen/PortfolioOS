from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import yaml


@dataclass(frozen=True)
class PITExposureStoreResult:
    exposure_panel_path: str
    coverage_report_path: str
    exposure_manifest_path: str
    exposure_count: int
    date_count: int
    asset_count: int
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool


PRICE_EXPOSURES = (
    "trailing_market_beta_252d",
    "log_market_cap",
    "liquidity_adv_60d",
    "residual_volatility_60d",
    "short_term_reversal_5d",
    "medium_term_momentum_12_1",
)
FUNDAMENTAL_EXPOSURES = (
    "fundamental_book_to_market",
    "fundamental_profitability_roa",
    "fundamental_asset_growth",
)
EXPECTED_EXPOSURES = ("sector", "industry") + PRICE_EXPOSURES + FUNDAMENTAL_EXPOSURES
MIN_BETA_OBSERVATIONS = 60


def run_pit_exposure_store(
    research_manifest_path: Path,
    fundamentals_manifest_path: Path | None,
    output_dir: Path,
) -> PITExposureStoreResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    research_manifest = _load_yaml(research_manifest_path)
    fundamentals_manifest = _load_yaml(fundamentals_manifest_path) if fundamentals_manifest_path else {}

    universe = _normalize_universe(_load_research_csv(research_manifest, research_manifest_path, "universe"))
    prices = _normalize_prices(_load_research_csv(research_manifest, research_manifest_path, "prices"))
    benchmark = _normalize_benchmark(_load_research_csv(research_manifest, research_manifest_path, "benchmark"))
    fundamentals = _normalize_fundamentals(
        _load_fundamentals_csv(fundamentals_manifest, fundamentals_manifest_path, "quarterly_fundamentals_panel")
        if fundamentals_manifest_path
        else pd.DataFrame()
    )

    trading_dates = sorted(pd.Timestamp(value) for value in prices["date"].dropna().unique())
    rebalance_dates = _eligible_month_end_dates(trading_dates)
    benchmark_returns = benchmark[["date", "benchmark_daily_return"]].dropna()
    prices_by_asset = {
        str(asset_id): group.sort_values("date").reset_index(drop=True)
        for asset_id, group in prices.groupby("asset_id", sort=False)
    }
    fundamentals_by_gvkey = (
        {
            str(gvkey): group.sort_values(["visibility_timestamp", "datadate"]).reset_index(drop=True)
            for gvkey, group in fundamentals.groupby("gvkey", sort=False)
        }
        if not fundamentals.empty
        else {}
    )
    empty_price_frame = pd.DataFrame(columns=prices.columns)

    records: list[dict[str, object]] = []
    for signal_date in rebalance_dates:
        tradable_date = _next_trading_date(trading_dates, signal_date)
        if tradable_date is None:
            continue
        active = _active_universe(universe, signal_date)
        for active_row in active.itertuples(index=False):
            asset_id = str(active_row.asset_id)
            gvkey = str(active_row.gvkey) if pd.notna(active_row.gvkey) else ""
            asset_prices = prices_by_asset.get(asset_id, empty_price_frame)
            asset_history = asset_prices[asset_prices["date"] <= signal_date].copy() if not asset_prices.empty else asset_prices
            price_context = _price_context(asset_history, benchmark_returns, signal_date)
            fundamentals_context = _fundamental_context(fundamentals_by_gvkey, gvkey, signal_date)
            records.extend(
                _records_for_asset(
                    signal_date=signal_date,
                    tradable_date=tradable_date,
                    active_row=active_row,
                    asset_history=asset_history,
                    price_context=price_context,
                    fundamentals_context=fundamentals_context,
                )
            )

    exposures = pd.DataFrame(records, columns=_exposure_columns())
    exposure_panel_path = output_dir / "exposure_panel.csv"
    coverage_report_path = output_dir / "exposure_coverage_report.json"
    exposure_manifest_path = output_dir / "exposure_manifest.yaml"
    exposures.to_csv(exposure_panel_path, index=False)
    coverage_report = _build_coverage_report(exposures)
    coverage_report_path.write_text(json.dumps(coverage_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    exposure_manifest_path.write_text(
        yaml.safe_dump(
            _build_exposure_manifest(
                research_manifest_path=research_manifest_path,
                fundamentals_manifest_path=fundamentals_manifest_path,
                exposure_panel_path=exposure_panel_path,
                coverage_report_path=coverage_report_path,
            ),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return PITExposureStoreResult(
        exposure_panel_path=str(exposure_panel_path),
        coverage_report_path=str(coverage_report_path),
        exposure_manifest_path=str(exposure_manifest_path),
        exposure_count=len(exposures),
        date_count=int(exposures["date"].nunique()) if not exposures.empty else 0,
        asset_count=int(exposures["asset_id"].nunique()) if not exposures.empty else 0,
        production_approval=False,
        live_trading=False,
        direct_q2_entry=False,
    )


def _load_yaml(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"manifest must be a mapping: {path}")
    return payload


def _load_research_csv(manifest: Mapping[str, Any], manifest_path: Path, section: str) -> pd.DataFrame:
    payload = manifest.get(section)
    if not isinstance(payload, Mapping):
        raise ValueError(f"{section} section is required")
    raw_path = Path(str(payload.get("path", "")))
    path = _resolve_manifest_path(raw_path, manifest_path)
    return pd.read_csv(path, dtype={"asset_id": str, "permno": str, "gvkey": str})


def _load_fundamentals_csv(
    manifest: Mapping[str, Any],
    manifest_path: Path | None,
    key: str,
) -> pd.DataFrame:
    paths = manifest.get("paths")
    if not isinstance(paths, Mapping) or key not in paths or manifest_path is None:
        return pd.DataFrame()
    raw_path = Path(str(paths[key]))
    path = _resolve_manifest_path(raw_path, manifest_path)
    return pd.read_csv(path, dtype={"gvkey": str})


def _resolve_manifest_path(raw_path: Path, manifest_path: Path) -> Path:
    if raw_path.is_absolute():
        return raw_path
    if raw_path.exists():
        return raw_path
    return manifest_path.parent / raw_path


def _normalize_universe(universe: pd.DataFrame) -> pd.DataFrame:
    normalized = universe.copy()
    if "asset_id" not in normalized.columns:
        normalized["asset_id"] = normalized["permno"].astype(str)
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    if "gvkey" not in normalized.columns:
        normalized["gvkey"] = ""
    normalized["gvkey"] = normalized["gvkey"].fillna("").astype(str)
    for column in ("membership_start", "membership_end", "entry_date", "exit_date", "as_of_timestamp"):
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    if "membership_start" not in normalized.columns:
        normalized["membership_start"] = normalized.get("entry_date", pd.NaT)
    if "membership_end" not in normalized.columns:
        normalized["membership_end"] = normalized.get("exit_date", pd.NaT)
    normalized["membership_start"] = normalized["membership_start"].fillna(pd.Timestamp("1900-01-01"))
    normalized["membership_end"] = normalized["membership_end"].fillna(pd.Timestamp("2100-01-01"))
    for column, fallback in (("sector", "unknown_sector"), ("industry", "unknown_industry"), ("source", "unknown")):
        if column not in normalized.columns:
            normalized[column] = fallback
        normalized[column] = normalized[column].fillna(fallback).astype(str)
    return normalized.sort_values(["asset_id", "membership_start"])


def _normalize_prices(prices: pd.DataFrame) -> pd.DataFrame:
    normalized = prices.copy()
    if "asset_id" not in normalized.columns:
        normalized["asset_id"] = normalized["permno"].astype(str)
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    for column in ("adjusted_close", "volume", "dlycap", "shrout", "dlyprcvol", "return"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=["asset_id", "date", "adjusted_close"]).sort_values(["asset_id", "date"])
    if "return" in normalized.columns:
        normalized["daily_return"] = normalized["return"]
    else:
        normalized["daily_return"] = normalized.groupby("asset_id")["adjusted_close"].pct_change()
    if "dlyprcvol" in normalized.columns:
        normalized["dollar_volume"] = normalized["dlyprcvol"]
    else:
        normalized["dollar_volume"] = normalized["adjusted_close"] * normalized.get("volume", np.nan)
    return normalized


def _normalize_benchmark(benchmark: pd.DataFrame) -> pd.DataFrame:
    normalized = benchmark.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["adjusted_close"] = pd.to_numeric(normalized["adjusted_close"], errors="coerce")
    if "return" in normalized.columns:
        normalized["benchmark_daily_return"] = pd.to_numeric(normalized["return"], errors="coerce")
    else:
        normalized["benchmark_daily_return"] = normalized["adjusted_close"].pct_change()
    return normalized.dropna(subset=["date", "adjusted_close"]).sort_values("date")


def _normalize_fundamentals(fundamentals: pd.DataFrame) -> pd.DataFrame:
    if fundamentals.empty:
        return fundamentals
    normalized = fundamentals.copy()
    normalized["gvkey"] = normalized["gvkey"].astype(str)
    normalized["datadate"] = pd.to_datetime(normalized["datadate"], errors="coerce")
    normalized["visibility_timestamp"] = pd.to_datetime(normalized["visibility_timestamp"], errors="coerce")
    normalized["tradable_timestamp"] = pd.to_datetime(normalized.get("tradable_timestamp"), errors="coerce")
    for column in ("atq", "ceqq", "seqq", "txditcq", "pstkq", "saleq", "niq", "oibdpq"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized.dropna(subset=["gvkey", "datadate", "visibility_timestamp"]).sort_values(
        ["gvkey", "visibility_timestamp", "datadate"]
    )


def _eligible_month_end_dates(trading_dates: list[pd.Timestamp]) -> list[pd.Timestamp]:
    if not trading_dates:
        return []
    frame = pd.DataFrame({"date": trading_dates})
    frame["month"] = frame["date"].dt.to_period("M")
    month_ends = list(frame.groupby("month")["date"].max())
    date_index = {date: index for index, date in enumerate(trading_dates)}
    return [date for date in month_ends if date_index.get(date, 0) >= 252]


def _next_trading_date(trading_dates: list[pd.Timestamp], signal_date: pd.Timestamp) -> pd.Timestamp | None:
    try:
        position = trading_dates.index(signal_date)
    except ValueError:
        return None
    next_position = position + 1
    if next_position >= len(trading_dates):
        return None
    return trading_dates[next_position]


def _active_universe(universe: pd.DataFrame, signal_date: pd.Timestamp) -> pd.DataFrame:
    active = universe[
        (universe["membership_start"] <= signal_date)
        & (universe["membership_end"] >= signal_date)
    ].copy()
    if active.empty:
        return active
    return active.sort_values(["asset_id", "membership_start"]).drop_duplicates("asset_id", keep="last")


def _price_context(asset_history: pd.DataFrame, benchmark_returns: pd.DataFrame, signal_date: pd.Timestamp) -> dict[str, object]:
    history = asset_history[asset_history["date"] <= signal_date].sort_values("date")
    if history.empty:
        return {"history": history, "merged_returns": pd.DataFrame(), "market_cap": np.nan}
    market_cap = _latest_market_cap(history)
    asset_returns = history[["date", "daily_return"]].dropna()
    merged_returns = asset_returns.merge(benchmark_returns, on="date", how="inner").dropna()
    return {"history": history, "merged_returns": merged_returns, "market_cap": market_cap}


def _latest_market_cap(history: pd.DataFrame) -> float:
    if "dlycap" in history.columns:
        value = pd.to_numeric(history["dlycap"], errors="coerce").dropna()
        if not value.empty:
            return float(value.iloc[-1])
    if {"shrout", "adjusted_close"} <= set(history.columns):
        shrout = pd.to_numeric(history["shrout"], errors="coerce").dropna()
        close = pd.to_numeric(history["adjusted_close"], errors="coerce").dropna()
        if not shrout.empty and not close.empty:
            return float(shrout.iloc[-1] * close.iloc[-1])
    return np.nan


def _fundamental_context(
    fundamentals_by_gvkey: Mapping[str, pd.DataFrame],
    gvkey: str,
    signal_date: pd.Timestamp,
) -> dict[str, object]:
    if not fundamentals_by_gvkey or not gvkey:
        return {"latest": None, "previous": None}
    fundamentals = fundamentals_by_gvkey.get(gvkey)
    if fundamentals is None or fundamentals.empty:
        return {"latest": None, "previous": None}
    visible = fundamentals[fundamentals["visibility_timestamp"] <= signal_date]
    if visible.empty:
        return {"latest": None, "previous": None}
    latest = visible.iloc[-1]
    previous = visible.iloc[-2] if len(visible) >= 2 else None
    return {"latest": latest, "previous": previous}


def _records_for_asset(
    signal_date: pd.Timestamp,
    tradable_date: pd.Timestamp,
    active_row: object,
    asset_history: pd.DataFrame,
    price_context: Mapping[str, object],
    fundamentals_context: Mapping[str, object],
) -> list[dict[str, object]]:
    asset_id = str(getattr(active_row, "asset_id"))
    records = [
        _covered_record(
            signal_date,
            tradable_date,
            asset_id,
            "sector",
            str(getattr(active_row, "sector")),
            getattr(active_row, "membership_start"),
            signal_date,
            str(getattr(active_row, "source")),
        ),
        _covered_record(
            signal_date,
            tradable_date,
            asset_id,
            "industry",
            str(getattr(active_row, "industry")),
            getattr(active_row, "membership_start"),
            signal_date,
            str(getattr(active_row, "source")),
        ),
    ]
    records.extend(_price_exposure_records(signal_date, tradable_date, asset_id, asset_history, price_context))
    records.extend(_fundamental_exposure_records(signal_date, tradable_date, asset_id, price_context, fundamentals_context))
    return records


def _price_exposure_records(
    signal_date: pd.Timestamp,
    tradable_date: pd.Timestamp,
    asset_id: str,
    asset_history: pd.DataFrame,
    price_context: Mapping[str, object],
) -> list[dict[str, object]]:
    history = price_context["history"]
    assert isinstance(history, pd.DataFrame)
    merged_returns = price_context["merged_returns"]
    assert isinstance(merged_returns, pd.DataFrame)
    market_cap = float(price_context["market_cap"]) if pd.notna(price_context["market_cap"]) else np.nan
    records: list[dict[str, object]] = []

    beta_window = merged_returns.tail(252)
    if len(beta_window) >= MIN_BETA_OBSERVATIONS and float(beta_window["benchmark_daily_return"].var(ddof=0)) > 0.0:
        beta = float(
            np.cov(
                beta_window["daily_return"].to_numpy(dtype=float),
                beta_window["benchmark_daily_return"].to_numpy(dtype=float),
                ddof=0,
            )[0, 1]
            / float(beta_window["benchmark_daily_return"].var(ddof=0))
        )
        records.append(
            _covered_record(
                signal_date,
                tradable_date,
                asset_id,
                "trailing_market_beta_252d",
                beta,
                signal_date,
                signal_date,
                "crsp_daily_trailing_252d",
                lookback_start=beta_window["date"].iloc[0],
                lookback_end=beta_window["date"].iloc[-1],
            )
        )
    else:
        records.append(_missing_record(signal_date, tradable_date, asset_id, "trailing_market_beta_252d", "insufficient_trailing_price_history"))

    if pd.notna(market_cap) and market_cap > 0.0:
        records.append(
            _covered_record(
                signal_date,
                tradable_date,
                asset_id,
                "log_market_cap",
                float(np.log(market_cap)),
                signal_date,
                signal_date,
                "crsp_daily_market_cap",
            )
        )
    else:
        records.append(_missing_record(signal_date, tradable_date, asset_id, "log_market_cap", "missing_market_cap"))

    liquidity_window = history.tail(60)
    dollar_volume = pd.to_numeric(liquidity_window.get("dollar_volume"), errors="coerce").dropna()
    if len(dollar_volume) >= 20:
        records.append(
            _covered_record(
                signal_date,
                tradable_date,
                asset_id,
                "liquidity_adv_60d",
                float(dollar_volume.mean()),
                signal_date,
                signal_date,
                "crsp_daily_trailing_60d",
                lookback_start=liquidity_window["date"].iloc[0],
                lookback_end=liquidity_window["date"].iloc[-1],
            )
        )
    else:
        records.append(_missing_record(signal_date, tradable_date, asset_id, "liquidity_adv_60d", "insufficient_trailing_price_history"))

    residual_window = merged_returns.tail(60)
    if len(residual_window) >= 40 and float(residual_window["benchmark_daily_return"].var(ddof=0)) > 0.0:
        beta = np.cov(
            residual_window["daily_return"].to_numpy(dtype=float),
            residual_window["benchmark_daily_return"].to_numpy(dtype=float),
            ddof=0,
        )[0, 1] / float(residual_window["benchmark_daily_return"].var(ddof=0))
        residual = residual_window["daily_return"].to_numpy(dtype=float) - beta * residual_window["benchmark_daily_return"].to_numpy(dtype=float)
        records.append(
            _covered_record(
                signal_date,
                tradable_date,
                asset_id,
                "residual_volatility_60d",
                float(np.std(residual, ddof=0)),
                signal_date,
                signal_date,
                "crsp_daily_trailing_60d",
                lookback_start=residual_window["date"].iloc[0],
                lookback_end=residual_window["date"].iloc[-1],
            )
        )
    else:
        records.append(_missing_record(signal_date, tradable_date, asset_id, "residual_volatility_60d", "insufficient_trailing_price_history"))

    close = pd.to_numeric(history["adjusted_close"], errors="coerce").dropna().reset_index(drop=True)
    if len(close) >= 6:
        records.append(
            _covered_record(
                signal_date,
                tradable_date,
                asset_id,
                "short_term_reversal_5d",
                -float(close.iloc[-1] / close.iloc[-6] - 1.0),
                signal_date,
                signal_date,
                "crsp_daily_trailing_5d",
                lookback_start=history["date"].iloc[-6],
                lookback_end=history["date"].iloc[-1],
            )
        )
    else:
        records.append(_missing_record(signal_date, tradable_date, asset_id, "short_term_reversal_5d", "insufficient_trailing_price_history"))

    if len(close) >= 253:
        records.append(
            _covered_record(
                signal_date,
                tradable_date,
                asset_id,
                "medium_term_momentum_12_1",
                float(close.iloc[-22] / close.iloc[-253] - 1.0),
                signal_date,
                signal_date,
                "crsp_daily_trailing_252d_skip_21d",
                lookback_start=history["date"].iloc[-253],
                lookback_end=history["date"].iloc[-22],
            )
        )
    else:
        records.append(_missing_record(signal_date, tradable_date, asset_id, "medium_term_momentum_12_1", "insufficient_trailing_price_history"))
    return records


def _fundamental_exposure_records(
    signal_date: pd.Timestamp,
    tradable_date: pd.Timestamp,
    asset_id: str,
    price_context: Mapping[str, object],
    fundamentals_context: Mapping[str, object],
) -> list[dict[str, object]]:
    latest = fundamentals_context.get("latest")
    previous = fundamentals_context.get("previous")
    if latest is None:
        return [
            _missing_record(signal_date, tradable_date, asset_id, exposure, "no_visible_fundamental_row")
            for exposure in FUNDAMENTAL_EXPOSURES
        ]
    assert isinstance(latest, pd.Series)
    market_cap = float(price_context["market_cap"]) if pd.notna(price_context["market_cap"]) else np.nan
    book_equity = _first_numeric(latest, ("ceqq", "seqq"))
    total_assets = _first_numeric(latest, ("atq",))
    net_income = _first_numeric(latest, ("niq", "oibdpq"))
    records: list[dict[str, object]] = []
    records.append(
        _fundamental_record(
            signal_date,
            tradable_date,
            asset_id,
            "fundamental_book_to_market",
            book_equity / market_cap if pd.notna(book_equity) and pd.notna(market_cap) and market_cap > 0.0 else np.nan,
            latest,
            "missing_book_equity_or_market_cap",
        )
    )
    records.append(
        _fundamental_record(
            signal_date,
            tradable_date,
            asset_id,
            "fundamental_profitability_roa",
            net_income / total_assets if pd.notna(net_income) and pd.notna(total_assets) and total_assets != 0.0 else np.nan,
            latest,
            "missing_profitability_inputs",
        )
    )
    if previous is not None and isinstance(previous, pd.Series):
        previous_assets = _first_numeric(previous, ("atq",))
        growth = total_assets / previous_assets - 1.0 if pd.notna(total_assets) and pd.notna(previous_assets) and previous_assets != 0.0 else np.nan
        missing_reason = "missing_asset_growth_inputs"
    else:
        growth = np.nan
        missing_reason = "missing_prior_visible_fundamental_row"
    records.append(
        _fundamental_record(
            signal_date,
            tradable_date,
            asset_id,
            "fundamental_asset_growth",
            growth,
            latest,
            missing_reason,
        )
    )
    return records


def _fundamental_record(
    signal_date: pd.Timestamp,
    tradable_date: pd.Timestamp,
    asset_id: str,
    exposure_name: str,
    value: float,
    latest: pd.Series,
    missing_reason: str,
) -> dict[str, object]:
    if pd.notna(value):
        return _covered_record(
            signal_date,
            tradable_date,
            asset_id,
            exposure_name,
            float(value),
            latest["datadate"],
            latest["visibility_timestamp"],
            "wrds_comp_fundq",
        )
    return _missing_record(signal_date, tradable_date, asset_id, exposure_name, missing_reason)


def _first_numeric(row: pd.Series, columns: tuple[str, ...]) -> float:
    for column in columns:
        if column in row and pd.notna(row[column]):
            return float(row[column])
    return np.nan


def _covered_record(
    signal_date: pd.Timestamp,
    tradable_date: pd.Timestamp,
    asset_id: str,
    exposure_name: str,
    exposure_value: object,
    exposure_date: object,
    visibility_timestamp: object,
    source: str,
    lookback_start: object | None = None,
    lookback_end: object | None = None,
) -> dict[str, object]:
    return {
        "schema_version": "pit_exposure.v1",
        "date": _date_str(signal_date),
        "asset_id": asset_id,
        "exposure_name": exposure_name,
        "exposure_value": exposure_value,
        "exposure_date": _date_str(exposure_date),
        "visibility_timestamp": _date_str(visibility_timestamp),
        "tradable_timestamp": _date_str(tradable_date),
        "source": source,
        "coverage_flag": True,
        "abstain_reason": "",
        "lookback_start_date": _date_str(lookback_start) if lookback_start is not None else "",
        "lookback_end_date": _date_str(lookback_end) if lookback_end is not None else "",
        "not_alpha_evidence": True,
        "same_close_trading_used": False,
    }


def _missing_record(
    signal_date: pd.Timestamp,
    tradable_date: pd.Timestamp,
    asset_id: str,
    exposure_name: str,
    abstain_reason: str,
) -> dict[str, object]:
    return {
        "schema_version": "pit_exposure.v1",
        "date": _date_str(signal_date),
        "asset_id": asset_id,
        "exposure_name": exposure_name,
        "exposure_value": np.nan,
        "exposure_date": _date_str(signal_date),
        "visibility_timestamp": _date_str(signal_date),
        "tradable_timestamp": _date_str(tradable_date),
        "source": "unavailable",
        "coverage_flag": False,
        "abstain_reason": abstain_reason,
        "lookback_start_date": "",
        "lookback_end_date": "",
        "not_alpha_evidence": True,
        "same_close_trading_used": False,
    }


def _date_str(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).date().isoformat()


def _exposure_columns() -> list[str]:
    return [
        "schema_version",
        "date",
        "asset_id",
        "exposure_name",
        "exposure_value",
        "exposure_date",
        "visibility_timestamp",
        "tradable_timestamp",
        "source",
        "coverage_flag",
        "abstain_reason",
        "lookback_start_date",
        "lookback_end_date",
        "not_alpha_evidence",
        "same_close_trading_used",
    ]


def _build_coverage_report(exposures: pd.DataFrame) -> dict[str, object]:
    coverage_by_exposure: dict[str, dict[str, object]] = {}
    for exposure_name, group in exposures.groupby("exposure_name", sort=True):
        covered = group[group["coverage_flag"].astype(bool)]
        coverage_by_exposure[str(exposure_name)] = {
            "total_rows": int(len(group)),
            "covered_rows": int(len(covered)),
            "abstain_rows": int(len(group) - len(covered)),
            "coverage_ratio": round(float(len(covered) / len(group)), 6) if len(group) else 0.0,
            "abstain_reasons": sorted(reason for reason in group["abstain_reason"].dropna().astype(str).unique() if reason),
        }
    return {
        "schema_version": "pit_exposure_coverage_report.v1",
        "date_count": int(exposures["date"].nunique()) if not exposures.empty else 0,
        "asset_count": int(exposures["asset_id"].nunique()) if not exposures.empty else 0,
        "exposure_count": int(len(exposures)),
        "expected_exposures": list(EXPECTED_EXPOSURES),
        "coverage_by_exposure": coverage_by_exposure,
        "non_claims": _non_claims(),
    }


def _build_exposure_manifest(
    research_manifest_path: Path,
    fundamentals_manifest_path: Path | None,
    exposure_panel_path: Path,
    coverage_report_path: Path,
) -> dict[str, object]:
    return {
        "schema_version": "pit_exposure_store_manifest.v1",
        "allowed_use_mode": "risk_attribution_input_only",
        "inputs": {
            "research_manifest": str(research_manifest_path),
            "fundamentals_manifest": str(fundamentals_manifest_path) if fundamentals_manifest_path else None,
        },
        "outputs": {
            "exposure_panel": str(exposure_panel_path),
            "coverage_report": str(coverage_report_path),
        },
        "expected_exposures": list(EXPECTED_EXPOSURES),
        "timestamp_policy": {
            "price_volume_exposures": "trailing_windows_visible_at_signal_date_only",
            "fundamental_exposures": "latest_quarterly_row_with_visibility_timestamp_on_or_before_signal_date",
            "tradable_timestamp": "next_trading_session_after_signal_date",
            "same_close_trading": False,
        },
        "terminology": {
            "limited_proxy_residual": "positive residual after configured proxy controls only",
            "forbidden_claim": "limited proxy residual is not style neutral alpha",
        },
        "non_claims": _non_claims(),
    }


def _non_claims() -> dict[str, bool]:
    return {
        "not_alpha_evidence": True,
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
        "allocator_entry": False,
    }
