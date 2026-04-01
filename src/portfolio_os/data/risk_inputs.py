"""Risk-model input loaders."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from portfolio_os.data.loaders import ensure_columns, normalize_ticker
from portfolio_os.domain.errors import InputValidationError


def load_returns_long(
    path: str | Path,
    *,
    required_tickers: Iterable[str],
    lookback_days: int,
    min_history_days: int,
) -> pd.DataFrame:
    """Load long-format returns and return a date x ticker matrix."""

    frame = pd.read_csv(Path(path), converters={"ticker": lambda value: str(value).strip()})
    ensure_columns(frame, ["date", "ticker", "return"], str(path))
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    if frame["date"].isna().any():
        raise InputValidationError("returns_long.csv contains invalid date values.")
    frame["return"] = pd.to_numeric(frame["return"], errors="coerce")
    if frame["return"].isna().any():
        raise InputValidationError("returns_long.csv contains non-numeric return values.")

    duplicated = frame.duplicated(subset=["date", "ticker"], keep=False)
    if duplicated.any():
        duplicated_rows = frame.loc[duplicated, ["date", "ticker"]].head(5).to_dict(orient="records")
        raise InputValidationError(
            "returns_long.csv contains duplicate (date, ticker) rows, e.g. "
            + ", ".join(f"({item['date'].date()}, {item['ticker']})" for item in duplicated_rows)
        )

    ordered_tickers = [str(ticker) for ticker in required_tickers]
    available_tickers = set(frame["ticker"].tolist())
    missing_tickers = sorted(set(ordered_tickers) - available_tickers)
    if missing_tickers:
        raise InputValidationError(
            "returns_long.csv is missing optimizer ticker(s): " + ", ".join(missing_tickers)
        )

    filtered = frame[frame["ticker"].isin(ordered_tickers)].copy()
    pivot = filtered.pivot(index="date", columns="ticker", values="return").sort_index()
    pivot = pivot.reindex(columns=ordered_tickers)
    if lookback_days > 0:
        pivot = pivot.tail(int(lookback_days))
    pivot = pivot.dropna(axis=0, how="any")
    if len(pivot) < int(min_history_days):
        raise InputValidationError(
            f"returns_long.csv has only {len(pivot)} complete history rows after alignment; "
            f"requires at least {min_history_days}."
        )
    return pivot


def load_factor_exposures(
    path: str | Path,
    *,
    required_tickers: Iterable[str],
) -> pd.DataFrame:
    """Load long-format factor exposures and return a ticker x factor matrix."""

    frame = pd.read_csv(Path(path), converters={"ticker": lambda value: str(value).strip()})
    ensure_columns(frame, ["ticker", "factor", "exposure"], str(path))
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    frame["factor"] = frame["factor"].map(lambda value: str(value).strip())
    if (frame["factor"] == "").any():
        raise InputValidationError("factor_exposure.csv contains empty factor names.")
    frame["exposure"] = pd.to_numeric(frame["exposure"], errors="coerce")
    if frame["exposure"].isna().any():
        raise InputValidationError("factor_exposure.csv contains non-numeric exposure values.")

    duplicated = frame.duplicated(subset=["ticker", "factor"], keep=False)
    if duplicated.any():
        duplicated_rows = frame.loc[duplicated, ["ticker", "factor"]].head(5).to_dict(orient="records")
        raise InputValidationError(
            "factor_exposure.csv contains duplicate (ticker, factor) rows, e.g. "
            + ", ".join(f"({item['ticker']}, {item['factor']})" for item in duplicated_rows)
        )

    ordered_tickers = [str(ticker) for ticker in required_tickers]
    available_tickers = set(frame["ticker"].tolist())
    missing_tickers = sorted(set(ordered_tickers) - available_tickers)
    if missing_tickers:
        raise InputValidationError(
            "factor_exposure.csv is missing optimizer ticker(s): " + ", ".join(missing_tickers)
        )

    filtered = frame[frame["ticker"].isin(ordered_tickers)].copy()
    pivot = filtered.pivot(index="ticker", columns="factor", values="exposure")
    pivot = pivot.reindex(index=ordered_tickers)
    if pivot.isna().any().any():
        missing_entries = pivot.isna()
        missing_pairs: list[str] = []
        for ticker, row in missing_entries.iterrows():
            missing_factors = [str(factor) for factor, is_missing in row.items() if bool(is_missing)]
            for factor in missing_factors:
                missing_pairs.append(f"({ticker}, {factor})")
                if len(missing_pairs) >= 5:
                    break
            if len(missing_pairs) >= 5:
                break
        raise InputValidationError(
            "factor_exposure.csv is missing exposure entries for required ticker/factor pairs, e.g. "
            + ", ".join(missing_pairs)
        )
    return pivot
