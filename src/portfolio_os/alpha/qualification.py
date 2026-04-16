"""Week 2 candidate qualification helpers for the US alpha core restart."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from portfolio_os.cost.fee import estimate_fee_array
from portfolio_os.cost.slippage import estimate_slippage_array
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.storage.snapshots import write_json, write_text
from portfolio_os.utils.config import FeeConfig, SlippageConfig, load_yaml_file


_FAMILY_ID = "A"
_BASELINE_ID = "alt_momentum_4_1"
_LIQUIDITY_CUT_FRACTION = 0.20
_NAV_NOTIONAL = 1_300_000.0
_MARKET_BETA_LOOKBACK = 126
_MARKET_BETA_MIN_PAIRS = 63
_MOMENTUM_LOOKBACK_DAYS = 84
_MOMENTUM_SKIP_DAYS = 21
_VOLATILITY_LOOKBACK_DAYS = 63
_VOLATILITY_FLOOR = 0.20
_IDIOSYNCRATIC_VOL_LOOKBACK_DAYS = 63
_MAX_LOOKBACK_DAYS = 21
_ILLIQUIDITY_LEVEL_LOOKBACK_DAYS = 63
_ILLIQUIDITY_SHORT_LOOKBACK_DAYS = 21
_SHORT_REVERSAL_LOOKBACK_DAYS = 5
_ANNUALIZATION_FACTOR = 252.0
_QUANTILES = 5
_MIN_EFFECTIVE_NAMES = 5


@dataclass(frozen=True)
class AlphaCoreCandidateDefinition:
    candidate_id: str
    family_id: str
    candidate_name: str
    description: str


@dataclass
class AlphaCoreQualificationResult:
    candidate: AlphaCoreCandidateDefinition
    output_dir: Path
    summary_payload: dict[str, Any]


@dataclass(frozen=True)
class _FamilyAInputs:
    returns_panel: pd.DataFrame
    price_panel: pd.DataFrame
    market_returns: pd.Series
    market_price: pd.Series
    decision_dates: list[pd.Timestamp]
    next_date_map: dict[pd.Timestamp, pd.Timestamp]
    raw_momentum: pd.DataFrame
    beta_frame: pd.DataFrame
    market_momentum: pd.Series
    realized_volatility: pd.DataFrame
    universe_reference: pd.DataFrame


FAMILY_A_DEFINITIONS: dict[str, AlphaCoreCandidateDefinition] = {
    "A1": AlphaCoreCandidateDefinition(
        candidate_id="A1",
        family_id=_FAMILY_ID,
        candidate_name="Market-Residual 84/21 Momentum",
        description="Raw 84/21 momentum residualized by trailing market beta times market 84/21 momentum.",
    ),
    "A2": AlphaCoreCandidateDefinition(
        candidate_id="A2",
        family_id=_FAMILY_ID,
        candidate_name="Sector-Residual 84/21 Momentum",
        description="Raw 84/21 momentum residualized against the static-sector median on each decision date.",
    ),
    "A3": AlphaCoreCandidateDefinition(
        candidate_id="A3",
        family_id=_FAMILY_ID,
        candidate_name="Vol-Managed Residual Momentum",
        description="A1 scaled by trailing 63-day annualized volatility with a 20% floor.",
    ),
}

FAMILY_B_DEFINITIONS: dict[str, AlphaCoreCandidateDefinition] = {
    "B1": AlphaCoreCandidateDefinition(
        candidate_id="B1",
        family_id="B",
        candidate_name="Amihud Illiquidity Level",
        description="Negative trailing 63-day median daily Amihud illiquidity using daily dollar volume.",
    ),
    "B2": AlphaCoreCandidateDefinition(
        candidate_id="B2",
        family_id="B",
        candidate_name="Illiquidity Shock / Change",
        description="Negative 21-day versus 63-day illiquidity shock using daily dollar volume.",
    ),
    "B3": AlphaCoreCandidateDefinition(
        candidate_id="B3",
        family_id="B",
        candidate_name="Abnormal-Turnover-Conditioned Short-Term Reversal",
        description="Five-day reversal conditioned on abnormal 5-day versus 63-day dollar volume.",
    ),
}

FAMILY_C_DEFINITIONS: dict[str, AlphaCoreCandidateDefinition] = {
    "C1": AlphaCoreCandidateDefinition(
        candidate_id="C1",
        family_id="C",
        candidate_name="Idiosyncratic Volatility",
        description="Negative trailing 63-day annualized idiosyncratic volatility from a daily market model.",
    ),
    "C2": AlphaCoreCandidateDefinition(
        candidate_id="C2",
        family_id="C",
        candidate_name="MAX Effect / Lottery Proxy",
        description="Negative trailing 21-day maximum daily return as a low-lottery cross-sectional signal.",
    ),
}

ALL_CANDIDATE_DEFINITIONS: dict[str, AlphaCoreCandidateDefinition] = {
    **FAMILY_A_DEFINITIONS,
    **FAMILY_B_DEFINITIONS,
    **FAMILY_C_DEFINITIONS,
}


def _safe_spearman(left: pd.Series, right: pd.Series) -> float:
    clean = pd.concat(
        [
            pd.to_numeric(left, errors="coerce"),
            pd.to_numeric(right, errors="coerce"),
        ],
        axis=1,
    ).dropna()
    if len(clean) < 2:
        return float("nan")
    if clean.iloc[:, 0].nunique() < 2 or clean.iloc[:, 1].nunique() < 2:
        return float("nan")
    return float(clean.iloc[:, 0].corr(clean.iloc[:, 1], method="spearman"))


def _mean_t_stat(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(clean) < 2:
        return 0.0
    std = float(clean.std(ddof=1))
    if std <= 0.0:
        return 0.0
    return float(clean.mean() / (std / np.sqrt(float(len(clean)))))


def _mean_ir(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(clean) < 2:
        return 0.0
    std = float(clean.std(ddof=1))
    if std <= 0.0:
        return 0.0
    return float(clean.mean() / std)


def _relative_improvement(candidate_value: float, baseline_value: float) -> float:
    if not np.isfinite(candidate_value) or not np.isfinite(baseline_value):
        return 0.0
    if baseline_value <= 0.0:
        return 0.0
    return float(candidate_value / baseline_value - 1.0)


def _quantile_buckets(scores: pd.Series, *, quantiles: int = _QUANTILES) -> pd.Series:
    ranked = scores.rank(method="first", pct=True)
    return np.ceil(ranked * quantiles).clip(1, quantiles).astype(int)


def _infer_anchor_close_column(universe_reference: pd.DataFrame) -> str:
    close_columns = [column for column in universe_reference.columns if str(column).startswith("close_")]
    if not close_columns:
        raise InputValidationError("Universe reference file must contain at least one close_* anchor price column.")
    return sorted(close_columns)[-1]


def _load_universe_reference(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    return _normalize_universe_reference(frame)


def _normalize_universe_reference(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"ticker", "sector", "avg_adv_20d"}
    missing = required - set(frame.columns)
    if missing:
        raise InputValidationError(
            "Universe reference file is missing required columns: " + ", ".join(sorted(missing))
        )
    if "anchor_close" in frame.columns:
        frame = frame.loc[:, ["ticker", "sector", "avg_adv_20d", "anchor_close"]].copy()
    else:
        anchor_column = _infer_anchor_close_column(frame)
        frame = frame.loc[:, ["ticker", "sector", "avg_adv_20d", anchor_column]].copy()
        frame = frame.rename(columns={anchor_column: "anchor_close"})
    frame["ticker"] = frame["ticker"].astype(str)
    frame["sector"] = frame["sector"].astype(str)
    frame["avg_adv_20d"] = pd.to_numeric(frame["avg_adv_20d"], errors="coerce")
    frame["anchor_close"] = pd.to_numeric(frame["anchor_close"], errors="coerce")
    frame = frame.dropna(subset=["avg_adv_20d", "anchor_close"]).sort_values("ticker").reset_index(drop=True)
    if frame.empty:
        raise InputValidationError("Universe reference file produced an empty ticker reference frame.")
    return frame


def _normalize_liquidity_long(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "ticker", "dollar_volume"}
    missing = required - set(frame.columns)
    if missing:
        raise InputValidationError(
            "Liquidity file is missing required columns: " + ", ".join(sorted(missing))
        )
    work = frame.loc[:, ["date", "ticker", "dollar_volume"]].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["ticker"] = work["ticker"].astype(str)
    work["dollar_volume"] = pd.to_numeric(work["dollar_volume"], errors="coerce")
    work = work.dropna(subset=["date", "ticker", "dollar_volume"]).copy()
    duplicated = work.duplicated(subset=["date", "ticker"], keep=False)
    if duplicated.any():
        raise InputValidationError("Liquidity file contains duplicate (date, ticker) rows.")
    if work.empty:
        raise InputValidationError("Liquidity file produced an empty dollar-volume frame.")
    return work.sort_values(["date", "ticker"]).reset_index(drop=True)


def _load_liquidity_long(path: str | Path) -> pd.DataFrame:
    return _normalize_liquidity_long(pd.read_csv(path))


def _reconstruct_price_panel(
    returns_panel: pd.DataFrame,
    *,
    anchor_prices: pd.Series,
) -> pd.DataFrame:
    ordered_anchor = anchor_prices.reindex(returns_panel.columns)
    relative_prices = (1.0 + returns_panel.fillna(0.0)).cumprod()
    scale = ordered_anchor.astype(float) / relative_prices.iloc[-1].astype(float)
    price_panel = relative_prices.mul(scale, axis=1)
    price_panel.index = pd.to_datetime(price_panel.index).normalize()
    price_panel.index.name = "date"
    return price_panel


def _build_month_end_grid(price_panel: pd.DataFrame) -> tuple[list[pd.Timestamp], dict[pd.Timestamp, pd.Timestamp]]:
    all_month_ends = (
        price_panel.index.to_series()
        .groupby(price_panel.index.to_period("M"))
        .max()
        .sort_values()
        .tolist()
    )
    if len(all_month_ends) < 2:
        raise InputValidationError("Price history does not span enough months for Week 2 qualification.")
    decision_dates = [pd.Timestamp(item).normalize() for item in all_month_ends[:-1]]
    next_date_map = {
        pd.Timestamp(left).normalize(): pd.Timestamp(right).normalize()
        for left, right in zip(all_month_ends[:-1], all_month_ends[1:], strict=True)
    }
    return decision_dates, next_date_map


def _prepare_family_a_inputs(
    returns_panel: pd.DataFrame,
    *,
    universe_reference: pd.DataFrame,
) -> _FamilyAInputs:
    ordered_reference = universe_reference.set_index("ticker").reindex(returns_panel.columns)
    if ordered_reference["anchor_close"].isna().any():
        missing = ordered_reference.loc[ordered_reference["anchor_close"].isna()].index.tolist()
        raise InputValidationError("Missing anchor_close reference for ticker(s): " + ", ".join(sorted(missing)))

    price_panel = _reconstruct_price_panel(
        returns_panel,
        anchor_prices=ordered_reference["anchor_close"],
    )
    decision_dates, next_date_map = _build_month_end_grid(price_panel)

    market_returns = returns_panel.mean(axis=1, skipna=True).astype(float)
    market_price = (1.0 + market_returns.fillna(0.0)).cumprod()
    raw_momentum = price_panel.shift(_MOMENTUM_SKIP_DAYS) / price_panel.shift(_MOMENTUM_LOOKBACK_DAYS) - 1.0
    market_momentum = market_price.shift(_MOMENTUM_SKIP_DAYS) / market_price.shift(_MOMENTUM_LOOKBACK_DAYS) - 1.0

    shifted_market = market_returns.shift(_MOMENTUM_SKIP_DAYS)
    market_variance = shifted_market.rolling(
        window=_MARKET_BETA_LOOKBACK,
        min_periods=_MARKET_BETA_MIN_PAIRS,
    ).var()
    beta_by_ticker: dict[str, pd.Series] = {}
    for ticker in returns_panel.columns:
        shifted_stock = returns_panel[str(ticker)].shift(_MOMENTUM_SKIP_DAYS)
        covariance = shifted_stock.rolling(
            window=_MARKET_BETA_LOOKBACK,
            min_periods=_MARKET_BETA_MIN_PAIRS,
        ).cov(shifted_market)
        beta_by_ticker[str(ticker)] = covariance / market_variance.replace(0.0, np.nan)
    beta_frame = pd.DataFrame(beta_by_ticker, index=returns_panel.index).reindex(columns=returns_panel.columns)

    realized_volatility = (
        returns_panel.rolling(window=_VOLATILITY_LOOKBACK_DAYS, min_periods=_VOLATILITY_LOOKBACK_DAYS).std()
        * np.sqrt(_ANNUALIZATION_FACTOR)
    )

    return _FamilyAInputs(
        returns_panel=returns_panel,
        price_panel=price_panel,
        market_returns=market_returns,
        market_price=market_price,
        decision_dates=decision_dates,
        next_date_map=next_date_map,
        raw_momentum=raw_momentum,
        beta_frame=beta_frame,
        market_momentum=market_momentum,
        realized_volatility=realized_volatility,
        universe_reference=ordered_reference.reset_index(),
    )


def _build_baseline_signal_frame(inputs: _FamilyAInputs) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for decision_date in inputs.decision_dates:
        momentum_row = inputs.raw_momentum.loc[decision_date]
        for ticker, signal_value in momentum_row.items():
            rows.append(
                {
                    "date": decision_date.strftime("%Y-%m-%d"),
                    "ticker": str(ticker),
                    "signal_value": float(signal_value) if pd.notna(signal_value) else np.nan,
                }
            )
    return pd.DataFrame(rows).dropna(subset=["signal_value"]).sort_values(["date", "ticker"]).reset_index(drop=True)


def build_family_a_monthly_signal_frame(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    candidate_id: str,
) -> pd.DataFrame:
    if candidate_id not in FAMILY_A_DEFINITIONS:
        raise InputValidationError(f"Unsupported Family A candidate_id: {candidate_id}")
    inputs = _prepare_family_a_inputs(
        returns_panel,
        universe_reference=_normalize_universe_reference(universe_reference.copy()),
    )
    ordered_reference = inputs.universe_reference.set_index("ticker")

    rows: list[dict[str, object]] = []
    for decision_date in inputs.decision_dates:
        raw_row = inputs.raw_momentum.loc[decision_date]
        if candidate_id == "A1":
            signal_row = raw_row - inputs.beta_frame.loc[decision_date] * float(inputs.market_momentum.loc[decision_date])
        elif candidate_id == "A2":
            work = pd.DataFrame(
                {
                    "ticker": raw_row.index.astype(str),
                    "raw_momentum": raw_row.to_numpy(),
                }
            )
            work["sector"] = work["ticker"].map(ordered_reference["sector"])
            work["sector_median"] = work.groupby("sector")["raw_momentum"].transform("median")
            signal_row = pd.Series(
                work["raw_momentum"].to_numpy() - work["sector_median"].to_numpy(),
                index=raw_row.index,
                dtype=float,
            )
        else:
            residual_row = raw_row - inputs.beta_frame.loc[decision_date] * float(inputs.market_momentum.loc[decision_date])
            volatility_row = inputs.realized_volatility.loc[decision_date].clip(lower=_VOLATILITY_FLOOR)
            signal_row = residual_row / volatility_row

        for ticker, signal_value in signal_row.items():
            rows.append(
                {
                    "date": decision_date.strftime("%Y-%m-%d"),
                    "ticker": str(ticker),
                    "signal_value": float(signal_value) if pd.notna(signal_value) else np.nan,
                }
            )
    frame = pd.DataFrame(rows).dropna(subset=["signal_value"]).sort_values(["date", "ticker"]).reset_index(drop=True)
    if frame.empty:
        raise InputValidationError(f"Candidate {candidate_id} produced an empty monthly signal frame.")
    return frame


def build_family_c_monthly_signal_frame(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    candidate_id: str,
) -> pd.DataFrame:
    if candidate_id not in FAMILY_C_DEFINITIONS:
        raise InputValidationError(f"Unsupported Family C candidate_id: {candidate_id}")

    inputs = _prepare_family_a_inputs(
        returns_panel,
        universe_reference=_normalize_universe_reference(universe_reference.copy()),
    )

    market_variance = inputs.market_returns.rolling(
        window=_IDIOSYNCRATIC_VOL_LOOKBACK_DAYS,
        min_periods=_IDIOSYNCRATIC_VOL_LOOKBACK_DAYS,
    ).var()
    ivol_signal = pd.DataFrame(index=inputs.returns_panel.index, columns=inputs.returns_panel.columns, dtype=float)
    for ticker in inputs.returns_panel.columns:
        stock_returns = inputs.returns_panel[str(ticker)]
        covariance = stock_returns.rolling(
            window=_IDIOSYNCRATIC_VOL_LOOKBACK_DAYS,
            min_periods=_IDIOSYNCRATIC_VOL_LOOKBACK_DAYS,
        ).cov(inputs.market_returns)
        beta = covariance / market_variance.replace(0.0, np.nan)
        residual_returns = stock_returns - beta * inputs.market_returns
        ivol_signal[str(ticker)] = -(
            residual_returns.rolling(
                window=_IDIOSYNCRATIC_VOL_LOOKBACK_DAYS,
                min_periods=_IDIOSYNCRATIC_VOL_LOOKBACK_DAYS,
            ).std()
            * np.sqrt(_ANNUALIZATION_FACTOR)
        )

    max_signal = -inputs.returns_panel.rolling(
        window=_MAX_LOOKBACK_DAYS,
        min_periods=_MAX_LOOKBACK_DAYS,
    ).max()

    selected_signal = ivol_signal if candidate_id == "C1" else max_signal

    rows: list[dict[str, object]] = []
    for decision_date in inputs.decision_dates:
        signal_row = selected_signal.loc[decision_date]
        for ticker, signal_value in signal_row.items():
            rows.append(
                {
                    "date": decision_date.strftime("%Y-%m-%d"),
                    "ticker": str(ticker),
                    "signal_value": float(signal_value) if pd.notna(signal_value) else np.nan,
                }
            )
    frame = pd.DataFrame(rows).dropna(subset=["signal_value"]).sort_values(["date", "ticker"]).reset_index(drop=True)
    if frame.empty:
        raise InputValidationError(f"Candidate {candidate_id} produced an empty monthly signal frame.")
    return frame


def build_family_b_monthly_signal_frame(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    liquidity_long: pd.DataFrame,
    candidate_id: str,
) -> pd.DataFrame:
    if candidate_id not in FAMILY_B_DEFINITIONS:
        raise InputValidationError(f"Unsupported Family B candidate_id: {candidate_id}")

    inputs = _prepare_family_a_inputs(
        returns_panel,
        universe_reference=_normalize_universe_reference(universe_reference.copy()),
    )
    liquidity_frame = _normalize_liquidity_long(liquidity_long.copy())
    dollar_volume_panel = (
        liquidity_frame.pivot(index="date", columns="ticker", values="dollar_volume")
        .sort_index()
        .reindex(index=inputs.returns_panel.index, columns=inputs.returns_panel.columns)
    )
    daily_illiq = inputs.returns_panel.abs().div(dollar_volume_panel.clip(lower=1.0))
    illiq63 = daily_illiq.rolling(
        window=_ILLIQUIDITY_LEVEL_LOOKBACK_DAYS,
        min_periods=_ILLIQUIDITY_LEVEL_LOOKBACK_DAYS,
    ).median()
    illiq21 = daily_illiq.rolling(
        window=_ILLIQUIDITY_SHORT_LOOKBACK_DAYS,
        min_periods=_ILLIQUIDITY_SHORT_LOOKBACK_DAYS,
    ).median()
    adv5 = dollar_volume_panel.rolling(
        window=_SHORT_REVERSAL_LOOKBACK_DAYS,
        min_periods=_SHORT_REVERSAL_LOOKBACK_DAYS,
    ).mean()
    adv63 = dollar_volume_panel.rolling(
        window=_ILLIQUIDITY_LEVEL_LOOKBACK_DAYS,
        min_periods=_ILLIQUIDITY_LEVEL_LOOKBACK_DAYS,
    ).mean()
    reversal5 = -(inputs.price_panel / inputs.price_panel.shift(_SHORT_REVERSAL_LOOKBACK_DAYS) - 1.0)
    abnormal_volume = (adv5 / adv63.clip(lower=1.0)).clip(lower=1.0) - 1.0

    if candidate_id == "B1":
        selected_signal = -illiq63
    elif candidate_id == "B2":
        selected_signal = -((illiq21 / illiq63.clip(lower=1e-12)) - 1.0)
    else:
        selected_signal = reversal5 * abnormal_volume

    rows: list[dict[str, object]] = []
    for decision_date in inputs.decision_dates:
        signal_row = selected_signal.loc[decision_date]
        for ticker, signal_value in signal_row.items():
            rows.append(
                {
                    "date": decision_date.strftime("%Y-%m-%d"),
                    "ticker": str(ticker),
                    "signal_value": float(signal_value) if pd.notna(signal_value) else np.nan,
                }
            )
    frame = pd.DataFrame(rows).dropna(subset=["signal_value"]).sort_values(["date", "ticker"]).reset_index(drop=True)
    if frame.empty:
        raise InputValidationError(f"Candidate {candidate_id} produced an empty monthly signal frame.")
    return frame


def _build_monthly_forward_return_frame(inputs: _FamilyAInputs) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for decision_date in inputs.decision_dates:
        next_date = inputs.next_date_map[decision_date]
        forward_return = inputs.price_panel.loc[next_date] / inputs.price_panel.loc[decision_date] - 1.0
        for ticker, return_value in forward_return.items():
            rows.append(
                {
                    "date": decision_date.strftime("%Y-%m-%d"),
                    "ticker": str(ticker),
                    "forward_return": float(return_value) if pd.notna(return_value) else np.nan,
                    "decision_date": decision_date,
                    "next_date": next_date,
                    "estimated_price": float(inputs.price_panel.loc[decision_date, str(ticker)]),
                }
            )
    return pd.DataFrame(rows).sort_values(["date", "ticker"]).reset_index(drop=True)


def _build_candidate_signal_frame(
    *,
    candidate_id: str,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    liquidity_long: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if candidate_id in FAMILY_A_DEFINITIONS:
        return build_family_a_monthly_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
            candidate_id=candidate_id,
        )
    if candidate_id in FAMILY_B_DEFINITIONS:
        if liquidity_long is None:
            raise InputValidationError(f"Candidate {candidate_id} requires a liquidity_file with daily dollar volume.")
        return build_family_b_monthly_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
            liquidity_long=liquidity_long,
            candidate_id=candidate_id,
        )
    if candidate_id in FAMILY_C_DEFINITIONS:
        return build_family_c_monthly_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
            candidate_id=candidate_id,
        )
    raise InputValidationError(f"Unsupported candidate_id: {candidate_id}")


def _load_fee_and_slippage(config_file: str | Path) -> tuple[FeeConfig, SlippageConfig]:
    payload = load_yaml_file(config_file)
    return FeeConfig.model_validate(payload["fees"]), SlippageConfig.model_validate(payload["slippage"])


def _top_quintile_weights(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    buckets = _quantile_buckets(frame["signal_value"], quantiles=_QUANTILES)
    winners = frame.loc[buckets == _QUANTILES, "ticker"].astype(str).tolist()
    if not winners:
        return pd.Series(dtype=float)
    weight = 1.0 / float(len(winners))
    return pd.Series({ticker: weight for ticker in winners}, dtype=float).sort_index()


def _estimate_cost_fraction(
    *,
    target_weights: pd.Series,
    previous_weights: pd.Series,
    price_row: pd.Series,
    adv_shares_row: pd.Series,
    nav_notional: float,
    fee_config: FeeConfig,
    slippage_config: SlippageConfig,
) -> tuple[float, float]:
    tickers = previous_weights.index.union(target_weights.index)
    previous_aligned = previous_weights.reindex(tickers, fill_value=0.0)
    target_aligned = target_weights.reindex(tickers, fill_value=0.0)
    delta_weights = target_aligned - previous_aligned
    turnover = float(delta_weights.abs().sum())
    if turnover <= 0.0:
        return 0.0, 0.0
    prices = price_row.reindex(tickers).astype(float).to_numpy()
    adv = adv_shares_row.reindex(tickers).fillna(1.0).astype(float).to_numpy()
    quantities = delta_weights.to_numpy(dtype=float) * float(nav_notional) / np.maximum(prices, 1e-6)
    fees = estimate_fee_array(quantities, prices, fee_config)
    slippage = estimate_slippage_array(quantities, prices, adv, slippage_config)
    total_cost = float(np.sum(fees) + np.sum(slippage))
    return turnover, float(total_cost / float(nav_notional))


def _build_candidate_monthly_metrics(
    *,
    candidate_id: str,
    signal_frame: pd.DataFrame,
    baseline_signal_frame: pd.DataFrame,
    inputs: _FamilyAInputs,
    fee_config: FeeConfig,
    slippage_config: SlippageConfig,
    nav_notional: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    reference = inputs.universe_reference.copy()
    reference["ticker"] = reference["ticker"].astype(str)
    signal_long = signal_frame.copy()
    baseline_long = baseline_signal_frame.copy()
    forward_returns = _build_monthly_forward_return_frame(inputs)
    baseline_map = (
        baseline_long.rename(columns={"signal_value": "baseline_signal_value"})
        .merge(forward_returns.loc[:, ["date", "ticker", "forward_return"]], on=["date", "ticker"], how="inner")
    )

    coverage_rows: list[dict[str, object]] = []
    spread_rows: list[dict[str, object]] = []
    previous_weights = pd.Series(dtype=float)
    static_adv = reference.set_index("ticker")["avg_adv_20d"]

    for decision_date in inputs.decision_dates:
        date_key = decision_date.strftime("%Y-%m-%d")
        next_date = inputs.next_date_map[decision_date]
        candidate_date_frame = (
            forward_returns.loc[forward_returns["date"] == date_key]
            .merge(signal_long.loc[signal_long["date"] == date_key], on=["date", "ticker"], how="left")
            .merge(reference, on="ticker", how="left")
        )
        baseline_date_frame = (
            baseline_map.loc[baseline_map["date"] == date_key]
            .merge(reference, on="ticker", how="left")
            .rename(columns={"baseline_signal_value": "signal_value"})
        )

        eligible_frame = candidate_date_frame.dropna(subset=["forward_return", "estimated_price", "avg_adv_20d"]).copy()
        liquidity_cut_count = int(np.floor(len(eligible_frame) * _LIQUIDITY_CUT_FRACTION))
        raw_signal_frame = eligible_frame.dropna(subset=["signal_value"]).copy()
        raw_signal_frame = raw_signal_frame.sort_values(["avg_adv_20d", "ticker"]).reset_index(drop=True)
        if liquidity_cut_count > 0 and not raw_signal_frame.empty:
            dropped_tickers = set(raw_signal_frame.head(liquidity_cut_count)["ticker"].astype(str).tolist())
            effective_frame = raw_signal_frame.loc[~raw_signal_frame["ticker"].astype(str).isin(dropped_tickers)].copy()
        else:
            effective_frame = raw_signal_frame.copy()

        baseline_raw = baseline_date_frame.dropna(subset=["signal_value", "forward_return"]).copy()
        baseline_raw = baseline_raw.sort_values(["avg_adv_20d", "ticker"]).reset_index(drop=True)
        if liquidity_cut_count > 0 and not baseline_raw.empty:
            baseline_dropped = set(baseline_raw.head(liquidity_cut_count)["ticker"].astype(str).tolist())
            baseline_effective = baseline_raw.loc[
                ~baseline_raw["ticker"].astype(str).isin(baseline_dropped)
            ].copy()
        else:
            baseline_effective = baseline_raw.copy()

        coverage_rows.append(
            {
                "date": date_key,
                "candidate_id": candidate_id,
                "eligible_universe_count": int(len(eligible_frame)),
                "raw_signal_count": int(len(raw_signal_frame)),
                "effective_signal_count": int(len(effective_frame)),
                "effective_coverage_ratio": (
                    float(len(raw_signal_frame) / len(eligible_frame)) if len(eligible_frame) else 0.0
                ),
                "effective_coverage_after_liquidity_cut": (
                    float(len(effective_frame) / len(eligible_frame)) if len(eligible_frame) else 0.0
                ),
            }
        )

        top_bucket_return = np.nan
        bottom_bucket_return = np.nan
        top_bottom_spread = np.nan
        rank_ic = np.nan
        baseline_spread = np.nan
        turnover = 0.0
        net_top_bottom_spread = np.nan
        target_weights = pd.Series(dtype=float)

        if len(effective_frame) >= _MIN_EFFECTIVE_NAMES:
            rank_ic = _safe_spearman(effective_frame["signal_value"], effective_frame["forward_return"])
            buckets = _quantile_buckets(effective_frame["signal_value"], quantiles=_QUANTILES)
            top_bucket_return = float(effective_frame.loc[buckets == _QUANTILES, "forward_return"].mean())
            bottom_bucket_return = float(effective_frame.loc[buckets == 1, "forward_return"].mean())
            top_bottom_spread = float(top_bucket_return - bottom_bucket_return)
            target_weights = _top_quintile_weights(effective_frame)
            turnover, cost_fraction = _estimate_cost_fraction(
                target_weights=target_weights,
                previous_weights=previous_weights,
                price_row=inputs.price_panel.loc[decision_date],
                adv_shares_row=static_adv,
                nav_notional=nav_notional,
                fee_config=fee_config,
                slippage_config=slippage_config,
            )
            net_top_bottom_spread = float(top_bottom_spread - cost_fraction)
            previous_weights = target_weights.copy()
        else:
            previous_weights = pd.Series(dtype=float)

        if len(baseline_effective) >= _MIN_EFFECTIVE_NAMES:
            baseline_buckets = _quantile_buckets(baseline_effective["signal_value"], quantiles=_QUANTILES)
            baseline_top = float(baseline_effective.loc[baseline_buckets == _QUANTILES, "forward_return"].mean())
            baseline_bottom = float(baseline_effective.loc[baseline_buckets == 1, "forward_return"].mean())
            baseline_spread = float(baseline_top - baseline_bottom)

        spread_rows.append(
            {
                "date": date_key,
                "candidate_id": candidate_id,
                "rank_ic": rank_ic,
                "top_bucket_return": top_bucket_return,
                "bottom_bucket_return": bottom_bucket_return,
                "top_bottom_spread": top_bottom_spread,
                "net_top_bottom_spread": net_top_bottom_spread,
                "turnover": float(turnover),
                "benchmark_spread": baseline_spread,
                "next_date": next_date.strftime("%Y-%m-%d"),
            }
        )
    return (
        pd.DataFrame(coverage_rows).sort_values("date").reset_index(drop=True),
        pd.DataFrame(spread_rows).sort_values("date").reset_index(drop=True),
    )


def _build_subperiod_metrics(spread_frame: pd.DataFrame) -> pd.DataFrame:
    valid = spread_frame.dropna(subset=["rank_ic", "top_bottom_spread"]).copy().reset_index(drop=True)
    if valid.empty:
        slices = [valid.copy(), valid.copy(), valid.copy()]
    else:
        slices = [slice_frame.copy() for slice_frame in np.array_split(valid, 3)]

    rows: list[dict[str, object]] = []
    for index, frame in enumerate(slices, start=1):
        rows.append(
            {
                "subperiod_id": f"p{index}",
                "start_date": str(frame["date"].min()) if not frame.empty else None,
                "end_date": str(frame["date"].max()) if not frame.empty else None,
                "observation_count": int(len(frame)),
                "mean_rank_ic": float(frame["rank_ic"].mean()) if not frame.empty else 0.0,
                "rank_ic_tstat": _mean_t_stat(frame["rank_ic"]) if not frame.empty else 0.0,
                "mean_alpha_only_spread": float(frame["top_bottom_spread"].mean()) if not frame.empty else 0.0,
                "alpha_only_tstat": _mean_t_stat(frame["top_bottom_spread"]) if not frame.empty else 0.0,
                "positive_rank_ic_ratio": float((frame["rank_ic"] > 0.0).mean()) if not frame.empty else 0.0,
            }
        )
    return pd.DataFrame(rows)


def _render_note(
    *,
    candidate: AlphaCoreCandidateDefinition,
    summary_payload: dict[str, Any],
    coverage_frame: pd.DataFrame,
    subperiod_frame: pd.DataFrame,
    orthogonality_frame: pd.DataFrame,
) -> str:
    lines = [
        f"# {candidate.candidate_id} Qualification Note",
        "",
        "## Candidate",
        f"- Candidate: `{candidate.candidate_id}`",
        f"- Family: `{candidate.family_id}`",
        f"- Name: {candidate.candidate_name}",
        f"- Baseline comparator: `{summary_payload['baseline_comparator_id']}` (`platform_native_comparable`)",
        "",
        "## Definition",
        f"- {candidate.description}",
        f"- Decision grid: `{summary_payload['decision_grid']}`",
        f"- PIT lag rule: `{summary_payload['pit_lag_rule']}`",
        "",
        "## Coverage",
        f"- Median effective coverage: `{summary_payload['coverage_median']:.4f}`",
        f"- Retention after liquidity cut: `{summary_payload['coverage_retention_after_liquidity_cut']:.4f}`",
        f"- Gross-to-net retention: `{summary_payload['gross_to_net_retention']:.4f}`",
        f"- Coverage rows: `{len(coverage_frame)}`",
        "",
        "## Economics",
        f"- Mean rank IC: `{summary_payload['oos_mean_rank_ic']:.4f}`",
        f"- Rank-IC t-stat: `{summary_payload['oos_rank_ic_tstat']:.4f}`",
        f"- Mean alpha-only spread: `{summary_payload['oos_mean_alpha_only_spread']:.4f}`",
        f"- Alpha-only t-stat: `{summary_payload['oos_alpha_only_tstat']:.4f}`",
        "",
        "## Subperiod Stability",
        subperiod_frame.to_markdown(index=False),
        "",
        "## Orthogonality Vs Baseline",
        orthogonality_frame.to_markdown(index=False),
        "",
        "## Gate Decision",
        f"- Admission gate pass: `{summary_payload['admission_gate_pass']}`",
        f"- Subperiod gate pass: `{summary_payload['subperiod_gate_pass']}`",
        f"- Orthogonality gate pass: `{summary_payload['orthogonality_gate_pass']}`",
        f"- Winner gate pass: `{summary_payload['winner_gate_pass']}`",
        "",
        "## Keep / Stop",
        (
            "- KEEP: candidate survives Week 2-3 qualification under the frozen contract."
            if bool(summary_payload["winner_gate_pass"])
            else "- STOP: candidate does not clear the frozen Week 2-3 qualification contract yet."
        ),
        "",
        "## Notes",
    ]
    for note in summary_payload["notes"]:
        lines.append(f"- {note}")
    return "\n".join(lines)


def run_alpha_core_candidate(
    *,
    candidate_id: str,
    returns_file: str | Path,
    universe_reference_file: str | Path,
    liquidity_file: str | Path | None,
    config_file: str | Path,
    output_dir: str | Path,
    as_of_date: str,
    nav_notional: float = _NAV_NOTIONAL,
) -> AlphaCoreQualificationResult:
    if candidate_id not in ALL_CANDIDATE_DEFINITIONS:
        raise InputValidationError(f"Unsupported candidate_id: {candidate_id}")

    candidate = ALL_CANDIDATE_DEFINITIONS[candidate_id]
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    returns_long = pd.read_csv(returns_file)
    if returns_long.empty:
        raise InputValidationError("returns_file produced an empty panel for alpha-core qualification.")
    returns_long["date"] = pd.to_datetime(returns_long["date"], errors="coerce").dt.normalize()
    returns_long["return"] = pd.to_numeric(returns_long["return"], errors="coerce")
    if returns_long["date"].isna().any() or returns_long["return"].isna().any():
        raise InputValidationError("returns_file contains invalid date or return values.")
    returns_panel = returns_long.pivot(index="date", columns="ticker", values="return").sort_index().fillna(0.0)

    universe_reference = _load_universe_reference(universe_reference_file)
    ordered_reference = universe_reference.loc[universe_reference["ticker"].isin(returns_panel.columns)].copy()
    if ordered_reference.empty:
        raise InputValidationError("No overlap between returns_file tickers and universe_reference_file.")
    returns_panel = returns_panel.reindex(columns=ordered_reference["ticker"].tolist())
    liquidity_long = _load_liquidity_long(liquidity_file) if liquidity_file is not None else None

    inputs = _prepare_family_a_inputs(returns_panel, universe_reference=ordered_reference)
    candidate_signal_frame = _build_candidate_signal_frame(
        candidate_id=candidate_id,
        returns_panel=returns_panel,
        universe_reference=ordered_reference,
        liquidity_long=liquidity_long,
    )
    baseline_signal_frame = _build_baseline_signal_frame(inputs)
    fee_config, slippage_config = _load_fee_and_slippage(config_file)

    coverage_frame, spread_frame = _build_candidate_monthly_metrics(
        candidate_id=candidate_id,
        signal_frame=candidate_signal_frame,
        baseline_signal_frame=baseline_signal_frame,
        inputs=inputs,
        fee_config=fee_config,
        slippage_config=slippage_config,
        nav_notional=nav_notional,
    )
    baseline_coverage_frame, baseline_spread_frame = _build_candidate_monthly_metrics(
        candidate_id=_BASELINE_ID,
        signal_frame=baseline_signal_frame,
        baseline_signal_frame=baseline_signal_frame,
        inputs=inputs,
        fee_config=fee_config,
        slippage_config=slippage_config,
        nav_notional=nav_notional,
    )

    valid_candidate = spread_frame.dropna(subset=["rank_ic", "top_bottom_spread"]).copy().sort_values("date").reset_index(drop=True)
    valid_baseline = baseline_spread_frame.dropna(subset=["rank_ic", "top_bottom_spread"]).copy().sort_values("date").reset_index(drop=True)
    aligned_dates = sorted(set(valid_candidate["date"]).intersection(set(valid_baseline["date"])))
    aligned_candidate = valid_candidate.loc[valid_candidate["date"].isin(aligned_dates)].copy().sort_values("date").reset_index(drop=True)
    aligned_baseline = valid_baseline.loc[valid_baseline["date"].isin(aligned_dates)].copy().sort_values("date").reset_index(drop=True)

    coverage_after_cut = coverage_frame["effective_coverage_after_liquidity_cut"]
    raw_coverage = coverage_frame["effective_coverage_ratio"].replace(0.0, np.nan)
    coverage_retention = (
        coverage_after_cut.div(raw_coverage).replace([np.inf, -np.inf], np.nan).median()
        if not coverage_frame.empty
        else np.nan
    )
    coverage_median = float(coverage_after_cut.median()) if not coverage_after_cut.empty else 0.0
    baseline_coverage_after_cut = baseline_coverage_frame["effective_coverage_after_liquidity_cut"]
    baseline_coverage_median = (
        float(baseline_coverage_after_cut.median()) if not baseline_coverage_after_cut.empty else 0.0
    )
    gross_spread_mean = float(valid_candidate["top_bottom_spread"].mean()) if not valid_candidate.empty else 0.0
    net_spread_mean = float(valid_candidate["net_top_bottom_spread"].mean()) if not valid_candidate.empty else 0.0
    gross_to_net_retention = max(float(net_spread_mean / gross_spread_mean), 0.0) if gross_spread_mean > 0.0 else 0.0
    baseline_gross_spread_mean = float(valid_baseline["top_bottom_spread"].mean()) if not valid_baseline.empty else 0.0
    baseline_net_spread_mean = float(valid_baseline["net_top_bottom_spread"].mean()) if not valid_baseline.empty else 0.0
    baseline_gross_to_net_retention = (
        max(float(baseline_net_spread_mean / baseline_gross_spread_mean), 0.0)
        if baseline_gross_spread_mean > 0.0
        else 0.0
    )

    baseline_mean_rank_ic = float(aligned_baseline["rank_ic"].mean()) if not aligned_baseline.empty else 0.0
    baseline_mean_spread = float(aligned_baseline["top_bottom_spread"].mean()) if not aligned_baseline.empty else 0.0
    spread_corr = (
        float(aligned_candidate["top_bottom_spread"].corr(aligned_baseline["top_bottom_spread"]))
        if len(aligned_candidate) >= 2 and len(aligned_candidate) == len(aligned_baseline)
        else 1.0
    )
    rank_ic_improvement = _relative_improvement(
        float(aligned_candidate["rank_ic"].mean()) if not aligned_candidate.empty else 0.0,
        baseline_mean_rank_ic,
    )
    rank_ir_improvement = _relative_improvement(
        _mean_ir(aligned_candidate["rank_ic"]) if not aligned_candidate.empty else 0.0,
        _mean_ir(aligned_baseline["rank_ic"]) if not aligned_baseline.empty else 0.0,
    )
    alpha_spread_improvement = _relative_improvement(
        float(aligned_candidate["top_bottom_spread"].mean()) if not aligned_candidate.empty else 0.0,
        baseline_mean_spread,
    )

    subperiod_frame = _build_subperiod_metrics(valid_candidate)
    subperiod_positive_count = int((subperiod_frame["mean_rank_ic"] > 0.0).sum())
    subperiod_min_rank_ic = float(subperiod_frame["mean_rank_ic"].min()) if not subperiod_frame.empty else 0.0

    admission_gate_pass = bool(
        float(valid_candidate["rank_ic"].mean()) > 0.0
        and _mean_t_stat(valid_candidate["rank_ic"]) >= 2.0
        and _mean_t_stat(valid_candidate["top_bottom_spread"]) >= 2.0
        and coverage_median >= 0.70
        and gross_to_net_retention >= 0.50
    )
    subperiod_gate_pass = bool(subperiod_positive_count >= 2 and subperiod_min_rank_ic >= 0.0)
    orthogonality_gate_pass = bool(spread_corr < 0.70)
    winner_increment_gate_pass = bool(rank_ic_improvement >= 0.15)
    winner_gate_pass = bool(
        admission_gate_pass and subperiod_gate_pass and orthogonality_gate_pass and winner_increment_gate_pass
    )

    summary_payload = {
        "candidate_id": candidate.candidate_id,
        "family_id": candidate.family_id,
        "candidate_name": candidate.candidate_name,
        "as_of_date": str(as_of_date),
        "universe_name": "us_expanded_platform_native_sample",
        "decision_grid": "monthly_21d_month_end",
        "pit_lag_rule": "close_of_decision_date_consumed_on_next_monthly_holding_interval",
        "input_spec": {
            "returns_file": str(Path(returns_file).resolve()),
            "universe_reference_file": str(Path(universe_reference_file).resolve()),
            "liquidity_file": str(Path(liquidity_file).resolve()) if liquidity_file is not None else None,
            "config_file": str(Path(config_file).resolve()),
            "market_proxy": "equal_weight_universe_returns",
            "liquidity_cut_fraction": _LIQUIDITY_CUT_FRACTION,
            "cost_nav_notional": float(nav_notional),
        },
        "baseline_comparator_id": _BASELINE_ID,
        "baseline_comparator_type": "platform_native_comparable",
        "coverage_median": coverage_median,
        "coverage_retention_after_liquidity_cut": float(coverage_retention) if pd.notna(coverage_retention) else 0.0,
        "gross_to_net_retention": gross_to_net_retention,
        "oos_mean_rank_ic": float(valid_candidate["rank_ic"].mean()) if not valid_candidate.empty else 0.0,
        "oos_rank_ic_tstat": _mean_t_stat(valid_candidate["rank_ic"]),
        "oos_mean_alpha_only_spread": gross_spread_mean,
        "oos_alpha_only_tstat": _mean_t_stat(valid_candidate["top_bottom_spread"]),
        "subperiod_rank_ic": subperiod_frame["mean_rank_ic"].tolist(),
        "subperiod_positive_count": subperiod_positive_count,
        "subperiod_min_rank_ic": subperiod_min_rank_ic,
        "spread_corr_vs_baseline": spread_corr,
        "rank_ic_improvement_vs_baseline": rank_ic_improvement,
        "rank_ir_improvement_vs_baseline": rank_ir_improvement,
        "winner_increment_gate_pass": winner_increment_gate_pass,
        "winner_gate_pass": winner_gate_pass,
        "admission_gate_pass": admission_gate_pass,
        "subperiod_gate_pass": subperiod_gate_pass,
        "orthogonality_gate_pass": orthogonality_gate_pass,
        "notes": [
            "platform_native_sample caveat: current Week 2 run uses the checked-in 50-name expanded-US sample, not a full rank_500_1500 universe.",
            "Market proxy is the equal-weight daily return across the current platform sample.",
            "Liquidity cut uses a static bottom-20% ADV filter from the universe reference snapshot.",
        ],
    }

    oos_metrics_frame = pd.DataFrame(
        [
            {
                "candidate_id": candidate.candidate_id,
                "coverage_median": summary_payload["coverage_median"],
                "gross_to_net_retention": summary_payload["gross_to_net_retention"],
                "oos_mean_rank_ic": summary_payload["oos_mean_rank_ic"],
                "oos_rank_ic_tstat": summary_payload["oos_rank_ic_tstat"],
                "oos_mean_alpha_only_spread": summary_payload["oos_mean_alpha_only_spread"],
                "oos_alpha_only_tstat": summary_payload["oos_alpha_only_tstat"],
                "baseline_id": _BASELINE_ID,
                "baseline_mean_rank_ic": baseline_mean_rank_ic,
                "baseline_mean_alpha_only_spread": baseline_mean_spread,
                "rank_ic_improvement_vs_baseline": rank_ic_improvement,
                "rank_ir_improvement_vs_baseline": rank_ir_improvement,
                "alpha_spread_improvement_vs_baseline": alpha_spread_improvement,
            }
        ]
    )
    orthogonality_frame = pd.DataFrame(
        [
            {
                "candidate_id": candidate.candidate_id,
                "baseline_id": _BASELINE_ID,
                "spread_corr": spread_corr,
                "rank_ic_improvement_vs_baseline": rank_ic_improvement,
                "rank_ir_improvement_vs_baseline": rank_ir_improvement,
                "alpha_spread_improvement_vs_baseline": alpha_spread_improvement,
                "coverage_delta_vs_baseline": float(summary_payload["coverage_median"] - baseline_coverage_median),
                "retention_delta_vs_baseline": float(
                    summary_payload["gross_to_net_retention"] - baseline_gross_to_net_retention
                ),
            }
        ]
    )
    note_markdown = _render_note(
        candidate=candidate,
        summary_payload=summary_payload,
        coverage_frame=coverage_frame,
        subperiod_frame=subperiod_frame,
        orthogonality_frame=orthogonality_frame,
    )

    coverage_frame.to_csv(output_path / "coverage_by_month.csv", index=False)
    spread_frame.loc[
        :,
        [
            "date",
            "candidate_id",
            "top_bucket_return",
            "bottom_bucket_return",
            "top_bottom_spread",
            "net_top_bottom_spread",
            "turnover",
            "benchmark_spread",
        ],
    ].to_csv(output_path / "spread_series.csv", index=False)
    subperiod_frame.to_csv(output_path / "subperiod_metrics.csv", index=False)
    orthogonality_frame.to_csv(output_path / "orthogonality_vs_baseline.csv", index=False)
    oos_metrics_frame.to_csv(output_path / "oos_metrics.csv", index=False)
    write_json(output_path / "summary.json", summary_payload)
    write_text(output_path / "note.md", note_markdown)

    return AlphaCoreQualificationResult(
        candidate=candidate,
        output_dir=output_path,
        summary_payload=summary_payload,
    )
