"""Alpaca-backed provider for US equity snapshot preparation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from portfolio_os.data.loaders import normalize_ticker
from portfolio_os.data.providers.base import (
    DailyMarketSnapshotRow,
    IndexWeightRow,
    ReferenceSnapshotRow,
)
from portfolio_os.domain.errors import (
    InputValidationError,
    ProviderDataError,
    ProviderPermissionError,
    ProviderRuntimeError,
)


def resolve_alpaca_credentials(
    *,
    api_key: str | None = None,
    secret_key: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Resolve Alpaca credentials from explicit args then environment variables."""

    resolved_api_key = str(api_key or "").strip() or str(os.getenv("ALPACA_API_KEY", "")).strip()
    resolved_secret_key = str(secret_key or "").strip() or str(os.getenv("ALPACA_SECRET_KEY", "")).strip()
    if resolved_api_key and resolved_secret_key:
        source = "explicit" if (api_key or secret_key) else "env"
        return resolved_api_key, resolved_secret_key, source
    return None, None, None


def _to_utc_timestamp(date_text: str) -> datetime:
    """Convert YYYY-MM-DD into a UTC timestamp anchor."""

    try:
        date_value = datetime.strptime(str(date_text), "%Y-%m-%d")
    except ValueError as exc:
        raise InputValidationError(
            f"Unsupported as_of_date format {date_text!r}. Use YYYY-MM-DD."
        ) from exc
    return date_value.replace(tzinfo=timezone.utc)


@dataclass
class _BarPoint:
    ticker: str
    timestamp: datetime
    close: float
    volume: float


class AlpacaProvider:
    """Alpaca-backed provider with yfinance fallback for static reference fields."""

    provider_name = "alpaca"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        secret_key: str | None = None,
        paper: bool = True,
    ) -> None:
        resolved_key, resolved_secret, source = resolve_alpaca_credentials(
            api_key=api_key,
            secret_key=secret_key,
        )
        if not resolved_key or not resolved_secret:
            raise InputValidationError(
                "Alpaca provider requires ALPACA_API_KEY and ALPACA_SECRET_KEY."
            )
        self._api_key = resolved_key
        self._secret_key = resolved_secret
        self._paper = bool(paper)
        self.provider_metadata = {
            "provider_token_source": source,
            "approximation_notes": {
                "market": [
                    "Daily bars are pulled from Alpaca historical data.",
                    "pre_close is approximated from the previous available daily close.",
                    "amount is approximated as close * volume.",
                    "adv_shares is approximated as the mean volume over the latest 20 sessions.",
                    "US equities have no static daily limit-up/down constraints in this provider.",
                ],
                "reference": [
                    "industry and issuer_total_shares are fetched from yfinance fallback fields.",
                    "industry uses info.sector and defaults to 'Unknown' when unavailable.",
                    "issuer_total_shares uses info.sharesOutstanding when available.",
                ],
                "target": [
                    "Index-weight construction is not provided by Alpaca provider; use client-provided target.csv.",
                ],
            },
        }
        self._reports: dict[str, dict[str, Any]] = {
            "market": self._default_report(),
            "reference": self._default_report(),
            "target": self._default_report(),
        }
        self._stock_data_client = None
        self._trading_client = None

    @staticmethod
    def _default_report() -> dict[str, Any]:
        return {
            "provider_capability_status": "available",
            "fallback_notes": [],
            "fallback_chain_used": [],
            "data_source_mix": ["alpaca"],
            "permission_notes": [],
            "recommended_alternative_path": None,
        }

    def _reset_report(self, feed_name: str) -> None:
        self._reports[feed_name] = self._default_report()

    def _mark_degraded(
        self,
        feed_name: str,
        *,
        fallback_note: str,
        source: str | None = None,
    ) -> None:
        report = self._reports.setdefault(feed_name, self._default_report())
        report["provider_capability_status"] = "degraded"
        if fallback_note not in report["fallback_notes"]:
            report["fallback_notes"].append(fallback_note)
        if source:
            if source not in report["data_source_mix"]:
                report["data_source_mix"].append(source)
            chain_entry = f"{source}:{fallback_note}"
            if chain_entry not in report["fallback_chain_used"]:
                report["fallback_chain_used"].append(chain_entry)

    def _mark_unavailable(
        self,
        feed_name: str,
        *,
        note: str,
        alternative: str | None = None,
    ) -> None:
        report = self._reports.setdefault(feed_name, self._default_report())
        report["provider_capability_status"] = "unavailable"
        if note not in report["permission_notes"]:
            report["permission_notes"].append(note)
        if alternative is not None:
            report["recommended_alternative_path"] = alternative

    def get_capability_report(self, feed_name: str) -> dict[str, Any]:
        """Return a provider capability report for one feed."""

        return dict(self._reports.get(feed_name, self._default_report()))

    def _load_alpaca_modules(self):
        """Import alpaca-py lazily and return required symbols."""

        try:
            from alpaca.data.historical import StockHistoricalDataClient  # type: ignore
            from alpaca.data.requests import StockBarsRequest  # type: ignore
            from alpaca.data.timeframe import TimeFrame  # type: ignore
            from alpaca.trading.client import TradingClient  # type: ignore
        except ImportError as exc:
            raise ProviderRuntimeError(
                "alpaca-py is required for Alpaca provider. Install with `pip install alpaca-py`."
            ) from exc
        return StockHistoricalDataClient, StockBarsRequest, TimeFrame, TradingClient

    def _stock_client(self):
        if self._stock_data_client is None:
            (
                StockHistoricalDataClient,
                _StockBarsRequest,
                _TimeFrame,
                _TradingClient,
            ) = self._load_alpaca_modules()
            self._stock_data_client = StockHistoricalDataClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
            )
        return self._stock_data_client

    def _trading_client_instance(self):
        if self._trading_client is None:
            (
                _StockHistoricalDataClient,
                _StockBarsRequest,
                _TimeFrame,
                TradingClient,
            ) = self._load_alpaca_modules()
            self._trading_client = TradingClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
                paper=self._paper,
            )
        return self._trading_client

    @staticmethod
    def _bars_to_frame(raw: Any) -> pd.DataFrame:
        """Normalize alpaca bar responses into a DataFrame with symbol/timestamp columns."""

        if raw is None:
            return pd.DataFrame()
        if hasattr(raw, "df"):
            frame = raw.df.copy()  # type: ignore[attr-defined]
        elif isinstance(raw, pd.DataFrame):
            frame = raw.copy()
        else:
            bars = getattr(raw, "data", None)
            if isinstance(bars, dict):
                rows: list[dict[str, Any]] = []
                for ticker, points in bars.items():
                    for point in points:
                        rows.append(
                            {
                                "symbol": ticker,
                                "timestamp": getattr(point, "timestamp", None),
                                "close": getattr(point, "close", None),
                                "volume": getattr(point, "volume", None),
                            }
                        )
                frame = pd.DataFrame(rows)
            else:
                frame = pd.DataFrame()
        if frame.empty:
            return pd.DataFrame(columns=["symbol", "timestamp", "close", "volume"])

        frame = frame.reset_index()
        rename_map = {}
        for column in frame.columns:
            lower = str(column).lower()
            if lower in {"symbol", "ticker"}:
                rename_map[column] = "symbol"
            elif lower in {"timestamp", "time", "datetime"}:
                rename_map[column] = "timestamp"
            elif lower == "close":
                rename_map[column] = "close"
            elif lower == "volume":
                rename_map[column] = "volume"
        frame = frame.rename(columns=rename_map)
        required = {"symbol", "timestamp", "close", "volume"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ProviderDataError(
                f"Alpaca bars response missing required fields: {', '.join(missing)}"
            )
        frame["symbol"] = frame["symbol"].astype(str).str.strip().map(normalize_ticker)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
        frame = frame.dropna(subset=["symbol", "timestamp", "close", "volume"])
        return frame.sort_values(["symbol", "timestamp"]).reset_index(drop=True)

    def _asset_tradeability_map(self, tickers: list[str]) -> dict[str, bool]:
        """Resolve tradability for symbols from Alpaca assets when available."""

        mapped = {normalize_ticker(ticker): True for ticker in tickers}
        try:
            assets = self._trading_client_instance().get_all_assets()
        except Exception:
            self._mark_degraded(
                "market",
                fallback_note="asset_status_unavailable_default_tradable",
                source="alpaca",
            )
            return mapped

        requested = set(mapped.keys())
        candidates: dict[str, bool] = {}
        for asset in assets or []:
            symbol = normalize_ticker(getattr(asset, "symbol", ""))
            if symbol not in requested:
                continue
            tradable = bool(getattr(asset, "tradable", True))
            status_raw = str(getattr(asset, "status", "")).strip().lower()
            status = status_raw.split(".")[-1]
            is_active = status in {"", "active"}
            candidate = bool(tradable and is_active)
            if symbol in candidates:
                candidates[symbol] = bool(candidates[symbol] or candidate)
            else:
                candidates[symbol] = candidate
        for ticker in mapped:
            if ticker in candidates:
                mapped[ticker] = bool(candidates[ticker])
        return mapped

    def get_market_data(self, tickers: list[str], as_of_date: str) -> pd.DataFrame:
        """Return market fields close/pre_close/volume/amount for requested symbols."""

        self._reset_report("market")
        normalized = [normalize_ticker(ticker) for ticker in tickers]
        as_of_ts = _to_utc_timestamp(as_of_date)
        start_ts = as_of_ts - timedelta(days=45)
        (
            _StockHistoricalDataClient,
            StockBarsRequest,
            TimeFrame,
            _TradingClient,
        ) = self._load_alpaca_modules()
        request = StockBarsRequest(
            symbol_or_symbols=normalized,
            timeframe=TimeFrame.Day,
            start=start_ts,
            end=as_of_ts + timedelta(days=1),
        )
        try:
            raw = self._stock_client().get_stock_bars(request)
        except Exception as exc:
            self._mark_unavailable(
                "market",
                note="alpaca_market_bars_unavailable",
                alternative="provide_market_csv_and_continue",
            )
            raise ProviderRuntimeError(f"Alpaca market bars request failed: {exc}") from exc

        frame = self._bars_to_frame(raw)
        if frame.empty:
            raise ProviderDataError(
                f"Alpaca returned no daily bars for requested symbols on or before {as_of_date}."
            )
        frame = frame[frame["symbol"].isin(normalized)].copy()
        if frame.empty:
            raise ProviderDataError(
                f"Alpaca bars response has no rows for requested symbols: {', '.join(normalized)}"
            )
        frame["date"] = frame["timestamp"].dt.date
        cutoff = as_of_ts.date()
        frame = frame[frame["date"] <= cutoff].copy()
        if frame.empty:
            raise ProviderDataError(f"Alpaca has no bars on or before {as_of_date}.")

        rows: list[dict[str, Any]] = []
        for ticker in normalized:
            history = frame.loc[frame["symbol"] == ticker].sort_values("timestamp").copy()
            if history.empty:
                continue
            latest = history.iloc[-1]
            pre_close = float(latest["close"])
            if len(history) >= 2:
                pre_close = float(history.iloc[-2]["close"])
            adv20 = float(history.tail(20)["volume"].mean())
            if adv20 <= 0:
                adv20 = float(latest["volume"])
            close = float(latest["close"])
            volume = float(latest["volume"])
            rows.append(
                {
                    "ticker": ticker,
                    "close": close,
                    "pre_close": pre_close,
                    "volume": volume,
                    "amount": close * volume,
                    "adv_shares": max(adv20, 1.0),
                }
            )
        result = pd.DataFrame(rows, columns=["ticker", "close", "pre_close", "volume", "amount", "adv_shares"])
        missing = sorted(set(normalized) - set(result["ticker"].tolist()))
        if missing:
            raise ProviderDataError(
                f"Alpaca market snapshot is missing ticker(s): {', '.join(missing)}"
            )
        return result.sort_values("ticker").reset_index(drop=True)

    def get_daily_market_snapshot(
        self,
        tickers: list[str],
        as_of_date: str,
    ) -> list[DailyMarketSnapshotRow]:
        """Return market rows required for `market.csv`."""

        market = self.get_market_data(tickers, as_of_date)
        tradable_map = self._asset_tradeability_map(list(market["ticker"]))
        rows: list[DailyMarketSnapshotRow] = []
        for row in market.to_dict(orient="records"):
            ticker = str(row["ticker"])
            rows.append(
                DailyMarketSnapshotRow(
                    ticker=ticker,
                    close=float(row["close"]),
                    vwap=float(row["close"]),
                    adv_shares=float(row["adv_shares"]),
                    tradable=bool(tradable_map.get(ticker, True)),
                    upper_limit_hit=False,
                    lower_limit_hit=False,
                )
            )
        return rows

    @staticmethod
    def _load_yfinance_module():
        try:
            import yfinance as yf  # type: ignore
        except ImportError as exc:
            raise ProviderRuntimeError(
                "yfinance is required for Alpaca reference fallback. Install with `pip install yfinance`."
            ) from exc
        return yf

    def _fetch_yfinance_info(self, ticker: str) -> dict[str, Any]:
        yf = self._load_yfinance_module()
        try:
            info = getattr(yf.Ticker(ticker), "info", {}) or {}
        except Exception:
            self._mark_degraded(
                "reference",
                fallback_note="yfinance_lookup_failed",
                source="yfinance",
            )
            return {}
        if not isinstance(info, dict):
            return {}
        return info

    def get_reference_data(self, tickers: list[str], as_of_date: str) -> pd.DataFrame:
        """Return reference fields industry and total_shares."""

        _ = as_of_date
        self._reset_report("reference")
        rows: list[dict[str, Any]] = []
        for ticker in [normalize_ticker(item) for item in tickers]:
            info = self._fetch_yfinance_info(ticker)
            sector = str(info.get("sector", "")).strip()
            shares_outstanding = info.get("sharesOutstanding")
            parsed_shares = pd.to_numeric(shares_outstanding, errors="coerce")
            if not sector:
                sector = "Unknown"
                self._mark_degraded(
                    "reference",
                    fallback_note="industry_missing_default_unknown",
                    source="yfinance",
                )
            if pd.isna(parsed_shares) or float(parsed_shares) <= 0:
                parsed_shares = None
                self._mark_degraded(
                    "reference",
                    fallback_note="shares_outstanding_missing",
                    source="yfinance",
                )
            rows.append(
                {
                    "ticker": ticker,
                    "industry": sector,
                    "total_shares": (float(parsed_shares) if parsed_shares is not None else None),
                }
            )
        result = pd.DataFrame(rows, columns=["ticker", "industry", "total_shares"])
        if result.empty:
            raise ProviderDataError("Alpaca reference snapshot returned no rows.")
        return result.sort_values("ticker").reset_index(drop=True)

    def get_reference_snapshot(
        self,
        tickers: list[str],
        as_of_date: str,
    ) -> list[ReferenceSnapshotRow]:
        """Return reference rows required for `reference.csv`."""

        frame = self.get_reference_data(tickers, as_of_date)
        rows: list[ReferenceSnapshotRow] = []
        for row in frame.to_dict(orient="records"):
            rows.append(
                ReferenceSnapshotRow(
                    ticker=str(row["ticker"]),
                    industry=str(row["industry"]).strip() or "Unknown",
                    benchmark_weight=None,
                    issuer_total_shares=(
                        float(row["total_shares"]) if row.get("total_shares") is not None else None
                    ),
                )
            )
        return rows

    def get_limit_data(self, tickers: list[str], as_of_date: str) -> pd.DataFrame:
        """Return a no-limit marker table for US equities."""

        _ = as_of_date
        return pd.DataFrame(
            [{"ticker": normalize_ticker(ticker), "limit_type": "no_limit"} for ticker in tickers],
            columns=["ticker", "limit_type"],
        )

    def get_index_weights(
        self,
        index_code: str,
        as_of_date: str,
    ) -> list[IndexWeightRow]:
        """Alpaca provider does not supply index weights for target builder."""

        _ = index_code
        _ = as_of_date
        self._reset_report("target")
        self._mark_unavailable(
            "target",
            note="index_weights_not_supported_by_alpaca_provider",
            alternative="provide_target_csv_and_continue",
        )
        raise ProviderPermissionError(
            "Alpaca provider does not expose index weights for target.csv generation. "
            "Use a client-provided target.csv."
        )
