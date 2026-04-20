"""Offline mock provider for data-preparation builders."""

from __future__ import annotations

from portfolio_os.data.loaders import normalize_ticker
from portfolio_os.data.providers.base import (
    DailyMarketSnapshotRow,
    IndexWeightRow,
    ReferenceSnapshotRow,
)
from portfolio_os.domain.errors import InputValidationError


MOCK_MARKET_DATA: dict[str, DailyMarketSnapshotRow] = {
    "600519": DailyMarketSnapshotRow(
        ticker="600519",
        close=1680.0,
        vwap=1675.0,
        adv_shares=150000.0,
        tradable=True,
        upper_limit_hit=False,
        lower_limit_hit=False,
    ),
    "300750": DailyMarketSnapshotRow(
        ticker="300750",
        close=220.0,
        vwap=219.0,
        adv_shares=2500000.0,
        tradable=True,
        upper_limit_hit=False,
        lower_limit_hit=False,
    ),
    "601318": DailyMarketSnapshotRow(
        ticker="601318",
        close=50.0,
        vwap=50.2,
        adv_shares=8000000.0,
        tradable=True,
        upper_limit_hit=False,
        lower_limit_hit=False,
    ),
    "000333": DailyMarketSnapshotRow(
        ticker="000333",
        close=60.0,
        vwap=59.8,
        adv_shares=6000000.0,
        tradable=True,
        upper_limit_hit=False,
        lower_limit_hit=False,
    ),
    "600276": DailyMarketSnapshotRow(
        ticker="600276",
        close=40.0,
        vwap=39.8,
        adv_shares=1200000.0,
        tradable=False,
        upper_limit_hit=False,
        lower_limit_hit=False,
    ),
    "000858": DailyMarketSnapshotRow(
        ticker="000858",
        close=130.0,
        vwap=129.5,
        adv_shares=900000.0,
        tradable=True,
        upper_limit_hit=True,
        lower_limit_hit=False,
    ),
    "601012": DailyMarketSnapshotRow(
        ticker="601012",
        close=25.0,
        vwap=25.1,
        adv_shares=7000000.0,
        tradable=True,
        upper_limit_hit=False,
        lower_limit_hit=False,
    ),
}

MOCK_REFERENCE_DATA: dict[str, ReferenceSnapshotRow] = {
    "600519": ReferenceSnapshotRow(
        ticker="600519",
        industry="Consumer",
        benchmark_weight=0.13,
        issuer_total_shares=1250000000.0,
    ),
    "300750": ReferenceSnapshotRow(
        ticker="300750",
        industry="Industrials",
        benchmark_weight=0.16,
        issuer_total_shares=2440000000.0,
    ),
    "601318": ReferenceSnapshotRow(
        ticker="601318",
        industry="Financials",
        benchmark_weight=0.14,
        issuer_total_shares=94000000000.0,
    ),
    "000333": ReferenceSnapshotRow(
        ticker="000333",
        industry="Industrials",
        benchmark_weight=0.17,
        issuer_total_shares=100000.0,
    ),
    "600276": ReferenceSnapshotRow(
        ticker="600276",
        industry="Healthcare",
        benchmark_weight=0.11,
        issuer_total_shares=8200000000.0,
    ),
    "000858": ReferenceSnapshotRow(
        ticker="000858",
        industry="Consumer",
        benchmark_weight=0.12,
        issuer_total_shares=1250000000.0,
    ),
    "601012": ReferenceSnapshotRow(
        ticker="601012",
        industry="Industrials",
        benchmark_weight=0.17,
        issuer_total_shares=27000000000.0,
    ),
}

MOCK_INDEX_WEIGHTS: dict[str, list[IndexWeightRow]] = {
    "000300.SH": [
        IndexWeightRow(ticker="600519", target_weight=0.13),
        IndexWeightRow(ticker="300750", target_weight=0.16),
        IndexWeightRow(ticker="601318", target_weight=0.14),
        IndexWeightRow(ticker="000333", target_weight=0.17),
        IndexWeightRow(ticker="600276", target_weight=0.11),
        IndexWeightRow(ticker="000858", target_weight=0.12),
        IndexWeightRow(ticker="601012", target_weight=0.17),
    ]
}


class MockDataProvider:
    """Offline mock provider backed by a small in-memory sample universe."""

    provider_name = "mock"
    provider_metadata = {
        "provider_token_source": None,
        "approximation_notes": {
            "market": ["Mock provider uses fixed local sample values."],
            "reference": ["Mock provider uses fixed local sample values."],
            "target": ["Mock provider uses fixed local sample index weights."],
        },
    }

    def __init__(self) -> None:
        self._reports = {
            "market": {
                "provider_capability_status": "available",
                "fallback_notes": [],
                "permission_notes": [],
                "recommended_alternative_path": None,
            },
            "reference": {
                "provider_capability_status": "available",
                "fallback_notes": [],
                "permission_notes": [],
                "recommended_alternative_path": None,
            },
            "target": {
                "provider_capability_status": "available",
                "fallback_notes": [],
                "permission_notes": [],
                "recommended_alternative_path": None,
            },
        }

    def get_capability_report(self, feed_name: str) -> dict[str, object]:
        """Return the current provider capability report for one feed."""

        return dict(self._reports.get(feed_name, {}))

    def _resolve_market_rows(self, tickers: list[str]) -> list[DailyMarketSnapshotRow]:
        normalized_tickers = [normalize_ticker(ticker) for ticker in tickers]
        missing = [ticker for ticker in normalized_tickers if ticker not in MOCK_MARKET_DATA]
        if missing:
            raise InputValidationError(
                f"Mock provider is missing market data for ticker(s): {', '.join(missing)}"
            )
        return [MOCK_MARKET_DATA[ticker] for ticker in normalized_tickers]

    def _resolve_reference_rows(self, tickers: list[str]) -> list[ReferenceSnapshotRow]:
        normalized_tickers = [normalize_ticker(ticker) for ticker in tickers]
        missing = [ticker for ticker in normalized_tickers if ticker not in MOCK_REFERENCE_DATA]
        if missing:
            raise InputValidationError(
                f"Mock provider is missing reference data for ticker(s): {', '.join(missing)}"
            )
        return [MOCK_REFERENCE_DATA[ticker] for ticker in normalized_tickers]

    def get_daily_market_snapshot(
        self,
        tickers: list[str],
        as_of_date: str,
    ) -> list[DailyMarketSnapshotRow]:
        """Return a stable offline market snapshot."""

        _ = as_of_date
        return self._resolve_market_rows(tickers)

    def get_reference_snapshot(
        self,
        tickers: list[str],
        as_of_date: str,
    ) -> list[ReferenceSnapshotRow]:
        """Return a stable offline reference snapshot."""

        _ = as_of_date
        return self._resolve_reference_rows(tickers)

    def get_index_weights(
        self,
        index_code: str,
        as_of_date: str,
    ) -> list[IndexWeightRow]:
        """Return stable offline index weights."""

        _ = as_of_date
        if index_code not in MOCK_INDEX_WEIGHTS:
            raise InputValidationError(
                f"Mock provider does not support index_code {index_code!r}."
            )
        return MOCK_INDEX_WEIGHTS[index_code]
