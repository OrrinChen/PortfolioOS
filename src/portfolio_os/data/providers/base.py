"""Provider abstraction for lightweight static data preparation."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field


class DailyMarketSnapshotRow(BaseModel):
    """Provider-level daily market row."""

    ticker: str
    close: float = Field(gt=0.0)
    vwap: float = Field(gt=0.0)
    adv_shares: float = Field(gt=0.0)
    tradable: bool
    upper_limit_hit: bool
    lower_limit_hit: bool


class ReferenceSnapshotRow(BaseModel):
    """Provider-level reference row."""

    ticker: str
    industry: str
    benchmark_weight: float | None = Field(default=None, ge=0.0)
    issuer_total_shares: float | None = Field(default=None, ge=0.0)


class IndexWeightRow(BaseModel):
    """Provider-level index-weight row."""

    ticker: str
    target_weight: float = Field(ge=0.0)


@runtime_checkable
class DataProvider(Protocol):
    """Protocol for replaceable local or remote data providers."""

    provider_name: str
    provider_metadata: dict[str, Any]

    def get_daily_market_snapshot(
        self,
        tickers: list[str],
        as_of_date: str,
    ) -> list[DailyMarketSnapshotRow]:
        """Return the fields required to build `market.csv`."""

    def get_reference_snapshot(
        self,
        tickers: list[str],
        as_of_date: str,
    ) -> list[ReferenceSnapshotRow]:
        """Return the fields required to build `reference.csv`."""

    def get_index_weights(
        self,
        index_code: str,
        as_of_date: str,
    ) -> list[IndexWeightRow]:
        """Return the fields required to build `target.csv`."""
