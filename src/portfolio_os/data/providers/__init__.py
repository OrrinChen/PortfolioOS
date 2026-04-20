"""Provider registry for data-preparation builders."""

from __future__ import annotations

from portfolio_os.data.providers.alpaca_provider import AlpacaProvider
from portfolio_os.data.providers.base import DataProvider
from portfolio_os.data.providers.mock import MockDataProvider
from portfolio_os.data.providers.tushare_provider import TushareProvider
from portfolio_os.domain.errors import InputValidationError


def get_data_provider(
    name: str,
    *,
    provider_token: str | None = None,
) -> DataProvider:
    """Return a configured data provider by name."""

    normalized = str(name).strip().lower()
    if normalized == "mock":
        return MockDataProvider()
    if normalized == "tushare":
        return TushareProvider(token=provider_token, token_source="cli" if provider_token else None)
    if normalized == "alpaca":
        return AlpacaProvider()
    raise InputValidationError(
        f"Unsupported data provider {name!r}. Available providers: mock, tushare, alpaca."
    )
