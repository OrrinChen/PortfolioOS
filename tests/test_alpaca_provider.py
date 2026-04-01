from __future__ import annotations

import os

import pandas as pd
import pytest

from portfolio_os.data.providers import get_data_provider
from portfolio_os.data.providers.alpaca_provider import AlpacaProvider


def _build_fake_bars_frame() -> pd.DataFrame:
    rows = []
    for ticker, close_1, close_2, vol_1, vol_2 in [
        ("AAPL", 190.0, 192.0, 100_000_000.0, 90_000_000.0),
        ("MSFT", 410.0, 415.0, 40_000_000.0, 38_000_000.0),
        ("GOOGL", 155.0, 156.0, 30_000_000.0, 29_000_000.0),
    ]:
        rows.append({"symbol": ticker, "timestamp": "2026-03-18T00:00:00Z", "close": close_1, "volume": vol_1})
        rows.append({"symbol": ticker, "timestamp": "2026-03-19T00:00:00Z", "close": close_2, "volume": vol_2})
    return pd.DataFrame(rows)


def test_provider_factory_supports_alpaca(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY", "demo_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "demo_secret")
    provider = get_data_provider("alpaca")
    assert provider.provider_name == "alpaca"


def test_alpaca_provider_returns_complete_market_and_reference_fields(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY", "demo_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "demo_secret")
    provider = AlpacaProvider()

    class _BarsResponse:
        def __init__(self, frame: pd.DataFrame) -> None:
            self.df = frame

    class _FakeStockClient:
        def get_stock_bars(self, _request):
            return _BarsResponse(_build_fake_bars_frame())

    class _DummyRequest:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    class _DummyTimeFrame:
        Day = "day"

    monkeypatch.setattr(provider, "_stock_client", lambda: _FakeStockClient())
    monkeypatch.setattr(
        provider,
        "_load_alpaca_modules",
        lambda: (object, _DummyRequest, _DummyTimeFrame, object),
    )
    monkeypatch.setattr(
        provider,
        "_asset_tradeability_map",
        lambda tickers: {ticker: True for ticker in tickers},
    )
    monkeypatch.setattr(
        provider,
        "_fetch_yfinance_info",
        lambda ticker: {
            "sector": "Technology" if ticker != "GOOGL" else "Communication Services",
            "sharesOutstanding": 10_000_000_000,
        },
    )

    market_rows = provider.get_daily_market_snapshot(["AAPL", "MSFT", "GOOGL"], "2026-03-19")
    reference_rows = provider.get_reference_snapshot(["AAPL", "MSFT", "GOOGL"], "2026-03-19")

    assert len(market_rows) == 3
    assert len(reference_rows) == 3
    assert all(row.close > 0 for row in market_rows)
    assert all(row.adv_shares > 0 for row in market_rows)
    assert all(row.industry for row in reference_rows)
    assert all((row.issuer_total_shares or 0) > 0 for row in reference_rows)

    market_report = provider.get_capability_report("market")
    reference_report = provider.get_capability_report("reference")
    assert market_report["provider_capability_status"] == "available"
    assert "alpaca" in market_report["data_source_mix"]
    assert reference_report["provider_capability_status"] in {"available", "degraded"}


@pytest.mark.integration
def test_alpaca_provider_integration_live_data() -> None:
    api_key = str(os.getenv("ALPACA_API_KEY", "")).strip()
    secret_key = str(os.getenv("ALPACA_SECRET_KEY", "")).strip()
    if not api_key or not secret_key:
        pytest.skip("ALPACA_API_KEY/ALPACA_SECRET_KEY not configured")

    provider = AlpacaProvider(api_key=api_key, secret_key=secret_key, paper=True)
    tickers = ["AAPL", "MSFT", "GOOGL"]
    market_rows = provider.get_daily_market_snapshot(tickers, "2026-03-20")
    reference_rows = provider.get_reference_snapshot(tickers, "2026-03-20")

    assert len(market_rows) == len(tickers)
    assert len(reference_rows) == len(tickers)
    assert all(row.close > 0 for row in market_rows)
    assert all(str(row.industry).strip() for row in reference_rows)
