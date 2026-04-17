from __future__ import annotations

import pandas as pd
import pytest

from portfolio_os.data.providers import get_data_provider
from portfolio_os.data.providers.tushare_provider import TushareProvider
from portfolio_os.domain.errors import InputValidationError, ProviderPermissionError, ProviderRuntimeError


def test_mock_provider_returns_stable_market_reference_and_index_results() -> None:
    provider = get_data_provider("mock")

    market_rows = provider.get_daily_market_snapshot(["600519", "000333"], "2026-03-23")
    reference_rows = provider.get_reference_snapshot(["600519", "000333"], "2026-03-23")
    index_rows = provider.get_index_weights("000300.SH", "2026-03-23")

    assert [row.ticker for row in market_rows] == ["600519", "000333"]
    assert market_rows[0].close == 1680.0
    assert reference_rows[1].industry == "Industrials"
    assert index_rows
    assert abs(sum(row.target_weight for row in index_rows) - 1.0) < 1e-9


def test_mock_provider_interface_fields_are_complete() -> None:
    provider = get_data_provider("mock")

    market_row = provider.get_daily_market_snapshot(["300750"], "2026-03-23")[0]
    reference_row = provider.get_reference_snapshot(["300750"], "2026-03-23")[0]
    index_row = provider.get_index_weights("000300.SH", "2026-03-23")[0]

    assert {
        "ticker",
        "close",
        "vwap",
        "adv_shares",
        "tradable",
        "upper_limit_hit",
        "lower_limit_hit",
    }.issubset(set(market_row.model_dump(mode="json").keys()))
    assert {
        "ticker",
        "industry",
        "benchmark_weight",
        "issuer_total_shares",
    }.issubset(set(reference_row.model_dump(mode="json").keys()))
    assert {
        "ticker",
        "target_weight",
    }.issubset(set(index_row.model_dump(mode="json").keys()))


def test_provider_factory_supports_mock_tushare_and_alpaca(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY", "demo_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "demo_secret")
    mock_provider = get_data_provider("mock")
    tushare_provider = get_data_provider("tushare", provider_token="demo_token")
    alpaca_provider = get_data_provider("alpaca")

    assert mock_provider.provider_name == "mock"
    assert tushare_provider.provider_name == "tushare"
    assert alpaca_provider.provider_name == "alpaca"


def test_tushare_provider_requires_token(monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setattr(
        "portfolio_os.data.providers.tushare_provider._read_windows_env_var",
        lambda _name: None,
    )

    with pytest.raises(InputValidationError, match="requires a token"):
        TushareProvider()

    with pytest.raises(InputValidationError, match="requires a token"):
        get_data_provider("tushare")


def test_tushare_provider_supports_windows_registry_fallback(monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setattr(
        "portfolio_os.data.providers.tushare_provider._read_windows_env_var",
        lambda _name: "registry_demo_token",
    )

    provider = TushareProvider()
    assert provider.provider_metadata["provider_token_source"] == "windows_registry"


def test_tushare_provider_permission_failure_sets_clear_capability_report(monkeypatch) -> None:
    provider = TushareProvider(token="demo_token", token_source="cli")

    def _raise_permission(*args, **kwargs):
        raise ProviderPermissionError("Tushare API error for index_weight: permission denied")

    monkeypatch.setattr(provider, "_call_api", _raise_permission)

    with pytest.raises(ProviderPermissionError, match="client-provided target.csv"):
        provider.get_index_weights("000300.SH", "2026-03-23")

    report = provider.get_capability_report("target")
    assert report["provider_capability_status"] == "unavailable"
    assert "index_weight_permission_missing" in report["permission_notes"]
    assert report["recommended_alternative_path"] == "provide_target_csv_and_continue"


def test_tushare_market_uses_akshare_limit_fallback_without_degradation(monkeypatch) -> None:
    provider = TushareProvider(token="demo_token", token_source="cli")

    def _call_api(api_name: str, *, params=None, fields=None):
        _ = params
        _ = fields
        if api_name == "daily":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "600519.SH",
                        "trade_date": "20260323",
                        "close": 1675.0,
                        "pre_close": 1660.0,
                        "vol": 12000.0,
                        "amount": 2010000.0,
                    }
                ]
            )
        if api_name == "stk_limit":
            raise ProviderPermissionError("stk_limit permission denied")
        raise AssertionError(f"unexpected api_name: {api_name}")

    monkeypatch.setattr(provider, "_call_api", _call_api)
    monkeypatch.setattr(
        provider,
        "_akshare_limits_for_tickers",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "trade_date": "20260323",
                    "up_limit": 1830.0,
                    "down_limit": 1490.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(provider, "_estimate_adv_shares", lambda *_args, **_kwargs: 100000.0)

    rows = provider.get_daily_market_snapshot(["600519"], "2026-03-23")
    assert rows and rows[0].ticker == "600519"
    report = provider.get_capability_report("market")
    assert report["provider_capability_status"] == "available"
    assert "akshare" in report["data_source_mix"]


def test_tushare_market_uses_tencent_limit_fallback_when_akshare_unavailable(monkeypatch) -> None:
    provider = TushareProvider(token="demo_token", token_source="cli")

    def _call_api(api_name: str, *, params=None, fields=None):
        _ = params
        _ = fields
        if api_name == "daily":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "600519.SH",
                        "trade_date": "20260323",
                        "close": 1675.0,
                        "pre_close": 1660.0,
                        "vol": 12000.0,
                        "amount": 2010000.0,
                    }
                ]
            )
        if api_name == "stk_limit":
            raise ProviderPermissionError("stk_limit permission denied")
        raise AssertionError(f"unexpected api_name: {api_name}")

    monkeypatch.setattr(provider, "_call_api", _call_api)
    monkeypatch.setattr(
        provider,
        "_akshare_limits_for_tickers",
        lambda **kwargs: pd.DataFrame(columns=["ts_code", "trade_date", "up_limit", "down_limit"]),
    )
    monkeypatch.setattr(
        provider,
        "_tencent_limits_for_tickers",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "trade_date": "20260323",
                    "up_limit": 1830.0,
                    "down_limit": 1490.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(provider, "_estimate_adv_shares", lambda *_args, **_kwargs: 100000.0)

    rows = provider.get_daily_market_snapshot(["600519"], "2026-03-23")
    assert rows and rows[0].ticker == "600519"
    report = provider.get_capability_report("market")
    assert report["provider_capability_status"] == "available"
    assert "tencent" in report["data_source_mix"]
    assert "tencent:stk_limit_filled_from_tencent" in report["fallback_chain_used"]


def test_tushare_reference_uses_akshare_profile_fallback_without_degradation(monkeypatch) -> None:
    provider = TushareProvider(token="demo_token", token_source="cli")

    def _call_api(api_name: str, *, params=None, fields=None):
        _ = params
        _ = fields
        if api_name in {"stock_basic", "daily_basic"}:
            raise ProviderPermissionError(f"{api_name} permission denied")
        if api_name == "bak_basic":
            return pd.DataFrame(columns=["ts_code", "industry", "total_share"])
        raise AssertionError(f"unexpected api_name: {api_name}")

    monkeypatch.setattr(provider, "_call_api", _call_api)
    monkeypatch.setattr(
        provider,
        "_akshare_individual_info",
        lambda _ticker: {"industry": "Consumer", "total_share": 1_250_000_000.0},
    )

    rows = provider.get_reference_snapshot(["600519"], "2026-03-23")
    assert rows and rows[0].industry == "Consumer"
    assert rows[0].issuer_total_shares == pytest.approx(1_250_000_000.0)
    report = provider.get_capability_report("reference")
    assert report["provider_capability_status"] == "available"
    assert "akshare" in report["data_source_mix"]


def test_tushare_reference_uses_xq_fallback_when_em_and_bak_basic_unavailable(monkeypatch) -> None:
    provider = TushareProvider(token="demo_token", token_source="cli")

    def _call_api(api_name: str, *, params=None, fields=None):
        _ = params
        _ = fields
        if api_name in {"stock_basic", "daily_basic", "bak_basic"}:
            raise ProviderPermissionError(f"{api_name} permission denied")
        raise AssertionError(f"unexpected api_name: {api_name}")

    monkeypatch.setattr(provider, "_call_api", _call_api)
    monkeypatch.setattr(
        provider,
        "_akshare_individual_info",
        lambda _ticker: (_ for _ in ()).throw(ProviderRuntimeError("em blocked")),
    )
    monkeypatch.setattr(
        provider,
        "_akshare_individual_info_xq",
        lambda _ticker: {"industry": "银行", "total_share": 19_405_900_000.0},
    )

    rows = provider.get_reference_snapshot(["000001"], "2026-03-23")
    assert rows and rows[0].industry == "银行"
    assert rows[0].issuer_total_shares == pytest.approx(19_405_900_000.0)
    report = provider.get_capability_report("reference")
    assert report["provider_capability_status"] == "available"
    assert "akshare" in report["data_source_mix"]


def test_tushare_provider_can_build_state_transition_daily_panel_history(monkeypatch) -> None:
    provider = TushareProvider(token="demo_token", token_source="cli")

    def _call_api(api_name: str, *, params=None, fields=None):
        _ = fields
        if api_name == "trade_cal":
            return pd.DataFrame(
                [
                    {"cal_date": "20260401", "is_open": 1},
                    {"cal_date": "20260402", "is_open": 1},
                ]
            )
        if api_name == "daily":
            trade_date = params["trade_date"]
            if trade_date == "20260401":
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260401",
                            "open": 10.0,
                            "high": 11.0,
                            "low": 9.9,
                            "close": 11.0,
                            "pre_close": 10.0,
                            "vol": 10000.0,
                            "amount": 10500.0,
                        }
                    ]
                )
            if trade_date == "20260402":
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260402",
                            "open": 11.1,
                            "high": 11.2,
                            "low": 10.8,
                            "close": 10.9,
                            "pre_close": 11.0,
                            "vol": 9000.0,
                            "amount": 9900.0,
                        }
                    ]
                )
        if api_name == "stk_limit":
            trade_date = params["trade_date"]
            if trade_date == "20260401":
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260401",
                            "up_limit": 11.0,
                            "down_limit": 9.0,
                        }
                    ]
                )
            if trade_date == "20260402":
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260402",
                            "up_limit": 12.1,
                            "down_limit": 9.9,
                        }
                    ]
                )
        raise AssertionError(f"unexpected api_name: {api_name} with params={params}")

    monkeypatch.setattr(provider, "_call_api", _call_api)
    monkeypatch.setattr(
        provider,
        "get_reference_snapshot",
        lambda tickers, as_of_date: [
            type(
                "_Row",
                (),
                {
                    "ticker": "000001",
                    "industry": "Industrials",
                    "issuer_total_shares": 10_000_000.0,
                    "model_dump": lambda self, mode="json": {
                        "ticker": "000001",
                        "industry": "Industrials",
                        "benchmark_weight": None,
                        "issuer_total_shares": 10_000_000.0,
                    },
                },
            )()
        ],
    )

    frame = provider.get_state_transition_daily_panel(
        ["000001"],
        "2026-04-01",
        "2026-04-02",
    )

    assert list(frame.columns) == [
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "upper_limit_price",
        "lower_limit_price",
        "tradable",
        "industry",
        "issuer_total_shares",
    ]
    assert frame.loc[0, "date"] == "2026-04-01"
    assert frame.loc[0, "ticker"] == "000001"
    assert frame.loc[0, "volume"] == pytest.approx(1_000_000.0)
    assert frame.loc[0, "amount"] == pytest.approx(10_500_000.0)
    assert frame.loc[0, "industry"] == "Industrials"


def test_tushare_provider_state_transition_history_falls_back_when_trade_cal_unavailable(
    monkeypatch,
) -> None:
    provider = TushareProvider(token="demo_token", token_source="cli")

    def _call_api(api_name: str, *, params=None, fields=None):
        _ = fields
        if api_name == "trade_cal":
            raise ProviderPermissionError("trade_cal permission denied")
        if api_name == "daily":
            ts_code = params.get("ts_code")
            if ts_code == "000001.SZ":
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260402",
                            "open": 11.1,
                            "high": 11.2,
                            "low": 10.8,
                            "close": 10.9,
                            "pre_close": 11.0,
                            "vol": 9000.0,
                            "amount": 9900.0,
                        },
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260401",
                            "open": 10.0,
                            "high": 11.0,
                            "low": 9.9,
                            "close": 11.0,
                            "pre_close": 10.0,
                            "vol": 10000.0,
                            "amount": 10500.0,
                        },
                    ]
                )
        if api_name == "stk_limit":
            trade_date = params["trade_date"]
            if trade_date == "20260401":
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260401",
                            "up_limit": 11.0,
                            "down_limit": 9.0,
                        }
                    ]
                )
            if trade_date == "20260402":
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260402",
                            "up_limit": 12.1,
                            "down_limit": 9.9,
                        }
                    ]
                )
        raise AssertionError(f"unexpected api_name: {api_name} with params={params}")

    monkeypatch.setattr(provider, "_call_api", _call_api)
    monkeypatch.setattr(
        provider,
        "get_reference_snapshot",
        lambda tickers, as_of_date: [
            type(
                "_Row",
                (object,),
                {
                    "ticker": "000001",
                    "industry": "Industrials",
                    "issuer_total_shares": 10_000_000.0,
                    "model_dump": lambda self, mode="json": {
                        "ticker": "000001",
                        "industry": "Industrials",
                        "benchmark_weight": None,
                        "issuer_total_shares": 10_000_000.0,
                    },
                },
            )()
        ],
    )

    frame = provider.get_state_transition_daily_panel(
        ["000001"],
        "2026-04-01",
        "2026-04-02",
    )

    report = provider.get_capability_report("state_transition_daily_panel")
    assert list(frame["date"]) == ["2026-04-01", "2026-04-02"]
    assert report["provider_capability_status"] == "available"
    assert "trade_cal_permission_missing" in report["fallback_notes"]


def test_tushare_provider_state_transition_history_uses_price_band_approximation_when_stk_limit_unavailable(
    monkeypatch,
) -> None:
    provider = TushareProvider(token="demo_token", token_source="cli")

    def _call_api(api_name: str, *, params=None, fields=None):
        _ = fields
        if api_name == "trade_cal":
            return pd.DataFrame(
                [
                    {"cal_date": "20260401", "is_open": 1},
                ]
            )
        if api_name == "daily":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20260401",
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.9,
                        "close": 11.0,
                        "pre_close": 10.0,
                        "vol": 10000.0,
                        "amount": 10500.0,
                    }
                ]
            )
        if api_name == "stk_limit":
            raise ProviderPermissionError("stk_limit permission denied")
        raise AssertionError(f"unexpected api_name: {api_name} with params={params}")

    monkeypatch.setattr(provider, "_call_api", _call_api)
    monkeypatch.setattr(
        provider,
        "_akshare_limits_for_tickers",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("akshare fallback should not run")),
    )
    monkeypatch.setattr(
        provider,
        "_tencent_limits_for_tickers",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("tencent fallback should not run")),
    )
    monkeypatch.setattr(
        provider,
        "get_reference_snapshot",
        lambda tickers, as_of_date: [
            type(
                "_Row",
                (object,),
                {
                    "ticker": "000001",
                    "industry": "Industrials",
                    "issuer_total_shares": 10_000_000.0,
                    "model_dump": lambda self, mode="json": {
                        "ticker": "000001",
                        "industry": "Industrials",
                        "benchmark_weight": None,
                        "issuer_total_shares": 10_000_000.0,
                    },
                },
            )()
        ],
    )

    frame = provider.get_state_transition_daily_panel(
        ["000001"],
        "2026-04-01",
        "2026-04-01",
    )

    report = provider.get_capability_report("state_transition_daily_panel")
    assert frame.loc[0, "upper_limit_price"] == pytest.approx(11.0)
    assert frame.loc[0, "lower_limit_price"] == pytest.approx(9.0)
    assert report["provider_capability_status"] == "degraded"
    assert "stk_limit_unavailable_used_price_band_approximation" in report["fallback_notes"]
