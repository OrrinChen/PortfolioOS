from __future__ import annotations

import importlib.util
import json
import sys
import types
from datetime import date
from pathlib import Path

import pandas as pd


def _load_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "generate_risk_inputs.py"
    spec = importlib.util.spec_from_file_location("generate_risk_inputs_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    payload = [header, *rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(payload) + "\n", encoding="utf-8")


def test_collect_tickers_from_samples_uses_replay_fallback_for_cn(tmp_path: Path) -> None:
    module = _load_module()
    samples_root = tmp_path / "samples"
    replay_root = tmp_path / "replay_samples"

    _write_csv(samples_root / "us" / "sample_us_01" / "holdings.csv", "ticker,quantity", ["AAPL,10"])
    _write_csv(replay_root / "sample_01" / "holdings.csv", "ticker,quantity", ["600519,100", "000858,20"])
    _write_csv(replay_root / "sample_01" / "target.csv", "ticker,target_weight", ["600276,0.1", "000858,0.2"])

    tickers = module.collect_tickers_from_samples(
        samples_root=samples_root,
        replay_samples_root=replay_root,
        market="cn",
    )

    assert tickers == ["000858", "600276", "600519"]


def test_collect_tickers_from_samples_uses_us_subtree_only_for_us(tmp_path: Path) -> None:
    module = _load_module()
    samples_root = tmp_path / "samples"
    replay_root = tmp_path / "replay_samples"

    _write_csv(samples_root / "us" / "sample_us_01" / "holdings.csv", "ticker,quantity", ["AAPL,10", "MSFT,8"])
    _write_csv(samples_root / "sample_01" / "holdings.csv", "ticker,quantity", ["600519,100"])
    _write_csv(replay_root / "sample_01" / "target.csv", "ticker,target_weight", ["000858,0.2"])

    tickers = module.collect_tickers_from_samples(
        samples_root=samples_root,
        replay_samples_root=replay_root,
        market="us",
    )

    assert tickers == ["AAPL", "MSFT"]


def test_collect_tickers_from_samples_excludes_us_subtree_for_cn_when_cn_present(tmp_path: Path) -> None:
    module = _load_module()
    samples_root = tmp_path / "samples"
    replay_root = tmp_path / "replay_samples"

    _write_csv(samples_root / "us" / "sample_us_01" / "holdings.csv", "ticker,quantity", ["AAPL,10"])
    _write_csv(samples_root / "sample_01" / "holdings.csv", "ticker,quantity", ["600519,100"])
    _write_csv(samples_root / "sample_01" / "target.csv", "ticker,target_weight", ["000858,0.2"])
    _write_csv(replay_root / "sample_02" / "target.csv", "ticker,target_weight", ["600276,0.1"])

    tickers = module.collect_tickers_from_samples(
        samples_root=samples_root,
        replay_samples_root=replay_root,
        market="cn",
    )

    assert tickers == ["000858", "600519"]


def test_collect_tickers_from_universe_file_reads_comments_and_normalizes(tmp_path: Path) -> None:
    module = _load_module()
    universe_file = tmp_path / "universe.txt"
    universe_file.write_text(
        "# core\nAAPL\nmsft\n\n  nvda  \n# stress\nPLTR\n",
        encoding="utf-8",
    )

    tickers = module.collect_tickers_from_universe_file(universe_file)

    assert tickers == ["AAPL", "MSFT", "NVDA", "PLTR"]


def test_generate_risk_inputs_uses_universe_file_tickers(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    output_dir = tmp_path / "risk_inputs"
    universe_file = tmp_path / "universe.txt"
    universe_file.write_text("AAPL\nMSFT\nNVDA\nPLTR\nSOFI\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def _fake_build_close_price_frame(**kwargs):
        captured["tickers"] = kwargs["tickers"]
        index = pd.to_datetime(
            ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]
        )
        prices = pd.DataFrame(
            {
                ticker: [10.0, 10.5, 10.8, 11.0, 11.1]
                for ticker in kwargs["tickers"]
            },
            index=index,
        )
        return prices, [], {"yfinance": len(kwargs["tickers"])}

    monkeypatch.setattr(module, "build_close_price_frame", _fake_build_close_price_frame)
    monkeypatch.setattr(
        module,
        "build_factor_exposure_frame",
        lambda **kwargs: pd.DataFrame(
            [{"ticker": ticker, "factor": "Tech", "exposure": 1.0} for ticker in kwargs["tickers"]]
        ),
    )

    config = module.RiskInputsConfig(
        tickers_from_samples=False,
        manual_tickers=[],
        universe_file=universe_file,
        market="us",
        lookback_days=252,
        end_date=date(2026, 1, 29),
        output_dir=output_dir,
        cool_down=0.0,
    )
    rc = module.generate_risk_inputs(config, logger=lambda _msg: None)

    assert rc == 0
    assert captured["tickers"] == ["AAPL", "MSFT", "NVDA", "PLTR", "SOFI"]
    manifest = json.loads((output_dir / "risk_inputs_manifest.json").read_text(encoding="utf-8"))
    assert manifest["ticker_count"] == 5
    assert manifest["tickers"] == ["AAPL", "MSFT", "NVDA", "PLTR", "SOFI"]


def test_build_parser_accepts_universe_file_argument() -> None:
    module = _load_module()
    parser = module.build_parser()

    args = parser.parse_args(
        [
            "--market",
            "us",
            "--end-date",
            "2026-03-27",
            "--universe-file",
            "data/universe/us_equity_expanded_tickers.txt",
        ]
    )

    assert Path(args.universe_file) == Path("data/universe/us_equity_expanded_tickers.txt")


def test_load_yfinance_module_sets_writable_cache(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()
    calls: dict[str, str] = {}
    fake_yf = types.SimpleNamespace(
        set_tz_cache_location=lambda path: calls.setdefault("cache_path", str(path))
    )
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    monkeypatch.setattr(module, "ROOT", tmp_path)

    loaded = module._load_yfinance_module()

    assert loaded is fake_yf
    assert "cache_path" in calls
    expected = (tmp_path / "outputs" / ".yfinance_cache").resolve()
    assert Path(calls["cache_path"]).resolve() == expected
    assert expected.exists()


def test_compute_returns_from_prices_calculates_pct_change() -> None:
    module = _load_module()
    index = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"])
    prices = pd.DataFrame(
        {
            "AAA": [100.0, 110.0, 121.0],
            "BBB": [50.0, 40.0, 44.0],
        },
        index=index,
    )

    returns, excluded = module.compute_returns_from_prices(prices)

    assert excluded == []
    assert round(float(returns.loc[pd.Timestamp("2026-01-02"), "AAA"]), 6) == 0.1
    assert round(float(returns.loc[pd.Timestamp("2026-01-03"), "BBB"]), 6) == 0.1


def test_returns_wide_to_long_sorts_and_rounds() -> None:
    module = _load_module()
    frame = pd.DataFrame(
        {
            "BBB": [0.123456789, -0.02],
            "AAA": [0.4, 0.001234567],
        },
        index=pd.to_datetime(["2026-01-03", "2026-01-02"]),
    )
    frame.index.name = "date"

    long_frame = module.returns_wide_to_long(frame)

    assert list(long_frame.columns) == ["date", "ticker", "return"]
    assert long_frame.iloc[0]["date"] == "2026-01-02"
    assert long_frame.iloc[0]["ticker"] == "AAA"
    assert abs(float(long_frame.iloc[-1]["return"]) - 0.123457) < 1e-9


def test_generate_risk_inputs_records_high_nan_exclusion(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    output_dir = tmp_path / "risk_inputs"
    tickers = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]

    index = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"])
    prices = pd.DataFrame(
        {
            "AAA": [10, 11, 12, 13, 14],
            "BBB": [20, None, None, None, 24],
            "CCC": [30, 31, 32, 33, 34],
            "DDD": [40, 41, 42, 43, 44],
            "EEE": [50, 51, 52, 53, 54],
            "FFF": [60, 61, 62, 63, 64],
        },
        index=index,
    )

    def _fake_build_close_price_frame(**kwargs):
        _ = kwargs
        return prices, [], {"tushare": 6}

    monkeypatch.setattr(module, "build_close_price_frame", _fake_build_close_price_frame)
    monkeypatch.setattr(
        module,
        "build_factor_exposure_frame",
        lambda **kwargs: pd.DataFrame(
            [{"ticker": ticker, "factor": "IndustryA", "exposure": 1.0} for ticker in tickers if ticker != "BBB"]
        ),
    )

    config = module.RiskInputsConfig(
        tickers_from_samples=False,
        manual_tickers=tickers,
        market="cn",
        lookback_days=252,
        end_date=date(2026, 1, 29),
        output_dir=output_dir,
        cool_down=0.0,
    )
    rc = module.generate_risk_inputs(config, logger=lambda _msg: None)

    assert rc == 0
    manifest = json.loads((output_dir / "risk_inputs_manifest.json").read_text(encoding="utf-8"))
    assert "BBB" in manifest["tickers_excluded_high_nan"]
    assert manifest["ticker_count"] == 5
