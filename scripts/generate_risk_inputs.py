from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from portfolio_os.data.loaders import normalize_ticker
from portfolio_os.data.providers import get_data_provider


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data" / "risk_inputs"
DEFAULT_SAMPLES_ROOT = ROOT / "data" / "samples"
DEFAULT_REPLAY_SAMPLES_ROOT = ROOT / "data" / "replay_samples"
TICKER_TIMEOUT_SECONDS = 30
MAX_NAN_RATIO = 0.2


@dataclass
class RiskInputsConfig:
    tickers_from_samples: bool
    manual_tickers: list[str]
    market: str
    lookback_days: int
    end_date: date
    output_dir: Path
    cool_down: float
    universe_file: Path | None = None


def _now_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _log(message: str) -> None:
    print(f"[{_now_timestamp()}] {message}")


def _parse_date(raw: str) -> date:
    return datetime.strptime(str(raw), "%Y-%m-%d").date()


def _read_tickers_from_csv(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if "ticker" not in set(reader.fieldnames or []):
            return set()
        output: set[str] = set()
        for row in reader:
            ticker_raw = str(row.get("ticker", "")).strip()
            if ticker_raw:
                output.add(normalize_ticker(ticker_raw))
    return output


def _sample_directories(
    *,
    samples_root: Path,
    replay_samples_root: Path,
    market: str,
) -> list[Path]:
    normalized_market = str(market).strip().lower()
    directories: list[Path] = []
    if normalized_market == "us":
        us_root = samples_root / "us"
        if us_root.exists():
            directories.extend(sorted(path for path in us_root.iterdir() if path.is_dir()))
        return directories

    if samples_root.exists():
        directories.extend(
            sorted(
                path
                for path in samples_root.iterdir()
                if path.is_dir() and path.name.strip().lower() != "us"
            )
        )
    has_cn_files = any(
        (path / "holdings.csv").exists() or (path / "target.csv").exists() for path in directories
    )
    if has_cn_files:
        return directories
    if replay_samples_root.exists():
        directories.extend(sorted(path for path in replay_samples_root.iterdir() if path.is_dir()))
    return directories


def collect_tickers_from_samples(
    *,
    samples_root: Path = DEFAULT_SAMPLES_ROOT,
    replay_samples_root: Path = DEFAULT_REPLAY_SAMPLES_ROOT,
    market: str,
) -> list[str]:
    tickers: set[str] = set()
    for sample_dir in _sample_directories(
        samples_root=samples_root,
        replay_samples_root=replay_samples_root,
        market=market,
    ):
        tickers.update(_read_tickers_from_csv(sample_dir / "holdings.csv"))
        tickers.update(_read_tickers_from_csv(sample_dir / "target.csv"))
    return sorted(tickers)


def _parse_manual_tickers(raw: str | None) -> list[str]:
    if raw is None:
        return []
    values = [item.strip() for item in str(raw).split(",")]
    normalized: set[str] = set()
    for value in values:
        if value:
            normalized.add(normalize_ticker(value))
    return sorted(normalized)


def collect_tickers_from_universe_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Universe file not found: {path}")
    normalized: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = str(raw_line).strip()
        if not line or line.startswith("#"):
            continue
        normalized.add(normalize_ticker(line).upper())
    return sorted(normalized)


def _run_with_timeout(task: Callable[[], Any], *, timeout_seconds: int) -> Any:
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(task)
        return future.result(timeout=timeout_seconds)


def _pick_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {str(column).strip().lower(): column for column in frame.columns}
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if key in normalized:
            return str(normalized[key])
    return None


def _frame_to_close_series(frame: pd.DataFrame, *, ticker: str, lookback_days: int) -> pd.Series:
    if frame is None or frame.empty:
        raise ValueError(f"No historical rows for ticker {ticker}.")
    date_column = _pick_column(frame, ["trade_date", "date", "日期", "datetime"])
    close_column = _pick_column(frame, ["close", "收盘", "adj_close"])
    if date_column is None or close_column is None:
        raise ValueError(f"Unable to locate date/close columns for ticker {ticker}.")
    working = frame[[date_column, close_column]].copy()
    working.columns = ["date", "close"]
    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    working["close"] = pd.to_numeric(working["close"], errors="coerce")
    working = working.dropna(subset=["date", "close"]).copy()
    if working.empty:
        raise ValueError(f"No valid date/close rows for ticker {ticker}.")
    working["date"] = working["date"].dt.date
    working = (
        working.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .tail(int(lookback_days))
    )
    if working.empty:
        raise ValueError(f"No rows after lookback trim for ticker {ticker}.")
    series = pd.Series(working["close"].to_numpy(), index=pd.to_datetime(working["date"]), name=ticker)
    return series


def _fetch_cn_tushare_series(
    provider: Any,
    *,
    ticker: str,
    end_date: date,
    lookback_days: int,
) -> pd.Series:
    if not hasattr(provider, "_call_api") or not hasattr(provider, "_ticker_to_ts_code"):
        raise ValueError("Tushare provider does not expose expected helper methods.")
    calendar_window = max(int(lookback_days) * 3, int(lookback_days) + 90)
    start_date = (end_date - timedelta(days=calendar_window)).strftime("%Y%m%d")
    end_date_text = end_date.strftime("%Y%m%d")
    ts_code = str(provider._ticker_to_ts_code(ticker))  # noqa: SLF001 - reuse existing provider transform

    def _task() -> pd.DataFrame:
        return provider._call_api(  # noqa: SLF001 - reuse existing provider transport
            "daily",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date_text},
            fields="trade_date,close",
        )

    frame = _run_with_timeout(_task, timeout_seconds=TICKER_TIMEOUT_SECONDS)
    return _frame_to_close_series(frame, ticker=ticker, lookback_days=lookback_days)


def _fetch_cn_akshare_series(
    *,
    ticker: str,
    end_date: date,
    lookback_days: int,
) -> pd.Series:
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("AKShare is unavailable.") from exc

    calendar_window = max(int(lookback_days) * 3, int(lookback_days) + 90)
    start_date = (end_date - timedelta(days=calendar_window)).strftime("%Y%m%d")
    end_date_text = end_date.strftime("%Y%m%d")

    def _task() -> pd.DataFrame:
        return ak.stock_zh_a_hist(
            symbol=normalize_ticker(ticker),
            period="daily",
            start_date=start_date,
            end_date=end_date_text,
            adjust="",
        )

    frame = _run_with_timeout(_task, timeout_seconds=TICKER_TIMEOUT_SECONDS)
    return _frame_to_close_series(frame, ticker=ticker, lookback_days=lookback_days)


def _load_yfinance_module():
    try:
        import yfinance as yf  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("yfinance is required for market=us.") from exc
    cache_dir = ROOT / "outputs" / ".yfinance_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    if hasattr(yf, "set_tz_cache_location"):
        yf.set_tz_cache_location(str(cache_dir))
    return yf


def _fetch_us_yfinance_series(
    *,
    ticker: str,
    end_date: date,
    lookback_days: int,
) -> pd.Series:
    yf = _load_yfinance_module()
    calendar_window = max(int(lookback_days) * 3, int(lookback_days) + 90)
    start_date = (end_date - timedelta(days=calendar_window)).strftime("%Y-%m-%d")
    end_exclusive = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

    def _task() -> pd.DataFrame:
        return yf.download(
            ticker,
            start=start_date,
            end=end_exclusive,
            progress=False,
            auto_adjust=False,
            timeout=TICKER_TIMEOUT_SECONDS,
        )

    frame = _run_with_timeout(_task, timeout_seconds=TICKER_TIMEOUT_SECONDS)
    if frame is None or getattr(frame, "empty", True):
        raise ValueError(f"No yfinance rows for ticker {ticker}.")
    if isinstance(frame.columns, pd.MultiIndex):
        if ("Close", ticker) in frame.columns:
            close = frame[("Close", ticker)]
        else:
            close_columns = [column for column in frame.columns if str(column[0]).strip().lower() == "close"]
            if not close_columns:
                raise ValueError(f"Unable to locate Close column for ticker {ticker}.")
            close = frame[close_columns[0]]
    else:
        if "Close" not in frame.columns:
            raise ValueError(f"Unable to locate Close column for ticker {ticker}.")
        close = frame["Close"]
    close_series = pd.to_numeric(close, errors="coerce").dropna()
    if close_series.empty:
        raise ValueError(f"Close series is empty for ticker {ticker}.")
    indexed = pd.Series(
        close_series.to_numpy(),
        index=pd.to_datetime(close_series.index).tz_localize(None),
        name=ticker,
    )
    indexed = indexed.sort_index().tail(int(lookback_days))
    if indexed.empty:
        raise ValueError(f"No rows after lookback trim for ticker {ticker}.")
    return indexed


def build_close_price_frame(
    *,
    market: str,
    tickers: list[str],
    end_date: date,
    lookback_days: int,
    cool_down: float,
    logger: Callable[[str], None] = _log,
) -> tuple[pd.DataFrame, list[dict[str, str]], dict[str, int]]:
    close_by_ticker: dict[str, pd.Series] = {}
    failures: list[dict[str, str]] = []
    source_usage: dict[str, int] = {}
    provider = get_data_provider("tushare") if market == "cn" else None

    for idx, ticker in enumerate(tickers):
        try:
            if market == "cn":
                try:
                    series = _fetch_cn_tushare_series(
                        provider,
                        ticker=ticker,
                        end_date=end_date,
                        lookback_days=lookback_days,
                    )
                    source = "tushare"
                except Exception:
                    series = _fetch_cn_akshare_series(
                        ticker=ticker,
                        end_date=end_date,
                        lookback_days=lookback_days,
                    )
                    source = "akshare_fallback"
            else:
                series = _fetch_us_yfinance_series(
                    ticker=ticker,
                    end_date=end_date,
                    lookback_days=lookback_days,
                )
                source = "yfinance"
            close_by_ticker[ticker] = series.rename(ticker)
            source_usage[source] = source_usage.get(source, 0) + 1
            logger(f"Fetched {ticker} ({source}, {len(series)} rows).")
        except FuturesTimeoutError:
            failures.append({"ticker": ticker, "error": f"timeout after {TICKER_TIMEOUT_SECONDS}s"})
            logger(f"Warning: {ticker} failed due to timeout.")
        except Exception as exc:
            failures.append({"ticker": ticker, "error": str(exc)})
            logger(f"Warning: {ticker} failed ({exc}).")
        if cool_down > 0 and idx < len(tickers) - 1:
            time.sleep(cool_down)

    frame = pd.DataFrame(close_by_ticker).sort_index()
    frame.index.name = "date"
    return frame, failures, source_usage


def compute_returns_from_prices(
    close_prices: pd.DataFrame,
    *,
    max_nan_ratio: float = MAX_NAN_RATIO,
) -> tuple[pd.DataFrame, list[str]]:
    if close_prices.empty:
        return pd.DataFrame(), []
    returns = close_prices.sort_index().pct_change(fill_method=None).iloc[1:].copy()
    if returns.empty:
        return returns, []
    nan_ratio = returns.isna().mean()
    excluded = sorted(
        str(column)
        for column, ratio in nan_ratio.items()
        if pd.notna(ratio) and float(ratio) > float(max_nan_ratio)
    )
    filtered = returns.drop(columns=excluded, errors="ignore")
    filtered = filtered.dropna(how="all")
    filtered.index.name = "date"
    return filtered, excluded


def returns_wide_to_long(returns_wide: pd.DataFrame) -> pd.DataFrame:
    if returns_wide.empty:
        return pd.DataFrame(columns=["date", "ticker", "return"])
    melted = (
        returns_wide.reset_index()
        .melt(id_vars=["date"], var_name="ticker", value_name="return")
        .dropna(subset=["return"])
    )
    melted["date"] = pd.to_datetime(melted["date"]).dt.strftime("%Y-%m-%d")
    melted["ticker"] = melted["ticker"].map(lambda value: normalize_ticker(str(value)))
    melted["return"] = pd.to_numeric(melted["return"], errors="coerce").round(6)
    melted = melted.dropna(subset=["return"])
    return melted.sort_values(["date", "ticker"]).reset_index(drop=True)


def _load_local_industry_map(tickers: list[str]) -> dict[str, str]:
    candidate_roots = [
        ROOT / "data" / "replay_samples",
        ROOT / "data" / "sample",
        ROOT / "data" / "samples" / "us",
    ]
    mapping: dict[str, str] = {}
    required = set(tickers)
    for root in candidate_roots:
        if not root.exists():
            continue
        for path in root.rglob("reference*.csv"):
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                if "ticker" not in set(reader.fieldnames or []) or "industry" not in set(reader.fieldnames or []):
                    continue
                for row in reader:
                    ticker = normalize_ticker(str(row.get("ticker", "")).strip()) if str(row.get("ticker", "")).strip() else ""
                    industry = str(row.get("industry", "")).strip()
                    if ticker and industry and ticker in required and ticker not in mapping:
                        mapping[ticker] = industry
    return mapping


def _build_one_hot_factor_exposure(industry_by_ticker: dict[str, str], ordered_tickers: list[str]) -> pd.DataFrame:
    if not ordered_tickers:
        return pd.DataFrame(columns=["ticker", "factor", "exposure"])
    factors = sorted(set(industry_by_ticker[ticker] for ticker in ordered_tickers))
    rows: list[dict[str, Any]] = []
    for ticker in ordered_tickers:
        industry = industry_by_ticker[ticker]
        for factor in factors:
            rows.append(
                {
                    "ticker": ticker,
                    "factor": factor,
                    "exposure": 1.0 if industry == factor else 0.0,
                }
            )
    return pd.DataFrame(rows, columns=["ticker", "factor", "exposure"]).sort_values(
        ["ticker", "factor"]
    )


def build_factor_exposure_frame(
    *,
    market: str,
    tickers: list[str],
    as_of_date: str,
    logger: Callable[[str], None] = _log,
) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame(columns=["ticker", "factor", "exposure"])
    industry_by_ticker: dict[str, str] = {}
    ordered = list(tickers)

    if market == "cn":
        try:
            provider = get_data_provider("tushare")
            rows = provider.get_reference_snapshot(ordered, as_of_date=as_of_date)
            for row in rows:
                ticker = normalize_ticker(str(row.ticker))
                industry = str(row.industry).strip()
                if ticker in ordered and industry:
                    industry_by_ticker[ticker] = industry
        except Exception as exc:
            logger(f"Warning: CN factor exposure lookup failed via provider ({exc}); falling back to local references.")
    else:
        yf = _load_yfinance_module()
        for ticker in ordered:
            sector = "Unknown"
            try:
                info = _run_with_timeout(
                    lambda: (getattr(yf.Ticker(ticker), "info", {}) or {}),
                    timeout_seconds=TICKER_TIMEOUT_SECONDS,
                )
                if isinstance(info, dict):
                    candidate = str(info.get("sector", "")).strip()
                    if candidate:
                        sector = candidate
            except Exception:
                sector = "Unknown"
            industry_by_ticker[ticker] = sector

    local_map = _load_local_industry_map(ordered)
    for ticker in ordered:
        if ticker not in industry_by_ticker or not str(industry_by_ticker.get(ticker, "")).strip():
            industry_by_ticker[ticker] = local_map.get(ticker, "Unknown")
    return _build_one_hot_factor_exposure(industry_by_ticker, ordered)


def _write_returns_csv(path: Path, returns_long: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    returns_long.to_csv(path, index=False, float_format="%.6f")


def _write_factor_csv(path: Path, factor_exposure: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    factor_exposure.to_csv(path, index=False, float_format="%.6f")


def _source_label(market: str, source_usage: dict[str, int]) -> str:
    if market == "us":
        return "yfinance"
    if source_usage.get("akshare_fallback", 0) > 0:
        return "tushare+akshare_fallback"
    return "tushare"


def generate_risk_inputs(
    config: RiskInputsConfig,
    *,
    logger: Callable[[str], None] = _log,
) -> int:
    sample_tickers: list[str] = []
    if config.tickers_from_samples:
        sample_tickers = collect_tickers_from_samples(market=config.market)
    universe_tickers: list[str] = []
    if config.universe_file is not None:
        universe_tickers = collect_tickers_from_universe_file(config.universe_file)
    merged = sorted(set(sample_tickers).union(config.manual_tickers).union(universe_tickers))
    if not merged:
        raise ValueError(
            "No tickers resolved. Use --tickers-from-samples, --tickers, and/or --universe-file."
        )

    logger(f"Resolved {len(merged)} tickers for market={config.market}: {', '.join(merged)}")
    close_prices, failures, source_usage = build_close_price_frame(
        market=config.market,
        tickers=merged,
        end_date=config.end_date,
        lookback_days=config.lookback_days,
        cool_down=config.cool_down,
        logger=logger,
    )
    if close_prices.empty:
        raise RuntimeError("No close-price history fetched for any ticker.")

    returns_wide, excluded = compute_returns_from_prices(close_prices, max_nan_ratio=MAX_NAN_RATIO)
    if excluded:
        logger(f"Excluded tickers with high missing ratio (>20%): {', '.join(excluded)}")
    remaining_tickers = sorted(str(column) for column in returns_wide.columns)
    if len(remaining_tickers) < 5:
        raise RuntimeError(
            f"Only {len(remaining_tickers)} tickers left after filtering; at least 5 required."
        )

    returns_long = returns_wide_to_long(returns_wide)
    factor_exposure = build_factor_exposure_frame(
        market=config.market,
        tickers=remaining_tickers,
        as_of_date=config.end_date.isoformat(),
        logger=logger,
    )

    output_dir = config.output_dir.resolve()
    returns_path = output_dir / "returns_long.csv"
    factor_path = output_dir / "factor_exposure.csv"
    manifest_path = output_dir / "risk_inputs_manifest.json"

    _write_returns_csv(returns_path, returns_long)
    _write_factor_csv(factor_path, factor_exposure)

    date_range = ["", ""]
    if not returns_wide.empty:
        date_range = [
            pd.to_datetime(returns_wide.index.min()).strftime("%Y-%m-%d"),
            pd.to_datetime(returns_wide.index.max()).strftime("%Y-%m-%d"),
        ]
    manifest = {
        "generated_at": _now_iso(),
        "market": config.market,
        "ticker_count": len(remaining_tickers),
        "tickers": remaining_tickers,
        "lookback_days": int(config.lookback_days),
        "actual_trading_days": int(len(returns_wide.index)),
        "date_range": date_range,
        "tickers_excluded_high_nan": excluded,
        "failed_tickers": failures,
        "data_source": _source_label(config.market, source_usage),
        "returns_long_rows": int(len(returns_long)),
        "factor_exposure_rows": int(len(factor_exposure)),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger(f"returns_long: {returns_path}")
    logger(f"factor_exposure: {factor_path}")
    logger(f"manifest: {manifest_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate risk-model input files from historical prices.")
    parser.add_argument("--tickers-from-samples", action="store_true", help="Extract tickers from sample holdings/target CSV files.")
    parser.add_argument("--tickers", default="", help="Manual ticker list, comma separated.")
    parser.add_argument(
        "--universe-file",
        type=Path,
        default=None,
        help="Optional text file with one ticker per line; blank lines and # comments are ignored.",
    )
    parser.add_argument("--market", choices=["cn", "us"], default="cn", help="Market code.")
    parser.add_argument("--lookback-days", type=int, default=252, help="Number of trading days to keep.")
    parser.add_argument("--end-date", required=True, help="History end date in YYYY-MM-DD.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory.")
    parser.add_argument("--cool-down", type=float, default=1.0, help="Sleep seconds between ticker API calls.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    end_date = _parse_date(str(args.end_date))
    if int(args.lookback_days) < 2:
        raise ValueError("--lookback-days must be >= 2.")
    if float(args.cool_down) < 0:
        raise ValueError("--cool-down must be >= 0.")

    config = RiskInputsConfig(
        tickers_from_samples=bool(args.tickers_from_samples),
        manual_tickers=_parse_manual_tickers(str(args.tickers)),
        universe_file=Path(args.universe_file).resolve() if args.universe_file is not None else None,
        market=str(args.market).strip().lower(),
        lookback_days=int(args.lookback_days),
        end_date=end_date,
        output_dir=Path(args.output_dir).resolve(),
        cool_down=float(args.cool_down),
    )
    return generate_risk_inputs(config)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
