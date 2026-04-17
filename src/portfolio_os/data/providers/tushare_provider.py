"""Real Tushare-backed provider for static snapshot preparation."""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import numpy as np
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


def _read_windows_env_var(name: str) -> str | None:
    """Read a user/system environment variable directly from Windows registry."""

    if os.name != "nt":
        return None
    try:
        import winreg  # type: ignore[attr-defined]
    except ImportError:
        return None

    lookup_order = (
        (winreg.HKEY_CURRENT_USER, r"Environment"),
        (
            winreg.HKEY_LOCAL_MACHINE,
            r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment",
        ),
    )
    for root, subkey in lookup_order:
        try:
            with winreg.OpenKey(root, subkey) as handle:
                value, _value_type = winreg.QueryValueEx(handle, name)
        except OSError:
            continue
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
    return None


def resolve_tushare_token(token: str | None = None) -> tuple[str | None, str | None]:
    """Resolve Tushare token from explicit token, env, then Windows registry."""

    if token is not None and str(token).strip():
        return str(token).strip(), "cli"
    env_token = str(os.getenv("TUSHARE_TOKEN", "")).strip()
    if env_token:
        return env_token, "env"
    registry_token = _read_windows_env_var("TUSHARE_TOKEN")
    if registry_token:
        return registry_token, "windows_registry"
    return None, None


class TushareProvider:
    """Tushare-backed provider using the official Pro HTTP endpoint."""

    provider_name = "tushare"

    def __init__(
        self,
        *,
        token: str | None = None,
        token_source: str | None = None,
        api_url: str = "http://api.tushare.pro",
    ) -> None:
        resolved_token, detected_token_source = resolve_tushare_token(token)
        resolved_token_source = token_source or detected_token_source
        if not resolved_token:
            raise InputValidationError(
                "Tushare provider requires a token. Pass --provider-token or set TUSHARE_TOKEN."
            )
        self._token = resolved_token
        self._api_url = api_url
        self.provider_metadata = {
            "provider_token_source": resolved_token_source,
            "approximation_notes": {
                "market": [
                    "vwap is approximated from daily amount and volume: amount * 10 / vol.",
                    "adv_shares is approximated as the 20-session mean of daily vol * 100 shares.",
                    "upper_limit_hit and lower_limit_hit are derived from stk_limit when available.",
                    "if stk_limit is unavailable, price-limit detection falls back to pre_close and board-based limit-rate heuristics.",
                    "tradable is approximated from the presence of a valid daily row and positive daily volume.",
                ],
                "reference": [
                    "industry is sourced from stock_basic when available, otherwise from bak_basic.",
                    "issuer_total_shares is sourced from daily_basic.total_share when available, otherwise from bak_basic.total_share.",
                    "daily_basic.total_share is treated as 10k-share units and scaled into shares.",
                    "bak_basic.total_share is treated as 100m-share units and scaled into shares.",
                    "benchmark_weight is not populated by get_reference_snapshot and remains blank unless added downstream.",
                ],
                "state_transition_daily_panel": [
                    "daily panel history is assembled from tushare trade_cal, daily, and stk_limit plus end-date reference fields.",
                    "volume is converted from Tushare vol lots into shares by multiplying by 100.",
                    "amount is converted from Tushare amount-thousand-yuan into CNY by multiplying by 1000.",
                    "industry and issuer_total_shares are static end-date reference fields joined onto each history row.",
                    "tradable is approximated from positive daily volume on each trade date.",
                ],
                "target": [
                    "index weights are sourced from index_weight.weight and scaled from percent to decimal by dividing by 100.",
                    "when the requested date has no exact index-weight sample, the latest available trade_date on or before as_of_date is used.",
                ],
            },
        }
        self._reports = {
            "market": self._default_report(),
            "reference": self._default_report(),
            "target": self._default_report(),
        }

    @staticmethod
    def _default_report() -> dict[str, object]:
        """Return the default capability report."""

        return {
            "provider_capability_status": "available",
            "fallback_notes": [],
            "fallback_chain_used": [],
            "data_source_mix": ["tushare"],
            "permission_notes": [],
            "recommended_alternative_path": None,
        }

    def get_capability_report(self, feed_name: str) -> dict[str, object]:
        """Return the current provider capability report for one feed."""

        return dict(self._reports.get(feed_name, self._default_report()))

    def _reset_report(self, feed_name: str) -> None:
        """Reset one feed report to its default state."""

        self._reports[feed_name] = self._default_report()

    def _mark_degraded(
        self,
        feed_name: str,
        *,
        fallback_note: str,
        permission_note: str | None = None,
        recommended_alternative_path: str | None = None,
    ) -> None:
        """Mark one feed report as degraded."""

        report = self._reports.setdefault(feed_name, self._default_report())
        report["provider_capability_status"] = "degraded"
        if fallback_note not in report["fallback_notes"]:
            report["fallback_notes"].append(fallback_note)
        if "tushare" not in report["data_source_mix"]:
            report["data_source_mix"].append("tushare")
        chain_entry = f"tushare_fallback:{fallback_note}"
        if chain_entry not in report["fallback_chain_used"]:
            report["fallback_chain_used"].append(chain_entry)
        if permission_note is not None and permission_note not in report["permission_notes"]:
            report["permission_notes"].append(permission_note)
        if recommended_alternative_path is not None:
            report["recommended_alternative_path"] = recommended_alternative_path

    def _mark_unavailable(
        self,
        feed_name: str,
        *,
        permission_note: str,
        recommended_alternative_path: str | None = None,
    ) -> None:
        """Mark one feed report as unavailable."""

        report = self._reports.setdefault(feed_name, self._default_report())
        report["provider_capability_status"] = "unavailable"
        if "tushare" not in report["data_source_mix"]:
            report["data_source_mix"].append("tushare")
        if permission_note not in report["permission_notes"]:
            report["permission_notes"].append(permission_note)
        if recommended_alternative_path is not None:
            report["recommended_alternative_path"] = recommended_alternative_path

    def _record_fallback_source(
        self,
        feed_name: str,
        *,
        source: str,
        note: str,
        permission_note: str | None = None,
    ) -> None:
        """Record a fallback source without forcing degraded status."""

        report = self._reports.setdefault(feed_name, self._default_report())
        if source not in report["data_source_mix"]:
            report["data_source_mix"].append(source)
        if note not in report["fallback_notes"]:
            report["fallback_notes"].append(note)
        chain_entry = f"{source}:{note}"
        if chain_entry not in report["fallback_chain_used"]:
            report["fallback_chain_used"].append(chain_entry)
        if permission_note is not None and permission_note not in report["permission_notes"]:
            report["permission_notes"].append(permission_note)

    def _merge_capability_reports(
        self,
        target_feed_name: str,
        component_feed_names: list[str],
    ) -> None:
        """Merge multiple feed capability reports into one composite report."""

        target_existing = self.get_capability_report(target_feed_name)
        merged = self._default_report()
        merged["fallback_notes"] = []
        merged["fallback_chain_used"] = []
        merged["data_source_mix"] = []
        merged["permission_notes"] = []
        statuses: list[str] = []

        for report in [target_existing, *[self.get_capability_report(feed_name) for feed_name in component_feed_names]]:
            statuses.append(str(report.get("provider_capability_status", "available")))
            for field in ("fallback_notes", "fallback_chain_used", "data_source_mix", "permission_notes"):
                for value in report.get(field, []) or []:
                    if value not in merged[field]:
                        merged[field].append(value)
            recommended = report.get("recommended_alternative_path")
            if recommended and merged["recommended_alternative_path"] is None:
                merged["recommended_alternative_path"] = recommended

        if "unavailable" in statuses:
            merged["provider_capability_status"] = "unavailable"
        elif "degraded" in statuses:
            merged["provider_capability_status"] = "degraded"
        else:
            merged["provider_capability_status"] = "available"

        if "tushare" not in merged["data_source_mix"]:
            merged["data_source_mix"].append("tushare")
        self._reports[target_feed_name] = merged

    @staticmethod
    def _to_share_count(value: Any) -> float | None:
        """Convert mixed numeric/unit text into absolute share count."""

        if value is None:
            return None
        if isinstance(value, (int, float)):
            numeric = float(value)
            return numeric if numeric > 0 else None
        text = str(value).strip().replace(",", "")
        if not text or text in {"-", "--", "nan", "None"}:
            return None
        match = re.search(r"(-?\d+(?:\.\d+)?)", text)
        if match is None:
            return None
        numeric = float(match.group(1))
        if numeric <= 0:
            return None
        multiplier = 1.0
        if "万亿" in text:
            multiplier = 1e12
        elif "亿" in text:
            multiplier = 1e8
        elif "万" in text:
            multiplier = 1e4
        return numeric * multiplier

    @staticmethod
    def _to_trade_date(as_of_date: str) -> str:
        """Convert an ISO-like date string into Tushare trade-date format."""

        try:
            return datetime.strptime(as_of_date, "%Y-%m-%d").strftime("%Y%m%d")
        except ValueError:
            try:
                datetime.strptime(as_of_date, "%Y%m%d")
                return as_of_date
            except ValueError as exc:
                raise InputValidationError(
                    f"Unsupported as_of_date format {as_of_date!r}. Use YYYY-MM-DD or YYYYMMDD."
                ) from exc

    @staticmethod
    def _window_start(as_of_date: str, days: int) -> str:
        """Return a trade-date string for a backward calendar window."""

        if len(as_of_date) == 8:
            current = datetime.strptime(as_of_date, "%Y%m%d").date()
        else:
            current = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        start = current - timedelta(days=days)
        return start.strftime("%Y%m%d")

    @staticmethod
    def _ticker_to_ts_code(ticker: str) -> str:
        """Map an internal A-share ticker into Tushare ts_code."""

        normalized = normalize_ticker(ticker)
        if normalized.startswith(("5", "6", "9")):
            suffix = "SH"
        elif normalized.startswith(("4", "8")):
            suffix = "BJ"
        else:
            suffix = "SZ"
        return f"{normalized}.{suffix}"

    @staticmethod
    def _ts_code_to_ticker(ts_code: str) -> str:
        """Map a Tushare ts_code back into the internal ticker string."""

        return normalize_ticker(str(ts_code).split(".")[0])

    def _call_api(
        self,
        api_name: str,
        *,
        params: dict[str, Any] | None = None,
        fields: str,
    ) -> pd.DataFrame:
        """Call one Tushare Pro HTTP API and return a DataFrame."""

        payload = {
            "api_name": api_name,
            "token": self._token,
            "params": params or {},
            "fields": fields,
        }
        request = Request(
            self._api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=30) as response:
                raw = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise ProviderRuntimeError(
                f"Tushare API HTTP error for {api_name}: {exc.code} {exc.reason}"
            ) from exc
        except URLError as exc:
            raise ProviderRuntimeError(
                f"Tushare API network error for {api_name}: {exc.reason}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise ProviderRuntimeError(f"Tushare API returned invalid JSON for {api_name}.") from exc

        if int(raw.get("code", -1)) != 0:
            raise ProviderPermissionError(
                f"Tushare API error for {api_name}: {raw.get('msg') or 'unknown error'}"
            )
        data = raw.get("data") or {}
        return pd.DataFrame(data.get("items") or [], columns=data.get("fields") or [])

    @staticmethod
    def _load_akshare_module():
        """Import AKShare lazily so the provider still works without it installed."""

        try:
            import akshare as ak  # type: ignore
        except ImportError as exc:
            raise ProviderRuntimeError("AKShare is not installed.") from exc
        return ak

    @staticmethod
    def _call_with_retry(func, *args, retries: int = 2, sleep_seconds: float = 0.2, **kwargs):
        """Run a callable with a lightweight retry policy for transient errors."""

        last_error: Exception | None = None
        for attempt in range(retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:  # pragma: no cover - depends on remote source
                last_error = exc
                if attempt >= retries:
                    break
                time.sleep(sleep_seconds)
        if last_error is not None:
            raise ProviderRuntimeError(str(last_error)) from last_error
        raise ProviderRuntimeError("Unknown retry failure.")

    def _akshare_limit_by_ticker(self, ticker: str) -> tuple[float, float] | None:
        """Fetch up/down limit via AKShare spot quote."""

        ak = self._load_akshare_module()
        frame = self._call_with_retry(ak.stock_bid_ask_em, symbol=ticker)
        if frame is None or frame.empty:
            return None
        if {"item", "value"}.issubset(set(frame.columns)):
            values = {
                str(row["item"]).strip(): row["value"]
                for row in frame.to_dict(orient="records")
            }
            up = values.get("涨停")
            down = values.get("跌停")
            if up is None or down is None:
                return None
            up_v = float(pd.to_numeric(up, errors="coerce"))
            down_v = float(pd.to_numeric(down, errors="coerce"))
            if np.isnan(up_v) or np.isnan(down_v):
                return None
            return up_v, down_v
        # Fallback for shape changes.
        columns = {str(column).strip(): column for column in frame.columns}
        up_col = columns.get("涨停价") or columns.get("涨停")
        down_col = columns.get("跌停价") or columns.get("跌停")
        if up_col is None or down_col is None:
            return None
        row0 = frame.iloc[0]
        up_v = float(pd.to_numeric(row0[up_col], errors="coerce"))
        down_v = float(pd.to_numeric(row0[down_col], errors="coerce"))
        if np.isnan(up_v) or np.isnan(down_v):
            return None
        return up_v, down_v

    def _akshare_individual_info(self, ticker: str) -> dict[str, Any]:
        """Fetch industry and total share from AKShare company profile."""

        ak = self._load_akshare_module()
        frame = self._call_with_retry(ak.stock_individual_info_em, symbol=ticker)
        if frame is None or frame.empty:
            return {}
        info = {
            str(row.get("item", "")).strip(): row.get("value")
            for row in frame.to_dict(orient="records")
        }
        industry_value = None
        for key in ("行业", "所属行业", "申万行业", "行业分类"):
            if str(info.get(key, "")).strip():
                industry_value = str(info.get(key)).strip()
                break
        total_share_value = None
        for key in ("总股本", "总股本(股)", "总股本(万股)", "总股本(亿股)"):
            if info.get(key) is not None:
                total_share_value = self._to_share_count(info.get(key))
                if total_share_value is not None:
                    break
        return {
            "industry": industry_value,
            "total_share": total_share_value,
        }

    def _akshare_limits_for_tickers(
        self,
        *,
        tickers: list[str],
        trade_date: str,
    ) -> pd.DataFrame:
        """Build a Tushare-compatible limits frame from AKShare."""

        rows: list[dict[str, Any]] = []
        for ticker in tickers:
            limits = self._akshare_limit_by_ticker(ticker)
            if limits is None:
                continue
            up_limit, down_limit = limits
            rows.append(
                {
                    "ts_code": self._ticker_to_ts_code(ticker),
                    "trade_date": trade_date,
                    "up_limit": up_limit,
                    "down_limit": down_limit,
                }
            )
        return pd.DataFrame(rows, columns=["ts_code", "trade_date", "up_limit", "down_limit"])

    @staticmethod
    def _ticker_to_tencent_symbol(ticker: str) -> str:
        """Map an internal A-share ticker into a Tencent quote symbol."""

        normalized = normalize_ticker(ticker)
        if normalized.startswith(("5", "6", "9")):
            prefix = "sh"
        elif normalized.startswith(("4", "8")):
            prefix = "bj"
        else:
            prefix = "sz"
        return f"{prefix}{normalized}"

    @staticmethod
    def _ticker_to_xq_symbol(ticker: str) -> str:
        """Map an internal A-share ticker into a Xueqiu symbol."""

        normalized = normalize_ticker(ticker)
        if normalized.startswith(("5", "6", "9")):
            prefix = "SH"
        elif normalized.startswith(("4", "8")):
            prefix = "BJ"
        else:
            prefix = "SZ"
        return f"{prefix}{normalized}"

    def _akshare_individual_info_xq(self, ticker: str) -> dict[str, Any]:
        """Fallback individual info from Xueqiu-backed AKShare endpoints."""

        ak = self._load_akshare_module()
        industry_value: str | None = None
        total_share_value: float | None = None
        symbol = self._ticker_to_xq_symbol(ticker)

        basic_frame = self._call_with_retry(ak.stock_individual_basic_info_xq, symbol=symbol)
        if basic_frame is not None and not basic_frame.empty:
            basic_info = {
                str(row.get("item", "")).strip(): row.get("value")
                for row in basic_frame.to_dict(orient="records")
            }
            affiliate = basic_info.get("affiliate_industry")
            if isinstance(affiliate, dict):
                candidate = str(affiliate.get("ind_name", "")).strip()
                if candidate:
                    industry_value = candidate
            elif affiliate is not None:
                candidate = str(affiliate).strip()
                if candidate:
                    industry_value = candidate

        spot_frame = self._call_with_retry(ak.stock_individual_spot_xq, symbol=symbol)
        if spot_frame is not None and not spot_frame.empty:
            spot_info = {
                str(row.get("item", "")).strip(): row.get("value")
                for row in spot_frame.to_dict(orient="records")
            }
            for key in ("基金份额/总股本", "总股本", "流通股"):
                raw = spot_info.get(key)
                if raw is None:
                    continue
                parsed = self._to_share_count(raw)
                if parsed is not None:
                    total_share_value = parsed
                    break

        return {
            "industry": industry_value,
            "total_share": total_share_value,
        }

    def _safe_akshare_individual_info(self, ticker: str) -> dict[str, Any]:
        """Resolve AKShare profile with EM primary and XQ fallback."""

        base: dict[str, Any] = {}
        try:
            base = self._akshare_individual_info(ticker)
        except ProviderRuntimeError:
            base = {}
        industry_value = str(base.get("industry") or "").strip() or None
        total_share_value = base.get("total_share")
        if industry_value is not None and total_share_value is not None:
            return {
                "industry": industry_value,
                "total_share": float(total_share_value),
            }
        xq_info = self._akshare_individual_info_xq(ticker)
        if industry_value is None:
            industry_value = str(xq_info.get("industry") or "").strip() or None
        if total_share_value is None and xq_info.get("total_share") is not None:
            total_share_value = float(xq_info["total_share"])
        return {
            "industry": industry_value,
            "total_share": total_share_value,
        }

    @staticmethod
    def _fetch_tencent_quote_text(symbols: list[str]) -> str:
        """Fetch batched Tencent quote payload text."""

        if not symbols:
            return ""
        query = ",".join(symbols)
        request = Request(
            f"https://qt.gtimg.cn/q={query}",
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://gu.qq.com/",
            },
        )
        with urlopen(request, timeout=20) as response:
            payload = response.read()
        for encoding in ("gbk", "utf-8"):
            try:
                return payload.decode(encoding, errors="ignore")
            except Exception:
                continue
        return payload.decode(errors="ignore")

    def _tencent_limits_for_tickers(
        self,
        *,
        tickers: list[str],
        trade_date: str,
    ) -> pd.DataFrame:
        """Build a Tushare-compatible limits frame from Tencent quote fields."""

        rows: list[dict[str, Any]] = []
        if not tickers:
            return pd.DataFrame(rows, columns=["ts_code", "trade_date", "up_limit", "down_limit"])

        symbol_to_ticker = {
            self._ticker_to_tencent_symbol(ticker): normalize_ticker(ticker)
            for ticker in tickers
        }
        symbols = list(symbol_to_ticker.keys())
        chunk_size = 80
        for start in range(0, len(symbols), chunk_size):
            chunk = symbols[start : start + chunk_size]
            text = self._call_with_retry(self._fetch_tencent_quote_text, chunk)
            if not text:
                continue
            for line in text.split(";"):
                line = str(line).strip()
                if not line:
                    continue
                match = re.match(r'^v_([^=]+)="(.*)"$', line)
                if match is None:
                    continue
                symbol = str(match.group(1)).strip()
                ticker = symbol_to_ticker.get(symbol)
                if ticker is None:
                    continue
                values = str(match.group(2)).split("~")
                if len(values) <= 48:
                    continue
                up_limit = float(pd.to_numeric(values[47], errors="coerce"))
                down_limit = float(pd.to_numeric(values[48], errors="coerce"))
                if np.isnan(up_limit) or np.isnan(down_limit) or up_limit <= 0 or down_limit <= 0:
                    continue
                rows.append(
                    {
                        "ts_code": self._ticker_to_ts_code(ticker),
                        "trade_date": trade_date,
                        "up_limit": up_limit,
                        "down_limit": down_limit,
                    }
                )
        frame = pd.DataFrame(rows, columns=["ts_code", "trade_date", "up_limit", "down_limit"])
        if frame.empty:
            return frame
        return frame.drop_duplicates(subset=["ts_code", "trade_date"], keep="last").copy()

    def _load_daily_for_trade_date(self, trade_date: str, ts_codes: list[str]) -> pd.DataFrame:
        """Load daily market rows for the requested trade date and filter the basket."""

        daily = self._call_api(
            "daily",
            params={"trade_date": trade_date},
            fields="ts_code,trade_date,close,pre_close,vol,amount",
        )
        if daily.empty:
            raise ProviderDataError(f"Tushare daily returned no rows for trade_date {trade_date}.")
        daily = daily[daily["ts_code"].isin(ts_codes)].copy()
        if daily.empty:
            raise ProviderDataError(
                f"Tushare daily returned no matching rows for the requested tickers on {trade_date}."
            )
        return daily

    def _load_stk_limit_for_trade_date(self, trade_date: str, ts_codes: list[str]) -> pd.DataFrame:
        """Load daily price limits for the requested trade date."""

        normalized_tickers = [self._ts_code_to_ticker(ts_code) for ts_code in ts_codes]
        try:
            limit_frame = self._call_api(
                "stk_limit",
                params={"trade_date": trade_date},
                fields="ts_code,trade_date,up_limit,down_limit",
            )
        except ProviderPermissionError:
            self._record_fallback_source(
                "market",
                source="tushare",
                note="stk_limit_permission_missing",
                permission_note="stk_limit_permission_missing_or_rate_limited",
            )
            limit_frame = pd.DataFrame(columns=["ts_code", "trade_date", "up_limit", "down_limit"])
        except ProviderRuntimeError:
            self._record_fallback_source(
                "market",
                source="tushare",
                note="stk_limit_runtime_unavailable",
                permission_note="stk_limit_runtime_unavailable",
            )
            limit_frame = pd.DataFrame(columns=["ts_code", "trade_date", "up_limit", "down_limit"])
        limit_frame = limit_frame[limit_frame["ts_code"].isin(ts_codes)].copy()
        if limit_frame.empty:
            limit_frame = pd.DataFrame(columns=["ts_code", "trade_date", "up_limit", "down_limit"])

        missing_codes = [code for code in ts_codes if code not in set(limit_frame.get("ts_code", []))]
        if missing_codes:
            missing_tickers = [self._ts_code_to_ticker(ts_code) for ts_code in missing_codes]
            akshare_limits = pd.DataFrame(columns=["ts_code", "trade_date", "up_limit", "down_limit"])
            try:
                akshare_limits = self._akshare_limits_for_tickers(
                    tickers=missing_tickers,
                    trade_date=trade_date,
                )
            except ProviderRuntimeError:
                self._record_fallback_source(
                    "market",
                    source="akshare",
                    note="stk_limit_fallback_failed",
                )
            if not akshare_limits.empty:
                self._record_fallback_source(
                    "market",
                    source="akshare",
                    note="stk_limit_filled_from_akshare",
                )
                if limit_frame.empty:
                    limit_frame = akshare_limits.copy()
                else:
                    limit_frame = pd.concat([limit_frame, akshare_limits], ignore_index=True)

        missing_codes = [code for code in ts_codes if code not in set(limit_frame.get("ts_code", []))]
        if missing_codes:
            missing_tickers = [self._ts_code_to_ticker(ts_code) for ts_code in missing_codes]
            tencent_limits = pd.DataFrame(columns=["ts_code", "trade_date", "up_limit", "down_limit"])
            try:
                tencent_limits = self._tencent_limits_for_tickers(
                    tickers=missing_tickers,
                    trade_date=trade_date,
                )
            except ProviderRuntimeError:
                self._record_fallback_source(
                    "market",
                    source="tencent",
                    note="stk_limit_fallback_failed",
                )
            if not tencent_limits.empty:
                self._record_fallback_source(
                    "market",
                    source="tencent",
                    note="stk_limit_filled_from_tencent",
                )
                if limit_frame.empty:
                    limit_frame = tencent_limits.copy()
                else:
                    limit_frame = pd.concat([limit_frame, tencent_limits], ignore_index=True)

        limit_frame = limit_frame.drop_duplicates(subset=["ts_code", "trade_date"], keep="last").copy()
        missing_codes = [code for code in ts_codes if code not in set(limit_frame.get("ts_code", []))]
        if missing_codes:
            self._mark_degraded(
                "market",
                fallback_note="stk_limit_unavailable_used_price_band_approximation",
                permission_note="stk_limit_data_unavailable",
                recommended_alternative_path="run_market_and_reference_builders_only",
            )
        return limit_frame[limit_frame["ts_code"].isin(ts_codes)].copy()

    def _load_state_transition_limits_for_trade_date(
        self,
        trade_date: str,
        ts_codes: list[str],
    ) -> pd.DataFrame:
        """Load daily price limits for history panels without external per-day fallbacks."""

        try:
            limit_frame = self._call_api(
                "stk_limit",
                params={"trade_date": trade_date},
                fields="ts_code,trade_date,up_limit,down_limit",
            )
        except (ProviderPermissionError, ProviderRuntimeError):
            self._mark_degraded(
                "state_transition_daily_panel",
                fallback_note="stk_limit_unavailable_used_price_band_approximation",
                permission_note="stk_limit_permission_missing_or_rate_limited",
                recommended_alternative_path="provide_state_transition_daily_panel_csv_and_continue",
            )
            return pd.DataFrame(columns=["ts_code", "trade_date", "up_limit", "down_limit"])

        limit_frame = limit_frame[limit_frame["ts_code"].isin(ts_codes)].copy()
        if limit_frame.empty:
            self._mark_degraded(
                "state_transition_daily_panel",
                fallback_note="stk_limit_unavailable_used_price_band_approximation",
                permission_note="stk_limit_data_unavailable",
                recommended_alternative_path="provide_state_transition_daily_panel_csv_and_continue",
            )
            return pd.DataFrame(columns=["ts_code", "trade_date", "up_limit", "down_limit"])

        missing_codes = [code for code in ts_codes if code not in set(limit_frame.get("ts_code", []))]
        if missing_codes:
            self._mark_degraded(
                "state_transition_daily_panel",
                fallback_note="stk_limit_partial_used_price_band_approximation",
                permission_note="stk_limit_partial_data_unavailable",
                recommended_alternative_path="provide_state_transition_daily_panel_csv_and_continue",
            )
        return limit_frame.drop_duplicates(subset=["ts_code", "trade_date"], keep="last").copy()

    @staticmethod
    def _approx_limit_rate_from_ticker(ticker: str) -> float:
        """Approximate the daily price-limit rate from the board code."""

        normalized = normalize_ticker(ticker)
        if normalized.startswith(("300", "301", "688", "689")):
            return 0.20
        if normalized.startswith(("4", "8")):
            return 0.30
        return 0.10

    def _estimate_adv_shares(self, ts_code: str, trade_date: str) -> float:
        """Estimate ADV shares from a recent daily history window."""

        history = self._call_api(
            "daily",
            params={
                "ts_code": ts_code,
                "start_date": self._window_start(trade_date, 45),
                "end_date": trade_date,
            },
            fields="ts_code,trade_date,vol",
        )
        if history.empty:
            raise ProviderDataError(
                f"Tushare daily history returned no rows for {ts_code} when estimating adv_shares."
            )
        history["vol"] = pd.to_numeric(history["vol"], errors="raise")
        recent = history.sort_values("trade_date", ascending=False).head(20)
        adv_shares = float((recent["vol"] * 100.0).mean())
        if adv_shares <= 0:
            raise ProviderDataError(
                f"Tushare daily history produced non-positive adv_shares for {ts_code}."
            )
        return adv_shares

    def get_state_transition_daily_panel(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Return a contract-shaped daily history panel for state-transition research."""

        self._reset_report("state_transition_daily_panel")
        self._reset_report("market")
        start_trade_date = self._to_trade_date(start_date)
        end_trade_date = self._to_trade_date(end_date)
        normalized_tickers = [normalize_ticker(ticker) for ticker in tickers]
        ts_codes = [self._ticker_to_ts_code(ticker) for ticker in normalized_tickers]

        daily_history_fallback: pd.DataFrame | None = None
        try:
            trade_calendar = self._call_api(
                "trade_cal",
                params={
                    "exchange": "",
                    "start_date": start_trade_date,
                    "end_date": end_trade_date,
                },
                fields="cal_date,is_open",
            )
            if trade_calendar.empty:
                raise ProviderDataError(
                    f"Tushare trade_cal returned no rows between {start_trade_date} and {end_trade_date}."
                )
            trade_calendar["cal_date"] = trade_calendar["cal_date"].astype(str)
            trade_calendar["is_open"] = pd.to_numeric(trade_calendar["is_open"], errors="coerce")
            open_trade_dates = (
                trade_calendar.loc[trade_calendar["is_open"] == 1, "cal_date"]
                .astype(str)
                .sort_values()
                .tolist()
            )
            if not open_trade_dates:
                raise ProviderDataError(
                    f"Tushare trade_cal has no open sessions between {start_trade_date} and {end_trade_date}."
                )
        except (ProviderPermissionError, ProviderRuntimeError):
            self._record_fallback_source(
                "state_transition_daily_panel",
                source="tushare",
                note="trade_cal_permission_missing",
                permission_note="trade_cal_permission_missing_or_rate_limited",
            )
            fallback_frames: list[pd.DataFrame] = []
            for ts_code in ts_codes:
                daily_history = self._call_api(
                    "daily",
                    params={
                        "ts_code": ts_code,
                        "start_date": start_trade_date,
                        "end_date": end_trade_date,
                    },
                    fields="ts_code,trade_date,open,high,low,close,pre_close,vol,amount",
                )
                if not daily_history.empty:
                    fallback_frames.append(daily_history.copy())
            if not fallback_frames:
                raise ProviderDataError(
                    f"Tushare daily history fallback returned no rows between {start_trade_date} and {end_trade_date}."
                )
            daily_history_fallback = pd.concat(fallback_frames, ignore_index=True)
            daily_history_fallback["trade_date"] = daily_history_fallback["trade_date"].astype(str)
            open_trade_dates = sorted(set(daily_history_fallback["trade_date"]))

        reference_rows = self.get_reference_snapshot(normalized_tickers, end_date)
        reference_records: list[dict[str, Any]] = []
        for row in reference_rows:
            if hasattr(row, "model_dump"):
                payload = row.model_dump(mode="json")
            else:
                payload = {
                    "ticker": getattr(row, "ticker", None),
                    "industry": getattr(row, "industry", None),
                    "benchmark_weight": getattr(row, "benchmark_weight", None),
                    "issuer_total_shares": getattr(row, "issuer_total_shares", None),
                }
            reference_records.append(payload)
        reference_frame = pd.DataFrame(reference_records)
        if reference_frame.empty:
            raise ProviderDataError("Tushare reference snapshot returned no rows for state-transition history.")
        reference_frame["ticker"] = reference_frame["ticker"].map(normalize_ticker)
        reference_frame = reference_frame.loc[:, ["ticker", "industry", "issuer_total_shares"]].copy()

        history_frames: list[pd.DataFrame] = []
        for trade_date in open_trade_dates:
            if daily_history_fallback is None:
                daily = self._call_api(
                    "daily",
                    params={"trade_date": trade_date},
                    fields="ts_code,trade_date,open,high,low,close,pre_close,vol,amount",
                )
            else:
                daily = daily_history_fallback[daily_history_fallback["trade_date"] == trade_date].copy()
            if daily.empty:
                continue
            daily = daily[daily["ts_code"].isin(ts_codes)].copy()
            if daily.empty:
                continue

            active_ts_codes = sorted(set(daily["ts_code"].astype(str)))
            limits = self._load_state_transition_limits_for_trade_date(trade_date, active_ts_codes)
            merged = daily.merge(limits, on=["ts_code", "trade_date"], how="left")
            merged["ticker"] = merged["ts_code"].map(self._ts_code_to_ticker)
            for column in (
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "vol",
                "amount",
                "up_limit",
                "down_limit",
            ):
                merged[column] = pd.to_numeric(merged.get(column), errors="coerce")
            if bool(merged["up_limit"].isna().any() or merged["down_limit"].isna().any()):
                self._mark_degraded(
                    "state_transition_daily_panel",
                    fallback_note="stk_limit_partial_used_price_band_approximation",
                    permission_note="stk_limit_partial_data_unavailable",
                    recommended_alternative_path="provide_state_transition_daily_panel_csv_and_continue",
                )
            merged["approx_limit_rate"] = merged["ticker"].map(self._approx_limit_rate_from_ticker)
            merged["up_limit"] = merged.apply(
                lambda row: (
                    float(row["up_limit"])
                    if pd.notna(row["up_limit"])
                    else float(row["pre_close"]) * (1.0 + float(row["approx_limit_rate"]))
                ),
                axis=1,
            )
            merged["down_limit"] = merged.apply(
                lambda row: (
                    float(row["down_limit"])
                    if pd.notna(row["down_limit"])
                    else float(row["pre_close"]) * (1.0 - float(row["approx_limit_rate"]))
                ),
                axis=1,
            )
            merged["date"] = pd.to_datetime(merged["trade_date"], format="%Y%m%d", errors="raise").dt.strftime(
                "%Y-%m-%d"
            )
            merged["volume"] = merged["vol"] * 100.0
            merged["amount_cny"] = merged["amount"] * 1000.0
            merged["tradable"] = merged["vol"].fillna(0.0) > 0.0
            history_frames.append(
                merged.loc[
                    :,
                    [
                        "date",
                        "ticker",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "amount_cny",
                        "up_limit",
                        "down_limit",
                        "tradable",
                    ],
                ].rename(
                    columns={
                        "amount_cny": "amount",
                        "up_limit": "upper_limit_price",
                        "down_limit": "lower_limit_price",
                    }
                )
            )

        if not history_frames:
            raise ProviderDataError(
                f"Tushare state-transition history returned no matching rows between {start_trade_date} and {end_trade_date}."
            )

        history = pd.concat(history_frames, ignore_index=True)
        history = history.merge(reference_frame, on="ticker", how="left")
        if history["industry"].astype(str).str.strip().eq("").any() or history["industry"].isna().any():
            raise ProviderDataError("Tushare state-transition history contains missing industry values.")
        history["issuer_total_shares"] = pd.to_numeric(history["issuer_total_shares"], errors="coerce")
        if history["issuer_total_shares"].isna().any():
            raise ProviderDataError(
                "Tushare state-transition history contains missing issuer_total_shares values."
            )

        self._merge_capability_reports("state_transition_daily_panel", ["reference"])
        return history.sort_values(["date", "ticker"]).reset_index(drop=True)

    def get_daily_market_snapshot(
        self,
        tickers: list[str],
        as_of_date: str,
    ) -> list[DailyMarketSnapshotRow]:
        """Return the fields required to build `market.csv`."""

        self._reset_report("market")
        trade_date = self._to_trade_date(as_of_date)
        normalized_tickers = [normalize_ticker(ticker) for ticker in tickers]
        ts_codes = [self._ticker_to_ts_code(ticker) for ticker in normalized_tickers]
        daily = self._load_daily_for_trade_date(trade_date, ts_codes)
        limits = self._load_stk_limit_for_trade_date(trade_date, ts_codes)
        merged = daily.merge(limits, on=["ts_code", "trade_date"], how="left")
        merged["ticker"] = merged["ts_code"].map(self._ts_code_to_ticker)
        merged["close"] = pd.to_numeric(merged["close"], errors="raise")
        merged["pre_close"] = pd.to_numeric(merged["pre_close"], errors="raise")
        merged["vol"] = pd.to_numeric(merged["vol"], errors="raise")
        merged["amount"] = pd.to_numeric(merged["amount"], errors="raise")
        merged["vwap"] = merged.apply(
            lambda row: float(row["amount"] * 10.0 / row["vol"]) if float(row["vol"]) > 0 else float(row["close"]),
            axis=1,
        )
        merged["tradable"] = merged["vol"] > 0
        merged["up_limit"] = pd.to_numeric(merged.get("up_limit"), errors="coerce")
        merged["down_limit"] = pd.to_numeric(merged.get("down_limit"), errors="coerce")
        if bool(merged["up_limit"].isna().any() or merged["down_limit"].isna().any()):
            self._mark_degraded(
                "market",
                fallback_note="stk_limit_partial_used_price_band_approximation",
                permission_note="stk_limit_partial_data_unavailable",
                recommended_alternative_path="run_market_and_reference_builders_only",
            )
        merged["approx_limit_rate"] = merged["ticker"].map(self._approx_limit_rate_from_ticker)
        merged["up_limit"] = merged.apply(
            lambda row: (
                float(row["up_limit"])
                if pd.notna(row["up_limit"])
                else float(row["pre_close"]) * (1.0 + float(row["approx_limit_rate"]))
            ),
            axis=1,
        )
        merged["down_limit"] = merged.apply(
            lambda row: (
                float(row["down_limit"])
                if pd.notna(row["down_limit"])
                else float(row["pre_close"]) * (1.0 - float(row["approx_limit_rate"]))
            ),
            axis=1,
        )
        merged["upper_limit_hit"] = merged.apply(
            lambda row: bool(float(row["close"]) >= float(row["up_limit"]) * 0.9999),
            axis=1,
        )
        merged["lower_limit_hit"] = merged.apply(
            lambda row: bool(float(row["close"]) <= float(row["down_limit"]) * 1.0001),
            axis=1,
        )

        rows_by_ticker = {row["ticker"]: row for row in merged.to_dict(orient="records")}
        missing = [ticker for ticker in normalized_tickers if ticker not in rows_by_ticker]
        if missing:
            raise ProviderDataError(
                f"Tushare market snapshot is missing ticker(s) on {trade_date}: {', '.join(missing)}"
            )

        results: list[DailyMarketSnapshotRow] = []
        for ticker in normalized_tickers:
            row = rows_by_ticker[ticker]
            adv_shares = self._estimate_adv_shares(row["ts_code"], trade_date)
            results.append(
                DailyMarketSnapshotRow(
                    ticker=ticker,
                    close=float(row["close"]),
                    vwap=float(row["vwap"]),
                    adv_shares=adv_shares,
                    tradable=bool(row["tradable"]),
                    upper_limit_hit=bool(row["upper_limit_hit"]),
                    lower_limit_hit=bool(row["lower_limit_hit"]),
                )
            )
        return results

    def get_reference_snapshot(
        self,
        tickers: list[str],
        as_of_date: str,
    ) -> list[ReferenceSnapshotRow]:
        """Return the fields required to build `reference.csv`."""

        self._reset_report("reference")
        trade_date = self._to_trade_date(as_of_date)
        normalized_tickers = [normalize_ticker(ticker) for ticker in tickers]
        ts_codes = [self._ticker_to_ts_code(ticker) for ticker in normalized_tickers]
        industry_by_ticker: dict[str, str] = {}
        total_share_by_ticker: dict[str, float | None] = {}

        try:
            stock_basic = self._call_api(
                "stock_basic",
                params={"exchange": "", "list_status": "L"},
                fields="ts_code,industry",
            )
            if stock_basic.empty:
                raise ProviderDataError("Tushare stock_basic returned no rows.")
            stock_basic = stock_basic[stock_basic["ts_code"].isin(ts_codes)].copy()
            stock_basic["ticker"] = stock_basic["ts_code"].map(self._ts_code_to_ticker)
            for row in stock_basic.to_dict(orient="records"):
                ticker = str(row.get("ticker", "")).strip()
                industry = str(row.get("industry", "")).strip()
                if ticker and industry:
                    industry_by_ticker[ticker] = industry
        except (ProviderPermissionError, ProviderRuntimeError):
            self._record_fallback_source(
                "reference",
                source="tushare",
                note="stock_basic_permission_missing",
                permission_note="stock_basic_permission_missing_or_rate_limited",
            )

        try:
            daily_basic = self._call_api(
                "daily_basic",
                params={"trade_date": trade_date},
                fields="ts_code,total_share",
            )
            daily_basic = daily_basic[daily_basic["ts_code"].isin(ts_codes)].copy()
            if "total_share" not in daily_basic.columns:
                daily_basic["total_share"] = pd.Series(dtype=float)
            daily_basic["total_share"] = pd.to_numeric(daily_basic["total_share"], errors="coerce")
            daily_basic["ticker"] = daily_basic["ts_code"].map(self._ts_code_to_ticker)
            for row in daily_basic.to_dict(orient="records"):
                ticker = str(row.get("ticker", "")).strip()
                if not ticker:
                    continue
                raw_total = row.get("total_share")
                if pd.notna(raw_total):
                    total_share_by_ticker[ticker] = float(raw_total) * 10000.0
        except (ProviderPermissionError, ProviderRuntimeError):
            self._record_fallback_source(
                "reference",
                source="tushare",
                note="daily_basic_permission_missing",
                permission_note="daily_basic_permission_missing_or_rate_limited",
            )

        missing_industry = [ticker for ticker in normalized_tickers if not str(industry_by_ticker.get(ticker, "")).strip()]
        missing_total_share = [ticker for ticker in normalized_tickers if total_share_by_ticker.get(ticker) is None]
        if missing_industry or missing_total_share:
            for ticker in sorted(set([*missing_industry, *missing_total_share])):
                try:
                    ak_info = self._safe_akshare_individual_info(ticker)
                except ProviderRuntimeError:
                    continue
                industry_value = str(ak_info.get("industry") or "").strip()
                total_share_value = ak_info.get("total_share")
                if ticker in missing_industry and industry_value:
                    industry_by_ticker[ticker] = industry_value
                if ticker in missing_total_share and total_share_value is not None:
                    total_share_by_ticker[ticker] = float(total_share_value)
            if any(industry_by_ticker.get(ticker) for ticker in normalized_tickers):
                self._record_fallback_source(
                    "reference",
                    source="akshare",
                    note="stock_info_filled_from_akshare",
                )

        missing_industry = [ticker for ticker in normalized_tickers if not str(industry_by_ticker.get(ticker, "")).strip()]
        missing_total_share = [ticker for ticker in normalized_tickers if total_share_by_ticker.get(ticker) is None]

        if missing_industry or missing_total_share:
            try:
                bak_basic = self._call_api(
                    "bak_basic",
                    params={"trade_date": trade_date},
                    fields="ts_code,industry,total_share",
                )
            except (ProviderPermissionError, ProviderRuntimeError) as exc:
                self._mark_unavailable(
                    "reference",
                    permission_note="bak_basic_permission_missing_or_rate_limited",
                    recommended_alternative_path="provide_reference_csv_and_continue",
                )
                raise ProviderPermissionError(
                    "Tushare reference snapshot requires stock_basic or bak_basic access. "
                    "You can continue with client-provided reference.csv."
                ) from exc
            if not bak_basic.empty:
                bak_basic = bak_basic[bak_basic["ts_code"].isin(ts_codes)].copy()
                bak_basic["ticker"] = bak_basic["ts_code"].map(self._ts_code_to_ticker)
                bak_basic["total_share"] = pd.to_numeric(bak_basic["total_share"], errors="coerce")
                for row in bak_basic.to_dict(orient="records"):
                    ticker = str(row.get("ticker", "")).strip()
                    if not ticker:
                        continue
                    if not str(industry_by_ticker.get(ticker, "")).strip():
                        industry = str(row.get("industry", "")).strip()
                        if industry:
                            industry_by_ticker[ticker] = industry
                    if total_share_by_ticker.get(ticker) is None and pd.notna(row.get("total_share")):
                        total_share_by_ticker[ticker] = float(row["total_share"]) * 100000000.0
                self._record_fallback_source(
                    "reference",
                    source="tushare",
                    note="bak_basic_fallback_used",
                )

        missing_industry = [ticker for ticker in normalized_tickers if not str(industry_by_ticker.get(ticker, "")).strip()]
        if missing_industry:
            raise ProviderDataError("Tushare reference snapshot contains missing industry values.")

        missing_total_share = [ticker for ticker in normalized_tickers if total_share_by_ticker.get(ticker) is None]
        if missing_total_share:
            self._mark_degraded(
                "reference",
                fallback_note="issuer_total_share_missing_after_fallback",
                permission_note="daily_basic_or_alternative_total_share_missing",
                recommended_alternative_path="provide_reference_csv_and_continue",
            )

        rows: list[ReferenceSnapshotRow] = []
        for ticker in normalized_tickers:
            rows.append(
                ReferenceSnapshotRow(
                    ticker=ticker,
                    industry=str(industry_by_ticker[ticker]).strip(),
                    benchmark_weight=None,
                    issuer_total_shares=(
                        float(total_share_by_ticker[ticker])
                        if total_share_by_ticker.get(ticker) is not None
                        else None
                    ),
                )
            )
        return rows

    def get_index_weights(
        self,
        index_code: str,
        as_of_date: str,
    ) -> list[IndexWeightRow]:
        """Return the fields required to build `target.csv`."""

        self._reset_report("target")
        trade_date = self._to_trade_date(as_of_date)
        try:
            weights = self._call_api(
                "index_weight",
                params={
                    "index_code": index_code,
                    "start_date": self._window_start(trade_date, 31),
                    "end_date": trade_date,
                },
                fields="index_code,con_code,trade_date,weight",
            )
        except (ProviderPermissionError, ProviderRuntimeError) as exc:
            self._mark_unavailable(
                "target",
                permission_note="index_weight_permission_missing",
                recommended_alternative_path="provide_target_csv_and_continue",
            )
            raise ProviderPermissionError(
                "Tushare index_weight access is unavailable for this account. "
                "You can continue with client-provided target.csv."
            ) from exc
        if weights.empty:
            raise ProviderDataError(
                f"Tushare index_weight returned no rows for {index_code!r} on or before {trade_date}."
            )
        weights["trade_date"] = weights["trade_date"].astype(str)
        latest_trade_date = str(weights["trade_date"].max())
        latest = weights[weights["trade_date"] == latest_trade_date].copy()
        latest["weight"] = pd.to_numeric(latest["weight"], errors="raise")
        latest = latest.sort_values(["con_code", "trade_date"]).drop_duplicates(subset=["con_code"], keep="last")
        latest["ticker"] = latest["con_code"].map(self._ts_code_to_ticker)
        if latest.empty:
            raise ProviderDataError(
                f"Tushare index_weight returned no usable rows for {index_code!r}."
            )
        return [
            IndexWeightRow(
                ticker=str(row["ticker"]),
                target_weight=float(row["weight"]) / 100.0,
            )
            for row in latest.to_dict(orient="records")
        ]
