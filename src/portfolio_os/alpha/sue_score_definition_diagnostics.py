"""SUE score-definition diagnostics.

This module diagnoses whether raw EPS surprise is an appropriate SUE score for
the already-built WRDS/PIT-labeled event panel. It does not run Q2, optimizers,
paper workflows, brokers, orders, or production approval paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.alpha.sue_historical_event_evidence import (
    EVENT_WINDOWS,
    SueHistoricalEventEvidenceConfig,
    _build_window_return_frame,
    _event_window_grid,
    _load_and_validate_events,
    _load_prices,
    _price_index,
    _rank_ic_by_date,
    _safe_events,
    _t_stat,
    _top_bottom_spread_by_date,
)
from portfolio_os.alpha.sue_historical_schema import validate_no_forward_return_feature_columns
from portfolio_os.provenance.hashing import hash_payload


SUE_SCORE_DEFINITION_DIAGNOSTICS_SCHEMA_VERSION = "sue_score_definition_diagnostics.v1"

DEFAULT_EVENTS_PATH = "outputs/sue_historical_event_panel_expanded/events.csv"
DEFAULT_CRSP_DAILY_PATH = "data/cache/wrds_sue_event_panel/crsp_daily.csv"
DEFAULT_OUTPUT_DIR = "outputs/sue_score_definition_diagnostics"
DEFAULT_REPORT_PATH = "reports/sue_score_definition_diagnostics_report.md"

SCORE_DEFINITIONS = [
    "raw_eps_diff",
    "raw_eps_diff_winsorized_global",
    "surprise_pct_expected_eps",
    "surprise_pct_expected_eps_winsorized_global",
    "surprise_pct_actual_eps",
    "surprise_pct_actual_eps_winsorized_global",
    "price_scaled_raw_eps_diff",
]

SCALE_AWARE_SCORE_PRIORITY = [
    "surprise_pct_actual_eps",
    "surprise_pct_expected_eps",
    "surprise_pct_actual_eps_winsorized_global",
    "surprise_pct_expected_eps_winsorized_global",
    "price_scaled_raw_eps_diff",
]

MISLEADING_SCORE_DEFINITION_CLAIMS = (
    "production approved",
    "paper ready",
    "paper-ready",
    "live-ready",
    "live ready",
    "live trading",
    "live alpha orders",
    "broker execution",
    "order generation",
    "sue alpha is proven",
    "real historical sue alpha proven",
    "historical sue alpha proven",
    "guaranteed tradable alpha",
    "auto trading",
    "investment recommendation",
)


class SueScoreDefinitionDiagnosticsConfig(BaseModel):
    """Config for H1D score-definition diagnostics."""

    model_config = ConfigDict(extra="forbid")

    events_path: str = DEFAULT_EVENTS_PATH
    crsp_daily_path: str = DEFAULT_CRSP_DAILY_PATH
    output_dir: str = DEFAULT_OUTPUT_DIR
    report_path: str = DEFAULT_REPORT_PATH
    quantiles: int = Field(default=5, gt=1)
    min_rank_ic_names: int = Field(default=3, gt=1)
    min_spread_names: int = Field(default=5, gt=1)
    winsor_lower_quantile: float = Field(default=0.01, ge=0.0, lt=0.5)
    winsor_upper_quantile: float = Field(default=0.99, gt=0.5, le=1.0)
    epsilon: float = Field(default=1.0e-9, gt=0.0)


@dataclass(frozen=True)
class SueScoreDefinitionDiagnosticsResult:
    """In-memory H1D diagnostic result."""

    config: SueScoreDefinitionDiagnosticsConfig
    score_definition_grid: pd.DataFrame
    score_definition_by_date: pd.DataFrame
    diagnostic_summary: dict[str, Any]
    report_text: str

    @property
    def summary(self) -> dict[str, Any]:
        """Backward-compatible alias used by focused tests."""

        return self.diagnostic_summary


def load_sue_score_definition_diagnostics_config(path: str | Path) -> SueScoreDefinitionDiagnosticsConfig:
    """Load H1D diagnostics config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    return SueScoreDefinitionDiagnosticsConfig(
        events_path=str(inputs.get("events_path") or payload.get("events_path") or DEFAULT_EVENTS_PATH),
        crsp_daily_path=str(inputs.get("crsp_daily_path") or payload.get("crsp_daily_path") or DEFAULT_CRSP_DAILY_PATH),
        output_dir=str(outputs.get("output_dir") or payload.get("output_dir") or DEFAULT_OUTPUT_DIR),
        report_path=str(outputs.get("report_path") or payload.get("report_path") or DEFAULT_REPORT_PATH),
        quantiles=int(payload.get("quantiles", 5)),
        min_rank_ic_names=int(payload.get("min_rank_ic_names", 3)),
        min_spread_names=int(payload.get("min_spread_names", 5)),
        winsor_lower_quantile=float(payload.get("winsor_lower_quantile", 0.01)),
        winsor_upper_quantile=float(payload.get("winsor_upper_quantile", 0.99)),
        epsilon=float(payload.get("epsilon", 1.0e-9)),
    )


def build_sue_score_definition_diagnostics(
    config: SueScoreDefinitionDiagnosticsConfig | None = None,
) -> SueScoreDefinitionDiagnosticsResult:
    """Build H1D score-definition diagnostics from PIT-safe SUE rows."""

    resolved = config or SueScoreDefinitionDiagnosticsConfig()
    events = _load_and_validate_events(resolved.events_path)
    validate_no_forward_return_feature_columns(list(events.columns))
    prices = _load_prices(resolved.crsp_daily_path)
    price_index = _price_index(prices)
    safe_events = _safe_events(events)
    helper_config = _helper_evidence_config(resolved)
    return_frame = _build_window_return_frame(
        safe_events=safe_events,
        price_index=price_index,
        anchor_column="tradable_timestamp",
        config=helper_config,
    )
    scored_frame = _attach_score_definitions(
        return_frame=return_frame,
        safe_events=safe_events,
        crsp_daily_path=resolved.crsp_daily_path,
        config=resolved,
    )
    score_definition_grid, score_definition_by_date = _score_definition_tables(
        scored_frame=scored_frame,
        helper_config=helper_config,
    )
    summary = _diagnostic_summary(
        config=resolved,
        safe_events=safe_events,
        scored_frame=scored_frame,
        score_definition_grid=score_definition_grid,
    )
    report_text = render_sue_score_definition_diagnostics_report(
        score_definition_grid=score_definition_grid,
        diagnostic_summary=summary,
    )
    validate_sue_score_definition_report_language(report_text)
    return SueScoreDefinitionDiagnosticsResult(
        config=resolved,
        score_definition_grid=score_definition_grid,
        score_definition_by_date=score_definition_by_date,
        diagnostic_summary=summary,
        report_text=report_text,
    )


def write_sue_score_definition_diagnostics_artifacts(
    result: SueScoreDefinitionDiagnosticsResult,
) -> dict[str, Path]:
    """Write H1D score-definition diagnostic artifacts."""

    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(result.config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    paths = {
        "score_definition_grid": output_dir / "score_definition_grid.csv",
        "score_definition_by_date": output_dir / "score_definition_by_date.csv",
        "diagnostic_summary": output_dir / "diagnostic_summary.json",
        "report": report_path,
    }
    result.score_definition_grid.to_csv(paths["score_definition_grid"], index=False)
    result.score_definition_by_date.to_csv(paths["score_definition_by_date"], index=False)
    _write_json(paths["diagnostic_summary"], result.diagnostic_summary)
    validate_sue_score_definition_report_language(result.report_text)
    paths["report"].write_text(result.report_text, encoding="utf-8")
    return paths


def render_sue_score_definition_diagnostics_report(
    *,
    score_definition_grid: pd.DataFrame,
    diagnostic_summary: dict[str, Any],
) -> str:
    """Render H1D diagnostics report."""

    best_window = diagnostic_summary.get("diagnostic_window", "plus_2_plus_22")
    preferred = diagnostic_summary.get("preferred_diagnostic_score")
    lines = [
        "# SUE Score Definition Diagnostics",
        "",
        "This is H1D score-definition diagnostics for the WRDS/PIT SUE historical panel.",
        "It does not prove SUE alpha success.",
        "It does not run Q2 or optimizer-path evaluation.",
        "It does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "Alpha Registry status is not promoted by this report.",
        "",
        "## Summary",
        "",
        f"- diagnostic_window: `{best_window}`",
        f"- preferred_diagnostic_score: `{preferred}`",
        f"- raw_eps_diff_scale_warning: `{diagnostic_summary['raw_eps_diff_scale_warning']}`",
        f"- missing_coverage_encoded_as_zero_alpha: `{diagnostic_summary['missing_coverage_encoded_as_zero_alpha']}`",
        f"- no_view_not_zero_alpha: `{diagnostic_summary['no_view_not_zero_alpha']}`",
        "",
        "raw EPS difference is not the preferred SUE score when scale-aware definitions produce a cleaner top-bottom diagnostic.",
        "",
        "## Score Definition Grid",
        "",
        "| Score | Window | Rows | Mean Rank IC | Rank IC t | Median Rank IC | Mean Top-Bottom Spread | Spread t | Status |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in score_definition_grid.to_dict(orient="records"):
        lines.append(
            f"| `{row['score_name']}` | `{row['window_name']}` | {int(row['safe_row_count'])} | "
            f"{_fmt(row['mean_rank_ic'])} | {_fmt(row['rank_ic_t_stat'])} | {_fmt(row['median_rank_ic'])} | "
            f"{_fmt(row['mean_top_bottom_spread'])} | {_fmt(row['top_bottom_t_stat'])} | `{row['status']}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- H1D diagnoses score definition and tail construction only.",
            "- Missing SUE, price, or return coverage remains unavailable/no_view, not zero alpha.",
            "- Downstream event evidence, typed projection, Q2, and optimizer-path work require separate explicit reopen phases.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_sue_score_definition_report_language(text: str) -> None:
    """Reject misleading H1D claims while allowing explicit non-claims."""

    scrubbed = str(text).lower()
    allowed_phrases = [
        "it does not prove sue alpha success.",
        "it does not run q2 or optimizer-path evaluation.",
        "it does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "no production approval is claimed.",
        "no broker workflow was added.",
        "no order workflow was added.",
        "no live trading workflow was added.",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in MISLEADING_SCORE_DEFINITION_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE score-definition claim detected: {claim}")


def _helper_evidence_config(config: SueScoreDefinitionDiagnosticsConfig) -> SueHistoricalEventEvidenceConfig:
    return SueHistoricalEventEvidenceConfig(
        events_path=config.events_path,
        sue_values_path=config.events_path,
        crsp_daily_path=config.crsp_daily_path,
        output_dir=config.output_dir,
        report_path=config.report_path,
        quantiles=config.quantiles,
        min_rank_ic_names=config.min_rank_ic_names,
        min_spread_names=config.min_spread_names,
        evidence_scope="expanded",
    )


def _attach_score_definitions(
    *,
    return_frame: pd.DataFrame,
    safe_events: pd.DataFrame,
    crsp_daily_path: str,
    config: SueScoreDefinitionDiagnosticsConfig,
) -> pd.DataFrame:
    event_columns = [
        "event_id",
        "actual_eps",
        "expected_eps",
        "price_anchor_date",
    ]
    merged = return_frame.merge(safe_events.loc[:, event_columns], on="event_id", how="left")
    merged["price_anchor_price"] = _anchor_prices(crsp_daily_path=crsp_daily_path, safe_events=safe_events)
    raw = pd.to_numeric(merged["sue_value"], errors="coerce")
    expected = pd.to_numeric(merged["expected_eps"], errors="coerce").abs()
    actual = pd.to_numeric(merged["actual_eps"], errors="coerce").abs()
    price = pd.to_numeric(merged["price_anchor_price"], errors="coerce").abs()
    merged["raw_eps_diff"] = raw
    merged["raw_eps_diff_winsorized_global"] = _winsorize(raw, config)
    merged["surprise_pct_expected_eps"] = raw / (expected + config.epsilon)
    merged["surprise_pct_expected_eps_winsorized_global"] = _winsorize(
        merged["surprise_pct_expected_eps"],
        config,
    )
    merged["surprise_pct_actual_eps"] = raw / (actual + config.epsilon)
    merged["surprise_pct_actual_eps_winsorized_global"] = _winsorize(
        merged["surprise_pct_actual_eps"],
        config,
    )
    merged["price_scaled_raw_eps_diff"] = raw / (price + config.epsilon)
    return merged


def _anchor_prices(*, crsp_daily_path: str, safe_events: pd.DataFrame) -> pd.Series:
    prices = pd.read_csv(crsp_daily_path)
    if "prc" not in prices.columns:
        return pd.Series([None] * len(safe_events), index=safe_events.index)
    frame = prices.loc[:, ["permno", "date", "prc"]].copy()
    frame["permno"] = pd.to_numeric(frame["permno"], errors="coerce")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["prc"] = pd.to_numeric(frame["prc"], errors="coerce").abs()
    event_prices = safe_events.loc[:, ["event_id", "permno", "price_anchor_date"]].copy()
    event_prices["permno"] = pd.to_numeric(event_prices["permno"], errors="coerce")
    event_prices["price_anchor_date"] = pd.to_datetime(event_prices["price_anchor_date"], errors="coerce").dt.date
    joined = event_prices.merge(
        frame,
        left_on=["permno", "price_anchor_date"],
        right_on=["permno", "date"],
        how="left",
    )
    return joined["prc"]


def _score_definition_tables(
    *,
    scored_frame: pd.DataFrame,
    helper_config: SueHistoricalEventEvidenceConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    grid_rows: list[dict[str, Any]] = []
    by_date_rows: list[pd.DataFrame] = []
    for score_name in SCORE_DEFINITIONS:
        if score_name not in scored_frame.columns:
            continue
        rank_rows = _rank_ic_by_date(scored_frame, score_column=score_name, config=helper_config)
        spread_rows = _top_bottom_spread_by_date(scored_frame, score_column=score_name, config=helper_config)
        grid = _event_window_grid(
            live_return_frame=scored_frame,
            rank_ic_by_date=rank_rows,
            top_bottom_spread_by_date=spread_rows,
        )
        for row in grid.to_dict(orient="records"):
            values_for_window = scored_frame.loc[
                scored_frame["window_name"].eq(row["window_name"]),
                score_name,
            ].dropna()
            rank_values = pd.to_numeric(
                rank_rows.loc[rank_rows["window_name"].eq(row["window_name"]), "rank_ic"],
                errors="coerce",
            ).dropna()
            spread_values = pd.to_numeric(
                spread_rows.loc[spread_rows["window_name"].eq(row["window_name"]), "top_bottom_spread"],
                errors="coerce",
            ).dropna()
            grid_rows.append(
                {
                    "schema_version": SUE_SCORE_DEFINITION_DIAGNOSTICS_SCHEMA_VERSION,
                    "score_name": score_name,
                    "window_name": row["window_name"],
                    "start_offset": row["start_offset"],
                    "end_offset": row["end_offset"],
                    "safe_row_count": row["safe_row_count"],
                    "missing_return_window_count": row["missing_return_window_count"],
                    "score_available_count": int(values_for_window.notna().sum()),
                    "rank_ic_date_count": row["rank_ic_date_count"],
                    "mean_rank_ic": row["mean_rank_ic"],
                    "rank_ic_t_stat": row["rank_ic_t_stat"],
                    "median_rank_ic": _median_or_none(rank_values),
                    "top_bottom_date_count": row["top_bottom_date_count"],
                    "mean_top_bottom_spread": row["mean_top_bottom_spread"],
                    "top_bottom_t_stat": row["top_bottom_t_stat"],
                    "median_top_bottom_spread": _median_or_none(spread_values),
                    "pooled_top_bottom_spread": _pooled_top_bottom_spread(
                        scored_frame=scored_frame,
                        score_column=score_name,
                        window_name=row["window_name"],
                        quantiles=helper_config.quantiles,
                        min_spread_names=helper_config.min_spread_names,
                    ),
                    "status": _score_status(values_for_window, row),
                }
            )
        rank_rows = rank_rows.rename(columns={"rank_ic": "rank_ic"}).copy()
        spread_rows = spread_rows.rename(columns={"top_bottom_spread": "top_bottom_spread"}).copy()
        date_table = rank_rows.merge(
            spread_rows.loc[:, ["window_name", "date", "top_bottom_spread", "top_count", "bottom_count"]],
            on=["window_name", "date"],
            how="outer",
        )
        date_table.insert(0, "score_name", score_name)
        by_date_rows.append(date_table.dropna(axis=1, how="all"))
    grid = pd.DataFrame(grid_rows)
    by_date = pd.concat(by_date_rows, ignore_index=True) if by_date_rows else pd.DataFrame()
    return grid, by_date


def _diagnostic_summary(
    *,
    config: SueScoreDefinitionDiagnosticsConfig,
    safe_events: pd.DataFrame,
    scored_frame: pd.DataFrame,
    score_definition_grid: pd.DataFrame,
) -> dict[str, Any]:
    diagnostic_window = "plus_2_plus_22"
    preferred = _preferred_score(score_definition_grid, diagnostic_window=diagnostic_window)
    raw_row = _grid_row(score_definition_grid, "raw_eps_diff", diagnostic_window)
    scale_rows = [
        _grid_row(score_definition_grid, score_name, diagnostic_window)
        for score_name in SCALE_AWARE_SCORE_PRIORITY
        if score_name in set(score_definition_grid["score_name"])
    ]
    raw_rank = _float_or_none(raw_row.get("mean_rank_ic"))
    raw_spread = _float_or_none(raw_row.get("mean_top_bottom_spread"))
    scale_spread_positive = any((_float_or_none(row.get("mean_top_bottom_spread")) or 0.0) > 0.0 for row in scale_rows)
    raw_warning = bool((raw_rank is not None and raw_rank > 0.0 and raw_spread is not None and raw_spread < 0.0) or scale_spread_positive)
    payload = {
        "schema_version": "sue_score_definition_diagnostic_summary.v1",
        "diagnostic_window": diagnostic_window,
        "event_count": int(safe_events["event_id"].nunique()) if not safe_events.empty else 0,
        "rebalance_date_count": int(safe_events["rebalance_date"].nunique()) if not safe_events.empty else 0,
        "return_frame_rows": int(len(scored_frame)),
        "score_definitions_tested": list(SCORE_DEFINITIONS),
        "preferred_diagnostic_score": preferred,
        "raw_eps_diff_mean_rank_ic": raw_rank,
        "raw_eps_diff_mean_top_bottom_spread": raw_spread,
        "raw_eps_diff_scale_warning": raw_warning,
        "diagnostic_interpretation": _diagnostic_interpretation(raw_warning=raw_warning, preferred=preferred),
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "alpha_registry_promoted": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
        "input_artifacts": {
            "events_path": config.events_path,
            "crsp_daily_path": config.crsp_daily_path,
        },
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _preferred_score(score_definition_grid: pd.DataFrame, *, diagnostic_window: str) -> str:
    window = score_definition_grid.loc[score_definition_grid["window_name"].eq(diagnostic_window)].copy()
    for score_name in SCALE_AWARE_SCORE_PRIORITY:
        row = _grid_row(window, score_name, diagnostic_window)
        spread = _float_or_none(row.get("mean_top_bottom_spread"))
        rank = _float_or_none(row.get("mean_rank_ic"))
        if spread is not None and spread > 0.0 and rank is not None and rank >= 0.0:
            return score_name
    observed = window.copy()
    observed["spread_score"] = pd.to_numeric(observed["mean_top_bottom_spread"], errors="coerce").fillna(-999.0)
    observed["rank_score"] = pd.to_numeric(observed["mean_rank_ic"], errors="coerce").fillna(-999.0)
    if observed.empty:
        return "unavailable"
    return str(observed.sort_values(["spread_score", "rank_score"], ascending=False).iloc[0]["score_name"])


def _grid_row(frame: pd.DataFrame, score_name: str, window_name: str) -> dict[str, Any]:
    rows = frame.loc[frame["score_name"].eq(score_name) & frame["window_name"].eq(window_name)]
    if rows.empty:
        return {}
    return rows.iloc[0].where(pd.notna(rows.iloc[0]), None).to_dict()


def _score_status(values_for_window: pd.Series, row: dict[str, Any]) -> str:
    if values_for_window.dropna().empty:
        return "unavailable_missing_score_definition_inputs"
    if int(row.get("rank_ic_date_count") or 0) == 0 and int(row.get("top_bottom_date_count") or 0) == 0:
        return "unavailable_insufficient_cross_section"
    return "observed"


def _diagnostic_interpretation(*, raw_warning: bool, preferred: str) -> str:
    if preferred == "unavailable":
        return "sue_score_definition_inconclusive"
    if raw_warning and preferred != "raw_eps_diff":
        return "sue_raw_eps_diff_not_preferred"
    return "sue_score_definition_mixed"


def _winsorize(values: pd.Series, config: SueScoreDefinitionDiagnosticsConfig) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    observed = numeric.dropna()
    if observed.empty:
        return numeric
    lower = float(observed.quantile(config.winsor_lower_quantile))
    upper = float(observed.quantile(config.winsor_upper_quantile))
    return numeric.clip(lower=lower, upper=upper)


def _pooled_top_bottom_spread(
    *,
    scored_frame: pd.DataFrame,
    score_column: str,
    window_name: str,
    quantiles: int,
    min_spread_names: int,
) -> float | None:
    observed = scored_frame.loc[scored_frame["window_name"].eq(window_name)].dropna(
        subset=[score_column, "window_return"]
    )
    if len(observed) < min_spread_names or observed[score_column].nunique() < 2:
        return None
    ranked = observed.sort_values(score_column)
    bucket_size = max(1, len(ranked) // quantiles)
    bottom = ranked.head(bucket_size)
    top = ranked.tail(bucket_size)
    return float(top["window_return"].mean() - bottom["window_return"].mean())


def _median_or_none(values: pd.Series) -> float | None:
    observed = pd.to_numeric(values, errors="coerce").dropna()
    if observed.empty:
        return None
    return float(observed.median())


def _float_or_none(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _fmt(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):.6f}"


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(__import__("json").dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
