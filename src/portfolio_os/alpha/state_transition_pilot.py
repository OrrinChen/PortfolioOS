"""Thin artifact runners for the A-share state-transition pilot."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from portfolio_os.alpha.state_transition_panel import (
    REQUIRED_STATE_TRANSITION_COLUMNS,
    build_state_transition_daily_panel,
    build_state_transition_matching_covariates,
    build_upper_limit_event_conditioned_null_pool,
    build_upper_limit_event_conditioned_null_summary,
    build_upper_limit_matched_control_comparison_frame,
    build_upper_limit_matched_non_event_control_frame,
    build_upper_limit_pilot_read_frame,
    build_upper_limit_pilot_expression_frame,
    build_upper_limit_pre_event_placebo_comparison_frame,
    extract_upper_limit_daily_state_slice,
)
from portfolio_os.data.loaders import normalize_ticker, parse_bool, read_csv
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.storage.snapshots import write_json, write_text


_UPPER_LIMIT_PILOT_REFERENCE_COLUMNS = ("industry", "issuer_total_shares")


@dataclass
class UpperLimitPilotRunResult:
    """Serializable outputs for one upper-limit pilot artifact run."""

    output_dir: Path
    read_frame: pd.DataFrame
    null_summary_frame: pd.DataFrame
    summary_payload: dict[str, object]
    note_markdown: str


def load_upper_limit_pilot_daily_panel_csv(path: str | Path) -> pd.DataFrame:
    """Load one daily CSV into the normalized upper-limit pilot panel contract."""

    source_path = Path(path)
    frame = read_csv(source_path)

    missing_reference = [
        column for column in _UPPER_LIMIT_PILOT_REFERENCE_COLUMNS if column not in frame.columns
    ]
    if missing_reference:
        raise InputValidationError(
            "upper-limit pilot daily csv requires pilot reference columns: "
            + ", ".join(missing_reference)
        )

    missing_state_columns = [
        column for column in REQUIRED_STATE_TRANSITION_COLUMNS if column not in frame.columns
    ]
    if missing_state_columns:
        raise InputValidationError(
            "upper-limit pilot daily csv is missing required state-transition columns: "
            + ", ".join(missing_state_columns)
        )

    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    frame["tradable"] = frame["tradable"].map(
        lambda value: parse_bool(value, "tradable")
    )
    frame["industry"] = frame["industry"].astype(str).str.strip()
    if frame["industry"].eq("").any():
        raise InputValidationError("upper-limit pilot daily csv contains blank industries.")

    frame["issuer_total_shares"] = pd.to_numeric(
        frame["issuer_total_shares"], errors="coerce"
    )
    if frame["issuer_total_shares"].isna().any() or (
        frame["issuer_total_shares"] <= 0
    ).any():
        raise InputValidationError(
            "upper-limit pilot daily csv contains non-positive issuer_total_shares."
        )

    return build_state_transition_daily_panel(frame)


def _build_upper_limit_pilot_reference_frame(daily_panel: pd.DataFrame) -> pd.DataFrame:
    reference = daily_panel.loc[:, ["ticker", "industry", "issuer_total_shares"]].copy()
    inconsistent_industry = (
        reference.groupby("ticker", sort=False)["industry"].nunique(dropna=False) > 1
    )
    inconsistent_shares = (
        reference.groupby("ticker", sort=False)["issuer_total_shares"].nunique(dropna=False) > 1
    )
    inconsistent_tickers = sorted(
        set(inconsistent_industry.loc[inconsistent_industry].index.tolist())
        | set(inconsistent_shares.loc[inconsistent_shares].index.tolist())
    )
    if inconsistent_tickers:
        raise InputValidationError(
            "upper-limit pilot daily csv contains inconsistent reference fields for ticker(s): "
            + ", ".join(inconsistent_tickers)
        )
    return reference.drop_duplicates(subset=["ticker"]).reset_index(drop=True)


def _render_upper_limit_pilot_note(
    *,
    read_frame: pd.DataFrame,
    summary_payload: dict[str, object],
) -> str:
    lines = [
        "# Upper-Limit Pilot Read",
        "",
        f"- pilot: `{summary_payload['pilot_name']}`",
        f"- expressions: `{int(summary_payload['expression_count'])}`",
        f"- null seeds: `{int(summary_payload['null_seed_count'])}`",
        f"- degenerate expressions: `{int(summary_payload['degenerate_expression_count'])}`",
    ]
    degenerate_ids = summary_payload.get("degenerate_expression_ids", [])
    if degenerate_ids:
        lines.extend(
            [
                "",
                "## Degenerate Nulls",
                "",
                *[f"- `{expression_id}`" for expression_id in degenerate_ids],
            ]
        )
    if not read_frame.empty:
        lines.extend(["", "## Expression Reads", ""])
        for row in read_frame.itertuples(index=False):
            lines.append(
                f"- `{row.expression_id}`: observed_mean={float(row.observed_mean_forward_return):.6f}, "
                f"excess_vs_control={float(row.mean_excess_vs_control):.6f}, "
                f"excess_vs_placebo={float(row.mean_excess_vs_placebo):.6f}, "
                f"degenerate_null={bool(row.null_is_degenerate)}"
            )
    return "\n".join(lines) + "\n"


def run_upper_limit_pilot_artifact_bundle(
    *,
    expression_frame: pd.DataFrame,
    control_comparison_frame: pd.DataFrame,
    placebo_comparison_frame: pd.DataFrame,
    null_pool: pd.DataFrame,
    random_seeds: list[int],
    output_dir: str | Path,
) -> UpperLimitPilotRunResult:
    """Write one thin upper-limit pilot artifact bundle from existing D2 read objects."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    null_summary_frame = build_upper_limit_event_conditioned_null_summary(
        null_pool,
        random_seeds=random_seeds,
    )
    read_frame = build_upper_limit_pilot_read_frame(
        expression_frame,
        control_comparison_frame,
        placebo_comparison_frame,
        null_summary_frame,
    )

    degenerate_ids = (
        read_frame.loc[read_frame["null_is_degenerate"].fillna(False), "expression_id"]
        .astype(str)
        .tolist()
        if not read_frame.empty
        else []
    )
    summary_payload: dict[str, object] = {
        "pilot_name": "upper_limit_daily_state",
        "expression_count": int(read_frame["expression_id"].nunique()) if not read_frame.empty else 0,
        "null_seed_count": int(len(random_seeds)),
        "degenerate_expression_count": int(len(degenerate_ids)),
        "degenerate_expression_ids": degenerate_ids,
    }
    note_markdown = _render_upper_limit_pilot_note(
        read_frame=read_frame,
        summary_payload=summary_payload,
    )

    expression_frame.to_csv(output_path / "expression_frame.csv", index=False)
    control_comparison_frame.to_csv(output_path / "control_comparison.csv", index=False)
    placebo_comparison_frame.to_csv(output_path / "placebo_comparison.csv", index=False)
    null_pool.to_csv(output_path / "null_pool.csv", index=False)
    null_summary_frame.to_csv(output_path / "null_summary.csv", index=False)
    read_frame.to_csv(output_path / "pilot_read_frame.csv", index=False)
    write_json(output_path / "summary.json", summary_payload)
    write_text(output_path / "note.md", note_markdown)

    return UpperLimitPilotRunResult(
        output_dir=output_path,
        read_frame=read_frame,
        null_summary_frame=null_summary_frame,
        summary_payload=summary_payload,
        note_markdown=note_markdown,
    )


def run_upper_limit_pilot_artifact_bundle_from_daily_csv(
    *,
    daily_panel_path: str | Path,
    lookback_days: int = 20,
    random_seeds: list[int],
    output_dir: str | Path,
) -> UpperLimitPilotRunResult:
    """Build and write one upper-limit pilot artifact bundle directly from a daily CSV."""

    daily_panel = load_upper_limit_pilot_daily_panel_csv(daily_panel_path)
    reference_frame = _build_upper_limit_pilot_reference_frame(daily_panel)
    matching_source = daily_panel.drop(
        columns=list(_UPPER_LIMIT_PILOT_REFERENCE_COLUMNS),
        errors="ignore",
    )
    matching_panel = build_state_transition_matching_covariates(
        matching_source,
        reference_frame,
        lookback_days=lookback_days,
    )
    event_panel = extract_upper_limit_daily_state_slice(matching_panel)
    expression_frame = build_upper_limit_pilot_expression_frame(event_panel)
    matched_control_frame = build_upper_limit_matched_non_event_control_frame(
        matching_panel
    )
    control_comparison_frame = build_upper_limit_matched_control_comparison_frame(
        expression_frame,
        matched_control_frame,
        matching_panel,
    )
    placebo_comparison_frame = build_upper_limit_pre_event_placebo_comparison_frame(
        expression_frame,
        daily_panel,
    )
    null_pool = build_upper_limit_event_conditioned_null_pool(
        expression_frame,
        matching_panel,
    )
    return run_upper_limit_pilot_artifact_bundle(
        expression_frame=expression_frame,
        control_comparison_frame=control_comparison_frame,
        placebo_comparison_frame=placebo_comparison_frame,
        null_pool=null_pool,
        random_seeds=random_seeds,
        output_dir=output_dir,
    )
