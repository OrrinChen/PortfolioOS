"""Thin artifact runners for the A-share state-transition pilot."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from portfolio_os.alpha.state_transition_panel import (
    build_upper_limit_event_conditioned_null_summary,
    build_upper_limit_pilot_read_frame,
)
from portfolio_os.storage.snapshots import write_json, write_text


@dataclass
class UpperLimitPilotRunResult:
    """Serializable outputs for one upper-limit pilot artifact run."""

    output_dir: Path
    read_frame: pd.DataFrame
    null_summary_frame: pd.DataFrame
    summary_payload: dict[str, object]
    note_markdown: str


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
