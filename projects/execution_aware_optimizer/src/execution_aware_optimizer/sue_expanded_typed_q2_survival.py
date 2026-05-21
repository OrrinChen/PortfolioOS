"""Expanded deterministic SUE typed-Q2 candidate benchmark.

Phase 56A expands the SUE fixture breadth while preserving the existing
Typed AlphaView -> Projection -> Q2 boundaries. It remains local-only and does
not call brokers, submit orders, use live data, or claim production approval.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, time, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from pydantic import Field

from execution_aware_optimizer.sue_expanded_survival_schema import (
    ExpandedSueEventRow,
    SueExpandedFixtureConfig,
    SueExpandedTypedQ2SurvivalInput,
    SueExpandedTypedQ2SurvivalResult,
)
from execution_aware_optimizer.sue_typed_q2_survival_schema import SueTypedQ2SurvivalRow
from execution_aware_optimizer.typed_expected_return_injection import (
    OPTIMIZER_INPUT_SNAPSHOT_COLUMNS,
    TypedExpectedReturnInjectionRun,
    run_typed_expected_return_injection,
)
from execution_aware_optimizer.typed_injection_schema import TypedExpectedReturnInjectionInput
from portfolio_os.alpha.projection import (
    ABSTAIN_REPORT_COLUMNS,
    EXPECTED_RETURN_PANEL_COLUMNS,
    AlphaProjectionConfig,
    AlphaProjectionResult,
    project_alpha_views_to_expected_returns,
)
from portfolio_os.alpha.view_contract import AlphaView
from portfolio_os.observability.events import TraceEvent
from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file


SUE_EXPANDED_MATRIX_COLUMNS = [
    "schema_version",
    "scenario_id",
    "alpha_family",
    "projection_policy",
    "abstain_policy",
    "layer",
    "date",
    "status",
    "active_rebalance_count",
    "active_name_count",
    "expected_return_used_share",
    "gross_return",
    "net_return",
    "turnover",
    "cost_drag",
    "gross_to_net_retention",
    "repair_retention",
    "unavailable_reason",
    "source_config_hash",
]


class SueExpandedTypedQ2SurvivalRun(SueExpandedTypedQ2SurvivalResult):
    """Expanded SUE result plus writeable local artifacts."""

    event_rows: list[ExpandedSueEventRow] = Field(default_factory=list, exclude=True)
    expected_return_panel: list[dict[str, Any]] = Field(default_factory=list, exclude=True)
    projection_manifest: dict[str, Any] = Field(default_factory=dict, exclude=True)
    projection_diagnostics: list[dict[str, Any]] = Field(default_factory=list, exclude=True)
    abstain_report: list[dict[str, Any]] = Field(default_factory=list, exclude=True)
    q2_input_contract_v2: dict[str, Any] = Field(default_factory=dict, exclude=True)
    optimizer_input_snapshot: Any = Field(default=None, exclude=True)


def run_sue_expanded_typed_q2_survival(
    survival_input: SueExpandedTypedQ2SurvivalInput,
) -> SueExpandedTypedQ2SurvivalRun:
    """Run the expanded deterministic SUE typed-Q2 candidate benchmark."""

    input_hashes = _input_artifact_hashes(survival_input)
    source_config_hash = hash_payload(
        {
            "survival_input": survival_input.model_dump(mode="json"),
            "input_artifact_hashes": input_hashes,
        }
    )
    missing = _validate_required_artifacts(survival_input)
    if missing:
        return _build_run(
            survival_input=survival_input,
            evidence_mode="deterministic_fixture",
            survival_status="rejected",
            injection_status="rejected",
            expected_return_reached_optimizer_input=False,
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            rejection_reasons=missing,
        )

    try:
        config = load_expanded_sue_fixture_config(survival_input.fixture_config_path)
        event_rows = generate_expanded_sue_event_rows(config)
        projection = project_expanded_sue_events(event_rows, config)
        q2_input_contract = _q2_input_contract(projection.alpha_projection_manifest, projection.expected_return_panel)
        representative_panel = _representative_injection_panel(
            projection.expected_return_panel,
            local_rebalance_date=survival_input.local_rebalance_date,
        )
        with TemporaryDirectory(prefix="portfolioos_sue_expanded_") as work_dir_text:
            injection_run = _run_local_injection(
                survival_input=survival_input,
                projection=projection,
                q2_input_contract=q2_input_contract,
                representative_panel=representative_panel,
                work_dir=Path(work_dir_text),
            )
    except Exception as exc:  # noqa: BLE001 - deterministic unavailable wrapper
        return _build_run(
            survival_input=survival_input,
            evidence_mode="deterministic_fixture",
            survival_status="unavailable",
            injection_status="unavailable",
            expected_return_reached_optimizer_input=False,
            source_config_hash=source_config_hash,
            input_hashes=input_hashes,
            unavailable_reason=f"expanded SUE typed-Q2 unavailable: {exc}",
        )

    summary_counts = _expanded_counts(
        event_rows=event_rows,
        projection=projection,
        universe_symbols=config.universe_symbols,
        rebalance_date_count=len(config.rebalance_dates),
    )
    q2_rows = _map_q2_rows(
        injection_run,
        active_rebalance_count=summary_counts["active_rebalance_count"],
        active_name_count=summary_counts["active_name_count"],
        expected_return_used_share=summary_counts["expected_return_used_share"],
    )
    return _build_run(
        survival_input=survival_input,
        evidence_mode=config.evidence_mode,
        survival_status=_survival_status(injection_run, q2_rows),
        injection_status=injection_run.result.injection_status,
        expected_return_reached_optimizer_input=injection_run.result.expected_return_reached_optimizer_input,
        source_config_hash=source_config_hash,
        input_hashes=input_hashes,
        event_count=summary_counts["event_count"],
        rebalance_date_count=summary_counts["rebalance_date_count"],
        active_rebalance_count=summary_counts["active_rebalance_count"],
        active_name_count=summary_counts["active_name_count"],
        active_names_by_rebalance_date=summary_counts["active_names_by_rebalance_date"],
        expected_return_used_share=summary_counts["expected_return_used_share"],
        abstain_count=summary_counts["abstain_count"],
        coverage_loss_count=summary_counts["coverage_loss_count"],
        q2_observed_rows=sum(1 for row in q2_rows if row.status == "observed"),
        q2_unavailable_rows=sum(1 for row in q2_rows if row.status == "unavailable"),
        q2_rows=q2_rows,
        event_rows=event_rows,
        expected_return_panel=projection.expected_return_panel,
        projection_manifest=projection.alpha_projection_manifest,
        projection_diagnostics=projection.alpha_projection_diagnostics,
        abstain_report=projection.alpha_abstain_report,
        q2_input_contract_v2=q2_input_contract,
        optimizer_input_snapshot=injection_run.optimizer_input_snapshot,
        rejection_reasons=injection_run.result.rejection_reasons,
        unavailable_reason=injection_run.result.unavailable_reason,
    )


def write_sue_expanded_typed_q2_survival_artifacts(
    result: SueExpandedTypedQ2SurvivalRun | SueExpandedTypedQ2SurvivalResult,
    output_dir: str | Path,
    *,
    report_path: str | Path | None = None,
) -> dict[str, Path]:
    """Write expanded SUE candidate artifacts."""

    run = _ensure_run(result)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_destination = Path(report_path) if report_path is not None else output_path / "sue_expanded_typed_q2_survival_report.md"
    report_destination.parent.mkdir(parents=True, exist_ok=True)

    event_rows_path = output_path / "sue_expanded_event_rows.csv"
    panel_path = output_path / "sue_expanded_expected_return_panel.csv"
    projection_manifest_path = output_path / "sue_expanded_projection_manifest.json"
    projection_diagnostics_path = output_path / "sue_expanded_projection_diagnostics.json"
    abstain_report_path = output_path / "sue_expanded_abstain_report.json"
    q2_input_path = output_path / "sue_expanded_q2_input_contract_v2.json"
    q2_matrix_path = output_path / "sue_expanded_q2_execution_matrix.csv"
    summary_path = output_path / "sue_expanded_typed_q2_survival_summary.json"
    result_path = output_path / "sue_expanded_typed_q2_survival_result.json"
    snapshot_path = output_path / "sue_expanded_optimizer_input_snapshot.csv"
    attribution_path = output_path / "sue_expanded_q2_attribution.json"
    trace_path = output_path / "sue_expanded_q2_trace.jsonl"

    pd.DataFrame([row.model_dump(mode="json") for row in run.event_rows]).to_csv(event_rows_path, index=False)
    pd.DataFrame(run.expected_return_panel).reindex(columns=EXPECTED_RETURN_PANEL_COLUMNS).to_csv(panel_path, index=False)
    _write_json(projection_manifest_path, run.projection_manifest)
    _write_json(projection_diagnostics_path, {"diagnostics": run.projection_diagnostics})
    _write_json(abstain_report_path, {"abstain_report": run.abstain_report})
    _write_json(q2_input_path, run.q2_input_contract_v2)
    pd.DataFrame([row.model_dump(mode="json") for row in run.q2_matrix_rows]).reindex(
        columns=SUE_EXPANDED_MATRIX_COLUMNS
    ).to_csv(q2_matrix_path, index=False)
    _write_json(summary_path, run.summary.model_dump(mode="json"))
    _write_json(result_path, run.model_dump(mode="json"))
    _snapshot_frame(run).reindex(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS).to_csv(snapshot_path, index=False)
    attribution = build_sue_expanded_q2_attribution(run)
    _write_json(attribution_path, attribution)
    report_destination.write_text(render_sue_expanded_typed_q2_survival_report(run, attribution), encoding="utf-8")
    _write_trace(trace_path, run)

    return {
        "event_rows": event_rows_path,
        "expected_return_panel": panel_path,
        "projection_manifest": projection_manifest_path,
        "projection_diagnostics": projection_diagnostics_path,
        "abstain_report": abstain_report_path,
        "q2_input_contract_v2": q2_input_path,
        "q2_matrix": q2_matrix_path,
        "summary": summary_path,
        "result": result_path,
        "optimizer_input_snapshot": snapshot_path,
        "attribution": attribution_path,
        "trace": trace_path,
        "report": report_destination,
    }


def load_expanded_sue_fixture_config(path: str | Path) -> SueExpandedFixtureConfig:
    """Load deterministic expanded SUE fixture config."""

    return SueExpandedFixtureConfig.model_validate(_read_json(path))


def generate_expanded_sue_event_rows(config: SueExpandedFixtureConfig) -> list[ExpandedSueEventRow]:
    """Generate deterministic event-name rows from fixture config."""

    rows: list[ExpandedSueEventRow] = []
    universe = list(config.universe_symbols)
    width = int(config.active_symbols_per_date)
    for date_index, rebalance_date in enumerate(sorted(config.rebalance_dates)):
        active_symbols = [universe[(date_index + offset) % len(universe)] for offset in range(width)]
        event_date = rebalance_date - timedelta(days=2)
        event_timestamp = datetime.combine(event_date, time(21, 5), tzinfo=timezone.utc)
        available_timestamp = event_timestamp + timedelta(minutes=10)
        tradable_timestamp = datetime.combine(rebalance_date, time(14, 30), tzinfo=timezone.utc)
        for rank, symbol in enumerate(active_symbols):
            rank_scale = float(width - rank) / float(width)
            expected_return = round(config.base_expected_return * rank_scale * (1.0 + date_index / 100.0), 12)
            rows.append(
                ExpandedSueEventRow(
                    event_id=f"SUE-EXP-{rebalance_date:%Y%m%d}-{symbol}",
                    symbol=symbol,
                    event_timestamp=event_timestamp,
                    event_available_timestamp=available_timestamp,
                    tradable_timestamp=tradable_timestamp,
                    rebalance_date=rebalance_date,
                    sue_score=round(2.5 * rank_scale + date_index / 50.0, 12),
                    expected_return=expected_return,
                    evidence_mode=config.evidence_mode,
                )
            )
    return rows


def project_expanded_sue_events(
    event_rows: list[ExpandedSueEventRow],
    config: SueExpandedFixtureConfig,
) -> AlphaProjectionResult:
    """Represent expanded SUE event rows as AlphaViews and project them."""

    views = _alpha_views_from_events(event_rows, config)
    return project_alpha_views_to_expected_returns(
        alpha_views=views,
        config=AlphaProjectionConfig(
            rebalance_dates=config.rebalance_dates,
            universe_symbols=config.universe_symbols,
            risk_horizon_days=config.risk_horizon_days,
            cost_assumptions={"cost_bps": config.cost_bps},
        ),
    )


def build_sue_expanded_q2_attribution(run: SueExpandedTypedQ2SurvivalResult) -> dict[str, Any]:
    """Build layer attribution for expanded SUE without alpha overclaiming."""

    observed_rows = [row for row in run.q2_matrix_rows if row.status == "observed"]
    unavailable_rows = [row for row in run.q2_matrix_rows if row.status == "unavailable"]
    layers = [
        {
            "layer": "evidence",
            "status": "observed",
            "details": f"{run.event_count} deterministic fixture event-name rows validate PIT sequencing.",
        },
        {
            "layer": "projection",
            "status": "observed" if run.active_rebalance_count > 0 else "failed",
            "details": (
                f"active_rebalance_count={run.active_rebalance_count}; "
                f"active_name_count={run.active_name_count}; "
                f"coverage_loss_count={run.coverage_loss_count}"
            ),
        },
        {
            "layer": "injection",
            "status": "observed" if run.expected_return_reached_optimizer_input else "unavailable",
            "details": f"injection_status={run.injection_status}",
        },
        {
            "layer": "optimizer_response",
            "status": "observed" if run.expected_return_reached_optimizer_input else "unavailable",
            "details": "Phase 49 validates local optimizer response; Phase 56A uses a representative expanded SUE date.",
        },
        _q2_layer("constraint_repair", observed_rows),
        _q2_layer("cost", observed_rows),
        _q2_layer("turnover", observed_rows),
        {
            "layer": "coverage / abstain",
            "status": "observed" if run.coverage_loss_count > 0 else "inconclusive",
            "details": f"abstain_count={run.abstain_count}; no_view is not encoded as zero alpha.",
        },
    ]
    if unavailable_rows:
        layers.append(
            {
                "layer": "unavailable fixture hooks",
                "status": "unavailable",
                "details": "; ".join(sorted({f"{row.layer}: {row.unavailable_reason}" for row in unavailable_rows})),
                "unavailable_rows": len(unavailable_rows),
            }
        )
    return {
        "schema_version": "sue_expanded_q2_attribution.v1",
        "run_id": run.run_id,
        "evidence_mode": run.evidence_mode,
        "survival_status": run.survival_status,
        "production_approval_claimed": False,
        "layers": layers,
        "non_claims": [
            "SUE alpha success is not proven by this deterministic expanded fixture.",
            "real historical evidence: not claimed",
            "paper-ready status is not claimed",
            "production approval is not claimed",
        ],
    }


def render_sue_expanded_typed_q2_survival_report(
    run: SueExpandedTypedQ2SurvivalResult,
    attribution: dict[str, Any] | None = None,
) -> str:
    """Render expanded SUE candidate report."""

    payload = attribution or build_sue_expanded_q2_attribution(run)
    lines = [
        "# Expanded SUE Typed-Q2 Survival Report",
        "",
        "This is an expanded typed-Q2 candidate benchmark, not production approval.",
        "production approval: not claimed",
        "",
        "## Evidence Scope",
        "",
        "- deterministic fixture evidence: expanded local SUE event panel",
        "- real historical evidence: not claimed",
        "- paper-ready status: not claimed",
        "",
        "## Summary",
        "",
        f"- event_count: `{run.event_count}`",
        f"- rebalance_date_count: `{run.rebalance_date_count}`",
        f"- active_rebalance_count: `{run.active_rebalance_count}`",
        f"- active_name_count: `{run.active_name_count}`",
        f"- median_active_names_per_active_date: `{run.median_active_names_per_active_date:.2f}`",
        f"- expected_return_used_share: `{run.expected_return_used_share:.6f}`",
        f"- abstain_count: `{run.abstain_count}`",
        f"- coverage_loss_count: `{run.coverage_loss_count}`",
        f"- q2_observed_rows: `{run.q2_observed_rows}`",
        f"- q2_unavailable_rows: `{run.q2_unavailable_rows}`",
        "",
        "## Attribution",
        "",
        "| layer | status | details |",
        "|---|---|---|",
    ]
    for layer in payload["layers"]:
        lines.append(f"| {layer['layer']} | {layer['status']} | {_escape_table(layer['details'])} |")
    lines.extend(
        [
            "",
            "## Safety Boundaries",
            "",
            "- no live data workflow",
            "- no broker workflow",
            "- no orders or trading instructions",
            "- no production alpha approval",
            "- missing coverage is explicit abstain; no_view != zero_alpha",
            "",
        ]
    )
    return "\n".join(lines)


def load_sue_expanded_result(path: str | Path) -> SueExpandedTypedQ2SurvivalResult:
    """Load expanded SUE result JSON."""

    return SueExpandedTypedQ2SurvivalResult.model_validate(_read_json(path))


def write_sue_expanded_q2_attribution_artifacts(
    result: SueExpandedTypedQ2SurvivalResult,
    *,
    output_dir: str | Path = "outputs/sue_expanded_typed_q2_survival",
    report_path: str | Path = "reports/sue_expanded_typed_q2_survival_report.md",
) -> dict[str, Path]:
    """Write attribution JSON and Markdown from an existing result."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_destination = Path(report_path)
    report_destination.parent.mkdir(parents=True, exist_ok=True)
    attribution_path = output_path / "sue_expanded_q2_attribution.json"
    attribution = build_sue_expanded_q2_attribution(result)
    _write_json(attribution_path, attribution)
    report_destination.write_text(render_sue_expanded_typed_q2_survival_report(result, attribution), encoding="utf-8")
    return {"attribution": attribution_path, "report": report_destination}


def _alpha_views_from_events(event_rows: list[ExpandedSueEventRow], config: SueExpandedFixtureConfig) -> list[AlphaView]:
    by_date: dict[str, list[ExpandedSueEventRow]] = {}
    for row in event_rows:
        by_date.setdefault(row.rebalance_date.isoformat(), []).append(row)

    views: list[AlphaView] = []
    for date_text, rows in sorted(by_date.items()):
        first = rows[0]
        active = {row.symbol: {"state": "active_view", "value": row.expected_return} for row in rows}
        active_symbols = set(active)
        no_view = {
            symbol: {"state": "no_view", "reason": "coverage_missing"}
            for symbol in config.universe_symbols
            if symbol not in active_symbols
        }
        payload = {
            "alpha_view_id": f"AV-US-SUE-EXPANDED-{date_text.replace('-', '')}",
            "family_id": "US_EVENT_SUE",
            "mechanism_type": "event",
            "universe_id": "US_EXPANDED_SUE_FIXTURE",
            "signal_timestamp": first.event_timestamp.isoformat().replace("+00:00", "Z"),
            "visibility_timestamp": first.event_available_timestamp.isoformat().replace("+00:00", "Z"),
            "tradable_timestamp": first.tradable_timestamp.isoformat().replace("+00:00", "Z"),
            "anchor_event_timestamp": first.event_timestamp.isoformat().replace("+00:00", "Z"),
            "horizon_type": "event_window",
            "holding_window": {"start_offset_days": 2, "end_offset_days": 22},
            "decay_policy": {"mode": "event_half_life", "half_life_days": 10},
            "coverage_mask": {
                "mode": "explicit_abstain",
                "covered_symbols": sorted(active_symbols),
                "uncovered_symbols": sorted(no_view),
            },
            "abstain_policy": {"mode": "explicit_abstain", "coverage_threshold": 0.8, "stale_after_days": 3},
            "expected_return_view": {**active, **no_view},
            "confidence_view": {
                "confidence_score": config.confidence_score,
                "evidence_strength": "expanded_deterministic_fixture",
            },
            "capacity_view": {"adv_participation_cap": 0.001, "capacity_bucket": "medium"},
            "cost_sensitivity_view": {"expected_cost_bps": config.cost_bps, "sensitivity_bucket": "medium"},
            "pit_safety_report": {
                "no_future_data": True,
                "visibility_not_after_tradable": True,
                "anchor_not_before_visibility": True,
                "pit_source": config.evidence_mode,
            },
            "provenance": {
                "created_by": "fixture",
                "source": "sue_expanded_deterministic_fixture",
                "version": config.fixture_id,
            },
        }
        views.append(AlphaView.validate_payload(payload))
    return views


def _representative_injection_panel(
    expected_return_panel: list[dict[str, Any]],
    *,
    local_rebalance_date: str | None,
) -> pd.DataFrame:
    frame = pd.DataFrame(expected_return_panel)
    if frame.empty:
        return pd.DataFrame(columns=EXPECTED_RETURN_PANEL_COLUMNS)
    first_date = sorted(frame["date"].astype(str).unique())[0]
    selected = frame.loc[frame["date"].astype(str) == first_date].copy()
    selected["date"] = local_rebalance_date or first_date
    return selected.reindex(columns=EXPECTED_RETURN_PANEL_COLUMNS)


def _run_local_injection(
    *,
    survival_input: SueExpandedTypedQ2SurvivalInput,
    projection: AlphaProjectionResult,
    q2_input_contract: dict[str, Any],
    representative_panel: pd.DataFrame,
    work_dir: Path,
) -> TypedExpectedReturnInjectionRun:
    work_dir.mkdir(parents=True, exist_ok=True)
    panel_path = work_dir / "sue_expanded_representative_panel.csv"
    representative_panel.to_csv(panel_path, index=False)
    projection_manifest = dict(projection.alpha_projection_manifest)
    projection_manifest["rebalance_dates"] = sorted(representative_panel["date"].astype(str).unique())
    projection_manifest["panel_row_count"] = int(len(representative_panel))
    projection_manifest.pop("content_hash", None)
    projection_manifest["content_hash"] = hash_payload(projection_manifest)
    projection_path = work_dir / "sue_expanded_projection_manifest.json"
    _write_json(projection_path, projection_manifest)
    contract = dict(q2_input_contract)
    contract["expected_return_panel_artifact"] = panel_path.name
    contract["projection_manifest_hash"] = projection_manifest["content_hash"]
    q2_path = work_dir / "sue_expanded_q2_input_contract_v2.json"
    _write_json(q2_path, contract)

    return run_typed_expected_return_injection(
        TypedExpectedReturnInjectionInput.model_validate(
            {
                "adapter_config_path": str(survival_input.adapter_config_path),
                "allow_portfolioos_run": survival_input.allow_portfolioos_run,
                "expected_return_panel_path": str(panel_path),
                "local_backtest_manifest_path": str(survival_input.local_backtest_manifest_path),
                "no_broker": True,
                "no_network": True,
                "projection_manifest_path": str(projection_path),
                "q2_input_contract_v2_path": str(q2_path),
                "rebalance_date": survival_input.local_rebalance_date,
                "run_id": survival_input.run_id,
            }
        )
    )


def _q2_input_contract(
    projection_manifest: dict[str, Any],
    expected_return_panel: list[dict[str, Any]],
) -> dict[str, Any]:
    _ = expected_return_panel
    alpha_view_ids = list(projection_manifest.get("alpha_view_ids", []))
    return {
        "schema_version": "q2_input_contract.v2",
        "bundle_id": "EB-SUE-EXPANDED-DETERMINISTIC-001",
        "alpha_view_id": alpha_view_ids[0],
        "input_type": "projected_expected_return_panel",
        "expected_return_panel_artifact": "sue_expanded_expected_return_panel.csv",
        "projection_manifest_hash": projection_manifest["content_hash"],
        "alpha_projection_diagnostics_artifact": "sue_expanded_projection_diagnostics.json",
        "alpha_abstain_report_artifact": "sue_expanded_abstain_report.json",
        "allowed_consumer": "projects/execution_aware_optimizer",
        "direct_q2_execution_allowed": False,
    }


def _expanded_counts(
    *,
    event_rows: list[ExpandedSueEventRow],
    projection: AlphaProjectionResult,
    universe_symbols: list[str],
    rebalance_date_count: int,
) -> dict[str, Any]:
    panel = projection.expected_return_panel
    active_names_by_date = Counter(str(row["date"]) for row in panel)
    active_pairs = {(str(row["date"]), str(row["symbol"]).upper()) for row in panel}
    denominator = len(universe_symbols) * rebalance_date_count
    coverage_loss_count = sum(
        1
        for row in projection.alpha_abstain_report
        if row.get("reason") in {"coverage_missing", "explicit_no_view", "missing_expected_return_view"}
    )
    return {
        "event_count": len(event_rows),
        "rebalance_date_count": rebalance_date_count,
        "active_rebalance_count": len(active_names_by_date),
        "active_name_count": len({symbol for _date, symbol in active_pairs}),
        "active_names_by_rebalance_date": dict(sorted(active_names_by_date.items())),
        "expected_return_used_share": float(len(active_pairs) / denominator) if denominator else 0.0,
        "abstain_count": len(projection.alpha_abstain_report),
        "coverage_loss_count": coverage_loss_count,
    }


def _map_q2_rows(
    injection_run: TypedExpectedReturnInjectionRun,
    *,
    active_rebalance_count: int,
    active_name_count: int,
    expected_return_used_share: float,
) -> list[SueTypedQ2SurvivalRow]:
    rows: list[SueTypedQ2SurvivalRow] = []
    for row in injection_run.q2_matrix_rows:
        rows.append(
            SueTypedQ2SurvivalRow(
                scenario_id=str(row.get("scenario_id") or "").replace(
                    "synthetic_typed_fixture", "sue_expanded_typed_q2"
                ),
                layer=str(row.get("layer")),
                date=row.get("date"),
                status=row.get("status"),
                active_rebalance_count=active_rebalance_count,
                active_name_count=active_name_count,
                expected_return_used_share=expected_return_used_share,
                gross_return=row.get("gross_return"),
                net_return=row.get("net_return"),
                turnover=row.get("turnover"),
                cost_drag=row.get("cost_drag"),
                gross_to_net_retention=row.get("gross_to_net_retention"),
                repair_retention=row.get("repair_retention"),
                unavailable_reason=row.get("unavailable_reason"),
                source_config_hash=str(row.get("source_config_hash") or ""),
            )
        )
    return rows


def _survival_status(
    injection_run: TypedExpectedReturnInjectionRun,
    rows: list[SueTypedQ2SurvivalRow],
) -> str:
    if injection_run.result.injection_status == "rejected":
        return "rejected"
    observed = sum(1 for row in rows if row.status == "observed")
    unavailable = sum(1 for row in rows if row.status == "unavailable")
    if observed and unavailable:
        return "partially_observed"
    if observed:
        return "observed"
    return "unavailable"


def _build_run(
    *,
    survival_input: SueExpandedTypedQ2SurvivalInput,
    evidence_mode: str,
    survival_status: str,
    injection_status: str,
    expected_return_reached_optimizer_input: bool,
    source_config_hash: str,
    input_hashes: dict[str, str],
    event_count: int = 0,
    rebalance_date_count: int = 0,
    active_rebalance_count: int = 0,
    active_name_count: int = 0,
    active_names_by_rebalance_date: dict[str, int] | None = None,
    expected_return_used_share: float = 0.0,
    abstain_count: int = 0,
    coverage_loss_count: int = 0,
    q2_observed_rows: int = 0,
    q2_unavailable_rows: int = 0,
    q2_rows: list[SueTypedQ2SurvivalRow] | None = None,
    event_rows: list[ExpandedSueEventRow] | None = None,
    expected_return_panel: list[dict[str, Any]] | None = None,
    projection_manifest: dict[str, Any] | None = None,
    projection_diagnostics: list[dict[str, Any]] | None = None,
    abstain_report: list[dict[str, Any]] | None = None,
    q2_input_contract_v2: dict[str, Any] | None = None,
    optimizer_input_snapshot: Any = None,
    rejection_reasons: list[str] | None = None,
    unavailable_reason: str | None = None,
) -> SueExpandedTypedQ2SurvivalRun:
    return SueExpandedTypedQ2SurvivalRun(
        run_id=survival_input.run_id,
        evidence_mode=evidence_mode,
        survival_status=survival_status,
        injection_status=injection_status,
        expected_return_reached_optimizer_input=expected_return_reached_optimizer_input,
        event_count=event_count,
        rebalance_date_count=rebalance_date_count,
        active_rebalance_count=active_rebalance_count,
        active_name_count=active_name_count,
        active_names_by_rebalance_date=active_names_by_rebalance_date or {},
        expected_return_used_share=expected_return_used_share,
        abstain_count=abstain_count,
        coverage_loss_count=coverage_loss_count,
        q2_observed_rows=q2_observed_rows,
        q2_unavailable_rows=q2_unavailable_rows,
        q2_matrix_rows=q2_rows or [],
        rejection_reasons=rejection_reasons or [],
        unavailable_reason=unavailable_reason,
        source_config_hash=source_config_hash,
        input_artifact_hashes=input_hashes,
        event_rows=event_rows or [],
        expected_return_panel=expected_return_panel or [],
        projection_manifest=projection_manifest or {},
        projection_diagnostics=projection_diagnostics or [],
        abstain_report=abstain_report or [],
        q2_input_contract_v2=q2_input_contract_v2 or {},
        optimizer_input_snapshot=optimizer_input_snapshot,
    )


def _q2_layer(layer_name: str, rows: list[SueTypedQ2SurvivalRow]) -> dict[str, Any]:
    if not rows:
        return {"layer": layer_name, "status": "unavailable", "details": "No observed local Q2 rows."}
    return {"layer": layer_name, "status": "observed", "details": f"observed_rows={len(rows)}"}


def _snapshot_frame(run: SueExpandedTypedQ2SurvivalRun) -> pd.DataFrame:
    if isinstance(run.optimizer_input_snapshot, pd.DataFrame):
        return run.optimizer_input_snapshot.copy()
    return pd.DataFrame(columns=OPTIMIZER_INPUT_SNAPSHOT_COLUMNS)


def _ensure_run(result: SueExpandedTypedQ2SurvivalRun | SueExpandedTypedQ2SurvivalResult) -> SueExpandedTypedQ2SurvivalRun:
    if isinstance(result, SueExpandedTypedQ2SurvivalRun):
        return result
    return SueExpandedTypedQ2SurvivalRun(**result.model_dump())


def _validate_required_artifacts(survival_input: SueExpandedTypedQ2SurvivalInput) -> list[str]:
    paths = {
        "adapter_config": survival_input.adapter_config_path,
        "fixture_config": survival_input.fixture_config_path,
        "local_backtest_manifest": survival_input.local_backtest_manifest_path,
    }
    return [f"{name} artifact is missing at {path}" for name, path in sorted(paths.items()) if not path.exists()]


def _input_artifact_hashes(survival_input: SueExpandedTypedQ2SurvivalInput) -> dict[str, str]:
    paths = {
        "adapter_config": survival_input.adapter_config_path,
        "fixture_config": survival_input.fixture_config_path,
        "local_backtest_manifest": survival_input.local_backtest_manifest_path,
    }
    return {name: sha256_file(path) if path.exists() else "missing" for name, path in sorted(paths.items())}


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def _write_trace(path: Path, run: SueExpandedTypedQ2SurvivalRun) -> None:
    events = [
        TraceEvent.create(event="sue_expanded_typed_q2_started", payload={"run_id": run.run_id}),
        TraceEvent.create(
            event="sue_expanded_typed_q2_completed",
            payload={
                "run_id": run.run_id,
                "survival_status": run.survival_status,
                "event_count": run.event_count,
                "q2_observed_rows": run.q2_observed_rows,
                "q2_unavailable_rows": run.q2_unavailable_rows,
            },
        ),
    ]
    path.write_text("".join(event.model_dump_json() + "\n" for event in events), encoding="utf-8")


def _escape_table(value: str) -> str:
    return str(value).replace("|", "\\|")
