from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import json
import statistics
from typing import Any, Callable

import yaml

from portfolio_os.explain.handoff import load_optional_json, render_execution_handoff_checklist
from portfolio_os.execution.reporting import (
    build_execution_child_orders_frame,
    build_execution_fills_frame,
    build_execution_report_payload,
    render_execution_report_markdown,
)
from portfolio_os.execution.simulator import run_execution_simulation
from portfolio_os.storage.runs import prepare_execution_artifacts
from portfolio_os.storage.snapshots import write_json, write_text


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "execution_mode_ab"
DAILY_CSV_NAME = "execution_mode_ab_daily.csv"
DAILY_JSON_NAME = "execution_mode_ab_daily.json"
SUMMARY_MD_NAME = "execution_mode_ab_summary.md"
MANIFEST_JSON_NAME = "experiment_manifest.json"
DELTA_TOLERANCE = 1e-9


@dataclass(frozen=True)
class ExecutionABRequestItem:
    sample_id: str
    request_path: Path
    start_date: str | None = None
    end_date: str | None = None
    notes: str = ""


@dataclass(frozen=True)
class ExecutionABConfig:
    manifest_path: Path
    start_date: str
    end_date: str
    market: str
    output_dir: Path
    baseline_mode: str = "participation_twap"
    candidate_mode: str = "impact_aware"
    calibration_profile_path: Path | None = None
    require_eligible: bool = False


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_iso_date(raw: str) -> date:
    return datetime.strptime(str(raw), "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> list[date]:
    current = start
    values: list[date] = []
    while current <= end:
        values.append(current)
        current += timedelta(days=1)
    return values


def generate_trading_days(
    start_date: str,
    end_date: str,
    *,
    market: str = "us",
) -> tuple[list[str], str, list[str]]:
    start = _parse_iso_date(start_date)
    end = _parse_iso_date(end_date)
    if end < start:
        raise ValueError("--end-date must be on or after --start-date.")
    warnings: list[str] = []
    if str(market).strip().lower() != "us":
        warnings.append(f"market={market} not explicitly supported; using weekday-only fallback calendar.")
    values = [item.isoformat() for item in _date_range(start, end) if item.weekday() < 5]
    return values, "weekday_fallback", warnings


def load_execution_ab_manifest(path: Path) -> list[ExecutionABRequestItem]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected YAML mapping in {path}.")
    raw_items = payload.get("requests")
    if not isinstance(raw_items, list) or not raw_items:
        raise ValueError("execution A/B manifest must define a non-empty 'requests' list.")
    items: list[ExecutionABRequestItem] = []
    for index, raw_item in enumerate(raw_items, start=1):
        if not isinstance(raw_item, dict):
            raise ValueError(f"Manifest entry #{index} must be a mapping.")
        sample_id = str(raw_item.get("sample_id", "")).strip()
        request_text = str(raw_item.get("request_path") or raw_item.get("request") or "").strip()
        if not sample_id:
            raise ValueError(f"Manifest entry #{index} is missing sample_id.")
        if not request_text:
            raise ValueError(f"Manifest entry #{index} is missing request_path.")
        request_path = Path(request_text)
        if not request_path.is_absolute():
            candidates = [
                (path.parent / request_path).resolve(),
                (ROOT / request_path).resolve(),
            ]
            request_path = next((candidate for candidate in candidates if candidate.exists()), candidates[-1])
        items.append(
            ExecutionABRequestItem(
                sample_id=sample_id,
                request_path=request_path,
                start_date=(str(raw_item.get("start_date")).strip() if raw_item.get("start_date") else None),
                end_date=(str(raw_item.get("end_date")).strip() if raw_item.get("end_date") else None),
                notes=str(raw_item.get("notes", "") or "").strip(),
            )
        )
    return items


def _request_applies_on_date(item: ExecutionABRequestItem, trading_day: str) -> bool:
    current = _parse_iso_date(trading_day)
    if item.start_date is not None and current < _parse_iso_date(item.start_date):
        return False
    if item.end_date is not None and current > _parse_iso_date(item.end_date):
        return False
    return True


def _ab_request_path(
    *,
    original_request_path: Path,
    output_dir: Path,
    mode: str,
) -> Path:
    payload = yaml.safe_load(original_request_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected YAML mapping in {original_request_path}.")
    simulation = payload.get("simulation")
    if not isinstance(simulation, dict):
        simulation = {}
        payload["simulation"] = simulation
    simulation["mode"] = str(mode)

    original_dir = original_request_path.parent.resolve()

    def _resolve_if_present(key: str) -> None:
        value = payload.get(key)
        if value is None:
            return
        text = str(value).strip()
        if not text:
            return
        resolved = Path(text)
        if not resolved.is_absolute():
            candidates = [
                (original_dir / resolved).resolve(),
                (ROOT / resolved).resolve(),
            ]
            resolved = next((candidate for candidate in candidates if candidate.exists()), candidates[-1])
        payload[key] = str(resolved)

    _resolve_if_present("artifact_dir")
    _resolve_if_present("market")
    _resolve_if_present("portfolio_state")
    _resolve_if_present("execution_profile")
    _resolve_if_present("calibration_profile")

    target_path = output_dir / f"{original_request_path.stem}_{mode}.yaml"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding="utf-8")
    return target_path


def _execute_mode_request(
    *,
    request_path: Path,
    mode: str,
    output_dir: Path,
    calibration_profile_path: Path | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        effective_request_path = _ab_request_path(
            original_request_path=request_path,
            output_dir=output_dir,
            mode=mode,
        )
        artifacts = prepare_execution_artifacts(output_dir)
        simulation_result = run_execution_simulation(
            effective_request_path,
            run_id=artifacts.run_id,
            created_at=artifacts.created_at,
            calibration_profile_path=calibration_profile_path,
        )
        report_payload = build_execution_report_payload(simulation_result)
        report_markdown = render_execution_report_markdown(simulation_result)
        fills_frame = build_execution_fills_frame(simulation_result)
        child_orders_frame = build_execution_child_orders_frame(simulation_result)
        handoff_checklist = render_execution_handoff_checklist(
            simulation_result,
            approval_record=load_optional_json(simulation_result.request_metadata.get("approval_record")),
            freeze_manifest=load_optional_json(
                Path(simulation_result.request_metadata["artifact_dir"]) / "freeze_manifest.json"
            ),
            audit_payload=load_optional_json(simulation_result.request_metadata.get("audit")),
        )
        write_json(artifacts.execution_report_json_path, report_payload)
        write_text(artifacts.execution_report_markdown_path, report_markdown)
        fills_frame.to_csv(artifacts.execution_fills_path, index=False)
        child_orders_frame.to_csv(artifacts.execution_child_orders_path, index=False)
        write_text(artifacts.handoff_checklist_path, handoff_checklist)
        write_json(
            artifacts.manifest_path,
            {
                "run_id": artifacts.run_id,
                "created_at": artifacts.created_at,
                "mode": mode,
                "request": str(effective_request_path),
                "calibration_profile": str(calibration_profile_path) if calibration_profile_path is not None else None,
                "execution_report_json_path": artifacts.execution_report_json_path,
                "execution_report_markdown_path": artifacts.execution_report_markdown_path,
                "execution_fills_path": artifacts.execution_fills_path,
                "execution_child_orders_path": artifacts.execution_child_orders_path,
                "handoff_checklist_path": artifacts.handoff_checklist_path,
            },
        )
        return {
            "status": "success",
            "mode": mode,
            "request_path": str(effective_request_path),
            "output_dir": str(output_dir),
            "artifacts": {
                "execution_report_json_path": artifacts.execution_report_json_path,
                "execution_report_markdown_path": artifacts.execution_report_markdown_path,
                "execution_fills_path": artifacts.execution_fills_path,
                "execution_child_orders_path": artifacts.execution_child_orders_path,
                "handoff_checklist_path": artifacts.handoff_checklist_path,
                "manifest_path": artifacts.manifest_path,
            },
            "report": report_payload,
        }
    except Exception as exc:
        error_payload = {
            "status": "failure",
            "mode": mode,
            "request_path": str(request_path),
            "output_dir": str(output_dir),
            "error": str(exc),
            "error_type": type(exc).__name__,
        }
        write_json(output_dir / "execution_error.json", error_payload)
        return error_payload


def _bucket_curve_signature(report_payload: dict[str, Any]) -> str:
    return json.dumps(report_payload.get("bucket_curve", {}), ensure_ascii=False, sort_keys=True)


def _resolved_defaults_signature(report_payload: dict[str, Any]) -> str:
    defaults = ((report_payload.get("resolved_calibration") or {}).get("resolved_simulation_defaults") or {})
    subset = {
        "participation_limit": defaults.get("participation_limit"),
        "volume_shock_multiplier": defaults.get("volume_shock_multiplier"),
    }
    return json.dumps(subset, ensure_ascii=False, sort_keys=True)


def _has_cost_fields(report_payload: dict[str, Any]) -> bool:
    comparison = report_payload.get("cost_comparison") or {}
    return "planned_cost" in comparison and "evaluated_cost" in comparison


def _build_daily_row(
    *,
    trading_day: str,
    item: ExecutionABRequestItem,
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    baseline_mode: str,
    candidate_mode: str,
) -> dict[str, Any]:
    row = {
        "date": trading_day,
        "sample_id": item.sample_id,
        "eligible": False,
        "eligibility_reason": "",
        "basket_notional": None,
        "mode_a": baseline_mode,
        "mode_b": candidate_mode,
        "planned_cost_a": None,
        "evaluated_cost_a": None,
        "planned_cost_b": None,
        "evaluated_cost_b": None,
        "fill_rate_a": None,
        "fill_rate_b": None,
        "partial_fill_count_a": None,
        "partial_fill_count_b": None,
        "unfilled_order_count_a": None,
        "unfilled_order_count_b": None,
        "winner": "ineligible",
        "notes": item.notes,
        "delta_evaluated_cost": None,
        "baseline_output_dir": baseline.get("output_dir"),
        "candidate_output_dir": candidate.get("output_dir"),
    }
    if baseline.get("status") != "success":
        row["eligibility_reason"] = f"baseline_failed:{baseline.get('error_type', 'error')}"
        return row
    if candidate.get("status") != "success":
        row["eligibility_reason"] = f"candidate_failed:{candidate.get('error_type', 'error')}"
        return row

    baseline_report = baseline["report"]
    candidate_report = candidate["report"]
    if not _has_cost_fields(baseline_report) or not _has_cost_fields(candidate_report):
        row["eligibility_reason"] = "missing_cost_fields"
        return row
    if _bucket_curve_signature(baseline_report) != _bucket_curve_signature(candidate_report):
        row["eligibility_reason"] = "bucket_curve_mismatch"
        return row
    if _resolved_defaults_signature(baseline_report) != _resolved_defaults_signature(candidate_report):
        row["eligibility_reason"] = "resolved_defaults_mismatch"
        return row

    baseline_summary = baseline_report["portfolio_summary"]
    candidate_summary = candidate_report["portfolio_summary"]
    baseline_costs = baseline_report["cost_comparison"]
    candidate_costs = candidate_report["cost_comparison"]

    row.update(
        {
            "eligible": True,
            "eligibility_reason": "",
            "basket_notional": baseline_summary.get("total_ordered_notional"),
            "planned_cost_a": baseline_costs.get("planned_cost"),
            "evaluated_cost_a": baseline_costs.get("evaluated_cost"),
            "planned_cost_b": candidate_costs.get("planned_cost"),
            "evaluated_cost_b": candidate_costs.get("evaluated_cost"),
            "fill_rate_a": baseline_summary.get("fill_rate"),
            "fill_rate_b": candidate_summary.get("fill_rate"),
            "partial_fill_count_a": baseline_summary.get("partial_fill_count"),
            "partial_fill_count_b": candidate_summary.get("partial_fill_count"),
            "unfilled_order_count_a": baseline_summary.get("unfilled_order_count"),
            "unfilled_order_count_b": candidate_summary.get("unfilled_order_count"),
        }
    )
    cost_a = float(row["evaluated_cost_a"] or 0.0)
    cost_b = float(row["evaluated_cost_b"] or 0.0)
    delta = cost_a - cost_b
    row["delta_evaluated_cost"] = delta
    if abs(delta) <= DELTA_TOLERANCE:
        row["winner"] = "tie"
    elif delta > 0:
        row["winner"] = candidate_mode
    else:
        row["winner"] = baseline_mode
    return row


def _render_summary(
    *,
    config: ExecutionABConfig,
    trading_days: list[str],
    daily_rows: list[dict[str, Any]],
) -> str:
    scheduled_dates = sorted({str(row["date"]) for row in daily_rows})
    rows_by_date: dict[str, list[dict[str, Any]]] = {}
    for row in daily_rows:
        rows_by_date.setdefault(str(row["date"]), []).append(row)
    eligible_rows = [row for row in daily_rows if bool(row.get("eligible"))]
    ineligible_rows = [row for row in daily_rows if not bool(row.get("eligible"))]

    fully_eligible_days = 0
    partially_eligible_days = 0
    fully_ineligible_days = 0
    for trading_day, rows in rows_by_date.items():
        eligible_count = sum(1 for row in rows if bool(row.get("eligible")))
        if eligible_count == len(rows):
            fully_eligible_days += 1
        elif eligible_count == 0:
            fully_ineligible_days += 1
        else:
            partially_eligible_days += 1

    deltas = [float(row["delta_evaluated_cost"]) for row in eligible_rows if row.get("delta_evaluated_cost") is not None]
    candidate_wins = sum(1 for row in eligible_rows if row.get("winner") == config.candidate_mode)
    baseline_wins = sum(1 for row in eligible_rows if row.get("winner") == config.baseline_mode)
    ties = sum(1 for row in eligible_rows if row.get("winner") == "tie")
    worse_fill_rate = sum(
        1 for row in eligible_rows
        if float(row.get("fill_rate_b") or 0.0) < float(row.get("fill_rate_a") or 0.0) - 1e-12
    )
    higher_unfilled = sum(
        1 for row in eligible_rows
        if int(row.get("unfilled_order_count_b") or 0) > int(row.get("unfilled_order_count_a") or 0)
    )
    higher_partial = sum(
        1 for row in eligible_rows
        if int(row.get("partial_fill_count_b") or 0) > int(row.get("partial_fill_count_a") or 0)
    )

    lines = [
        "# Execution Mode A/B Summary",
        "",
        "## Scope",
        f"- start_date: {config.start_date}",
        f"- end_date: {config.end_date}",
        f"- market: {config.market}",
        f"- baseline_mode: {config.baseline_mode}",
        f"- candidate_mode: {config.candidate_mode}",
        f"- request_manifest: {config.manifest_path}",
        f"- scheduled_trading_days: {len(trading_days)}",
        f"- active_days_with_requests: {len(scheduled_dates)}",
        "",
        "## Eligibility",
        f"- eligible_comparison_count: {len(eligible_rows)}",
        f"- ineligible_comparison_count: {len(ineligible_rows)}",
        f"- fully_eligible_day_count: {fully_eligible_days}",
        f"- partially_eligible_day_count: {partially_eligible_days}",
        f"- fully_ineligible_day_count: {fully_ineligible_days}",
        "",
        "## Winner Counts",
        f"- {config.baseline_mode}: {baseline_wins}",
        f"- {config.candidate_mode}: {candidate_wins}",
        f"- tie: {ties}",
        "",
        "## Cost Delta",
    ]
    if deltas:
        lines.extend(
            [
                f"- mean_delta_evaluated_cost: {statistics.fmean(deltas):.6f}",
                f"- median_delta_evaluated_cost: {statistics.median(deltas):.6f}",
                f"- min_delta_evaluated_cost: {min(deltas):.6f}",
                f"- max_delta_evaluated_cost: {max(deltas):.6f}",
            ]
        )
    else:
        lines.append("- no eligible comparisons")
    lines.extend(
        [
            "",
            "## Fill-Quality Guard",
            f"- worse_fill_rate_count: {worse_fill_rate}",
            f"- higher_unfilled_count: {higher_unfilled}",
            f"- higher_partial_fill_count: {higher_partial}",
            "",
            "## Ineligible Rows",
        ]
    )
    if not ineligible_rows:
        lines.append("- none")
    else:
        for row in ineligible_rows[:20]:
            lines.append(
                f"- {row['date']} {row['sample_id']}: {row.get('eligibility_reason', '')}"
            )
    return "\n".join(lines) + "\n"


def _write_daily_artifacts(output_dir: Path, daily_rows: list[dict[str, Any]]) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_csv_path = output_dir / DAILY_CSV_NAME
    daily_json_path = output_dir / DAILY_JSON_NAME
    fieldnames = [
        "date",
        "sample_id",
        "eligible",
        "eligibility_reason",
        "basket_notional",
        "mode_a",
        "mode_b",
        "planned_cost_a",
        "evaluated_cost_a",
        "planned_cost_b",
        "evaluated_cost_b",
        "fill_rate_a",
        "fill_rate_b",
        "partial_fill_count_a",
        "partial_fill_count_b",
        "unfilled_order_count_a",
        "unfilled_order_count_b",
        "winner",
        "delta_evaluated_cost",
        "notes",
        "baseline_output_dir",
        "candidate_output_dir",
    ]
    with daily_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in daily_rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    daily_json_path.write_text(json.dumps({"rows": daily_rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "daily_csv": daily_csv_path,
        "daily_json": daily_json_path,
    }


def run_execution_mode_ab(
    config: ExecutionABConfig,
    *,
    execute_mode: Callable[..., dict[str, Any]] = _execute_mode_request,
    trading_day_fn: Callable[..., tuple[list[str], str, list[str]]] = generate_trading_days,
) -> dict[str, Path]:
    manifest_items = load_execution_ab_manifest(config.manifest_path)
    trading_days, calendar_source, calendar_warnings = trading_day_fn(
        config.start_date,
        config.end_date,
        market=config.market,
    )

    daily_rows: list[dict[str, Any]] = []
    daily_root = config.output_dir / "daily"
    for trading_day in trading_days:
        active_items = [item for item in manifest_items if _request_applies_on_date(item, trading_day)]
        for item in active_items:
            sample_root = daily_root / trading_day / item.sample_id
            baseline = execute_mode(
                request_path=item.request_path,
                mode=config.baseline_mode,
                output_dir=sample_root / config.baseline_mode,
                calibration_profile_path=config.calibration_profile_path,
            )
            candidate = execute_mode(
                request_path=item.request_path,
                mode=config.candidate_mode,
                output_dir=sample_root / config.candidate_mode,
                calibration_profile_path=config.calibration_profile_path,
            )
            row = _build_daily_row(
                trading_day=trading_day,
                item=item,
                baseline=baseline,
                candidate=candidate,
                baseline_mode=config.baseline_mode,
                candidate_mode=config.candidate_mode,
            )
            daily_rows.append(row)

    artifact_paths = _write_daily_artifacts(config.output_dir, daily_rows)
    summary_path = config.output_dir / SUMMARY_MD_NAME
    summary_text = _render_summary(
        config=config,
        trading_days=trading_days,
        daily_rows=daily_rows,
    )
    write_text(summary_path, summary_text)
    manifest_path = config.output_dir / MANIFEST_JSON_NAME
    write_json(
        manifest_path,
        {
            "generated_at": _now_iso(),
            "manifest_path": str(config.manifest_path),
            "start_date": config.start_date,
            "end_date": config.end_date,
            "market": config.market,
            "calendar_source": calendar_source,
            "calendar_warnings": calendar_warnings,
            "baseline_mode": config.baseline_mode,
            "candidate_mode": config.candidate_mode,
            "calibration_profile": str(config.calibration_profile_path) if config.calibration_profile_path is not None else None,
            "daily_csv": str(artifact_paths["daily_csv"]),
            "daily_json": str(artifact_paths["daily_json"]),
            "summary_md": str(summary_path),
            "require_eligible": bool(config.require_eligible),
        },
    )
    if config.require_eligible and any(not bool(row.get("eligible")) for row in daily_rows):
        raise RuntimeError("Execution mode A/B eligibility gate failed (--require-eligible).")
    return {
        "daily_csv": artifact_paths["daily_csv"],
        "daily_json": artifact_paths["daily_json"],
        "summary_md": summary_path,
        "manifest_json": manifest_path,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a rolling A/B comparison for execution simulation modes.")
    parser.add_argument("--manifest", type=Path, required=True, help="Execution A/B manifest YAML.")
    parser.add_argument("--start-date", required=True, help="Window start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Window end date in YYYY-MM-DD.")
    parser.add_argument("--market", default="us", help="Trading calendar market label.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Comparison output directory.")
    parser.add_argument("--baseline-mode", default="participation_twap", choices=["participation_twap", "impact_aware"])
    parser.add_argument("--candidate-mode", default="impact_aware", choices=["participation_twap", "impact_aware"])
    parser.add_argument("--calibration-profile", type=Path, default=None, help="Optional calibration profile override for both modes.")
    parser.add_argument("--require-eligible", action="store_true", help="Exit non-zero if any daily comparison is ineligible.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = ExecutionABConfig(
        manifest_path=Path(args.manifest).resolve(),
        start_date=str(args.start_date),
        end_date=str(args.end_date),
        market=str(args.market),
        output_dir=Path(args.output_dir).resolve(),
        baseline_mode=str(args.baseline_mode),
        candidate_mode=str(args.candidate_mode),
        calibration_profile_path=(Path(args.calibration_profile).resolve() if args.calibration_profile is not None else None),
        require_eligible=bool(args.require_eligible),
    )
    artifact_paths = run_execution_mode_ab(config)
    print(f"execution_mode_ab_daily_csv: {artifact_paths['daily_csv']}")
    print(f"execution_mode_ab_daily_json: {artifact_paths['daily_json']}")
    print(f"execution_mode_ab_summary_md: {artifact_paths['summary_md']}")
    print(f"execution_mode_ab_manifest_json: {artifact_paths['manifest_json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
