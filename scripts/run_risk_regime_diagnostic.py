from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[1]
RISK_COMPONENTS: tuple[str, ...] = ("risk_term", "tracking_error", "transaction_cost")
LEGACY_COMPONENTS: tuple[str, ...] = ("target_deviation", "transaction_fee", "turnover_penalty", "slippage_penalty")
ALL_COMPONENTS: tuple[str, ...] = RISK_COMPONENTS + LEGACY_COMPONENTS


@dataclass
class DiagnosticConfig:
    baseline_dashboard: Path
    date: str
    phase: str
    market: str
    risk_inputs_dir: Path
    risk_term_values: list[float]
    tracking_error_weight: float
    output_dir: Path
    real_sample: bool
    skip_replay_eligibility_gate: bool
    include_risk_term_zero_anchor: bool
    risk_integration_mode: str = "replace"


@dataclass
class CellSpec:
    risk_model_enabled: bool
    risk_term_weight: float
    is_anchor: bool = False

    @property
    def cell_id(self) -> str:
        enabled_label = "on" if self.risk_model_enabled else "off"
        weight_label = str(self.risk_term_weight).replace(".", "_")
        suffix = "_anchor" if self.is_anchor else ""
        return f"enabled_{enabled_label}_risk_term_{weight_label}{suffix}"


def _now_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _log(message: str) -> None:
    print(f"[{_now_timestamp()}] {message}")


def _parse_iso_date(text: str) -> pd.Timestamp:
    return pd.to_datetime(str(text), format="%Y-%m-%d", errors="raise")


def _parse_float_list(raw: str) -> list[float]:
    values: list[float] = []
    for token in str(raw).split(","):
        token = token.strip()
        if not token:
            continue
        values.append(float(token))
    if not values:
        raise ValueError("--risk-term-values cannot be empty.")
    return values


def _run_subprocess(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if pd.isna(parsed):
        return float(default)
    return float(parsed)


def _validate_baseline_dashboard(baseline_dashboard: Path, *, date: str) -> dict[str, str]:
    if not baseline_dashboard.exists():
        raise FileNotFoundError(f"Baseline dashboard not found: {baseline_dashboard}")
    frame = pd.read_csv(baseline_dashboard, encoding="utf-8-sig")
    required_columns = {"date", "run_root"}
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        raise ValueError(
            "Baseline dashboard missing required columns: " + ", ".join(missing)
        )
    rows = frame[frame["date"].astype(str) == str(date)]
    if rows.empty:
        raise ValueError(f"Baseline dashboard has no row for date={date}.")
    row = rows.iloc[-1].to_dict()
    return {str(key): str(value) for key, value in row.items()}


def _build_cell_specs(config: DiagnosticConfig) -> list[CellSpec]:
    unique_values: list[float] = []
    for value in config.risk_term_values:
        if float(value) not in unique_values:
            unique_values.append(float(value))
    cells: list[CellSpec] = []
    for enabled in (False, True):
        for value in unique_values:
            cells.append(CellSpec(risk_model_enabled=enabled, risk_term_weight=float(value)))
    if config.include_risk_term_zero_anchor:
        has_anchor = any(cell.risk_model_enabled and abs(cell.risk_term_weight) <= 1e-12 for cell in cells)
        if not has_anchor:
            cells.append(CellSpec(risk_model_enabled=True, risk_term_weight=0.0, is_anchor=True))
    return cells


def _build_overlay_payload(*, config: DiagnosticConfig, cell: CellSpec) -> dict[str, Any]:
    returns_path = (config.risk_inputs_dir / "returns_long.csv").resolve()
    factor_path = (config.risk_inputs_dir / "factor_exposure.csv").resolve()
    if not returns_path.exists():
        raise FileNotFoundError(f"Missing risk inputs file: {returns_path}")

    payload: dict[str, Any] = {
        "risk_model": {
            "enabled": bool(cell.risk_model_enabled),
            "integration_mode": str(config.risk_integration_mode).strip().lower(),
            "estimator": "ledoit_wolf",
            "returns_path": str(returns_path),
        },
        "objective_weights": {
            "risk_term": float(cell.risk_term_weight),
            "tracking_error": float(config.tracking_error_weight),
        },
    }
    if factor_path.exists():
        payload["risk_model"]["factor_exposure_path"] = str(factor_path)
    return payload


def _write_overlay_file(*, output_dir: Path, cell: CellSpec, config: DiagnosticConfig) -> Path:
    target_dir = output_dir / f"config_{cell.cell_id}"
    target_dir.mkdir(parents=True, exist_ok=True)
    overlay_path = target_dir / "overlay.yaml"
    overlay_payload = _build_overlay_payload(config=config, cell=cell)
    overlay_path.write_text(
        yaml.safe_dump(overlay_payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return overlay_path


def _load_progress_status(tracking_dir: Path) -> str:
    progress_path = tracking_dir / "replay_progress.json"
    if not progress_path.exists():
        return ""
    payload = json.loads(progress_path.read_text(encoding="utf-8"))
    return str(payload.get("status", "")).strip().lower()


def _load_tracking_row(*, dashboard_path: Path, date: str) -> dict[str, str]:
    if not dashboard_path.exists():
        raise FileNotFoundError(f"Tracking dashboard not found: {dashboard_path}")
    frame = pd.read_csv(dashboard_path, encoding="utf-8-sig")
    required_columns = {"date", "run_root"}
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        raise ValueError(
            f"Tracking dashboard missing required columns ({dashboard_path}): " + ", ".join(missing)
        )
    rows = frame[frame["date"].astype(str) == str(date)]
    if rows.empty:
        raise ValueError(f"Tracking dashboard has no row for date={date}: {dashboard_path}")
    row = rows.iloc[-1].to_dict()
    return {str(key): str(value) for key, value in row.items()}


def _collect_audit_paths(run_root: Path) -> list[Path]:
    samples_root = run_root / "samples"
    if not samples_root.exists():
        raise FileNotFoundError(f"Run root has no samples directory: {samples_root}")
    audits = sorted(samples_root.glob("*/main/audit.json"))
    if not audits:
        raise FileNotFoundError(f"No sample audit files found under: {samples_root}")
    return audits


def _parse_audit_payload(path: Path, *, require_decomposition: bool) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise ValueError(f"audit.summary missing or invalid: {path}")
    for key in ("gross_traded_notional", "estimated_total_cost"):
        if key not in summary:
            raise ValueError(f"audit.summary missing field '{key}': {path}")
    orders = payload.get("orders")
    if not isinstance(orders, list):
        raise ValueError(f"audit.orders missing or invalid: {path}")
    optimization_metadata = payload.get("optimization_metadata")
    if optimization_metadata is None:
        optimization_metadata = {}
    if not isinstance(optimization_metadata, dict):
        raise ValueError(f"audit.optimization_metadata invalid: {path}")
    decomposition = optimization_metadata.get("objective_decomposition")
    if require_decomposition and not isinstance(decomposition, dict):
        raise ValueError(f"audit.optimization_metadata.objective_decomposition missing: {path}")
    return {
        "orders": orders,
        "summary": summary,
        "decomposition": decomposition if isinstance(decomposition, dict) else {},
    }


def _aggregate_decomposition(samples: list[dict[str, Any]]) -> tuple[str, dict[str, dict[str, float]], dict[str, Any]]:
    mode_values: list[str] = []
    buckets: dict[str, dict[str, list[float]]] = {}
    representative: dict[str, Any] = {}

    for sample in samples:
        decomposition = sample.get("decomposition")
        sample_id = str(sample.get("sample_id", ""))
        if not isinstance(decomposition, dict):
            continue
        mode = str(decomposition.get("mode", "")).strip().lower()
        if mode:
            mode_values.append(mode)
        components = decomposition.get("components", {})
        if not isinstance(components, dict):
            raise ValueError(f"objective_decomposition.components invalid for sample={sample_id}")
        if not representative:
            representative = {"sample_id": sample_id, "decomposition": decomposition}
        for name, comp_payload in components.items():
            if not isinstance(comp_payload, dict):
                raise ValueError(f"objective_decomposition component invalid: sample={sample_id}, component={name}")
            entry = buckets.setdefault(
                str(name),
                {
                    "raw_value": [],
                    "weight": [],
                    "weighted_value": [],
                    "share_abs_weighted": [],
                },
            )
            entry["raw_value"].append(_safe_float(comp_payload.get("raw_value")))
            entry["weight"].append(_safe_float(comp_payload.get("weight")))
            entry["weighted_value"].append(_safe_float(comp_payload.get("weighted_value")))
            entry["share_abs_weighted"].append(_safe_float(comp_payload.get("share_abs_weighted")))

    resolved_mode = mode_values[0] if mode_values else ""
    if mode_values and any(mode != resolved_mode for mode in mode_values):
        raise ValueError("Mixed objective decomposition modes detected in one cell.")

    component_means: dict[str, dict[str, float]] = {}
    for name, values in buckets.items():
        component_means[name] = {
            "raw_value_mean": float(sum(values["raw_value"]) / len(values["raw_value"])) if values["raw_value"] else 0.0,
            "weight_mean": float(sum(values["weight"]) / len(values["weight"])) if values["weight"] else 0.0,
            "weighted_value_mean": (
                float(sum(values["weighted_value"]) / len(values["weighted_value"]))
                if values["weighted_value"]
                else 0.0
            ),
            "share_abs_weighted_mean": (
                float(sum(values["share_abs_weighted"]) / len(values["share_abs_weighted"]))
                if values["share_abs_weighted"]
                else 0.0
            ),
        }

    return resolved_mode, component_means, representative


def _aggregate_cell_metrics(*, run_root: Path, require_decomposition: bool) -> dict[str, Any]:
    audits = _collect_audit_paths(run_root)
    sample_metrics: list[dict[str, Any]] = []
    order_count_total = 0
    non_zero_order_run_count = 0
    gross_traded_notional_total = 0.0
    estimated_total_cost_total = 0.0

    for audit_path in audits:
        parsed = _parse_audit_payload(audit_path, require_decomposition=require_decomposition)
        sample_id = audit_path.parents[2].name
        orders = parsed["orders"]
        summary = parsed["summary"]
        order_count = int(len(orders))
        order_count_total += order_count
        if order_count > 0:
            non_zero_order_run_count += 1
        gross_traded_notional_total += _safe_float(summary.get("gross_traded_notional"))
        estimated_total_cost_total += _safe_float(summary.get("estimated_total_cost"))
        sample_metrics.append(
            {
                "sample_id": sample_id,
                "order_count": order_count,
                "gross_traded_notional": _safe_float(summary.get("gross_traded_notional")),
                "estimated_total_cost": _safe_float(summary.get("estimated_total_cost")),
                "decomposition": parsed.get("decomposition", {}),
            }
        )

    mode, component_means, representative = _aggregate_decomposition(sample_metrics)
    abs_weighted_sum_mean = 0.0
    weighted_abs_pairs: list[tuple[str, float]] = []
    for component_name, values in component_means.items():
        weighted_abs = abs(_safe_float(values.get("weighted_value_mean")))
        abs_weighted_sum_mean += weighted_abs
        weighted_abs_pairs.append((component_name, weighted_abs))
    weighted_abs_pairs.sort(key=lambda item: item[1], reverse=True)

    dominant_component = weighted_abs_pairs[0][0] if weighted_abs_pairs else ""
    risk_term_abs_weighted = abs(
        _safe_float(component_means.get("risk_term", {}).get("weighted_value_mean"))
    )
    other_abs_weighted = [
        abs(_safe_float(values.get("weighted_value_mean")))
        for component_name, values in component_means.items()
        if component_name != "risk_term"
    ]
    second_largest_other_abs_weighted = max(other_abs_weighted) if other_abs_weighted else 0.0
    if second_largest_other_abs_weighted <= 0.0:
        risk_term_to_second_ratio = float("inf") if risk_term_abs_weighted > 0.0 else 0.0
    else:
        risk_term_to_second_ratio = risk_term_abs_weighted / second_largest_other_abs_weighted

    dominant_share = 0.0
    if dominant_component:
        dominant_share = _safe_float(
            component_means.get(dominant_component, {}).get("share_abs_weighted_mean"),
        )
    risk_term_share = _safe_float(component_means.get("risk_term", {}).get("share_abs_weighted_mean"))

    return {
        "sample_count": len(sample_metrics),
        "order_count_total": int(order_count_total),
        "non_zero_order_run_count": int(non_zero_order_run_count),
        "gross_traded_notional_total": float(gross_traded_notional_total),
        "estimated_total_cost_total": float(estimated_total_cost_total),
        "objective_mode": mode,
        "component_means": component_means,
        "abs_weighted_sum_mean": float(abs_weighted_sum_mean),
        "dominant_component": dominant_component,
        "dominant_share_abs_weighted": float(dominant_share),
        "risk_term_share_abs_weighted_mean": float(risk_term_share),
        "risk_term_to_second_abs_weighted_ratio": float(risk_term_to_second_ratio),
        "representative_sample": representative,
    }


def _run_cell(
    *,
    config: DiagnosticConfig,
    cell: CellSpec,
    runner: Callable[..., subprocess.CompletedProcess[str]],
    logger: Callable[[str], None],
) -> dict[str, Any]:
    overlay_path = _write_overlay_file(output_dir=config.output_dir, cell=cell, config=config)
    tracking_dir = config.output_dir / f"tracking_{cell.cell_id}"
    tracking_dir.mkdir(parents=True, exist_ok=True)

    init_command = [
        sys.executable,
        str(ROOT / "scripts" / "pilot_ops.py"),
        "init",
        "--output-dir",
        str(tracking_dir),
    ]
    init_result = runner(init_command, cwd=ROOT)
    if int(init_result.returncode) != 0:
        raise RuntimeError(
            f"init failed for {cell.cell_id} (exit={init_result.returncode}): {str(init_result.stderr or '').strip()}"
        )

    replay_command = [
        sys.executable,
        str(ROOT / "scripts" / "pilot_historical_replay.py"),
        "--start-date",
        config.date,
        "--end-date",
        config.date,
        "--phase",
        config.phase,
        "--market",
        config.market,
        "--cool-down",
        "0",
        "--max-failures",
        "1",
        "--output-dir",
        str(tracking_dir),
        "--config-overlay",
        str(overlay_path),
        "--ab-flow",
    ]
    if not config.skip_replay_eligibility_gate:
        replay_command.append("--require-eligibility-gate")
    if config.real_sample:
        replay_command.append("--real-sample")
    replay_result = runner(replay_command, cwd=ROOT)
    progress_status = _load_progress_status(tracking_dir)
    dashboard_path = tracking_dir / "pilot_dashboard.csv"
    row = _load_tracking_row(dashboard_path=dashboard_path, date=config.date)
    run_root = Path(str(row["run_root"]))
    if not run_root.exists():
        raise FileNotFoundError(f"run_root from tracking dashboard does not exist: {run_root}")

    aggregated = _aggregate_cell_metrics(run_root=run_root, require_decomposition=True)
    logger(
        (
            f"{cell.cell_id}: rc={replay_result.returncode}, progress={progress_status or 'unknown'}, "
            f"orders={aggregated['order_count_total']}, gross={aggregated['gross_traded_notional_total']:.4f}"
        )
    )
    return {
        "cell_id": cell.cell_id,
        "risk_model_enabled": bool(cell.risk_model_enabled),
        "risk_term_weight": float(cell.risk_term_weight),
        "tracking_error_weight": float(config.tracking_error_weight),
        "is_anchor": bool(cell.is_anchor),
        "return_code": int(replay_result.returncode),
        "progress_status": progress_status,
        "tracking_dir": str(tracking_dir),
        "dashboard_path": str(dashboard_path),
        "run_root": str(run_root),
        **aggregated,
    }


def _matrix_row_for_csv(row: dict[str, Any]) -> dict[str, Any]:
    output = {
        "cell_id": row["cell_id"],
        "risk_model_enabled": row["risk_model_enabled"],
        "risk_term_weight": row["risk_term_weight"],
        "tracking_error_weight": row["tracking_error_weight"],
        "is_anchor": row["is_anchor"],
        "return_code": row["return_code"],
        "progress_status": row["progress_status"],
        "order_count_total": row["order_count_total"],
        "non_zero_order_run_count": row["non_zero_order_run_count"],
        "gross_traded_notional_total": row["gross_traded_notional_total"],
        "estimated_total_cost_total": row["estimated_total_cost_total"],
        "objective_mode": row["objective_mode"],
        "abs_weighted_sum_mean": row["abs_weighted_sum_mean"],
        "dominant_component": row["dominant_component"],
        "dominant_share_abs_weighted": row["dominant_share_abs_weighted"],
        "risk_term_share_abs_weighted_mean": row["risk_term_share_abs_weighted_mean"],
        "risk_term_to_second_abs_weighted_ratio": row["risk_term_to_second_abs_weighted_ratio"],
    }
    component_means = row.get("component_means", {})
    for component_name in ALL_COMPONENTS:
        stats = component_means.get(component_name, {})
        output[f"{component_name}_raw_mean"] = stats.get("raw_value_mean")
        output[f"{component_name}_weight_mean"] = stats.get("weight_mean")
        output[f"{component_name}_weighted_mean"] = stats.get("weighted_value_mean")
        output[f"{component_name}_share_abs_weighted_mean"] = stats.get("share_abs_weighted_mean")
    output["component_means_json"] = json.dumps(component_means, ensure_ascii=False, sort_keys=True)
    return output


def _evaluate_rules(rows: list[dict[str, Any]]) -> dict[str, Any]:
    enabled_positive = [
        row
        for row in rows
        if bool(row.get("risk_model_enabled")) and _safe_float(row.get("risk_term_weight")) > 0.0
    ]
    dominant_flags = [
        (
            _safe_float(row.get("risk_term_share_abs_weighted_mean")) >= 0.95
            and _safe_float(row.get("risk_term_to_second_abs_weighted_ratio")) >= 100.0
        )
        for row in enabled_positive
    ]
    rule_scale_suppression = (
        bool(enabled_positive)
        and int(sum(1 for flag in dominant_flags if flag)) >= (len(enabled_positive) // 2 + 1)
    )

    rule_weight_not_effective = False
    share_range = None
    if len(enabled_positive) >= 2:
        shares = [_safe_float(row.get("risk_term_share_abs_weighted_mean")) for row in enabled_positive]
        share_range = max(shares) - min(shares)
        rule_weight_not_effective = share_range <= 0.01

    anchor_row = next((row for row in rows if bool(row.get("is_anchor"))), None)
    rule_regime_coupling = False
    if anchor_row is not None:
        rule_regime_coupling = int(anchor_row.get("order_count_total", 0)) == 0

    return {
        "scale_suppression_likely": rule_scale_suppression,
        "weight_not_effective_suspected": rule_weight_not_effective,
        "regime_coupling_suspected_by_anchor": rule_regime_coupling,
        "enabled_positive_cell_count": len(enabled_positive),
        "enabled_positive_scale_dominant_count": int(sum(1 for flag in dominant_flags if flag)),
        "risk_term_share_range_enabled_positive": share_range,
        "anchor_row_present": anchor_row is not None,
    }


def _render_markdown_report(
    *,
    config: DiagnosticConfig,
    baseline_row: dict[str, str],
    rows: list[dict[str, Any]],
    rule_eval: dict[str, Any],
) -> str:
    lines = [
        "# Risk Regime Diagnostic Report",
        "",
        "## Configuration",
        f"- date: {config.date}",
        f"- phase: {config.phase}",
        f"- market: {config.market}",
        f"- risk_inputs_dir: {config.risk_inputs_dir}",
        f"- risk_term_values: {', '.join(str(value) for value in config.risk_term_values)}",
        f"- tracking_error_weight: {config.tracking_error_weight}",
        f"- risk_integration_mode: {config.risk_integration_mode}",
        f"- include_risk_term_zero_anchor: {config.include_risk_term_zero_anchor}",
        f"- skip_replay_eligibility_gate: {config.skip_replay_eligibility_gate}",
        "",
        "## Baseline Reference",
        f"- baseline_dashboard: {config.baseline_dashboard}",
        f"- baseline_run_root_at_date: {baseline_row.get('run_root', '')}",
        "",
        "## Matrix",
        "| cell_id | enabled | risk_term_weight | order_count_total | gross_traded_notional_total | estimated_total_cost_total | objective_mode | risk_term_share_abs_weighted_mean | risk_term_to_second_abs_weighted_ratio |",
        "|---|---|---:|---:|---:|---:|---|---:|---:|",
    ]
    for row in rows:
        ratio_value = _safe_float(row.get("risk_term_to_second_abs_weighted_ratio"))
        ratio_text = "inf" if ratio_value == float("inf") else f"{ratio_value:.6f}"
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("cell_id", "")),
                    str(row.get("risk_model_enabled", "")),
                    f"{_safe_float(row.get('risk_term_weight')):.6f}",
                    str(int(row.get("order_count_total", 0))),
                    f"{_safe_float(row.get('gross_traded_notional_total')):.6f}",
                    f"{_safe_float(row.get('estimated_total_cost_total')):.6f}",
                    str(row.get("objective_mode", "")),
                    f"{_safe_float(row.get('risk_term_share_abs_weighted_mean')):.6f}",
                    ratio_text,
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Rule Evaluation",
            f"- scale_suppression_likely: {rule_eval.get('scale_suppression_likely')}",
            f"- weight_not_effective_suspected: {rule_eval.get('weight_not_effective_suspected')}",
            f"- regime_coupling_suspected_by_anchor: {rule_eval.get('regime_coupling_suspected_by_anchor')}",
            "",
            "## Interpretation Guide",
            "- If `scale_suppression_likely=true`, risk term likely dominates objective scale.",
            "- If `weight_not_effective_suspected=true`, weight changes might not be propagating effectively.",
            "- If `regime_coupling_suspected_by_anchor=true`, no-trade persists even at `enabled=true, risk_term_weight=0`.",
        ]
    )
    return "\n".join(lines)


def run_risk_regime_diagnostic(
    config: DiagnosticConfig,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = _run_subprocess,
    logger: Callable[[str], None] = _log,
) -> int:
    _parse_iso_date(config.date)
    returns_path = config.risk_inputs_dir / "returns_long.csv"
    if not returns_path.exists():
        raise FileNotFoundError(f"Missing risk inputs file: {returns_path}")

    baseline_row = _validate_baseline_dashboard(config.baseline_dashboard, date=config.date)
    cells = _build_cell_specs(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    logger(f"Running diagnostic matrix for {len(cells)} cells on date={config.date}")

    matrix_rows: list[dict[str, Any]] = []
    for cell in cells:
        matrix_rows.append(
            _run_cell(
                config=config,
                cell=cell,
                runner=runner,
                logger=logger,
            )
        )

    rule_eval = _evaluate_rules(matrix_rows)
    csv_rows = [_matrix_row_for_csv(row) for row in matrix_rows]
    matrix_frame = pd.DataFrame(csv_rows)
    matrix_frame = matrix_frame.sort_values(
        by=["risk_model_enabled", "risk_term_weight", "is_anchor", "cell_id"],
        ascending=[True, True, True, True],
    )

    matrix_csv_path = config.output_dir / "diagnostic_matrix.csv"
    matrix_json_path = config.output_dir / "diagnostic_matrix.json"
    report_md_path = config.output_dir / "diagnostic_report.md"
    manifest_path = config.output_dir / "diagnostic_manifest.json"

    matrix_frame.to_csv(matrix_csv_path, index=False)
    matrix_payload = {
        "generated_at": _now_iso(),
        "date": config.date,
        "market": config.market,
        "phase": config.phase,
        "tracking_error_weight": float(config.tracking_error_weight),
        "risk_integration_mode": str(config.risk_integration_mode).strip().lower(),
        "risk_term_values": [float(value) for value in config.risk_term_values],
        "include_risk_term_zero_anchor": bool(config.include_risk_term_zero_anchor),
        "skip_replay_eligibility_gate": bool(config.skip_replay_eligibility_gate),
        "baseline_dashboard": str(config.baseline_dashboard),
        "baseline_row": baseline_row,
        "rows": matrix_rows,
        "rule_evaluation": rule_eval,
    }
    matrix_json_path.write_text(json.dumps(matrix_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    report_md_path.write_text(
        _render_markdown_report(
            config=config,
            baseline_row=baseline_row,
            rows=matrix_rows,
            rule_eval=rule_eval,
        ),
        encoding="utf-8",
    )
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at": _now_iso(),
                "diagnostic_matrix_csv": str(matrix_csv_path),
                "diagnostic_matrix_json": str(matrix_json_path),
                "diagnostic_report_md": str(report_md_path),
                "rule_evaluation": rule_eval,
                "cell_count": len(matrix_rows),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    logger(f"diagnostic_matrix.csv: {matrix_csv_path}")
    logger(f"diagnostic_matrix.json: {matrix_json_path}")
    logger(f"diagnostic_report.md: {report_md_path}")
    logger(f"diagnostic_manifest.json: {manifest_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run single-day risk regime diagnostic matrix.")
    parser.add_argument("--baseline-dashboard", type=Path, required=True, help="Baseline pilot_dashboard.csv path.")
    parser.add_argument("--date", required=True, help="Single diagnostic date (YYYY-MM-DD).")
    parser.add_argument("--phase", default="phase_1", help="Pilot phase passed to replay script.")
    parser.add_argument("--market", choices=["cn", "us"], default="us", help="Market mode.")
    parser.add_argument("--risk-inputs-dir", type=Path, required=True, help="Risk inputs directory.")
    parser.add_argument(
        "--risk-term-values",
        default="0.001,0.01,0.1,1.0",
        help="Comma-separated risk_term weights for matrix columns.",
    )
    parser.add_argument(
        "--tracking-error-weight",
        type=float,
        default=1.0,
        help="Fixed tracking_error weight applied to all cells.",
    )
    parser.add_argument(
        "--risk-integration-mode",
        choices=["replace", "augment"],
        default="replace",
        help="Risk objective integration mode used in overlay (default: replace).",
    )
    parser.add_argument("--output-dir", type=Path, required=True, help="Diagnostic output directory.")
    parser.add_argument("--real-sample", action="store_true", help="Include real sample in replay runs.")
    parser.add_argument(
        "--skip-replay-eligibility-gate",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip replay-level eligibility gate (default: true).",
    )
    parser.add_argument(
        "--include-risk-term-zero-anchor",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include anchor cell enabled=true,risk_term_weight=0 (default: true).",
    )
    parser.add_argument(
        "--anchor",
        action="store_true",
        help="Alias for --include-risk-term-zero-anchor.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = DiagnosticConfig(
        baseline_dashboard=Path(args.baseline_dashboard).resolve(),
        date=str(args.date),
        phase=str(args.phase),
        market=str(args.market).strip().lower(),
        risk_inputs_dir=Path(args.risk_inputs_dir).resolve(),
        risk_term_values=_parse_float_list(str(args.risk_term_values)),
        tracking_error_weight=float(args.tracking_error_weight),
        risk_integration_mode=str(args.risk_integration_mode).strip().lower(),
        output_dir=Path(args.output_dir).resolve(),
        real_sample=bool(args.real_sample),
        skip_replay_eligibility_gate=bool(args.skip_replay_eligibility_gate),
        include_risk_term_zero_anchor=bool(args.include_risk_term_zero_anchor or args.anchor),
    )
    return run_risk_regime_diagnostic(config)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
