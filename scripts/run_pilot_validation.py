from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any

import yaml

from portfolio_os.data.providers import get_data_provider
from portfolio_os.data.providers.alpaca_provider import resolve_alpaca_credentials
from portfolio_os.data.providers.tushare_provider import resolve_tushare_token
from portfolio_os.domain.errors import ProviderDataError, ProviderPermissionError, ProviderRuntimeError


ROOT = Path(__file__).resolve().parents[1]
REPLAY_ROOT = ROOT / "data" / "replay_samples"
US_REPLAY_ROOT = ROOT / "data" / "samples" / "us"
SCENARIO_TEMPLATE_PATH = ROOT / "data" / "scenario_samples" / "manifest.yaml"
RUN_TAG = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_ROOT = ROOT / "outputs" / f"pilot_validation_{RUN_TAG}"
DEFAULT_VENV_SCRIPTS = Path(
    r"C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts"
)

CLI_APP_OBJECTS = {
    "portfolio-os": "app",
    "portfolio-os-build-market": "build_market_app",
    "portfolio-os-build-reference": "build_reference_app",
    "portfolio-os-scenarios": "scenario_app",
    "portfolio-os-approve": "approval_app",
    "portfolio-os-execute": "execution_app",
}

REVIEWER_SCORE_COLUMNS = [
    "sample_id",
    "reviewer_id",
    "order_reasonableness_score",
    "findings_explainability_score",
    "scenario_tradeoff_score",
    "approval_handoff_score",
    "execution_credibility_score",
    "notes",
]

MARKET_REQUIRED_COLUMNS = {
    "ticker",
    "close",
    "vwap",
    "adv_shares",
    "tradable",
    "upper_limit_hit",
    "lower_limit_hit",
}

REFERENCE_REQUIRED_COLUMNS = {
    "ticker",
    "industry",
}


BASE_SAMPLES = [
    {
        "sample_id": "sample_01",
        "sample_feature": "high_cash_buy_bias",
        "account_type": "public_fund",
        "provider_type": "mock_provider",
        "source_dir": REPLAY_ROOT / "sample_01",
        "holdings_file": "holdings.csv",
        "target_file": "target.csv",
        "portfolio_state_file": "portfolio_state.yaml",
        "reference_overlay_file": "reference.csv",
        "market_fallback_file": "market.csv",
        "reference_fallback_file": "reference.csv",
        "build_provider": "mock",
        "allow_main_fallback": True,
    },
    {
        "sample_id": "sample_02",
        "sample_feature": "tight_cash_sell_driven",
        "account_type": "public_fund",
        "provider_type": "mock_provider",
        "source_dir": REPLAY_ROOT / "sample_02",
        "holdings_file": "holdings.csv",
        "target_file": "target.csv",
        "portfolio_state_file": "portfolio_state.yaml",
        "reference_overlay_file": "reference.csv",
        "market_fallback_file": "market.csv",
        "reference_fallback_file": "reference.csv",
        "build_provider": "mock",
        "allow_main_fallback": True,
    },
    {
        "sample_id": "sample_03",
        "sample_feature": "multi_untradable_and_blacklist",
        "account_type": "public_fund",
        "provider_type": "mock_provider",
        "source_dir": REPLAY_ROOT / "sample_03",
        "holdings_file": "holdings.csv",
        "target_file": "target.csv",
        "portfolio_state_file": "portfolio_state.yaml",
        "reference_overlay_file": "reference.csv",
        "market_fallback_file": "market.csv",
        "reference_fallback_file": "reference.csv",
        "build_provider": "mock",
        "allow_main_fallback": True,
    },
    {
        "sample_id": "sample_04",
        "sample_feature": "constraint_pressure",
        "account_type": "public_fund",
        "provider_type": "mock_provider",
        "source_dir": REPLAY_ROOT / "sample_04",
        "holdings_file": "holdings.csv",
        "target_file": "target.csv",
        "portfolio_state_file": "portfolio_state.yaml",
        "reference_overlay_file": "reference.csv",
        "market_fallback_file": "market.csv",
        "reference_fallback_file": "reference.csv",
        "build_provider": "mock",
        "allow_main_fallback": True,
    },
    {
        "sample_id": "sample_05",
        "sample_feature": "low_liquidity_stress",
        "account_type": "public_fund",
        "provider_type": "mock_provider",
        "source_dir": REPLAY_ROOT / "sample_05",
        "holdings_file": "holdings.csv",
        "target_file": "target.csv",
        "portfolio_state_file": "portfolio_state.yaml",
        "reference_overlay_file": "reference.csv",
        "market_fallback_file": "market.csv",
        "reference_fallback_file": "reference.csv",
        "build_provider": "mock",
        "allow_main_fallback": True,
    },
]

US_BASE_SAMPLES = [
    {
        "sample_id": "sample_us_01",
        "sample_feature": "us_large_cap_core",
        "account_type": "us_pilot",
        "provider_type": "alpaca_provider",
        "source_dir": US_REPLAY_ROOT / "sample_us_01",
        "holdings_file": "holdings.csv",
        "target_file": "target.csv",
        "portfolio_state_file": "portfolio_state.yaml",
        "reference_overlay_file": "reference.csv",
        "market_fallback_file": "market.csv",
        "reference_fallback_file": "reference.csv",
        "build_provider": "alpaca",
        "allow_main_fallback": True,
    },
    {
        "sample_id": "sample_us_02",
        "sample_feature": "us_single_name_pressure",
        "account_type": "us_pilot",
        "provider_type": "alpaca_provider",
        "source_dir": US_REPLAY_ROOT / "sample_us_02",
        "holdings_file": "holdings.csv",
        "target_file": "target.csv",
        "portfolio_state_file": "portfolio_state.yaml",
        "reference_overlay_file": "reference.csv",
        "market_fallback_file": "market.csv",
        "reference_fallback_file": "reference.csv",
        "build_provider": "alpaca",
        "allow_main_fallback": True,
    },
    {
        "sample_id": "sample_us_03",
        "sample_feature": "us_low_liquidity_mix",
        "account_type": "us_pilot",
        "provider_type": "alpaca_provider",
        "source_dir": US_REPLAY_ROOT / "sample_us_03",
        "holdings_file": "holdings.csv",
        "target_file": "target.csv",
        "portfolio_state_file": "portfolio_state.yaml",
        "reference_overlay_file": "reference.csv",
        "market_fallback_file": "market.csv",
        "reference_fallback_file": "reference.csv",
        "build_provider": "alpaca",
        "allow_main_fallback": True,
    },
]


@dataclass
class CommandResult:
    name: str
    ok: bool
    return_code: int
    command: list[str]
    log_path: Path


@dataclass
class ValidationOptions:
    mode: str
    reviewer_input: Path | None
    include_real_sample: bool
    real_feed_as_of_date: str
    market: str = "cn"
    config_overlay: Path | None = None
    gate_mode_compat: str | None = None


def _parse_args(argv: list[str] | None = None) -> ValidationOptions:
    parser = argparse.ArgumentParser(description="Run PortfolioOS pilot validation workflow.")
    parser.add_argument(
        "--mode",
        choices=["nightly", "release"],
        default="nightly",
        help="nightly allows provisional outputs; release enforces strict gate and exits non-zero when gate fails.",
    )
    parser.add_argument(
        "--gate-mode",
        choices=["provisional", "strict_gate"],
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--reviewer-input",
        type=Path,
        default=None,
        help="Reviewer CSV with subjective scores per sample. Required in release mode.",
    )
    parser.add_argument(
        "--real-sample",
        action="store_true",
        help="Run one additional real-feed full-chain sample when TUSHARE_TOKEN is available.",
    )
    parser.add_argument(
        "--real-feed-as-of-date",
        default="2026-03-23",
        help="As-of date used for real feed builders.",
    )
    parser.add_argument(
        "--market",
        choices=["cn", "us"],
        default="cn",
        help="Run validation in cn (A-share) or us (US equities) mode.",
    )
    parser.add_argument(
        "--config-overlay",
        type=Path,
        default=None,
        help="Optional YAML overlay merged into the default config for this validation run.",
    )
    args = parser.parse_args(argv)
    normalized_mode = str(args.mode)
    if args.gate_mode is not None:
        normalized_mode = "release" if str(args.gate_mode) == "strict_gate" else "nightly"
    return ValidationOptions(
        mode=normalized_mode,
        reviewer_input=Path(args.reviewer_input).resolve() if args.reviewer_input is not None else None,
        include_real_sample=bool(args.real_sample),
        real_feed_as_of_date=str(args.real_feed_as_of_date),
        market=str(args.market).lower(),
        config_overlay=Path(args.config_overlay).resolve() if args.config_overlay is not None else None,
        gate_mode_compat=(str(args.gate_mode) if args.gate_mode is not None else None),
    )


def _cli_executable(name: str) -> str:
    env_candidate = ROOT / ".venv" / "Scripts" / f"{name}.exe"
    if env_candidate.exists():
        return str(env_candidate)
    global_candidate = DEFAULT_VENV_SCRIPTS / f"{name}.exe"
    return str(global_candidate)


def _normalize_cli_name(raw: str) -> str:
    name = Path(str(raw)).name.strip().lower()
    if name.endswith(".exe"):
        return name[:-4]
    return name


def _resolve_effective_command(command: list[str]) -> list[str]:
    if not command:
        return []
    cli_name = _normalize_cli_name(str(command[0]))
    app_object = CLI_APP_OBJECTS.get(cli_name)
    if app_object is not None:
        # Use the current interpreter to launch the Typer app object directly.
        # This avoids Windows PATH/entrypoint lookup issues for `portfolio-os-*` binaries.
        return [
            sys.executable,
            "-c",
            f"from portfolio_os.api.cli import {app_object} as _app; _app()",
            *command[1:],
        ]
    return list(command)


def _run_command(name: str, command: list[str], log_path: Path) -> CommandResult:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    effective_command = _resolve_effective_command(command)
    completed = subprocess.run(
        effective_command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    with log_path.open("w", encoding="utf-8") as handle:
        handle.write("COMMAND:\n")
        handle.write(" ".join(effective_command))
        handle.write("\n\nSTDOUT:\n")
        handle.write(completed.stdout)
        handle.write("\n\nSTDERR:\n")
        handle.write(completed.stderr)
        handle.write(f"\n\nRETURN_CODE: {completed.returncode}\n")
    return CommandResult(
        name=name,
        ok=completed.returncode == 0,
        return_code=completed.returncode,
        command=effective_command,
        log_path=log_path,
    )


def _load_csv_tickers(path: Path) -> set[str]:
    out: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ticker = str(row.get("ticker", "")).strip()
            if ticker:
                out.add(ticker)
    return out


def _extract_as_of_date(portfolio_state_path: Path) -> str:
    payload = yaml.safe_load(portfolio_state_path.read_text(encoding="utf-8"))
    return str(payload["as_of_date"])


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _deep_merge_dict(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge overlay values into base mapping."""

    merged = deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def _build_runtime_config_path(
    *,
    base_config_path: Path,
    config_overlay_path: Path | None,
    run_root: Path,
) -> Path:
    """Resolve the effective config path for this run, applying overlay when present."""

    if config_overlay_path is None:
        return base_config_path
    if not config_overlay_path.exists():
        raise FileNotFoundError(f"Config overlay not found: {config_overlay_path}")
    base_payload = yaml.safe_load(base_config_path.read_text(encoding="utf-8")) or {}
    overlay_payload = yaml.safe_load(config_overlay_path.read_text(encoding="utf-8")) or {}
    if not isinstance(base_payload, dict):
        raise ValueError(f"Base config must be a YAML mapping: {base_config_path}")
    if not isinstance(overlay_payload, dict):
        raise ValueError(f"Overlay config must be a YAML mapping: {config_overlay_path}")

    merged = _deep_merge_dict(base_payload, overlay_payload)
    resolved_overlay = run_root / "config_overlay_merged.yaml"
    resolved_overlay.write_text(
        yaml.safe_dump(merged, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return resolved_overlay


def _score_orders(cost_diff: float, blocked_diff: float, target_delta: float) -> int:
    score = 3
    if cost_diff > 0:
        score += 1
    if blocked_diff >= 0:
        score += 1
    if target_delta > 0.05:
        score -= 1
    return min(5, max(1, score))


def _score_findings(finding_count: int, blocked_count: int) -> int:
    if blocked_count > 3:
        return 2
    if finding_count <= 8:
        return 5
    if finding_count <= 14:
        return 4
    return 3


def _score_scenario(spread: float) -> int:
    if spread >= 50:
        return 5
    if spread >= 20:
        return 4
    if spread >= 5:
        return 3
    return 2


def _score_approval(approval_status: str, selected_differs: bool) -> int:
    if approval_status not in {"approved", "approved_with_override"}:
        return 1
    if approval_status == "approved_with_override":
        return 4
    return 5 if selected_differs else 4


def _score_execution(fill_rate: float, partial_count: int, unfilled_count: int) -> int:
    if unfilled_count > 0:
        return 4
    if partial_count > 0:
        return 5
    if fill_rate >= 0.99:
        return 3
    if fill_rate >= 0.9:
        return 4
    return 2


def _sample_conclusion(chain_ok: bool, cost_diff: float, fill_rate: float) -> str:
    if not chain_ok:
        return "not useful"
    if cost_diff > 0 and fill_rate >= 0.9:
        return "usable"
    if cost_diff > -50:
        return "borderline"
    return "not useful"


def _build_scenario_manifest(base_inputs: dict[str, str], source_scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "name": "pilot_validation_scenarios",
        "description": "Scenario suite for pilot validation sample",
        "base_inputs": base_inputs,
        "scenarios": source_scenarios,
    }


def _build_approval_request(
    scenario_output_dir: Path,
    selected_scenario: str,
    sample_id: str,
    *,
    use_override: bool,
) -> dict[str, Any]:
    payload = {
        "name": f"approval_{sample_id}",
        "description": f"Approval request for {sample_id}",
        "scenario_output_dir": str(scenario_output_dir),
        "selected_scenario": selected_scenario,
        "decision_maker": "pilot_pm",
        "decision_role": "portfolio_manager",
        "rationale": "Pilot validation run; select scenario for freeze and execution simulation.",
        "acknowledged_warning_codes": [
            "no_order_due_to_constraint",
            "no_tradable_securities_in_snapshot",
            "manager_aggregate_limit",
            "locked_single_name_above_limit",
            "locked_industry_above_max",
            "locked_industry_below_min",
        ],
        "handoff": {
            "trader": "pilot_trader",
            "reviewer": "pilot_risk",
            "compliance_contact": "pilot_compliance",
        },
        "tags": ["pilot_validation", sample_id],
        "override_auto_generated": use_override,
    }
    if use_override:
        payload["override"] = {
            "enabled": True,
            "reason": "Controlled override for pilot validation continuity on blocking findings.",
            "override_reason_code": "workflow_continuity",
            "approver": "pilot_risk_head",
            "approved_at": "2026-03-24T09:30:00+00:00",
        }
    return payload


def _build_execution_request(
    artifact_dir: Path,
    input_orders: str,
    portfolio_state_path: Path,
    sample_id: str,
    low_liquidity_stress: bool,
    market_path: Path | None = None,
    audit_path: str | None = None,
) -> dict[str, Any]:
    simulation = {
        "mode": "participation_twap",
        "bucket_count": 5,
        "allow_partial_fill": True,
        "force_completion": False,
        "max_bucket_participation_override": None,
    }
    if low_liquidity_stress:
        simulation["max_bucket_participation_override"] = 0.0005
    return {
        "name": f"execution_{sample_id}",
        "description": f"Execution simulation request for {sample_id}",
        "artifact_dir": str(artifact_dir),
        "input_orders": input_orders,
        "audit": audit_path,
        "market": str(market_path) if market_path is not None else None,
        "portfolio_state": str(portfolio_state_path),
        "execution_profile": str(ROOT / "config" / "execution" / "conservative.yaml"),
        "calibration_profile": str(ROOT / "config" / "calibration_profiles" / "balanced_day.yaml"),
        "simulation": simulation,
    }


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )


def _write_reviewer_template(path: Path, sample_ids: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REVIEWER_SCORE_COLUMNS)
        writer.writeheader()
        for sample_id in sample_ids:
            writer.writerow(
                {
                    "sample_id": sample_id,
                    "reviewer_id": "",
                    "order_reasonableness_score": "",
                    "findings_explainability_score": "",
                    "scenario_tradeoff_score": "",
                    "approval_handoff_score": "",
                    "execution_credibility_score": "",
                    "notes": "",
                }
            )


def _load_reviewer_scores(path: Path | None) -> tuple[dict[str, dict[str, Any]], str]:
    if path is None:
        return {}, "heuristic"
    if not path.exists():
        raise FileNotFoundError(f"Reviewer input not found: {path}")
    rows: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing_columns = [column for column in REVIEWER_SCORE_COLUMNS if column not in (reader.fieldnames or [])]
        if missing_columns:
            raise ValueError(f"Reviewer input missing required columns: {', '.join(missing_columns)}")
        for row in reader:
            sample_id = str(row.get("sample_id", "")).strip()
            if not sample_id:
                continue
            rows[sample_id] = row
    return rows, "reviewer_csv"


def _get_score_from_reviewer(row: dict[str, Any], key: str) -> int | None:
    raw = str(row.get(key, "")).strip()
    if not raw:
        return None
    value = int(float(raw))
    if value < 1 or value > 5:
        raise ValueError(f"Reviewer score {key} must be in [1, 5], received {value}.")
    return value


def _build_skipped_result(name: str, log_path: Path, reason: str) -> CommandResult:
    """Create a synthetic skipped command result with an explicit log."""

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as handle:
        handle.write("COMMAND:\n")
        handle.write("<skipped>\n\n")
        handle.write("STDOUT:\n")
        handle.write("\n\nSTDERR:\n")
        handle.write(f"{reason}\n")
        handle.write("\nRETURN_CODE: -99\n")
    return CommandResult(
        name=name,
        ok=False,
        return_code=-99,
        command=[],
        log_path=log_path,
    )


def _build_reused_success_result(
    name: str,
    log_path: Path,
    *,
    source_csv: Path,
    source_manifest: Path,
) -> CommandResult:
    """Create a synthetic success result when a prior successful artifact is reused."""

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as handle:
        handle.write("COMMAND:\n")
        handle.write("<reused_from_real_feed_check>\n\n")
        handle.write("STDOUT:\n")
        handle.write(
            f"Reused artifact from precheck: csv={source_csv}; manifest={source_manifest}\n"
        )
        handle.write("\n\nSTDERR:\n")
        handle.write("\nRETURN_CODE: 0\n")
    return CommandResult(
        name=name,
        ok=True,
        return_code=0,
        command=[],
        log_path=log_path,
    )


def _is_skipped_result(result: CommandResult) -> bool:
    return result.return_code == -99


def _validate_snapshot_csv(
    *,
    path: Path,
    expected_tickers: set[str],
    required_columns: set[str],
) -> tuple[bool, str]:
    """Run a lightweight input precheck for market/reference snapshots."""

    if not path.exists():
        return False, f"{path.name} missing"
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        missing_columns = sorted(required_columns - columns)
        if missing_columns:
            return False, f"{path.name} missing columns: {', '.join(missing_columns)}"
        rows = list(reader)
    if not rows:
        return False, f"{path.name} has no rows"
    actual_tickers = {
        str(row.get("ticker", "")).strip() for row in rows if str(row.get("ticker", "")).strip()
    }
    missing_tickers = sorted(expected_tickers - actual_tickers)
    if missing_tickers:
        return False, f"{path.name} missing tickers: {', '.join(missing_tickers[:5])}"
    return True, "ok"


def _is_no_data_for_trade_date_error(message: str) -> bool:
    normalized = message.lower()
    return "returned no rows for trade_date" in normalized or "no matching rows" in normalized


def _to_iso_date(date_text: str) -> str:
    if len(date_text) == 8:
        return datetime.strptime(date_text, "%Y%m%d").strftime("%Y-%m-%d")
    return datetime.strptime(date_text, "%Y-%m-%d").strftime("%Y-%m-%d")


def _resolve_real_trade_date(
    *,
    requested_date: str,
    tickers: list[str],
    provider_name: str = "tushare",
    max_lookback_days: int = 14,
) -> tuple[str, list[str], str | None]:
    """Resolve a real sample date to the nearest available trading date."""

    provider = get_data_provider(provider_name)
    current_date = datetime.strptime(_to_iso_date(requested_date), "%Y-%m-%d").date()
    attempted: list[str] = []
    last_error: str | None = None
    for _offset in range(max_lookback_days + 1):
        probe_date = current_date.strftime("%Y-%m-%d")
        attempted.append(probe_date)
        try:
            provider.get_daily_market_snapshot(tickers, probe_date)
            return probe_date, attempted, None
        except ProviderDataError as exc:
            last_error = str(exc)
            if _is_no_data_for_trade_date_error(last_error):
                current_date = current_date - timedelta(days=1)
                continue
            return _to_iso_date(requested_date), attempted, last_error
        except (ProviderPermissionError, ProviderRuntimeError) as exc:
            return _to_iso_date(requested_date), attempted, str(exc)
    return _to_iso_date(requested_date), attempted, (
        last_error or f"no valid trading date found within {max_lookback_days} days"
    )


def _build_widened_scenarios(source_scenarios: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Expand scenario parameter spread once when the first run is near-tied."""

    widened = deepcopy(source_scenarios)
    for scenario in widened:
        scenario_id = str(scenario.get("id", ""))
        overrides = dict(scenario.get("overrides") or {})
        if scenario_id == "public_aggressive":
            overrides["max_turnover"] = min(0.98, float(overrides.get("max_turnover", 0.8)) + 0.1)
            overrides["participation_limit"] = min(
                0.45, float(overrides.get("participation_limit", 0.25)) + 0.1
            )
            overrides["min_order_notional"] = max(
                500.0, float(overrides.get("min_order_notional", 1000.0)) * 0.5
            )
            overrides["min_cash_buffer"] = max(
                5000.0, float(overrides.get("min_cash_buffer", 10000.0)) * 0.5
            )
        elif scenario_id == "private_flexible":
            overrides["max_turnover"] = min(0.99, float(overrides.get("max_turnover", 0.95)) + 0.03)
            overrides["participation_limit"] = min(
                0.5, float(overrides.get("participation_limit", 0.35)) + 0.1
            )
            overrides["min_order_notional"] = max(
                500.0, float(overrides.get("min_order_notional", 1000.0)) * 0.5
            )
        elif scenario_id == "quant_execution":
            overrides["max_turnover"] = min(0.99, float(overrides.get("max_turnover", 0.9)) + 0.05)
            overrides["participation_limit"] = min(
                0.45, float(overrides.get("participation_limit", 0.3)) + 0.1
            )
            overrides["min_order_notional"] = max(
                500.0, float(overrides.get("min_order_notional", 800.0)) * 0.6
            )
        elif scenario_id == "public_high_cash_buffer":
            overrides["min_cash_buffer"] = float(overrides.get("min_cash_buffer", 400000.0)) * 1.75
            overrides["min_order_notional"] = float(overrides.get("min_order_notional", 15000.0)) * 1.5
            overrides["participation_limit"] = max(
                0.02, float(overrides.get("participation_limit", 0.05)) * 0.6
            )
        scenario["overrides"] = overrides
    return widened


def _safe_float(raw: Any, default: float = 0.0) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _apply_us_scenario_feasibility_guards(
    source_scenarios: list[dict[str, Any]],
    *,
    portfolio_state_path: Path,
) -> list[dict[str, Any]]:
    """Cap US high-cash scenario parameters so the scenario set stays feasible for small-NAV samples."""

    scenarios = deepcopy(source_scenarios)
    if not portfolio_state_path.exists():
        return scenarios
    try:
        portfolio_state = yaml.safe_load(portfolio_state_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return scenarios

    available_cash = max(0.0, _safe_float(portfolio_state.get("available_cash"), 0.0))
    if available_cash <= 0:
        return scenarios

    max_cash_buffer = max(10000.0, available_cash * 0.75)
    for scenario in scenarios:
        if str(scenario.get("id", "")) != "public_high_cash_buffer":
            continue
        overrides = dict(scenario.get("overrides") or {})
        current_cash_buffer = _safe_float(overrides.get("min_cash_buffer"), 0.0)
        if current_cash_buffer > max_cash_buffer:
            overrides["min_cash_buffer"] = round(max_cash_buffer, 2)
        scenario["overrides"] = overrides
    return scenarios


def _extract_selected_scenario(
    *,
    scenario_payload: dict[str, Any],
) -> tuple[str, float, int]:
    selected_scenario = str(
        scenario_payload.get("labels", {}).get("recommended_scenario", "public_conservative")
    )
    gap = float(scenario_payload.get("recommendation_diagnostics", {}).get("score_gap_to_second", 0.0))
    scenario_rows = {str(row["scenario_id"]): row for row in scenario_payload.get("scenarios", [])}
    selected_row = scenario_rows.get(selected_scenario) or {}
    selected_blocking_count = int(selected_row.get("blocking_finding_count", 0))
    return selected_scenario, gap, selected_blocking_count


def _has_complete_reviewer_scores(
    *,
    sample_ids: list[str],
    reviewer_rows: dict[str, dict[str, Any]],
) -> tuple[bool, list[str]]:
    missing: list[str] = []
    required_score_columns = [
        "order_reasonableness_score",
        "findings_explainability_score",
        "scenario_tradeoff_score",
        "approval_handoff_score",
        "execution_credibility_score",
    ]
    for sample_id in sample_ids:
        row = reviewer_rows.get(sample_id)
        if row is None:
            missing.append(f"{sample_id}:missing_row")
            continue
        for column in required_score_columns:
            value = str(row.get(column, "")).strip()
            if not value:
                missing.append(f"{sample_id}:{column}")
    return len(missing) == 0, missing


def _collect_capability_blockers(
    *,
    market_manifest: dict[str, Any] | None,
    reference_manifest: dict[str, Any] | None,
) -> list[str]:
    blockers: list[str] = []
    for feed_name, payload in (("market", market_manifest), ("reference", reference_manifest)):
        manifest = payload or {}
        build_status = str(manifest.get("build_status", "unknown"))
        capability = str(manifest.get("provider_capability_status", "unknown"))
        permission_notes = manifest.get("permission_notes") or []
        if build_status != "success" or capability != "available":
            blockers.append(
                f"{feed_name}:build_status={build_status};capability={capability};permission_notes={','.join(permission_notes) or 'none'}"
            )
    return blockers

def _build_sample_markdown(path: Path, report: dict[str, Any]) -> None:
    lines = [
        f"# Sample Evaluation - {report['sample_id']}",
        "",
        "## Basic Info",
        f"- sample_id: {report['sample_id']}",
        f"- date: {report['date']}",
        f"- requested_date: {report.get('requested_date', report['date'])}",
        f"- effective_trade_date: {report.get('effective_trade_date', report['date'])}",
        f"- account_type: {report['account_type']}",
        f"- sample_feature: {report['sample_feature']}",
        f"- data_path: {report['data_path']}",
        f"- provider_type: {report['provider_type']}",
        "",
        "## Technical Results",
        f"- market_builder_success: {report['market_builder_success']}",
        f"- reference_builder_success: {report['reference_builder_success']}",
        f"- main_flow_success: {report['main_flow_success']}",
        f"- scenario_success: {report['scenario_success']}",
        f"- approval_success: {report['approval_success']}",
        f"- approval_status: {report.get('approval_status', 'unknown')}",
        f"- override_used: {report.get('override_used', False)}",
        f"- override_auto_generated: {report.get('override_auto_generated', False)}",
        f"- execution_success: {report['execution_success']}",
        f"- process_state_main: {report.get('process_state_main', 'unknown')}",
        f"- process_state_execution: {report.get('process_state_execution', 'unknown')}",
        "",
        "## Business Metrics",
        f"- naive_cost: {report['naive_cost']:.4f}",
        f"- portfolio_os_cost: {report['portfolio_os_cost']:.4f}",
        f"- cost_difference: {report['cost_difference']:.4f}",
        f"- naive_turnover: {report['naive_turnover']:.6f}",
        f"- portfolio_os_turnover: {report['portfolio_os_turnover']:.6f}",
        f"- blocked_trade_difference: {report['blocked_trade_difference']}",
        f"- single_name_blocking_count: {report.get('single_name_blocking_count', 0)}",
        f"- single_name_blocked_untradeable_count: {report.get('single_name_blocked_untradeable_count', 0)}",
        f"- fill_rate: {report['fill_rate']:.6f}",
        f"- partial_fill_count: {report['partial_fill_count']}",
        f"- unfilled_count: {report['unfilled_count']}",
        f"- stress_partial_fill_count: {report.get('stress_partial_fill_count', 0)}",
        f"- stress_unfilled_count: {report.get('stress_unfilled_count', 0)}",
        f"- scenario_score_gap: {report.get('scenario_score_gap', 0.0):.4f}",
        f"- provider_capability_blockers: {', '.join(report.get('provider_capability_blockers', [])) or 'none'}",
        f"- solver_used: {report.get('solver_used') or 'N/A'}",
        f"- solver_fallback_used: {report.get('solver_fallback_used', False)}",
        f"- constraint_residual_max: {report.get('constraint_residual_max', 0.0):.6g}",
        f"- data_source_mix_market: {', '.join(report.get('data_source_mix_market', [])) or 'unknown'}",
        f"- data_source_mix_reference: {', '.join(report.get('data_source_mix_reference', [])) or 'unknown'}",
        "",
        "## Subjective Scores",
        f"- subjective_score_source: {report['subjective_score_source']}",
        f"- reviewer_id: {report.get('reviewer_id') or 'N/A'}",
        f"- order_reasonableness: {report['order_reasonableness_score']}/5",
        f"- findings_explainability: {report['findings_explainability_score']}/5",
        f"- scenario_tradeoff: {report['scenario_tradeoff_score']}/5",
        f"- approval_handoff: {report['approval_handoff_score']}/5",
        f"- execution_credibility: {report['execution_credibility_score']}/5",
        "",
        "## Traceability",
        f"- benchmark_json_path: {report.get('benchmark_json_path') or 'N/A'}",
        f"- execution_report_path: {report.get('execution_report_path') or 'N/A'}",
        f"- scenario_comparison_path: {report.get('scenario_comparison_path') or 'N/A'}",
        f"- approval_record_path: {report.get('approval_record_path') or 'N/A'}",
        f"- audit_path: {report.get('main_audit_path') or 'N/A'}",
        "",
        "## Conclusion",
        f"- verdict: {report['verdict']}",
        "",
        "## Notes",
        f"- biggest_issue: {report['biggest_issue']}",
        f"- one_thing_to_fix: {report['one_thing_to_fix']}",
        f"- reviewer_notes: {report.get('reviewer_notes') or 'N/A'}",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _build_sample_list(options: ValidationOptions, *, provider_creds_available: bool) -> list[dict[str, Any]]:
    if options.market == "us":
        samples = [dict(item) for item in US_BASE_SAMPLES]
        if options.include_real_sample and provider_creds_available:
            samples.append(
                {
                    "sample_id": "real_sample_01",
                    "sample_feature": "real_alpaca_snapshot",
                    "account_type": "us_pilot",
                    "provider_type": "alpaca_live",
                    "source_dir": US_REPLAY_ROOT / "sample_us_01",
                    "holdings_file": "holdings.csv",
                    "target_file": "target.csv",
                    "portfolio_state_file": "portfolio_state.yaml",
                    "reference_overlay_file": "reference.csv",
                    "market_fallback_file": "market.csv",
                    "reference_fallback_file": "reference.csv",
                    "build_provider": "alpaca",
                    "allow_main_fallback": False,
                    "requested_as_of_date": options.real_feed_as_of_date,
                }
            )
        return samples

    samples = [dict(item) for item in BASE_SAMPLES]
    if options.include_real_sample and provider_creds_available:
        samples.append(
            {
                "sample_id": "real_sample_01",
                "sample_feature": "real_tushare_snapshot",
                "account_type": "public_fund",
                "provider_type": "tushare_live",
                "source_dir": ROOT / "data" / "sample",
                "holdings_file": "holdings_example.csv",
                "target_file": "target_example.csv",
                "portfolio_state_file": "portfolio_state_example.yaml",
                "reference_overlay_file": "reference_overlay_example.csv",
                "market_fallback_file": "market_example.csv",
                "reference_fallback_file": "reference_example.csv",
                "build_provider": "tushare",
                "allow_main_fallback": False,
                "requested_as_of_date": options.real_feed_as_of_date,
            }
        )
    return samples


def _collect_dynamic_issues(
    *,
    reports: list[dict[str, Any]],
    real_feed_check: dict[str, Any],
    mode: str,
    reviewer_complete: bool,
    market: str,
) -> list[str]:
    issues: list[str] = []
    if not real_feed_check.get("credentials_available", False):
        if market == "us":
            issues.append(
                "No live Alpaca credentials in current environment, so this round is semi-real (static US samples only)."
            )
        else:
            issues.append(
                "No live Tushare token in current environment, so this round is semi-real (mock samples only)."
            )
    else:
        degraded = []
        for feed_name in ("market_manifest", "reference_manifest"):
            manifest = real_feed_check.get(feed_name) or {}
            status = str(manifest.get("build_status", ""))
            if "degradation" in status or "degraded" in status:
                degraded.append(feed_name.replace("_manifest", ""))
        if degraded:
            issues.append(
                "Real feed builders succeeded with degradation in "
                + ", ".join(degraded)
                + f"; monitor permission-limited endpoints and fallback quality (see {RUN_ROOT / 'real_feed_check'})."
            )
        else:
            issues.append("Real feed builders succeeded without reported degradation in this run.")

    stress_unfilled_total = sum(float(item.get("stress_total_unfilled_notional", 0.0)) for item in reports)
    stress_partial_total = sum(int(item.get("stress_partial_fill_count", 0)) for item in reports)
    if stress_unfilled_total <= 0 and stress_partial_total <= 0:
        issues.append("Execution stress still shows weak residual-risk separation; calibrate tighter stressed liquidity assumptions.")

    narrow_gap_count = sum(1 for item in reports if float(item.get("scenario_score_gap", 0.0)) < 0.01)
    if narrow_gap_count > 1:
        issues.append(
            f"Scenario score gaps are <0.01 in {narrow_gap_count}/{len(reports)} samples; increase scenario spread for stronger PM contrast."
        )

    override_count = sum(1 for item in reports if bool(item.get("override_used", False)))
    if reports and (override_count / len(reports)) > 0.4:
        issues.append(
            f"Override usage is {override_count}/{len(reports)}; reduce blocking friction through finding clarity and policy tuning."
        )

    if mode == "release" and not reviewer_complete:
        issues.append("release mode requires complete reviewer inputs for all samples; current verdict is not releasable.")
    if mode == "nightly":
        issues.append("nightly mode allows heuristic fallback; use release mode + reviewer CSV for gate-qualified verdicts.")

    fallback_count = sum(1 for item in reports if bool(item.get("main_used_fallback", False)))
    if fallback_count > 0:
        issues.append(f"Main flow used fallback inputs in {fallback_count} samples; keep builder consistency monitoring active.")

    deduped_issues: list[str] = []
    seen_issues: set[str] = set()
    for issue in issues:
        if issue in seen_issues:
            continue
        seen_issues.add(issue)
        deduped_issues.append(issue)

    filler_candidates = [
        "Continue daily regression on static + real sample modes to keep pilot readiness stable.",
        "Track weekly drift in scenario gap, override ratio, and stress residual-risk metrics.",
        "Keep reviewer rubric calibration synchronized across PM, risk, and compliance.",
    ]
    filler_index = 0
    while len(deduped_issues) < 5:
        if filler_index < len(filler_candidates):
            candidate = filler_candidates[filler_index]
            filler_index += 1
        else:
            candidate = f"Operational follow-up item {filler_index - len(filler_candidates) + 1}: keep pilot controls stable."
            filler_index += 1
        if candidate in seen_issues:
            continue
        seen_issues.add(candidate)
        deduped_issues.append(candidate)

    return deduped_issues[:5]


def _evaluate_release_gate(
    *,
    reports: list[dict[str, Any]],
    options: ValidationOptions,
    reviewer_complete: bool,
    real_feed_check: dict[str, Any],
) -> tuple[bool, list[str], dict[str, Any]]:
    static_reports = [item for item in reports if not str(item["sample_id"]).startswith("real_sample_")]
    real_reports = [item for item in reports if str(item["sample_id"]).startswith("real_sample_")]
    static_count = len(static_reports)
    real_count = len(real_reports)

    full_chain_success_static = sum(
        1
        for item in static_reports
        if item["market_builder_success"]
        and item["reference_builder_success"]
        and item["main_flow_success"]
        and item["scenario_success"]
        and item["approval_success"]
        and item["execution_success"]
    )
    full_chain_success_real = sum(
        1
        for item in real_reports
        if item["market_builder_success"]
        and item["reference_builder_success"]
        and item["main_flow_success"]
        and item["scenario_success"]
        and item["approval_success"]
        and item["execution_success"]
    )
    static_cost_better_count = sum(1 for item in static_reports if float(item["cost_difference"]) > 0)
    static_cost_better_ratio = (static_cost_better_count / static_count) if static_count else 0.0
    static_override_count = sum(1 for item in static_reports if bool(item.get("override_used", False)))
    static_gap_ok_count = sum(1 for item in static_reports if float(item.get("scenario_score_gap", 0.0)) >= 0.01)
    static_single_name_blocking_count = sum(
        int(item.get("single_name_blocking_count", 0)) for item in static_reports
    )
    static_single_name_blocked_untradeable_count = sum(
        int(item.get("single_name_blocked_untradeable_count", 0)) for item in static_reports
    )
    static_solver_fallback_count = sum(1 for item in static_reports if bool(item.get("solver_fallback_used", False)))
    mean_order = mean(float(item["order_reasonableness_score"]) for item in static_reports) if static_reports else 0.0
    mean_findings = (
        mean(float(item["findings_explainability_score"]) for item in static_reports) if static_reports else 0.0
    )
    mean_execution = (
        mean(float(item["execution_credibility_score"]) for item in static_reports) if static_reports else 0.0
    )

    reasons: list[str] = []
    expected_static_count = 3 if options.market == "us" else 5
    if not options.include_real_sample:
        reasons.append("release mode requires --real-sample.")
    if not reviewer_complete:
        reasons.append("release mode requires complete reviewer CSV scores for every sample.")
    if static_count != expected_static_count or full_chain_success_static != expected_static_count:
        reasons.append(
            "static full-chain success is "
            f"{full_chain_success_static}/{static_count}; expected {expected_static_count}/{expected_static_count}."
        )
    if real_count != 1 or full_chain_success_real != 1:
        reasons.append(f"real-sample full-chain success is {full_chain_success_real}/{real_count}; expected 1/1.")
    if static_cost_better_ratio < 0.7:
        reasons.append(f"static cost_better_ratio is {static_cost_better_ratio:.2%}; expected >= 70%.")
    if static_override_count > 2:
        reasons.append(f"static override usage is {static_override_count}/{expected_static_count}; expected <= 2/5.")
    expected_gap_count = min(4, expected_static_count)
    if static_gap_ok_count < expected_gap_count:
        reasons.append(
            "static scenario score_gap>=0.01 count is "
            f"{static_gap_ok_count}/{expected_static_count}; expected >= {expected_gap_count}/{expected_static_count}."
        )
    if mean_order < 4 or mean_findings < 4 or mean_execution < 4:
        reasons.append(
            "mean reviewer scores must all be >= 4/5 for order_reasonableness/findings_explainability/execution_credibility."
        )
    capability_blockers = list(real_feed_check.get("provider_capability_blockers") or [])
    if capability_blockers:
        reasons.append("real feed capability is degraded or unavailable; strict release requires non-degraded success.")

    metrics = {
        "full_chain_success_static": full_chain_success_static,
        "full_chain_success_real": full_chain_success_real,
        "static_count": static_count,
        "real_count": real_count,
        "cost_better_ratio_static": static_cost_better_ratio,
        "override_used_static": static_override_count,
        "score_gap_ge_001_static": static_gap_ok_count,
        "single_name_blocking_static": static_single_name_blocking_count,
        "single_name_blocked_untradeable_static": static_single_name_blocked_untradeable_count,
        "solver_fallback_used_static": static_solver_fallback_count,
        "mean_order_reasonableness_static": mean_order,
        "mean_findings_explainability_static": mean_findings,
        "mean_execution_credibility_static": mean_execution,
    }
    return len(reasons) == 0, reasons, metrics


def main(argv: list[str] | None = None) -> int:
    options = _parse_args(argv if argv is not None else sys.argv[1:])
    RUN_ROOT.mkdir(parents=True, exist_ok=True)
    (RUN_ROOT / "samples").mkdir(parents=True, exist_ok=True)
    (RUN_ROOT / "evaluation").mkdir(parents=True, exist_ok=True)

    scenario_template = yaml.safe_load(SCENARIO_TEMPLATE_PATH.read_text(encoding="utf-8"))
    scenario_defs = scenario_template["scenarios"]
    if options.market == "us":
        scenario_defs = deepcopy(scenario_defs)
        for scenario in scenario_defs:
            scenario["constraints"] = str(ROOT / "config" / "constraints" / "us_public_fund.yaml")

    reports: list[dict[str, Any]] = []
    base_config_path = ROOT / ("config/us_default.yaml" if options.market == "us" else "config/default.yaml")
    config_path = _build_runtime_config_path(
        base_config_path=base_config_path,
        config_overlay_path=options.config_overlay,
        run_root=RUN_ROOT,
    )
    constraints_path = ROOT / (
        "config/constraints/us_public_fund.yaml"
        if options.market == "us"
        else "config/constraints/public_fund.yaml"
    )
    real_provider_name = "alpaca" if options.market == "us" else "tushare"
    real_sample_source_dir = (
        US_REPLAY_ROOT / "sample_us_01" if options.market == "us" else ROOT / "data" / "sample"
    )
    real_holdings_filename = "holdings.csv" if options.market == "us" else "holdings_example.csv"
    real_overlay_filename = "reference.csv" if options.market == "us" else "reference_overlay_example.csv"

    credential_source = "missing"
    provider_creds_available = False
    if options.market == "us":
        resolved_api_key, resolved_secret_key, resolved_source = resolve_alpaca_credentials()
        provider_creds_available = bool(resolved_api_key and resolved_secret_key)
        credential_source = resolved_source or "missing"
    else:
        resolved_tushare_token, resolved_tushare_token_source = resolve_tushare_token()
        if resolved_tushare_token and not str(os.getenv("TUSHARE_TOKEN", "")).strip():
            os.environ["TUSHARE_TOKEN"] = resolved_tushare_token
        provider_creds_available = bool(resolved_tushare_token)
        credential_source = resolved_tushare_token_source or "missing"

    real_feed_check: dict[str, Any] = {
        "credentials_available": provider_creds_available,
        "credential_source": credential_source,
        "token_available": provider_creds_available,
        "token_source": credential_source,
        "requested_date": options.real_feed_as_of_date,
        "effective_trade_date": options.real_feed_as_of_date,
        "rollback_applied": False,
        "rollback_attempted_dates": [],
        "rollback_error": None,
        "market_builder_success": False,
        "reference_builder_success": False,
        "market_manifest": {},
        "reference_manifest": {},
        "provider_capability_blockers": [],
    }

    sample_defs = _build_sample_list(options, provider_creds_available=provider_creds_available)
    reviewer_rows, reviewer_source = _load_reviewer_scores(options.reviewer_input)
    reviewer_template_path = RUN_ROOT / "evaluation" / "reviewer_template.csv"
    _write_reviewer_template(reviewer_template_path, [sample["sample_id"] for sample in sample_defs])
    reviewer_complete, reviewer_missing_fields = _has_complete_reviewer_scores(
        sample_ids=[sample["sample_id"] for sample in sample_defs],
        reviewer_rows=reviewer_rows,
    )

    if provider_creds_available:
        real_tickers = real_sample_source_dir / "tickers.txt"
        real_market_output = RUN_ROOT / "real_feed_check" / "market_real.csv"
        real_reference_output = RUN_ROOT / "real_feed_check" / "reference_real.csv"
        real_logs_dir = RUN_ROOT / "real_feed_check" / "logs"
        requested_real_date = options.real_feed_as_of_date
        real_ticker_list = sorted(_load_csv_tickers(real_sample_source_dir / real_holdings_filename))
        (
            effective_real_date,
            rollback_attempted_dates,
            rollback_error,
        ) = _resolve_real_trade_date(
            requested_date=requested_real_date,
            tickers=real_ticker_list,
            provider_name=real_provider_name,
        )
        real_feed_check["requested_date"] = requested_real_date
        real_feed_check["effective_trade_date"] = effective_real_date
        real_feed_check["rollback_applied"] = requested_real_date != effective_real_date
        real_feed_check["rollback_attempted_dates"] = rollback_attempted_dates
        real_feed_check["rollback_error"] = rollback_error

        market_real_result = _run_command(
            "real_build_market",
            [
                _cli_executable("portfolio-os-build-market"),
                "--tickers-file",
                str(real_tickers),
                "--as-of-date",
                effective_real_date,
                "--provider",
                real_provider_name,
                "--output",
                str(real_market_output),
            ],
            real_logs_dir / "01_real_build_market.log",
        )
        reference_real_result = _run_command(
            "real_build_reference",
            [
                _cli_executable("portfolio-os-build-reference"),
                "--tickers-file",
                str(real_tickers),
                "--as-of-date",
                effective_real_date,
                "--provider",
                real_provider_name,
                "--overlay",
                str(real_sample_source_dir / real_overlay_filename),
                "--output",
                str(real_reference_output),
            ],
            real_logs_dir / "02_real_build_reference.log",
        )
        real_feed_check["market_builder_success"] = market_real_result.ok
        real_feed_check["reference_builder_success"] = reference_real_result.ok
        market_manifest = real_market_output.parent / "market_real_manifest.json"
        reference_manifest = real_reference_output.parent / "reference_real_manifest.json"
        if market_manifest.exists():
            real_feed_check["market_manifest"] = _load_json(market_manifest)
        if reference_manifest.exists():
            real_feed_check["reference_manifest"] = _load_json(reference_manifest)
        real_feed_check["provider_capability_blockers"] = _collect_capability_blockers(
            market_manifest=real_feed_check.get("market_manifest"),
            reference_manifest=real_feed_check.get("reference_manifest"),
        )
    elif options.include_real_sample:
        if options.market == "us":
            real_feed_check["provider_capability_blockers"] = ["credentials_missing:ALPACA_API_KEY/ALPACA_SECRET_KEY"]
        else:
            real_feed_check["provider_capability_blockers"] = ["token_missing:TUSHARE_TOKEN"]

    for sample in sample_defs:
        sample_id = sample["sample_id"]
        source_dir = Path(sample["source_dir"])
        sample_run_dir = RUN_ROOT / "samples" / sample_id
        inputs_dir = sample_run_dir / "inputs"
        logs_dir = sample_run_dir / "logs"
        inputs_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        holdings_path = source_dir / str(sample["holdings_file"])
        target_path = source_dir / str(sample["target_file"])
        state_path = source_dir / str(sample["portfolio_state_file"])
        overlay_path = source_dir / str(sample["reference_overlay_file"])
        source_market = source_dir / str(sample["market_fallback_file"])
        source_reference = source_dir / str(sample["reference_fallback_file"])
        build_provider = str(sample.get("build_provider", "mock"))
        allow_main_fallback = bool(sample.get("allow_main_fallback", True))

        tickers = sorted(_load_csv_tickers(holdings_path) | _load_csv_tickers(target_path))
        tickers_path = inputs_dir / "tickers.txt"
        tickers_path.write_text("\n".join(tickers) + "\n", encoding="utf-8")
        requested_date = str(sample.get("requested_as_of_date") or _extract_as_of_date(state_path))
        effective_trade_date = requested_date
        if sample_id.startswith("real_sample_") and real_feed_check.get("effective_trade_date"):
            effective_trade_date = str(real_feed_check["effective_trade_date"])
        as_of_date = effective_trade_date

        generated_market = inputs_dir / "market.csv"
        generated_reference = inputs_dir / "reference.csv"
        cmd_results: dict[str, CommandResult] = {}
        market_manifest_path = generated_market.parent / "market_manifest.json"
        reference_manifest_path = generated_reference.parent / "reference_manifest.json"
        reused_from_real_precheck = False
        if sample_id.startswith("real_sample_"):
            real_market_manifest_payload = dict(real_feed_check.get("market_manifest") or {})
            real_reference_manifest_payload = dict(real_feed_check.get("reference_manifest") or {})
            real_market_source = Path(str(real_market_manifest_payload.get("output_path", "")).strip())
            real_reference_source = Path(str(real_reference_manifest_payload.get("output_path", "")).strip())
            real_market_manifest_source = RUN_ROOT / "real_feed_check" / "market_real_manifest.json"
            real_reference_manifest_source = RUN_ROOT / "real_feed_check" / "reference_real_manifest.json"
            reuse_ready = (
                bool(real_feed_check.get("market_builder_success"))
                and bool(real_feed_check.get("reference_builder_success"))
                and not bool(real_feed_check.get("provider_capability_blockers"))
                and real_market_source.exists()
                and real_reference_source.exists()
                and real_market_manifest_source.exists()
                and real_reference_manifest_source.exists()
            )
            if reuse_ready:
                shutil.copy2(real_market_source, generated_market)
                shutil.copy2(real_reference_source, generated_reference)
                market_manifest_payload = dict(real_market_manifest_payload)
                reference_manifest_payload = dict(real_reference_manifest_payload)
                market_manifest_payload["output_path"] = str(generated_market)
                reference_manifest_payload["output_path"] = str(generated_reference)
                market_manifest_path.write_text(
                    json.dumps(market_manifest_payload, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                reference_manifest_path.write_text(
                    json.dumps(reference_manifest_payload, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                cmd_results["build_market"] = _build_reused_success_result(
                    "build_market",
                    logs_dir / "01_build_market.log",
                    source_csv=real_market_source,
                    source_manifest=real_market_manifest_source,
                )
                cmd_results["build_reference"] = _build_reused_success_result(
                    "build_reference",
                    logs_dir / "02_build_reference.log",
                    source_csv=real_reference_source,
                    source_manifest=real_reference_manifest_source,
                )
                reused_from_real_precheck = True
        if not reused_from_real_precheck:
            cmd_results["build_market"] = _run_command(
                "build_market",
                [
                    _cli_executable("portfolio-os-build-market"),
                    "--tickers-file",
                    str(tickers_path),
                    "--as-of-date",
                    as_of_date,
                    "--provider",
                    build_provider,
                    "--output",
                    str(generated_market),
                ],
                logs_dir / "01_build_market.log",
            )
            cmd_results["build_reference"] = _run_command(
                "build_reference",
                [
                    _cli_executable("portfolio-os-build-reference"),
                    "--tickers-file",
                    str(tickers_path),
                    "--as-of-date",
                    as_of_date,
                    "--provider",
                    build_provider,
                    "--overlay",
                    str(overlay_path),
                    "--output",
                    str(generated_reference),
                ],
                logs_dir / "02_build_reference.log",
            )
        market_manifest_payload = _load_json(market_manifest_path) if market_manifest_path.exists() else {}
        reference_manifest_payload = _load_json(reference_manifest_path) if reference_manifest_path.exists() else {}

        market_precheck_ok, market_precheck_message = _validate_snapshot_csv(
            path=generated_market,
            expected_tickers=set(tickers),
            required_columns=MARKET_REQUIRED_COLUMNS,
        )
        reference_precheck_ok, reference_precheck_message = _validate_snapshot_csv(
            path=generated_reference,
            expected_tickers=set(tickers),
            required_columns=REFERENCE_REQUIRED_COLUMNS,
        )
        precheck_ok = market_precheck_ok and reference_precheck_ok

        main_output_dir = sample_run_dir / "main"
        if options.mode == "release" and (
            (not cmd_results["build_market"].ok)
            or (not cmd_results["build_reference"].ok)
            or (not precheck_ok)
        ):
            cmd_results["main_primary"] = _build_skipped_result(
                "main",
                logs_dir / "03_main.log",
                (
                    "main skipped in release mode because builder/precheck failed: "
                    f"market_ok={cmd_results['build_market'].ok}, "
                    f"reference_ok={cmd_results['build_reference'].ok}, "
                    f"market_precheck={market_precheck_message}, "
                    f"reference_precheck={reference_precheck_message}"
                ),
            )
        else:
            cmd_results["main_primary"] = _run_command(
                "main",
                [
                    _cli_executable("portfolio-os"),
                    "--holdings",
                    str(holdings_path),
                    "--target",
                    str(target_path),
                    "--market",
                    str(generated_market),
                    "--reference",
                    str(generated_reference),
                    "--portfolio-state",
                    str(state_path),
                    "--constraints",
                    str(constraints_path),
                    "--config",
                    str(config_path),
                    "--execution-profile",
                    str(ROOT / "config" / "execution" / "conservative.yaml"),
                    "--output-dir",
                    str(main_output_dir),
                ],
                logs_dir / "03_main.log",
            )
        main_market_path = generated_market
        main_reference_path = generated_reference
        main_used_fallback = False
        if (
            options.mode == "nightly"
            and (not cmd_results["main_primary"].ok)
            and allow_main_fallback
        ):
            cmd_results["main_fallback"] = _run_command(
                "main_fallback",
                [
                    _cli_executable("portfolio-os"),
                    "--holdings",
                    str(holdings_path),
                    "--target",
                    str(target_path),
                    "--market",
                    str(source_market),
                    "--reference",
                    str(source_reference),
                    "--portfolio-state",
                    str(state_path),
                    "--constraints",
                    str(constraints_path),
                    "--config",
                    str(config_path),
                    "--execution-profile",
                    str(ROOT / "config" / "execution" / "conservative.yaml"),
                    "--output-dir",
                    str(main_output_dir),
                ],
                logs_dir / "03_main_fallback.log",
            )
            if cmd_results["main_fallback"].ok:
                main_market_path = source_market
                main_reference_path = source_reference
                main_used_fallback = True

        scenario_manifest_path = inputs_dir / "scenario_manifest.yaml"
        sample_scenario_defs = deepcopy(scenario_defs)
        if options.market == "us":
            sample_scenario_defs = _apply_us_scenario_feasibility_guards(
                sample_scenario_defs,
                portfolio_state_path=state_path,
            )
        _write_yaml(
            scenario_manifest_path,
            _build_scenario_manifest(
                base_inputs={
                    "holdings": str(holdings_path),
                    "target": str(target_path),
                    "market": str(main_market_path),
                    "reference": str(main_reference_path),
                    "portfolio_state": str(state_path),
                    "config": str(config_path),
                },
                source_scenarios=sample_scenario_defs,
            ),
        )

        scenario_output_dir = sample_run_dir / "scenario"
        if options.mode == "release" and not (
            cmd_results["main_primary"].ok or ("main_fallback" in cmd_results and cmd_results["main_fallback"].ok)
        ):
            cmd_results["scenario"] = _build_skipped_result(
                "scenario",
                logs_dir / "04_scenario.log",
                "scenario skipped in release mode because main failed.",
            )
        else:
            cmd_results["scenario"] = _run_command(
                "scenario",
                [
                    _cli_executable("portfolio-os-scenarios"),
                    "--manifest",
                    str(scenario_manifest_path),
                    "--output-dir",
                    str(scenario_output_dir),
                ],
                logs_dir / "04_scenario.log",
            )

        scenario_json_path = scenario_output_dir / "scenario_comparison.json"
        selected_scenario = "public_conservative"
        scenario_score_gap = 0.0
        selected_scenario_blocking_count = 0
        scenario_widened = False
        if cmd_results["scenario"].ok and scenario_json_path.exists():
            scenario_payload = _load_json(scenario_json_path)
            (
                selected_scenario,
                scenario_score_gap,
                selected_scenario_blocking_count,
            ) = _extract_selected_scenario(scenario_payload=scenario_payload)
            if scenario_score_gap < 0.01:
                scenario_widened = True
                widened_manifest_path = inputs_dir / "scenario_manifest_widened.yaml"
                _write_yaml(
                    widened_manifest_path,
                    _build_scenario_manifest(
                        base_inputs={
                            "holdings": str(holdings_path),
                            "target": str(target_path),
                            "market": str(main_market_path),
                            "reference": str(main_reference_path),
                            "portfolio_state": str(state_path),
                            "config": str(config_path),
                        },
                        source_scenarios=(
                            _apply_us_scenario_feasibility_guards(
                                _build_widened_scenarios(sample_scenario_defs),
                                portfolio_state_path=state_path,
                            )
                            if options.market == "us"
                            else _build_widened_scenarios(sample_scenario_defs)
                        ),
                    ),
                )
                scenario_widened_result = _run_command(
                    "scenario_widened",
                    [
                        _cli_executable("portfolio-os-scenarios"),
                        "--manifest",
                        str(widened_manifest_path),
                        "--output-dir",
                        str(scenario_output_dir),
                    ],
                    logs_dir / "04_scenario_widened.log",
                )
                cmd_results["scenario_widened"] = scenario_widened_result
                if scenario_widened_result.ok and scenario_json_path.exists():
                    scenario_payload = _load_json(scenario_json_path)
                    (
                        selected_scenario,
                        scenario_score_gap,
                        selected_scenario_blocking_count,
                    ) = _extract_selected_scenario(scenario_payload=scenario_payload)

        override_auto_generated = selected_scenario_blocking_count > 0 and cmd_results["scenario"].ok
        approval_request_path = inputs_dir / "approval_request.yaml"
        approval_output_dir = sample_run_dir / "approval"
        if options.mode == "release" and not cmd_results["scenario"].ok:
            cmd_results["approval"] = _build_skipped_result(
                "approval",
                logs_dir / "05_approval.log",
                "approval skipped in release mode because scenario step failed.",
            )
        else:
            _write_yaml(
                approval_request_path,
                _build_approval_request(
                    scenario_output_dir=scenario_output_dir,
                    selected_scenario=selected_scenario,
                    sample_id=sample_id,
                    use_override=override_auto_generated,
                ),
            )
            cmd_results["approval"] = _run_command(
                "approval",
                [
                    _cli_executable("portfolio-os-approve"),
                    "--request",
                    str(approval_request_path),
                    "--output-dir",
                    str(approval_output_dir),
                ],
                logs_dir / "05_approval.log",
            )

        execution_used_probe = False
        execution_request_path = inputs_dir / "execution_request.yaml"
        execution_output_dir = sample_run_dir / "execution"
        if options.mode == "release" and not cmd_results["approval"].ok:
            cmd_results["execution"] = _build_skipped_result(
                "execution",
                logs_dir / "06_execution.log",
                "execution skipped in release mode because approval step failed.",
            )
        else:
            _write_yaml(
                execution_request_path,
                _build_execution_request(
                    artifact_dir=approval_output_dir,
                    input_orders="final_orders_oms.csv",
                    portfolio_state_path=state_path,
                    sample_id=sample_id,
                    low_liquidity_stress=sample_id in {"sample_05", "sample_us_03", "real_sample_01"},
                    market_path=main_market_path,
                    audit_path="final_audit.json",
                ),
            )
            cmd_results["execution"] = _run_command(
                "execution",
                [
                    _cli_executable("portfolio-os-execute"),
                    "--request",
                    str(execution_request_path),
                    "--calibration-profile",
                    str(ROOT / "config" / "calibration_profiles" / "balanced_day.yaml"),
                    "--output-dir",
                    str(execution_output_dir),
                ],
                logs_dir / "06_execution.log",
            )
        if options.mode == "nightly" and (not cmd_results["execution"].ok) and (not cmd_results["approval"].ok):
            execution_used_probe = True
            execution_probe_request_path = inputs_dir / "execution_request_probe.yaml"
            scenario_selected_dir = scenario_output_dir / "scenario_results" / selected_scenario
            _write_yaml(
                execution_probe_request_path,
                _build_execution_request(
                    artifact_dir=scenario_selected_dir,
                    input_orders="orders_oms.csv",
                    portfolio_state_path=state_path,
                    sample_id=f"{sample_id}_probe",
                    low_liquidity_stress=sample_id in {"sample_05", "sample_us_03", "real_sample_01"},
                    market_path=main_market_path,
                    audit_path="audit.json",
                ),
            )
            cmd_results["execution"] = _run_command(
                "execution_probe",
                [
                    _cli_executable("portfolio-os-execute"),
                    "--request",
                    str(execution_probe_request_path),
                    "--calibration-profile",
                    str(ROOT / "config" / "calibration_profiles" / "balanced_day.yaml"),
                    "--output-dir",
                    str(execution_output_dir),
                ],
                logs_dir / "06_execution_probe.log",
            )

        benchmark_path = main_output_dir / "benchmark_comparison.json"
        execution_report_path = execution_output_dir / "execution_report.json"
        approval_record_path = approval_output_dir / "approval_record.json"
        scenario_comparison_path = scenario_output_dir / "scenario_comparison.json"
        main_audit_path = main_output_dir / "audit.json"

        naive_cost = 0.0
        portfolio_cost = 0.0
        naive_turnover = 0.0
        portfolio_turnover = 0.0
        blocked_diff = 0.0
        target_delta_vs_naive = 0.0
        finding_count = 0
        blocked_count = 0
        single_name_blocking_count = 0
        single_name_blocked_untradeable_count = 0
        solver_used = ""
        solver_fallback_used = False
        constraint_residual_max = 0.0
        fill_rate = 0.0
        partial_count = 0
        unfilled_count = 0
        stress_partial_count = 0
        stress_unfilled_count = 0
        stress_unfilled_notional = 0.0
        scenario_spread = 0.0
        selected_differs = False
        approval_status = "unknown"
        approval_ok = False
        override_used = False

        if benchmark_path.exists():
            benchmark_payload = _load_json(benchmark_path)
            by_name = {item["strategy_name"]: item for item in benchmark_payload["strategies"]}
            naive = by_name["naive_target_rebalance"]
            portfolio = by_name["portfolio_os_rebalance"]
            naive_cost = float(naive["estimated_total_cost"])
            portfolio_cost = float(portfolio["estimated_total_cost"])
            naive_turnover = float(naive["turnover"])
            portfolio_turnover = float(portfolio["turnover"])
            blocked_diff = float(benchmark_payload["comparison_summary"]["blocked_trade_reduction_vs_naive"])
            target_delta_vs_naive = float(benchmark_payload["comparison_summary"]["target_deviation_delta_vs_naive"])

        if execution_report_path.exists():
            execution_payload = _load_json(execution_report_path)
            summary = execution_payload["portfolio_summary"]
            fill_rate = float(summary["fill_rate"])
            partial_count = int(summary["partial_fill_count"])
            unfilled_count = int(summary["unfilled_order_count"])
            stress_summary = execution_payload.get("stress_test", {}).get("portfolio_summary") or {}
            stress_partial_count = int(stress_summary.get("partial_fill_count", 0))
            stress_unfilled_count = int(stress_summary.get("unfilled_order_count", 0))
            stress_unfilled_notional = float(stress_summary.get("total_unfilled_notional", 0.0))

        if scenario_comparison_path.exists():
            scenario_payload = _load_json(scenario_comparison_path)
            scenario_costs = [float(item["estimated_total_cost"]) for item in scenario_payload["scenarios"]]
            if scenario_costs:
                scenario_spread = max(scenario_costs) - min(scenario_costs)

        if approval_record_path.exists():
            approval_payload = _load_json(approval_record_path)
            selected_differs = bool(approval_payload.get("selected_differs_from_recommended", False))
            approval_status = str(approval_payload.get("approval_status", "unknown"))
            approval_ok = approval_status in {"approved", "approved_with_override"}
            override_used = bool(approval_payload.get("override_used", False))

        if main_audit_path.exists():
            audit_payload = _load_json(main_audit_path)
            findings = audit_payload.get("findings", [])
            finding_count = len(findings)
            blocked_count = sum(1 for item in findings if bool(item.get("blocking", False)))
            single_name_blocking_count = sum(
                1
                for item in findings
                if str(item.get("code", "")) == "single_name_limit"
                and bool(item.get("blocking", False))
            )
            single_name_blocked_untradeable_count = sum(
                1
                for item in findings
                if str(item.get("code", "")) == "single_name_limit"
                and str((item.get("details") or {}).get("disposition", "")) == "blocked_untradeable"
            )
            optimization_metadata = audit_payload.get("optimization_metadata", {}) or {}
            solver_used = str(optimization_metadata.get("solver_used", ""))
            solver_fallback_used = bool(optimization_metadata.get("solver_fallback_used", False))
            constraint_residual_max = float(optimization_metadata.get("constraint_residual_max", 0.0) or 0.0)

        reviewer_row = reviewer_rows.get(sample_id)
        if reviewer_row is not None:
            order_score = _get_score_from_reviewer(reviewer_row, "order_reasonableness_score")
            findings_score = _get_score_from_reviewer(reviewer_row, "findings_explainability_score")
            scenario_score = _get_score_from_reviewer(reviewer_row, "scenario_tradeoff_score")
            approval_score = _get_score_from_reviewer(reviewer_row, "approval_handoff_score")
            execution_score = _get_score_from_reviewer(reviewer_row, "execution_credibility_score")
            reviewer_complete_row = all(
                score is not None
                for score in [order_score, findings_score, scenario_score, approval_score, execution_score]
            )
            if not reviewer_complete_row:
                order_score = _score_orders(naive_cost - portfolio_cost, blocked_diff, target_delta_vs_naive)
                findings_score = _score_findings(finding_count, blocked_count)
                scenario_score = _score_scenario(scenario_spread)
                approval_score = _score_approval(approval_status, selected_differs)
                execution_score = _score_execution(fill_rate, partial_count, unfilled_count)
                subjective_score_source = "mixed_reviewer_and_heuristic"
            else:
                subjective_score_source = reviewer_source
        else:
            order_score = _score_orders(naive_cost - portfolio_cost, blocked_diff, target_delta_vs_naive)
            findings_score = _score_findings(finding_count, blocked_count)
            scenario_score = _score_scenario(scenario_spread)
            approval_score = _score_approval(approval_status, selected_differs)
            execution_score = _score_execution(fill_rate, partial_count, unfilled_count)
            subjective_score_source = "heuristic"

        main_success = cmd_results["main_primary"].ok or (
            "main_fallback" in cmd_results and cmd_results["main_fallback"].ok
        )
        all_chain_ok = (
            cmd_results["build_market"].ok
            and cmd_results["build_reference"].ok
            and main_success
            and cmd_results["scenario"].ok
            and approval_ok
            and cmd_results["execution"].ok
        )
        cost_diff = naive_cost - portfolio_cost
        verdict = _sample_conclusion(all_chain_ok, cost_diff, fill_rate)

        if not cmd_results["build_market"].ok or not cmd_results["build_reference"].ok:
            biggest_issue = "builder step stability blocks full-chain confidence"
            one_fix = "improve provider/runtime error handling and retries"
        elif main_used_fallback:
            biggest_issue = "builder-generated inputs caused infeasible optimization for this sample"
            one_fix = "align builder market assumptions with tight-cash optimization constraints"
        elif not precheck_ok:
            biggest_issue = "input precheck failed before optimization"
            one_fix = "repair market/reference schema coverage before main/scenario runs"
        elif stress_unfilled_notional <= 0 and stress_partial_count == 0 and sample_id in {
            "sample_05",
            "sample_us_03",
            "real_sample_01",
        }:
            biggest_issue = "execution stress lacks visible residual-risk separation"
            one_fix = "calibrate stricter stressed liquidity assumptions and volume shock"
        elif scenario_score_gap < 0.01:
            biggest_issue = "scenario differentiation is weak for this snapshot"
            one_fix = "increase scenario parameter spread to improve decision contrast"
        else:
            biggest_issue = "manual review still required for final PM acceptance"
            one_fix = "use reviewer template and strict_gate mode for final sign-off"

        report = {
            "sample_id": sample_id,
            "date": as_of_date,
            "requested_date": requested_date,
            "effective_trade_date": effective_trade_date,
            "account_type": sample["account_type"],
            "sample_feature": sample["sample_feature"],
            "data_path": str(source_dir),
            "provider_type": sample["provider_type"],
            "market_builder_success": cmd_results["build_market"].ok,
            "reference_builder_success": cmd_results["build_reference"].ok,
            "main_flow_success": main_success,
            "scenario_success": cmd_results["scenario"].ok,
            "approval_success": approval_ok,
            "execution_success": cmd_results["execution"].ok,
            "approval_status": approval_status,
            "override_used": override_used,
            "override_auto_generated": override_auto_generated,
            "process_state_market_builder": (
                "failed"
                if not cmd_results["build_market"].ok
                else (
                    "degraded"
                    if "degradation" in str(market_manifest_payload.get("build_status", "")).lower()
                    or str(market_manifest_payload.get("provider_capability_status", "")).lower() == "degraded"
                    else "success"
                )
            ),
            "process_state_reference_builder": (
                "failed"
                if not cmd_results["build_reference"].ok
                else (
                    "degraded"
                    if "degradation" in str(reference_manifest_payload.get("build_status", "")).lower()
                    or str(reference_manifest_payload.get("provider_capability_status", "")).lower() == "degraded"
                    else "success"
                )
            ),
            "process_state_main": (
                "degraded"
                if main_used_fallback
                else (
                    "skipped"
                    if _is_skipped_result(cmd_results["main_primary"])
                    else ("success" if main_success else "failed")
                )
            ),
            "process_state_scenario": (
                "skipped"
                if _is_skipped_result(cmd_results["scenario"])
                else ("success" if cmd_results["scenario"].ok else "failed")
            ),
            "process_state_approval": (
                "skipped"
                if _is_skipped_result(cmd_results["approval"])
                else ("success" if approval_ok else "failed")
            ),
            "process_state_execution": (
                "degraded"
                if execution_used_probe
                else (
                    "skipped"
                    if _is_skipped_result(cmd_results["execution"])
                    else ("success" if cmd_results["execution"].ok else "failed")
                )
            ),
            "naive_cost": naive_cost,
            "portfolio_os_cost": portfolio_cost,
            "cost_difference": cost_diff,
            "naive_turnover": naive_turnover,
            "portfolio_os_turnover": portfolio_turnover,
            "blocked_trade_difference": blocked_diff,
            "single_name_blocking_count": single_name_blocking_count,
            "single_name_blocked_untradeable_count": single_name_blocked_untradeable_count,
            "fill_rate": fill_rate,
            "partial_fill_count": partial_count,
            "unfilled_count": unfilled_count,
            "stress_partial_fill_count": stress_partial_count,
            "stress_unfilled_count": stress_unfilled_count,
            "stress_total_unfilled_notional": stress_unfilled_notional,
            "scenario_score_gap": scenario_score_gap,
            "order_reasonableness_score": order_score,
            "findings_explainability_score": findings_score,
            "scenario_tradeoff_score": scenario_score,
            "approval_handoff_score": approval_score,
            "execution_credibility_score": execution_score,
            "subjective_score_source": subjective_score_source,
            "reviewer_id": (reviewer_row or {}).get("reviewer_id", ""),
            "reviewer_notes": (reviewer_row or {}).get("notes", ""),
            "verdict": verdict,
            "biggest_issue": biggest_issue,
            "one_thing_to_fix": one_fix,
            "main_used_fallback": main_used_fallback,
            "execution_used_probe": execution_used_probe,
            "scenario_widened_once": scenario_widened,
            "precheck_market_ok": market_precheck_ok,
            "precheck_market_message": market_precheck_message,
            "precheck_reference_ok": reference_precheck_ok,
            "precheck_reference_message": reference_precheck_message,
            "provider_capability_blockers": _collect_capability_blockers(
                market_manifest=market_manifest_payload,
                reference_manifest=reference_manifest_payload,
            ),
            "solver_used": solver_used,
            "solver_fallback_used": solver_fallback_used,
            "constraint_residual_max": constraint_residual_max,
            "data_source_mix_market": list(market_manifest_payload.get("data_source_mix", []) or []),
            "data_source_mix_reference": list(reference_manifest_payload.get("data_source_mix", []) or []),
            "effective_market_path": str(main_market_path),
            "effective_reference_path": str(main_reference_path),
            "benchmark_json_path": str(benchmark_path) if benchmark_path.exists() else "",
            "execution_report_path": str(execution_report_path) if execution_report_path.exists() else "",
            "scenario_comparison_path": str(scenario_comparison_path) if scenario_comparison_path.exists() else "",
            "approval_record_path": str(approval_record_path) if approval_record_path.exists() else "",
            "main_audit_path": str(main_audit_path) if main_audit_path.exists() else "",
            "log_dir": str(logs_dir),
        }
        reports.append(report)
        _build_sample_markdown(RUN_ROOT / "evaluation" / f"{sample_id}_evaluation.md", report)

    csv_path = RUN_ROOT / "evaluation" / "sample_assessment.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(reports[0].keys()))
        writer.writeheader()
        writer.writerows(reports)

    full_chain_success_count = sum(
        1
        for item in reports
        if item["market_builder_success"]
        and item["reference_builder_success"]
        and item["main_flow_success"]
        and item["scenario_success"]
        and item["approval_success"]
        and item["execution_success"]
    )
    cost_better_count = sum(1 for item in reports if item["cost_difference"] > 0)
    cost_better_ratio = cost_better_count / len(reports) if reports else 0.0
    mean_order_score = mean(item["order_reasonableness_score"] for item in reports)
    mean_explain_score = mean(item["findings_explainability_score"] for item in reports)
    mean_execution_score = mean(item["execution_credibility_score"] for item in reports)
    single_name_blocking_total = sum(int(item.get("single_name_blocking_count", 0)) for item in reports)
    single_name_blocked_untradeable_total = sum(
        int(item.get("single_name_blocked_untradeable_count", 0)) for item in reports
    )
    solver_fallback_used_count = sum(1 for item in reports if bool(item.get("solver_fallback_used", False)))
    data_source_mix_market = sorted(
        {
            str(source)
            for item in reports
            for source in (item.get("data_source_mix_market") or [])
            if str(source).strip()
        }
    )
    data_source_mix_reference = sorted(
        {
            str(source)
            for item in reports
            for source in (item.get("data_source_mix_reference") or [])
            if str(source).strip()
        }
    )

    score_mode = "provisional" if options.mode == "nightly" or not reviewer_complete else "gate_qualified"

    release_gate_passed = False
    release_gate_reasons: list[str] = []
    release_gate_metrics: dict[str, Any] = {}
    if options.mode == "release":
        release_gate_passed, release_gate_reasons, release_gate_metrics = _evaluate_release_gate(
            reports=reports,
            options=options,
            reviewer_complete=reviewer_complete,
            real_feed_check=real_feed_check,
        )
        pilot_verdict = "usable" if release_gate_passed else "not_releasable"
    elif score_mode == "provisional":
        pilot_verdict = "provisional"
    elif (
        full_chain_success_count >= 5
        and cost_better_ratio >= 0.7
        and mean_order_score >= 4
        and mean_explain_score >= 4
        and mean_execution_score >= 4
    ):
        pilot_verdict = "usable"
    elif full_chain_success_count >= 3 and cost_better_ratio >= 0.5:
        pilot_verdict = "borderline"
    else:
        pilot_verdict = "not useful"

    top_issues = _collect_dynamic_issues(
        reports=reports,
        real_feed_check=real_feed_check,
        mode=options.mode,
        reviewer_complete=reviewer_complete,
        market=options.market,
    )

    one_line_verdict = (
        "PortfolioOS is currently **not_releasable**: release mode gate is not satisfied."
        if pilot_verdict == "not_releasable"
        else "PortfolioOS is currently **provisional** for pilot use: chain is operational, but strict gate is not satisfied."
        if pilot_verdict == "provisional"
        else f"PortfolioOS is currently **{pilot_verdict}** for design-partner pilot under static cross-sectional workflow conditions."
    )

    capability_report_path = RUN_ROOT / "evaluation" / "provider_capability_report.json"
    capability_report_path.write_text(
        json.dumps(real_feed_check, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    summary_lines = [
        "# Pilot Validation Summary",
        "",
        "## Overall Result",
        f"- pilot_verdict: {pilot_verdict}",
        f"- verdict_mode: {score_mode}",
        f"- mode: {options.mode}",
        f"- market: {options.market}",
        f"- gate_mode: {'strict_gate' if options.mode == 'release' else 'provisional'}",
        f"- base_config_path: {base_config_path}",
        f"- config_overlay_path: {str(options.config_overlay) if options.config_overlay is not None else 'none'}",
        f"- effective_config_path: {config_path}",
        f"- reviewer_input: {str(options.reviewer_input) if options.reviewer_input is not None else 'none'}",
        f"- reviewer_complete: {reviewer_complete}",
        f"- reviewer_missing_fields_count: {len(reviewer_missing_fields)}",
        f"- reviewer_template: {reviewer_template_path}",
        f"- release_gate_passed: {release_gate_passed if options.mode == 'release' else 'N/A'}",
        f"- full_chain_success_count: {full_chain_success_count}/{len(reports)}",
        f"- cost_better_count: {cost_better_count}/{len(reports)}",
        f"- cost_better_ratio: {cost_better_ratio:.2%}",
        f"- mean_order_reasonableness_score: {mean_order_score:.2f}/5",
        f"- mean_findings_explainability_score: {mean_explain_score:.2f}/5",
        f"- mean_execution_credibility_score: {mean_execution_score:.2f}/5",
        f"- override_used_count: {sum(1 for item in reports if item.get('override_used'))}/{len(reports)}",
        f"- single_name_blocking_count_total: {single_name_blocking_total}",
        f"- single_name_blocked_untradeable_count_total: {single_name_blocked_untradeable_total}",
        f"- solver_fallback_used_count: {solver_fallback_used_count}/{len(reports)}",
        f"- data_source_mix_market: {', '.join(data_source_mix_market) or 'none'}",
        f"- data_source_mix_reference: {', '.join(data_source_mix_reference) or 'none'}",
        f"- provider_credentials_available: {real_feed_check['credentials_available']}",
        f"- provider_credential_source: {real_feed_check['credential_source']}",
        f"- real_requested_date: {real_feed_check.get('requested_date')}",
        f"- real_effective_trade_date: {real_feed_check.get('effective_trade_date')}",
        f"- real_date_rollback_applied: {real_feed_check.get('rollback_applied')}",
        f"- real_market_builder_success: {real_feed_check['market_builder_success']}",
        f"- real_reference_builder_success: {real_feed_check['reference_builder_success']}",
        f"- provider_capability_blockers: {', '.join(real_feed_check.get('provider_capability_blockers', [])) or 'none'}",
        f"- real_sample_enabled: {options.include_real_sample}",
        "",
        "## One-line Answer",
        f"- {one_line_verdict}",
        "",
    ]
    if options.mode == "release":
        summary_lines.extend(
            [
                "## Release Gate",
                f"- passed: {release_gate_passed}",
                f"- reasons: {', '.join(release_gate_reasons) if release_gate_reasons else 'none'}",
                f"- metrics: {json.dumps(release_gate_metrics, ensure_ascii=False)}",
                "",
            ]
        )
    summary_lines.append("## Top 5 Issues")
    for issue in top_issues:
        summary_lines.append(f"- {issue}")
    summary_lines.extend(
        [
            "",
            "## Artifacts",
            f"- sample_assessment_csv: {csv_path}",
            f"- sample_pages_dir: {RUN_ROOT / 'evaluation'}",
            f"- sample_run_dir: {RUN_ROOT / 'samples'}",
            f"- provider_capability_report: {capability_report_path}",
            "",
        ]
    )
    (RUN_ROOT / "pilot_validation_summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    print(f"Validation completed. Root: {RUN_ROOT}")
    print(f"Summary: {RUN_ROOT / 'pilot_validation_summary.md'}")
    print(f"Sample table: {csv_path}")
    if options.mode == "release" and not release_gate_passed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
