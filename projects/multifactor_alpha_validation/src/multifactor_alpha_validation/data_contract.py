from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from multifactor_alpha_validation.schema import FactorSpec


@dataclass(frozen=True)
class PITValidationResult:
    factor_id: str
    pit_passed: bool
    reasons: tuple[str, ...]


def validate_pit_contract(spec: FactorSpec) -> PITValidationResult:
    reasons: list[str] = []
    contract = spec.pit_contract
    if not contract.signal_timestamp_rule:
        reasons.append("missing signal timestamp rule")
    if not contract.visibility_timestamp_rule:
        reasons.append("missing visibility timestamp rule")
    if not contract.tradable_timestamp_rule:
        reasons.append("missing tradable timestamp rule")
    if spec.coverage.missing_policy != "explicit_abstain":
        reasons.append("missing coverage is not explicit abstain")
    if spec.data_tier == "tier_2_fundamental" and contract.reporting_lag_days < 45:
        reasons.append("fundamental reporting lag is too short")
    if spec.factor_id == "analyst_revision_disabled" and spec.status != "disabled":
        reasons.append("analyst revision is enabled without PIT source")
    return PITValidationResult(
        factor_id=spec.factor_id,
        pit_passed=not reasons,
        reasons=tuple(reasons),
    )


@dataclass(frozen=True)
class ResearchModePreflightResult:
    research_mode_ready: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    rows_checked: dict[str, int]
    non_claims: dict[str, bool]
    outputs: dict[str, str]


def run_research_mode_preflight(
    manifest: Mapping[str, Any] | str | Path,
    output_dir: Path,
) -> ResearchModePreflightResult:
    payload, manifest_base = _load_manifest(manifest)
    output_dir.mkdir(parents=True, exist_ok=True)

    blockers: list[str] = []
    warnings: list[str] = []
    rows_checked: dict[str, int] = {}

    if payload.get("schema_version") != "research_mode_dataset_manifest.v1":
        blockers.append("research mode dataset manifest schema is invalid")
    if payload.get("mode") != "research_mode":
        blockers.append("manifest mode must be research_mode")

    universe = _section(payload, "universe")
    prices = _section(payload, "prices")
    benchmark = _section(payload, "benchmark")
    delisting = _section(payload, "delisting")
    timestamp_policy = _section(payload, "timestamp_policy")
    non_claims = _non_claims(payload)

    rows_checked["historical_universe"] = _validate_universe(
        universe,
        manifest_base,
        blockers,
        warnings,
    )
    rows_checked["prices"] = _validate_csv_section(
        prices,
        manifest_base,
        {"date", "ticker", "adjusted_close", "volume"},
        "price panel",
        blockers,
    )
    if prices.get("adjusted") is not True:
        blockers.append("price panel must use adjusted prices")

    benchmark_columns = _read_csv_columns(_resolve_path(benchmark.get("path"), manifest_base), blockers, "benchmark")
    if benchmark_columns:
        rows_checked["benchmark"] = _count_rows(_resolve_path(benchmark.get("path"), manifest_base))
        if "date" not in benchmark_columns or "adjusted_close" not in benchmark_columns:
            blockers.append("benchmark must include date and adjusted_close")
        if not ({"benchmark", "ticker"} & benchmark_columns):
            blockers.append("benchmark must identify QQQ by benchmark or ticker column")
    else:
        rows_checked["benchmark"] = 0
    if benchmark.get("benchmark_id") != "QQQ":
        blockers.append("QQQ benchmark is required")

    rows_checked["delistings"] = _validate_delisting(delisting, manifest_base, blockers)
    _validate_timestamp_policy(timestamp_policy, blockers)
    _validate_non_claims(non_claims, blockers)

    if not blockers and rows_checked["historical_universe"] == 0:
        blockers.append("historical universe membership is required")

    result = ResearchModePreflightResult(
        research_mode_ready=not blockers,
        blockers=tuple(dict.fromkeys(blockers)),
        warnings=tuple(dict.fromkeys(warnings)),
        rows_checked=rows_checked,
        non_claims=non_claims,
        outputs={
            "pit_contract_validation": str(output_dir / "pit_contract_validation.json"),
            "pit_universe_report": str(output_dir / "pit_universe_report.csv"),
            "research_mode_readiness": str(output_dir / "research_mode_readiness.md"),
        },
    )
    _write_preflight_outputs(result, payload, output_dir)
    return result


def _load_manifest(manifest: Mapping[str, Any] | str | Path) -> tuple[dict[str, Any], Path | None]:
    if isinstance(manifest, Mapping):
        return dict(manifest), None
    path = Path(manifest)
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        payload = json.loads(text)
    else:
        import yaml

        payload = yaml.safe_load(text) or {}
    if not isinstance(payload, dict):
        raise ValueError("research mode manifest must be a mapping")
    return payload, path.parent


def _section(payload: Mapping[str, Any], name: str) -> dict[str, Any]:
    section = payload.get(name)
    if isinstance(section, Mapping):
        return dict(section)
    return {}


def _non_claims(payload: Mapping[str, Any]) -> dict[str, bool]:
    defaults = {
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
    }
    supplied = payload.get("non_claims")
    if isinstance(supplied, Mapping):
        defaults.update({key: bool(supplied.get(key, defaults[key])) for key in defaults})
    return defaults


def _resolve_path(value: object, manifest_base: Path | None) -> Path | None:
    if not value:
        return None
    path = Path(str(value))
    if path.is_absolute() or manifest_base is None:
        return path
    return manifest_base / path


def _validate_universe(
    universe: Mapping[str, Any],
    manifest_base: Path | None,
    blockers: list[str],
    warnings: list[str],
) -> int:
    if universe.get("constituent_mode") != "historical_membership":
        blockers.append("historical universe membership is required")
    if universe.get("constituent_mode") == "current_constituents":
        blockers.append("current constituents are survivorship-biased")
    if universe.get("source_is_pit") is not True:
        blockers.append("historical universe source must be PIT-certified")
    if str(universe.get("source", "")).lower() == "yfinance":
        blockers.append("yfinance cannot certify PIT historical index membership")

    path = _resolve_path(universe.get("path"), manifest_base)
    columns = _read_csv_columns(path, blockers, "historical universe")
    if not columns:
        return 0
    required = {"ticker", "membership_start", "membership_end", "as_of_timestamp", "source", "source_is_pit"}
    missing = sorted(required - columns)
    if missing:
        blockers.append(f"historical universe missing columns: {', '.join(missing)}")
    row_count = _count_rows(path)
    if row_count == 0:
        blockers.append("historical universe membership is required")
    if row_count < 100:
        warnings.append("historical universe has fewer than 100 rows; verify coverage before validation")
    return row_count


def _validate_csv_section(
    section: Mapping[str, Any],
    manifest_base: Path | None,
    required: set[str],
    label: str,
    blockers: list[str],
) -> int:
    path = _resolve_path(section.get("path"), manifest_base)
    columns = _read_csv_columns(path, blockers, label)
    if not columns:
        return 0
    missing = sorted(required - columns)
    if missing:
        blockers.append(f"{label} missing columns: {', '.join(missing)}")
    return _count_rows(path)


def _validate_delisting(
    delisting: Mapping[str, Any],
    manifest_base: Path | None,
    blockers: list[str],
) -> int:
    if delisting.get("handling") != "explicit_file":
        blockers.append("delisting handling must be explicit before research mode")
        return 0
    return _validate_csv_section(
        delisting,
        manifest_base,
        {"ticker", "delisting_date", "delisting_return"},
        "delisting panel",
        blockers,
    )


def _validate_timestamp_policy(timestamp_policy: Mapping[str, Any], blockers: list[str]) -> None:
    if timestamp_policy.get("allow_same_close_trading") is not False:
        blockers.append("same-close trading is not allowed")
    if str(timestamp_policy.get("tradable", "")).lower() == "same_close":
        blockers.append("same-close trading is not allowed")
    if not timestamp_policy.get("signal"):
        blockers.append("signal timestamp policy is required")
    if not timestamp_policy.get("visibility"):
        blockers.append("visibility timestamp policy is required")
    if not timestamp_policy.get("tradable"):
        blockers.append("tradable timestamp policy is required")


def _validate_non_claims(non_claims: Mapping[str, bool], blockers: list[str]) -> None:
    for key, value in non_claims.items():
        if value:
            blockers.append(f"non-claim flag must remain false: {key}")


def _read_csv_columns(path: Path | None, blockers: list[str], label: str) -> set[str]:
    if path is None:
        blockers.append(f"{label} path is required")
        return set()
    if not path.exists():
        blockers.append(f"{label} path does not exist")
        return set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return set(reader.fieldnames or [])


def _count_rows(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return sum(1 for _ in reader)


def _write_preflight_outputs(
    result: ResearchModePreflightResult,
    payload: Mapping[str, Any],
    output_dir: Path,
) -> None:
    report = {
        "schema_version": "research_mode_preflight.v1",
        "research_mode_ready": result.research_mode_ready,
        "blockers": list(result.blockers),
        "warnings": list(result.warnings),
        "rows_checked": result.rows_checked,
        "non_claims": result.non_claims,
        "mode": payload.get("mode"),
        "universe_source": _section(payload, "universe").get("source"),
        "benchmark_id": _section(payload, "benchmark").get("benchmark_id"),
        "not_alpha_evidence": True,
    }
    (output_dir / "pit_contract_validation.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "pit_universe_report.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["artifact", "row_count", "source", "ready", "blocker_count"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "artifact": "historical_universe",
                "row_count": result.rows_checked.get("historical_universe", 0),
                "source": _section(payload, "universe").get("source", ""),
                "ready": result.research_mode_ready,
                "blocker_count": len(result.blockers),
            }
        )
    (output_dir / "research_mode_readiness.md").write_text(_readiness_markdown(result), encoding="utf-8")


def _readiness_markdown(result: ResearchModePreflightResult) -> str:
    status = "ready" if result.research_mode_ready else "blocked"
    lines = [
        "# Research Mode Readiness",
        "",
        f"Status: `{status}`",
        "",
        "This preflight is not alpha evidence and does not approve production, live trading, orders, or direct Q2 entry.",
        "",
    ]
    if result.research_mode_ready:
        lines.append("Research mode is ready for PIT-safe validation.")
    else:
        lines.append("Research mode is blocked until these issues are resolved:")
        lines.extend(f"- {blocker}" for blocker in result.blockers)
    if result.warnings:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {warning}" for warning in result.warnings)
    lines.append("")
    return "\n".join(lines)
