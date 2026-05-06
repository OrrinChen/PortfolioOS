from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from multifactor_alpha_validation.wrds_ingest import (
    ARTIFACTS,
    WRDSQueryConfigError,
    validate_wrds_query_config,
)


@dataclass(frozen=True)
class ExternalDatasetSourceResult:
    status: str
    blockers: tuple[str, ...]
    dataset_source_manifest_path: str
    source_field_mapping_path: str
    dataset_ingest_validation_path: str
    dataset_readiness_path: str


def validate_external_pit_dataset_source(
    config: Mapping[str, Any] | str | Path,
    output_dir: Path,
) -> ExternalDatasetSourceResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    blockers: list[str] = []
    try:
        payload, _ = validate_wrds_query_config(config)
    except WRDSQueryConfigError as exc:
        payload = _safe_payload(config)
        blockers.append(str(exc))

    queries = payload.get("queries") if isinstance(payload, Mapping) else {}
    if not isinstance(queries, Mapping):
        queries = {}

    checks = _build_checks(payload, queries)
    for check in checks.values():
        if not check["passed"]:
            blockers.append(str(check["blocker"]))

    status = "ready" if not blockers else "blocked"
    manifest = _source_manifest(payload, status, blockers, checks)
    mapping = _field_mapping()
    validation = {
        "schema_version": "dataset_ingest_validation.v1",
        "status": status,
        "blockers": blockers,
        "checks": checks,
        "ingest_executed": False,
        "credentials_embedded": any("credential" in blocker.lower() for blocker in blockers),
        "raw_data_committed": False,
        "non_claims": {
            "alpha_evidence": False,
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }

    manifest_path = output_dir / "dataset_source_manifest.yaml"
    mapping_path = output_dir / "source_field_mapping.yaml"
    validation_path = output_dir / "dataset_ingest_validation.json"
    readiness_path = output_dir / "dataset_readiness.md"
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    mapping_path.write_text(yaml.safe_dump(mapping, sort_keys=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    readiness_path.write_text(_readiness_markdown(status, blockers), encoding="utf-8")
    return ExternalDatasetSourceResult(
        status=status,
        blockers=tuple(dict.fromkeys(blockers)),
        dataset_source_manifest_path=str(manifest_path),
        source_field_mapping_path=str(mapping_path),
        dataset_ingest_validation_path=str(validation_path),
        dataset_readiness_path=str(readiness_path),
    )


def _safe_payload(config: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(config, Mapping):
        return dict(config)
    path = Path(config)
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    try:
        loaded = yaml.safe_load(text) if path.suffix.lower() != ".json" else json.loads(text)
    except Exception:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _build_checks(payload: Mapping[str, Any], queries: Mapping[str, Any]) -> dict[str, dict[str, object]]:
    paths = _path_check(payload)
    return {
        "local_config_paths": paths,
        "historical_universe_membership": _sql_check(
            queries,
            "historical_universe_membership",
            required_any=("idxcst_his", "membership_start"),
            required_all=("membership_end", "source_is_pit"),
            forbidden=("current_constituents", "yfinance"),
            blocker="historical universe membership source is not proven PIT/historical",
        ),
        "adjusted_price_volume": _adjusted_price_query_check(queries),
        "qqq_benchmark": _qqq_benchmark_query_check(queries),
        "delisting_handling": _sql_check(
            queries,
            "delisting_returns",
            required_any=("stkdelists", "msedelist", "delisting"),
            required_all=("delisting_date", "delisting_return"),
            forbidden=(),
            blocker="delisting/inactive handling query is missing required fields",
        ),
    }


def _adjusted_price_query_check(queries: Mapping[str, Any]) -> dict[str, object]:
    sql = _query_sql(queries, "adjusted_price_volume_panel")
    direct_adjusted = all(token in sql for token in ("adjusted_open", "adjusted_close", "volume"))
    derivable_adjusted = all(
        token in sql
        for token in ("raw_open", "raw_close", "return", "volume", "adjusted_price_convention")
    )
    passed = bool(sql.strip()) and (direct_adjusted or derivable_adjusted)
    return {
        "passed": passed,
        "blocker": "" if passed else "price-volume query does not prove adjusted OHLCV output",
        "artifact": "adjusted_price_volume_panel",
    }


def _qqq_benchmark_query_check(queries: Mapping[str, Any]) -> dict[str, object]:
    sql = _query_sql(queries, "qqq_benchmark_panel")
    qqq_identified = "'qqq'" in sql or " qqq" in sql or "benchmark" in sql
    direct_adjusted = all(token in sql for token in ("adjusted_open", "adjusted_close", "volume"))
    derivable_adjusted = all(
        token in sql
        for token in ("raw_open", "raw_close", "return", "volume", "adjusted_price_convention")
    )
    passed = bool(sql.strip()) and qqq_identified and (direct_adjusted or derivable_adjusted)
    return {
        "passed": passed,
        "blocker": "" if passed else "benchmark query does not prove QQQ adjusted benchmark coverage",
        "artifact": "qqq_benchmark_panel",
    }


def _path_check(payload: Mapping[str, Any]) -> dict[str, object]:
    required = ("raw_output_dir", "standardized_output_dir", "preflight_output_dir")
    missing = [key for key in required if not str(payload.get(key, "")).strip()]
    raw_output_dir = str(payload.get("raw_output_dir", ""))
    cache_backed = raw_output_dir.startswith("data/cache/")
    passed = not missing and cache_backed
    blocker = ""
    if missing:
        blocker = f"dataset paths must be provided through local config: {', '.join(missing)}"
    elif not cache_backed:
        blocker = "raw output dir must stay under ignored data/cache"
    return {
        "passed": passed,
        "blocker": blocker,
        "raw_output_dir_gitignored": cache_backed,
    }


def _sql_check(
    queries: Mapping[str, Any],
    artifact: str,
    required_any: tuple[str, ...],
    required_all: tuple[str, ...],
    forbidden: tuple[str, ...],
    blocker: str,
) -> dict[str, object]:
    normalized = _query_sql(queries, artifact)
    passed = (
        bool(normalized.strip())
        and any(token.lower() in normalized for token in required_any)
        and all(token.lower() in normalized for token in required_all)
        and not any(token.lower() in normalized for token in forbidden)
    )
    return {
        "passed": passed,
        "blocker": "" if passed else blocker,
        "artifact": artifact,
    }


def _query_sql(queries: Mapping[str, Any], artifact: str) -> str:
    query_payload = queries.get(artifact)
    if not isinstance(query_payload, Mapping):
        return ""
    return str(query_payload.get("sql", "")).lower()


def _source_manifest(
    payload: Mapping[str, Any],
    status: str,
    blockers: list[str],
    checks: Mapping[str, Mapping[str, object]],
) -> dict[str, Any]:
    return {
        "schema_version": "external_pit_dataset_source_manifest.v1",
        "source_type": "wrds",
        "status": status,
        "blockers": blockers,
        "as_of_timestamp": str(payload.get("as_of_timestamp", "")),
        "allowed_use_mode": "formal_research",
        "paths": {
            "raw_output_dir": str(payload.get("raw_output_dir", "")),
            "standardized_output_dir": str(payload.get("standardized_output_dir", "")),
            "preflight_output_dir": str(payload.get("preflight_output_dir", "")),
        },
        "connection_policy": {
            "credentials_in_repo": False,
            "credential_source": "environment_or_local_wrds_config",
            "live_connection_required_for_check": False,
        },
        "source_proofs": {
            key: bool(value.get("passed"))
            for key, value in checks.items()
            if key != "local_config_paths"
        },
        "raw_data_committed": False,
        "non_claims": {
            "alpha_evidence": False,
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }


def _field_mapping() -> dict[str, Any]:
    return {
        "schema_version": "source_field_mapping.v1",
        "source_type": "wrds",
        "artifacts": {
            "historical_universe_membership": {
                "source_tables": ["comp.idxcst_his", "crsp_a_ccm.ccmxpf_lnkhist", "comp.security"],
                "required_output_fields": [
                    "date",
                    "asset_id",
                    "ticker",
                    "in_universe",
                    "entry_date",
                    "exit_date",
                    "membership_start",
                    "membership_end",
                    "as_of_timestamp",
                    "source",
                    "source_is_pit",
                ],
            },
            "adjusted_price_volume_panel": {
                "source_tables": ["crsp_a_stock.stkdlysecuritydata"],
                "required_output_fields": [
                    "date",
                    "asset_id",
                    "adjusted_open",
                    "adjusted_close",
                    "volume",
                    "adjusted_price_convention",
                ],
            },
            "qqq_benchmark_panel": {
                "source_tables": ["crsp_a_stock.stkdlysecuritydata"],
                "required_output_fields": ["date", "benchmark", "adjusted_open", "adjusted_close", "volume"],
            },
            "delisting_returns": {
                "source_tables": ["crsp_a_stock.stkdelists"],
                "required_output_fields": [
                    "asset_id",
                    "delisting_date",
                    "delisting_return",
                    "inactive_reason",
                    "last_trade_date",
                ],
            },
        },
    }


def _readiness_markdown(status: str, blockers: list[str]) -> str:
    lines = [
        "# External PIT Dataset Readiness",
        "",
        f"Status: `{status}`",
        "",
        "This check validates source configuration only. It does not pull WRDS data and does not create alpha evidence.",
        "",
    ]
    if blockers:
        lines.append("Blockers:")
        lines.extend(f"- {blocker}" for blocker in blockers)
    else:
        lines.append("Source adapter is ready for a local WRDS ingest run when credentials are configured outside the repo.")
    lines.extend(
        [
            "",
            "Non-claims: no production approval, no live trading, no broker output, no orders, no direct Q2 entry.",
            "",
        ]
    )
    return "\n".join(lines)
