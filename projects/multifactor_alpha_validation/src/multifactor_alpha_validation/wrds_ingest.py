from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
import hashlib

import pandas as pd
import yaml

from multifactor_alpha_validation.data_contract import (
    ResearchModePreflightResult,
    run_research_mode_preflight,
)


class WRDSQueryConfigError(ValueError):
    pass


class WRDSConnection(Protocol):
    def raw_sql(self, query: str) -> pd.DataFrame:
        ...

    def close(self) -> None:
        ...


ConnectionFactory = Callable[[], WRDSConnection]


@dataclass(frozen=True)
class WRDSIngestResult:
    manifest_path: Path
    raw_files: dict[str, Path]
    standardized_files: dict[str, Path]
    preflight: ResearchModePreflightResult


ARTIFACTS = {
    "historical_universe_membership": "historical_universe_membership.csv",
    "adjusted_price_volume_panel": "adjusted_price_volume_panel.csv",
    "qqq_benchmark_panel": "qqq_benchmark_panel.csv",
    "delisting_returns": "delisting_returns.csv",
}
SECRET_KEYS = {"password", "passwd", "token", "api_key", "secret", "wrds_password"}


def run_wrds_multifactor_ingest(
    config: Mapping[str, Any] | str | Path,
    base_dir: Path | None = None,
    connection_factory: ConnectionFactory | None = None,
    require_ready: bool = False,
) -> WRDSIngestResult:
    payload, config_base = validate_wrds_query_config(config)

    root = base_dir or config_base or Path(".")
    raw_dir = _resolve_dir(payload.get("raw_output_dir", "data/cache/wrds_multifactor/raw"), root)
    standardized_dir = _resolve_dir(
        payload.get("standardized_output_dir", "data/cache/wrds_multifactor/standardized"),
        root,
    )
    preflight_dir = _resolve_dir(
        payload.get("preflight_output_dir", "outputs/multifactor_alpha_validation/wrds_preflight"),
        root,
    )
    raw_dir.mkdir(parents=True, exist_ok=True)
    standardized_dir.mkdir(parents=True, exist_ok=True)

    queries = payload["queries"]
    assert isinstance(queries, Mapping)

    conn = (connection_factory or _default_wrds_connection_factory)()
    raw_files: dict[str, Path] = {}
    standardized_files: dict[str, Path] = {}
    try:
        for artifact, file_name in ARTIFACTS.items():
            query_payload = queries[artifact]
            assert isinstance(query_payload, Mapping)
            sql = str(query_payload.get("sql", "")).strip()
            frame = conn.raw_sql(sql)
            if frame.empty:
                raise WRDSQueryConfigError(f"{artifact} query returned zero rows")
            raw_path = raw_dir / file_name
            standardized_path = standardized_dir / file_name
            frame.to_csv(raw_path, index=False)
            _standardize_frame(artifact, frame).to_csv(standardized_path, index=False)
            raw_files[artifact] = raw_path
            standardized_files[artifact] = standardized_path
    finally:
        conn.close()

    manifest_path = standardized_dir / "research_mode_dataset_manifest.yaml"
    manifest = _build_research_manifest(payload, standardized_files)
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    preflight = run_research_mode_preflight(manifest_path, preflight_dir)
    if require_ready and not preflight.research_mode_ready:
        raise WRDSQueryConfigError("WRDS ingest completed but research-mode preflight is blocked")
    return WRDSIngestResult(
        manifest_path=manifest_path,
        raw_files=raw_files,
        standardized_files=standardized_files,
        preflight=preflight,
    )


def validate_wrds_query_config(config: Mapping[str, Any] | str | Path) -> tuple[dict[str, Any], Path | None]:
    payload, config_base = _load_config(config)
    _reject_secret_fields(payload)
    if payload.get("schema_version") != "wrds_multifactor_query_config.v1":
        raise WRDSQueryConfigError("invalid WRDS query config schema")
    queries = payload.get("queries")
    if not isinstance(queries, Mapping):
        raise WRDSQueryConfigError("queries section is required")
    _validate_queries(queries)
    return payload, config_base


def _load_config(config: Mapping[str, Any] | str | Path) -> tuple[dict[str, Any], Path | None]:
    if isinstance(config, Mapping):
        return dict(config), None
    path = Path(config)
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        payload = json.loads(text)
    else:
        payload = yaml.safe_load(text) or {}
    if not isinstance(payload, dict):
        raise WRDSQueryConfigError("WRDS query config must be a mapping")
    return payload, path.parent


def _reject_secret_fields(value: object, path: str = "config") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            key_text = str(key).lower()
            if key_text in SECRET_KEYS or any(secret in key_text for secret in SECRET_KEYS):
                raise WRDSQueryConfigError("WRDS query config must not contain credentials")
            _reject_secret_fields(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_secret_fields(nested, f"{path}[{index}]")


def _resolve_dir(value: object, root: Path) -> Path:
    path = Path(str(value))
    if path.is_absolute():
        return path
    return root / path


def _validate_queries(queries: Mapping[str, Any]) -> None:
    for artifact in ARTIFACTS:
        query_payload = queries.get(artifact)
        if not isinstance(query_payload, Mapping) or not str(query_payload.get("sql", "")).strip():
            raise WRDSQueryConfigError(f"missing sql for {artifact}")


def _standardize_frame(artifact: str, frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column).lower() for column in normalized.columns]
    if artifact == "historical_universe_membership":
        required = {"membership_start", "membership_end", "as_of_timestamp", "source", "source_is_pit"}
    elif artifact == "adjusted_price_volume_panel":
        required = {"date", "adjusted_close", "volume"}
    elif artifact == "qqq_benchmark_panel":
        required = {"date", "benchmark", "adjusted_close"}
    elif artifact == "delisting_returns":
        required = {"delisting_date", "delisting_return"}
    else:
        raise WRDSQueryConfigError(f"unknown artifact: {artifact}")
    missing = sorted(required - set(normalized.columns))
    if missing:
        raise WRDSQueryConfigError(f"{artifact} missing standardized columns: {', '.join(missing)}")
    if artifact != "qqq_benchmark_panel" and not ({"ticker", "permno", "asset_id"} & set(normalized.columns)):
        raise WRDSQueryConfigError(f"{artifact} must include ticker, permno, or asset_id")
    return normalized


def _build_research_manifest(
    payload: Mapping[str, Any],
    standardized_files: Mapping[str, Path],
) -> dict[str, Any]:
    timestamp_policy = payload.get("timestamp_policy")
    if not isinstance(timestamp_policy, Mapping):
        timestamp_policy = {
            "signal": "month_end_close",
            "visibility": "after_month_end_close",
            "tradable": "next_session_close",
            "allow_same_close_trading": False,
        }
    return {
        "schema_version": "research_mode_dataset_manifest.v1",
        "mode": "research_mode",
        "allowed_use_mode": "formal_research",
        "content_hash": _hash_standardized_files(standardized_files),
        "source_provenance": {
            "provider": "wrds",
            "as_of_timestamp": str(payload.get("as_of_timestamp", "configured_locally")),
            "license_mode": "local_research_subscription",
        },
        "universe": {
            "path": str(standardized_files["historical_universe_membership"]),
            "constituent_mode": "historical_membership",
            "source": "wrds_index_constituents",
            "source_is_pit": True,
        },
        "prices": {
            "path": str(standardized_files["adjusted_price_volume_panel"]),
            "source": "wrds_crsp",
            "adjusted": True,
        },
        "benchmark": {
            "path": str(standardized_files["qqq_benchmark_panel"]),
            "benchmark_id": "QQQ",
            "source": "wrds_crsp",
        },
        "delisting": {
            "handling": "explicit_file",
            "path": str(standardized_files["delisting_returns"]),
        },
        "trading_calendar": {
            "path": str(standardized_files["adjusted_price_volume_panel"]),
            "source": "wrds_crsp_trading_dates",
        },
        "timestamp_policy": dict(timestamp_policy),
        "non_claims": {
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }


def _hash_standardized_files(standardized_files: Mapping[str, Path]) -> str:
    digest = hashlib.sha256()
    for key in sorted(standardized_files):
        digest.update(key.encode("utf-8"))
        digest.update(standardized_files[key].read_bytes())
    return digest.hexdigest()


def _default_wrds_connection_factory() -> WRDSConnection:
    try:
        import wrds  # type: ignore[import-not-found]
    except ImportError as exc:
        raise WRDSQueryConfigError(
            "wrds package is not installed; install/configure it locally outside secrets and retry"
        ) from exc
    return wrds.Connection()
