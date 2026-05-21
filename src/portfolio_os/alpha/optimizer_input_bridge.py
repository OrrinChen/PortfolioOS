"""Typed AlphaView expected-return optimizer input bridge.

This module adapts typed alpha projection artifacts into the optimizer input
frame. It is local-only: it does not call live services, create broker payloads,
or approve production trading.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file


TYPED_ALPHA_OPTIMIZER_BRIDGE_CONFIG_SCHEMA_VERSION = "typed_alpha_optimizer_bridge_config.v1"
TYPED_ALPHA_OPTIMIZER_BRIDGE_RESULT_SCHEMA_VERSION = "typed_alpha_optimizer_bridge_result.v1"
TYPED_ALPHA_OPTIMIZER_BRIDGE_MANIFEST_SCHEMA_VERSION = "typed_alpha_optimizer_injection_manifest.v1"
TYPED_ALPHA_OPTIMIZER_COVERAGE_REPORT_SCHEMA_VERSION = "typed_alpha_optimizer_coverage_report.v1"

FORBIDDEN_INPUT_KEYS = {
    "orders",
    "target_order",
    "trade_instruction",
    "trading_instruction",
    "broker_output",
    "alpaca_order",
    "live_performance",
    "production_approval",
    "production_alpha_approved",
    "recommended_trade",
    "submitted_order",
    "filled_order",
    "account_id",
    "api_key",
    "secret",
}


class TypedAlphaOptimizerBridgeConfig(BaseModel):
    """Opt-in bridge configuration for typed alpha optimizer injection."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["typed_alpha_optimizer_bridge_config.v1"] = (
        TYPED_ALPHA_OPTIMIZER_BRIDGE_CONFIG_SCHEMA_VERSION
    )
    allow_typed_alpha_optimizer_injection: bool = False
    no_network: bool = True
    no_broker: bool = True

    @model_validator(mode="after")
    def require_local_safety_flags(self) -> "TypedAlphaOptimizerBridgeConfig":
        if not self.no_network:
            raise ValueError("TypedAlphaOptimizerBridgeConfig requires no_network=true")
        if not self.no_broker:
            raise ValueError("TypedAlphaOptimizerBridgeConfig requires no_broker=true")
        return self


class TypedAlphaOptimizerBridgeResult(BaseModel):
    """Top-level result for a typed alpha optimizer input bridge run."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["typed_alpha_optimizer_bridge_result.v1"] = (
        TYPED_ALPHA_OPTIMIZER_BRIDGE_RESULT_SCHEMA_VERSION
    )
    run_id: str
    bridge_status: Literal["injected", "disabled", "rejected"]
    expected_return_reached_actual_optimizer_input: bool
    optimizer_decision_used_typed_expected_return: bool = False
    rebalance_date: str | None = None
    expected_return_used_count: int = 0
    active_name_count: int = 0
    abstain_count: int = 0
    missing_coverage_count: int = 0
    zero_expected_return_active_count: int = 0
    no_view_objective_neutral_fill_count: int = 0
    source_config_hash: str
    projection_manifest_hash: str | None = None
    rejection_reasons: list[str] = Field(default_factory=list)
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True
    production_approval_claimed: bool = False


class TypedAlphaOptimizerBridgeRun(TypedAlphaOptimizerBridgeResult):
    """Result plus writeable in-memory artifacts."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    optimizer_input_with_typed_alpha: Any = Field(exclude=True)
    coverage_report: dict[str, Any] = Field(default_factory=dict, exclude=True)

    @property
    def result(self) -> TypedAlphaOptimizerBridgeResult:
        return TypedAlphaOptimizerBridgeResult.model_validate(
            self.model_dump(exclude={"optimizer_input_with_typed_alpha", "coverage_report"})
        )


def inject_typed_expected_returns_into_optimizer_universe(
    *,
    universe: pd.DataFrame,
    expected_return_panel: pd.DataFrame,
    projection_manifest: dict[str, Any],
    q2_input_contract: dict[str, Any],
    alpha_abstain_report: dict[str, Any] | list[dict[str, Any]] | None,
    rebalance_date: str,
    config: TypedAlphaOptimizerBridgeConfig | None = None,
    run_id: str = "typed-alpha-optimizer-input-bridge",
) -> TypedAlphaOptimizerBridgeRun:
    """Inject typed expected returns into an optimizer universe frame."""

    bridge_config = config or TypedAlphaOptimizerBridgeConfig()
    projection_hash = str(projection_manifest.get("content_hash") or "")
    source_config_hash = hash_payload(
        {
            "run_id": run_id,
            "rebalance_date": str(rebalance_date),
            "projection_manifest_hash": projection_hash,
            "bridge_config": bridge_config.model_dump(mode="json"),
        }
    )

    rejection_reasons = [
        *_forbidden_key_reasons(q2_input_contract, "q2_input_contract"),
        *_forbidden_key_reasons(projection_manifest, "projection_manifest"),
    ]
    if rejection_reasons:
        return _run(
            run_id=run_id,
            bridge_status="rejected",
            expected_return_reached_actual_optimizer_input=False,
            source_config_hash=source_config_hash,
            projection_manifest_hash=projection_hash,
            rejection_reasons=rejection_reasons,
            frame=_base_disabled_frame(universe, status="rejected"),
            coverage_report={},
        )

    if not bridge_config.allow_typed_alpha_optimizer_injection:
        frame = _base_disabled_frame(universe, status="not_injected")
        coverage = _coverage_report(
            run_id=run_id,
            rebalance_date=rebalance_date,
            bridge_status="disabled",
            frame=frame,
            projection_manifest_hash=projection_hash,
        )
        return _run(
            run_id=run_id,
            bridge_status="disabled",
            expected_return_reached_actual_optimizer_input=False,
            source_config_hash=source_config_hash,
            projection_manifest_hash=projection_hash,
            frame=frame,
            coverage_report=coverage,
        )

    try:
        selected = _select_expected_return_panel(expected_return_panel, rebalance_date=rebalance_date)
        _validate_timestamps(selected)
        frame = _inject_selected_panel(
            universe=universe,
            selected=selected,
            abstain_reasons=_abstain_reason_map(alpha_abstain_report, rebalance_date=rebalance_date),
            rebalance_date=rebalance_date,
            projection_manifest=projection_manifest,
        )
    except Exception as exc:  # noqa: BLE001 - deterministic rejection
        return _run(
            run_id=run_id,
            bridge_status="rejected",
            expected_return_reached_actual_optimizer_input=False,
            source_config_hash=source_config_hash,
            projection_manifest_hash=projection_hash,
            rejection_reasons=[str(exc)],
            frame=_base_disabled_frame(universe, status="rejected"),
            coverage_report={},
        )

    coverage = _coverage_report(
        run_id=run_id,
        rebalance_date=rebalance_date,
        bridge_status="injected",
        frame=frame,
        projection_manifest_hash=projection_hash,
    )
    active_mask = frame["typed_alpha_view_status"].eq("active_view")
    return _run(
        run_id=run_id,
        bridge_status="injected",
        expected_return_reached_actual_optimizer_input=bool(active_mask.any()),
        source_config_hash=source_config_hash,
        projection_manifest_hash=projection_hash,
        expected_return_used_count=int(active_mask.sum()),
        active_name_count=int(frame.loc[active_mask, "ticker"].nunique()),
        abstain_count=int(frame["typed_alpha_view_status"].eq("no_view").sum()),
        missing_coverage_count=int(frame["typed_alpha_view_status"].eq("no_view").sum()),
        zero_expected_return_active_count=int(
            (
                active_mask
                & pd.to_numeric(frame["typed_alpha_expected_return"], errors="coerce").eq(0.0)
            ).sum()
        ),
        no_view_objective_neutral_fill_count=int(frame["expected_return_source"].eq("no_view_abstain_objective_neutral_fill").sum()),
        frame=frame,
        coverage_report=coverage,
    )


def write_typed_alpha_optimizer_bridge_artifacts(
    run: TypedAlphaOptimizerBridgeRun | TypedAlphaOptimizerBridgeResult,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write typed alpha optimizer bridge artifacts."""

    bundle = _ensure_run(run)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    optimizer_input_path = output_path / "optimizer_input_with_typed_alpha.csv"
    manifest_path = output_path / "typed_alpha_optimizer_injection_manifest.json"
    coverage_path = output_path / "typed_alpha_optimizer_coverage_report.json"

    bundle.optimizer_input_with_typed_alpha.to_csv(optimizer_input_path, index=False)
    coverage = bundle.coverage_report or _coverage_report(
        run_id=bundle.run_id,
        rebalance_date=bundle.rebalance_date or "",
        bridge_status=bundle.bridge_status,
        frame=bundle.optimizer_input_with_typed_alpha,
        projection_manifest_hash=bundle.projection_manifest_hash,
    )
    _write_json(coverage_path, coverage)
    manifest = {
        "schema_version": TYPED_ALPHA_OPTIMIZER_BRIDGE_MANIFEST_SCHEMA_VERSION,
        "run_id": bundle.run_id,
        "bridge_status": bundle.bridge_status,
        "source_config_hash": bundle.source_config_hash,
        "projection_manifest_hash": bundle.projection_manifest_hash,
        "expected_return_reached_actual_optimizer_input": bundle.expected_return_reached_actual_optimizer_input,
        "optimizer_decision_used_typed_expected_return": bundle.optimizer_decision_used_typed_expected_return,
        "production_approval_claimed": bundle.production_approval_claimed,
        "no_live_data_confirmed": bundle.no_live_data_confirmed,
        "no_orders_confirmed": bundle.no_orders_confirmed,
        "no_broker_confirmed": bundle.no_broker_confirmed,
        "output_artifacts": {
            "optimizer_input_with_typed_alpha": sha256_file(optimizer_input_path),
            "typed_alpha_optimizer_coverage_report": sha256_file(coverage_path),
        },
    }
    manifest["content_hash"] = hash_payload(manifest)
    _write_json(manifest_path, manifest)
    return {
        "optimizer_input": optimizer_input_path,
        "manifest": manifest_path,
        "coverage_report": coverage_path,
    }


def _inject_selected_panel(
    *,
    universe: pd.DataFrame,
    selected: pd.DataFrame,
    abstain_reasons: dict[str, str],
    rebalance_date: str,
    projection_manifest: dict[str, Any],
) -> pd.DataFrame:
    work = universe.copy()
    if "ticker" not in work.columns:
        if "symbol" not in work.columns:
            raise ValueError("optimizer universe must include ticker or symbol")
        work = work.rename(columns={"symbol": "ticker"})
    work["ticker"] = work["ticker"].astype(str).str.upper()

    selected_columns = [
        "symbol",
        "expected_return",
        "event_timestamp",
        "event_available_timestamp",
        "tradable_timestamp",
        "projection_policy",
        "source_config_hash",
    ]
    if "diagnostic_score" in selected.columns:
        selected_columns.append("diagnostic_score")
    expected = selected.loc[:, [column for column in selected_columns if column in selected.columns]].copy()
    expected["symbol"] = expected["symbol"].astype(str).str.upper()
    expected["typed_alpha_expected_return"] = pd.to_numeric(
        expected["expected_return"], errors="raise"
    ).astype(float)
    expected = expected.drop(columns=["expected_return"]).rename(columns={"symbol": "ticker"})

    merged = work.merge(expected, on="ticker", how="left")
    active_mask = merged["typed_alpha_expected_return"].notna()
    merged["typed_alpha_view_status"] = "no_view"
    merged.loc[active_mask, "typed_alpha_view_status"] = "active_view"
    merged["typed_alpha_abstain_reason"] = merged["ticker"].map(abstain_reasons)
    merged.loc[active_mask, "typed_alpha_abstain_reason"] = ""
    merged["expected_return"] = merged["typed_alpha_expected_return"]
    merged.loc[~active_mask, "expected_return"] = 0.0
    merged["expected_return_source"] = "no_view_abstain_objective_neutral_fill"
    merged.loc[active_mask, "expected_return_source"] = "typed_alpha_projection"
    merged["alpha_family"] = _alpha_family(projection_manifest)
    merged["rebalance_date"] = str(rebalance_date)
    for column in [
        "event_timestamp",
        "event_available_timestamp",
        "tradable_timestamp",
        "projection_policy",
        "source_config_hash",
        "diagnostic_score",
    ]:
        if column not in merged.columns:
            merged[column] = ""
    merged.loc[~active_mask, "source_config_hash"] = str(projection_manifest.get("content_hash") or "")
    return merged.sort_values("ticker").reset_index(drop=True)


def _select_expected_return_panel(expected_return_panel: pd.DataFrame, *, rebalance_date: str) -> pd.DataFrame:
    required = {"date", "symbol", "expected_return"}
    missing = required - set(expected_return_panel.columns)
    if missing:
        raise ValueError("expected_return_panel missing required columns: " + ", ".join(sorted(missing)))
    frame = expected_return_panel.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.strftime("%Y-%m-%d")
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["expected_return"] = pd.to_numeric(frame["expected_return"], errors="raise").astype(float)
    selected = frame.loc[frame["date"] == pd.Timestamp(rebalance_date).strftime("%Y-%m-%d")].copy()
    return selected


def _validate_timestamps(selected: pd.DataFrame) -> None:
    if {"event_available_timestamp", "tradable_timestamp"}.issubset(selected.columns):
        available = pd.to_datetime(selected["event_available_timestamp"], errors="raise", utc=True)
        tradable = pd.to_datetime(selected["tradable_timestamp"], errors="raise", utc=True)
        if bool((available > tradable).any()):
            raise ValueError("event_available_timestamp must be <= tradable_timestamp")


def _abstain_reason_map(
    alpha_abstain_report: dict[str, Any] | list[dict[str, Any]] | None,
    *,
    rebalance_date: str,
) -> dict[str, str]:
    rows: list[dict[str, Any]]
    if alpha_abstain_report is None:
        rows = []
    elif isinstance(alpha_abstain_report, dict):
        payload = alpha_abstain_report.get("abstain_report", alpha_abstain_report.get("rows", []))
        rows = payload if isinstance(payload, list) else []
    else:
        rows = alpha_abstain_report
    reason_map: dict[str, str] = {}
    date_text = pd.Timestamp(rebalance_date).strftime("%Y-%m-%d")
    for row in rows:
        row_date = row.get("date")
        if row_date is not None and pd.Timestamp(row_date).strftime("%Y-%m-%d") != date_text:
            continue
        symbol = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
        if symbol:
            reason_map[symbol] = str(row.get("reason") or "coverage_missing")
    return reason_map


def _base_disabled_frame(universe: pd.DataFrame, *, status: str) -> pd.DataFrame:
    frame = universe.copy()
    if "ticker" not in frame.columns and "symbol" in frame.columns:
        frame = frame.rename(columns={"symbol": "ticker"})
    if "ticker" not in frame.columns:
        frame["ticker"] = pd.Series(dtype=str)
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    if "expected_return" not in frame.columns:
        frame["expected_return"] = pd.NA
    frame["typed_alpha_expected_return"] = pd.NA
    frame["typed_alpha_view_status"] = status
    frame["typed_alpha_abstain_reason"] = ""
    frame["expected_return_source"] = status
    frame["alpha_family"] = ""
    frame["rebalance_date"] = ""
    return frame.sort_values("ticker").reset_index(drop=True)


def _coverage_report(
    *,
    run_id: str,
    rebalance_date: str,
    bridge_status: str,
    frame: pd.DataFrame,
    projection_manifest_hash: str | None,
) -> dict[str, Any]:
    active = frame["typed_alpha_view_status"].eq("active_view") if "typed_alpha_view_status" in frame else pd.Series(dtype=bool)
    no_view = frame["typed_alpha_view_status"].eq("no_view") if "typed_alpha_view_status" in frame else pd.Series(dtype=bool)
    no_view_not_encoded = bool(
        no_view.any()
        and frame.loc[no_view, "typed_alpha_expected_return"].isna().all()
        and frame.loc[no_view, "expected_return_source"].eq("no_view_abstain_objective_neutral_fill").all()
    ) if len(frame) else True
    return {
        "schema_version": TYPED_ALPHA_OPTIMIZER_COVERAGE_REPORT_SCHEMA_VERSION,
        "run_id": run_id,
        "rebalance_date": str(rebalance_date),
        "bridge_status": bridge_status,
        "projection_manifest_hash": projection_manifest_hash,
        "expected_return_used_count": int(active.sum()) if len(frame) else 0,
        "active_name_count": int(frame.loc[active, "ticker"].nunique()) if len(frame) else 0,
        "abstain_count": int(no_view.sum()) if len(frame) else 0,
        "missing_coverage_count": int(no_view.sum()) if len(frame) else 0,
        "zero_expected_return_active_count": int(
            pd.to_numeric(frame.loc[active, "typed_alpha_expected_return"], errors="coerce").eq(0.0).sum()
        ) if len(frame) else 0,
        "no_view_objective_neutral_fill_count": int(
            frame["expected_return_source"].eq("no_view_abstain_objective_neutral_fill").sum()
        ) if len(frame) else 0,
        "no_view_not_encoded_as_zero": no_view_not_encoded,
    }


def _run(
    *,
    run_id: str,
    bridge_status: Literal["injected", "disabled", "rejected"],
    expected_return_reached_actual_optimizer_input: bool,
    source_config_hash: str,
    projection_manifest_hash: str | None,
    frame: pd.DataFrame,
    coverage_report: dict[str, Any],
    expected_return_used_count: int = 0,
    active_name_count: int = 0,
    abstain_count: int = 0,
    missing_coverage_count: int = 0,
    zero_expected_return_active_count: int = 0,
    no_view_objective_neutral_fill_count: int = 0,
    rejection_reasons: list[str] | None = None,
) -> TypedAlphaOptimizerBridgeRun:
    rebalance_date = str(frame["rebalance_date"].iloc[0]) if "rebalance_date" in frame.columns and len(frame) else None
    return TypedAlphaOptimizerBridgeRun(
        run_id=run_id,
        bridge_status=bridge_status,
        expected_return_reached_actual_optimizer_input=expected_return_reached_actual_optimizer_input,
        rebalance_date=rebalance_date,
        expected_return_used_count=expected_return_used_count,
        active_name_count=active_name_count,
        abstain_count=abstain_count,
        missing_coverage_count=missing_coverage_count,
        zero_expected_return_active_count=zero_expected_return_active_count,
        no_view_objective_neutral_fill_count=no_view_objective_neutral_fill_count,
        source_config_hash=source_config_hash,
        projection_manifest_hash=projection_manifest_hash,
        rejection_reasons=rejection_reasons or [],
        optimizer_input_with_typed_alpha=frame,
        coverage_report=coverage_report,
    )


def _ensure_run(
    run: TypedAlphaOptimizerBridgeRun | TypedAlphaOptimizerBridgeResult,
) -> TypedAlphaOptimizerBridgeRun:
    if isinstance(run, TypedAlphaOptimizerBridgeRun):
        return run
    return TypedAlphaOptimizerBridgeRun(
        **run.model_dump(),
        optimizer_input_with_typed_alpha=pd.DataFrame(),
        coverage_report={},
    )


def _forbidden_key_reasons(payload: Any, context: str) -> list[str]:
    reasons: list[str] = []

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                key_text = str(key).strip().lower()
                child_path = f"{path}.{key_text}"
                if key_text in FORBIDDEN_INPUT_KEYS:
                    reasons.append(f"forbidden typed optimizer bridge input key at {child_path}")
                walk(child, child_path)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")

    walk(payload, context)
    return reasons


def _alpha_family(projection_manifest: dict[str, Any]) -> str:
    for key in ("family_id", "alpha_family"):
        if projection_manifest.get(key):
            return str(projection_manifest[key])
    alpha_view_ids = projection_manifest.get("alpha_view_ids") or []
    if alpha_view_ids:
        text = str(alpha_view_ids[0])
        if "SUE" in text.upper():
            return "US_EVENT_SUE"
    return "typed_alpha_projection"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
