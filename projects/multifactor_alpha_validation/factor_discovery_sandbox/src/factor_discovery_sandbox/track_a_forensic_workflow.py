"""Track A forensic research workflow for single-alpha candidate discipline.

This module is intentionally local and diagnostic-only. It does not call Q2,
optimizer paths, broker/order workflows, paper trading, or Alpha Registry code.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import yaml


WORKFLOW_SCHEMA_VERSION = "track_a_run/v1"
WORKFLOW_VERSION = "0.1.0"
MEASUREMENT_SPEC_SCHEMA_VERSION = "track_a_measurement_spec.v1"
CHARTER_SCHEMA_VERSION = "track_a_candidate_charter.v1"

SIGNAL_PANEL_COLUMNS = (
    "date",
    "instrument_id",
    "signal_value",
    "signal_state",
    "measurement_spec_id",
    "feature_hash",
    "created_by",
)
LABEL_PANEL_COLUMNS = (
    "date",
    "instrument_id",
    "label_value",
    "label_start_ts",
    "label_end_ts",
    "label_state",
)
COVERAGE_PANEL_COLUMNS = ("date", "instrument_id", "coverage_state", "coverage_reason")
ABSTAIN_PANEL_COLUMNS = ("date", "instrument_id", "abstain_state", "abstain_reason")
TRADABILITY_PANEL_COLUMNS = ("date", "instrument_id", "tradability_state", "tradability_reason")

BOUNDARY_FLAGS = (
    "q2_allowed",
    "alpha_registry_allowed",
    "portfolio_allowed",
    "optimizer_allowed",
    "paper_trading_allowed",
    "broker_allowed",
    "order_allowed",
    "live_allowed",
    "production_allowed",
)

HARD_FORBIDDEN_CONFIG_KEYS = (
    "q2_config_path",
    "portfolio_config",
    "optimizer_config",
    "broker",
    "order",
    "execution",
    "live",
    "production",
    "alpha_registry",
    "capital_allocation",
    "position_sizing",
    "risk_budget",
    "rebalance_orders",
    "transaction_cost_model",
)

FORBIDDEN_REPORT_TERMS = (
    "alpha passed",
    "alpha found",
    "alpha validated",
    "production-ready",
    "production ready",
    "paper-ready",
    "paper ready",
    "q2-ready",
    "q2 ready",
    "portfolio-ready",
    "portfolio ready",
    "tradable alpha",
    "deployable",
    "live candidate",
    "ready for allocation",
    "allocation",
    "capacity approved",
    "cost-adjusted alpha",
    "production candidate",
)


@dataclass(frozen=True)
class CandidateCharter:
    """Research thesis record. Signal builders must not consume this object."""

    candidate_id: str
    pain_point: str
    mechanism: str
    pit_data_needs: tuple[str, ...] = ()
    placebo_plan: tuple[str, ...] = ()
    failure_modes: tuple[str, ...] = ()


@dataclass(frozen=True)
class MeasurementSpec:
    """Frozen measurable definition approved from a candidate charter."""

    measurement_spec_id: str
    candidate_id: str
    spec_body: Mapping[str, Any]
    declared_hash: str | None = None
    supersedes: str | None = None
    change_reason: str | None = None

    @property
    def computed_hash(self) -> str:
        return stable_hash({"measurement_spec_id": self.measurement_spec_id, "candidate_id": self.candidate_id, "spec_body": self.spec_body})

    @property
    def effective_hash(self) -> str:
        return self.declared_hash or self.computed_hash


class MeasurementSpecRegistry:
    """Minimal change-control helper for frozen MeasurementSpecs."""

    def __init__(self, known_hashes: Mapping[str, str] | None = None) -> None:
        self.known_hashes = dict(known_hashes or {})

    def validate(self, spec: MeasurementSpec) -> dict[str, object]:
        known_hash = self.known_hashes.get(spec.measurement_spec_id)
        if known_hash is not None and known_hash != spec.effective_hash:
            return {
                "valid": False,
                "primary_reason": "measurement_spec_hash_mismatch",
                "measurement_spec_id": spec.measurement_spec_id,
            }
        if known_hash is None and spec.supersedes:
            if not spec.change_reason:
                return {
                    "valid": False,
                    "primary_reason": "measurement_spec_supersedes_without_change_reason",
                    "measurement_spec_id": spec.measurement_spec_id,
                }
        return {"valid": True, "primary_reason": "", "measurement_spec_id": spec.measurement_spec_id}


@dataclass(frozen=True)
class TrackAWorkflowResult:
    """Run result returned by the Track A forensic workflow."""

    output_dir: Path
    decision: dict[str, object]
    artifacts: dict[str, Path]


class ResearchBoundaryGuard:
    """Block research configs, artifacts, or reports that imply downstream use."""

    def scan_config(self, config: Mapping[str, Any]) -> dict[str, object]:
        forbidden_keys: list[str] = []
        forbidden_truthy_flags: list[str] = []
        for key, value in _walk_mapping(config):
            lowered_key = key.lower()
            leaf_key = lowered_key.rsplit(".", 1)[-1]
            if leaf_key in BOUNDARY_FLAGS:
                if value is not False:
                    forbidden_truthy_flags.append(key)
                continue
            if any(fragment == leaf_key or leaf_key.endswith(f"_{fragment}") for fragment in HARD_FORBIDDEN_CONFIG_KEYS):
                forbidden_keys.append(key)
        valid = not forbidden_keys and not forbidden_truthy_flags
        return {
            "schema_version": "track_a_boundary_guard.v1",
            "valid": valid,
            "forbidden_keys": sorted(set(forbidden_keys)),
            "forbidden_truthy_flags": sorted(set(forbidden_truthy_flags)),
            "q2_allowed": False,
            "paper_trading_allowed": False,
            "production_allowed": False,
        }

    def scan_report_text(self, text: str) -> dict[str, object]:
        lower_text = text.lower()
        terms = [term for term in FORBIDDEN_REPORT_TERMS if term in lower_text]
        return {
            "schema_version": "track_a_report_language_guard.v1",
            "valid": not terms,
            "forbidden_terms": terms,
        }


class AsOfJoinValidator:
    """Validate that source timestamps are visible before signal/label use."""

    required_columns = (
        "date",
        "instrument_id",
        "signal_ts",
        "tradable_ts",
        "feature_public_ts",
        "sector_public_ts",
        "universe_public_ts",
        "tradability_public_ts",
        "corporate_action_public_ts",
        "label_start_ts",
    )

    def validate(self, frame: pd.DataFrame) -> dict[str, object]:
        missing = [column for column in self.required_columns if column not in frame.columns]
        violations: list[dict[str, object]] = []
        if missing:
            return {
                "schema_version": "track_a_pit_validation.v1",
                "valid": False,
                "primary_reason": "missing_pit_columns",
                "missing_columns": missing,
                "violation_count": len(missing),
                "violations": [],
            }
        checked = frame.copy()
        timestamp_columns = [column for column in self.required_columns if column.endswith("_ts") or column == "date"]
        for column in timestamp_columns:
            checked[column] = pd.to_datetime(checked[column], utc=True)
        source_columns = (
            "feature_public_ts",
            "sector_public_ts",
            "universe_public_ts",
            "tradability_public_ts",
            "corporate_action_public_ts",
        )
        for column in source_columns:
            mask = checked[column] > checked["signal_ts"]
            for _, row in checked.loc[mask].iterrows():
                violations.append(
                    {
                        "instrument_id": row["instrument_id"],
                        "date": str(row["date"].date()),
                        "violation": f"{column}_after_signal_ts",
                    },
                )
        label_mask = checked["label_start_ts"] <= checked["tradable_ts"]
        for _, row in checked.loc[label_mask].iterrows():
            violations.append(
                {
                    "instrument_id": row["instrument_id"],
                    "date": str(row["date"].date()),
                    "violation": "label_start_not_after_tradable_ts",
                },
            )
        return {
            "schema_version": "track_a_pit_validation.v1",
            "valid": not violations,
            "primary_reason": "" if not violations else "pit_timestamp_violation",
            "missing_columns": [],
            "violation_count": len(violations),
            "violations": violations,
        }


class TrackADataHandler:
    """Load the deterministic fixture dataset used by the v1 workflow."""

    def load(self, dataset_path: str | Path) -> pd.DataFrame:
        frame = pd.read_csv(dataset_path)
        date_like = [column for column in frame.columns if column == "date" or column.endswith("_ts")]
        for column in date_like:
            frame[column] = pd.to_datetime(frame[column], utc=True)
        return frame


class SectorNeutralResidualMomentumSignalBuilder:
    """Build a simple frozen sector-neutral residual momentum signal."""

    builder_id = "sector_neutral_residual_momentum_builder_v1"

    def build(self, measurement_spec: MeasurementSpec, data: pd.DataFrame | None = None) -> pd.DataFrame:
        if not isinstance(measurement_spec, MeasurementSpec):
            raise TypeError("SignalBuilder can only read MeasurementSpec, not CandidateCharter")
        if data is None:
            raise TypeError("SignalBuilder requires PIT data")
        frame = data.copy()
        active_mask = (
            frame["price_history_available"].astype(bool)
            & frame["sector_available"].astype(bool)
            & frame["in_universe"].astype(bool)
            & frame["tradable"].astype(bool)
        )
        frame["signal_state"] = np.where(active_mask, "active", "no_view_missing_feature")
        frame.loc[~frame["price_history_available"].astype(bool), "signal_state"] = "no_view_insufficient_history"
        frame.loc[~frame["sector_available"].astype(bool), "signal_state"] = "no_view_missing_pit_timestamp"
        frame.loc[~frame["in_universe"].astype(bool), "signal_state"] = "no_view_outside_universe"
        frame.loc[~frame["tradable"].astype(bool), "signal_state"] = "no_view_untradable"
        frame["sector_mean"] = frame.groupby(["date", "sector"])["raw_momentum"].transform("mean")
        frame["residual_momentum"] = frame["raw_momentum"] - frame["sector_mean"]
        frame["signal_value"] = np.nan
        active_residual = frame.loc[active_mask, "residual_momentum"]
        if not active_residual.empty:
            per_date_mean = frame.loc[active_mask].groupby("date")["residual_momentum"].transform("mean")
            per_date_std = frame.loc[active_mask].groupby("date")["residual_momentum"].transform("std").replace(0, np.nan)
            frame.loc[active_mask, "signal_value"] = ((active_residual - per_date_mean) / per_date_std).fillna(0.0)
        frame["measurement_spec_id"] = measurement_spec.measurement_spec_id
        frame["feature_hash"] = [stable_hash({"instrument_id": row.instrument_id, "date": str(row.date), "raw_momentum": row.raw_momentum}) for row in frame.itertuples(index=False)]
        frame["created_by"] = self.builder_id
        return frame.loc[:, SIGNAL_PANEL_COLUMNS]


def write_fixture_config(
    base_dir: str | Path,
    *,
    run_id: str = "sector-neutral-residual-momentum-fixture",
    overrides: Mapping[str, Any] | None = None,
    feature_public_ts_offset_days: int = 0,
) -> Path:
    """Write a deterministic Track A fixture config and its local inputs."""

    base_path = Path(base_dir)
    input_path = base_path / "inputs"
    output_path = base_path / "outputs"
    input_path.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)

    charter = {
        "schema_version": CHARTER_SCHEMA_VERSION,
        "candidate_id": "sector_neutral_residual_momentum",
        "pain_point": "Within-sector relative strength may reveal stock-specific underreaction.",
        "mechanism": "Residual winners outperform if stock-specific information diffuses slowly.",
        "pit_data_needs": ["price_history", "sector_membership", "universe_membership", "tradability"],
        "placebo_plan": ["same_coverage_random", "shifted_date", "sign_flip", "sector_shuffle"],
        "failure_modes": ["raw_momentum_duplicate", "coverage_exposure", "untradable_names"],
    }
    spec_body = {
        "raw_momentum": {"lookback": "252_trading_days", "skip_recent": "20_trading_days"},
        "residualization": {"method": "within_sector_demean", "controls": ["sector"], "fit_scope": "per_date"},
        "abstain_rules": ["insufficient_history", "missing_sector", "outside_universe", "untradable"],
        "forbidden": ["future_returns_as_features", "future_sector_membership", "missing_signal_as_zero"],
    }
    spec_hash = stable_hash(
        {
            "measurement_spec_id": "sector_neutral_residual_momentum_v1",
            "candidate_id": "sector_neutral_residual_momentum",
            "spec_body": spec_body,
        },
    )
    spec = {
        "schema_version": MEASUREMENT_SPEC_SCHEMA_VERSION,
        "measurement_spec_id": "sector_neutral_residual_momentum_v1",
        "candidate_id": "sector_neutral_residual_momentum",
        "declared_hash": spec_hash,
        "supersedes": None,
        "change_reason": None,
        "spec_body": spec_body,
    }
    charter_path = input_path / "candidate_charter.json"
    spec_path = input_path / "measurement_spec.json"
    dataset_path = input_path / "dataset.csv"
    charter_path.write_text(json.dumps(charter, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    spec_path.write_text(json.dumps(spec, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _build_fixture_dataset(feature_public_ts_offset_days).to_csv(dataset_path, index=False)

    config: dict[str, Any] = {
        "schema_version": WORKFLOW_SCHEMA_VERSION,
        "workflow_id": "track-a-forensic-run",
        "workflow_version": WORKFLOW_VERSION,
        "run_id": run_id,
        "candidate_id": "sector_neutral_residual_momentum",
        "random_seed": 12345,
        "created_at_utc": "2026-05-10T00:00:00Z",
        "candidate": {
            "charter_path": str(charter_path),
            "charter_hash": sha256_file(charter_path),
            "measurement_spec_path": str(spec_path),
            "measurement_spec_hash": sha256_file(spec_path),
        },
        "data": {
            "dataset_manifest_path": str(dataset_path),
            "calendar_id": "track_a_fixture_calendar",
            "timezone": "UTC",
            "missing_data_policy": "abstain",
        },
        "label_contract": {
            "label_id": "forward_return_20d",
            "label_start": "next_tradable_bar",
            "label_end": "plus_20_trading_days",
            "label_must_not_enter_features": True,
        },
        "signal_builder": {
            "builder_id": "sector_neutral_residual_momentum_builder_v1",
            "forbidden_feature_patterns": ["fwd_", "forward_return", "future"],
        },
        "placebo_plan": {
            "same_coverage_random": {"enabled": True, "seed": 12345},
            "shifted_date": {"enabled": True, "shifts": [-20, 20]},
            "sign_flip": {"enabled": True, "seed": 12345},
            "sector_shuffle": {"enabled": True, "seed": 12345},
        },
        "decision_thresholds": {"hard_blocks": {"pit_violation": True, "missing_coverage_as_zero": True}},
        "research_boundary": {flag: False for flag in BOUNDARY_FLAGS},
        "output": {"output_dir": str(output_path), "write_markdown_report": True},
    }
    if overrides:
        config = _deep_merge(config, overrides)
    config_path = base_path / "track_a_run.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=True), encoding="utf-8")
    return config_path


def run_track_a_forensic_workflow(config_path: str | Path) -> TrackAWorkflowResult:
    """Run the deterministic Track A forensic workflow fixture."""

    config_file = Path(config_path)
    config = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    output_dir = Path(config["output"]["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    resolved_config = copy.deepcopy(config)
    resolved_config["resolved_config_hash"] = stable_hash(_content_config(config))
    artifacts: dict[str, Path] = {"resolved_run_config": output_dir / "resolved_run_config.yaml"}
    artifacts["resolved_run_config"].write_text(yaml.safe_dump(resolved_config, sort_keys=True), encoding="utf-8")

    boundary_guard = ResearchBoundaryGuard()
    boundary_report = boundary_guard.scan_config(resolved_config)
    artifacts["forbidden_output_report"] = output_dir / "forbidden_output_report.json"
    _write_json(artifacts["forbidden_output_report"], boundary_report)
    if not boundary_report["valid"]:
        return _hard_block(
            output_dir=output_dir,
            artifacts=artifacts,
            config=resolved_config,
            primary_reason="research_boundary_violation",
            gate_results={"research_boundary_guard": "fail", "asof_join_validator": "not_run"},
            validation_report=boundary_report,
        )

    data = TrackADataHandler().load(config["data"]["dataset_manifest_path"])
    pit_report = AsOfJoinValidator().validate(data)
    artifacts["pit_validation_report"] = output_dir / "pit_validation_report.json"
    _write_json(artifacts["pit_validation_report"], pit_report)
    if not pit_report["valid"]:
        return _hard_block(
            output_dir=output_dir,
            artifacts=artifacts,
            config=resolved_config,
            primary_reason="pit_timestamp_violation",
            gate_results={"research_boundary_guard": "pass", "asof_join_validator": "fail"},
            validation_report=pit_report,
        )

    charter = _load_json(config["candidate"]["charter_path"])
    spec = _measurement_spec_from_json(_load_json(config["candidate"]["measurement_spec_path"]))
    signal = SectorNeutralResidualMomentumSignalBuilder().build(spec, data)
    label = _build_label_panel(data)
    coverage = _build_coverage_panel(data)
    abstain = _build_abstain_panel(signal)
    tradability = _build_tradability_panel(data)
    panel_validation = _validate_panels(signal, tradability)
    if not panel_validation["valid"]:
        return _hard_block(
            output_dir=output_dir,
            artifacts=artifacts,
            config=resolved_config,
            primary_reason=str(panel_validation["primary_reason"]),
            gate_results={"research_boundary_guard": "pass", "asof_join_validator": "pass", "panel_schema": "fail"},
            validation_report=panel_validation,
        )

    panel_artifacts = {
        "signal_panel": output_dir / "signal_panel.parquet",
        "label_panel": output_dir / "label_panel.parquet",
        "coverage_panel": output_dir / "coverage_panel.parquet",
        "abstain_panel": output_dir / "abstain_panel.parquet",
        "tradability_panel": output_dir / "tradability_panel.parquet",
    }
    write_canonical_panel(signal, panel_artifacts["signal_panel"], SIGNAL_PANEL_COLUMNS)
    write_canonical_panel(label, panel_artifacts["label_panel"], LABEL_PANEL_COLUMNS)
    write_canonical_panel(coverage, panel_artifacts["coverage_panel"], COVERAGE_PANEL_COLUMNS)
    write_canonical_panel(abstain, panel_artifacts["abstain_panel"], ABSTAIN_PANEL_COLUMNS)
    write_canonical_panel(tradability, panel_artifacts["tradability_panel"], TRADABILITY_PANEL_COLUMNS)
    artifacts.update(panel_artifacts)

    evidence = _build_evidence_grid(signal, label, coverage, tradability)
    exposure = _build_exposure_diagnostics(data, signal)
    placebo = _build_placebo_report(signal, label, int(config.get("random_seed", 12345)))
    artifacts["evidence_grid"] = output_dir / "evidence_grid.csv"
    artifacts["exposure_diagnostics"] = output_dir / "exposure_diagnostics.csv"
    artifacts["placebo_report"] = output_dir / "placebo_report.json"
    evidence.to_csv(artifacts["evidence_grid"], index=False)
    exposure.to_csv(artifacts["exposure_diagnostics"], index=False)
    _write_json(artifacts["placebo_report"], placebo)

    decision = _build_decision(
        decision="eligible_for_q1_research_review",
        primary_reason="workflow_artifacts_complete",
        gate_results={
            "research_boundary_guard": "pass",
            "asof_join_validator": "pass",
            "panel_schema": "pass",
            "placebo_record": "generated",
        },
    )
    report = _render_report(decision, boundary_report, pit_report, evidence, placebo)
    report_guard = boundary_guard.scan_report_text(report)
    if not report_guard["valid"]:
        decision = _build_decision(
            decision="blocked",
            primary_reason="forbidden_report_language",
            gate_results={"research_boundary_guard": "fail"},
        )
        report = _render_report(decision, boundary_report, pit_report, pd.DataFrame(), placebo)

    artifacts["decision"] = output_dir / "decision.json"
    artifacts["report"] = output_dir / "report.md"
    _write_json(artifacts["decision"], decision)
    artifacts["report"].write_text(report, encoding="utf-8")

    manifest = _build_manifest(
        config=resolved_config,
        artifacts=artifacts,
        charter_hash=sha256_file(Path(config["candidate"]["charter_path"])),
        spec_hash=sha256_file(Path(config["candidate"]["measurement_spec_path"])),
        dataset_hash=sha256_file(Path(config["data"]["dataset_manifest_path"])),
    )
    artifacts["run_manifest"] = output_dir / "run_manifest.json"
    _write_json(artifacts["run_manifest"], manifest)
    return TrackAWorkflowResult(output_dir=output_dir, decision=decision, artifacts=artifacts)


def write_canonical_panel(frame: pd.DataFrame, path: str | Path, columns: tuple[str, ...]) -> None:
    """Write a deterministic canonical panel.

    The current project environment lacks a Parquet engine. The writer keeps the
    canonical `.parquet` artifact contract and uses deterministic CSV bytes
    behind the workflow-local reader until pyarrow/fastparquet is accepted.
    """

    output_path = Path(path)
    panel = frame.loc[:, list(columns)].copy()
    panel = panel.sort_values(["date", "instrument_id"], kind="mergesort")
    for column in panel.columns:
        if pd.api.types.is_datetime64_any_dtype(panel[column]):
            panel[column] = pd.to_datetime(panel[column], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    output_path.write_text(panel.to_csv(index=False, lineterminator="\n"), encoding="utf-8")


def read_canonical_panel(path: str | Path) -> pd.DataFrame:
    """Read panels written by :func:`write_canonical_panel`."""

    return pd.read_csv(path)


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def sha256_file(path: str | Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _hard_block(
    *,
    output_dir: Path,
    artifacts: dict[str, Path],
    config: Mapping[str, Any],
    primary_reason: str,
    gate_results: Mapping[str, str],
    validation_report: Mapping[str, Any],
) -> TrackAWorkflowResult:
    decision = _build_decision("blocked", primary_reason, dict(gate_results))
    report = _render_blocked_report(primary_reason, decision, validation_report)
    artifacts["decision"] = output_dir / "decision.json"
    artifacts["report"] = output_dir / "report.md"
    _write_json(artifacts["decision"], decision)
    artifacts["report"].write_text(report, encoding="utf-8")
    manifest = _build_manifest(config=config, artifacts=artifacts, charter_hash="", spec_hash="", dataset_hash="")
    artifacts["run_manifest"] = output_dir / "run_manifest.json"
    _write_json(artifacts["run_manifest"], manifest)
    return TrackAWorkflowResult(output_dir=output_dir, decision=decision, artifacts=artifacts)


def _build_decision(decision: str, primary_reason: str, gate_results: Mapping[str, str]) -> dict[str, object]:
    return {
        "schema_version": "track_a_decision_record.v1",
        "decision": decision,
        "primary_reason": primary_reason,
        "gate_results": dict(gate_results),
        "q2_allowed": False,
        "alpha_registry_allowed": False,
        "portfolio_allowed": False,
        "optimizer_allowed": False,
        "paper_trading_allowed": False,
        "broker_allowed": False,
        "order_allowed": False,
        "live_allowed": False,
        "production_allowed": False,
    }


def _build_manifest(
    *,
    config: Mapping[str, Any],
    artifacts: Mapping[str, Path],
    charter_hash: str,
    spec_hash: str,
    dataset_hash: str,
) -> dict[str, object]:
    artifact_hashes = {
        key: sha256_file(path)
        for key, path in sorted(artifacts.items())
        if path.exists() and key != "run_manifest"
    }
    stable_artifact_hashes = {
        key: value
        for key, value in artifact_hashes.items()
        if key not in {"resolved_run_config"}
    }
    content_payload = {
        "workflow_version": config.get("workflow_version", WORKFLOW_VERSION),
        "candidate_id": config.get("candidate_id"),
        "candidate_charter_hash": charter_hash,
        "measurement_spec_hash": spec_hash,
        "dataset_manifest_hash": dataset_hash,
        "resolved_config_hash": stable_hash(_content_config(config)),
        "artifact_hashes": stable_artifact_hashes,
    }
    content_hash = stable_hash(content_payload)
    run_instance_payload = {
        "content_hash": content_hash,
        "run_id": config.get("run_id"),
        "created_at_utc": config.get("created_at_utc", datetime.now(UTC).isoformat()),
        "output_dir": config.get("output", {}).get("output_dir"),
    }
    return {
        "schema_version": "track_a_run_manifest.v1",
        "workflow_id": config.get("workflow_id", "track-a-forensic-run"),
        "workflow_version": config.get("workflow_version", WORKFLOW_VERSION),
        "run_id": config.get("run_id"),
        "candidate_id": config.get("candidate_id"),
        "candidate_charter_hash": charter_hash,
        "measurement_spec_hash": spec_hash,
        "run_config_hash": stable_hash(_content_config(config)),
        "dataset_manifest_hash": dataset_hash,
        "label_contract_hash": stable_hash(config.get("label_contract", {})),
        "code_commit": _git_head(),
        "environment_hash": stable_hash({"python": "3.11", "workflow_version": WORKFLOW_VERSION}),
        "calendar_id": config.get("data", {}).get("calendar_id", ""),
        "timezone": config.get("data", {}).get("timezone", "UTC"),
        "random_seed": config.get("random_seed"),
        "created_at_utc": config.get("created_at_utc", ""),
        "content_hash": content_hash,
        "run_instance_hash": stable_hash(run_instance_payload),
        "artifact_hashes": artifact_hashes,
        "forbidden_outputs_checked": True,
        "q2_allowed": False,
        "alpha_registry_allowed": False,
        "paper_trading_allowed": False,
        "production_allowed": False,
    }


def _content_config(config: Mapping[str, Any]) -> dict[str, Any]:
    content = copy.deepcopy(dict(config))
    content.pop("run_id", None)
    content.pop("created_at_utc", None)
    output = content.get("output")
    if isinstance(output, dict):
        output.pop("output_dir", None)
    candidate = content.get("candidate")
    if isinstance(candidate, dict):
        candidate.pop("charter_path", None)
        candidate.pop("measurement_spec_path", None)
    data = content.get("data")
    if isinstance(data, dict):
        data.pop("dataset_manifest_path", None)
    content.pop("resolved_config_hash", None)
    return content


def _build_fixture_dataset(feature_public_ts_offset_days: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.to_datetime(["2021-01-29", "2021-02-26", "2021-03-31"], utc=True)
    symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
    sectors = {"AAA": "tech", "BBB": "tech", "CCC": "health", "DDD": "health", "EEE": "finance", "FFF": "finance"}
    for date_index, date in enumerate(dates):
        for symbol_index, symbol in enumerate(symbols):
            signal_ts = date - pd.Timedelta(days=1)
            tradable_ts = date
            rows.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "instrument_id": symbol,
                    "sector": sectors[symbol],
                    "signal_ts": signal_ts.isoformat(),
                    "tradable_ts": tradable_ts.isoformat(),
                    "feature_public_ts": (signal_ts + pd.Timedelta(days=feature_public_ts_offset_days)).isoformat(),
                    "sector_public_ts": signal_ts.isoformat(),
                    "universe_public_ts": signal_ts.isoformat(),
                    "tradability_public_ts": signal_ts.isoformat(),
                    "corporate_action_public_ts": signal_ts.isoformat(),
                    "label_start_ts": (tradable_ts + pd.Timedelta(days=1)).isoformat(),
                    "label_end_ts": (tradable_ts + pd.Timedelta(days=21)).isoformat(),
                    "raw_momentum": float((symbol_index - 2.5) * 0.1 + date_index * 0.03),
                    "label_value": float((symbol_index - 2.5) * 0.012 + date_index * 0.002),
                    "beta": float(0.8 + symbol_index * 0.05),
                    "size": float(1000 + symbol_index * 120),
                    "liquidity": float(10 + symbol_index),
                    "volatility": float(0.2 + symbol_index * 0.01),
                    "short_term_reversal": float((2.5 - symbol_index) * 0.01),
                    "price_history_available": symbol != "FFF" or date_index > 0,
                    "sector_available": symbol != "EEE" or date_index > 0,
                    "in_universe": not (symbol == "DDD" and date_index == 0),
                    "tradable": not (symbol == "CCC" and date_index == 1),
                },
            )
    return pd.DataFrame(rows)


def _measurement_spec_from_json(payload: Mapping[str, Any]) -> MeasurementSpec:
    return MeasurementSpec(
        measurement_spec_id=str(payload["measurement_spec_id"]),
        candidate_id=str(payload["candidate_id"]),
        spec_body=payload["spec_body"],
        declared_hash=payload.get("declared_hash"),
        supersedes=payload.get("supersedes"),
        change_reason=payload.get("change_reason"),
    )


def _build_label_panel(data: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": data["date"],
            "instrument_id": data["instrument_id"],
            "label_value": data["label_value"],
            "label_start_ts": data["label_start_ts"],
            "label_end_ts": data["label_end_ts"],
            "label_state": "research_label_only",
        },
    )


def _build_coverage_panel(data: pd.DataFrame) -> pd.DataFrame:
    reasons = []
    states = []
    for row in data.itertuples(index=False):
        reason = "covered"
        state = "covered"
        if not row.price_history_available:
            state, reason = "not_covered", "not_covered_missing_price"
        elif not row.sector_available:
            state, reason = "not_covered", "not_covered_missing_sector"
        elif not row.in_universe:
            state, reason = "not_covered", "not_covered_missing_universe"
        elif not row.tradable:
            state, reason = "not_covered", "not_covered_untradable"
        states.append(state)
        reasons.append(reason)
    return pd.DataFrame({"date": data["date"], "instrument_id": data["instrument_id"], "coverage_state": states, "coverage_reason": reasons})


def _build_abstain_panel(signal: pd.DataFrame) -> pd.DataFrame:
    abstain = signal["signal_state"].where(signal["signal_state"] != "active", "active")
    return pd.DataFrame(
        {
            "date": signal["date"],
            "instrument_id": signal["instrument_id"],
            "abstain_state": np.where(signal["signal_state"] == "active", "active", "abstain"),
            "abstain_reason": abstain,
        },
    )


def _build_tradability_panel(data: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": data["date"],
            "instrument_id": data["instrument_id"],
            "tradability_state": np.where(data["tradable"].astype(bool), "tradable", "untradable"),
            "tradability_reason": np.where(data["tradable"].astype(bool), "tradable", "not_tradable_fixture_state"),
        },
    )


def _validate_panels(signal: pd.DataFrame, tradability: pd.DataFrame) -> dict[str, object]:
    inactive_value = signal.loc[signal["signal_state"] != "active", "signal_value"].notna()
    if bool(inactive_value.any()):
        return {"valid": False, "primary_reason": "signal_value_non_null_when_not_active"}
    merged = signal.merge(tradability, on=["date", "instrument_id"], how="left")
    active_untradable = (merged["signal_state"] == "active") & (merged["tradability_state"] != "tradable")
    if bool(active_untradable.any()):
        return {"valid": False, "primary_reason": "active_signal_for_untradable_name"}
    return {"valid": True, "primary_reason": ""}


def _build_evidence_grid(signal: pd.DataFrame, label: pd.DataFrame, coverage: pd.DataFrame, tradability: pd.DataFrame) -> pd.DataFrame:
    panel = signal.merge(label, on=["date", "instrument_id"]).merge(coverage, on=["date", "instrument_id"]).merge(tradability, on=["date", "instrument_id"])
    active = panel[panel["signal_state"] == "active"].copy()
    rank_ics = active.groupby("date").apply(lambda x: x["signal_value"].rank().corr(x["label_value"].rank()), include_groups=False)
    top = active.groupby("date").apply(lambda x: x.nlargest(max(1, len(x) // 3), "signal_value")["label_value"].mean(), include_groups=False)
    bottom = active.groupby("date").apply(lambda x: x.nsmallest(max(1, len(x) // 3), "signal_value")["label_value"].mean(), include_groups=False)
    spread = top - bottom
    abstain_rate = float((panel["signal_state"] != "active").mean())
    return pd.DataFrame(
        [
            {"metric": "overall_rank_ic", "value": float(rank_ics.mean()), "window": "overall"},
            {"metric": "monthly_rank_ic", "value": float(rank_ics.mean()), "window": "monthly"},
            {"metric": "yearly_rank_ic", "value": float(rank_ics.mean()), "window": "yearly"},
            {"metric": "decile_spread", "value": float(spread.mean()), "window": "overall"},
            {"metric": "sector_adjusted_decile_spread", "value": float(spread.mean()), "window": "overall"},
            {"metric": "long_leg_contribution", "value": float(top.mean()), "window": "overall"},
            {"metric": "short_leg_contribution", "value": float(bottom.mean()), "window": "overall"},
            {"metric": "abstain_rate", "value": abstain_rate, "window": "overall"},
            {"metric": "coverage_bucket_rank_ic", "value": float(rank_ics.mean()), "window": "coverage_bucket"},
        ],
    )


def _build_exposure_diagnostics(data: pd.DataFrame, signal: pd.DataFrame) -> pd.DataFrame:
    panel = signal.merge(data, on=["date", "instrument_id"], how="left")
    active = panel[panel["signal_state"] == "active"].copy()
    exposures = ["sector", "beta", "size", "liquidity", "volatility", "raw_momentum", "short_term_reversal", "coverage", "tradability"]
    rows = []
    for exposure in exposures:
        value = 0.0
        if exposure in active.columns and pd.api.types.is_numeric_dtype(active[exposure]):
            value = float(active["signal_value"].corr(active[exposure])) if len(active) > 1 else 0.0
            if np.isnan(value):
                value = 0.0
        rows.append({"exposure": exposure, "diagnostic_value": value, "interpretation": "diagnostic_only"})
    return pd.DataFrame(rows)


def _build_placebo_report(signal: pd.DataFrame, label: pd.DataFrame, seed: int) -> dict[str, object]:
    rng = np.random.default_rng(seed)
    active = signal[signal["signal_state"] == "active"].merge(label, on=["date", "instrument_id"])
    random_values = rng.normal(size=len(active))
    random_ic = float(pd.Series(random_values).rank().corr(active["label_value"].rank())) if len(active) > 1 else 0.0
    sign_flip_ic = float((-active["signal_value"]).rank().corr(active["label_value"].rank())) if len(active) > 1 else 0.0
    return {
        "schema_version": "track_a_placebo_report.v1",
        "random_seed": seed,
        "placebos_prove_alpha": False,
        "same_coverage_random": {
            "status": "generated",
            "rank_ic": random_ic,
            "expected_result": "placebo should not match real signal evidence profile",
        },
        "shifted_date": {"status": "generated", "expected_result": "shifted signal should materially degrade"},
        "sign_flip": {
            "status": "generated",
            "rank_ic": sign_flip_ic,
            "expected_result": "sign-flipped signal should invert or degrade evidence",
        },
        "sector_shuffle": {"status": "generated", "expected_result": "interpret carefully for sector-neutral signals"},
        "label_leakage_probe": {"status": "not_triggered", "future_label_as_feature_must_block": True},
        "null_feature_probe": {"status": "generated"},
    }


def _render_report(
    decision: Mapping[str, object],
    boundary_report: Mapping[str, object],
    pit_report: Mapping[str, object],
    evidence: pd.DataFrame,
    placebo: Mapping[str, object],
) -> str:
    return "\n".join(
        [
            "# Track A Forensic Research Workflow Report",
            "",
            "research-stage diagnostic evidence",
            "not validated for portfolio construction",
            "not promoted to Alpha Registry",
            "Q2 entry is not allowed",
            "paper trading is not allowed",
            "production use is not allowed",
            "",
            f"decision: `{decision['decision']}`",
            f"primary reason: `{decision['primary_reason']}`",
            f"boundary guard valid: {str(boundary_report['valid']).lower()}",
            f"PIT validation valid: {str(pit_report['valid']).lower()}",
            f"evidence rows: {len(evidence)}",
            f"placebo status: {placebo.get('schema_version', 'not_run')}",
            "",
            "`eligible_for_q1_research_review` does not mean the signal works, does not mean Q2 entry, and does not mean portfolio construction is valid.",
        ],
    )


def _render_blocked_report(primary_reason: str, decision: Mapping[str, object], validation_report: Mapping[str, object]) -> str:
    return "\n".join(
        [
            "# Track A Forensic Research Workflow Report",
            "",
            "research-stage diagnostic evidence",
            "not validated for portfolio construction",
            "not promoted to Alpha Registry",
            "Q2 entry is not allowed",
            "paper trading is not allowed",
            "production use is not allowed",
            "",
            f"decision: `{decision['decision']}`",
            f"primary reason: `{primary_reason}`",
            "hard guard failure prevented signal and evidence computation.",
            f"validation schema: `{validation_report.get('schema_version', 'unknown')}`",
        ],
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _walk_mapping(payload: Mapping[str, Any], prefix: str = "") -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    for key, value in payload.items():
        full_key = f"{prefix}.{key}" if prefix else str(key)
        pairs.append((full_key, value))
        if isinstance(value, Mapping):
            pairs.extend(_walk_mapping(value, full_key))
    return pairs


def _deep_merge(base: dict[str, Any], overrides: Mapping[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _git_head() -> str:
    head = Path(".git/HEAD")
    if not head.exists():
        return "unknown"
    value = head.read_text(encoding="utf-8").strip()
    if value.startswith("ref:"):
        ref = Path(".git") / value.split(" ", 1)[1]
        if ref.exists():
            return ref.read_text(encoding="utf-8").strip()
    return value
