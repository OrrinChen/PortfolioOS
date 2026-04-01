"""Lightweight intraday execution simulation for frozen order baskets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, model_validator

from portfolio_os.cost.fee import estimate_fee
from portfolio_os.cost.slippage import estimate_slippage
from portfolio_os.data.import_profiles import ImportProfile, load_import_profile
from portfolio_os.data.loaders import ensure_columns, normalize_ticker, read_csv, read_yaml
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import load_portfolio_state
from portfolio_os.domain.enums import OrderSide
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.domain.models import PortfolioState
from portfolio_os.execution.calibration import (
    CalibrationProfile,
    ExecutionMarketCurve,
    build_resolved_calibration_payload,
    default_calibration_profile_path,
    load_calibration_profile,
    resolve_optional_path,
)
from portfolio_os.execution.slicer import build_bucket_fill_plan
from portfolio_os.optimizer.multi_period import build_multi_period_plan
from portfolio_os.storage.snapshots import file_metadata
from portfolio_os.utils.config import FeeConfig, SlippageConfig, TradingConfig


class ExecutionSimulationConfig(BaseModel):
    """Execution simulation mode and request-level overrides."""

    mode: Literal["participation_twap", "impact_aware"] = "impact_aware"
    bucket_count: int | None = Field(default=None, ge=1)
    allow_partial_fill: bool | None = None
    force_completion: bool | None = None
    max_bucket_participation_override: float | None = Field(default=None, ge=0.0, le=1.0)


class ExecutionRequest(BaseModel):
    """Execution simulation request payload."""

    name: str
    description: str | None = None
    artifact_dir: str
    input_orders: str
    audit: str | None = None
    market: str | None = None
    portfolio_state: str | None = None
    execution_profile: str | None = None
    calibration_profile: str | None = None
    market_curve: ExecutionMarketCurve | None = None
    simulation: ExecutionSimulationConfig

    @model_validator(mode="after")
    def validate_bucket_count(self) -> "ExecutionRequest":
        """Require the declared bucket count to match any inline curve definition."""

        if (
            self.market_curve is not None
            and self.simulation.bucket_count is not None
            and self.simulation.bucket_count != len(self.market_curve.buckets)
        ):
            raise ValueError(
                "simulation.bucket_count must match the number of market_curve buckets."
            )
        return self


class ExecutionBucketResult(BaseModel):
    """One bucket-level execution outcome."""

    bucket_index: int
    bucket_label: str
    status: Literal["filled", "partial_fill", "unfilled", "inactive"]
    requested_quantity: int
    filled_quantity: int
    remaining_quantity: int
    bucket_available_volume: float
    bucket_participation_limit: float
    bucket_fill_cap: int
    fill_price: float | None = None
    estimated_fee: float = 0.0
    estimated_slippage: float = 0.0
    estimated_total_cost: float = 0.0
    evaluated_fill_price: float | None = None
    evaluated_fee: float = 0.0
    evaluated_slippage: float = 0.0
    evaluated_total_cost: float = 0.0
    slippage_multiplier: float
    forced_completion: bool = False
    liquidity_constrained: bool = False


class ExecutionOrderResult(BaseModel):
    """Per-order execution outcome."""

    ticker: str
    side: str
    ordered_quantity: int
    filled_quantity: int
    unfilled_quantity: int
    fill_ratio: float
    ordered_notional: float
    filled_notional: float
    unfilled_notional: float
    average_fill_price: float | None = None
    estimated_fee: float
    estimated_slippage: float
    estimated_total_cost: float
    evaluated_average_fill_price: float | None = None
    evaluated_fee: float = 0.0
    evaluated_slippage: float = 0.0
    evaluated_total_cost: float = 0.0
    status: Literal["filled", "partial_fill", "unfilled"]
    participation_limit_used: float
    liquidity_constrained: bool = False
    bucket_results: list[ExecutionBucketResult] = Field(default_factory=list)


class ExecutionPortfolioSummary(BaseModel):
    """Portfolio-level execution summary."""

    total_ordered_notional: float
    total_filled_notional: float
    total_unfilled_notional: float
    total_fee: float
    total_slippage: float
    total_cost: float
    evaluated_total_fee: float = 0.0
    evaluated_total_slippage: float = 0.0
    evaluated_total_cost: float = 0.0
    fill_rate: float
    filled_order_count: int
    partial_fill_count: int
    unfilled_order_count: int
    inactive_bucket_count: int = 0


class ExecutionSimulationResult(BaseModel):
    """Serialized execution simulation payload."""

    run_id: str
    created_at: str
    disclaimer: str
    request_path: str
    request_metadata: dict[str, Any]
    resolved_calibration: dict[str, Any]
    bucket_curve: dict[str, Any]
    source_artifacts: dict[str, dict[str, Any]]
    per_order_results: list[ExecutionOrderResult]
    portfolio_summary: ExecutionPortfolioSummary
    conclusion: str


@dataclass
class ExecutionContext:
    """Resolved inputs required for execution simulation."""

    request_path: Path
    request: ExecutionRequest
    artifact_dir: Path
    input_orders_path: Path
    audit_path: Path
    approval_record_path: Path | None
    freeze_manifest_path: Path | None
    market_path: Path
    import_profile_path: Path | None
    import_profile: ImportProfile | None
    portfolio_state_path: Path | None
    execution_profile_path: Path | None
    selected_calibration_profile_path: Path
    selected_calibration_profile: CalibrationProfile
    orders_frame: pd.DataFrame
    market_frame: pd.DataFrame
    audit_payload: dict[str, Any]
    portfolio_state: PortfolioState
    execution_profile_payload: dict[str, Any]
    fee_config: FeeConfig
    slippage_config: SlippageConfig
    trading_config: TradingConfig
    resolved_allow_partial_fill: bool
    resolved_force_completion: bool
    resolved_default_participation_limit: float
    resolved_volume_shock_multiplier: float
    resolved_curve: ExecutionMarketCurve
    resolved_calibration: dict[str, Any]
    disclaimer: str
    source_artifacts: dict[str, dict[str, Any]]


def _load_json(path: Path) -> dict[str, Any]:
    """Load a JSON mapping from disk."""

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise InputValidationError(f"Expected a JSON mapping in {path}.")
    return payload


def load_execution_request(path: str | Path) -> ExecutionRequest:
    """Load and validate an execution simulation request."""

    payload = read_yaml(path)
    try:
        return ExecutionRequest.model_validate(payload)
    except ValidationError as exc:
        raise InputValidationError(f"Invalid execution request: {exc}") from exc


def _find_audit_path(artifact_dir: Path) -> Path:
    """Return the preferred audit artifact inside the source artifact directory."""

    for filename in ("final_audit.json", "audit.json"):
        candidate = artifact_dir / filename
        if candidate.exists():
            return candidate
    raise InputValidationError(
        f"Artifact directory does not contain final_audit.json or audit.json: {artifact_dir}"
    )


def _load_orders_frame(path: Path) -> pd.DataFrame:
    """Load and validate the OMS-style order basket."""

    frame = read_csv(path)
    ensure_columns(frame, ["ticker", "side", "quantity", "estimated_price"], str(path))
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    frame["side"] = frame["side"].astype(str).str.strip().str.upper()
    invalid_sides = sorted(
        set(frame.loc[~frame["side"].isin({side.value for side in OrderSide}), "side"].tolist())
    )
    if invalid_sides:
        raise InputValidationError(
            f"Unsupported side value(s) in execution orders: {', '.join(invalid_sides)}"
        )
    quantity_series = pd.to_numeric(frame["quantity"], errors="raise")
    if (quantity_series <= 0).any():
        raise InputValidationError("Execution orders contain non-positive quantity values.")
    if ((quantity_series % 1) != 0).any():
        raise InputValidationError("Execution orders must use whole-share quantities.")
    frame["quantity"] = quantity_series.astype(int)
    frame["estimated_price"] = pd.to_numeric(frame["estimated_price"], errors="raise")
    if (frame["estimated_price"] <= 0).any():
        raise InputValidationError("Execution orders contain non-positive estimated_price values.")
    if "estimated_notional" not in frame.columns:
        frame["estimated_notional"] = frame["quantity"] * frame["estimated_price"]
    else:
        frame["estimated_notional"] = pd.to_numeric(
            frame["estimated_notional"],
            errors="coerce",
        ).fillna(frame["quantity"] * frame["estimated_price"])
    return frame


def _load_portfolio_state_from_context(
    *,
    request: ExecutionRequest,
    audit_payload: dict[str, Any],
    request_dir: Path,
    cwd: Path,
) -> tuple[PortfolioState, Path | None]:
    """Resolve portfolio-state metadata from request or audit."""

    if request.portfolio_state is not None:
        resolved_path = resolve_optional_path(request.portfolio_state, anchors=[request_dir, cwd])
        if not resolved_path.exists():
            raise InputValidationError(f"portfolio_state path does not exist: {resolved_path}")
        return load_portfolio_state(resolved_path), resolved_path

    audit_path_text = str(audit_payload.get("inputs", {}).get("portfolio_state", {}).get("path", "")).strip()
    if audit_path_text:
        resolved_path = Path(audit_path_text).resolve()
        if resolved_path.exists():
            return load_portfolio_state(resolved_path), resolved_path

    try:
        return PortfolioState.model_validate(audit_payload["parameters"]["portfolio_state"]), None
    except (KeyError, ValidationError) as exc:
        raise InputValidationError(
            "Execution simulator could not recover portfolio_state from the request or source audit payload."
        ) from exc


def _load_execution_profile_payload(
    *,
    request: ExecutionRequest,
    audit_payload: dict[str, Any],
    request_dir: Path,
    cwd: Path,
) -> tuple[dict[str, Any], Path | None]:
    """Resolve the optional execution-profile payload."""

    if request.execution_profile is not None:
        resolved_path = resolve_optional_path(request.execution_profile, anchors=[request_dir, cwd])
        if not resolved_path.exists():
            raise InputValidationError(f"execution_profile path does not exist: {resolved_path}")
        return read_yaml(resolved_path), resolved_path

    audit_path_text = str(audit_payload.get("inputs", {}).get("execution_profile", {}).get("path", "")).strip()
    if audit_path_text:
        resolved_path = Path(audit_path_text).resolve()
        if resolved_path.exists():
            return read_yaml(resolved_path), resolved_path

    payload = audit_payload.get("parameters", {}).get("execution", {})
    if isinstance(payload, dict):
        return payload, None
    return {}, None


def _load_import_profile_from_audit(audit_payload: dict[str, Any]) -> tuple[ImportProfile | None, Path | None]:
    """Resolve an optional import profile from the source audit payload."""

    import_profile_path_text = str(audit_payload.get("inputs", {}).get("import_profile", {}).get("path", "")).strip()
    if not import_profile_path_text:
        return None, None
    resolved_path = Path(import_profile_path_text).resolve()
    if not resolved_path.exists():
        raise InputValidationError(f"Import profile path from source audit does not exist: {resolved_path}")
    return load_import_profile(resolved_path), resolved_path


def _resolve_selected_calibration_profile(
    *,
    calibration_profile_path: str | Path | None,
    request: ExecutionRequest,
    request_dir: Path,
    cwd: Path,
    execution_profile_payload: dict[str, Any],
    execution_profile_path: Path | None,
) -> tuple[Path, CalibrationProfile, str, dict[str, Any]]:
    """Resolve the selected calibration profile and execution-profile defaults."""

    execution_profile_default_path: Path | None = None
    execution_profile_default = execution_profile_payload.get("default_calibration_profile")
    if execution_profile_default:
        anchors = [execution_profile_path.parent] if execution_profile_path is not None else []
        anchors.extend([request_dir, cwd])
        execution_profile_default_path = resolve_optional_path(
            str(execution_profile_default),
            anchors=anchors,
        )

    if calibration_profile_path is not None:
        selected_path = Path(calibration_profile_path).resolve()
        selected_source = "cli"
    elif request.calibration_profile is not None:
        selected_path = resolve_optional_path(request.calibration_profile, anchors=[request_dir, cwd])
        selected_source = "request"
    elif execution_profile_default_path is not None:
        selected_path = execution_profile_default_path
        selected_source = "execution_profile_default"
    else:
        selected_path = default_calibration_profile_path()
        selected_source = "built_in_default"

    if not selected_path.exists():
        raise InputValidationError(f"Calibration profile path does not exist: {selected_path}")
    selected_profile = load_calibration_profile(selected_path)
    execution_profile_defaults = {
        "default_calibration_profile": str(execution_profile_default_path) if execution_profile_default_path else None,
        "participation_limit": execution_profile_payload.get("participation_limit"),
        "force_completion": execution_profile_payload.get("force_completion"),
    }
    return selected_path, selected_profile, selected_source, execution_profile_defaults


def load_execution_context(
    path: str | Path,
    *,
    calibration_profile_path: str | Path | None = None,
) -> ExecutionContext:
    """Resolve all files and parameters needed for simulation."""

    request_path = Path(path).resolve()
    request_dir = request_path.parent
    cwd = Path.cwd().resolve()
    request = load_execution_request(request_path)

    artifact_dir = resolve_optional_path(request.artifact_dir, anchors=[request_dir, cwd])
    if not artifact_dir.exists() or not artifact_dir.is_dir():
        raise InputValidationError(f"artifact_dir does not exist: {artifact_dir}")

    input_orders_path = resolve_optional_path(request.input_orders, anchors=[artifact_dir, request_dir, cwd])
    if not input_orders_path.exists():
        raise InputValidationError(f"input_orders path does not exist: {input_orders_path}")

    if request.audit is not None:
        audit_path = resolve_optional_path(request.audit, anchors=[artifact_dir, request_dir, cwd])
        if not audit_path.exists():
            raise InputValidationError(f"audit path does not exist: {audit_path}")
    else:
        audit_path = _find_audit_path(artifact_dir)
    audit_payload = _load_json(audit_path)
    approval_record_path = artifact_dir / "approval_record.json"
    if not approval_record_path.exists():
        approval_record_path = None
    freeze_manifest_path = artifact_dir / "freeze_manifest.json"
    if not freeze_manifest_path.exists():
        freeze_manifest_path = None

    if request.market is not None:
        market_path = resolve_optional_path(request.market, anchors=[artifact_dir, request_dir, cwd])
    else:
        market_path_text = str(audit_payload.get("inputs", {}).get("market", {}).get("path", "")).strip()
        if not market_path_text:
            raise InputValidationError("Source audit payload does not include the market input path.")
        market_path = Path(market_path_text).resolve()
    if not market_path.exists():
        raise InputValidationError(f"Market input path does not exist: {market_path}")

    orders_frame = _load_orders_frame(input_orders_path)
    required_tickers = orders_frame["ticker"].tolist()
    import_profile, import_profile_path = _load_import_profile_from_audit(audit_payload)
    market_frame = market_to_frame(
        load_market_snapshot(
            market_path,
            required_tickers,
            import_profile=import_profile,
        )
    )

    portfolio_state, portfolio_state_path = _load_portfolio_state_from_context(
        request=request,
        audit_payload=audit_payload,
        request_dir=request_dir,
        cwd=cwd,
    )
    execution_profile_payload, execution_profile_path = _load_execution_profile_payload(
        request=request,
        audit_payload=audit_payload,
        request_dir=request_dir,
        cwd=cwd,
    )

    (
        selected_calibration_profile_path,
        selected_calibration_profile,
        selected_calibration_source,
        execution_profile_defaults,
    ) = _resolve_selected_calibration_profile(
        calibration_profile_path=calibration_profile_path,
        request=request,
        request_dir=request_dir,
        cwd=cwd,
        execution_profile_payload=execution_profile_payload,
        execution_profile_path=execution_profile_path,
    )

    try:
        fee_config = FeeConfig.model_validate(audit_payload["parameters"]["fees"])
        slippage_config = SlippageConfig.model_validate(audit_payload["parameters"]["slippage"])
        trading_config = TradingConfig.model_validate(audit_payload["parameters"]["trading"])
        base_participation_limit = float(audit_payload["parameters"]["constraints"]["participation_limit"])
    except (KeyError, TypeError, ValidationError, ValueError) as exc:
        raise InputValidationError(
            "Execution simulator could not recover fee, slippage, trading, or participation settings from the source audit payload."
        ) from exc

    overridden_fields: list[str] = []
    resolved_curve = selected_calibration_profile.market_curve
    if request.market_curve is not None:
        resolved_curve = request.market_curve
        overridden_fields.append("market_curve")
    if request.simulation.bucket_count is not None and request.simulation.bucket_count != len(resolved_curve.buckets):
        raise InputValidationError(
            "simulation.bucket_count must match the number of resolved market_curve buckets."
        )

    resolved_allow_partial_fill = (
        selected_calibration_profile.defaults.allow_partial_fill
        if selected_calibration_profile.defaults.allow_partial_fill is not None
        else True
    )
    if request.simulation.allow_partial_fill is not None:
        resolved_allow_partial_fill = bool(request.simulation.allow_partial_fill)
        overridden_fields.append("simulation.allow_partial_fill")

    resolved_force_completion = (
        selected_calibration_profile.defaults.force_completion
        if selected_calibration_profile.defaults.force_completion is not None
        else bool(execution_profile_payload.get("force_completion", False))
    )
    if request.simulation.force_completion is not None:
        resolved_force_completion = bool(request.simulation.force_completion)
        overridden_fields.append("simulation.force_completion")

    resolved_default_participation_limit = (
        float(selected_calibration_profile.defaults.participation_limit)
        if selected_calibration_profile.defaults.participation_limit is not None
        else float(execution_profile_payload.get("participation_limit", base_participation_limit))
    )
    resolved_volume_shock_multiplier = float(selected_calibration_profile.defaults.volume_shock_multiplier)
    if request.simulation.max_bucket_participation_override is not None:
        overridden_fields.append("simulation.max_bucket_participation_override")

    resolved_calibration = build_resolved_calibration_payload(
        execution_profile_defaults=execution_profile_defaults,
        selected_profile_path=selected_calibration_profile_path,
        selected_profile=selected_calibration_profile,
        selected_profile_source=selected_calibration_source,
        overridden_fields=overridden_fields,
        resolved_curve=resolved_curve,
        resolved_allow_partial_fill=resolved_allow_partial_fill,
        resolved_force_completion=resolved_force_completion,
        resolved_default_participation_limit=resolved_default_participation_limit,
        resolved_volume_shock_multiplier=resolved_volume_shock_multiplier,
    )

    source_artifacts = {
        "request": file_metadata(request_path),
        "input_orders": file_metadata(input_orders_path),
        "audit": file_metadata(audit_path),
        "market": file_metadata(market_path),
        "calibration_profile": file_metadata(selected_calibration_profile_path),
    }
    if approval_record_path is not None:
        source_artifacts["approval_record"] = file_metadata(approval_record_path)
    if freeze_manifest_path is not None:
        source_artifacts["freeze_manifest"] = file_metadata(freeze_manifest_path)
    if import_profile_path is not None:
        source_artifacts["import_profile"] = file_metadata(import_profile_path)
    if portfolio_state_path is not None:
        source_artifacts["portfolio_state"] = file_metadata(portfolio_state_path)
    if execution_profile_path is not None:
        source_artifacts["execution_profile"] = file_metadata(execution_profile_path)

    return ExecutionContext(
        request_path=request_path,
        request=request,
        artifact_dir=artifact_dir,
        input_orders_path=input_orders_path,
        audit_path=audit_path,
        approval_record_path=approval_record_path,
        freeze_manifest_path=freeze_manifest_path,
        market_path=market_path,
        import_profile_path=import_profile_path,
        import_profile=import_profile,
        portfolio_state_path=portfolio_state_path,
        execution_profile_path=execution_profile_path,
        selected_calibration_profile_path=selected_calibration_profile_path,
        selected_calibration_profile=selected_calibration_profile,
        orders_frame=orders_frame,
        market_frame=market_frame,
        audit_payload=audit_payload,
        portfolio_state=portfolio_state,
        execution_profile_payload=execution_profile_payload,
        fee_config=fee_config,
        slippage_config=slippage_config,
        trading_config=trading_config,
        resolved_allow_partial_fill=resolved_allow_partial_fill,
        resolved_force_completion=resolved_force_completion,
        resolved_default_participation_limit=resolved_default_participation_limit,
        resolved_volume_shock_multiplier=resolved_volume_shock_multiplier,
        resolved_curve=resolved_curve,
        resolved_calibration=resolved_calibration,
        disclaimer=str(audit_payload.get("disclaimer", "Auxiliary decision-support tool only. Not investment advice.")),
        source_artifacts=source_artifacts,
    )


def _resolve_participation_limit(
    *,
    order_row: pd.Series,
    context: ExecutionContext,
) -> float:
    """Choose the participation limit for one order."""

    override = context.request.simulation.max_bucket_participation_override
    if override is not None:
        return float(override)
    for column in ("participation_limit", "max_bucket_participation", "max_participation"):
        if column in order_row.index and pd.notna(order_row[column]):
            return float(order_row[column])
    return context.resolved_default_participation_limit


def _bucket_status(
    *,
    requested_quantity: int,
    filled_quantity: int,
) -> Literal["filled", "partial_fill", "unfilled", "inactive"]:
    """Return the bucket-level fill status label."""

    if requested_quantity <= 0:
        return "inactive"
    if filled_quantity <= 0:
        return "unfilled"
    if filled_quantity < requested_quantity:
        return "partial_fill"
    return "filled"


def _order_status(
    *,
    ordered_quantity: int,
    filled_quantity: int,
) -> Literal["filled", "partial_fill", "unfilled"]:
    """Return the order-level fill status label."""

    if filled_quantity <= 0:
        return "unfilled"
    if filled_quantity < ordered_quantity:
        return "partial_fill"
    return "filled"


def _build_conclusion(
    summary: ExecutionPortfolioSummary,
    order_results: list[ExecutionOrderResult],
) -> str:
    """Build a short business-facing conclusion sentence."""

    constrained_orders = [result for result in order_results if result.status != "filled"]
    if summary.unfilled_order_count == 0 and summary.fill_rate >= 0.99:
        return "Conclusion: the frozen basket looks fully executable under the current intraday curve and participation cap."
    if not constrained_orders:
        return "Conclusion: the basket is executable in this MVP simulation, with costs concentrated in normal slippage and fees."
    worst_tickers = ", ".join(result.ticker for result in constrained_orders[:3])
    return (
        "Conclusion: the basket is only partially executable under the current curve; "
        f"residual risk remains in {worst_tickers}."
    )


def evaluate_execution_cost(
    *,
    bucket_results: list[ExecutionBucketResult],
    side: OrderSide,
    base_price: float,
    fee_config: FeeConfig,
    slippage_config: SlippageConfig,
) -> tuple[list[ExecutionBucketResult], dict[str, float | None]]:
    """Re-evaluate bucket costs under a unified bucket-participation accounting rule."""

    evaluated_bucket_results: list[ExecutionBucketResult] = []
    total_fee = 0.0
    total_slippage = 0.0
    total_filled_notional = 0.0
    total_filled_quantity = 0

    for bucket in bucket_results:
        filled_quantity = int(bucket.filled_quantity)
        evaluated_fill_price = None
        evaluated_fee = 0.0
        evaluated_slippage = 0.0
        if filled_quantity > 0:
            signed_quantity = filled_quantity if side == OrderSide.BUY else -filled_quantity
            evaluated_slippage = estimate_slippage(
                signed_quantity,
                base_price,
                float(bucket.bucket_available_volume),
                slippage_config,
            ) * float(bucket.slippage_multiplier)
            price_bump = evaluated_slippage / max(base_price * filled_quantity, 1e-12)
            if side == OrderSide.BUY:
                evaluated_fill_price = base_price * (1.0 + price_bump)
            else:
                evaluated_fill_price = max(base_price * (1.0 - price_bump), 0.0)
            evaluated_fee = estimate_fee(signed_quantity, evaluated_fill_price, fee_config)
            total_fee += float(evaluated_fee)
            total_slippage += float(evaluated_slippage)
            total_filled_notional += float(evaluated_fill_price * filled_quantity)
            total_filled_quantity += filled_quantity
        evaluated_bucket_results.append(
            bucket.model_copy(
                update={
                    "evaluated_fill_price": evaluated_fill_price,
                    "evaluated_fee": float(evaluated_fee),
                    "evaluated_slippage": float(evaluated_slippage),
                    "evaluated_total_cost": float(evaluated_fee + evaluated_slippage),
                }
            )
        )

    return evaluated_bucket_results, {
        "evaluated_average_fill_price": (
            total_filled_notional / total_filled_quantity if total_filled_quantity > 0 else None
        ),
        "evaluated_fee": float(total_fee),
        "evaluated_slippage": float(total_slippage),
        "evaluated_total_cost": float(total_fee + total_slippage),
        "evaluated_filled_notional": float(total_filled_notional),
    }


def _translate_impact_aware_results(
    *,
    context: ExecutionContext,
) -> list[ExecutionOrderResult]:
    """Build execution-order results using the multi-period planner."""

    order_results: list[ExecutionOrderResult] = []
    for order_row in context.orders_frame.to_dict(orient="records"):
        order_frame = pd.DataFrame([order_row])
        order_series = pd.Series(order_row)
        participation_limit = _resolve_participation_limit(order_row=order_series, context=context)
        plan = build_multi_period_plan(
            orders_frame=order_frame,
            market_frame=context.market_frame,
            market_curve=context.resolved_curve,
            fee_config=context.fee_config,
            slippage_config=context.slippage_config,
            trading_config=context.trading_config,
            participation_limit=participation_limit,
            allow_partial_fill=context.resolved_allow_partial_fill,
            force_completion=context.resolved_force_completion,
            volume_shock_multiplier=context.resolved_volume_shock_multiplier,
        )
        if not plan.orders:
            continue
        order_plan = plan.orders[0]
        bucket_results: list[ExecutionBucketResult] = []
        requested_quantity = int(order_plan.ordered_quantity)
        for bucket in order_plan.bucket_allocations:
            filled_quantity = int(bucket.planned_quantity)
            bucket_results.append(
                ExecutionBucketResult(
                    bucket_index=int(bucket.bucket_index),
                    bucket_label=bucket.bucket_label,
                    status=_bucket_status(
                        requested_quantity=requested_quantity,
                        filled_quantity=filled_quantity,
                    ),
                    requested_quantity=requested_quantity,
                    filled_quantity=filled_quantity,
                    remaining_quantity=int(bucket.residual_after_bucket),
                    bucket_available_volume=float(bucket.bucket_available_volume),
                    bucket_participation_limit=float(order_plan.participation_limit_used),
                    bucket_fill_cap=int(bucket.bucket_capacity),
                    fill_price=bucket.estimated_fill_price,
                    estimated_fee=float(bucket.estimated_fee),
                    estimated_slippage=float(bucket.estimated_slippage),
                    estimated_total_cost=float(bucket.estimated_total_cost),
                    slippage_multiplier=float(bucket.slippage_multiplier),
                    forced_completion=bool(bucket.forced_completion),
                    liquidity_constrained=bool(bucket.capacity_constrained and requested_quantity > filled_quantity),
                )
            )
            requested_quantity = int(bucket.residual_after_bucket)

        average_fill_price = (
            order_plan.planned_notional / order_plan.planned_quantity
            if order_plan.planned_quantity > 0
            else None
        )
        evaluated_bucket_results, evaluated_summary = evaluate_execution_cost(
            bucket_results=bucket_results,
            side=OrderSide(order_plan.side),
            base_price=float(order_plan.base_price),
            fee_config=context.fee_config,
            slippage_config=context.slippage_config,
        )
        order_results.append(
            ExecutionOrderResult(
                ticker=order_plan.ticker,
                side=order_plan.side,
                ordered_quantity=int(order_plan.ordered_quantity),
                filled_quantity=int(order_plan.planned_quantity),
                unfilled_quantity=int(order_plan.residual_quantity),
                fill_ratio=(
                    order_plan.planned_quantity / order_plan.ordered_quantity
                    if order_plan.ordered_quantity > 0
                    else 0.0
                ),
                ordered_notional=float(order_plan.ordered_notional),
                filled_notional=float(order_plan.planned_notional),
                unfilled_notional=float(order_plan.residual_notional),
                average_fill_price=average_fill_price,
                estimated_fee=float(order_plan.estimated_fee),
                estimated_slippage=float(order_plan.estimated_slippage),
                estimated_total_cost=float(order_plan.estimated_total_cost),
                evaluated_average_fill_price=evaluated_summary["evaluated_average_fill_price"],
                evaluated_fee=float(evaluated_summary["evaluated_fee"] or 0.0),
                evaluated_slippage=float(evaluated_summary["evaluated_slippage"] or 0.0),
                evaluated_total_cost=float(evaluated_summary["evaluated_total_cost"] or 0.0),
                status=_order_status(
                    ordered_quantity=int(order_plan.ordered_quantity),
                    filled_quantity=int(order_plan.planned_quantity),
                ),
                participation_limit_used=float(order_plan.participation_limit_used),
                liquidity_constrained=bool(order_plan.residual_quantity > 0),
                bucket_results=evaluated_bucket_results,
            )
        )
    return order_results


def run_execution_simulation(
    request_path: str | Path,
    *,
    run_id: str,
    created_at: str,
    calibration_profile_path: str | Path | None = None,
) -> ExecutionSimulationResult:
    """Run the lightweight execution simulation from one request file."""

    context = load_execution_context(
        request_path,
        calibration_profile_path=calibration_profile_path,
    )
    bucket_definitions = context.resolved_curve.buckets
    if context.request.simulation.mode == "impact_aware":
        order_results = _translate_impact_aware_results(context=context)
    else:
        order_results = []

        for order_row in context.orders_frame.to_dict(orient="records"):
            order_series = pd.Series(order_row)
            ticker = str(order_series["ticker"])
            side = OrderSide(str(order_series["side"]).upper())
            ordered_quantity = int(order_series["quantity"])
            base_price = float(order_series["estimated_price"])
            market_row = context.market_frame.loc[context.market_frame["ticker"] == ticker].iloc[0]
            adv_shares = float(market_row["adv_shares"])
            effective_adv_shares = max(
                adv_shares * context.resolved_volume_shock_multiplier,
                1.0,
            )
            participation_limit = _resolve_participation_limit(order_row=order_series, context=context)
            bucket_available_volumes = [
                effective_adv_shares * bucket.volume_share for bucket in bucket_definitions
            ]
            bucket_caps, planned_fills = build_bucket_fill_plan(
                ordered_quantity=ordered_quantity,
                bucket_available_volumes=bucket_available_volumes,
                participation_limit=participation_limit,
                lot_size=context.trading_config.lot_size,
                allow_partial_fill=context.resolved_allow_partial_fill,
                force_completion=context.resolved_force_completion,
            )

            bucket_results: list[ExecutionBucketResult] = []
            remaining_quantity = ordered_quantity
            total_filled_quantity = 0
            total_fee = 0.0
            total_slippage = 0.0
            total_filled_notional = 0.0

            for bucket_index, bucket in enumerate(bucket_definitions):
                requested_quantity = remaining_quantity
                bucket_fill_cap = bucket_caps[bucket_index]
                filled_quantity = planned_fills[bucket_index]
                forced_completion = bool(
                    context.resolved_force_completion
                    and bucket_index == len(bucket_definitions) - 1
                    and filled_quantity > bucket_fill_cap
                )
                estimated_slippage = 0.0
                fill_price = None
                estimated_fee = 0.0

                if filled_quantity > 0:
                    signed_quantity = filled_quantity if side == OrderSide.BUY else -filled_quantity
                    estimated_slippage = estimate_slippage(
                        signed_quantity,
                        base_price,
                        effective_adv_shares,
                        context.slippage_config,
                    ) * bucket.slippage_multiplier
                    price_bump = estimated_slippage / (base_price * filled_quantity)
                    if side == OrderSide.BUY:
                        fill_price = base_price * (1.0 + price_bump)
                    else:
                        fill_price = max(base_price * (1.0 - price_bump), 0.0)
                    estimated_fee = estimate_fee(signed_quantity, fill_price, context.fee_config)
                    total_fee += estimated_fee
                    total_slippage += estimated_slippage
                    total_filled_notional += filled_quantity * fill_price

                total_filled_quantity += filled_quantity
                remaining_quantity -= filled_quantity
                liquidity_constrained = requested_quantity > 0 and filled_quantity < requested_quantity
                bucket_results.append(
                    ExecutionBucketResult(
                        bucket_index=bucket_index + 1,
                        bucket_label=bucket.label,
                        status=_bucket_status(
                            requested_quantity=requested_quantity,
                            filled_quantity=filled_quantity,
                        ),
                        requested_quantity=requested_quantity,
                        filled_quantity=filled_quantity,
                        remaining_quantity=remaining_quantity,
                        bucket_available_volume=bucket_available_volumes[bucket_index],
                        bucket_participation_limit=participation_limit,
                        bucket_fill_cap=bucket_fill_cap,
                        fill_price=fill_price,
                        estimated_fee=estimated_fee,
                        estimated_slippage=estimated_slippage,
                        estimated_total_cost=estimated_fee + estimated_slippage,
                        slippage_multiplier=bucket.slippage_multiplier,
                        forced_completion=forced_completion,
                        liquidity_constrained=liquidity_constrained,
                    )
                )

            ordered_notional = float(ordered_quantity * base_price)
            unfilled_quantity = ordered_quantity - total_filled_quantity
            average_fill_price = (
                total_filled_notional / total_filled_quantity if total_filled_quantity > 0 else None
            )
            evaluated_bucket_results, evaluated_summary = evaluate_execution_cost(
                bucket_results=bucket_results,
                side=side,
                base_price=base_price,
                fee_config=context.fee_config,
                slippage_config=context.slippage_config,
            )
            order_results.append(
                ExecutionOrderResult(
                    ticker=ticker,
                    side=side.value,
                    ordered_quantity=ordered_quantity,
                    filled_quantity=total_filled_quantity,
                    unfilled_quantity=unfilled_quantity,
                    fill_ratio=(total_filled_quantity / ordered_quantity) if ordered_quantity > 0 else 0.0,
                    ordered_notional=ordered_notional,
                    filled_notional=total_filled_notional,
                    unfilled_notional=float(unfilled_quantity * base_price),
                    average_fill_price=average_fill_price,
                    estimated_fee=total_fee,
                    estimated_slippage=total_slippage,
                    estimated_total_cost=total_fee + total_slippage,
                    evaluated_average_fill_price=evaluated_summary["evaluated_average_fill_price"],
                    evaluated_fee=float(evaluated_summary["evaluated_fee"] or 0.0),
                    evaluated_slippage=float(evaluated_summary["evaluated_slippage"] or 0.0),
                    evaluated_total_cost=float(evaluated_summary["evaluated_total_cost"] or 0.0),
                    status=_order_status(
                        ordered_quantity=ordered_quantity,
                        filled_quantity=total_filled_quantity,
                    ),
                    participation_limit_used=participation_limit,
                    liquidity_constrained=any(bucket.liquidity_constrained for bucket in evaluated_bucket_results),
                    bucket_results=evaluated_bucket_results,
                )
            )

    total_ordered_notional = float(sum(result.ordered_notional for result in order_results))
    total_filled_notional = float(sum(result.filled_notional for result in order_results))
    total_unfilled_notional = float(sum(result.unfilled_notional for result in order_results))
    all_bucket_results = [
        bucket
        for result in order_results
        for bucket in result.bucket_results
    ]
    portfolio_summary = ExecutionPortfolioSummary(
        total_ordered_notional=total_ordered_notional,
        total_filled_notional=total_filled_notional,
        total_unfilled_notional=total_unfilled_notional,
        total_fee=float(sum(result.estimated_fee for result in order_results)),
        total_slippage=float(sum(result.estimated_slippage for result in order_results)),
        total_cost=float(sum(result.estimated_total_cost for result in order_results)),
        evaluated_total_fee=float(sum(result.evaluated_fee for result in order_results)),
        evaluated_total_slippage=float(sum(result.evaluated_slippage for result in order_results)),
        evaluated_total_cost=float(sum(result.evaluated_total_cost for result in order_results)),
        fill_rate=(
            (total_ordered_notional - total_unfilled_notional) / total_ordered_notional
            if total_ordered_notional > 0
            else 0.0
        ),
        filled_order_count=sum(1 for result in order_results if result.status == "filled"),
        partial_fill_count=sum(1 for result in order_results if result.status == "partial_fill"),
        unfilled_order_count=sum(1 for result in order_results if result.status == "unfilled"),
        inactive_bucket_count=sum(1 for bucket in all_bucket_results if bucket.status == "inactive"),
    )
    conclusion = _build_conclusion(portfolio_summary, order_results)

    return ExecutionSimulationResult(
        run_id=run_id,
        created_at=created_at,
        disclaimer=context.disclaimer,
        request_path=str(context.request_path),
        request_metadata={
            "name": context.request.name,
            "description": context.request.description,
            "artifact_dir": str(context.artifact_dir),
            "input_orders": str(context.input_orders_path),
            "audit": str(context.audit_path),
            "approval_record": (
                str(context.approval_record_path) if context.approval_record_path is not None else None
            ),
            "market": str(context.market_path),
            "import_profile": (
                str(context.import_profile_path) if context.import_profile_path is not None else None
            ),
            "portfolio_state": str(context.portfolio_state_path) if context.portfolio_state_path is not None else None,
            "execution_profile": (
                str(context.execution_profile_path) if context.execution_profile_path is not None else None
            ),
            "calibration_profile": str(context.selected_calibration_profile_path),
            "account_id": context.portfolio_state.account_id,
            "as_of_date": context.portfolio_state.as_of_date,
            "simulation": {
                "mode": context.request.simulation.mode,
                "bucket_count": len(bucket_definitions),
                "allow_partial_fill": context.resolved_allow_partial_fill,
                "force_completion": context.resolved_force_completion,
                "max_bucket_participation_override": context.request.simulation.max_bucket_participation_override,
                "volume_shock_multiplier": context.resolved_volume_shock_multiplier,
            },
        },
        resolved_calibration=context.resolved_calibration,
        bucket_curve={
            "bucket_count": len(bucket_definitions),
            "buckets": [bucket.model_dump(mode="json") for bucket in bucket_definitions],
        },
        source_artifacts=context.source_artifacts,
        per_order_results=order_results,
        portfolio_summary=portfolio_summary,
        conclusion=conclusion,
    )
