from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.alpha.backtest_bridge import build_alpha_snapshot_for_rebalance
from portfolio_os.backtest.engine import (
    _StrategyState,
    _apply_pending_orders,
    _build_strategy_universe,
    _decision_horizon_days_for_rebalance,
    _inject_expected_returns,
    _load_returns_long,
    build_monthly_rebalance_schedule,
    reconstruct_price_panel,
)
from portfolio_os.backtest.manifest import load_backtest_manifest
from portfolio_os.compliance.pretrade import collect_data_quality_findings
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import build_portfolio_frame, load_holdings, load_portfolio_state, load_target_weights
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.optimizer.acceptance_proof import (
    build_deterministic_synthetic_alpha_frame,
    evaluate_synthetic_alpha_case,
    summarize_acceptance_proof,
)
from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.utils.config import load_app_config


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"
DEFAULT_CHOSEN_CONTEXT = ROOT / "outputs" / "objective_units_v1_structural_ablation_2026-04-15" / "chosen_context.json"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "optimizer_isolated_acceptance_2026-04-16"
DEFAULT_RELATIVE_SCALES = (0.0, 0.5, 1.0, 2.0)


@dataclass(frozen=True)
class RebalanceContext:
    rebalance_date: pd.Timestamp
    period_end_date: pd.Timestamp
    decision_horizon_days: int
    quantities: pd.Series
    cash: float
    price_row: pd.Series
    manifest_path: Path


def _parse_scales(raw: str) -> list[float]:
    values = [item.strip() for item in str(raw).split(",")]
    parsed = [float(value) for value in values if value]
    if not parsed:
        raise ValueError("--relative-scales cannot be empty.")
    return parsed


def _prepare_backtest_context(manifest_path: Path) -> dict[str, Any]:
    manifest = load_backtest_manifest(manifest_path)
    holdings = load_holdings(manifest.initial_holdings)
    targets = load_target_weights(manifest.target_weights)
    initial_state = load_portfolio_state(manifest.portfolio_state)
    app_config = load_app_config(
        default_path=manifest.config,
        constraints_path=manifest.constraints,
        execution_path=manifest.execution_profile,
        portfolio_state=initial_state,
    )
    portfolio_frame = build_portfolio_frame(holdings, targets)
    required_tickers = portfolio_frame["ticker"].astype(str).tolist()
    base_market_frame = market_to_frame(load_market_snapshot(manifest.market_snapshot, required_tickers))
    base_reference_frame = reference_to_frame(load_reference_snapshot(manifest.reference, required_tickers))
    anchor_prices = base_market_frame.set_index("ticker")["close"].astype(float).reindex(required_tickers)
    returns_long = _load_returns_long(manifest.returns_file)
    price_panel = reconstruct_price_panel(returns_long, anchor_prices=anchor_prices).reindex(columns=required_tickers)
    schedule = build_monthly_rebalance_schedule(price_panel)
    dates = [pd.Timestamp(item).normalize() for item in price_panel.index.tolist()]
    initial_quantities = (
        portfolio_frame.set_index("ticker")["quantity"].reindex(required_tickers).fillna(0).astype(int)
    )
    state = _StrategyState(
        name="optimizer",
        quantities=initial_quantities.copy(),
        cash=float(initial_state.available_cash),
    )
    return {
        "manifest": manifest,
        "targets": targets,
        "app_config": app_config,
        "base_market_frame": base_market_frame,
        "base_reference_frame": base_reference_frame,
        "price_panel": price_panel,
        "schedule": schedule,
        "dates": dates,
        "state": state,
    }


def _replay_to_rebalance_context(manifest_path: Path, rebalance_date: pd.Timestamp) -> tuple[dict[str, Any], RebalanceContext]:
    context = _prepare_backtest_context(manifest_path)
    manifest = context["manifest"]
    app_config = context["app_config"]
    targets = context["targets"]
    base_market_frame = context["base_market_frame"]
    base_reference_frame = context["base_reference_frame"]
    price_panel = context["price_panel"]
    schedule = context["schedule"]
    dates = context["dates"]
    state = context["state"]

    if rebalance_date not in schedule:
        raise InputValidationError(f"Rebalance date {rebalance_date.date()} is not on the monthly schedule.")

    commission_rate = float(app_config.fees.commission_rate)
    half_spread_bps = float(app_config.execution.backtest_fixed_half_spread_bps)
    date_position_map = {pd.Timestamp(item).normalize(): idx for idx, item in enumerate(dates)}

    for idx, current_date in enumerate(dates):
        price_row = price_panel.loc[current_date]
        price_row.name = pd.Timestamp(current_date).normalize()
        _apply_pending_orders(
            state,
            current_date=current_date,
            price_row=price_row,
            commission_rate=commission_rate,
            half_spread_bps=half_spread_bps,
        )
        if current_date not in schedule:
            continue
        period_end_date = schedule[schedule.index(current_date) + 1] if schedule.index(current_date) + 1 < len(schedule) else dates[-1]
        decision_horizon_days = _decision_horizon_days_for_rebalance(
            current_index=idx,
            period_end_date=period_end_date,
            date_position_map=date_position_map,
        )
        if current_date == rebalance_date:
            return context, RebalanceContext(
                rebalance_date=pd.Timestamp(current_date).normalize(),
                period_end_date=pd.Timestamp(period_end_date).normalize(),
                decision_horizon_days=int(decision_horizon_days),
                quantities=state.quantities.copy(),
                cash=float(state.cash),
                price_row=price_row.copy(),
                manifest_path=manifest_path,
            )

        alpha_snapshot = None
        try:
            alpha_snapshot = build_alpha_snapshot_for_rebalance(
                returns_file=manifest.returns_file,
                rebalance_date=current_date,
                quantiles=manifest.alpha_model.quantiles,
                min_evaluation_dates=manifest.alpha_model.min_evaluation_dates,
                zscore_winsor_limit=manifest.alpha_model.zscore_winsor_limit,
                t_stat_full_confidence=manifest.alpha_model.t_stat_full_confidence,
                max_abs_expected_return=manifest.alpha_model.max_abs_expected_return,
                decision_horizon_days=decision_horizon_days,
            )
        except InputValidationError:
            alpha_snapshot = None

        optimizer_universe, optimizer_config = _build_strategy_universe(
            current_date=current_date,
            quantities=state.quantities.copy(),
            cash=float(state.cash),
            targets=targets,
            base_market_frame=base_market_frame,
            base_reference_frame=base_reference_frame,
            price_row=price_row,
            app_config_template=app_config,
        )
        optimizer_universe = _inject_expected_returns(optimizer_universe, alpha_snapshot=alpha_snapshot)
        optimizer_findings = collect_data_quality_findings(optimizer_universe, optimizer_config)
        optimizer_run = run_rebalance(
            optimizer_universe,
            optimizer_config,
            input_findings=optimizer_findings,
        )
        next_date = dates[idx + 1]
        state.pending_orders = list(optimizer_run.orders)
        state.pending_fill_date = pd.Timestamp(next_date).normalize()

    raise InputValidationError(f"Failed to replay into rebalance date {rebalance_date.date()}.")


def _resolve_reference_expected_return(
    manifest_path: Path,
    rebalance_date: pd.Timestamp,
    decision_horizon_days: int,
    chosen_context_path: Path | None,
) -> tuple[float, str]:
    if chosen_context_path is not None and chosen_context_path.exists():
        payload = json.loads(chosen_context_path.read_text(encoding="utf-8"))
        payload_date = pd.Timestamp(payload.get("date")).normalize() if payload.get("date") else None
        value = payload.get("max_abs_expected_return")
        if payload_date == rebalance_date and value is not None:
            return float(value), f"chosen_context:{chosen_context_path.name}"
    snapshot = build_alpha_snapshot_for_rebalance(
        returns_file=load_backtest_manifest(manifest_path).returns_file,
        rebalance_date=rebalance_date,
        quantiles=load_backtest_manifest(manifest_path).alpha_model.quantiles,
        min_evaluation_dates=load_backtest_manifest(manifest_path).alpha_model.min_evaluation_dates,
        zscore_winsor_limit=load_backtest_manifest(manifest_path).alpha_model.zscore_winsor_limit,
        t_stat_full_confidence=load_backtest_manifest(manifest_path).alpha_model.t_stat_full_confidence,
        max_abs_expected_return=load_backtest_manifest(manifest_path).alpha_model.max_abs_expected_return,
        decision_horizon_days=decision_horizon_days,
    )
    max_abs = float(snapshot.current_cross_section["expected_return"].abs().max())
    return max_abs, "live_alpha_snapshot"


def _row_from_case(case, *, relative_scale: float) -> dict[str, Any]:
    payload = asdict(case)
    payload["relative_scale"] = float(relative_scale)
    return payload


def _render_summary_markdown(
    *,
    context: RebalanceContext,
    reference_scale_source: str,
    unit_scale: float,
    reference_max_abs_expected_return: float,
    detail: pd.DataFrame,
    proof_summary: dict[str, Any],
) -> str:
    lines = [
        "# Optimizer-Isolated Acceptance Proof",
        "",
        f"- Rebalance date: `{context.rebalance_date.date()}`",
        f"- Period end date: `{context.period_end_date.date()}`",
        f"- Decision horizon days: `{context.decision_horizon_days}`",
        f"- Reference max abs expected return: `{reference_max_abs_expected_return:.6f}`",
        f"- Reference source: `{reference_scale_source}`",
        f"- Synthetic unit scale per score point: `{unit_scale:.8f}`",
        "",
        "## Proof Flags",
    ]
    for key, value in proof_summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Grid",
        ]
    )
    for row in detail.to_dict(orient="records"):
        lines.append(
            "- "
            + ", ".join(
                [
                    f"sign={row['alpha_sign']}",
                    f"relative_scale={row['relative_scale']}",
                    f"alpha_share={row['alpha_share_abs_weighted']:.4f}",
                    f"base_alignment={row['base_alignment_spearman']:.4f}",
                    f"base_top_bottom={row['base_top_minus_bottom_weight_delta']:.4f}",
                    f"continuous_gross={row['continuous_gross_traded_notional']:.2f}",
                    f"repaired_gross={row['repaired_gross_traded_notional']:.2f}",
                    f"repair_retention={row['repair_retention_ratio']:.4f}",
                    f"objective_cash={row['objective_cash']:.4f}",
                    f"effective_N={row['effective_n_invested']:.2f}",
                    f"solver_status={row['solver_status']}",
                ]
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "- This artifact tests whether the optimizer can structurally respond to alpha when alpha is injected directly into `expected_return`.",
            "- It does not prove full time-series reception under real signal sparsity; it isolates optimizer acceptance on one realistic rebalance context.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run optimizer-isolated synthetic alpha acceptance proof.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--chosen-context", type=Path, default=DEFAULT_CHOSEN_CONTEXT)
    parser.add_argument("--rebalance-date", type=str, default=None)
    parser.add_argument("--relative-scales", type=str, default="0,0.5,1,2")
    parser.add_argument("--sign-flip-relative-scale", type=float, default=1.0)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    manifest_path = args.manifest.resolve()
    chosen_context_path = args.chosen_context.resolve() if args.chosen_context else None
    if args.rebalance_date:
        rebalance_date = pd.Timestamp(args.rebalance_date).normalize()
    elif chosen_context_path is not None and chosen_context_path.exists():
        payload = json.loads(chosen_context_path.read_text(encoding="utf-8"))
        rebalance_date = pd.Timestamp(payload["date"]).normalize()
    else:
        raise InputValidationError("Either --rebalance-date or a valid --chosen-context file is required.")

    context_payload, rebalance_context = _replay_to_rebalance_context(manifest_path, rebalance_date)
    optimizer_universe, optimizer_config = _build_strategy_universe(
        current_date=rebalance_context.rebalance_date,
        quantities=rebalance_context.quantities.copy(),
        cash=float(rebalance_context.cash),
        targets=context_payload["targets"],
        base_market_frame=context_payload["base_market_frame"],
        base_reference_frame=context_payload["base_reference_frame"],
        price_row=rebalance_context.price_row,
        app_config_template=context_payload["app_config"],
    )
    alpha_frame = build_deterministic_synthetic_alpha_frame(optimizer_universe)
    max_abs_score = float(alpha_frame["synthetic_alpha_score"].abs().max())
    reference_max_abs_expected_return, reference_scale_source = _resolve_reference_expected_return(
        manifest_path,
        rebalance_context.rebalance_date,
        rebalance_context.decision_horizon_days,
        chosen_context_path,
    )
    unit_scale = float(reference_max_abs_expected_return / max_abs_score) if max_abs_score > 0.0 else 0.0
    relative_scales = _parse_scales(args.relative_scales)

    cases = [
        evaluate_synthetic_alpha_case(
            optimizer_universe,
            optimizer_config,
            alpha_scale=unit_scale * float(relative_scale),
            alpha_sign=1,
            alpha_frame=alpha_frame,
        )
        for relative_scale in relative_scales
    ]
    sign_flip_case = evaluate_synthetic_alpha_case(
        optimizer_universe,
        optimizer_config,
        alpha_scale=unit_scale * float(args.sign_flip_relative_scale),
        alpha_sign=-1,
        alpha_frame=alpha_frame,
    )
    all_cases = [*cases, sign_flip_case]
    proof_summary = summarize_acceptance_proof(all_cases)
    detail = pd.DataFrame(
        [
            _row_from_case(case, relative_scale=float(relative_scale))
            for case, relative_scale in zip(cases, relative_scales, strict=True)
        ]
        + [_row_from_case(sign_flip_case, relative_scale=float(args.sign_flip_relative_scale))]
    )

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    detail_path = output_dir / "acceptance_detail.csv"
    summary_path = output_dir / "acceptance_summary.json"
    note_path = output_dir / "optimizer_isolated_acceptance.md"
    detail.to_csv(detail_path, index=False, encoding="utf-8-sig")
    summary_payload = {
        "manifest_path": str(manifest_path),
        "rebalance_date": rebalance_context.rebalance_date.strftime("%Y-%m-%d"),
        "period_end_date": rebalance_context.period_end_date.strftime("%Y-%m-%d"),
        "decision_horizon_days": int(rebalance_context.decision_horizon_days),
        "reference_max_abs_expected_return": float(reference_max_abs_expected_return),
        "reference_scale_source": reference_scale_source,
        "unit_scale": float(unit_scale),
        "relative_scales": [float(item) for item in relative_scales],
        "sign_flip_relative_scale": float(args.sign_flip_relative_scale),
        "proof_summary": proof_summary,
        "artifacts": {
            "detail_csv": str(detail_path),
            "note_md": str(note_path),
        },
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    note_path.write_text(
        _render_summary_markdown(
            context=rebalance_context,
            reference_scale_source=reference_scale_source,
            unit_scale=unit_scale,
            reference_max_abs_expected_return=reference_max_abs_expected_return,
            detail=detail,
            proof_summary=proof_summary,
        ),
        encoding="utf-8",
    )
    print(json.dumps(summary_payload, indent=2))


if __name__ == "__main__":
    main()
