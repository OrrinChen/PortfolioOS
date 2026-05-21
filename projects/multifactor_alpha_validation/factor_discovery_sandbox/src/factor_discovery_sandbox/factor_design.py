"""Factor design-layer contracts for the Factor Discovery Sandbox."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


REQUIRED_DESIGN_CONTRACT_KEYS = (
    "market_pain_point",
    "mechanism_hypothesis",
    "investor_constraint_or_behavior",
    "expected_universe",
    "expected_regime",
    "why_not_arbitraged_away",
    "observable_pre_formula_diagnostics",
    "formula_measurement_role",
    "placebo_design",
    "cost_capacity_risks",
    "expected_failure_modes",
)

DESIGN_GUARDS = {
    "allocator_entry_allowed": False,
    "q1_entry_allowed": False,
    "q2_entry_allowed": False,
    "alpha_registry_update_allowed": False,
    "production_approval_claimed": False,
    "direct_q2_entry_allowed": False,
    "not_alpha_evidence": True,
}


@dataclass(frozen=True)
class FDFactorDesignLayerResult:
    """Artifacts and summary for FD-D0."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def build_design_contract(factor_id: str, mechanism_family: str) -> dict[str, object]:
    """Build a mechanism-first design contract for a candidate factor spec."""

    family = str(mechanism_family)
    template = _FAMILY_DESIGN_TEMPLATES.get(family, _DEFAULT_DESIGN_TEMPLATE)
    contract = {
        "design_contract_schema": "fd_factor_design_contract.v1",
        "factor_id": factor_id,
        **template,
        "formula_measurement_role": (
            "The formula is only a measurement of the stated market pain point; it is not itself the thesis."
        ),
        "design_review_questions": [
            "What pain point or repeated constraint creates the signal?",
            "Which investors or flows plausibly create the mispricing?",
            "Why should the effect persist after costs, capacity, and placebo controls?",
            "Where should this mechanism fail if the thesis is real?",
        ],
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }
    return contract


def build_candidate_design_manifest(
    candidate_id: str,
    family_id: str,
    mechanism_family: str,
) -> dict[str, object]:
    """Build a standalone candidate-family design manifest."""

    contract = build_design_contract(candidate_id, mechanism_family)
    manifest = {
        "schema_version": "fd_candidate_design_manifest.v1",
        "stage": "FD-D0",
        "candidate_id": candidate_id,
        "family_id": family_id,
        "mechanism_family": mechanism_family,
        "design_contract": contract,
        "design_review_required": True,
        "pre_formula_evidence_required": True,
        "formula_is_measurement_not_thesis": True,
        "design_layer_required_before_formula": True,
        "manifest_written_before_validation": True,
        **DESIGN_GUARDS,
    }
    validation = validate_design_contract({"factor_id": candidate_id, **manifest})
    manifest["design_contract_valid"] = bool(validation["valid"])
    manifest["candidate_validation_allowed"] = bool(validation["valid"])
    manifest["design_failure_reasons"] = validation["failure_reasons"]
    return manifest


def write_candidate_design_manifest(
    path: str | Path,
    candidate_id: str,
    family_id: str,
    mechanism_family: str,
) -> dict[str, object]:
    """Write a standalone candidate design manifest and return its payload."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = build_candidate_design_manifest(
        candidate_id=candidate_id,
        family_id=family_id,
        mechanism_family=mechanism_family,
    )
    output_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def validate_design_contract(spec: Mapping[str, object]) -> dict[str, object]:
    """Validate that a factor spec has a mechanism-first design contract."""

    failures: list[str] = []
    contract = spec.get("design_contract")
    if not isinstance(contract, Mapping):
        failures.append("missing_design_contract")
        missing = list(REQUIRED_DESIGN_CONTRACT_KEYS)
    else:
        missing = [key for key in REQUIRED_DESIGN_CONTRACT_KEYS if not _nonempty(contract.get(key))]
        if missing:
            failures.append(f"missing_design_fields:{','.join(sorted(missing))}")
    if spec.get("formula_is_measurement_not_thesis") is not True:
        failures.append("formula_measurement_boundary_missing")
    if spec.get("pre_formula_evidence_required") is not True:
        failures.append("pre_formula_evidence_gate_missing")
    if spec.get("design_review_required") is not True:
        failures.append("design_review_gate_missing")
    if spec.get("not_alpha_evidence") is not True:
        failures.append("not_alpha_evidence_guard_missing")
    if spec.get("direct_q2_entry_allowed") is not False:
        failures.append("direct_q2_guard_missing")
    return {
        "schema_version": "fd_factor_design_contract_check.v1",
        "factor_id": str(spec.get("factor_id", "unknown")),
        "valid": not failures,
        "missing_design_fields": missing,
        "failure_reasons": failures,
        "design_layer_required_before_formula": True,
        "formula_is_measurement_not_thesis": spec.get("formula_is_measurement_not_thesis") is True,
        "pre_formula_evidence_required": spec.get("pre_formula_evidence_required") is True,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }


def write_factor_design_layer_spec(output_dir: str | Path, report_path: str | Path) -> FDFactorDesignLayerResult:
    """Write the FD-D0 design-layer charter and validation manifest."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "design_contract_validation": output_path / "factor_design_contract_validation.json",
        "design_layer_report": report_file,
    }
    validation = {
        "schema_version": "fd_factor_design_contract_validation.v1",
        "stage": "FD-D0",
        "design_layer_required_before_formula": True,
        "factor_formula_allowed_without_design_contract": False,
        "required_design_fields": list(REQUIRED_DESIGN_CONTRACT_KEYS),
        "formula_is_measurement_not_thesis": True,
        "candidate_family_approval_allowed": False,
        **DESIGN_GUARDS,
    }
    artifacts["design_contract_validation"].write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["design_layer_report"].write_text(_render_design_layer_report(validation), encoding="utf-8")
    summary = {
        "schema_version": "fd_factor_design_layer_summary.v1",
        "stage": "FD-D0",
        "design_layer_required_before_formula": True,
        **DESIGN_GUARDS,
    }
    return FDFactorDesignLayerResult(summary=summary, artifacts=artifacts)


def _nonempty(value: object) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value is not None


def _render_design_layer_report(validation: Mapping[str, object]) -> str:
    fields = "\n".join(f"- {field.replace('_', ' ')}" for field in validation["required_design_fields"])
    return "\n".join(
        [
            "# FD-D0 Factor Design Layer Spec",
            "",
            "not alpha evidence",
            "allocator entry: blocked",
            "Q1 entry: blocked",
            "Q2 entry: blocked",
            "Alpha Registry update: blocked",
            "production approval: not claimed",
            "",
            "Factor Discovery now requires a design contract before a formula is treated as a candidate.",
            "The formula is measurement, not thesis.",
            "",
            "## Required Design Fields",
            fields,
            "",
            "## Operating Rule",
            (
                "Every new factor or candidate family must state the market pain point, mechanism, observable "
                "pre-formula diagnostics, placebo design, expected failures, and cost/capacity bottlenecks before "
                "FD-R3/FD-S validation can run."
            ),
            "",
        ]
    )


_DEFAULT_DESIGN_TEMPLATE = {
    "market_pain_point": "A repeated market behavior may create a measurable pricing pattern.",
    "mechanism_hypothesis": "The candidate must state a specific mechanism before formula validation.",
    "investor_constraint_or_behavior": "The design must identify the constrained investor, flow, or behavior.",
    "expected_universe": "Must be specified before validation.",
    "expected_regime": "Must be specified before validation.",
    "why_not_arbitraged_away": "Must explain frictions, limits to arbitrage, or implementation constraints.",
    "observable_pre_formula_diagnostics": [
        "Check whether the pain point exists before scoring formulas.",
        "Compare against same-coverage placebo and regime controls.",
    ],
    "placebo_design": ["same_coverage_placebo", "mechanism_matched_placebo", "rebalance_shifted_placebo"],
    "cost_capacity_risks": ["turnover", "spread", "ADV capacity", "impact", "shortability if short leg is used"],
    "expected_failure_modes": ["placebo dominance", "cost drag", "capacity concentration", "regime instability"],
}


_FAMILY_DESIGN_TEMPLATES = {
    "small_cap_quality_residual_momentum": {
        "market_pain_point": "Small-cap stocks may reflect delayed information diffusion, low coverage, and capacity-constrained sponsorship.",
        "mechanism_hypothesis": "Residual momentum should matter only after controlling sector, beta, size, liquidity, and quality or junk risk.",
        "investor_constraint_or_behavior": "Institutions face liquidity, coverage, benchmark, and quality-screen constraints in smaller names.",
        "expected_universe": "small_cap_investable names with delisting handling, PIT market cap, ADV, spread proxy, and quality coverage.",
        "expected_regime": "Should be strongest when small-cap breadth and stock-specific dispersion are high.",
        "why_not_arbitraged_away": "Capacity, spread, shortability, and low coverage limit clean arbitrage.",
        "observable_pre_formula_diagnostics": [
            "Confirm investable small-cap breadth after microcap quarantine.",
            "Measure whether live signal beats lagged, capacity-matched, and same-coverage placebos.",
            "Check whether payoff is not reducible to size, liquidity, beta, or sector exposure.",
        ],
        "placebo_design": [
            "same-coverage placebo",
            "capacity-matched placebo",
            "size-bucket shuffled signal",
            "sector-shuffled signal",
            "rebalance-shifted placebo",
        ],
        "cost_capacity_risks": ["spread drag", "ADV capacity", "impact", "shortability unknown", "microcap contamination"],
        "expected_failure_modes": [
            "lagged signal dominates live",
            "capacity-matched placebo dominates",
            "cost-adjusted spread fails",
            "signal is only size or liquidity exposure",
        ],
    },
    "momentum_low_vol": {
        "market_pain_point": "Raw momentum can be dominated by unstable high-volatility winners rather than persistent risk-adjusted demand.",
        "mechanism_hypothesis": "12-1 momentum penalized by 3m volatility should isolate steadier trend continuation.",
        "investor_constraint_or_behavior": "Risk-managed and benchmark-aware investors may prefer smoother winners and avoid volatile round trips.",
        "expected_universe": "Daily PIT price-volume equities with enough 12m history and investable liquidity.",
        "expected_regime": "Should work in broad trend regimes and weaken during momentum crashes or sharp reversals.",
        "why_not_arbitraged_away": "Crash risk, turnover, and volatility-timing errors limit capital deployment.",
        "observable_pre_formula_diagnostics": [
            "Check whether low-vol winners beat high-vol winners before formula tuning.",
            "Run industry-neutral and capacity-filtered diagnostics before any candidate claim.",
        ],
        "placebo_design": [
            "lagged signal placebo",
            "rebalance-date shifted placebo",
            "random same-coverage placebo",
            "future-return leakage negative control",
        ],
        "cost_capacity_risks": ["momentum crowding", "turnover", "capacity in low-liquidity names", "spread drag"],
        "expected_failure_modes": [
            "industry-neutral placebo dominates",
            "capacity-filtered variant collapses",
            "3m horizon reverses",
            "random same-coverage placebo beats live",
        ],
    },
    "revision_confirmed_earnings_underreaction": {
        "market_pain_point": "Analyst revision and earnings-confirmation information may diffuse slowly after public event availability.",
        "mechanism_hypothesis": "Positive revision acceleration confirmed by post-announcement event evidence may underreact after tradability lag.",
        "investor_constraint_or_behavior": "Analyst coverage, post-earnings processing, benchmark constraints, and next-event risk can slow repricing.",
        "expected_universe": "Names with PIT analyst estimates, event timestamps, price coverage, industry labels, and ADV coverage.",
        "expected_regime": "Should work when earnings information is clean and price coverage matches event coverage.",
        "why_not_arbitraged_away": "Timestamp proof, event-window uncertainty, capacity, and next-announcement risk limit clean arbitrage.",
        "observable_pre_formula_diagnostics": [
            "Audit public availability and tradability timestamps before scoring.",
            "Verify revision-only, event-confirmed, industry-neutral, and capacity-filtered variants separately.",
            "Check event-date shift and timestamp permutation placebos.",
        ],
        "placebo_design": [
            "shifted event date placebo",
            "random same-coverage placebo",
            "permuted revision timestamp placebo",
            "industry-only placebo",
            "short-term return-only placebo",
        ],
        "cost_capacity_risks": ["ADV capacity", "event crowding", "shortability", "next-announcement exclusion", "price coverage mismatch"],
        "expected_failure_modes": [
            "timestamp anchor blocked",
            "placebo event shift dominates",
            "capacity-filtered version collapses",
            "daily price coverage does not match event universe",
        ],
    },
    "sue_event_timing": {
        "market_pain_point": (
            "Earnings-event signals can look strong when the tradable anchor is late or when shifted-event "
            "windows capture pre-event drift rather than post-announcement underreaction."
        ),
        "mechanism_hypothesis": (
            "SUE can only become a valid design family after an auditable public-availability timestamp proves "
            "when the surprise became tradable."
        ),
        "investor_constraint_or_behavior": (
            "Event-driven, benchmark, and analyst-following investors react around earnings, but the exploitable "
            "lag depends on timestamp visibility rather than stronger shifted returns."
        ),
        "expected_universe": "Earnings events with linked securities, CRSP price windows, and auditable release timestamps.",
        "expected_regime": "Event windows where post-announcement repricing is observable after publication, not before it.",
        "why_not_arbitraged_away": (
            "Timestamp uncertainty, announcement timing, liquidity, and event crowding limit reliable exploitation."
        ),
        "observable_pre_formula_diagnostics": [
            "Audit actual-EPS public availability before choosing any tradable anchor.",
            "Compare current, announcement-date, and shifted anchors without using stronger shifted returns as proof.",
            "Separate pre-event drift from post-announcement delayed repricing.",
        ],
        "placebo_design": [
            "event-date shifted placebo",
            "pre-event drift control",
            "same-coverage random placebo",
            "timestamp-source unavailable control",
        ],
        "cost_capacity_risks": ["event crowding", "spread around announcements", "ADV coverage", "shortability"],
        "expected_failure_modes": [
            "no auditable earlier timestamp",
            "shifted placebo dominates",
            "pre-event drift explains effect",
            "price window coverage blocks validation",
        ],
    },
    "price_momentum": {
        "market_pain_point": "price trend persistence may reflect slow information diffusion or persistent demand.",
        "mechanism_hypothesis": "Recent winners continue only if demand or information adjustment is not complete.",
        "investor_constraint_or_behavior": "Slow-moving capital, benchmark constraints, and delayed attention.",
        "expected_universe": "Liquid enough equities where trend continuation can be traded after timestamp lag.",
        "expected_regime": "Trending regimes; should weaken in violent reversals and crowded momentum unwind periods.",
        "why_not_arbitraged_away": "Crowding, risk limits, turnover, and crash risk limit arbitrage capital.",
        "observable_pre_formula_diagnostics": [
            "Trend persistence by horizon before formula selection.",
            "Reversal and crash-regime sensitivity before candidate promotion.",
        ],
        "placebo_design": ["lag-shifted trend placebo", "same-coverage shuffled signal", "sector-neutral placebo"],
        "cost_capacity_risks": ["turnover", "momentum crowding", "spread drag", "capacity in smaller names"],
        "expected_failure_modes": ["momentum crash", "sector beta explanation", "mega-cap growth exposure"],
    },
    "sector_neutral_residual_momentum": {
        "market_pain_point": "Within-sector relative strength may reveal stock-specific underreaction.",
        "mechanism_hypothesis": "Residual winners outperform if stock-specific information diffuses slowly.",
        "investor_constraint_or_behavior": "Sector allocators and benchmark-relative investors may adjust single-name weights slowly.",
        "expected_universe": "Names with reliable PIT sector classification and enough within-sector breadth.",
        "expected_regime": "Should work better when stock dispersion is high and sector rotation does not dominate.",
        "why_not_arbitraged_away": "Sector neutrality reduces but does not remove turnover, crowding, and capacity constraints.",
        "observable_pre_formula_diagnostics": [
            "Within-sector dispersion and breadth check.",
            "Residual signal must not be rank-identical to raw momentum.",
        ],
        "placebo_design": ["sector-shuffled placebo", "raw momentum control", "same-sector random signal"],
        "cost_capacity_risks": ["sector concentration", "turnover", "ADV capacity", "shortability for short leg"],
        "expected_failure_modes": ["sector classification drift", "raw momentum duplicate", "capacity-matched placebo dominance"],
    },
    "trend_quality": {
        "market_pain_point": "Endpoint returns can confuse smooth trend with noisy round trips.",
        "mechanism_hypothesis": "Smooth directional pressure may be more persistent than choppy endpoint momentum.",
        "investor_constraint_or_behavior": "Trend-following and risk-managed flows may favor stable paths.",
        "expected_universe": "Daily price histories with enough observations for path-quality measurement.",
        "expected_regime": "Should improve in persistent trend regimes and weaken after discontinuous news jumps.",
        "why_not_arbitraged_away": "False positives, lag, and turnover limit exploitation.",
        "observable_pre_formula_diagnostics": ["Path smoothness distribution", "Endpoint-return duplicate audit"],
        "placebo_design": ["endpoint momentum control", "path-shuffled placebo", "same-coverage shuffled signal"],
        "cost_capacity_risks": ["lagged entries", "turnover", "crowding in smooth winners"],
        "expected_failure_modes": ["becomes momentum clone", "lags regime shifts", "placebo path smoothness wins"],
    },
    "path_fragility": {
        "market_pain_point": "Current distance from highs misses intrawindow path damage.",
        "mechanism_hypothesis": "Large peak-to-trough damage may indicate fragile demand or impaired risk appetite.",
        "investor_constraint_or_behavior": "Drawdown-sensitive investors may reduce exposure after severe path damage.",
        "expected_universe": "Assets with reliable daily adjusted price paths.",
        "expected_regime": "Should matter more in risk-off and deleveraging regimes.",
        "why_not_arbitraged_away": "Drawdown signals can be risk premia and are costly to short in stressed names.",
        "observable_pre_formula_diagnostics": ["Peak-to-trough distribution", "distance-to-high duplicate audit"],
        "placebo_design": ["price-to-high control", "volatility-matched placebo", "rebalance-shifted placebo"],
        "cost_capacity_risks": ["wide spreads in damaged names", "shortability", "gap risk"],
        "expected_failure_modes": ["volatility proxy only", "mean reversion dominates", "cost drag"],
    },
    "overshoot_reversal": {
        "market_pain_point": "Short-term forced-flow overshoots may create mean-reversion pressure.",
        "mechanism_hypothesis": "Recent moves reverse only when they extend an existing trend under high pressure.",
        "investor_constraint_or_behavior": "De-risking, stop-outs, and attention shocks may temporarily overshoot prices.",
        "expected_universe": "Liquid names where reversal can survive spread and impact.",
        "expected_regime": "Should work in liquidity-stress overshoot windows and fail in persistent trend regimes.",
        "why_not_arbitraged_away": "High turnover, adverse selection, and crash continuation risk limit arbitrage.",
        "observable_pre_formula_diagnostics": ["Overshoot event frequency", "prior-trend gate activation rate"],
        "placebo_design": ["negative momentum control", "volatility-matched placebo", "event-window shifted placebo"],
        "cost_capacity_risks": ["high turnover", "spread", "impact", "shortability"],
        "expected_failure_modes": ["pure negative momentum clone", "trend continuation", "cost-adjusted failure"],
    },
    "liquidity_shock": {
        "market_pain_point": "Abnormal trading activity can reveal attention or liquidity-regime changes.",
        "mechanism_hypothesis": "Dollar-volume shocks matter only if they proxy for attention, flows, or tradability changes.",
        "investor_constraint_or_behavior": "Liquidity-sensitive capital may enter or exit after activity shocks.",
        "expected_universe": "Names with reliable dollar-volume histories and investable ADV.",
        "expected_regime": "Event-driven and attention-driven regimes; should fail when volume is only noise.",
        "why_not_arbitraged_away": "Attention shocks can be transient and expensive to trade.",
        "observable_pre_formula_diagnostics": ["ADV shock persistence", "event/news concentration check"],
        "placebo_design": ["ADV-level control", "same-capacity shuffled signal", "rebalance-shifted placebo"],
        "cost_capacity_risks": ["turnover", "spread", "impact", "capacity illusion"],
        "expected_failure_modes": ["size/liquidity exposure", "event contamination", "placebo volume shock dominance"],
    },
    "capacity_level": {
        "market_pain_point": "Investability and capacity can determine whether a signal can be expressed.",
        "mechanism_hypothesis": "Persistent dollar volume is a context and capacity proxy, not alpha by itself.",
        "investor_constraint_or_behavior": "Institutional participation is constrained by ADV and impact budgets.",
        "expected_universe": "All candidate universes where execution survival matters.",
        "expected_regime": "Capacity level should explain tradability more than directional return.",
        "why_not_arbitraged_away": "It is not an alpha thesis; it controls implementation feasibility.",
        "observable_pre_formula_diagnostics": ["ADV breadth", "capacity frontier", "liquidity exposure attribution"],
        "placebo_design": ["capacity-matched placebo", "value-weight control", "same-coverage shuffled signal"],
        "cost_capacity_risks": ["capacity concentration", "large-cap beta", "liquidity exposure"],
        "expected_failure_modes": ["treated as alpha", "pure size/capacity exposure", "capacity-matched placebo wins"],
    },
    "turnover_shock": {
        "market_pain_point": "Abnormal turnover can reveal attention, flow pressure, or temporary crowding.",
        "mechanism_hypothesis": "Turnover shocks matter only when they reflect non-random flow imbalance.",
        "investor_constraint_or_behavior": "Flow-driven investors and liquidity providers adjust around abnormal activity.",
        "expected_universe": "Names with PIT shares outstanding or float coverage.",
        "expected_regime": "Attention or forced-flow regimes; should fail when activity is noisy.",
        "why_not_arbitraged_away": "Turnover shocks decay quickly and can be expensive to trade.",
        "observable_pre_formula_diagnostics": ["Turnover shock persistence", "shares-data quality audit"],
        "placebo_design": ["volume-only proxy control", "capacity-matched placebo", "rebalance-shifted placebo"],
        "cost_capacity_risks": ["turnover", "spread", "impact", "shares-data degradation"],
        "expected_failure_modes": ["volume proxy only", "event noise", "cost-adjusted failure"],
    },
    "turnover_trend": {
        "market_pain_point": "Persistent changes in trading activity may show improving or deteriorating sponsorship.",
        "mechanism_hypothesis": "Turnover trend matters if active participation changes persist rather than spike.",
        "investor_constraint_or_behavior": "Institutional accumulation or distribution may appear as gradual activity trend.",
        "expected_universe": "Names with reliable volume and shares histories.",
        "expected_regime": "Gradual accumulation/distribution regimes; should fail in one-off event spikes.",
        "why_not_arbitraged_away": "The signal is noisy and can be capacity or attention exposure.",
        "observable_pre_formula_diagnostics": ["Turnover trend persistence", "shock-vs-trend separation audit"],
        "placebo_design": ["turnover shock control", "capacity-matched placebo", "same-coverage shuffled signal"],
        "cost_capacity_risks": ["activity crowding", "spread", "impact", "capacity concentration"],
        "expected_failure_modes": ["liquidity exposure only", "event contamination", "placebo dominance"],
    },
}
