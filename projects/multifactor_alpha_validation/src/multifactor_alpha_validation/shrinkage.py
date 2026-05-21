from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.schema import FactorSpec


@dataclass(frozen=True)
class ShrinkageResult:
    posterior_mu: pd.DataFrame
    summary: dict[str, object]


def build_shrinkage_results(
    specs: list[FactorSpec],
    factor_evidence_table: pd.DataFrame,
    marginal_value_decision_table: pd.DataFrame,
) -> ShrinkageResult:
    spec_map = {spec.factor_id: spec for spec in specs}
    evidence = factor_evidence_table.set_index("factor_id")
    rows: list[dict[str, object]] = []
    for marginal in marginal_value_decision_table.to_dict("records"):
        factor_id = str(marginal["factor_id"])
        evidence_row = evidence.loc[factor_id]
        raw_mu = float(evidence_row["top_bottom_spread"])
        evidence_strength = min(abs(float(evidence_row["raw_rank_ic_t"])) / 2.0, 1.0)
        stability_score = min(abs(float(evidence_row["rolling_stability"])), 1.0)
        coverage_score = float(evidence_row["coverage_ratio"])
        decay_score = 0.75 if "21d" in str(evidence_row["decay_profile"]) else 0.40
        marginal_score = min(float(marginal["marginal_value_score"]), 1.0)
        decision_multiplier = _decision_multiplier(str(marginal["decision"]))
        h = round(
            decision_multiplier
            * (0.30 * evidence_strength + 0.20 * stability_score + 0.20 * coverage_score + 0.15 * decay_score + 0.15 * marginal_score),
            6,
        )
        posterior = 0.0 if decision_multiplier == 0.0 else h * raw_mu
        rows.append(
            {
                "schema_version": "factor_shrinkage.v1",
                "factor_id": factor_id,
                "family_id": spec_map[factor_id].family_id,
                "decision": marginal["decision"],
                "raw_expected_return": round(raw_mu, 6),
                "prior_expected_return": 0.0,
                "posterior_expected_return": round(posterior, 6),
                "shrinkage_intensity": h,
                "prior_type": "zero_conservative",
                "evidence_strength": round(evidence_strength, 6),
                "stability_score": round(stability_score, 6),
                "coverage_score": round(coverage_score, 6),
                "decay_score": round(decay_score, 6),
                "marginal_value_score": round(marginal_score, 6),
                "reason_for_shrinkage": _reason(str(marginal["decision"])),
            }
        )
    posterior_mu = pd.DataFrame(rows)
    summary = {
        "schema_version": "factor_shrinkage_summary.v1",
        "factor_count": int(len(posterior_mu)),
        "parameters_preregistered": True,
        "prior_type": "zero_conservative",
        "rejected_factor_revival_allowed": False,
        "mean_shrinkage_intensity": round(float(posterior_mu["shrinkage_intensity"].mean()), 6),
    }
    return ShrinkageResult(posterior_mu=posterior_mu, summary=summary)


def write_shrinkage_outputs(result: ShrinkageResult, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    result.posterior_mu.to_csv(output_dir / "factor_posterior_mu.csv", index=False)
    (output_dir / "shrinkage_summary.json").write_text(
        json.dumps(result.summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return ["factor_posterior_mu.csv", "shrinkage_summary.json"]


def _decision_multiplier(decision: str) -> float:
    return {
        "promote_to_allocator": 0.85,
        "needs_more_evidence": 0.45,
        "real_but_redundant": 0.18,
        "archive_no_marginal_value": 0.0,
        "diagnostic_only": 0.0,
    }.get(decision, 0.0)


def _reason(decision: str) -> str:
    return {
        "promote_to_allocator": "evidence and marginal value support partial posterior weight",
        "needs_more_evidence": "positive but unconfirmed evidence receives moderate shrinkage",
        "real_but_redundant": "redundancy forces strong shrinkage",
        "archive_no_marginal_value": "archived factors are not revived by shrinkage",
        "diagnostic_only": "diagnostic factors are not allocator candidates",
    }.get(decision, "unsupported decision receives zero posterior")
