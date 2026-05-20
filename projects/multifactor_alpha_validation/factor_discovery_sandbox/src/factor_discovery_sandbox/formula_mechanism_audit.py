"""FD-R5.1 formula mechanism separation audit."""

from __future__ import annotations

import json
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from .factor_formula_registry import FORMULA_VERSION


HARD_IDENTITY_THRESHOLD = 0.999999
HIGH_CORRELATION_THRESHOLD = 0.95


@dataclass(frozen=True)
class FormulaMechanismAuditResult:
    """Artifact paths and summary for the v2 formula mechanism audit."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_formula_mechanism_audit(
    factor_panel_path: str | Path,
    output_dir: str | Path,
    report_path: str | Path,
) -> FormulaMechanismAuditResult:
    """Audit whether v2 factor ids are exact formula, rank, or sign-flip clones."""

    panel_file = Path(factor_panel_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)

    panel = pd.read_csv(panel_file)
    active = panel[panel["coverage_status"] == "active_view"].copy()
    score_column = "oriented_score" if "oriented_score" in active.columns else "normalized_value"
    active[score_column] = pd.to_numeric(active[score_column], errors="coerce")
    active = active[active[score_column].notna()]

    audit = _pairwise_audit(active, score_column)
    duplicates = _duplicate_clusters(audit)
    rank_identity = audit[
        [
            "factor_id_a",
            "factor_id_b",
            "rank_identity_rate",
            "exact_value_identity_rate",
            "sign_flip_identity_rate",
            "is_rank_duplicate",
            "is_formula_duplicate",
            "is_sign_flip_duplicate",
            "decision",
            "reason",
            "not_alpha_evidence",
            "direct_q2_entry_allowed",
        ]
    ].copy()

    artifacts = {
        "formula_mechanism_audit": output_path / "formula_mechanism_audit.csv",
        "duplicate_cluster_audit": output_path / "duplicate_cluster_audit.csv",
        "rank_identity_audit": output_path / "rank_identity_audit.csv",
        "formula_mechanism_audit_report": report_file,
    }
    audit.to_csv(artifacts["formula_mechanism_audit"], index=False)
    duplicates.to_csv(artifacts["duplicate_cluster_audit"], index=False)
    rank_identity.to_csv(artifacts["rank_identity_audit"], index=False)

    hard_fail_pair_count = int((audit["decision"] == "hard_fail_formula_or_rank_duplicate").sum()) if not audit.empty else 0
    high_corr_pair_count = int((audit["decision"] == "review_high_correlation").sum()) if not audit.empty else 0
    factor_count = int(active["factor_id"].nunique()) if not active.empty else 0
    summary = {
        "schema_version": "fd_formula_mechanism_audit_summary.v1",
        "formula_version": FORMULA_VERSION,
        "factor_panel_path": str(panel_file),
        "factor_count": factor_count,
        "pair_count": int(len(audit)),
        "hard_fail_pair_count": hard_fail_pair_count,
        "high_correlation_review_pair_count": high_corr_pair_count,
        "formula_mechanism_validation_only": True,
        "allocator_entry_allowed": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "alpha_registry_entry_allowed": False,
        "direct_q2_entry_allowed": False,
        "not_alpha_evidence": True,
    }
    artifacts["formula_mechanism_audit_report"].write_text(_render_report(summary, audit, duplicates), encoding="utf-8")
    return FormulaMechanismAuditResult(summary=summary, artifacts=artifacts)


def _pairwise_audit(active: pd.DataFrame, score_column: str) -> pd.DataFrame:
    factors = sorted(active["factor_id"].dropna().astype(str).unique())
    meta = (
        active.drop_duplicates("factor_id", keep="last")
        .set_index("factor_id")
        .reindex(factors)
        .to_dict(orient="index")
    )
    rows = []
    for factor_a, factor_b in combinations(factors, 2):
        pair = _aligned_pair(active, score_column, factor_a, factor_b)
        pearson = _corr(pair, method="pearson")
        spearman = _corr(pair, method="spearman")
        exact_identity = _exact_identity_rate(pair["score_a"], pair["score_b"])
        rank_identity = _rank_identity_rate(active, score_column, factor_a, factor_b, sign=1.0)
        sign_flip_identity = _rank_identity_rate(active, score_column, factor_a, factor_b, sign=-1.0)
        formula_duplicate = exact_identity >= HARD_IDENTITY_THRESHOLD
        rank_duplicate = rank_identity >= HARD_IDENTITY_THRESHOLD
        sign_flip_duplicate = sign_flip_identity >= HARD_IDENTITY_THRESHOLD
        decision, reason = _decision(
            formula_duplicate=formula_duplicate,
            rank_duplicate=rank_duplicate,
            sign_flip_duplicate=sign_flip_duplicate,
            spearman=spearman,
        )
        rows.append(
            {
                "schema_version": "fd_formula_mechanism_audit.v1",
                "factor_id_a": factor_a,
                "factor_id_b": factor_b,
                "formula_version_a": str(meta.get(factor_a, {}).get("formula_version", FORMULA_VERSION)),
                "formula_version_b": str(meta.get(factor_b, {}).get("formula_version", FORMULA_VERSION)),
                "formula_hash_a": str(meta.get(factor_a, {}).get("formula_hash", "")),
                "formula_hash_b": str(meta.get(factor_b, {}).get("formula_hash", "")),
                "mechanism_family_a": str(meta.get(factor_a, {}).get("mechanism_family", "")),
                "mechanism_family_b": str(meta.get(factor_b, {}).get("mechanism_family", "")),
                "pearson_corr": pearson,
                "spearman_corr": spearman,
                "rank_identity_rate": rank_identity,
                "exact_value_identity_rate": exact_identity,
                "sign_flip_identity_rate": sign_flip_identity,
                "is_formula_duplicate": bool(formula_duplicate),
                "is_rank_duplicate": bool(rank_duplicate),
                "is_sign_flip_duplicate": bool(sign_flip_duplicate),
                "is_mechanism_duplicate": bool(formula_duplicate or rank_duplicate or sign_flip_duplicate),
                "decision": decision,
                "reason": reason,
                "not_alpha_evidence": True,
                "direct_q2_entry_allowed": False,
            }
        )
    return pd.DataFrame(rows)


def _aligned_pair(active: pd.DataFrame, score_column: str, factor_a: str, factor_b: str) -> pd.DataFrame:
    left = active[active["factor_id"] == factor_a][["date", "asset_id", score_column]].rename(
        columns={score_column: "score_a"}
    )
    right = active[active["factor_id"] == factor_b][["date", "asset_id", score_column]].rename(
        columns={score_column: "score_b"}
    )
    pair = left.merge(right, on=["date", "asset_id"], how="inner")
    pair["score_a"] = pd.to_numeric(pair["score_a"], errors="coerce")
    pair["score_b"] = pd.to_numeric(pair["score_b"], errors="coerce")
    return pair.dropna(subset=["score_a", "score_b"])


def _corr(pair: pd.DataFrame, method: str) -> float:
    if len(pair) < 3:
        return float("nan")
    if pair["score_a"].nunique() <= 1 or pair["score_b"].nunique() <= 1:
        return float("nan")
    value = pair["score_a"].corr(pair["score_b"], method=method)
    return float(value) if pd.notna(value) else float("nan")


def _exact_identity_rate(left: pd.Series, right: pd.Series) -> float:
    if left.empty:
        return 0.0
    return float(np.isclose(left.to_numpy(dtype="float64"), right.to_numpy(dtype="float64"), equal_nan=False).mean())


def _rank_identity_rate(active: pd.DataFrame, score_column: str, factor_a: str, factor_b: str, sign: float) -> float:
    matches = []
    left = active[active["factor_id"] == factor_a][["date", "asset_id", score_column]].rename(
        columns={score_column: "score_a"}
    )
    right = active[active["factor_id"] == factor_b][["date", "asset_id", score_column]].rename(
        columns={score_column: "score_b"}
    )
    pair = left.merge(right, on=["date", "asset_id"], how="inner")
    if pair.empty:
        return 0.0
    pair["score_a"] = pd.to_numeric(pair["score_a"], errors="coerce")
    pair["score_b"] = sign * pd.to_numeric(pair["score_b"], errors="coerce")
    pair = pair.dropna(subset=["score_a", "score_b"])
    for _date, group in pair.groupby("date"):
        if len(group) < 2:
            continue
        rank_a = group["score_a"].rank(method="average")
        rank_b = group["score_b"].rank(method="average")
        matches.extend(np.isclose(rank_a.to_numpy(), rank_b.to_numpy()).tolist())
    if not matches:
        return 0.0
    return float(np.mean(matches))


def _decision(
    *,
    formula_duplicate: bool,
    rank_duplicate: bool,
    sign_flip_duplicate: bool,
    spearman: float,
) -> tuple[str, str]:
    if formula_duplicate:
        return "hard_fail_formula_or_rank_duplicate", "exact oriented-score duplicate"
    if rank_duplicate:
        return "hard_fail_formula_or_rank_duplicate", "cross-sectional rank duplicate"
    if sign_flip_duplicate:
        return "hard_fail_formula_or_rank_duplicate", "pure sign-flip rank duplicate"
    if np.isfinite(spearman) and abs(spearman) >= HIGH_CORRELATION_THRESHOLD:
        return "review_high_correlation", "high correlation is review-only, not a hard fail"
    return "mechanism_separated", "no exact value, rank, or sign-flip duplicate detected"


def _duplicate_clusters(audit: pd.DataFrame) -> pd.DataFrame:
    if audit.empty:
        return pd.DataFrame(
            columns=[
                "schema_version",
                "cluster_id",
                "factor_id",
                "paired_factor_id",
                "duplicate_type",
                "decision",
                "not_alpha_evidence",
                "direct_q2_entry_allowed",
            ]
        )
    duplicate_rows = audit[audit["decision"] == "hard_fail_formula_or_rank_duplicate"].copy()
    rows = []
    for cluster_id, row in enumerate(duplicate_rows.itertuples(index=False), start=1):
        duplicate_type = _duplicate_type(row)
        for factor_id, paired_factor_id in [
            (row.factor_id_a, row.factor_id_b),
            (row.factor_id_b, row.factor_id_a),
        ]:
            rows.append(
                {
                    "schema_version": "fd_formula_duplicate_cluster_audit.v1",
                    "cluster_id": f"duplicate_cluster_{cluster_id:03d}",
                    "factor_id": factor_id,
                    "paired_factor_id": paired_factor_id,
                    "duplicate_type": duplicate_type,
                    "decision": row.decision,
                    "not_alpha_evidence": True,
                    "direct_q2_entry_allowed": False,
                }
            )
    return pd.DataFrame(rows)


def _duplicate_type(row: object) -> str:
    if bool(getattr(row, "is_formula_duplicate")):
        return "exact_value_duplicate"
    if bool(getattr(row, "is_rank_duplicate")):
        return "rank_duplicate"
    if bool(getattr(row, "is_sign_flip_duplicate")):
        return "sign_flip_rank_duplicate"
    return "unknown_duplicate"


def _render_report(summary: Mapping[str, object], audit: pd.DataFrame, duplicates: pd.DataFrame) -> str:
    hard_fail = audit[audit["decision"] == "hard_fail_formula_or_rank_duplicate"] if not audit.empty else audit
    high_corr = audit[audit["decision"] == "review_high_correlation"] if not audit.empty else audit
    lines = [
        "# Factor Formula Mechanism v2 Audit",
        "",
        "formula mechanism validation only",
        "not alpha evidence",
        "allocator entry: blocked",
        "Q1/Q2 entry: blocked",
        "Alpha Registry entry: blocked",
        "",
        f"- formula version: {summary['formula_version']}",
        f"- factor count: {summary['factor_count']}",
        f"- pair count: {summary['pair_count']}",
        f"- hard fail duplicate pairs: {summary['hard_fail_pair_count']}",
        f"- high-correlation review pairs: {summary['high_correlation_review_pair_count']}",
        "",
        "## Rules",
        "- exact value duplicates, rank duplicates, and pure sign-flip duplicates are hard failures.",
        "- high absolute correlation alone is review-only and does not fail mechanism separation.",
        "- this audit does not open allocator, Q1, Q2, production approval, or Alpha Registry paths.",
        "",
    ]
    if not hard_fail.empty:
        lines.extend(["## Hard Fail Pairs"])
        for row in hard_fail.itertuples(index=False):
            lines.append(f"- {row.factor_id_a} / {row.factor_id_b}: {row.reason}")
        lines.append("")
    if not high_corr.empty:
        lines.extend(["## High-Correlation Review Pairs"])
        for row in high_corr.head(25).itertuples(index=False):
            lines.append(f"- {row.factor_id_a} / {row.factor_id_b}: spearman={row.spearman_corr:.6f}")
        lines.append("")
    if duplicates.empty:
        lines.extend(["## Duplicate Clusters", "- no hard duplicate clusters detected", ""])
    return "\n".join(lines)
