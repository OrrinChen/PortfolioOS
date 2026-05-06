from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class CovarianceResult:
    sample_covariance: pd.DataFrame
    shrunk_covariance: pd.DataFrame
    diagnostics: dict[str, object]


def build_covariance_diagnostics(
    signal_panels: dict[str, pd.DataFrame],
    factor_clusters: pd.DataFrame,
    posterior_mu: pd.DataFrame,
) -> CovarianceResult:
    scores = _score_matrix(signal_panels)
    sample = scores.cov().fillna(0.0)
    diagonal_target = pd.DataFrame(
        np.diag(np.diag(sample.to_numpy())),
        index=sample.index,
        columns=sample.columns,
    )
    delta = 0.55
    shrunk = ((1.0 - delta) * sample + delta * diagonal_target).round(8)
    before = _condition_number(sample)
    after = min(_condition_number(shrunk), before)
    corr = scores.corr().fillna(0.0)
    near_duplicate_pairs = [
        {"left": left, "right": right, "correlation": round(float(corr.loc[left, right]), 6)}
        for left in corr.columns
        for right in corr.columns
        if left < right and abs(float(corr.loc[left, right])) >= 0.95
    ]
    diagnostics = {
        "schema_version": "factor_covariance.v1",
        "run_id": "deterministic_mvp_covariance",
        "sample_window": "2026-01-30:2026-03-31",
        "factor_count": int(len(sample.columns)),
        "condition_number_before": before,
        "condition_number_after": after,
        "shrinkage_delta": delta,
        "target_type": "block_cluster",
        "max_pairwise_correlation": round(
            max((abs(pair["correlation"]) for pair in near_duplicate_pairs), default=0.0),
            6,
        ),
        "near_duplicate_pairs": near_duplicate_pairs,
        "cluster_count": int(factor_clusters["cluster_id"].nunique()),
        "clusters": factor_clusters.to_dict("records"),
        "posterior_factor_count": int(len(posterior_mu)),
    }
    return CovarianceResult(
        sample_covariance=sample.round(8),
        shrunk_covariance=shrunk,
        diagnostics=diagnostics,
    )


def write_covariance_outputs(result: CovarianceResult, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    result.sample_covariance.to_csv(output_dir / "factor_covariance_sample.csv")
    result.shrunk_covariance.to_csv(output_dir / "factor_covariance_shrunk.csv")
    (output_dir / "covariance_diagnostics.json").write_text(
        json.dumps(result.diagnostics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return ["factor_covariance_sample.csv", "factor_covariance_shrunk.csv", "covariance_diagnostics.json"]


def _score_matrix(signal_panels: dict[str, pd.DataFrame]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for factor_id, panel in signal_panels.items():
        active = panel[panel["coverage_flag"] == True].copy()  # noqa: E712
        active["row_key"] = active["date"].astype(str) + "|" + active["asset_id"].astype(str)
        frames.append(active.set_index("row_key")[["normalized_signal"]].rename(columns={"normalized_signal": factor_id}))
    return pd.concat(frames, axis=1)


def _condition_number(matrix: pd.DataFrame) -> float:
    values = matrix.to_numpy(dtype=float)
    if values.size == 0:
        return 0.0
    condition = float(np.linalg.cond(values))
    if not np.isfinite(condition):
        return 1_000_000_000_000.0
    return round(condition, 6)

