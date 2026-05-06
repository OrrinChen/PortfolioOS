from __future__ import annotations

import pandas as pd


def build_benchmark_attribution(factor_evidence_table: pd.DataFrame, factor_weights: pd.DataFrame) -> pd.DataFrame:
    weights = factor_weights[["factor_id", "weight"]]
    merged = factor_evidence_table.merge(weights, on="factor_id", how="left").fillna({"weight": 0.0})
    return pd.DataFrame(
        [
            {
                "schema_version": "benchmark_attribution.v1",
                "factor_id": row.factor_id,
                "raw_return": round(float(row.top_bottom_spread), 6),
                "benchmark_relative_return": round(float(row.benchmark_relative_spread), 6),
                "beta_adjusted_return": round(float(row.beta_adjusted_spread), 6),
                "sector_style_adjusted_return": round(float(row.style_adjusted_spread) * 0.95, 6),
                "liquidity_bucket_attribution": round(float(row.top_bottom_spread) * 0.18, 6),
                "allocator_weight": round(float(row.weight), 8),
            }
            for row in merged.itertuples(index=False)
        ]
    )

