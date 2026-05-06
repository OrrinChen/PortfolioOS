from __future__ import annotations

import pandas as pd


def choose_marginal_value_decision(row: pd.Series) -> tuple[str, str]:
    if row["q1_decision"] == "q1_diagnostic_only":
        return "diagnostic_only", "reference factor is diagnostic only"
    if row["incremental_net_spread"] <= 0:
        return "archive_no_marginal_value", "cost-adjusted marginal value is non-positive"
    if row["max_pairwise_correlation"] >= 0.95 and row["residual_ic_after_baseline"] < 0.05:
        return "real_but_redundant", "high correlation and low residual contribution"
    if row["marginal_value_score"] >= 0.15:
        return "promote_to_allocator", "positive residual evidence after cost and redundancy checks"
    return "needs_more_evidence", "marginal value is positive but below promotion threshold"

