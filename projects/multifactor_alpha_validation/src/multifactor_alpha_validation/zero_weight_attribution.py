from __future__ import annotations

import pandas as pd


def zero_weight_reason(row: pd.Series) -> str:
    decision = str(row["decision"])
    posterior = float(row["posterior_expected_return"])
    turnover = float(row["incremental_turnover"])
    if decision == "diagnostic_only":
        return "no_view"
    if decision == "real_but_redundant":
        return "high_redundancy"
    if turnover >= 0.75:
        return "high_turnover"
    if decision == "archive_no_marginal_value":
        return "low_posterior_alpha"
    if posterior <= 0.001:
        return "low_posterior_alpha"
    if decision == "needs_more_evidence":
        return "insufficient_evidence"
    return "constraint_bound"
