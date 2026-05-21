# Expanded Historical SUE Event Evidence Grid

This is expanded WRDS/PIT historical evidence, not production approval.
It does not prove full historical SUE alpha.
It does not prove paper readiness or production approval.
It does not approve paper trading, live trading, broker workflows, orders, or production deployment.
It does not run Q2 or optimizer-path evaluation.
This expanded sample must still be diagnosed before stronger claims.

## Summary

- interpretation: `sue_expanded_evidence_mixed`
- pit_safe_rows: `15365`
- safe_rebalance_dates: `253`
- best_window: `plus_2_plus_22`
- best_window_mean_rank_ic: `0.039434`
- best_window_rank_ic_t_stat: `1.948866`
- best_window_mean_top_bottom_spread: `-0.006599`

## Event Window Grid

| Window | Rows | Mean Rank IC | Rank IC t | Mean Top-Bottom Spread | Spread t |
| --- | ---: | ---: | ---: | ---: | ---: |
| `plus_2_plus_2` | 15315 | -0.016989 | -0.822164 | 0.000022 | 0.007352 |
| `plus_2_plus_3` | 15315 | -0.024299 | -1.168167 | -0.005512 | -1.271341 |
| `plus_2_plus_22` | 15267 | 0.039434 | 1.948866 | -0.006599 | -0.464123 |

## Diagnostics

- placebo_diagnostics_generated: `True`
- missing_return_window_count: `198`
- missing_coverage_encoded_as_zero_alpha: `False`
- no_view_not_zero_alpha: `True`
- forward_return_feature_columns_detected: `[]`

## Boundaries

- This phase evaluates event-window evidence only.
- Missing SUE, price, or return coverage remains unavailable/no_view, not zero alpha.
- Alpha Registry status is not promoted by this report.
- Downstream typed AlphaView, Q2, and optimizer-path evaluation require separate explicit reopen phases.
