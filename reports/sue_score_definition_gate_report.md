# Scale-Aware SUE Score Definition Gate

This is Reopen-H1E, a pre-registered scale-aware SUE score definition gate.
raw EPS diff is diagnostic-only after this phase.
Scale-aware SUE is a candidate, not production approval.
This phase does not run Q2 or optimizer-path evaluation.
This phase does not prove paper readiness.
This phase does not create broker/order/live workflows.
Downstream typed projection and Q2 require a separate explicit reopen.

## Selection Summary

- interpretation: `scale_aware_sue_mixed`
- selected_score: `None`
- provisional_score: `surprise_pct_actual_eps`
- raw_eps_diff_diagnostic_only: `True`
- placebo_passed: `False`
- event_date_shift_passed: `False`
- provisional_denominator_guard_passed: `True`
- provisional_tail_concentration_passed: `True`
- denominator_guard_passed: `False`
- tail_concentration_passed: `False`
- month_breadth: `13`
- year_breadth: `2`

## Candidate Score Grid

| Score | Diagnostic Only | Selected | Primary Rank IC | Primary Spread | Denominator Pass | Tail Pass | Placebo Pass |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `raw_eps_diff` | True | False | 0.042332 | -0.007180 | False | True | False |
| `surprise_pct_actual_eps` | False | False | 0.053752 | 0.020775 | True | True | False |
| `surprise_pct_expected_eps` | False | False | 0.051666 | 0.011613 | True | True | False |
| `surprise_scaled_price` | False | False | 0.053147 | 0.015899 | True | False | False |
| `surprise_scaled_eps_vol` | True | False |  |  | False | False | False |

## Primary Window Metrics

| Score | Window | Rows | Mean Rank IC | Rank IC t | Mean Top-Bottom Spread | Spread t | Status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `raw_eps_diff` | `plus_2_plus_22` | 15267 | 0.042332 | 2.103846 | -0.007180 | -0.502992 | `observed` |
| `surprise_pct_actual_eps` | `plus_2_plus_22` | 15267 | 0.053752 | 2.590522 | 0.020775 | 1.548520 | `observed` |
| `surprise_pct_expected_eps` | `plus_2_plus_22` | 15267 | 0.051666 | 2.492335 | 0.011613 | 0.882972 | `observed` |
| `surprise_scaled_price` | `plus_2_plus_22` | 15267 | 0.053147 | 2.586463 | 0.015899 | 0.928886 | `observed` |
| `surprise_scaled_eps_vol` | `plus_2_plus_22` | 15267 |  |  |  |  | `unavailable` |

## Boundary

- Raw EPS difference remains available only as a diagnostic comparator.
- Missing denominators, prices, or return windows remain unavailable/no_view, not zero alpha.
- Failed variants are reported instead of hidden.
- No Alpha Registry promotion is made by this report.
