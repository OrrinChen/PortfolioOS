# SUE Score Definition Diagnostics

This is H1D score-definition diagnostics for the WRDS/PIT SUE historical panel.
It does not prove SUE alpha success.
It does not run Q2 or optimizer-path evaluation.
It does not approve paper trading, live trading, broker workflows, orders, or production deployment.
Alpha Registry status is not promoted by this report.

## Summary

- diagnostic_window: `plus_2_plus_22`
- preferred_diagnostic_score: `surprise_pct_actual_eps`
- raw_eps_diff_scale_warning: `True`
- missing_coverage_encoded_as_zero_alpha: `False`
- no_view_not_zero_alpha: `True`

raw EPS difference is not the preferred SUE score when scale-aware definitions produce a cleaner top-bottom diagnostic.

## Score Definition Grid

| Score | Window | Rows | Mean Rank IC | Rank IC t | Median Rank IC | Mean Top-Bottom Spread | Spread t | Status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `raw_eps_diff` | `plus_2_plus_2` | 15315 | -0.016989 | -0.822164 | -0.024148 | 0.000022 | 0.007352 | `observed` |
| `raw_eps_diff` | `plus_2_plus_3` | 15315 | -0.024299 | -1.168167 | -0.007075 | -0.005512 | -1.271341 | `observed` |
| `raw_eps_diff` | `plus_2_plus_22` | 15267 | 0.039434 | 1.948866 | -0.004501 | -0.006599 | -0.464123 | `observed` |
| `raw_eps_diff_winsorized_global` | `plus_2_plus_2` | 15315 | -0.016513 | -0.795928 | -0.024148 | 0.000022 | 0.007352 | `observed` |
| `raw_eps_diff_winsorized_global` | `plus_2_plus_3` | 15315 | -0.023942 | -1.152474 | -0.007016 | -0.005512 | -1.271341 | `observed` |
| `raw_eps_diff_winsorized_global` | `plus_2_plus_22` | 15267 | 0.040267 | 1.989134 | -0.004501 | -0.006525 | -0.458985 | `observed` |
| `surprise_pct_expected_eps` | `plus_2_plus_2` | 15315 | -0.017436 | -0.859690 | -0.020773 | -0.000319 | -0.099705 | `observed` |
| `surprise_pct_expected_eps` | `plus_2_plus_3` | 15315 | -0.015779 | -0.739466 | -0.009666 | -0.001434 | -0.324283 | `observed` |
| `surprise_pct_expected_eps` | `plus_2_plus_22` | 15267 | 0.054311 | 2.610377 | 0.015525 | 0.014104 | 1.061763 | `observed` |
| `surprise_pct_expected_eps_winsorized_global` | `plus_2_plus_2` | 15315 | -0.017252 | -0.850587 | -0.020773 | -0.000220 | -0.068643 | `observed` |
| `surprise_pct_expected_eps_winsorized_global` | `plus_2_plus_3` | 15315 | -0.016100 | -0.754490 | -0.009666 | -0.001615 | -0.364571 | `observed` |
| `surprise_pct_expected_eps_winsorized_global` | `plus_2_plus_22` | 15267 | 0.054181 | 2.603600 | 0.015525 | 0.014104 | 1.061763 | `observed` |
| `surprise_pct_actual_eps` | `plus_2_plus_2` | 15315 | -0.017425 | -0.845172 | -0.031525 | -0.001758 | -0.570342 | `observed` |
| `surprise_pct_actual_eps` | `plus_2_plus_3` | 15315 | -0.018969 | -0.911383 | -0.015155 | -0.004116 | -0.947534 | `observed` |
| `surprise_pct_actual_eps` | `plus_2_plus_22` | 15267 | 0.057052 | 2.760747 | 0.013515 | 0.022334 | 1.669660 | `observed` |
| `surprise_pct_actual_eps_winsorized_global` | `plus_2_plus_2` | 15315 | -0.017368 | -0.842127 | -0.031525 | -0.001758 | -0.570342 | `observed` |
| `surprise_pct_actual_eps_winsorized_global` | `plus_2_plus_3` | 15315 | -0.018917 | -0.908832 | -0.015122 | -0.004116 | -0.947534 | `observed` |
| `surprise_pct_actual_eps_winsorized_global` | `plus_2_plus_22` | 15267 | 0.057019 | 2.759228 | 0.013515 | 0.022334 | 1.669660 | `observed` |
| `price_scaled_raw_eps_diff` | `plus_2_plus_2` | 15315 | -0.040870 | -1.272987 | -0.011922 | -0.009635 | -2.121214 | `observed` |
| `price_scaled_raw_eps_diff` | `plus_2_plus_3` | 15315 | -0.071865 | -2.315488 | -0.046660 | -0.015543 | -2.994177 | `observed` |
| `price_scaled_raw_eps_diff` | `plus_2_plus_22` | 15267 | 0.028540 | 0.980443 | 0.003603 | -0.034451 | -1.264600 | `observed` |

## Boundary

- H1D diagnoses score definition and tail construction only.
- Missing SUE, price, or return coverage remains unavailable/no_view, not zero alpha.
- Downstream event evidence, typed projection, Q2, and optimizer-path work require separate explicit reopen phases.
