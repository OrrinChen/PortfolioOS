# SUE Event-Date-Shift Placebo Failure Attribution

H1E did not select a production SUE score.
This phase diagnoses placebo failure only.
No Q2, optimizer-path, paper, live, broker, order, or production workflow is opened.
If placebo failure is unresolved, SUE remains mixed and should not enter typed projection.

## Summary

- schema_version: `sue_placebo_failure_attribution.v1`
- score_name: `surprise_pct_actual_eps`
- interpretation: `placebo_failure_due_to_market_regime`
- live_primary_mean_rank_ic: `0.055597`
- live_primary_mean_top_bottom_spread: `0.020484`
- best_placebo_shift: `-5`
- q2_evaluation_ran: `False`
- optimizer_path_evaluation_ran: `False`
- production_approval_claimed: `False`

## Primary Window Timing Curve

| Shift | Mean Rank IC | Mean Top-Bottom Spread | Date Count |
| ---: | ---: | ---: | ---: |
| -10 | 0.170826 | 0.088502 | 217 |
| -5 | 0.191015 | 0.089401 | 220 |
| -2 | 0.129437 | 0.055756 | 220 |
| 0 | 0.055597 | 0.020484 | 220 |
| 2 | 0.075276 | 0.033436 | 220 |
| 5 | 0.062452 | 0.027873 | 220 |
| 10 | 0.045631 | 0.010412 | 220 |

## Window Audit

- shifted_anchors_used: `True`
- original_anchor_reused_for_shifted_windows: `False`
- event_available_after_tradable_violations: `0`
- live_return_window_start_before_tradable_count: `0`
- live_shift_window_overlap_rate: `0.332667`

## Denominator / Tail Audit

- low_denominator_count: `102`
- high_tail_event_count: `172`
- missing_coverage_encoded_as_zero_alpha: `False`
- no_view_not_zero_alpha: `True`

## Regime Audit

- sample_start: `2020-01-03`
- sample_end: `2021-01-01`
- includes_march_2020: `True`
- short_crash_rebound_window_possible: `True`

## Boundaries

- This diagnostic does not select a production SUE score.
- It does not run Q2 or optimizer-path evaluation.
- It does not promote Alpha Registry state.
- Downstream typed projection and Q2 require a separate explicit reopen.
