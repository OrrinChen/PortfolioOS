# SUE Event Timing / Anchor Definition Audit

This is an event timing and anchor audit only.
It does not select a production SUE score.
It does not run Q2 or optimizer-path evaluation.
It does not approve paper/live/broker/order/production workflows.
SUE remains blocked before typed projection unless anchor definition is corrected and the score gate is rerun.

## Summary

- schema_version: `sue_event_timing_anchor_audit.v1`
- score_name: `surprise_pct_actual_eps`
- interpretation: `anchor_definition_likely_late`
- selected_score: `None`
- best_anchor_definition: `shift_minus_5_td`
- best_pre_event_window: `minus_5_minus_1`
- q2_evaluation_ran: `False`
- optimizer_path_evaluation_ran: `False`
- production_approval_claimed: `False`

## Primary Anchor Grid

| Anchor | Rank IC | Top-Bottom Spread | Date Count |
| --- | ---: | ---: | ---: |
| `shift_minus_5_td` | 0.191015 | 0.089401 | 220 |
| `shift_minus_10_td` | 0.170826 | 0.088502 | 217 |
| `shift_minus_2_td` | 0.129437 | 0.055756 | 220 |
| `shift_plus_2_td` | 0.075276 | 0.033436 | 220 |
| `shift_plus_5_td` | 0.062452 | 0.027873 | 220 |
| `announcement_plus_2_td` | 0.068058 | 0.025758 | 220 |
| `current_tradable` | 0.055597 | 0.020484 | 220 |
| `announcement_plus_1_td` | 0.055388 | 0.020461 | 220 |
| `announcement_plus_0_td` | 0.064038 | 0.017141 | 220 |
| `shift_plus_10_td` | 0.045631 | 0.010412 | 220 |

## Pre-Event Drift Grid

| Window | Rank IC | Top-Bottom Spread |
| --- | ---: | ---: |
| `minus_5_minus_1` | 0.120486 | 0.037396 |
| `minus_2_minus_1` | 0.125072 | 0.029534 |
| `zero_plus_1` | 0.130691 | 0.028816 |
| `plus_2_plus_22` | 0.055597 | 0.020484 |
| `minus_10_minus_6` | 0.067046 | 0.013650 |
| `plus_2_plus_3` | -0.018227 | -0.006376 |

## Timing Quality

- after_close: `76`
- ambiguous_timing: `8529`
- announcement_time_known: `0`
- announcement_time_missing: `0`
- before_open: `6757`
- date_only: `3`
- diagnostic_only_timing: `0`

## Window Overlap Audit

- shifted_anchor_actually_changes_return_window: `True`
- shifted_placebo_window_bug_detected: `False`
- benchmark_window_uses_shifted_anchor: `True`
- market_adjusted_spread_uses_shifted_anchor: `True`
- event_available_after_tradable_violations: `0`

## Boundaries

- This audit does not promote Alpha Registry state.
- Missing SUE, denominator, price, or return coverage remains unavailable/no_view and is not encoded as zero alpha.
- If anchor definition is revised, H1E must be rerun before any typed projection or Q2 work.
