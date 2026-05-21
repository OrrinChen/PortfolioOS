# Alpha Registry v2

This registry freezes alpha decision states after the typed-alpha closeout path.
production approval: not claimed
no live trading allowed by registry

## Summary

- registry_id: `portfolioos_alpha_registry_v2`
- operating_mode: `paper-stage only`
- entry_count: `8`
- primary_status_counts: `{'diagnostic_only': 2, 'canonical_pilot': 1, 'real_shadow_branch': 1, 'archived_no_marginal_value': 1, 'calibration_only': 1, 'background_partially_real': 1, 'rejected_leakage': 1}`

## Latest Decision History

| alpha_id | latest_decision_label | evidence_type | event_count | q2_observed_rows | q2_unavailable_rows |
|---|---|---|---|---|---|
| sue_pead | sue_expanded_fixture_q2_observed_survives | deterministic_expanded_fixture | 120 | 30 | 0 |
| revision_1m |  |  |  |  |  |
| sue_revision_composite |  |  |  |  |  |
| phase_1_5_bridge |  |  |  |  |  |
| qlib_fixed_horizon_revision |  |  |  |  |  |
| residual_momentum_reversal |  |  |  |  |  |
| ashare_anti_mom_21_5 |  |  |  |  |  |
| forward_return_leakage_fixture |  |  |  |  |  |

## Decision Table

| alpha_id | display_name | primary_status | stop_layer | source_phase |
|---|---|---|---|---|
| sue_pead | SUE / PEAD | canonical_pilot | q2_observed_survives_local_fixture | Phase 50-51 |
| revision_1m | revision_1m | real_shadow_branch | revision_marginal_value_gate | Phase 52 |
| sue_revision_composite | SUE + revision composite | archived_no_marginal_value | not_built_phase52_archive | Phase 52 |
| phase_1_5_bridge | Old real alpha package / Phase 1.5 bridge | diagnostic_only | typed_projection_activation_gap | Pre-Phase 35 alpha package audit |
| qlib_fixed_horizon_revision | Qlib fixed-horizon + revision | diagnostic_only | fixed_horizon_absorption_gap | Typed alpha closeout |
| residual_momentum_reversal | Residual momentum / residual reversal | calibration_only | placebo_dominance_calibration | Phase 62 locked future work |
| ashare_anti_mom_21_5 | A-share anti_mom_21_5 | background_partially_real | branch_frozen_requires_phase63_charter | Phase 63 locked future work |
| forward_return_leakage_fixture | Forward-return leakage fixtures | rejected_leakage | leakage_gate | Q1 / Evidence Bundle / Promotion Gate |

## Non-Claims

- no broker workflow
- no orders or trading instructions
- no production alpha approval
- no paper canary approval
- no new alpha research branch is opened by this registry
