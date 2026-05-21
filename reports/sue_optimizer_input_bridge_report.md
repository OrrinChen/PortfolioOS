# SUE Optimizer Input Bridge Report

This proves local optimizer-path integration only.
This does not prove real historical SUE alpha.
This does not prove paper readiness or production approval.
This does not create broker/order/live workflows.
Q2 rows in this report are based on actual local optimizer outputs, not adapter-hook mapping.

## Summary

- bridge_status: `observed`
- expected_return_reached_actual_optimizer_input: `true`
- optimizer_decision_used_typed_expected_return: `true`
- sue_rank_weight_alignment_observed: `true`
- sign_flip_reversal_observed: `true`
- scaled_alpha_monotonicity_observed: `true`
- no_view_not_encoded_as_zero: `true`
- actual_optimizer_output_rows: `7`
- adapter_hook_only: `false`
- production_approval_claimed: `false`

The unscaled deterministic SUE row can still be dominated by existing risk, target, cost, and repair constraints. Directional optimizer response is therefore reported through the local sign-flip and scale-grid diagnostics, not as a real SUE alpha success claim.

## Boundaries

- no live data workflow was added
- no broker workflow was added
- no order workflow was added
- no paper canary was approved
- no production alpha approval is claimed
