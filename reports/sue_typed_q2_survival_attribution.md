# SUE Execution-Survival Attribution Report

This report is local-only. SUE remains an integration benchmark and Q2 candidate.
production approval: not claimed

## Decision

- decision_label: `sue_q2_observed_survives`
- primary_stop_layer: `none`
- phase52_revision_marginal_value_should_proceed: `true`

## What This Proves

- SUE typed expected-return values can be represented and passed into the local optimizer input path.
- Configured local Q2 adapter rows can be observed, while unsupported rows would remain unavailable rather than fabricated.
- Observed rows expose gross, net, turnover, cost drag, and gross-to-net retention where local fixture hooks exist.

## What This Does Not Prove

- SUE alpha success is not proven by this local fixture attribution.
- Revision marginal value is not established by this report.
- Paper-stage readiness and production approval are not claimed.
- Unavailable intermediate hooks are not treated as zero performance.

## Attribution Layers

| layer | status | observed_rows | unavailable_rows | details |
|---|---|---:|---:|---|
| evidence | passed | 0 | 0 | SUE event evidence and Promotion Gate artifacts were accepted before Phase 50. |
| projection | observed | 0 | 0 | active_rebalance_count=1; active_name_count=2; expected_return_used_share=0.666667 |
| injection | observed | 0 | 0 | injection_status=injected; optimizer_rebalance_date=2026-02-27 |
| optimizer_response | observed | 0 | 0 | Phase 49 separately validated directional optimizer response to typed expected-return variants. |
| constraint_repair | observed | 30 | 0 | Observed local Q2 rows expose this metric where fixture hooks exist. |
| cost | observed | 30 | 0 | Observed local Q2 rows expose this metric where fixture hooks exist. |
| turnover | observed | 30 | 0 | Observed local Q2 rows expose this metric where fixture hooks exist. |
| coverage_abstain | observed | 0 | 0 | SUE uses explicit abstain for missing coverage; no_view is not silently encoded as zero alpha. |

## Alpha Failure vs Execution Failure

- alpha_failure_detected: `false`
- execution_failure_detected: `false`
- Interpretation: a local Q2 execution limitation is not the same thing as an alpha failure.

## Projection Sparsity vs Optimizer Response

- projection_sparsity_detected: `false`
- optimizer_failure_detected: `false`
- Interpretation: sparse or unavailable projection coverage is tracked separately from optimizer response.

## Phase 52 Recommendation

Proceed to Phase 52 as a marginal-value diagnostic. This is not a production approval; it only asks whether revision adds information beyond the SUE typed benchmark.

## Limitations

- Observed Q2 rows come from stable local fixture mappings, not live execution.
- The risk-controlled fixture layer uses the local naive_pro_rata mapping; richer optimizer risk diagnostics still require dedicated PortfolioOS hooks.
- The SUE fixture covers a small local typed projection and should be read as an integration benchmark.

## Safety Boundaries

- no live data workflow
- no broker workflow
- no orders or trading instructions
- no production alpha approval
