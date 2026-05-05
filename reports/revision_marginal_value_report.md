# Revision Marginal-Value Gate

This report is local-only. It decides whether revision should enter composite evaluation after SUE.
production approval: not claimed

## Decision

- gate_decision: `revision_real_but_no_marginal_value`
- composite_promotion_allowed: `false`
- beats_sue_adjusted_marginal_threshold: `false`

## PIT Source

- Required PIT source for analyst revision research: `WRDS`.
- observed_pit_source: `WRDS`
- FMP frozen estimate history is rejected as PIT-safe analyst revision data.

## Required Tests

| test | status | metric | value | details |
|---|---|---|---:|---|
| sue_only_baseline | passed | net_return | 0.010000 | Local SUE-only baseline fixture was evaluated. |
| revision_only_shadow_branch | passed | net_return | 0.011000 | Revision-only shadow branch was evaluated. |
| sue_revision_equal_composite | passed | net_return | 0.010400 | Equal composite branch was evaluated. |
| sue_revision_confidence_weighted_composite | passed | net_return | 0.010500 | Confidence-weighted composite branch was evaluated. |
| sue_residualized_against_revision | passed | residual_rank_ic_t | 3.000000 | SUE residual branch was evaluated. |
| revision_residualized_against_sue | passed | residual_rank_ic_t | 2.800000 | Revision residual branch was evaluated. |
| event_overlap_coverage_overlap | passed | coverage_overlap | 0.240000 | Event and coverage overlap are disclosed. |
| cost_aware_marginal_contribution | passed | net_improvement | -0.000800 | Cost-aware marginal contribution was evaluated and did not clear the threshold. |

## Marginal Metrics

| metric | value | threshold | passed |
|---|---:|---:|---|
| marginal_rank_ic_t | 3.100000 | 2.000000 | true |
| marginal_alpha_only_t | 2.800000 | 2.000000 | true |
| sue_adjusted_net_improvement | 0.000400 | 0.001000 | false |
| cost_aware_net_improvement | -0.000800 | 0.001000 | false |
| gross_to_net_retention | 0.460000 | 0.500000 | false |
| event_overlap_ratio | 0.180000 | 0.750000 | true |
| coverage_overlap_ratio | 0.240000 | 0.850000 | true |

## Decision Reasons

- SUE-adjusted net improvement failed threshold: 0.000400 >= 0.001000.
- cost-aware net improvement failed threshold: -0.000800 >= 0.001000.
- gross-to-net retention failed threshold: 0.460000 >= 0.500000.
- Revision remains a real shadow branch but does not clear the composite-evaluation gate.

## Non-Claims

- no live data workflow
- no broker workflow
- no orders or trading instructions
- no production alpha approval
- no conclusion that SUE or revision is ready for paper trading
