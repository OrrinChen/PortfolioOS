# Multi-Factor Alpha Validation Report

## 1. Scope
This is a PIT-safe, redundancy-aware, cost-aware multi-factor validation system.
It is not a production trading system.

## 2. Factor Library
- factor_count: 10
- enabled_count: 8

## 3. Data and PIT Contract
- missing coverage is explicit abstain
- no_view != zero_alpha

## 4. Q1 Evidence
- evidence_rows: 9

## 5. Redundancy and Marginal Value
- cluster_count: 1
- promoted_count: 1

## 6. Shrinkage and Covariance
- mean_shrinkage_intensity: 0.158011
- condition_number_after: 19.000325

## 7. Factor Allocator
- active_factor_count: 1
- zero_weight_count: 8

## 8. Zero-Weight Attribution
| factor_id               | zero_weight_reason   |
|:------------------------|:---------------------|
| investment_asset_growth | high_redundancy      |
| liquidity_turnover      | high_redundancy      |
| low_vol_60d             | high_redundancy      |
| momentum_12_1           | high_redundancy      |
| profitability_quality   | high_redundancy      |
| reversal_5_1            | high_turnover        |
| sue_event_reference     | no_view              |
| value_bm                | high_redundancy      |

## 9. Cost and Capacity Survival
- cost_killed_rows: 24
- capacity_rows: 4

## 10. Final Registry
| factor_id                 | final_status                     | stop_layer             |
|:--------------------------|:---------------------------------|:-----------------------|
| accruals                  | cost_survived                    | cost_capacity_survival |
| analyst_revision_disabled | pit_rejected                     | pit_unavailable        |
| investment_asset_growth   | allocator_weight_zero_redundancy | allocator_zero         |
| liquidity_turnover        | allocator_weight_zero_redundancy | allocator_zero         |
| low_vol_60d               | allocator_weight_zero_redundancy | allocator_zero         |
| momentum_12_1             | allocator_weight_zero_redundancy | allocator_zero         |
| profitability_quality     | allocator_weight_zero_redundancy | allocator_zero         |
| reversal_5_1              | allocator_weight_zero_cost       | allocator_zero         |
| sue_event_reference       | q1_diagnostic_only               | allocator_zero         |
| value_bm                  | allocator_weight_zero_redundancy | allocator_zero         |

## 11. Non-Claims
- No production approval.
- No live trading.
- No security-level output.
- No direct Q2 entry.
- No claim that public factors are proprietary alpha.
