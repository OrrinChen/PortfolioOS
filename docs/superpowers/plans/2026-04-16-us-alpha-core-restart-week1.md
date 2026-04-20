# US Alpha Core Restart Week 1 Checklist

## Goal

Freeze the US factor-layer restart so Weeks 2-4 are execution rather than debate.

This checklist exists to eliminate drift before qualification begins.

## Required Outputs By End Of Week 1

### 1. Frozen Baseline Scorecard

Re-run all frozen baseline factors under the current:

- `rank_500_1500` universe
- evaluator
- impact-aware cost model

Required fields:

- coverage
- rank IC
- rank-IC t-stat
- alpha-only spread t-stat
- retention
- top-bottom spread time-series

### 2. Candidate Definition Sheet

Freeze all candidate definitions:

- `A1`: market-residual `84/21` momentum
- `A2`: sector-residual `84/21` momentum
- `A3`: vol-managed residual momentum
- `B1`: Amihud illiquidity level
- `B2`: illiquidity shock / change
- `B3`: abnormal-turnover-conditioned short-term reversal
- `C1`: idiosyncratic volatility
- `C2`: MAX effect / lottery proxy

For each candidate, freeze:

- exact formula
- direction
- PIT lag
- cadence
- required inputs

### 3. Qualification Contract

Freeze the evaluation contract:

- coverage threshold `>= 70%`
- retention threshold `>= 50%`
- OOS rank IC `> 0`
- rank-IC t-stat `>= 2.0`
- alpha-only spread t-stat `>= 2.0`
- subperiod rule:
  - 3 equal OOS slices
  - at least 2 positive
  - weakest slice `>= 0`
- orthogonality rule:
  - top-bottom spread time-series correlation with strongest baseline `< 0.70`
- winner increment:
  - `>= 15%` better than strongest frozen baseline on comparable rank-IC / IR

### 4. Artifact Template

Freeze one common artifact layout for all Week 2-3 candidate runs:

- summary JSON
- markdown note
- factor coverage table
- OOS metric table
- top-bottom spread series
- subperiod table
- orthogonality comparison vs strongest frozen baseline

### 5. Sidecar Containment

Record the sidecar policy tweak explicitly:

- `min_evaluation_dates: 20 -> 19`

Status:

- allowed as backlog
- not allowed to consume Week 1-4 mainline time
- not allowed to redefine winner criteria

## Exit Condition

Week 1 is complete only if all five outputs above are frozen.

If any candidate definition, gate, or baseline reference is still under debate at the end of Week 1, Week 2 must not start.
