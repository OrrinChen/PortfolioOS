# US Alpha Core Qualification Artifact Contract (2026-04-16)

## Purpose

This contract freezes the artifact shape for every Week 2-3 candidate run.

The point is to make every candidate comparable without inventing new report structure mid-sprint.

## Artifact Root

Each candidate run must write one artifact bundle under:

`outputs/us_alpha_core_restart/<candidate_id>/<run_date>/`

Example:

`outputs/us_alpha_core_restart/A1/2026-04-16/`

The bundle must contain exactly the files listed below.

## Required Files

### 1. `summary.json`

Single-file machine-readable gate summary.

Required top-level fields:

- `candidate_id`
- `family_id`
- `candidate_name`
- `as_of_date`
- `universe_name`
- `decision_grid`
- `pit_lag_rule`
- `input_spec`
- `baseline_comparator_id`
- `coverage_median`
- `coverage_retention_after_liquidity_cut`
- `gross_to_net_retention`
- `oos_mean_rank_ic`
- `oos_rank_ic_tstat`
- `oos_mean_alpha_only_spread`
- `oos_alpha_only_tstat`
- `subperiod_rank_ic`
- `subperiod_positive_count`
- `subperiod_min_rank_ic`
- `spread_corr_vs_baseline`
- `rank_ic_improvement_vs_baseline`
- `rank_ir_improvement_vs_baseline`
- `winner_gate_pass`
- `admission_gate_pass`
- `subperiod_gate_pass`
- `orthogonality_gate_pass`
- `notes`

### 2. `oos_metrics.csv`

One-row tabular version of the key OOS metrics.

Required columns:

- `candidate_id`
- `coverage_median`
- `gross_to_net_retention`
- `oos_mean_rank_ic`
- `oos_rank_ic_tstat`
- `oos_mean_alpha_only_spread`
- `oos_alpha_only_tstat`
- `baseline_id`
- `baseline_mean_rank_ic`
- `baseline_mean_alpha_only_spread`
- `rank_ic_improvement_vs_baseline`
- `rank_ir_improvement_vs_baseline`
- `alpha_spread_improvement_vs_baseline`

### 3. `coverage_by_month.csv`

Monthly effective-coverage trace.

Required columns:

- `date`
- `candidate_id`
- `eligible_universe_count`
- `raw_signal_count`
- `effective_signal_count`
- `effective_coverage_ratio`
- `effective_coverage_after_liquidity_cut`

### 4. `spread_series.csv`

Monthly top-bottom spread time series used for both economics and orthogonality.

Required columns:

- `date`
- `candidate_id`
- `top_bucket_return`
- `bottom_bucket_return`
- `top_bottom_spread`
- `net_top_bottom_spread`
- `turnover`
- `benchmark_spread`

### 5. `subperiod_metrics.csv`

Three-row table, one row per equal-length OOS slice.

Required columns:

- `subperiod_id`
- `start_date`
- `end_date`
- `observation_count`
- `mean_rank_ic`
- `rank_ic_tstat`
- `mean_alpha_only_spread`
- `alpha_only_tstat`
- `positive_rank_ic_ratio`

### 6. `orthogonality_vs_baseline.csv`

Direct comparison against the strongest frozen baseline.

Required columns:

- `candidate_id`
- `baseline_id`
- `spread_corr`
- `rank_ic_improvement_vs_baseline`
- `rank_ir_improvement_vs_baseline`
- `alpha_spread_improvement_vs_baseline`
- `coverage_delta_vs_baseline`
- `retention_delta_vs_baseline`

### 7. `note.md`

Human-readable run note with the same structure for every candidate.

Required sections:

- `Candidate`
- `Definition`
- `Coverage`
- `Economics`
- `Subperiod Stability`
- `Orthogonality Vs Baseline`
- `Gate Decision`
- `Keep / Stop`

## Frozen Gate Logic

Every candidate note and `summary.json` must evaluate the same Week 1 gates:

### Admission Gate

All must pass:

- `oos_mean_rank_ic > 0`
- `oos_rank_ic_tstat >= 2.0`
- `oos_alpha_only_tstat >= 2.0`
- `coverage_median >= 0.70`
- `gross_to_net_retention >= 0.50`

### Subperiod Stability Gate

- split OOS window into exactly `3` equal-length slices
- at least `2` slices must have positive `mean_rank_ic`
- the weakest slice must satisfy `mean_rank_ic >= 0`

### Orthogonality Gate

- use time-series correlation of monthly top-bottom spreads
- candidate passes only if `spread_corr < 0.70`

### Winner Increment Gate

- compare to the strongest frozen baseline (`alt_momentum_4_1` until superseded by a fresh rerun)
- primary Week 4 increment read = `rank_ic_improvement_vs_baseline`
- secondary diagnostic read = `rank_ir_improvement_vs_baseline` when the runner emits a directly comparable IR
- candidate passes only if the primary comparable improvement is at least `15%`

## Baseline Comparison Rule

The artifact bundle must make clear whether the comparator is:

- `platform_native_comparable`
- `external_method_benchmark`
- or `historical_reference_only`

For Week 2-3 qualification, only `platform_native_comparable` baselines count toward the formal winner gate.

## Naming Rule

Candidate IDs are frozen:

- `A1`
- `A2`
- `A3`
- `B1`
- `B2`
- `B3`
- `C1`
- `C2`

No aliasing and no late-added suffixes are allowed during Week 2-3.

If a candidate needs a materially different definition, it is a new sprint, not a silent in-sprint variant.

## Non-Negotiable Scope Rules

- no parameter sweeps inside the artifact bundle
- no extra plots required for Week 2-3 admission
- no bootstrap confidence intervals before Week 5 finalist work
- no package-level blend tests before a single-factor winner exists
- no silent switch from direct comparator to external benchmark

## What Counts As A Completed Candidate Run

A candidate is considered properly evaluated only if:

1. all seven required files exist
2. all required columns / fields are present
3. the note and `summary.json` agree on the gate result
4. the bundle names one frozen baseline comparator explicitly

If any of the four conditions above fail, the candidate run is incomplete and may not be used in Week 4 winner discussion.
