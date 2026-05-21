# Factor Discovery Real-Data Validation Roadmap

This roadmap starts a separate real-data validation line after the completed
fixture sandbox line.

```text
FD sandbox fixture line
  -> FD real-data validation line
  -> Phase 64 import review
  -> Q1 / Promotion Gate / Q2
```

The real-data line asks whether candidate factors deserve typed validation. It
does not prove a formal strategy, approve production, enter Q2 directly, or
update Alpha Registry.

## Phase Outline

- FD-R0: Real Data Admission Contract
- FD-D0: Factor Design Layer Contract
- FD-R1: PIT Universe Builder
- FD-R2: Return / Corporate Action Engine
- FD-R3: Real Factor Replay
- FD-R4: True Rolling OOS Validation
- FD-R5: Placebo / Robustness / Family Test
- FD-R5.1: Candidate Failure Diagnosis
- FD-R5.1b: Formula Mechanism Separation Audit
- FD-R6: Marginal Value v2
- FD-R7: Allocator v2 / Shrinkage / Zero-Weight Attribution
- FD-R8: Cost / Capacity / Benchmark Survival v2
- FD-R4.1 / FD-R5.2: Rolling Weighting Reliability Gate
- FD-S0/S1/S3: Small-Cap Candidate Family v1
- FD-R9: Phase 64 Real-Data Import Review

## Current Batch

FD-D0 and FD-R0 through FD-R5.1 plus the formula-mechanism audit and FD-R4.1 /
FD-R5.2 weighting reliability gate are complete on local WRDS-backed data.
FD-D0 adds the mechanism-first design contract: market pain point, mechanism
hypothesis, investor constraint or flow, expected universe/regime, observable
pre-formula diagnostics, placebo design, cost/capacity risks, and expected
failure modes must be specified before formula validation.
FD-R0/R1/R2 validate data admission, PIT universe
construction, and return / corporate-action audit readiness. FD-R3 now replays
the 29 price-volume FactorSpecs with
`formula_version=price_volume_29_mechanism_v2` on the admitted daily PIT bundle
and writes the real-data factor panel, raw values, oriented scores,
cross-sectional ranks, formula hashes, fallback audit fields, coverage report,
timestamp audit, replay report, summary JSON, and parquet-status JSON under
`outputs/factor_discovery/real_data_daily/fd_r3/`. FD-R4 runs true rolling OOS
validation on the FD-R3 panel and writes rolling ICIR weights, OOS factor
scores, 1m/3m QQQ-relative decile spreads, validation report, and summary JSON
under `outputs/factor_discovery/real_data_daily/fd_r4/`. FD-R5 runs placebo,
robustness, and factor-family diagnostics under
`outputs/factor_discovery/real_data_daily/fd_r5/`. FD-R5.1 reads existing
FD-R3/R4/R5 artifacts and writes failure attribution under
`outputs/factor_discovery/real_data_daily/fd_r5_1/`. The formula-mechanism
audit reads the FD-R3 panel and writes mechanism, duplicate-cluster, and
rank-identity artifacts under `outputs/factor_discovery/research_mode/` plus
`reports/factor_formula_mechanism_v2_audit.md`. FD-R4.1 / FD-R5.2 reads the
FD-R3/R4/R5 artifacts and writes weighting estimator comparison, stability,
placebo comparison, failure diagnosis, and report artifacts under
`outputs/factor_discovery/research_mode/` plus
`reports/factor_discovery_weighting_reliability_report.md`.

Daily price-volume data has now been pulled into the local ignored WRDS cache
at `data/cache/wrds_multifactor/nasdaq100_daily_full10/`. The admitted daily
bundle includes raw open/close fields and Compustat sector metadata, and the
FD-R0/R1/R2 daily admission run returns `full_daily_price_volume_ready=true`.
FD-R3 v2 produced `row_count=615090`, `active_view_rows=600674`,
`explicit_abstain_rows=14416`, preserved all 29 factor IDs, and passed the
no same-close timestamp audit.
FD-R4 produced `rebalance_count=176`, `validation_rebalance_count=12`,
`test_rebalance_count=164`, `score_row_count=35322`, and `decile_row_count=352`.
The current FD-R4 real-data diagnostics are weak / negative on average in the
test window, so they do not support an alpha-success claim. FD-R5 returned
`placebo_status=failed_placebo_gate` and
`recommended_next_action=stop_before_allocator`. FD-R5.1 v2 says
data/timestamp, coverage, factor definition, and redundancy are not the primary
failure; rolling ICIR noise remains true. The formula-mechanism audit checked
406 factor pairs and found `hard_fail_pair_count=0`, meaning no exact value,
rank, or sign-flip duplicate pairs survived the v2 rewrite. The weighting
reliability gate returned `decision=close`, `best_estimator=equal_weight_all`,
`rolling_icir_overfit_noise_failure=true`, and `estimator_count=11`; learned
weighting estimators did not beat baseline and placebo requirements.
Marginal-value, allocator, and survival stages remain blocked for this candidate
set unless the weighting/candidate-family design is revised and replayed from
FD-R3/FD-R4.

FD-R5.1, the formula-mechanism audit, and the weighting reliability gate do not
allocate factor weights for a portfolio, claim alpha success, approve
production, enter Q2, or update Alpha Registry.

## Small-Cap Candidate Family v1

The small-cap line is separate from `price_volume_29_mechanism_v2`; it does not
try to rescue the closed price-volume weighting path. FD-S0 validates whether a
local PIT bundle has the required small-cap research fields: PIT market cap,
shares outstanding or float, adjusted/raw prices, volume, corporate-action
handling, delisting returns/events, exchange/share-class filters,
sector/industry, benchmark returns, ADV, and spread/proxy diagnostics. FD-S1
tiers the universe into `large_cap_control`, `small_cap_investable`, and
`microcap_quarantine`; microcap rows are diagnostic-only.

The first family shell is `small_cap_quality_residual_momentum_v1` with primary
signal `small_cap_quality_residual_momentum_6m_ex1m`. It is fixed single-signal
only and records `rolling_icir_used=false`, `ridge_weighting_used=false`, and
`learned_weighting_used=false`.

The current default NASDAQ100 daily bundle is correctly rejected for this line:
`decision_label=reject_data_admission`, with missing PIT market cap, shares
outstanding/float, and exchange/share-class filters. This is a data admission
block, not an alpha result.
