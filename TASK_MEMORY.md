# TASK_MEMORY

This file is the concise handoff note for continuing PortfolioOS. It keeps only the current mainline state, stable conclusions, and the next useful steps.

## Project Snapshot

PortfolioOS is now a compliance-aware portfolio rebalance, scenario, approval, execution-simulation, backtest, TCA, and research CLI platform.

Current high-level state:

- Core platform buildout through Phase 12 is complete.
- Historical backtest, replay, scenario, approval/freeze, execution simulation, import profiles, data builders, and snapshot bundles are all implemented and stable.
- The runtime default execution simulation mode is `impact_aware`.
- `participation_twap` remains intentionally preserved in sample execution requests as the baseline comparison mode.
- A frozen expanded US research stack exists alongside the original A-share-oriented MVP.
- The repo is now a git repository on `main`; future edits should be committed.

Latest full regression on this machine:

- `python -m pytest -q` -> `307 passed, 38 warnings`

## Stable Platform Conclusions

### Workflow And Backtest Foundation

- The canonical single-run orchestration path is `src/portfolio_os/workflow/single_run.py`.
- Historical backtests are intended to run via library calls, not CLI subprocess chains.
- Shared workflow extraction is complete and is the correct integration point for further research features.

### Data, Providers, And Research Inputs

- Provider and builder infrastructure is complete for standard `market.csv`, `reference.csv`, `target.csv`, and snapshot bundles.
- Tushare permission-aware fallbacks are implemented.
- Full live snapshot generation is still limited by current Tushare `index_weight` permissions, so client-provided `target.csv` remains the official fallback path when needed.
- The expanded US research universe is frozen at 50 names and lives under:
  - `data/universe/us_equity_expanded_tickers.txt`
  - `data/universe/us_universe_reference.csv`
  - `data/universe/us_universe_market_2026-03-27.csv`
- Expanded US risk inputs live under:
  - `data/risk_inputs_us_expanded/returns_long.csv`
  - `data/risk_inputs_us_expanded/factor_exposure.csv`
  - `data/risk_inputs_us_expanded/risk_inputs_manifest.json`
- An external FMP Ultimate freeze now exists under:
  - `C:\Users\14574\Quant\fmp_data_freeze`
- Canonical FMP freeze summary lives at:
  - `C:\Users\14574\Quant\fmp_data_freeze\summary\fmp_coverage_summary.json`
- Stable FMP freeze snapshot on `2026-04-03`:
  - primary universe = `2671`
  - supplement universe = `607`
  - `blocked = false`
  - total stored size estimate = `23.416 GB`
- Stable FMP coverage conclusions:
  - main-list `has_all_core_quarterly_rate = 98.99%`
  - main-list `min_12_quarters_all_core_rate = 93.82%`
  - main-list `min_20_quarters_all_core_rate = 89.74%`
  - supplement `min_12_quarters_all_core_rate = 88.47%`
  - transcript backfill is complete and main-list `median_transcript_count = 48`
  - transcript-positive main tickers = `2322 / 2671`
- Important FMP caveats:
  - SEC filings endpoints were largely unavailable in this account regime (`stable` returned `400` requiring unsupported query shape; `api/v3` returned legacy `403`)
  - ESG endpoint was unavailable (`stable` `404`, `api/v3` legacy `403`)
  - the frozen `analyst_estimates_quarterly.json` payloads expose forward estimate levels by fiscal date, but do **not** include historical snapshot metadata such as `snapshot_date`, `as_of_date`, or `updated_at`
  - therefore the current FMP freeze cannot support a clean PIT analyst-revision alpha by itself; using the frozen estimate levels as if they were historical revision snapshots would introduce lookahead
  - if US fundamentals/transcript research resumes, use the frozen FMP workspace rather than re-probing `yfinance`
- Latest external fundamentals spike on `2026-04-03` lives under:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\fundamentals_spike`
  - canonical machine-readable outputs:
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\fundamentals_spike\coverage_check.json`
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\fundamentals_spike\fundamentals_spike_summary.json`
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\fundamentals_spike\alpha_only_vs_naive_summary.json`
- Stable fundamentals-spike conclusions:
  - the canonical `us_liquid_mid_large_300_2026-03-27` universe did **not** fully intersect the US-only FMP freeze
  - `covered_ticker_count = 254 / 300`
  - `covered_rate = 84.67%`
  - the missing set is mostly non-US registrants and dotted share classes inside the canonical 300-name list, so this is a dataset-scope mismatch, not a probe bug
  - despite the coverage miss, all `36` canonical rebalance dates evaluated successfully on the covered subset
  - composite signal density stayed strong on the covered subset:
    - `mean_scored_ticker_count = 253.69`
    - `signal_ready_rate = 1.0`
    - dates with coverage below `80%` = `0`
  - best single fundamentals factor was `book_to_price`
    - `mean_rank_ic = 0.02189`
    - `rank_ic_tstat = 0.8510`
    - `alpha_only_vs_naive_mean = 0.00529`
    - `alpha_only_vs_naive_tstat = 1.4613`
  - equal-weight composite was positive but weak
    - `mean_rank_ic = 0.01895`
    - `rank_ic_tstat = 0.7148`
    - `alpha_only_vs_naive_mean = 0.00207`
    - `alpha_only_vs_naive_tstat = 0.5541`
  - Phase 2.6 deterministic momentum baseline remains stronger:
    - `alpha_only_vs_naive_mean = 0.02156`
    - `alpha_only_vs_naive_tstat = 2.1261`
  - final probe decision:
    - `decision_branch = fundamentals_complementary`
    - `next_recommended_step = do momentum + fundamentals multi-factor`
  - do **not** treat standalone fundamentals as a superior replacement for deterministic momentum on current evidence

### Execution And Expanded-US Validation

- Expanded-US replay, single-run, and execution validation for `sample_us_04`, `sample_us_05`, and `sample_us_06` has already been completed successfully.
- The expanded US replay/sample stack is healthy enough to support further research work without first revisiting infrastructure.

## TCA And Cost Model State

### Fill Collection Infrastructure

- The fill-collection pipeline preserves `reference_price` end-to-end for calibration.
- Windows BOM-tolerant JSON handling is required for some calibration and broker-state artifacts.
- The constrained fill batch generator exists and supports:
  - sell-side inventory constraints via `--broker-positions-file`
  - buy-side total notional constraints via `--buying-power`
  - audit fields in `fill_collection_batch_manifest.json`

### Key Physical Constraint

- The ADV reachability check on the frozen 50-name US universe showed that high participation buckets are not physically reachable at the current account scale.
- For the current account + universe regime, realistic participation is effectively `0-0.1%`.
- This means the old "cover broad participation buckets and then promote" TCA path is not the right mainline.

### Current Calibration Regime

- A low-participation overlay sufficiency path now exists.
- `overlay_readiness = sufficient` can now trigger in the current production regime when:
  - `fit_eligible_count >= 30`
  - fit-eligible coverage includes both `buy` and `sell`
  - observed fit participation stays within `0-0.1%`
  - the existing signal-quality and model-improvement checks still pass
- This path is intentionally limited:
  - it supports paper-overlay readiness
  - it does not justify broad extrapolation
  - it does not justify default promotion

### Latest Closed TCA Result

- Low-participation live-fill closure has already been reached.
- Latest important machine-readable outcome:
  - `fit_eligible_count = 32`
  - `bidirectional_fit_coverage = true`
  - `sufficient_low_participation_coverage = true`
  - `overlay_readiness = sufficient`
  - `next_recommended_action = apply_as_paper_overlay`
  - `recommendation = provisional_only`
  - `recommendation_reason = low_participation_overlay_only`
  - `candidate_k = 3.498400399110418`
- Applicability boundary:
  - calibration is validated only for `0-0.1% participation`
  - do not extrapolate above that regime

### Methodological Caveat

- The current `k` fit still uses `positive_signal` observations only.
- That likely biases the fitted `k` upward.
- Including negative-signal eligible fills in the estimator remains an important follow-up, but it is not blocking the low-participation sufficiency closure.

## Cost Model And Optimizer Research Conclusions

### Calibrated Cost Model Decision

- A calibrated research config exists:
  - `config/us_expanded_tca_calibrated.yaml`
- Calibrated `k` should not be promoted into `config/us_expanded.yaml` yet.
- The decision rationale is recorded in:
  - `docs/cost_model_decision_note.md`

### Core Research Finding

- Across the `k = 0.015`, `k = 1.4457`, and `k = 3.4984` sweep rounds, the efficient frontier shifts downward as the cost model becomes more realistic.
- The problem is not just cost-multiplier tuning.
- The deeper issue is structural:
  - without an explicit alpha / expected-return term, the current optimizer mostly differentiates itself from naive via trade suppression
  - with more realistic costs, trade suppression sacrifices more holding return than it saves in cost

## Risk-Aversion Research State

- Risk-aversion sweep tooling is now implemented and exposed via:
  - `portfolio-os-risk-sweep`
- Both augment-mode and replace-mode research have already been run on real expanded-US data.

Main conclusion:

- Risk aversion now clearly moves portfolio construction away from naive, but no Sharpe-improving sweet spot was found.
- In augment mode:
  - volatility and drawdown improve as risk aversion rises
  - return, Sharpe, and relative performance versus naive worsen
- In replace mode:
  - the sweep is mostly degenerate until extreme multipliers
  - the first materially different point is worse, not better

Interpretation:

- Risk-aware construction without an explicit alpha / expected-return signal behaves like drag, not like a better frontier.
- The tooling is worth keeping.
- Promotion of a risk-aware default objective should wait until there is a real alpha input.

## Phase 1 Alpha Research State

### Strategic Direction

- The long-term roadmap is now documented in:
  - `docs/platform_ml_rl_roadmap.md`
- Direction:
  - shared US/CN-capable research platform
  - US-first depth
  - ML for alpha / risk / cost prediction
  - optimization for allocation
  - RL for sequential execution and control

### Current Implementation Scope

- The first alpha research slice is implemented under:
  - `src/portfolio_os/alpha/`
- Public workflow:
  - `run_alpha_research(...)`
- Acceptance workflow:
  - `run_alpha_acceptance_gate(...)`
- CLI:
  - `portfolio-os-alpha-research`
  - `portfolio-os-alpha-acceptance`
- Current scope is intentionally research-only:
  - consumes `returns_long.csv`
  - builds deterministic reversal and momentum signals
  - computes forward-return labels
  - emits signal panel, IC diagnostics, gate summary JSON, and markdown reports
- Alpha is now connected into the historical backtest stack only.

### Phase 1 Closeout Result

- The Phase 1 alpha acceptance gate is now implemented and closed on the frozen expanded-US snapshot.
- Canonical runtime output:
  - `outputs/phase1_alpha_acceptance_us_expanded`
- Final machine-readable decision:
  - `status = accepted`
  - `acceptance_mode = accepted_by_relative_and_absolute_gates`
  - `accepted_recipe_name = alt_momentum_4_1`
  - `baseline_recipe_name = equal_weight_momentum_6_1`
  - `completed_round_count = 1`
- Accepted holdout metrics:
  - `mean_rank_ic = 0.10630492196878749`
  - `positive_rank_ic_ratio = 0.775`
  - `mean_top_bottom_spread = 0.012255149986753346`
  - `evaluation_date_count = 40`
  - `mean_monthly_factor_turnover = 0.35`

Interpretation:

- Phase 1 closed with a momentum-first winner.
- The accepted recipe is shorter-lookback pure momentum:
  - `momentum_lookback_days = 84`
  - `momentum_skip_days = 21`
  - `reversal_weight = 0.0`
  - `momentum_weight = 1.0`
- The old equal-weight reversal/momentum blend should not be the starting point for optimizer integration.

## Phase 1.5 Alpha Integration State

### Implemented Scope

- Phase 1.5 alpha integration is now implemented in the backtest stack.
- Added capabilities:
  - objective-level `alpha_weight`
  - walk-forward expected-return bridge from `alt_momentum_4_1`
  - `alpha_only_top_quintile` baseline
  - `alpha_panel.csv` audit artifact
  - optimizer-vs-naive, optimizer-vs-alpha-only, and alpha-only-vs-naive reporting
- Dedicated research inputs:
  - `config/us_expanded_alpha_phase_1_5.yaml`
  - `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml`

### Research Outcome

- Canonical runtime output:
  - `outputs/phase1_5_alpha_us_expanded`
- Supporting sensitivity probe:
  - `outputs/phase1_5_alpha_weight_probe/alpha_weight_probe_summary.csv`

Main Phase 1.5 conclusion:

- engineering integration succeeded
- research gate failed

Important machine-readable outcome from the main run:

- `optimizer` annualized return: `6.37%`
- `optimizer` Sharpe: `0.78`
- `optimizer` turnover: `0.0`
- `naive_pro_rata` annualized return: `14.47%`
- `naive_pro_rata` Sharpe: `1.24`
- `alpha_only_top_quintile` annualized return: `0.10%`
- `alpha_only_top_quintile` Sharpe: `0.06`
- `rebalance_dates_with_alpha_signal = 6`
- `rebalance_dates_without_alpha_signal = 6`
- effective nonzero expected-return dates: `2 / 12`

### Structural Interpretation

- The alpha channel is wired correctly, but it did not change optimizer behavior on the frozen expanded-US sample.
- `optimizer` matched the no-trade `buy_and_hold` path exactly in the main run.
- The ad hoc alpha-weight probe over `0, 1, 10, 100, 1000, 10000` produced identical optimizer results, so the current failure is not just a small-weight tuning issue.
- The accepted Phase 1 signal becomes too sparse after walk-forward shrinkage:
  - half the rebalance dates are cold-start
  - most remaining dates still shrink to zero expected return
- The alpha-only benchmark was much worse than naive, which means the accepted recipe is not yet strong enough for direct portfolio construction under realistic costs.

### Decision Boundary

- Do not promote alpha into `config/us_expanded.yaml`.
- Keep Phase 1.5 as infrastructure-complete but research-negative.
- The next research bottleneck is signal robustness and signal translation, not more optimizer plumbing.

## Phase 2 Multi-Factor Spike State

### Implemented Scope

- Ran a deterministic `momentum + book_to_price` multi-factor spike in the isolated `qlib_spikes` workspace.
- Canonical research universe remained the frozen 300-name list, but only `254 / 300` were covered by the FMP freeze.
- The Phase 2 multi-factor result was therefore evaluated on the `254-name` covered subset, not the full canonical 300.

### Research Outcome

- Canonical runtime output:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\momentum_fundamentals_multifactor_spike`
- Final machine-readable decision:
  - `decision_branch = multifactor_promising_but_subset_limited`
  - `promotion_sample_incomplete = true`
- Main subset results:
  - `multifactor alpha_only_vs_naive_mean = 0.005664360008941142`
  - `multifactor alpha_only_vs_naive_tstat = 1.69760258816023`
  - `multifactor mean_rank_ic = 0.019376416809790166`
  - `book_to_price alpha_only_vs_naive_mean = 0.005285596815864608`
  - `book_to_price alpha_only_vs_naive_tstat = 1.4613145110787824`
  - `momentum alpha_only_vs_naive_mean = 0.00014226385096099351`
  - `momentum alpha_only_vs_naive_tstat = 0.042748673618950524`

### Structural Interpretation

- Multi-factor beat naive after costs on the covered subset, but not strongly enough for unconditional promotion.
- Standalone momentum collapsed on the `254-name` subset relative to the earlier full-300 Phase 2.6 result, exposing universe-composition sensitivity in the prior baseline.
- The `46-name` FMP coverage gap was therefore treated as a real interpretation caveat, not just a nuisance data issue.
- The correct next move after Phase 2 was to move away from the small-sample deterministic-only loop and test ML on a materially larger universe.

## Phase 3 Qlib ML Alpha State

### Implemented Scope

- Phase 3 was executed in the isolated Qlib worktree:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha`
- Implemented components:
  - `expanded_liquid_core` universe builder
  - PIT-safe FMP staging layer
  - staging-to-Qlib conversion
  - deterministic parity baselines
  - LightGBM walk-forward training
  - `alpha_panel.csv` export
  - Layer 1 signal evaluation
- Canonical runtime output root:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\outputs\phase3_ml_alpha`

### Research Outcome

- Primary universe:
  - `expanded_liquid_core_count = 1989`
- Continuity control:
  - `control_300_available_count = 227`
- Walk-forward training:
  - `fold_count = 6`
  - `feature_count = 167`
  - `prediction_row_count = 1451970`
  - `objective_requested = lambdarank`
  - `objectives_used = [regression]`
- Final machine-readable decision:
  - `promotion_status = reject`
  - `reason = ml_failed_layer_1_signal_gate`
  - `costed_backtest.status = skipped`
- Expanded-core ML Layer 1 results:
  - `mean_rank_ic = -0.0010591874763032012`
  - `rank_ic_tstat = -0.07577984751762754`
  - `alpha_only_vs_naive_mean = -0.003677472026942384`
  - `alpha_only_vs_naive_tstat = -1.9493232574622625`
- Expanded-core deterministic parity reference:
  - `multifactor mean_rank_ic = -0.014459652998030066`
  - `multifactor alpha_only_vs_naive_mean = 0.0017845643160054384`
- Control-universe ML Layer 1 results:
  - `mean_rank_ic = -0.025032854721437556`
  - `alpha_only_vs_naive_mean = -0.004126771166029848`

### Phase 3.5 Diagnostics And Targeted Retry

- Phase 3.5 was executed in the same isolated Qlib worktree and closed with a stable gate result.
- Canonical Phase 3.5 output root:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\outputs\phase3_5_ml_diagnostics`
- First diagnosis:
  - the original `lambdarank` failure was a real pipeline bug, not a market conclusion
  - root cause: query-group sizes were computed before objective-specific row drops, so query counts exceeded the final training matrix length
  - integrity outcome:
    - `pipeline_integrity_status = invalid_fixed`
    - `legacy_mismatch_fold_count = 6`
    - `current_mismatch_fold_count = 0`
- Post-mortem findings on the original Phase 3 run:
  - early folds were mildly positive, later folds turned materially negative
  - `Alpha158` dominated the configured feature mix:
    - `Alpha158 = 158 / 167 features`
    - `custom_fundamentals = 8 / 167`
    - `custom_technical = 1 / 167`
  - PIT/fundamental coverage on evaluation dates was not the bottleneck:
    - `full_custom_fundamentals_rate_mean` stayed around `0.958-0.961` by fold
  - dynamic liquidity segment diagnostics did not reveal a clean positive pocket under the original regression fallback
- One targeted retry was then run with:
  - true `lambdarank`
  - primary universe = `top_500_liquid_dynamic`
  - compact feature set = `6 technical + 8 fundamentals`
  - control universe = `control_300_available`
- Retry output summary:
  - `objectives_used = [lambdarank]`
  - `prediction_row_count = 1451958`
  - `fold_count = 6`
- Retry primary-universe result (`top_500_liquid_dynamic`):
  - `mean_rank_ic = 0.00621153119435888`
  - `rank_ic_tstat = 0.21394083528717797`
  - `alpha_only_vs_naive_mean = 0.0069113685169759275`
  - `alpha_only_vs_naive_tstat = 1.1464660284220172`
- Retry control-universe result (`control_300_available`):
  - `mean_rank_ic = 0.027207684392606478`
  - `rank_ic_tstat = 1.0429181225747441`
  - `alpha_only_vs_naive_mean = 0.010074155664304554`
  - `alpha_only_vs_naive_tstat = 2.1732733742381085`
- Ensemble branch was explicitly checked against the deterministic multifactor baseline:
  - `rank_ic_series_correlation = -0.03507252319528281`
  - `alpha_only_series_correlation = 0.4352095990857516`
  - simple equal-weight ensemble did not beat both standalone signals on Layer 1 metrics
  - outcome:
    - `passes_ensemble_gate = false`
- Final Phase 3.5 decision:
  - `outcome = retry_reject`
  - `next_recommended_step = Keep deterministic baseline as the research default and do not promote ML alpha.`

### Phase 3.5 Universe-Split Diagnosis

- A follow-up diagnostic was run to explain the apparent `300 vs 500` split using the targeted-retry alpha panel, PIT-safe daily `market_cap`, canonical-300 membership flags, and placebo sampling.
- Canonical output:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\outputs\phase3_5_ml_diagnostics\universe_split_summary.json`
- Diagnostic sample:
  - primary universe remained `top_500_liquid_dynamic`
  - `primary_row_count = 17500`
  - `primary_ticker_count = 938`
  - `canonical_available_ticker_count = 220`
- Market-cap quintile read:
  - `Q1 mean_rank_ic = 0.02004185636880605`
  - `Q2 mean_rank_ic = 0.0014792692025384366`
  - `Q3 mean_rank_ic = 0.016913370902554528`
  - `Q4 mean_rank_ic = 0.015924391855806373`
  - `Q5 mean_rank_ic = -0.008023834695503579`
- Canonical-membership read inside the same `top_500_liquid_dynamic` universe:
  - canonical names:
    - `mean_rank_ic = 0.01911331858563089`
    - `alpha_only_vs_naive_mean = 0.007088411250605896`
  - non-canonical names:
    - `mean_rank_ic = 0.0006685522828736416`
    - `alpha_only_vs_naive_mean = 0.0074076857029048045`
- Placebo read:
  - size-matched random samples from the same top-500 universe:
    - `canonical_rank_ic_percentile_vs_placebo = 0.88`
    - `canonical_alpha_percentile_vs_placebo = 0.59`
  - fixed-size random 300 samples:
    - `canonical_rank_ic_percentile_vs_placebo = 0.99`
    - `canonical_alpha_percentile_vs_placebo = 0.69`
- Stable interpretation:
  - this does **not** look like a clean pure selection-bias artifact
  - the signal is more consistent with a weak `large-cap / upper-universe alpha candidate`
  - canonical-300 membership does improve IC versus the rest of the top-500 universe, but the size-matched placebo does not make that edge strong enough to treat canonical membership itself as the hidden signal source
  - therefore, if ML research is reopened, the more legitimate next scope is an objectively defined large-cap / liquid universe, not another canonical-300-specific branch

### Phase 3.6 Neutralized Regime-Aware Retry

- Phase 3.6 was executed in the same isolated Qlib worktree with the neutralized pipeline documented in the `phase3_6` spec.
- Canonical Phase 3.6 output root:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\outputs\phase3_6_neutralized_retry`
- Core pipeline changes relative to Phase 3.5:
  - per-date cross-sectional neutralization of structured features and the forward-return label
  - structural controls used in neutralization:
    - `sector`
    - `log_market_cap`
  - supplementary regime features retained for the model:
    - `market_cap_percentile`
    - `liquidity_percentile`
  - time-varying cap-bucket-balanced sample weights
  - fair shadow benchmark:
    - `top_500-only` training with the same neutralized feature family and evaluation folds
- Neutralization diagnostics confirmed the intended structure removal:
  - post-neutralization size correlations for technicals, fundamentals, and the label were effectively zero
  - post-neutralization sector dispersion for the structured fundamentals was effectively zero
  - cap-bucket weights were balanced by date
- Primary neutralized retry configuration:
  - training scope = `expanded_liquid_core` full sample
  - primary deployment slices evaluated:
    - `expanded_liquid_core_full`
    - `top_500_liquid_dynamic`
    - `top_300_liquid_dynamic`
    - `control_300_available`
  - walk-forward folds = `6`
  - stitched holdout prediction rows = `1451958`
- Final machine-readable decision:
  - `outcome = retry_reject`
  - `next_recommended_step = Do not promote the neutralized retry; keep deterministic baseline as default.`
- Gate read:
  - `expanded_liquid_core_full` safety pass = `true`
  - `top_300_liquid_dynamic` pass = `true`
  - `control_300_available` pass = `true`
  - `top_500_liquid_dynamic` primary gate pass = `false`
  - `continuity_pass = true`
- Primary top-500 metrics:
  - `mean_rank_ic = 0.006725852934286424`
  - `rank_ic_tstat = 0.24265631012607727`
  - `alpha_only_vs_naive_mean = 0.0052600910577531215`
  - `alpha_only_vs_naive_tstat = 1.0110018794584064`
- Primary top-300 metrics:
  - `mean_rank_ic = 0.01239915300471979`
  - `rank_ic_tstat = 0.4250894705022766`
  - `alpha_only_vs_naive_mean = 0.007109039445402122`
  - `alpha_only_vs_naive_tstat = 1.3488343701154135`
- Primary control-300-available metrics:
  - `mean_rank_ic = 0.013626311055886917`
  - `rank_ic_tstat = 0.4987410637628581`
  - `alpha_only_vs_naive_mean = 0.008200035003898534`
  - `alpha_only_vs_naive_tstat = 1.6603171910625243`
- Primary expanded-core full-sample metrics:
  - `mean_rank_ic = -0.002918162210979371`
  - `rank_ic_tstat = -0.11174811555866837`
  - `alpha_only_vs_naive_mean = 0.020447664762589947`
  - `alpha_only_vs_naive_tstat = 3.3943960487955356`
- Shadow comparison on the actual deployment slice:
  - shadow slice = `top_500_liquid_dynamic`
  - shadow `mean_rank_ic = 0.0018785443385658585`
  - shadow `alpha_only_vs_naive_mean = 0.00878579629508262`
  - `primary_beats_shadow_on_top_500 = false`
  - `delta_mean_rank_ic = 0.004847308595720566`
  - `delta_alpha_only_vs_naive_mean = -0.0035257052373294986`
- Stable interpretation:
  - neutralization materially improved robustness and removed the earlier broad-universe failure mode
  - the model no longer looked structurally dangerous on the full expanded universe
  - continuity across `top_300` and `control_300_available` became acceptable
  - but the actual primary deployment slice, `top_500_liquid_dynamic`, still did not pass the promotion gate
  - broader full-sample training also failed to beat the fair `top_500-only` shadow benchmark where it mattered most
  - therefore, Phase 3.6 closed as another non-promotable ML branch rather than a promotion-ready recovery

### Decision Boundary

- Do not promote `qlib_lgbm` into any mainline PortfolioOS config.
- Do not run optimizer-promotion or RL-execution work off this Phase 3 result.
- Costed PortfolioOS handoff was intentionally skipped because Layer 1 already failed on the primary universe; adding optimizer noise would not change the gate result.
- The deterministic research baseline remains the current reference alpha path.
- The raw Phase 3 regression reject should be read with the Phase 3.5 qualifier:
  - the original negative result was contaminated by a real `lambdarank` pipeline bug
  - after fixing the bug, ML became directionally positive but still not strong enough on the primary universe
- The `300 vs 500` split should also be read with the universe-split qualifier:
  - evidence leans toward a weak large-cap / upper-universe effect
  - evidence does **not** strongly support a pure canonical-300 selection-bias story
- The Phase 3.6 neutralized retry should be read with its own qualifier:
  - neutralization fixed robustness and continuity issues
  - but the main `top_500` deployment gate still failed
  - and broad-sample training did not beat the `top_500-only` shadow benchmark on alpha-only payoff
- Any future ML retry should be treated as a new research branch rather than a continuation of this exact setup.
- Do not treat the current ML stack as signal-ready for optimizer promotion, RL execution, or live-paper alpha handoff.

### Phase 3.7 Horizon Audit + Transcript Alpha

- Phase 3.7 was executed end-to-end in the same isolated Qlib worktree and is now closed.
- Canonical Phase 3.7 output root:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\outputs\phase3_7_transcript_alpha`
- Main artifacts:
  - `horizon_audit_summary.json`
  - `transcript_manifest.json`
  - `transcript_event_panel.parquet`
  - `transcript_daily_features.parquet`
  - `transcript_walkforward_summary.json`
  - `phase3_7_gate_summary.json`
- Horizon-audit result:
  - no short-horizon override was triggered
  - Phase 3.7 training stayed on `21d`
  - however, the audit itself showed that the stronger deterministic/structured signal pocket was actually slower-moving:
    - `top_500_liquid_dynamic`:
      - `21d mean_rank_ic = 0.014595761703939556`
      - `21d rank_ic_tstat = 2.358474141532583`
      - `42d mean_rank_ic = 0.023127802236678506`
      - `42d rank_ic_tstat = 3.8264956615270203`
    - `top_300_liquid_dynamic`:
      - `21d mean_rank_ic = 0.013829830168416572`
      - `21d rank_ic_tstat = 2.2237850549044698`
      - `42d mean_rank_ic = 0.025651046832987676`
      - `42d rank_ic_tstat = 4.236620424907249`
  - stable interpretation of the horizon audit:
    - the current research stack likely underestimates slower-moving value / quality style signal at `21d`
    - `42d` is a stronger research candidate horizon than `21d`
    - but this should still be treated as a research finding, not an automatic production-default change, until a non-overlapping parity / backtest check is run
- Transcript pipeline actually used:
  - PIT mapping used transcript `call_date -> next trading day`
  - event carry used `120` calendar-day staleness
  - runtime was limited to the deployment-relevant whitelist (`top_500` dynamic plus `control_300_available`)
  - transcript events were further limited to `2022-12-01` through `2026-02-27`
  - FinBERT was used with bounded sentence sampling to keep the branch computationally tractable
- Real transcript event coverage:
  - whitelist size = `1024`
  - transcript-positive tickers in whitelist = `1011`
  - event rows = `12761`
  - section-detection success rate = `0.5431`
- Daily transcript feature coverage:
  - row count = `3569102`
  - active ticker count = `1011`
  - active row rate = `0.2119`
  - sector/size neutralization again removed almost all post-size correlation from transcript features
- Transcript-augmented top-500 walk-forward training result:
  - training horizon = `21`
  - fold count = `6`
  - transcript feature gain share = `0.0111`
  - top transcript gain contributors were uncertainty, transcript word count, word-count delta, and mean sentiment
- Primary deployment-slice (`top_500_liquid_dynamic`) Layer 1 result:
  - `mean_rank_ic = -0.00016260841522979717`
  - `rank_ic_tstat = -0.005646386437604967`
  - `alpha_only_vs_naive_mean = 0.00763020454346959`
  - `alpha_only_vs_naive_tstat = 1.3236458755566833`
- Structured baseline on the same slice remained slightly stronger:
  - structured `mean_rank_ic = 0.0018785443385658585`
  - structured `alpha_only_vs_naive_mean = 0.00878579629508262`
- Complementarity result was clearly negative:
  - `rank_ic_series_correlation = 0.9923`
  - `alpha_only_vs_naive_series_correlation = 0.9692`
  - `prediction_series_correlation = 0.5398`
  - structured-weak-period alpha lift = `0.0005607680575636973`
  - complementarity gate = `false`
- Broad-safety / continuity result:
  - `continuity_pass = true`
  - `broad_safety_pass = false`
  - full expanded-core rank IC stayed negative
- Final Phase 3.7 decision:
  - `outcome = transcript_inconclusive`
  - `signal_pass = false`
  - `layer_2_ready = false`
  - no PortfolioOS costed backtest was run
- Stable interpretation:
  - transcripts did not rescue the structured ML branch
  - the text branch remained too weak on the actual `top_500` deployment slice
  - transcript features were only lightly used by the model
  - the transcript branch was also highly correlated with the structured baseline and did not provide meaningful structured-weak-period relief
  - therefore, the current transcript V1 branch should not be promoted and should not be used to justify optimizer promotion, RL execution, or live-paper alpha handoff

### Phase 3.7x Transcript Diagnostic Improvement Pass

- A focused transcript-diagnostics follow-up was run to answer whether the Phase 3.7 failure was mainly due to a broken text feature, excessive carry smoothing, or a deeper information ceiling.
- Canonical Phase 3.7x output root:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\outputs\phase3_7x_transcript_diagnostics`
- Main artifacts:
  - `improvement_manifest.json`
  - `enhanced_transcript_event_manifest.json`
  - `transcript_daily_feature_comparison.json`
  - `transcript_univariate_diagnostics.json`
- What changed in the diagnostic pass:
  - `uncertainty_tone_score` was repaired from an all-zero placeholder into a real lexicon-based uncertainty feature
  - transcript section-structure diagnostics were added:
    - `management_sentence_count`
    - `qa_sentence_count`
    - `qa_question_count`
    - `qa_share_of_words`
  - transcript daily features were rebuilt across carry windows:
    - `10`
    - `21`
    - `60`
    - `120`
- Event-level improvement read:
  - baseline transcript rows = `12761`
  - enhanced transcript rows = `28143`
  - section-detection success rate improved from `0.5431` to `0.5923`
  - uncertainty non-zero rate improved from `0.0` to `0.7356`
  - enhanced average QA question count = `15.38`
- Carry-window diagnosis:
  - baseline active row rate under the old Phase 3.7 pipeline = `0.2119`
  - rebuilt carry windows:
    - `10d active_row_rate = 0.0589`
    - `21d active_row_rate = 0.1218`
    - `60d active_row_rate = 0.3212`
    - `120d active_row_rate = 0.4830`
  - on `top_500_liquid_dynamic`, transcript-active coverage rises sharply with longer carry:
    - `10d = 0.1224`
    - `21d = 0.2575`
    - `60d = 0.6561`
    - `120d = 0.9734`
- Stable interpretation of the carry diagnosis:
  - short carry windows preserve event freshness but are sparse
  - long carry windows provide broad coverage but strongly risk smearing event information into a slow factor-like proxy
  - this supports the view that transcript alpha is more naturally an event-driven problem than a monthly cross-sectional carry problem
- Univariate transcript diagnostics:
  - several transcript and structure features showed non-zero pockets, especially at longer horizons
  - on `top_500_liquid_dynamic` and `42d`, positive pockets appeared in:
    - `management_sentiment_mean`
    - `transcript_sentiment_delta_vs_prev_call`
    - `uncertainty_tone_score`
  - negative pockets also appeared in:
    - `transcript_word_count`
    - `qa_question_count`
    - `qa_sentence_count`
- Stable conclusion from Phase 3.7x:
  - transcript V1 did **not** fail purely because "text has no information"
  - it failed because the current `21d` / monthly-style carry formulation is a poor fit for sparse earnings-call events
  - if transcript research resumes, it should reopen as an explicitly event-driven branch rather than as another near-identical monthly LightGBM retry

### Phase 3.8 42d Horizon Promotion Check

- Phase 3.8 was implemented in the same isolated Qlib worktree and stopped at Gate A by design.
- Canonical Phase 3.8 output root:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\outputs\phase3_8_42d_event_alpha`
- Main implementation artifacts:
  - `scripts/run_phase3_8_nonoverlap_42d.py`
  - `scripts/build_phase3_8_event_panel.py`
  - `scripts/run_phase3_8_event_alpha.py`
- Main machine-readable artifacts:
  - `nonoverlap_42d_baseline_sweep.json`
  - `phase3_8_gate_summary.json`
  - `phase3_8_gate_report.md`
- Gate A design:
  - non-overlapping `42-trading-day` forward windows
  - evaluated on `top_500_liquid_dynamic` and `top_300_liquid_dynamic`
  - tested deterministic candidates instead of reopening a new ML branch prematurely
- Candidate sweep result:
  - `mom_84_21`:
    - `top_500 mean_rank_ic = -0.018248642461049007`
    - `top_500 rank_ic_tstat = -0.5556388229961094`
  - `book_to_price`:
    - `top_500 mean_rank_ic = 0.020757686076202854`
    - `top_500 rank_ic_tstat = 0.712796150509914`
  - `multifactor_equal`:
    - `top_500 mean_rank_ic = -0.005315871182602591`
    - `top_500 rank_ic_tstat = -0.16973935125799197`
- Stable Phase 3.8 outcome:
  - `outcome = 42d_not_promoted`
  - `reason = nonoverlap_42d_failed_on_all_deterministic_candidates`
  - `best_candidate = book_to_price`
  - `event_branch_run = false`
- Interpretation:
  - the earlier overlapping-window `42d` pocket was real enough to justify investigation but did **not** survive as a promotion-worthy deterministic default under non-overlapping evaluation
  - the current research stack should therefore **not** switch its default horizon from `21d` to `42d`
  - because Gate A failed, the Phase 3.8 event-driven transcript / grades branch was correctly **not** opened
  - any future event-driven branch should be treated as a new hypothesis, not as an automatic continuation of a failed `42d` horizon promotion

## Phase 3.9 Quasi-PIT SUE Spike

Scope:

- worktree: `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha`
- runner: `scripts/run_phase3_9_sue_spike.py`
- outputs: `outputs/phase3_9_sue_spike/`
- universe: `top_500_liquid_dynamic`
- label: `21d` forward return with non-overlapping evaluation dates
- PIT anchor priority: `acceptedDate > filingDate > date`
- actual EPS choice: `epsDiluted`, fallback `eps`
- estimate/actual join key: `(ticker, fiscal_quarter_end_date)`

Feasibility findings:

- `analyst_estimates_quarterly.json` remained broadly usable for a quasi-PIT surprise spike:
  - non-empty coverage previously verified at `2497 / 2671` main-list tickers
  - estimate/actual quarter overlap previously verified at `2494` tickers with `median overlap = 8`
- this branch is still **not** a true estimate-snapshot study:
  - FMP freeze lacks `snapshot_date / as_of_date / updated_at`
  - therefore the signal is best described as `quasi-PIT SUE using frozen final consensus proxy`

Stable Phase 3.9 outcome:

- `outcome = fail`
- `reason = top500_rank_ic_tstat_lt_1_5`
- `event_count = 13324`
- `signal_row_count = 806`
- `top_500 mean_rank_ic = 0.21316550878628812`
- `top_500 rank_ic_tstat = 1.4313686301552095`
- `top_500 alpha_only_vs_naive_mean = 0.001040876667221086`
- `top_500 alpha_only_vs_naive_tstat = 0.21648319440090733`
- `top_300 rank_ic_tstat = 0.5868005210151751`
- `momentum IC-series correlation = 0.8595847383380031`
- `SUE + momentum` equal-weight composite on the same event dates also failed to clear a promotion-style bar:
  - `composite top_500 rank_ic_tstat = 1.5693985448003813`

Critical sparse-event diagnostic:

- the headline `top_500` result was flattered by very thin event dates:
  - `top_500 signal dates total = 38`
  - `median top_500 count per date = 1`
  - only `15` dates had at least `2` names
  - only `9` dates had at least `5` names
- on the denser `>= 5 names/date` slice, SUE was actually weak-to-negative:
  - `mean_rank_ic = -0.0891685964672975`
  - `rank_ic_tstat = -0.7772978430520316`
  - `alpha_only_vs_naive_mean = -0.021947734679702662`
  - `alpha_only_vs_naive_tstat = -1.5173643330053417`

Interpretation:

- SUE was worth testing and is technically feasible on the current freeze, but it does **not** rescue the US large-cap alpha story under the current `21d non-overlapping` event framing
- the raw headline rank IC looked interesting, but the dense-date slice shows that the apparent strength was largely a sparse-event artifact rather than a robust large-cap cross-sectional signal
- because SUE is also highly correlated with the existing momentum baseline on the event dates that do score, it does not currently open a strong new ensemble branch

## Phase 4A Mid-Cap Momentum Universe Test

Scope:

- worktree: `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha`
- runner: `scripts/run_phase4a_midcap_momentum.py`
- outputs: `outputs/phase4a_midcap_momentum/`
- data source:
  - `C:\Users\14574\Quant\fmp_data_freeze\fundamentals\`
  - `C:\Users\14574\Quant\fmp_data_freeze\fundamentals_small_cap\`
- signal: `mom_84_21`
- horizon: `21d non-overlapping`
- ranking mode: static freeze-date `profile.json.marketCap`
- slices:
  - `top_300`
  - `top_500`
  - `rank_500_1500` defined as static ranks `501-1500`

Stable result:

- full freeze daily panel coverage:
  - `3193` tickers in evaluation panel
  - `254` non-overlapping evaluation dates in the raw panel
- `top_300`:
  - `mean_rank_ic = -0.008344924323776556`
  - `rank_ic_tstat = -0.6810990862686463`
  - `alpha_only_vs_naive_tstat = 1.0609002348740775`
- `top_500`:
  - `mean_rank_ic = -0.009184677509241831`
  - `rank_ic_tstat = -0.800945066047217`
  - `alpha_only_vs_naive_tstat = 1.0198177579247052`
- `rank_500_1500`:
  - `mean_rank_ic = 0.01187808824967314`
  - `rank_ic_tstat = 0.92466329374959`
  - `alpha_only_vs_naive_mean = 0.005038817822073995`
  - `alpha_only_vs_naive_tstat = 2.296564716654677`

Interpretation:

- moving the universe down-cap **did** help relative to the large-cap slices
- however, the improvement showed up more clearly in `alpha_only_vs_naive` than in monotonic rank IC
- this supports the idea that universe choice matters, but does **not** yet establish a clean strong mid-cap ranker
- practical read:
  - US large-cap remains an especially hostile research battleground
  - mid-cap / lower-efficiency slices are more promising than `top_500`
  - but the current deterministic momentum factor still does not become a clearly promotion-worthy standalone signal just by moving to `501-1500`

## Phase 4B SEC 13F Dataset Pipeline

Scope:

- runner: `scripts/run_phase4b_edgar_13f_pipeline.py`
- outputs: `outputs/phase4b_edgar_13f/`
- source:
  - `https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets`
- implementation choice:
  - use the official quarterly SEC `Form 13F Data Sets`
  - do **not** scrape raw EDGAR full-index / XML in v1

Stable result:

- official dataset discovery worked:
  - `dataset_link_count = 52`
- latest parsed dataset:
  - `01dec2025-28feb2026_form13f.zip`
- row counts from the latest dataset:
  - `submission_count = 11372`
  - `coverpage_count = 11372`
  - `infotable_count = 3473209`
  - `sample_holdings_count = 10000`
- emitted artifacts:
  - raw quarterly ZIP under `outputs/phase4b_edgar_13f/raw/`
  - `filings_manifest.csv`
  - `sample_holdings.csv`
  - `pipeline_summary.json`

Interpretation:

- a free institutional-ownership data path is now real and working
- the official SEC quarterly 13F data sets are a better v1 source than raw EDGAR full-index scraping because they already flatten holdings into `SUBMISSION / COVERPAGE / INFOTABLE` tables
- next research step, if resumed, is not more ingestion work; it is factor construction:
  - holder-count change
  - institutional-ownership change
  - later, filer-quality weighting if desired
- the main missing piece before direct alpha use is identifier normalization / ticker mapping from 13F holdings rows into the research universe

## Phase 4C SEC 13F Factors V1

Scope:

- spec: `docs/superpowers/specs/2026-04-05-phase4c-13f-factors-design.md`
- plan: `docs/superpowers/plans/2026-04-05-phase4c-13f-factors.md`
- runner: `scripts/run_phase4c_13f_factors.py`
- outputs: `outputs/phase4c_13f_factors/`

Implementation details:

- source:
  - latest two official SEC `Form 13F Data Sets`
  - latest run used:
    - `01sep2025-30nov2025_form13f.zip`
    - `01dec2025-28feb2026_form13f.zip`
- holdings normalization:
  - use `SUBMISSION.tsv + INFOTABLE.tsv`
  - dedupe amendments by keeping the latest filing per `(CIK, CUSIP9, report_quarter)`
  - group by `REPORTCALENDAR / PERIODOFREPORT`, not by ZIP file name
  - explicitly filter to the latest two `report_quarter` values before factor construction
- identifier mapping:
  - use unique FMP `profile.json.cusip` normalized to `CUSIP9 -> ticker`
- factor definitions:
  - `holder_count_change = log(holder_count_t / holder_count_t_minus_1)`
  - `ownership_pct_change = shares_held_t / shares_outstanding_t - shares_held_t_minus_1 / shares_outstanding_t_minus_1`
- denominator proxy:
  - use `weightedAverageShsOutDil`, fallback `weightedAverageShsOut`
  - if proxy shares outstanding changes by more than `20%` quarter-over-quarter, drop `ownership_pct_change` for that ticker-quarter

Stable result:

- latest two quarters used:
  - `2025-09-30`
  - `2025-12-31`
- mapped ticker-quarter rows:
  - `holdings_by_ticker_quarter_count = 6058`
- factor rows:
  - `row_count = 3010`
- slice coverage:
  - `top_500 = 487`
  - `rank_500_1500 = 960`
  - `ownership_pct_non_null = 2190`
- momentum orthogonality:
  - overall:
    - `holder_count_change corr = 0.20819591705855975`
    - `ownership_pct_change corr = 0.013204600933922176`
  - `top_500`:
    - `holder_count_change corr = 0.4728759841457169`
    - `ownership_pct_change corr = -0.12214405781018825`
  - `rank_500_1500`:
    - `holder_count_change corr = 0.2157903222104423`
    - `ownership_pct_change corr = 0.028417729178840213`

Interpretation:

- the free SEC 13F branch is now past ingestion and into usable factor construction
- `ownership_pct_change` looks genuinely orthogonal to momentum on the current slices
- `holder_count_change` is **not** cleanly orthogonal in `top_500`, and is only moderately separated in `rank_500_1500`
- practical read:
  - `ownership_pct_change` is the more promising v1 institutional-flow factor
  - `holder_count_change` likely overlaps with momentum / popularity effects, especially in large-cap
  - next 13F research, if resumed, should prioritize:
    - evaluating `ownership_pct_change` first
    - optionally adding filer-quality weighting or additional quarters before promoting `holder_count_change`

## Phase 4D SEC 13F History Backfill

Scope:

- spec: `docs/superpowers/specs/2026-04-05-phase4d-13f-history-backfill-design.md`
- plan: `docs/superpowers/plans/2026-04-05-phase4d-13f-history-backfill.md`
- runner: `scripts/run_phase4d_13f_history_backfill.py`
- outputs: `outputs/phase4d_13f_history_backfill/`

Stable implementation details:

- selected SEC quarterly ZIPs across a widened candidate window, then filtered by actual `report_quarter`
- earliest vs latest dataset schema smoke now passes
- shared SEC ZIP loader was hardened to support archives where required TSVs live under a directory prefix, not only at ZIP root
- PIT handling for historical institutional-flow signals now explicitly drops late amendments from quarterly history construction:
  - first filter to the target `report_quarter` range
  - then keep only filings within a `60` calendar-day timely window after `report_quarter`
  - then dedupe latest filing per `(CIK, CUSIP9, report_quarter)`
- this prevents very late `13F-HR/A` amendments from rewriting historical availability dates or holdings totals with future information

Stable result:

- selected SEC dataset count: `14`
- selected files cover:
  - `2022q4_form13f.zip`
  - through `01dec2025-28feb2026_form13f.zip`
- actual report quarters backfilled:
  - `2022-12-31` through `2025-12-31`
- holdings-by-ticker-quarter rows: `37760`
- factor rows: `34662`
- unique tickers: `3098`
- quarter coverage increased steadily from:
  - `2022-12-31: 2794`
  - to `2025-12-31: 3040`
- `ownership_pct_non_null_count = 25915`
- schema smoke pass: `true`
- timely filing window: `60` days

Interpretation:

- the free SEC 13F branch is now backfilled deeply enough to support an actual multi-quarter alpha evaluation
- there are now `13` report quarters in the holdings history and `12` quarter-to-quarter factor transitions available for later testing
- the data-layer blocker for an institutional-flow spike is now closed
- the next step, if resumed, should be a true `ownership_pct_change` / `holder_count_change` evaluation branch on:
  - `rank_500_1500` primary slice
  - `top_500` control slice
  - `21d non-overlapping`
  - filing-date-based PIT anchor with quarterly carry

## Phase 4E SEC 13F Alpha Evaluation

Scope:

- spec: `docs/superpowers/specs/2026-04-05-phase4e-13f-alpha-evaluation-design.md`
- plan: `docs/superpowers/plans/2026-04-05-phase4e-13f-alpha-evaluation.md`
- runner: `scripts/run_phase4e_13f_alpha.py`
- outputs: `outputs/phase4e_13f_alpha/`

Stable implementation details:

- primary slice:
  - `rank_500_1500_dynamic`
- control slice:
  - `top_500_dynamic`
- horizon:
  - `21d non-overlapping`
- dynamic universe membership:
  - recomputed by evaluation date using `close * latest quarter-end shares proxy`
  - more realistic than static ranking, but still not a perfect PIT market-cap history
- two evaluation modes are now reported for each factor:
  - `full_carry`
    - quarterly factor values carried forward until the next quarterly update
  - `first_eval_only`
    - only the first non-overlapping evaluation date on or after the quarter's latest cross-sectional availability date
- factors evaluated:
  - `ownership_pct_change`
  - `holder_count_change`

Stable result:

- evaluation dates on the `21d` grid: `246`
- independent quarterly first-eval dates: `11`
- primary gate factor remained `ownership_pct_change`
- `ownership_pct_change` on `rank_500_1500_dynamic`:
  - `full_carry mean_rank_ic = -0.00548`
  - `full_carry rank_ic_tstat = -0.7093`
  - `full_carry alpha_only_vs_naive_tstat = 0.2299`
  - `first_eval_only mean_rank_ic = -0.00847`
  - `first_eval_only rank_ic_tstat = -0.7387`
  - `first_eval_only alpha_only_vs_naive_tstat = 0.2143`
  - `first_eval_only factor_momentum_corr = 0.0671`
- `holder_count_change` on `rank_500_1500_dynamic`:
  - `full_carry mean_rank_ic = 0.01229`
  - `full_carry rank_ic_tstat = 0.9818`
  - `full_carry alpha_only_vs_naive_tstat = 1.7685`
  - `first_eval_only mean_rank_ic = 0.03138`
  - `first_eval_only rank_ic_tstat = 1.4053`
  - `first_eval_only alpha_only_vs_naive_tstat = 1.7259`
  - `first_eval_only factor_momentum_corr = 0.1917`
- `ownership_pct_change` on `top_500_dynamic` was also not compelling:
  - `full_carry rank_ic_tstat = -1.5189`
  - `first_eval_only rank_ic_tstat = 0.3302`
- final gate:
  - `outcome = close_13f_branch`
  - `direction_consistent = true`
  - `carry_warning = null`

Interpretation:

- the initially promising free SEC 13F branch does **not** currently produce a promotable institutional-flow alpha on the primary `rank_500_1500_dynamic` slice
- `ownership_pct_change` stayed weak-to-negative in both `full_carry` and `first_eval_only`, so this is **not** a carry-style illusion
- the low momentum correlation on `ownership_pct_change` confirms orthogonality, but orthogonality alone was not enough to produce usable alpha
- `holder_count_change` was directionally better than `ownership_pct_change`, especially in alpha-only spread, but:
  - it did not clear the intended primary gate
  - it overlaps more with momentum / popularity effects
  - it should be treated as an auxiliary observation, not as the new main institutional-flow factor
- practical read:
  - the free SEC 13F branch is now a validated data asset, but not a validated alpha source under the current `21d non-overlapping + quarterly carry` framing
  - if institutional-flow research resumes later, it should reopen as a clearly new branch with materially different factor construction rather than as a direct continuation of Phase 4E

## Phase 5 RL Execution Sandbox

Scope:

- spec: `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\specs\2026-04-05-phase5a-5b-rl-execution-sandbox-design.md`
- model freeze note: `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\notes\2026-04-05-phase5-model-freeze.md`
- main runners:
  - `scripts/build_phase5_execution_simulator.py`
  - `scripts/run_phase5_execution_benchmarks.py`
  - `scripts/train_phase5_bc_policy.py`
  - `scripts/train_phase5_ppo_execution.py`
  - `scripts/evaluate_phase5_execution_policies.py`
  - `scripts/export_phase5_offline_rl_dataset.py`
- outputs:
  - `outputs/phase5_rl_execution/`

Stable implementation details:

- GPU path is active in the worktree venv:
  - `torch = 2.11.0+cu128`
  - `torch.cuda.is_available() = true`
  - device = `NVIDIA GeForce RTX 3090`
- current framing is explicitly `execution sandbox`, not promoted mainline execution research
- the correct boundary is:
  - `offline-from-simulator`
  - not market-validated offline RL
- simulator v1 acceptance currently passes:
  - volume-profile calibration MAE is around `0.006`
  - benchmark ordering is sensible:
    - `impact_aware < vwap < twap`
- the first minimal PPO sandbox uncovered two real design bugs that are now fixed:
  - reward-scale bug:
    - unfinished inventory penalty was originally too weak relative to dollar-denominated execution costs
    - reward is now normalized in target-notional bps space so no-trade is no longer spuriously advantaged
  - target-size bug:
    - the original `target_qty = 100` let `action = 1.0` clear the whole order in one bucket because synthetic bucket liquidity was much larger
    - target quantity is now derived from episode liquidity via `derive_target_qty(...)`, forcing multi-bucket execution
- behavior-cloning setup now exists:
  - mixed expert dataset:
    - `impact_aware = 70%`
    - `twap = 15%`
    - `vwap = 15%`
  - BC checkpoint can be copied into PPO actor initialization
- evaluation harness now reports:
  - PPO-from-random
  - PPO-from-BC
  - heuristic rewards on the same env-reward scale
  - simulator benchmark costs as a supplemental diagnostic
- offline dataset export now exists:
  - `offline_transitions.csv`
  - `offline_episode_manifest.csv`
  - `offline_rl_readiness_report.json`
  - policy tags currently exported:
    - `impact_aware`
    - `twap`
    - `vwap`
    - `ppo_random`
    - `ppo_bc`

Stable result:

- sandbox dependency and implementation tests currently pass:
  - focused suite = `15 / 15`
- latest real `offline-from-simulator` export:
  - `transition_count = 287`
  - `episode_count = 40`
  - `offline_ready = true`
- current sandbox conclusion:
  - the RL branch is now infrastructurally real
  - the simulator, reward, and export stack are good enough for PPO/BC/offline experiments
  - but PPO-vs-BC superiority is **not yet a stable conclusion** under the more realistic reward and target sizing
  - BC warm start no longer shows the earlier clean dominance once the degenerate one-step environment bug is removed

Interpretation:

- Phase 5 is now unblocked as an execution-sandbox branch
- the highest-value next work is:
  - simulator fidelity
  - reward shaping
  - state design
  - evaluation depth
  - then offline-from-simulator RL
- do **not** yet treat PPO, BC warm start, or offline RL as promoted execution-policy research results
- treat the current sandbox as a credible engineering foundation, not as a final execution alpha story

## Phase 5C Offline-From-Simulator RL

Scope:

- main runners:
  - `scripts/export_phase5_offline_rl_dataset.py`
  - `scripts/train_phase5_iql_offline.py`
  - `scripts/train_phase5_cql_offline.py`
- outputs:
  - `outputs/phase5_rl_execution/offline_rl_readiness_report.json`
  - `outputs/phase5_rl_execution/iql_policy_report.json`
  - `outputs/phase5_rl_execution/cql_policy_report.json`
  - `outputs/phase5_rl_execution/offline_regime_breakdown_report.json`
  - `outputs/phase5_rl_execution_1000eps/offline_regime_breakdown_report.json`
  - `outputs/phase5_rl_execution_1000eps/router_summary.json`

Stable implementation details:

- rollout dataset scaling is now parameterized by `episode_count_per_policy`
- readiness report now includes:
  - `mean_steps_per_episode`
  - `min_steps_per_episode`
  - `max_steps_per_episode`
  - `policy_transition_share`
- latest larger rollout run used:
  - `episode_count_per_policy = 50`
  - total episodes = `250`
  - total transitions = `2000`
- offline algorithms currently use:
  - `d3rlpy`
  - GPU device path = `cuda:0`
- current comparison remains explicitly:
  - `offline-from-simulator`
  - not real-market logged execution learning

Stable result:

- expanded rollout export is now materially larger than the earlier `40`-episode toy sample:
  - `transition_count = 2000`
  - `episode_count = 250`
  - `mean_steps_per_episode = 8.0`
  - `min_steps_per_episode = 8`
  - `max_steps_per_episode = 8`
- offline algorithm results on the latest large-rollout run:
  - `IQL mean_reward = -11.92`
  - best heuristic on the same held-out evaluation = `impact_aware = -17.42`
  - `CQL mean_reward = -11.19`
  - best heuristic on the same held-out evaluation = `impact_aware = -17.42`
- held-out regime evaluation now exists on the same `250`-episode / `2000`-transition rollout base:
  - CQL remains better than the best heuristic in `8 / 9` held-out regime buckets
  - the only held-out bucket where CQL loses is `open_heavy + small`, where `VWAP` remains best
  - IQL is not a near-zero action-collapse policy under the current setup:
    - `mean_action = 0.414`
    - `std_action = 0.059`
  - CQL is also not saturating the action space:
    - `mean_action = 0.460`
    - `std_action = 0.025`
    - `near_zero_rate = 0.0`
    - `near_one_rate = 0.0`

Interpretation:

- scaling the rollout dataset was the correct first move before evaluating offline RL
- on the current simulator and dataset mix:
  - `IQL` no longer looks like a broken implementation; the earlier extreme underperformance was not the stable takeaway after re-running on the larger rollout base
  - `IQL` is still weaker and less expressive than `CQL`, and behaves more like a low-variance, near-constant execution-rate policy
  - `CQL` currently looks stronger than `IQL` and beats the heuristic baselines on the same held-out reward scale in most regimes
- this does **not** mean offline RL is promoted:
  - the result is still simulator-bound
  - it is based on one synthetic environment family and one logged-policy mixture
  - the improvement is not universal across all held-out regimes
- scaling the offline dataset from `250 episodes / 2000 transitions` to `1000 episodes / 8000 transitions` did **not** change the held-out CQL conclusion:
  - CQL still wins `8 / 9` buckets
  - the same losing bucket remains `open_heavy + small`
  - bucket-level sign pattern is unchanged (`9 / 9` same-sign buckets vs the smaller run)
- a minimal regime router was then tested:
  - `open_heavy + small -> VWAP`
  - all other buckets -> `CQL`
  - on the `1000`-episode held-out run, router moves the policy from `8 / 9` winning buckets to `9 / 9`
  - this is a narrow but clean gain:
    - router improves only the one known weak bucket
    - it matches pure CQL everywhere else
    - mean router-minus-CQL across buckets is about `+0.065` reward units
- practical read:
  - if offline RL continues, `CQL` should now be treated as the leading control branch
  - `IQL` remains a useful baseline/ablation, but not the preferred offline method under the current setup
  - the next highest-value evaluation step is deeper held-out regime analysis rather than immediate promotion

## Current Mainline Documents

Use these first when picking work back up:

- `docs/execution_mode_decision_note.md`
- `docs/cost_model_decision_note.md`
- `docs/platform_ml_rl_roadmap.md`
- `docs/phase_1_alpha_closeout_note.md`
- `docs/phase_1_5_alpha_decision_note.md`
- `docs/superpowers/specs/2026-04-01-phase-1-us-alpha-core-design.md`
- `docs/superpowers/specs/2026-04-01-phase-1-alpha-acceptance-gate-design.md`
- `docs/superpowers/specs/2026-04-01-phase-1-5-alpha-integration-design.md`
- `docs/superpowers/plans/2026-04-01-phase-1-us-alpha-core.md`
- `docs/superpowers/plans/2026-04-01-phase-1-alpha-acceptance-gate.md`
- `docs/superpowers/plans/2026-04-01-phase-1-5-alpha-integration.md`

## Recommended Next Steps

Priority order:

1. Treat Phase 1, Phase 1.5, Phase 2, and the first Phase 3 ML attempt as closed.
2. Keep the deterministic research baseline as the current reference signal path; do not promote `qlib_lgbm`.
3. Treat signal robustness and signal-translation quality as the bottleneck, ahead of more optimizer tuning or RL execution work.
4. Treat the current TCA overlay as valid only for `0-0.1% participation`.
5. Do not promote calibrated `k = 3.498400399110418` into default config until the estimator and alpha story both improve.
6. Treat Phase 3.5 as closed:
   - pipeline bug fixed
   - targeted retry executed
   - primary-universe ML still not promotion-worthy
7. Treat Phase 3.6 as closed:
   - neutralization and balanced weighting improved robustness
   - `top_300` and continuity slices turned acceptable
   - the actual `top_500` promotion slice still failed
   - full-sample training did not justify itself versus the `top_500-only` shadow benchmark
8. Treat Phase 3.7 and Phase 3.7x as closed:
   - transcript V1 did not rescue the structured ML branch
   - the improved transcript diagnostics showed real event information, but not in a form suitable for the current monthly carry formulation
   - transcript research should only resume as a new event-driven branch
9. Treat Phase 3.8 as closed:
   - non-overlapping `42d` parity did **not** confirm a strong enough deterministic baseline
   - `42d` is therefore not promoted as the new default research horizon
   - the event-driven transcript / grades branch did not open because Gate A failed
10. Treat Phase 3.9 as closed:
   - quasi-PIT SUE was feasible on the freeze but failed to clear the `top_500` gate
   - the apparent headline IC was materially diluted by sparse-event cross sections
   - the dense-date slice was negative, so SUE is not promoted as the missing incremental large-cap alpha
11. Treat Phase 4A as an informative universe diagnostic rather than a promotion result:
   - moving from `top_500` to `rank_500_1500` improved the deterministic momentum result materially
   - the improvement was strongest in alpha-only spread, not in monotonic rank IC
   - this is evidence that universe efficiency matters, but not yet enough to declare a clean new default universe
12. Treat Phase 4B as a successful data-layer extension:
   - free SEC 13F ingestion now exists via the official quarterly data sets
   - identifier normalization and factor construction are the next open tasks, not raw ingestion
13. Treat Phase 4C, 4D, and 4E together as a useful but currently non-promoted institutional-flow branch:
   - free SEC 13F ingestion, mapping, and multi-quarter backfill are now real assets
   - `ownership_pct_change` proved orthogonal but weak-to-negative on the primary dynamic mid-cap slice
   - `holder_count_change` looked somewhat better but was too overlapping / borderline to justify promotion
   - the branch should stay closed unless reopened under a materially different research framing
14. If alpha research resumes, either:
   - stay with deterministic baseline as the working alpha path, or
   - open a clearly new ML branch with materially different feature / label design and an objectively defined large-cap / liquid universe

Concrete next research step:

- keep the canonical 300-name list as a continuity control, but treat `expanded_liquid_core` as the primary research universe
- keep `21d` as the default research horizon until a future branch shows a stronger non-overlapping case for another horizon
- do not run another near-identical LightGBM retry on the same compact-feature setup or the same neutralized broad-training setup
- only reopen ML if there is a new hypothesis strong enough to justify a new branch
- if ML is reopened, prefer an objective `top-N liquid large-cap` research universe over a canonical-300-specific scope
- if ML is reopened, treat `top_500-only` style training as the more defensible reference branch than broad-sample training under the current feature family
- do not spend time on a PIT analyst-revision alpha unless a true historical estimate snapshot source exists; the current frozen FMP analyst-estimate payloads are not sufficient for clean revision-history research
- do not treat quasi-PIT SUE as a rescued large-cap alpha path under the current `21d non-overlapping` setup; the dense-date slice evidence is too weak
- if US research continues, prefer exploring lower-efficiency / mid-cap slices over spending more time on top-500 large-cap structured signals
- if a new institutional-flow branch is opened, use the SEC 13F official quarterly data sets as the starting point rather than rebuilding raw EDGAR filing ingestion first
- only reopen transcript or grades research as explicitly event-driven hypotheses with fresh gates, not as automatic follow-ons from the failed Phase 3.8 horizon check
- only return to PortfolioOS alpha integration once a new signal passes the primary-universe Layer 1 gate
- do not resume optimizer-promotion or RL-execution work until the alpha layer is signal-ready again

## Simplified Historical Summary

Older work is intentionally compressed here.

- Core CLI platform, replay, scenario analysis, approval/freeze, and execution simulation are complete and stable.
- Import profiles, provider-backed data builders, and snapshot bundles are complete and stable.
- Permission-aware Tushare fallbacks are implemented; `target.csv` fallback remains part of the supported path.
- Multi-period impact-aware execution infrastructure is complete and is the default simulation mode.
- Expanded-US research infrastructure, manifests, and samples are already in place and validated.
- TCA fill collection, calibration, and readiness gating have been closed for the low-participation regime.
- Cost-model and risk-aversion research both concluded that better alpha information was the highest-value bottleneck.
- Phase 1 alpha research and acceptance are now closed, with `alt_momentum_4_1` accepted as a provisional seed signal.
- Phase 1.5 alpha integration is now implemented and closed with a negative research result: the optimizer did not beat naive and the alpha-only benchmark was weak.
- The first standalone fundamentals spike on the canonical 300-name universe produced a `fundamentals_complementary` result rather than a momentum replacement, with an unresolved `254 / 300` data-coverage mismatch against the US-only FMP freeze.
- The deterministic momentum-plus-fundamentals multi-factor spike closed as `multifactor_promising_but_subset_limited` on the `254-name` covered subset.
- The first expanded-universe Qlib + LightGBM alpha attempt closed as `reject` at Layer 1 on `expanded_liquid_core`, so no ML alpha was promoted into mainline.
- Phase 3.5 then diagnosed and fixed a real `lambdarank` pipeline bug, reran one targeted retry on `top_500_liquid_dynamic`, and still closed the ML branch as `retry_reject`: improved and weakly positive, but not strong enough for promotion and not additive enough for the ensemble branch.
- A later universe-split diagnostic showed that the `300 vs 500` divergence is better interpreted as a weak large-cap / upper-universe effect than as a clean canonical-300 selection-bias artifact.
- Phase 3.6 then ran a neutralized regime-aware retry with sector/size residualization, supplementary cap/liquidity regime features, and balanced cap-bucket weights; it improved robustness and continuity but still closed as `retry_reject` because the real `top_500` deployment slice did not clear the promotion gate and broad-sample training did not beat the `top_500-only` shadow benchmark.
- Phase 3.7 then added transcript features and a horizon audit, but the transcript branch closed as `transcript_inconclusive`: weak usage, weak complementarity, and no Layer 2 promotion.
- Phase 3.7x then repaired transcript uncertainty and added carry-window / univariate diagnostics, showing that transcripts likely contain some event information but are misframed by the current monthly carry setup.
- Phase 3.8 then ran a non-overlapping `42d` promotion check and found that none of the tested deterministic candidates cleared the Gate A threshold, so `42d` was **not** promoted and the event-driven branch did not open.
- Phase 3.9 then tested a quasi-PIT earnings-surprise signal using `acceptedDate > filingDate > date` and `epsDiluted` actuals, but the top-500 result failed to clear the research gate and was revealed by dense-date diagnostics to be largely a sparse-event artifact rather than a robust new alpha source.
- Phase 4A then expanded the deterministic momentum test to the full FMP freeze with a static market-cap `501-1500` slice and showed that moving down-cap helps materially versus top-500 large-cap, especially in alpha-only spread, but still did not produce a clean strong rank-IC winner.
- Phase 4B then established a working free SEC 13F data path by ingesting the official quarterly `Form 13F Data Sets`, avoiding raw full-index scraping and making future institutional-flow factor work feasible.
- Phase 4C then built first-pass 13F institutional-flow factors and showed that `ownership_pct_change` is much more orthogonal to momentum than `holder_count_change`.
- Phase 4D then backfilled the official SEC 13F data sets from `2022-12-31` through `2025-12-31`, closing the data-history blocker for a real institutional-flow evaluation.
- Phase 4E then ran the multi-quarter 13F alpha evaluation and closed the branch: `ownership_pct_change` remained weak-to-negative on the primary dynamic `rank_500_1500` slice in both `full_carry` and `first_eval_only`, while `holder_count_change` was only borderline and too overlapping to promote.
- Horizon choice remains a first-order design decision, but `42d` is no longer an open promotion candidate under the current deterministic stack.

## Workflow Notes

- Commit every substantive change.
- Use `python -m pytest -q` as the default full-regression command on this machine.
- Generated runtime artifacts under `outputs/` remain out of version control.
