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

- `python -m pytest -q` -> `288 passed, 28 warnings`

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
- CLI:
  - `portfolio-os-alpha-research`
- Current scope is intentionally research-only:
  - consumes `returns_long.csv`
  - builds deterministic reversal and momentum signals
  - computes forward-return labels
  - emits signal panel, IC diagnostics, summary JSON, and markdown report
- Alpha is not yet connected into portfolio construction.

### Current Best Research Reading

- Alpha component diagnostics now compare:
  - `reversal_only`
  - `momentum_only`
  - `blended_alpha`
- The workflow now writes:
  - `alpha_signal_summary.csv`
- Latest real-data smoke output:
  - `outputs/alpha_research_us_expanded_component_diagnostics`

Current frozen expanded-US result:

- `momentum_only` is the strongest of the three signals.
- The current equal-weight `blended_alpha` is materially worse than pure momentum.
- The current reversal component is harmful on average in this sample.

Important numbers from the latest smoke run:

- `momentum_only`:
  - `mean_rank_ic = 0.0169594237695078`
  - `mean_top_bottom_spread = 0.004812966103636091`
- `blended_alpha`:
  - `mean_rank_ic = 0.0023249785139220847`
  - `mean_top_bottom_spread = -0.0052516018996480365`
- `reversal_only`:
  - `mean_rank_ic = -0.011141416566626648`
  - `mean_top_bottom_spread = -0.0048010166983345395`

Interpretation:

- The next alpha iteration should be momentum-first.
- The reversal leg should be reduced, redesigned, or removed before broader sweeps or optimizer integration.

## Current Mainline Documents

Use these first when picking work back up:

- `docs/execution_mode_decision_note.md`
- `docs/cost_model_decision_note.md`
- `docs/platform_ml_rl_roadmap.md`
- `docs/superpowers/specs/2026-04-01-phase-1-us-alpha-core-design.md`
- `docs/superpowers/plans/2026-04-01-phase-1-us-alpha-core.md`

## Recommended Next Steps

Priority order:

1. Continue Phase 1 alpha iteration, not optimizer surgery.
2. Rework the alpha recipe around the momentum leg first.
3. After a better alpha candidate exists, revisit alpha-aware portfolio construction.
4. Treat the current TCA overlay as valid only for `0-0.1% participation`.
5. Do not promote calibrated `k = 3.498400399110418` into default config until the estimator and optimizer story both improve.

Concrete next research step:

- add one or two momentum-first alpha variants
- compare them with the existing baseline on the same expanded-US sample
- only then decide whether Phase 1.5 should connect expected returns into the optimizer

## Simplified Historical Summary

Older work is intentionally compressed here.

- Core CLI platform, replay, scenario analysis, approval/freeze, and execution simulation are complete and stable.
- Import profiles, provider-backed data builders, and snapshot bundles are complete and stable.
- Permission-aware Tushare fallbacks are implemented; `target.csv` fallback remains part of the supported path.
- Multi-period impact-aware execution infrastructure is complete and is the default simulation mode.
- Expanded-US research infrastructure, manifests, and samples are already in place and validated.
- TCA fill collection, calibration, and readiness gating have been closed for the low-participation regime.
- Cost-model and risk-aversion research both concluded that better alpha information is now the highest-value bottleneck.

## Workflow Notes

- Commit every substantive change.
- Use `python -m pytest -q` as the default full-regression command on this machine.
- Generated runtime artifacts under `outputs/` remain out of version control.
