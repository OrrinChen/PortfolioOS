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

1. Treat Phase 1 and Phase 1.5 as closed.
2. Keep `alt_momentum_4_1` as the accepted seed alpha for research, but not as a promoted production signal.
3. Treat the current TCA overlay as valid only for `0-0.1% participation`.
4. Do not promote calibrated `k = 3.498400399110418` into default config until the estimator and optimizer story both improve.
5. Treat signal robustness and signal-translation quality as the next bottleneck, ahead of more optimizer tuning.

Concrete next research step:

- revisit the signal layer on a longer or broader US sample
- diagnose why walk-forward expected returns are nonzero on only a small subset of rebalance dates
- only return to optimizer-promotion work after the alpha layer survives realistic costed backtests

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

## Workflow Notes

- Commit every substantive change.
- Use `python -m pytest -q` as the default full-regression command on this machine.
- Generated runtime artifacts under `outputs/` remain out of version control.
