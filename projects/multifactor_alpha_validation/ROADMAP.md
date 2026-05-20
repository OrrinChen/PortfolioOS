# Multi-Factor Alpha Validation Engine Roadmap

Subtitle:

```text
A PIT-safe, redundancy-aware, cost-aware institutional factor research and backtest system.
```

Status:
Week 1-8 implemented as an independent extension roadmap. This document does
not change the root PortfolioOS phase sequence, does not create an automatic
Phase 67, and does not promote any factor family into Q2 without the root Phase
64 research import contract.

Current state:

```text
Multi-Factor Alpha Validation Engine: research workflow shape complete
Dataset onboarding gate: complete
Synthetic PIT-ready path: complete
WRDS monthly PIT dry run: complete
WRDS daily PIT bundle: pulled
First real rolling OOS evidence: complete as diagnostic evidence
Real evidence closeout: diagnostic_only
PIT exposure store: complete
Cross-sectional risk model attribution: complete as ex-post attribution only
Factor attribution waterfall: complete as diagnostic attribution only
Strict residual evidence closeout: complete
Portfolio component gate: complete
Candidate filter audit / soft resurrection: complete
Portfolio-level diagnostic ensemble OOS validation: complete
Component OOS observation expansion: complete
Post-portfolio contribution / ablation diagnosis: complete
Current next step: stop or run bounded diagnostic cost/capacity attribution; OR remains locked
```

## Positioning

The Multi-Factor Alpha Validation Engine is a standalone project story under
`projects/multifactor_alpha_validation/`. It uses PortfolioOS principles and
local APIs where useful, but it is not the PortfolioOS v1 release track.

The project asks:

```text
Can public and semi-public factor families survive institutional validation
after PIT contracts, neutralization, redundancy, marginal-value testing,
shrinkage, allocation, cost, capacity, and benchmark attribution?
```

The intended resume story is not factor replication. It is a research audit
trail that explains which candidate factors survive, which fail, and at which
layer.

## Non-Interference Contract

This roadmap is independent from the root `ROADMAP.md` active phase.

Rules:

- The root PortfolioOS roadmap remains the source of truth for PortfolioOS v1
  release packaging.
- This project may maintain its own roadmap, configs, reports, and validation
  targets under `projects/multifactor_alpha_validation/`.
- Outputs from this project do not enter Q2 directly.
- Any candidate family that should leave this project must pass the root Phase
  64 research import contract before Q1 evidence, Promotion Gate, Q2 execution
  evaluation, or Alpha Registry updates.
- Factor Discovery Sandbox artifacts remain calibration-only until separately
  imported through governance.
- Teaching-mode artifacts must remain labeled survivorship-biased,
  educational-only, and not alpha evidence.
- Missing coverage must be explicit abstain. `no_view != zero_alpha` remains a
  hard contract.
- The project must not emit broker, live-trading, production-approval, or
  security-order artifacts.

## Relationship to Existing Work

Existing sandbox path:

```text
projects/multifactor_alpha_validation/factor_discovery_sandbox/
```

The sandbox has already proved a local, controlled version of several concepts:

- factor spec generation for 29 price-volume candidates
- timestamp contracts and explicit abstain coverage rules
- rolling out-of-sample ICIR weighting
- weighting reliability diagnostics comparing rolling ICIR against equal,
  family-equal, shrunk ICIR, signed shrunk ICIR, and rolling ridge estimators
- a separate small-cap candidate-family line with data admission, universe
  tiering, microcap quarantine, a locally pulled WRDS CRSP daily small-cap
  bundle, and fixed single-signal residual momentum diagnostics
- redundancy and marginal-value decisions
- shrinkage, covariance stabilization, diagnostic allocation, and zero-weight
  attribution
- cost stress, capacity frontier, benchmark attribution, survival funnel, and a
  Phase 64 import bundle

This roadmap upgrades the idea into a formal project story with a smaller,
institutional MVP factor library and clearer separation between:

- candidate generation sandbox
- formal factor validation engine
- PortfolioOS Q1/Q2 governance

## Target Repository Layout

```text
projects/multifactor_alpha_validation/
  ROADMAP.md
  README.md
  configs/
    mvp_us_liquid.yaml
    cost_model_mvp.yaml
    allocator_mvp.yaml
  factor_specs/
    momentum_12_1.yaml
    reversal_5_1.yaml
    low_vol_60d.yaml
    liquidity_turnover.yaml
    value_bm.yaml
    profitability_quality.yaml
    investment_asset_growth.yaml
    accruals.yaml
    sue_event_reference.yaml
    analyst_revision_disabled.yaml
  src/multifactor_alpha_validation/
    schema.py
    data_contract.py
    factor_library.py
    signal_builders/
    alpha_view_mapper.py
    backtest_kernel.py
    neutralization.py
    q1_evidence.py
    redundancy_gate.py
    marginal_value.py
    shrinkage.py
    covariance.py
    allocator.py
    zero_weight_attribution.py
    cost_capacity.py
    benchmark_attribution.py
    registry.py
    reports.py
    dashboard.py
  scripts/
    run_multifactor_research_mode_preflight.py
    run_wrds_multifactor_ingest.py
    run_factor_signal_builders.py
    run_factor_q1_evidence.py
    run_factor_redundancy_gate.py
    run_factor_allocator.py
    run_factor_survival.py
    build_factor_registry.py
    build_factor_research_report.py
    render_factor_dashboard.py
  tests/
```

The existing `factor_discovery_sandbox/` stays as a separate sandbox. Its code
may be reused through explicit adapters, but it should not become the formal
engine by rename alone.

## MVP Factor Library

The first formal engine version should stay below 10 candidate families:

| Factor | Data tier | Initial status |
| --- | --- | --- |
| 12-1 momentum | Tier 1 price | enabled |
| short-term reversal | Tier 1 price | enabled |
| low volatility / low beta | Tier 1 price | enabled |
| liquidity / turnover | Tier 1 price-volume | enabled |
| value / book-to-market | Tier 2 fundamentals | enabled with lag |
| profitability / quality | Tier 2 fundamentals | enabled with lag |
| investment / asset growth | Tier 2 fundamentals | enabled with lag |
| accruals | Tier 2 fundamentals | enabled with lag |
| SUE / earnings surprise | Tier 3 event | reference benchmark |
| analyst revision | Tier 3 estimates | disabled unless PIT source exists |

Tier rules:

- Tier 1 price-volume factors require signal, visibility, and tradability
  timestamps.
- Tier 2 fundamental factors require reporting lag and unavailable handling.
- Tier 3 event or analyst factors require strict visibility timestamps.
- Analyst revision remains disabled without a WRDS-style PIT source.

## Core Contracts

### FactorSpec

Each factor spec must record:

- factor id, family id, display name, mechanism, and mechanism type
- required and optional data fields
- signal timestamp rule
- visibility timestamp rule
- tradable timestamp rule
- reporting lag
- missing coverage policy
- lookback, skip, transform, winsorization, horizon, and rebalance frequency
- beta, sector, size, liquidity, and cost-sensitivity reporting requirements
- known failure modes
- explicit boundary flags preventing live or production claims

### SignalPanel

Each signal row must record:

- schema version
- factor id
- date and asset id
- raw and normalized signal
- coverage flag
- abstain reason when coverage is false
- signal timestamp
- visibility timestamp
- tradable timestamp
- horizon start and horizon end
- provenance hash

Rules:

- `coverage_flag=false` means the row cannot enter expected-return projection.
- `abstain_reason` is required when coverage is false.
- `visibility_timestamp <= tradable_timestamp`.
- `no_view != zero_alpha`.

### Evidence, Marginal Value, Shrinkage, and Allocation

The engine should emit typed summaries for:

- factor evidence
- marginal value
- shrinkage result
- covariance diagnostics
- allocator result
- zero-weight attribution
- cost/capacity survival
- factor registry status

The result should be a layered research audit record, not a single pass/fail
backtest.

## Eight-Week Roadmap

### Week 1: Charter, Factor Specs, PIT Data Contract

Goal:
Start from contracts, not factor formulas.

Work:

- Write project charter.
- Implement FactorSpec schema.
- Implement PIT data contract validation.
- Add a research-mode preflight that validates actual dataset manifests before
  formal factor validation.
- Add an opt-in WRDS ingest path that uses local credentials without storing
  secrets in the repository.
- Add eight base factor specs.
- Add SUE reference and disabled analyst revision specs.

Artifacts:

- `docs/multifactor_alpha_validation_charter.md`
- `factor_specs/*.yaml`
- `outputs/factor_spec_validation/spec_validation_report.json`
- `outputs/multifactor_alpha_validation/research_mode_preflight/pit_contract_validation.json`
- `outputs/multifactor_alpha_validation/research_mode_preflight/research_mode_readiness.md`
- `data/cache/wrds_multifactor/standardized/research_mode_dataset_manifest.yaml`

Acceptance:

- At least eight factor specs validate.
- Every factor has signal, visibility, and tradability timestamp rules.
- Fundamentals require reporting lag.
- Analyst revision is disabled without a PIT source.
- Missing coverage policy is explicit abstain.
- Local current-constituent/yfinance proxy data fails closed before real
  research mode.
- WRDS query config validation rejects embedded credentials.
- No live, broker, production-approval, or security-order artifact is produced.

### Week 2: Signal Builders and AlphaView Mapper

Goal:
Convert formulas into auditable signal panels and typed predictive views.

Work:

- Implement price-volume signal builders for momentum, reversal, low volatility,
  and liquidity.
- Implement fundamental builder skeletons for value, profitability, investment,
  and accruals.
- Emit standardized SignalPanel outputs.
- Map SignalPanel rows to Typed AlphaView-compatible objects.
- Preserve no-view, zero-alpha, and abstain distinctions.

Artifacts:

- `outputs/factor_signals/signal_panel_{factor_id}.csv`
- `outputs/factor_alpha_views/alpha_view_{factor_id}.json`
- `outputs/factor_signals/abstain_report.csv`

Acceptance:

- Enabled factors produce standard signal panels.
- All signal rows have timestamp fields.
- Insufficient coverage writes abstain output, not zero alpha.
- Price-only factors map to fixed-horizon AlphaViews.
- SUE maps only as an event reference.
- Analyst revision remains disabled unless PIT source exists.

### Week 3: Backtest Kernel, Evidence, Neutralization

Goal:
Separate raw factor performance from beta, sector, style, liquidity, coverage,
turnover, and decay.

Work:

- Implement minimal cross-sectional backtest kernel.
- Support monthly rebalance, quantile spread, optional tilted sleeve, and
  visibility-to-tradability gap.
- Compute rank IC, IC t-stat, top-bottom spread, turnover, coverage, and decay.
- Report beta, sector, size, and liquidity exposures.
- Emit raw and neutralized evidence tables.

Artifacts:

- `outputs/factor_q1/factor_evidence_table.csv`
- `outputs/factor_q1/factor_evidence_{factor_id}.json`
- `outputs/factor_q1/neutralization_report.csv`
- `outputs/factor_q1/q1_summary.md`

Acceptance:

- Every factor has raw and neutralized readouts.
- Coverage, turnover, and decay are reported.
- Benchmark-relative and raw performance are separate.
- Exposure reports are present.
- Single-factor IC cannot automatically promote a factor.

### Week 4: Redundancy and Marginal-Value Gate

Goal:
Ask whether a factor adds value after the existing factor set.

Work:

- Compute factor correlation matrix.
- Assign clusters.
- Select baseline set.
- Compute residual IC after baseline.
- Compute incremental gross and net spread.
- Compute incremental turnover and cost drag.
- Assign marginal-value decision.

Artifacts:

- `outputs/factor_redundancy/factor_correlation_matrix.csv`
- `outputs/factor_redundancy/factor_clusters.csv`
- `outputs/factor_redundancy/marginal_value_report.json`
- `outputs/factor_redundancy/marginal_value_decision_table.csv`
- `outputs/factor_redundancy/redundancy_report.md`

Acceptance:

- Highly correlated factors share clusters.
- Every factor has residual contribution.
- Negative cost-adjusted marginal value cannot promote.
- Redundant factors receive explicit decisions.
- Raw IC alone cannot route a factor to allocation.

### Week 5: Shrinkage, Covariance Stabilization, Overfit Controls

Goal:
Control noisy public-factor estimates before allocation.

Work:

- Implement posterior expected-return shrinkage.
- Derive shrinkage intensity from evidence, stability, coverage, decay, and
  marginal-value scores.
- Implement covariance shrinkage toward diagonal and block-cluster targets.
- Report condition-number diagnostics.
- Add half-sample, rolling-window, regime-split, and placebo sanity checks.

Artifacts:

- `outputs/factor_shrinkage/factor_posterior_mu.csv`
- `outputs/factor_shrinkage/shrinkage_summary.json`
- `outputs/factor_covariance/factor_covariance_sample.csv`
- `outputs/factor_covariance/factor_covariance_shrunk.csv`
- `outputs/factor_covariance/covariance_diagnostics.json`
- `outputs/factor_stability/stability_report.csv`

Acceptance:

- Low stability, coverage, or marginal value leads to stronger shrinkage.
- Rejected factors cannot be revived by shrinkage.
- Covariance diagnostics report before/after condition numbers.
- Near-duplicate factors are reported.
- Shrinkage parameters are preregistered, not tuned to best backtest output.

### Week 6: Nonnegative Factor Allocator and Zero-Weight Attribution

Goal:
Allocate only after evidence, redundancy, shrinkage, covariance, and cost
controls, then explain every zero factor weight.

Work:

- Implement nonnegative factor allocator.
- Penalize risk, turnover, expected cost, and cluster concentration.
- Enforce per-factor, per-family, per-cluster, and evidence-threshold
  constraints.
- Implement zero-weight attribution.
- Add sign-flip, scale-response, no-view, zero-alpha, and redundancy sanity
  checks.

Artifacts:

- `outputs/factor_allocator/factor_allocator_weights.csv`
- `outputs/factor_allocator/zero_weight_attribution.csv`
- `outputs/factor_allocator/allocator_diagnostics.json`
- `outputs/factor_allocator/allocator_sanity_checks.json`
- `outputs/factor_allocator/allocator_report.md`

Acceptance:

- Every zero-weight factor has a reason.
- Sign-flip check changes allocator ranking.
- Scale-response check is monotone.
- No-view and zero-alpha remain distinct.
- High-redundancy factors are compressed or archived.
- The allocator remains factor-level research output only.

### Week 7: Cost, Capacity, Benchmark Survival

Goal:
Turn factor allocation into an institutional research report.

Work:

- Implement fixed, spread, turnover, impact-proxy, and fee cost stress.
- Implement capacity stress across AUM, ADV participation, liquidity buckets,
  bottleneck names, and net alpha versus AUM.
- Implement benchmark attribution across raw, relative, beta-adjusted,
  sector/style-adjusted, and liquidity-bucket views.
- Implement survival funnel.

Artifacts:

- `outputs/factor_survival/survival_funnel.csv`
- `outputs/factor_survival/cost_stress_matrix.csv`
- `outputs/factor_survival/capacity_frontier.csv`
- `outputs/factor_survival/benchmark_attribution.csv`
- `outputs/factor_survival/failure_attribution.json`
- `outputs/factor_survival/survival_summary.md`

Acceptance:

- Negative cost-adjusted net result cannot be called survived.
- Capacity curve shows net alpha versus AUM.
- Beta-adjusted and raw benchmark results are separate.
- Factor failure is attributed to a specific layer.
- Unavailable rows stay unavailable.

### Week 8: Registry, Report, Dashboard, Resume Packaging

Goal:
Package the project as a reproducible, interview-ready research audit trail.

Work:

- Build Factor Registry.
- Generate final research report.
- Render static read-only dashboard.
- Generate artifact manifest.
- Write README, RUNBOOK, VALIDATION, resume bullets, and interview talking
  points.

Artifacts:

- `outputs/factor_registry/factor_registry.yaml`
- `outputs/factor_registry/factor_decision_table.csv`
- `reports/multifactor_alpha_validation_report.md`
- `outputs/factor_dashboard/dashboard.html`
- `outputs/factor_release/artifact_manifest.json`
- `docs/interview_talking_points.md`

Acceptance:

- Every factor has final status and stop layer.
- Every zero factor weight has attribution.
- Report states all non-claims.
- Dashboard shows survival funnel, factor table, allocator weights,
  zero-weight attribution, cost/capacity frontier, and registry.
- `make factor-validate` passes after the target exists.

## Registry Statuses

Allowed final statuses:

- `spec_validated`
- `pit_rejected`
- `q1_pass`
- `q1_diagnostic_only`
- `real_but_redundant`
- `archive_no_marginal_value`
- `promote_to_allocator`
- `allocator_weight_positive`
- `allocator_weight_zero_low_confidence`
- `allocator_weight_zero_redundancy`
- `allocator_weight_zero_cost`
- `allocator_weight_zero_capacity`
- `cost_survived`
- `cost_killed`
- `capacity_limited`
- `benchmark_exposure_only`
- `survival_inconclusive`

Pass/fail-only labels are not allowed.

## Validation Target

The eventual project-level validation target should be:

```bash
make factor-validate
```

Minimum test coverage:

- factor spec timestamp rules
- missing PIT contract rejection
- visibility before tradability
- missing coverage to explicit abstain
- `no_view != zero_alpha`
- reporting lag for fundamentals
- raw versus neutralized evidence separation
- redundancy gate high-correlation rejection
- shrinkage of low-stability alpha
- covariance condition-number improvement
- allocator sign-flip and scale-response sanity checks
- zero-weight reason requirement
- cost-survival block for negative net alpha
- capacity bottleneck reporting
- beta exposure reporting
- registry stop-layer labels
- non-claim language in report
- read-only dashboard behavior
- no forbidden research-to-trading artifacts

## Success Criteria

Technical success:

- 8-10 factor specs validate.
- Every factor has PIT, timestamp, and horizon contract.
- Every signal emits a standardized signal panel.
- Every factor has Q1-style evidence.
- Every factor has redundancy and marginal-value decision.
- Allocator emits factor weights and zero-weight attribution.
- Final registry records each factor stop layer.

Research success:

- Several factors retain neutralized evidence after Q1-style checks.
- A smaller subset clears marginal-value gates.
- Allocator is not dominated by a single factor cluster.
- Cost and capacity stress produce either local survivors or an honest null.

Honest failure success:

- PIT failures, benchmark exposures, redundancy, shrinkage, costs, capacity, and
  allocator zero weights are explained explicitly.
- Unavailable evidence remains unavailable.
- The final report is credible even if no factor survives all layers.

## Final Deliverable

The final deliverable is not a backtest curve. It is a full research audit trail
showing:

- which factors were tested
- how each factor was timestamped
- what evidence survived neutralization
- what was redundant
- what was shrunk
- what the allocator used or rejected
- where cost or capacity killed apparent alpha
- what each final registry status means

## Next Roadmap: Real PIT Dataset Onboarding

Do not add more factor logic before the real dataset gate is solved. The next
roadmap is `MF-R0` through `MF-R5`, focused only on formal research-mode data
readiness.

Do not open:

- more factors
- better allocator logic
- dashboard polish
- ML models
- return tuning

### MF-R0: Dataset Manifest Contract

Goal:
Define which datasets are eligible for formal research mode.

Required manifest fields:

- historical universe membership
- adjusted price-volume panel
- QQQ benchmark panel
- delisting and inactive asset handling
- trading calendar
- timestamp policy
- source provenance
- content hash
- allowed use mode

Acceptance:

- Current-constituent/yfinance-style manifests are blocked.
- Synthetic ready fixture manifests can pass.
- Missing historical membership is blocked.
- Missing adjusted price-volume panel is blocked.
- Missing benchmark panel is blocked.
- Missing delisting handling is blocked.
- Same-close trading is blocked.

### MF-R1: Historical Universe Membership Loader

Goal:
Load historical NASDAQ100-style membership instead of current constituents.

Required fields:

- date
- asset_id
- ticker
- in_universe
- entry_date
- exit_date
- source

Acceptance:

- Each rebalance date sees only the universe visible at that time.
- Current constituents cannot be backfilled into history.
- Exited names remain present during their valid historical windows.
- Universe snapshots are written as artifacts.

Artifacts:

- `outputs/multifactor_alpha_validation/research_dataset/universe_snapshots/`
- `outputs/multifactor_alpha_validation/research_dataset/historical_membership_validation.json`

### MF-R2: Adjusted Price/Volume and Benchmark Panel

Goal:
Load adjusted OHLCV and QQQ benchmark panels.

Required asset fields:

- date
- asset_id
- adjusted_open
- adjusted_close
- volume

Required QQQ fields:

- date
- adjusted_open
- adjusted_close
- volume

Acceptance:

- Price panel covers the universe membership window.
- QQQ benchmark covers the full backtest date range.
- Adjusted price convention is explicit.
- Missing data enters coverage/abstain handling instead of being silently filled
  with zero.

### MF-R3: Delisting and Inactive Asset Handling

Goal:
Block survivorship leakage from names that disappear from the active universe.

Required fields:

- delisting_date
- delisting_return or terminal_return_policy
- inactive_reason
- last_trade_date

Acceptance:

- Delisted or inactive assets do not disappear from historical validation.
- Delisting policy is explicitly recorded.
- Missing delisting handling blocks research mode.
- Delisting coverage is reported.

### MF-R4: First Real Research Dry Run

Goal:
Run the smallest real PIT research pass without a full strategy or allocator.

Allowed factors:

- `momentum_12_1`
- `reversal_5_1`
- `low_vol_60d`

Allowed layers:

- signal builder
- AlphaView mapper
- Q1 evidence
- benchmark attribution

Acceptance:

- Research preflight status is ready.
- Signal timestamps are correct.
- Same-close trading is not used.
- QQQ-relative and beta-adjusted readouts are separate.
- Report does not claim alpha success.

### MF-R5: Rolling OOS Factor Validation

Goal:
Move from infrastructure fixtures to real out-of-sample factor validation.

Rule:

```text
for each rebalance month t:
  estimate IC / ICIR / shrinkage input using history before t
  form scores at t
  trade no earlier than t+1 under the timestamp policy
```

Acceptance:

- Full-sample ICIR weighting is forbidden.
- Train, validation, and test windows are explicit.
- Raw, neutralized, and cost-adjusted readouts are separate.
- Factor survival funnel is generated.
- Weak or collapsed results are recorded honestly.

### MF-R6: External PIT Dataset Source Adapter

Goal:
Wire a real external PIT source into source validation artifacts before running
alpha evidence.

Allowed sources:

- WRDS / CRSP / Compustat / historical index membership source
- user-provided PIT parquet/csv bundle configured through local paths

Artifacts:

- `outputs/multifactor_alpha_validation/external_dataset_source/dataset_source_manifest.yaml`
- `outputs/multifactor_alpha_validation/external_dataset_source/source_field_mapping.yaml`
- `outputs/multifactor_alpha_validation/external_dataset_source/dataset_ingest_validation.json`
- `outputs/multifactor_alpha_validation/external_dataset_source/dataset_readiness.md`

Acceptance:

- Credentials are not embedded in config.
- Raw external data is not committed.
- All source and output paths are specified through local config.
- Historical universe membership is proven historical, not current constituents.
- Adjusted price-volume output is explicitly proven.
- QQQ benchmark coverage source is explicit.
- Delisting and inactive handling source exists.
- If any proof is missing, readiness remains blocked.

### MF-R7: Real Dataset Dry Run, No Factor Claims

Goal:
After real data is ingested locally, run only data-readiness dry checks.

Allowed checks:

- data coverage
- timestamp alignment
- universe snapshots
- signal availability
- benchmark alignment
- delisting coverage

Forbidden at this phase:

- strategy return claims
- alpha conclusions
- factor ranking
- optimizer weights

Status:

- Complete for the local WRDS monthly PIT bundle.
- The dry run reads the real monthly manifest and CSVs, writes coverage,
  timestamp alignment, universe snapshot, signal availability, benchmark
  alignment, and delisting coverage artifacts, and keeps all non-claim flags
  closed.
- Daily price-volume validation remains a separate long task and was not
  started by MF-R7.

### MF-R8: First Real Rolling OOS Evidence

Goal:
Run first real rolling OOS evidence only, without allocator promotion.

Rules:

- estimate IC/ICIR using data strictly before each rebalance month
- form signals at the rebalance month
- trade no earlier than the next session under timestamp policy
- report raw, QQQ-relative, beta-adjusted, sector-adjusted, gross, and net
  evidence separately

Status:

- Complete on the local WRDS daily PIT bundle.
- Inputs: historical Nasdaq100 membership, daily adjusted price-volume, QQQ
  benchmark, and explicit delisting/inactive handling under ignored
  `data/cache/wrds_multifactor/nasdaq100_daily/`.
- Scope: `momentum_12_1`, `reversal_5_1`, and `low_vol_60d` only.
- Outputs live under
  `outputs/multifactor_alpha_validation/wrds_real_oos_evidence/`.
- The run records `full_sample_icir_used=false`,
  `prior_history_only=true`, `allocator_ran=false`,
  `alpha_success_claimed=false`, and `not_alpha_evidence=true`.
- Sector attribution is observed from the historical universe membership panel.
- Style attribution is observed as WRDS size/liquidity/volatility proxies:
  `market_cap`, `liquidity_score_60d`, and `volatility_score_60d`. The refreshed
  cache lives under `data/cache/wrds_multifactor/nasdaq100_daily_size/`.
- The output remains diagnostic evidence only.

### MF-R9: Real Evidence Closeout Gate

Goal:
Decide whether the first real evidence can enter redundancy or must stay
blocked/diagnostic.

Allowed decisions:

- `ready_for_redundancy_gate`
- `needs_data_fix`
- `diagnostic_only`
- `blocked`

No decision may approve production, paper canary, live trading, or portfolio
deployment.

Status:

- Complete.
- Output lives under
  `outputs/multifactor_alpha_validation/wrds_real_evidence_closeout_size/`.
- Current decision: `diagnostic_only`.
- Reasons: `style_proxy_only` and `benchmark_beta_style_conflict`. The proxy
  attribution is useful for diagnostics, but `momentum_12_1` has negative
  QQQ-relative and beta-adjusted readouts while its style-adjusted proxy net
  spread is positive. That positive proxy residual does not override
  benchmark/beta failure and is not strong enough to enter the redundancy gate.
- The closeout writes `real_evidence_conflict_diagnostics.csv` so this conflict
  is an explicit artifact, not an informal interpretation.
- Allocator entry, redundancy-gate entry, direct Q2 entry, paper canary, live
  trading, security orders, and production approval all remain false.

### MF-R10: PIT Exposure Store

Goal:
Build a PIT-safe exposure panel for risk attribution. This is not a factor
builder and does not create alpha evidence.

Scope:

- sector
- industry
- trailing market beta
- log market cap
- liquidity ADV
- residual volatility
- short-term reversal exposure
- medium-term momentum exposure
- book-to-market proxy from visible Compustat quarterly fundamentals
- profitability proxy from visible Compustat quarterly fundamentals
- asset-growth proxy from visible Compustat quarterly fundamentals

Status:

- Complete.
- Output lives under `outputs/multifactor_alpha_validation/risk_model/`.
- The current real WRDS smoke writes `223,960` exposure rows across `204`
  month-end dates, `226` assets, and `11` exposure names.
- Timestamp checks pass: `exposure_date <= visibility_timestamp <=
  tradable_timestamp`, and same-close trading is false.
- Fundamentals use only the latest quarterly row with
  `visibility_timestamp <= signal_date`; missing or unusable rows become
  explicit abstain rows.
- The coverage report shows high coverage for price/sector exposures and
  explicit abstain reasons for missing fundamentals or insufficient trailing
  price history.
- The manifest sets `allowed_use_mode=risk_attribution_input_only`.
- R10 does not run cross-sectional attribution, redundancy gates, allocator
  weights, Q2, paper canary, live trading, security orders, or production
  approval.

### MF-R11: Cross-Sectional Risk Model

Goal:
Use the PIT exposure store to decompose realized next-period returns into
common risk components and residuals. This is ex-post attribution only, not a
prediction model.

Scope:

- intercept contribution
- trailing market beta contribution
- industry contribution
- configured style proxy contribution
- fitted return
- residual return
- coefficient table by rebalance period
- fit diagnostics and regression-instability reporting

Status:

- Complete.
- Output lives under `outputs/multifactor_alpha_validation/risk_model/`.
- The current real WRDS smoke writes `203` period rows, `37,152`
  residual-return rows, and `8,622` coefficient rows.
- Artifacts:
  - `risk_model_returns_by_period.csv`
  - `risk_model_exposure_coefficients.csv`
  - `risk_model_residual_returns.csv`
  - `risk_model_fit_diagnostics.json`
- Rows with missing required exposures become explicit abstain rows. Missing
  exposure coverage is not filled with zero alpha or a synthetic residual.
- The diagnostics set `model_use=ex_post_attribution_only` and explicitly state
  that configured proxy residuals are not style-neutral alpha.
- R11 does not create factor signals, allocator weights, Q2 input, paper canary,
  live trading, security orders, or production approval.

### MF-R12: Attribution Waterfall

Goal:
Explain each factor sleeve's realized spread as a configured risk-model
decomposition before any redundancy or allocator gate can see it.

Scope:

- gross spread
- QQQ-relative spread
- market-beta contribution and beta-adjusted spread
- industry contribution and industry-adjusted spread
- configured style-proxy contribution and style-proxy-adjusted spread
- full residual spread
- reconstruction diagnostics
- style-proxy conflict flags

Status:

- Complete.
- Output lives under `outputs/multifactor_alpha_validation/risk_model/`.
- The current real WRDS smoke writes `3` factor rows and `609` factor-period
  waterfall rows.
- Artifacts:
  - `factor_attribution_waterfall.csv`
  - `factor_attribution_waterfall_by_period.csv`
  - `factor_attribution_waterfall_{factor_id}.json`
  - `factor_attribution_diagnostics.json`
  - `factor_attribution_report.md`
- Current factor statuses are `style_proxy_conflict` for `momentum_12_1`,
  `reversal_5_1`, and `low_vol_60d`. Positive configured proxy residuals do
  not override negative benchmark/beta readouts.
- R12 outputs are diagnostic attribution only. They do not create alpha
  evidence, factor promotion, redundancy-gate access, allocator weights, Q2
  input, paper canary, live trading, security orders, or production approval.

### MF-R13: Strict Residual Evidence Closeout

Goal:
Convert the MF-R12 factor attribution waterfall into strict factor-level
stop-layer decisions before any redundancy or allocator gate can see the
candidate set.

Scope:

- raw, QQQ-relative, beta-adjusted, industry-adjusted, style-proxy-adjusted, and
  full-residual gate checks
- residual-positive stability check
- style-proxy conflict blocking
- factor registry risk-model update
- fixed failure diagnosis report
- QQQ-relative hard-gate review for long-short factor spreads
- explicit non-claim and no-Q2/no-allocator boundaries

Status:

- Complete.
- Output lives under `outputs/multifactor_alpha_validation/risk_model/`.
- The current real WRDS smoke writes `3` factor decisions and
  `ready_for_redundancy_count=0`.
- Artifacts:
  - `strict_residual_closeout_decision_table.csv`
  - `strict_residual_closeout_diagnostics.json`
  - `factor_registry_risk_model_update.yaml`
  - `strict_residual_closeout_report.md`
  - `factor_failure_diagnosis.csv`
  - `qqq_relative_guard_review.json`
  - `factor_failure_diagnosis_report.md`
- Current factor decisions:
  - `momentum_12_1`: `insufficient_residual_evidence`
  - `reversal_5_1`: `style_proxy_conflict`
  - `low_vol_60d`: `style_proxy_conflict`
- Positive configured proxy residuals do not override negative benchmark/beta
  readouts or unstable full-residual evidence.
- The fixed failure diagnosis says the QQQ-relative guard is over-strict as a
  hard gate for long-short factor spreads, but softening it rescues `0` current
  factors because residual stability and beta/exposure blockers remain.
- R13 outputs are research closeout states only. They do not create alpha
  evidence, redundancy-gate access, allocator weights, Q2 input, paper canary,
  live trading, security orders, or production approval.

### MF-R14: Portfolio Component Gate

Goal:
Convert risk-attributed closeout rows into portfolio component roles. A factor
does not need standalone clean residual alpha to be studied as a documented
portfolio component, but hard validity failures still block it.

Scope:

- standalone clean alpha count
- eligible benchmark/style premia components
- eligible hedge/diversifier components
- hard validity blocking for PIT, timestamp, coverage, or unavailable data
- diagnostic ensemble validation mode
- explicit non-claim and no-Q2/no-unrestricted-allocator boundaries

Status:

- Complete.
- Output lives under `outputs/multifactor_alpha_validation/risk_model/`.
- The current real WRDS smoke writes `3` component rows with
  `standalone_clean_alpha_count=0`, `component_candidate_count=3`, and
  `portfolio_validation_mode=diagnostic_ensemble_only`.
- Artifacts:
  - `component_candidate_table.csv`
  - `portfolio_component_gate_summary.json`
  - `portfolio_component_gate_report.md`
- Current component roles:
  - `momentum_12_1`: `eligible_benchmark_premia_component`
  - `reversal_5_1`: `eligible_benchmark_premia_component`
  - `low_vol_60d`: `eligible_hedge_component`
- R14 does not promote factors as standalone alpha. It allows only portfolio-level
  diagnostic ensemble validation with later ablation and baseline comparison.

### MF-R14.5: Filter Audit and Candidate Resurrection

Goal:
Audit the formal factor candidate set before portfolio-level validation, so R15
does not accidentally run only the factors that survived early standalone
residual screening.

Scope:

- audit every formal `factor_specs/*.yaml` candidate
- separate hard validity failures from soft evidence labels
- keep PIT, timestamp, lookahead, survivorship, same-close trading, missing
  required data, and forward-return leakage failures blocked
- resurrect soft failures such as weak standalone IC, insufficient residual
  evidence, benchmark exposure, style-proxy conflict, high correlation, high
  turnover warning, or unstable standalone evidence as portfolio components
- preserve Factor Discovery teaching/proxy candidates outside formal research
  mode unless separately imported through the approved contract
- write the R15 input component pool explicitly

Status:

- Complete.
- Output lives under `outputs/multifactor_alpha_validation/risk_model/`.
- The current formal-spec audit writes `10` candidate rows with
  `component_pool_count=9` and `hard_excluded_count=1`.
- Artifacts:
  - `candidate_filter_audit.csv`
  - `hard_excluded_candidates.csv`
  - `soft_resurrected_component_pool.csv`
  - `component_pool_manifest.json`
  - `filter_audit_report.md`
- Current hard exclusion:
  - `analyst_revision_disabled`: `missing_pit_estimate_source`
- Current resurrected pool includes the three R14 component rows plus enabled
  formal specs not yet risk-attributed and the `sue_event_reference` reference
  component.
- R14.5 does not create alpha evidence, redundancy-gate access, allocator
  weights, Q2 input, paper canary, live trading, security orders, or production
  approval. It only defines the correct portfolio-level diagnostic input set.

### MF-R15: Portfolio-Level OOS Ensemble Validation

Goal:
Evaluate whether the soft-resurrected component pool has basic portfolio-level
life before any OR optimizer, unrestricted allocator, or security-level
portfolio construction is opened.

Scope:

- consume `soft_resurrected_component_pool.csv`
- exclude hard-blocked components
- keep soft-labeled components eligible when OOS observations exist
- report unavailable component observations explicitly
- evaluate rolling/prior-only diagnostic ensembles:
  - `equal_weight_all_components`
  - `equal_weight_by_cluster`
  - `inverse_vol_ensemble`
  - `simple_shrinkage_ensemble`
  - `current_three_factor_component_ensemble`
  - `best_single_factor`
  - `QQQ_benchmark`
  - `random_weight_placebo`
  - `permuted_signal_placebo`
- compare against QQQ, best single factor, random-weight placebo, and permuted
  placebo
- keep `full_sample_weights_used=false`
- keep OR optimizer, security-level construction, Q2, paper/live, broker/order,
  and production approval disabled

Status:

- Complete.
- Output lives under `outputs/multifactor_alpha_validation/portfolio_validation/`.
- Artifacts:
  - `portfolio_ensemble_oos_report.csv`
  - `ensemble_vs_baselines.csv`
  - `ensemble_validation_summary.json`
  - `random_weight_placebo_report.csv`
  - `permuted_signal_placebo_report.csv`
  - `portfolio_validation_report.md`
- Current real WRDS smoke:
  - `validation_status=evaluated`
  - `decision_state=portfolio_component_pool_fails_cost`
  - `input_component_count=9`
  - `available_component_count=8`
  - `unavailable_component_count=1`
  - `hard_blocked_component_count=0`
  - `full_sample_weights_used=false`
  - `or_optimizer_used=false`
- Current available components are `momentum_12_1`, `reversal_5_1`,
  `low_vol_60d`, `liquidity_turnover`, `value_bm`,
  `profitability_quality`, `investment_asset_growth`, and `accruals`.
  `sue_event_reference` remains unavailable until a PIT event visibility
  timestamp path is wired.
- The primary equal-weight component ensemble has negative gross,
  cost-adjusted annualized, and QQQ-relative returns, so the component pool does
  not justify OR optimization.
- R15 is diagnostic portfolio validation only. It does not create alpha evidence,
  redundancy-gate access, allocator weights, Q2 input, paper canary, live
  trading, security orders, OR optimization, or production approval.

### MF-R15.5: Portfolio Assembly Audit

Goal:
Determine whether the R15 result reflects a true component-pool failure, an
observed-subset failure, a construction issue, a cost issue, a direction issue,
or an incomplete-observation problem.

Scope:

- consume the R15 portfolio validation artifacts
- compare observed components against the full resurrected component pool
- report unavailable components instead of fabricating returns
- audit component direction conventions
- split gross return, cost drag, and net return
- evaluate role-aware diagnostic ensemble mixes:
  - `return_driver_only`
  - `hedge_only`
  - `benchmark_premia_only`
  - `return_driver_plus_hedge_80_20`
  - `momentum_plus_low_vol`
  - `momentum_plus_reversal`
  - `risk_balanced_by_component_vol`
  - `turnover_capped_equal_weight`
- review whether QQQ-relative weakness is a benchmark/beta conflict
- keep OR optimizer, security-level construction, Q2, paper/live, broker/order,
  and production approval disabled

Status:

- Complete.
- Output lives under `outputs/multifactor_alpha_validation/portfolio_validation/`.
- Artifacts:
  - `portfolio_assembly_audit.json`
  - `observed_subset_coverage_report.csv`
  - `component_direction_audit.csv`
  - `gross_to_net_waterfall.csv`
  - `role_aware_ensemble_report.csv`
  - `decision_state_reclassification.md`
- Current real WRDS smoke after component OOS observation expansion:
  - `original_decision_state=portfolio_component_pool_fails_cost`
  - `reclassified_decision_state=component_pool_fails_gross`
  - `component_pool_validation_state=component_pool_observation_sufficient`
  - `eligible_component_count=9`
  - `observed_component_count=8`
  - `unavailable_component_count=1`
  - `coverage_ratio=0.8888888889`
  - `benchmark_exposure_conflict=true`
  - `or_optimizer_used=false`
  - `security_level_portfolio_construction_used=false`
- R15.5 now diagnoses the current observed component pool: the primary
  construction fails gross and cost-adjusted OOS, while `sue_event_reference`
  remains explicitly unavailable. OR optimization remains blocked.

### MF-R15.6A: Component OOS Observation Expansion

Goal:
Safely add real OOS observations for resurrected components where the local
WRDS daily price-volume bundle and lagged Compustat fundamentals satisfy PIT,
timestamp, lag, and horizon requirements.

Scope:

- consume the original real OOS observations from the three price/style factors
- consume `soft_resurrected_component_pool.csv`
- read the local WRDS daily research-mode manifest
- read the local lagged Compustat fundamentals manifest when present
- generate observations only for components with supported builders:
  - `liquidity_turnover`
  - `value_bm`
  - `profitability_quality`
  - `investment_asset_growth`
  - `accruals`
- keep `sue_event_reference` unavailable until a PIT event visibility timestamp
  path is wired
- keep same-close trading, full-sample ICIR, OR, Q2, broker/order, live, and
  production-approval paths disabled
- write no raw WRDS data into git and fabricate no returns

Status:

- Complete.
- Output lives under
  `outputs/multifactor_alpha_validation/component_oos_observations/`.
- Artifacts:
  - `real_oos_observations.csv`
  - `component_oos_observation_expansion_summary.json`
  - `component_oos_observation_enablement_report.csv`
- Current real WRDS smoke:
  - generated factors: `accruals`, `investment_asset_growth`,
    `liquidity_turnover`, `profitability_quality`, `value_bm`
  - `observed_factor_count_after_expansion=8`
  - `unavailable_factor_ids_after_expansion=sue_event_reference`
  - `fabricated_returns=false`
  - `same_close_trading_used=false`
  - `full_sample_icir_used=false`
- R15 and R15.5 should be run after this target so portfolio diagnostics use the
  expanded observation panel.

### MF-R15.6: Component OOS Availability Expansion

Goal:
Classify why resurrected components are observed or unavailable before any
full-pool contribution, survival, or OR decision is attempted.

Scope:

- consume `soft_resurrected_component_pool.csv`
- consume current real OOS observations
- load formal FactorSpecs for data tier, horizon, timestamp policy, and
  reporting lag
- assign an unavailable reason from the fixed taxonomy:
  - `missing_signal_panel`
  - `missing_pit_source`
  - `missing_fundamental_lag`
  - `missing_event_timestamp`
  - `missing_oos_return_alignment`
  - `horizon_incompatible_with_monthly_ensemble`
  - `insufficient_coverage`
  - `disabled_by_policy`
  - `research_mode_blocked`
- require tier-2 fundamentals to retain reporting lag
- require event/reference components to retain visibility timestamps
- enforce `component_pool_validation_min_coverage=0.60`
- keep full-pool pass/fail decisions blocked below the coverage threshold
- write no fabricated returns
- keep OR optimizer, security-level construction, Q2, paper/live, broker/order,
  and production approval disabled

Status:

- Complete.
- Output lives under `outputs/multifactor_alpha_validation/portfolio_validation/`.
- Artifacts:
  - `component_oos_availability_report.csv`
  - `component_oos_availability_summary.json`
  - `component_enablement_plan.md`
- Current real WRDS smoke after component OOS observation expansion:
  - `eligible_component_count=9`
  - `observed_component_count=8`
  - `unavailable_component_count=1`
  - `coverage_ratio=0.8888888889`
  - `component_pool_validation_state=component_pool_observation_coverage_sufficient`
  - `full_pool_decision_allowed=true`
  - unavailable reasons: `missing_event_timestamp=1`
  - `fabricated_returns=false`
- R15.6 confirms that observation coverage is now sufficient for a current
  observed component-pool diagnostic, but not for OR. The current R15/R15.5
  rerun still fails gross and cost-adjusted OOS under the primary diagnostic
  construction. OR remains blocked.

### MF-R16: Post-Portfolio Contribution / Ablation

Goal:
Explain which observed components, clusters, roles, and regimes contributed to
the R15 portfolio-level failure before any optimizer or security-level
construction is considered.

Scope:

- consume `soft_resurrected_component_pool.csv`
- consume expanded component OOS observations
- consume the R15 validation summary for source decision context
- run leave-one-factor-out ablation
- run drop-cluster ablation
- run component-role contribution diagnostics
- split factor contribution by QQQ up/down regimes
- classify component contribution as one of:
  - `core_component`
  - `diversifier_component`
  - `hedge_component`
  - `regime_specific_component`
  - `redundant_after_portfolio`
  - `cost_negative_component`
  - `diagnostic_component`
- keep OR optimizer, security-level construction, Q2, paper/live, broker/order,
  and production approval disabled

Status:

- Complete.
- Output lives under
  `outputs/multifactor_alpha_validation/portfolio_contribution/`.
- Artifacts:
  - `factor_ablation_report.csv`
  - `cluster_ablation_report.csv`
  - `factor_role_contribution.csv`
  - `contribution_by_regime.csv`
  - `portfolio_contribution_summary.json`
  - `post_portfolio_contribution_report.md`
- Current real WRDS smoke:
  - `validation_status=evaluated`
  - `decision_state=portfolio_contribution_diagnostic_only`
  - `observed_component_count=8`
  - baseline gross annualized return `-0.0260405898`
  - baseline cost-adjusted return `-0.0376911544`
  - baseline Sharpe `-0.6257279985`
  - `or_optimizer_used=false`
  - `security_level_portfolio_construction_used=false`
- Current diagnosis:
  - `fundamental_premia_component` and `style_premia_return_driver` have
    positive role contribution under the diagnostic equal-weight construction.
  - `hedge_or_diversifier_component` is negative as a role.
  - `low_vol_60d` has positive QQQ-down contribution but is negative overall.
  - `liquidity_turnover` is a negative post-portfolio contribution.
- R16 is attribution only. It does not create alpha evidence, factor weights,
  security-level targets, OR entry, Q2 input, paper canary, live trading,
  security orders, or production approval.
