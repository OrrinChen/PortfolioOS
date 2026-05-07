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
Current blocker: style_proxy_only plus benchmark_beta_style_conflict before redundancy or allocator entry
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
