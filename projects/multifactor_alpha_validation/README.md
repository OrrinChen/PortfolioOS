# Multi-Factor Alpha Validation Engine

A PIT-safe, redundancy-aware, cost-aware institutional factor research and
backtest system.

This standalone project lives under `projects/multifactor_alpha_validation/`.
It does not replace the root PortfolioOS roadmap, does not enter Q2 directly,
and does not claim production approval.

Current status:

```text
Research workflow shape complete.
Dataset onboarding gate complete.
Synthetic PIT-ready path complete.
WRDS monthly PIT dry run complete.
WRDS daily PIT bundle pulled.
MF-R8 first real rolling OOS evidence complete as diagnostic evidence.
MF-R9 closeout decision: diagnostic_only.
Sector attribution and size/liquidity/volatility style proxy attribution are wired.
Strict benchmark/beta/style conflict closeout is wired.
MF-R10 PIT exposure store complete for risk attribution input only.
MF-R11 cross-sectional risk model attribution complete as ex-post attribution only.
MF-R12 factor attribution waterfall complete as diagnostic attribution only.
MF-R13 strict residual evidence closeout complete.
MF-R14 portfolio component gate complete.
MF-R14.5 candidate filter audit and soft-failure resurrection complete.
MF-R15 portfolio-level OOS ensemble validation complete as diagnostic validation only.
MF-R15.5 portfolio assembly audit complete as component-pool assembly diagnosis.
MF-R15.6 component OOS availability expansion complete.
Component OOS observation expansion complete for WRDS daily price-volume and
lagged Compustat components.
MF-R16 post-portfolio contribution / ablation diagnosis complete.
Current next step: decide whether to stop, revise component construction, or run
a bounded diagnostic R17 cost/capacity attribution. OR remains locked.
```

## Scope

The engine converts a small MVP factor library into:

- FactorSpec contracts
- PIT timestamp validation
- deterministic local signal panels
- AlphaView-compatible predictive views
- raw and neutralized Q1-style evidence
- redundancy and marginal-value decisions
- posterior shrinkage and covariance diagnostics
- factor-level nonnegative allocation
- zero-weight attribution
- cost, capacity, and benchmark survival
- final registry, report, dashboard, and manifest
- risk-attributed component classification before portfolio-level validation

## Commands

```bash
make factor-spec-validate
make factor-signals
make factor-q1
make factor-redundancy
make factor-shrinkage
make factor-allocator
make factor-survival
make factor-registry
make factor-report
make factor-dashboard
make factor-validate
```

WRDS option B uses your local WRDS configuration and never stores credentials in
the repo:

```bash
make multifactor-wrds-config-check
make multifactor-external-source-check
WRDS_USERNAME=<your_wrds_username> PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run python projects/multifactor_alpha_validation/scripts/run_wrds_multifactor_ingest.py --config projects/multifactor_alpha_validation/configs/wrds_nasdaq100_research_mode.yaml --require-ready
make multifactor-real-dataset-dry-run
```

The source check writes `dataset_source_manifest.yaml`,
`source_field_mapping.yaml`, `dataset_ingest_validation.json`, and
`dataset_readiness.md` without opening a WRDS connection. The ingest command
uses the committed Nasdaq100 WRDS config only after credentials are configured
outside the repo. Raw and standardized WRDS extracts are written under
`data/cache/wrds_multifactor/`, which is ignored by git and must not be
committed.

`make multifactor-real-dataset-dry-run` reads the local WRDS monthly PIT bundle
manifest under `data/cache/wrds_multifactor/nasdaq100/standardized/` and writes
MF-R7 readiness artifacts under
`outputs/multifactor_alpha_validation/wrds_real_dataset_dry_run/`. It checks
coverage, timestamp alignment, universe snapshots, signal availability,
benchmark alignment, and delisting coverage only. It does not rank factors,
run allocator weights, claim strategy returns, claim alpha success, approve
production, or enter Q2.

Daily price-volume validation is explicitly separated in
`configs/wrds_nasdaq100_daily_price_volume_long_task.yaml`. The committed
daily WRDS research-mode config can now pull the local daily PIT bundle without
storing credentials in the repo:

```bash
WRDS_USERNAME=<your_wrds_username> PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run python projects/multifactor_alpha_validation/scripts/run_wrds_multifactor_ingest.py --config projects/multifactor_alpha_validation/configs/wrds_nasdaq100_daily_research_mode.yaml --require-ready
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run python projects/multifactor_alpha_validation/scripts/run_real_rolling_oos_evidence.py --manifest data/cache/wrds_multifactor/nasdaq100_daily_size/standardized/research_mode_dataset_manifest.yaml --output-dir outputs/multifactor_alpha_validation/wrds_real_oos_evidence_size
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run python projects/multifactor_alpha_validation/scripts/run_real_evidence_closeout.py --real-oos-output-dir outputs/multifactor_alpha_validation/wrds_real_oos_evidence_size --output-dir outputs/multifactor_alpha_validation/wrds_real_evidence_closeout_size
```

The current WRDS daily dry evidence path evaluates only `momentum_12_1`,
`reversal_5_1`, and `low_vol_60d`; it writes diagnostic OOS evidence, an
exposure panel, sector-adjusted readouts, price-volume style-proxy-adjusted
readouts, and a closeout gate. It does not run allocator weights, redundancy
promotion, paper canaries, Q2, strategy deployment, or alpha success claims.
The refreshed daily WRDS cache under
`data/cache/wrds_multifactor/nasdaq100_daily_size/` contains non-null `dlycap`,
`shrout`, and `dlyprcvol`, so R8 now uses size, liquidity, and volatility
style proxies.

The R9 closeout also writes `real_evidence_conflict_diagnostics.csv`. The
current real-size run flags `momentum_12_1` because its QQQ-relative and
beta-adjusted spreads are negative while its style-adjusted proxy net spread is
positive. The positive style-proxy residual is diagnostic only; it does not
override benchmark/beta failure or unlock redundancy/allocator entry.

MF-R10 builds a PIT exposure store for the next risk-attribution layer:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run python projects/multifactor_alpha_validation/scripts/run_pit_exposure_store.py --research-manifest data/cache/wrds_multifactor/nasdaq100_daily_size/standardized/research_mode_dataset_manifest.yaml --fundamentals-manifest data/cache/wrds_multifactor/nasdaq100_fundamentals/standardized/fundamentals_manifest.yaml --output-dir outputs/multifactor_alpha_validation/risk_model
```

The current R10 smoke writes `exposure_panel.csv`,
`exposure_coverage_report.json`, and `exposure_manifest.yaml` under
`outputs/multifactor_alpha_validation/risk_model/`. It covers sector, industry,
trailing market beta, log market cap, liquidity ADV, residual volatility,
short-term reversal, medium-term momentum, book-to-market, profitability, and
asset growth exposures. These rows are `risk_attribution_input_only`; they are
not factor signals, alpha evidence, allocator input, Q2 input, paper canary, or
production approval.

MF-R11 uses the R10 exposure store for ex-post cross-sectional risk attribution:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run python projects/multifactor_alpha_validation/scripts/run_cross_sectional_risk_model.py --research-manifest data/cache/wrds_multifactor/nasdaq100_daily_size/standardized/research_mode_dataset_manifest.yaml --exposure-panel outputs/multifactor_alpha_validation/risk_model/exposure_panel.csv --output-dir outputs/multifactor_alpha_validation/risk_model
```

The current R11 smoke writes `risk_model_returns_by_period.csv`,
`risk_model_exposure_coefficients.csv`, `risk_model_residual_returns.csv`, and
`risk_model_fit_diagnostics.json`. It decomposes realized next-period returns
into intercept, market beta, industry, configured style proxy, fitted, and
residual components. The residual is an attribution artifact only; it is not a
tradeable prediction, factor signal, alpha claim, allocator input, Q2 input, or
production approval.

MF-R12 turns the R11 per-asset residual components into factor-level waterfalls:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run python projects/multifactor_alpha_validation/scripts/run_factor_attribution_waterfall.py --research-manifest data/cache/wrds_multifactor/nasdaq100_daily_size/standardized/research_mode_dataset_manifest.yaml --residual-returns outputs/multifactor_alpha_validation/risk_model/risk_model_residual_returns.csv --output-dir outputs/multifactor_alpha_validation/risk_model
```

The current R12 smoke writes `factor_attribution_waterfall.csv`,
`factor_attribution_waterfall_by_period.csv`,
`factor_attribution_waterfall_{factor_id}.json`,
`factor_attribution_diagnostics.json`, and `factor_attribution_report.md`. It
reports gross, QQQ-relative, beta-adjusted, industry-adjusted,
style-proxy-adjusted, and full-residual spread readouts. All three current MVP
price factors are marked `style_proxy_conflict`, so they remain diagnostic-only
until MF-R13 strict residual closeout.

MF-R13 turns the R12 waterfall into strict stop-layer decisions:

```bash
make multifactor-strict-residual-closeout
```

The current R13 smoke writes `strict_residual_closeout_decision_table.csv`,
`strict_residual_closeout_diagnostics.json`,
`factor_registry_risk_model_update.yaml`, and
`strict_residual_closeout_report.md`. Current decisions are:

- `momentum_12_1`: `insufficient_residual_evidence`
- `reversal_5_1`: `style_proxy_conflict`
- `low_vol_60d`: `style_proxy_conflict`

`ready_for_redundancy_count=0`. Positive configured proxy residuals remain
diagnostic only and do not override negative benchmark/beta or unstable
full-residual evidence.

The fixed failure diagnosis report can be rebuilt with:

```bash
make multifactor-failure-diagnosis-report
```

It writes `factor_failure_diagnosis.csv`,
`qqq_relative_guard_review.json`, and
`factor_failure_diagnosis_report.md`. The current diagnosis says the
QQQ-relative guard is over-strict as a hard gate for long-short factor spreads,
but softening it rescues `0` current factors because the remaining blockers are
residual instability or beta/exposure conflict.

MF-R14 converts strict closeout rows into portfolio component roles:

```bash
make multifactor-portfolio-component-gate
```

This writes `component_candidate_table.csv`,
`portfolio_component_gate_summary.json`, and
`portfolio_component_gate_report.md`. The current component gate returns:

- `standalone_clean_alpha_count=0`
- `component_candidate_count=3`
- `portfolio_validation_mode=diagnostic_ensemble_only`

Current roles are `eligible_benchmark_premia_component` for `momentum_12_1`
and `reversal_5_1`, and `eligible_hedge_component` for `low_vol_60d`. This
allows only portfolio-level diagnostic ensemble validation. It is not an alpha
claim, unrestricted allocator entry, Q2 input, or production approval.

MF-R14.5 audits the full formal FactorSpec candidate set before R15:

```bash
make multifactor-candidate-filter-audit
```

This writes `candidate_filter_audit.csv`, `hard_excluded_candidates.csv`,
`soft_resurrected_component_pool.csv`, `component_pool_manifest.json`, and
`filter_audit_report.md`. The current audit checks all `10` formal specs:

- `component_pool_count=9`
- `hard_excluded_count=1`
- hard excluded: `analyst_revision_disabled`

The soft-resurrected component pool includes the three R14 component rows plus
enabled formal specs that have not yet been risk-attributed. `sue_event_reference`
is carried as a reference component. The audit deliberately does not import
Factor Discovery teaching/proxy candidates into formal research mode.

MF-R15 evaluates the component pool at the portfolio ensemble layer:

```bash
make multifactor-portfolio-validation
```

This writes `portfolio_ensemble_oos_report.csv`, `ensemble_vs_baselines.csv`,
`ensemble_validation_summary.json`, `random_weight_placebo_report.csv`,
`permuted_signal_placebo_report.csv`, and `portfolio_validation_report.md`
under `outputs/multifactor_alpha_validation/portfolio_validation/`.

The current R15 smoke uses the 9-row resurrected component pool after component
OOS observation expansion. Eight components have OOS observations:
`momentum_12_1`, `reversal_5_1`, `low_vol_60d`, `liquidity_turnover`,
`value_bm`, `profitability_quality`, `investment_asset_growth`, and
`accruals`. `sue_event_reference` remains unavailable rather than receiving
fabricated event returns. The R15 decision is still
`portfolio_component_pool_fails_cost`: the primary equal-weight component
ensemble has negative gross, negative cost-adjusted annualized return, and
negative QQQ-relative return.

MF-R15.5 audits the portfolio assembly before post-portfolio ablation:

```bash
make multifactor-portfolio-assembly-audit
```

This writes `portfolio_assembly_audit.json`,
`observed_subset_coverage_report.csv`, `component_direction_audit.csv`,
`gross_to_net_waterfall.csv`, `role_aware_ensemble_report.csv`, and
`decision_state_reclassification.md` under
`outputs/multifactor_alpha_validation/portfolio_validation/`.

The current audit says:

- `reclassified_decision_state=component_pool_fails_gross`
- `component_pool_validation_state=component_pool_observation_sufficient`
- `observed_component_count=8`
- `unavailable_component_count=1`
- `coverage_ratio=0.8888888889`
- `benchmark_exposure_conflict=true`

This means the current observed component pool fails gross and cost-adjusted
OOS under the primary construction. The SUE event reference remains explicit
unavailable because the event visibility timestamp path is not wired. OR remains
blocked.

MF-R15.6 component OOS observation expansion can be rebuilt with:

```bash
make multifactor-component-oos-observations
```

This writes `real_oos_observations.csv`,
`component_oos_observation_expansion_summary.json`, and
`component_oos_observation_enablement_report.csv` under
`outputs/multifactor_alpha_validation/component_oos_observations/`. The current
run generates observations for `liquidity_turnover`, `value_bm`,
`profitability_quality`, `investment_asset_growth`, and `accruals` from local
WRDS daily price-volume and lagged Compustat fundamentals. It does not commit
raw WRDS data, fabricate returns, claim alpha, unlock OR, or enter Q2.

MF-R15.6 expands component observability before any full-pool judgment:

```bash
make multifactor-component-oos-availability
```

This writes `component_oos_availability_report.csv`,
`component_oos_availability_summary.json`, and
`component_enablement_plan.md` under
`outputs/multifactor_alpha_validation/portfolio_validation/`.

The current availability expansion says:

- `eligible_component_count=9`
- `observed_component_count=8`
- `unavailable_component_count=1`
- `coverage_ratio=0.8888888889`
- `component_pool_validation_state=component_pool_observation_coverage_sufficient`
- `full_pool_decision_allowed=true`
- unavailable reasons: `missing_event_timestamp=1`

The remaining unavailable component is `sue_event_reference`; it still requires
a formal event visibility timestamp path. No returns are fabricated.

MF-R16 diagnoses contribution after the portfolio-level failure:

```bash
make multifactor-portfolio-contribution
```

This writes `factor_ablation_report.csv`, `cluster_ablation_report.csv`,
`factor_role_contribution.csv`, `contribution_by_regime.csv`,
`portfolio_contribution_summary.json`, and
`post_portfolio_contribution_report.md` under
`outputs/multifactor_alpha_validation/portfolio_contribution/`.

The current run is diagnostic-only:

- `decision_state=portfolio_contribution_diagnostic_only`
- `observed_component_count=8`
- baseline gross annualized return `-0.0260405898`
- baseline cost-adjusted return `-0.0376911544`
- baseline Sharpe `-0.6257279985`
- positive role contribution: `fundamental_premia_component`,
  `style_premia_return_driver`
- negative role contribution: `hedge_or_diversifier_component`
- `low_vol_60d` has QQQ-down hedge contribution but is negative overall
- `liquidity_turnover` is a negative post-portfolio contribution

R16 does not turn these observations into factor weights or security-level
portfolio decisions. It does not unlock OR, Q2, paper/live, broker/order, or
production approval.

The local historical-universe smoke writes PIT-style universe snapshots from a
synthetic fixture:

```bash
make multifactor-research-universe
make multifactor-research-panels
make multifactor-research-delistings
make multifactor-first-research-dry-run
make multifactor-rolling-oos-validation
```

## Boundaries

- Missing coverage is explicit abstain.
- `no_view != zero_alpha`.
- Analyst revision remains disabled without a PIT estimate source.
- Outputs do not enter Q2 directly.
- No production approval, live trading, security-level output, or direct Q2
  entry is produced.
- Current-constituent/yfinance-style proxy data remains teaching/proxy only and
  is not accepted as formal alpha evidence.

## Next Phase

MF-R8 through MF-R15.6 are complete on the local WRDS daily PIT bundle as
diagnostic workflow checks. The strict residual closeout finds no standalone
clean residual alpha, while the component gate and filter audit preserve soft
failures as documented portfolio components. Component OOS observation expansion
raises observed coverage to 8 of 9; the remaining SUE reference is unavailable
because event visibility timestamps are not wired. R15/R15.5 now diagnose the
current observed component pool, and the primary construction still fails gross
and cost-adjusted OOS. MF-R16 records which observed factors, clusters, roles,
and regimes contributed to that failure. OR optimization remains locked.
