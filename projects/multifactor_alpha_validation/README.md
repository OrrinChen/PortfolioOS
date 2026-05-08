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
Real allocator/redundancy promotion remains locked because no current factor has clean residual evidence.
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

MF-R8 through MF-R13 are complete on the local WRDS daily PIT bundle as
diagnostic workflow checks. The strict residual closeout keeps redundancy and
allocator entry blocked because the current factor set has no clean residual
survivor under the configured proxy risk model.

The correct next step is not more factors, allocator tuning, ML models, or
return-display polish. The useful next phase is either an institutional
risk-model/data improvement pass or an explicit stop/closeout memo for this
candidate set.
