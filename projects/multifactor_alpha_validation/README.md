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
Real allocator/redundancy promotion remains locked by style_proxy_only and benchmark_beta_style_conflict closeout.
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

The next roadmap is Real PIT Dataset Onboarding:

- `MF-R6` External PIT Dataset Source Adapter
- `MF-R7` Real Dataset Dry Run, No Factor Claims
- `MF-R8` First Real Rolling OOS Evidence
- `MF-R9` Real Evidence Closeout Gate

MF-R8 and MF-R9 are complete on the local WRDS daily PIT bundle as diagnostic
workflow checks. The closeout decision is `diagnostic_only` with
`style_proxy_only` and `benchmark_beta_style_conflict`: sector attribution is
observed, and style attribution uses WRDS size/liquidity/volatility proxies, but
this is still not a full institutional risk model. Momentum's positive
style-adjusted proxy residual conflicts with negative QQQ-relative and
beta-adjusted readouts, so the correct next step is attribution/data model
strengthening, not more factors, allocator tuning, ML models, or return-display
polish.
