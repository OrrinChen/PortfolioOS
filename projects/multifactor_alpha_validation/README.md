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
Real research evidence is still locked.
The active blocker is external PIT dataset wiring.
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
```

The source check writes `dataset_source_manifest.yaml`,
`source_field_mapping.yaml`, `dataset_ingest_validation.json`, and
`dataset_readiness.md` without opening a WRDS connection. The ingest command
uses the committed Nasdaq100 WRDS config only after credentials are configured
outside the repo. Raw and standardized WRDS extracts are written under
`data/cache/wrds_multifactor/`, which is ignored by git and must not be
committed.

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

Do not add factors, tune the allocator, add ML models, or polish return displays
before the dataset gate is ready.
