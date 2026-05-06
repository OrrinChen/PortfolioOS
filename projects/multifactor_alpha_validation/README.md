# Multi-Factor Alpha Validation Engine

A PIT-safe, redundancy-aware, cost-aware institutional factor research and
backtest system.

This standalone project lives under `projects/multifactor_alpha_validation/`.
It does not replace the root PortfolioOS roadmap, does not enter Q2 directly,
and does not claim production approval.

Current status:

```text
Infrastructure complete.
Research-grade alpha evidence is not unlocked.
The active blocker is real PIT dataset readiness.
The next phase is dataset onboarding, not more factor logic.
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
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run python projects/multifactor_alpha_validation/scripts/run_wrds_multifactor_ingest.py --config projects/multifactor_alpha_validation/configs/wrds_multifactor_query_template.yaml --require-ready
```

Before running the ingest command, edit the query template's NASDAQ100
membership SQL to point at the PIT constituent table available in your WRDS
subscription. Raw and standardized WRDS extracts are written under
`data/cache/wrds_multifactor/`, which is ignored by git.

The local historical-universe smoke writes PIT-style universe snapshots from a
synthetic fixture:

```bash
make multifactor-research-universe
make multifactor-research-panels
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

- `MF-R0` Dataset Manifest Contract
- `MF-R1` Historical Universe Membership Loader
- `MF-R2` Adjusted Price/Volume and QQQ Benchmark Panel
- `MF-R3` Delisting and Inactive Asset Handling
- `MF-R4` First Real Research Dry Run
- `MF-R5` Rolling OOS Factor Validation

Do not add factors, tune the allocator, add ML models, or polish return displays
before the dataset gate is ready.
