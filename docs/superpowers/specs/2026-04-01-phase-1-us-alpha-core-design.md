# Phase 1 US Alpha Core Design

## Objective

Phase 1 adds the first reusable alpha research layer to PortfolioOS.

The goal of this slice is not yet to redesign the optimizer. The goal is to establish a clean, testable baseline workflow that turns a `returns_long.csv` input into:

- baseline alpha signals
- forward-return labels
- IC diagnostics
- quantile spread diagnostics
- auditable research artifacts

This gives the project its first explicit answer to "what should we own?" before alpha is later connected to portfolio construction.

## Scope

This first implementation slice is intentionally narrow.

Included:

- a new `portfolio_os.alpha` package
- baseline signal generation from point-in-time historical returns
- forward-return label generation
- per-date IC and rank-IC evaluation
- top-minus-bottom quantile spread diagnostics
- a standalone CLI that writes research artifacts

Excluded for this slice:

- optimizer integration
- factor neutralization
- sector neutralization
- model training beyond deterministic baseline signals
- walk-forward orchestration across multiple sample manifests
- RL or execution changes

## Design

### Inputs

The initial alpha workflow consumes only:

- a `returns_long.csv` file with `date,ticker,return`

This keeps the slice portable across US and CN as long as the market adapter can supply normalized returns history later.

### Baseline Signals

The first two signals are simple and interpretable:

1. short-horizon reversal
2. medium-horizon momentum with a skip window

For each date:

- reversal is the negative cumulative return over a recent lookback window
- momentum is the cumulative return over an older window, shifted by a skip period

The system then converts each raw signal into a centered cross-sectional rank and blends them into a single `alpha_score`.

### Labels

Forward returns are computed from future realized returns over a configurable horizon. At date `t`, the label uses returns strictly after `t`, which preserves the no-lookahead boundary.

### Evaluation

For each date with enough valid names:

- Pearson IC is computed between `alpha_score` and forward return
- Spearman rank-IC is computed between `alpha_score` and forward return
- a top-minus-bottom quantile spread is computed from alpha-score buckets

The CLI writes:

- one long-form signal panel
- one per-date diagnostics CSV
- one JSON summary
- one markdown report

## File Design

### New Package

- `src/portfolio_os/alpha/research.py`
  - returns loading
  - signal generation
  - label generation
  - summary metrics
- `src/portfolio_os/alpha/report.py`
  - markdown rendering
- `src/portfolio_os/alpha/__init__.py`
  - public exports

### CLI

- `src/portfolio_os/api/cli.py`
  - add `alpha_research_app`
- `pyproject.toml`
  - add `portfolio-os-alpha-research`

### Tests

- `tests/test_alpha_research.py`
  - synthetic returns fixture
  - signal and label checks
  - evaluation checks
  - artifact-writing check
  - CLI check

## Success Criteria

Phase 1 is successful when:

- the project can generate baseline alpha diagnostics from `returns_long.csv`
- the workflow is deterministic and test-covered
- artifacts are easy to inspect and reuse
- the resulting code is market-agnostic enough to support both US and CN later

## Next Step After Phase 1

The next step is Phase 1.5:

- connect an `expected_return` layer into portfolio construction
- compare alpha-aware optimization against the existing target-driven baseline
