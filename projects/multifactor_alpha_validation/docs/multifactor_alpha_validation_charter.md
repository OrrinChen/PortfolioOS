# Multi-Factor Alpha Validation Engine Charter

This project is a standalone multi-factor research validation extension. It is
not the root PortfolioOS release track and does not create an automatic next
root phase.

The engine starts from contracts rather than formulas. Each candidate factor
must declare a PIT-safe timestamp policy, a horizon, coverage semantics, known
failure modes, and explicit non-claims before any signal or backtest layer can
consume it.

Boundaries:

- Outputs do not enter Q2 directly.
- Any candidate that should leave this project must pass the root Phase 64
  research import contract.
- Missing coverage is explicit abstain.
- `no_view != zero_alpha`.
- Teaching-mode sandbox artifacts remain survivorship-biased, educational-only,
  and not alpha evidence.
- The project does not provide production approval, live trading, or
  security-order artifacts.

Week 1 scope is complete when at least eight enabled/reference FactorSpecs
validate with timestamp rules, fundamental reporting lags, disabled analyst
revision without PIT source, and explicit abstain coverage.

Formal research mode also requires a dataset preflight. Current-constituent or
yfinance-style local proxy files must be blocked until a PIT historical universe
membership table, adjusted price-volume panel, QQQ benchmark panel, explicit
delisting handling, and no-same-close timestamp policy are supplied.
