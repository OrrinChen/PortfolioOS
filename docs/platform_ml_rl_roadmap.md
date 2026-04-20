# PortfolioOS ML+RL Platform Roadmap

## Vision

PortfolioOS evolves from a compliance-aware rebalance and execution CLI into a learning-based alpha-to-execution research platform.

The target platform supports both US equities and China A-shares through a shared research core plus market-specific adapters.

Core design principle:

- build one platform, not two codebases
- prioritize US equities first for depth and faster research iteration
- preserve US/CN portability through explicit market adapters

## End State

The final platform has four cooperating decision layers:

1. ML alpha models predict cross-sectional expected returns.
2. ML risk and cost models forecast covariance, liquidity, and execution friction.
3. The optimizer converts alpha, risk, and cost into constrained portfolios.
4. RL controls sequential execution and dynamic budget decisions.

This yields an auditable net-alpha loop:

- forecast alpha
- build a portfolio
- execute the trades
- collect fills
- recalibrate cost and execution models
- evaluate net alpha after cost

## Architecture

### Shared Core

- feature store
- label generation
- alpha research
- risk and cost modeling
- optimizer and attribution
- simulator and TCA
- experiment registry and decision notes

### Market Adapters

- security master conventions
- market calendar
- lot-size rules
- tax and fee rules
- price-limit rules
- tradability filters
- settlement and replay assumptions
- broker and execution adapters

## Phase Roadmap

### Phase 1: US Alpha Core

Goal:

- add the first reusable alpha research layer on top of the existing backtest and TCA stack

Deliverables:

- point-in-time style returns research pipeline
- baseline alpha signals
- forward-return label generation
- IC and rank-IC evaluation
- quantile spread diagnostics
- markdown and CSV research artifacts

### Phase 2: US Risk And Cost Intelligence

Goal:

- move risk and cost from static parameters into forecast layers

Deliverables:

- covariance and regime forecasts
- ML slippage and impact models
- capacity diagnostics
- unified alpha, risk, and cost attribution

### Phase 3: US RL Execution

Goal:

- use RL for dynamic execution and scheduling rather than direct stock selection

Deliverables:

- execution simulator upgrades
- execution benchmark policies
- RL urgency and slicing policy
- offline policy evaluation
- paper/live feedback loop

### Phase 4: China A-Share Adapter

Goal:

- port the shared platform into China A-shares without forking the research stack

Deliverables:

- CN market adapter
- T+1, lot-size, price-limit, and fee logic
- CN data and execution baselines
- unified US/CN manifests and reports

### Phase 5: Governance And Meta-Control

Goal:

- make the platform production-candidate ready from a research-governance perspective

Deliverables:

- model registry
- promotion and rejection workflow
- champion-challenger tracking
- meta-control policies for turnover and risk budgets
- unified research closeout notes

## Why US First

US equities are the first priority because:

- the project already has strong US execution and TCA infrastructure
- US data and execution assumptions are easier to standardize
- US-first alpha research is the fastest path to a net-alpha proof point
- that proof point can then be ported to CN through the adapter layer

## Why RL Is Not The First Layer

RL is valuable, but not as a replacement for forecasting or portfolio construction.

PortfolioOS uses:

- ML for prediction
- optimization for allocation
- RL for sequential control

This keeps the system interpretable and closer to how institutional platforms are actually built.

## Near-Term Priority

The immediate next step is Phase 1:

- build the first alpha research baseline
- validate that it has out-of-sample signal quality
- only then connect alpha into the optimizer
