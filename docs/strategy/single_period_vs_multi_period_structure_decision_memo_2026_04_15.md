# Single-Period Vs Multi-Period Structure Decision Memo

Date: 2026-04-15

Status: accepted

## Decision

Do **not** start a full multi-period portfolio-construction implementation now.

Do freeze a project-level structural read:

- the current PortfolioOS bottleneck is real
- but it is **not** cleanly explained by "single-period is mathematically impossible"
- it is a combination of:
  - weak or absent platform-validated alpha input
  - objective configurations that often demote alpha to zero or near-zero influence
  - an execution-side multi-period capability that has not yet been connected to portfolio construction

Current project decision:

- keep the live platform on the existing single-period optimizer path
- treat full multi-period portfolio construction as a **conditional Phase 2/3 reopen trigger**
- do not spend the next 2-4 weeks building a rough multi-period allocator without a passing alpha package

## Why This Memo Exists

The last two weeks were spent mostly on platform infrastructure:

- `paper-calibration`
- `promotion-registry`

Those were good investments, but they do not themselves resolve the main portfolio-construction question:

> is PortfolioOS still fundamentally a constrained max-return / target-tracking engine, or is it ready for a real intertemporal alpha-risk-cost tradeoff?

That question is now important enough to answer explicitly before more alpha or execution work changes direction again.

## Evidence Base

### 1. The current optimizer is single-period by construction

The portfolio optimizer still solves one rebalance at a time:

- objective assembly:
  - `src/portfolio_os/optimizer/objective.py`
- single-period solve path:
  - `src/portfolio_os/optimizer/solver.py`
- orchestration entry:
  - `src/portfolio_os/workflow/single_run.py`

The optimizer chooses one post-trade portfolio from one current state.

It does not optimize across future rebalance states, future costs, or future inventory path dependence.

### 2. PortfolioOS already has a "multi-period" component, but only in execution

There is already a multi-period-style planning surface in:

- `src/portfolio_os/optimizer/multi_period.py`

But this module allocates **execution quantity across intraday liquidity buckets**.

It does not choose:

- today's portfolio vs tomorrow's portfolio
- current turnover vs future turnover
- current risk vs future risk

So the codebase already uses "multi-period" in execution semantics, while portfolio construction remains single-period.

### 3. Current risk integration is structurally weak for more than one reason

The evidence in code and outputs points to three separate issues.

#### A. Many portfolio configs still do not express a real alpha-risk tradeoff

Example:

- `config/us_expanded_tca_calibrated.yaml`

This config uses:

- `risk_model.enabled: true`
- `integration_mode: augment`
- `target_deviation: 100000.0`

but leaves:

- `alpha_weight = 0`

In that shape, the optimizer is not doing mean-variance allocation around a forecast alpha.

It is mainly minimizing:

- target deviation
- fees / slippage / turnover
- risk regularization

That is a materially different problem.

#### B. The target-deviation anchor is large enough to dominate augment-mode behavior

In `augment` mode the objective adds risk and cost terms on top of the legacy target-tracking stack.

With:

- `target_deviation = 100000.0`

the optimizer is heavily incentivized to stay close to the target weights.

So even when risk is "on", risk behaves mostly like a regularizer, not like the main allocator.

#### C. Replace-mode sweeps show that risk-only does not rescue the problem

The strongest direct evidence comes from:

- `outputs/risk_sweep_us_expanded_tca_calibrated_replace/risk_aversion_frontier_report.md`
- `outputs/risk_sweep_us_expanded_tca_calibrated_replace/risk_sweep_summary.csv`

Observed result:

- risk-aversion multipliers from `1x` through `10000x` produced the same portfolio outcome
- turnover stayed at `0.00`
- the frontier stayed flat
- only `100000x` changed behavior, and it worsened ending NAV further

This means the problem is not just:

- "augment mode hid the risk term"

It also means:

- once the anchor is removed, current risk-only replace mode still does not create a useful frontier on this sample

So the binding issue is deeper than one config switch.

### 4. The platform roadmap still assumes alpha comes first

The canonical roadmap remains:

- `docs/platform_ml_rl_roadmap.md`

Its current order is:

1. alpha core
2. risk and cost intelligence
3. execution baseline / RL later

That sequencing is still directionally correct.

The structural correction is not "skip alpha and go straight to multi-period."

The correction is:

- admit that the current single-period objective is not yet a trustworthy consumer of future alpha packages
- define the exact trigger for when a multi-period redesign becomes worth its engineering cost

## Root-Cause Read

The right answer is **not** one clean cause.

### What is true

- single-period portfolio construction is currently the active architecture
- risk term often does not materially change portfolio selection
- realistic cost/risk sweeps have not produced a better frontier
- execution simulation is already richer than portfolio construction

### What is not yet justified

- "single-period is the only problem"
- "a multi-period optimizer would automatically fix the platform"

Current read:

1. **Alpha bottleneck is still primary.**
   Without a reusable alpha package, multi-period optimization has little real economic signal to preserve.

2. **Objective shape is a secondary bottleneck.**
   The current objective often either:
   - gives alpha zero weight
   - or subordinates it to target tracking

3. **Intertemporal tradeoff is a real missing capability.**
   Once a reusable alpha package exists, single-period optimization will remain myopic about:
   - trading now vs later
   - paying impact now vs accepting temporary target deviation
   - choosing risk today vs preserving flexibility for the next rebalance

So the honest project read is:

> multi-period is probably the right eventual direction, but not the right immediate build.

## Minimal Meaningful Multi-Period Reopen

If PortfolioOS reopens this branch later, the minimum meaningful version should **not** be full RL or a large solver rewrite.

It should be a narrow **two-period lookahead portfolio-construction prototype**:

- period 0:
  - choose today's trades
- period 1:
  - represent one next rebalance state with a simple carry / rebalance approximation
- objective:
  - alpha reward
  - current transaction cost
  - next-step transaction cost proxy
  - current and next-step variance / tracking error
- constraints:
  - reuse existing hard constraints as much as possible
- solver:
  - stay in `cvxpy` / `CLARABEL` if convexity is preserved

That is the smallest version that would actually test the structural hypothesis.

Anything smaller is just another single-period regularization tweak.

Anything larger turns into a platform rewrite before the alpha layer has earned it.

## Career-Narrative Read

For an interview or portfolio piece, the strongest current story is still:

- disciplined research governance
- promotion contracts and review surfaces
- execution calibration with explicit audit trails
- honest closure of negative branches

A rough multi-period optimizer built too early would weaken that story more than it would help it.

The better narrative is:

> I identified the exact structural limit of the current optimizer, proved why naive risk sweeps were insufficient, and froze a scoped reopen trigger instead of overbuilding.

That is a stronger research-engineering story than:

> I added a multi-period optimizer, but the alpha side was not ready so the result stayed inconclusive.

## Go / No-Go Outcome

### No-Go Now

Do not start immediate implementation of:

- full multi-period portfolio construction
- solver migration
- RL-driven portfolio allocation
- optimizer redesign detached from a passing alpha package

### Go Later, But Only If Triggered

Reopen this branch only if at least one of the following becomes true:

1. A reusable alpha package passes its platform gate and still looks materially sensitive to turnover / hold-tradeoff.
2. Cost-retention analysis shows that a meaningful share of gross alpha is lost specifically because of myopic single-period rebalancing.
3. A paper-calibration or execution-validation result reveals a stable execution tradeoff that cannot be consumed by the current single-period allocator.

## Immediate Next Step

The immediate deliverable was this memo itself.

Practical next-step implication:

- keep `paper-calibration` as the active time-sensitive lane
- keep promotion-registry as governance infrastructure
- keep multi-period portfolio construction closed until one of the reopen triggers above is met

This freezes the structural decision without pretending the platform is already ready for the next architectural jump.
