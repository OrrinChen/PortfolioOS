# Objective Function Units Spec

Date: 2026-04-15

Status: draft for implementation

## Purpose

This spec defines the minimum coherent single-period objective for PortfolioOS before any further optimizer tuning or alpha-integration work.

It exists because the current objective mixes:

- annualized alpha-like expected return
- one-shot transaction cost
- variance-style risk terms
- and a large `target_deviation` anchor

without one shared decision horizon or one clear economic interpretation.

This spec is intentionally narrow.

It does not define:

- multi-period portfolio construction
- RL execution
- final production coefficient values

It only defines the first objective that is economically interpretable enough to debug.

## Decision Horizon

The decision horizon for this objective is:

- one rebalance period

For the current monthly backtest path, that means:

- horizon = one monthly rebalance interval

All objective terms must be expressed on that same horizon.

That implies:

- alpha must be period expected return, not annualized return
- transaction cost must be one-shot cost for this rebalance
- risk and tracking terms must be period variance terms on the same horizon

## Core Objective

The v1 single-period objective is:

maximize expected period portfolio return
minus estimated one-period transaction cost
minus risk aversion times period variance
minus active-risk aversion times period tracking-error variance

One convenient form is:

```text
maximize over post-trade weights w and trades delta_w:

J(w, delta_w)
  = mu_h^T w
  - c_h(delta_w)
  - lambda_risk * w^T Sigma_h w
  - lambda_active * (w - w_b)^T Sigma_h (w - w_b)
```

Where:

- `mu_h`
  - forecast expected excess return over the next rebalance horizon
  - unit = fraction of NAV over one horizon
- `c_h(delta_w)`
  - estimated one-shot fee + slippage from executing `delta_w` now
  - unit = fraction of NAV
- `Sigma_h`
  - covariance matrix over the same horizon
  - unit = return variance over one horizon
- `w`
  - post-trade portfolio weights
- `w_b`
  - optional benchmark or policy weights when active-risk control is desired
- `lambda_risk`
  - converts total variance into expected-return units
- `lambda_active`
  - converts active variance into expected-return units

## Unit Rules

### 1. Alpha

Alpha must enter the optimizer as:

- expected period return
- not annualized return
- not rank
- not z-score

If the alpha model currently produces annualized return, it must be de-annualized before entering the objective.

For a horizon of `h` trading days:

```text
mu_h ~= mu_ann * h / 252
```

for small returns, or more exactly:

```text
mu_h = (1 + mu_ann)^(h / 252) - 1
```

If the alpha model currently produces a score rather than a return forecast, a separate calibration step is required:

- score -> expected period return

That mapping must happen before optimization, not inside a free-floating `alpha_weight`.

### 2. Transaction Cost

Transaction cost must enter as:

- estimated one-shot cost for the current rebalance
- divided by pre-trade NAV

So the cost term is:

```text
c_h(delta_w) = estimated_cost_currency(delta_w) / NAV
```

This means the current `raw_currency` objective mode is not acceptable for the main objective path.

The correct economic mode is:

- `nav_fraction`

### 3. Risk

Risk terms remain variance-like quantities, but their semantics must be explicit.

For a covariance estimated on annualized returns, convert to horizon covariance first:

```text
Sigma_h ~= Sigma_ann * h / 252
```

Then:

- `w^T Sigma_h w`
  - period portfolio variance
- `(w - w_b)^T Sigma_h (w - w_b)`
  - period active variance

These are not the same physical unit as return, but the aversion coefficients:

- `lambda_risk`
- `lambda_active`

must be interpreted as:

- expected-return penalty per unit of period variance

In other words, they are the only allowed bridge between risk variance and return space.

## What Happens To `target_deviation`

`target_deviation` does not belong in the economic core objective.

It currently overloads multiple roles:

- benchmark anchor
- cash deployment anchor
- pseudo-risk regularizer

Those roles must be separated.

### Role A: Benchmark Anchor

If the target weights represent a real policy benchmark, then benchmark control should come from:

- active-risk term relative to `w_b`

not from raw squared distance to target weights.

In that case:

- keep `tracking_error`
- remove `target_deviation`

### Role B: Cash Or Deployment Anchor

If the issue is that the strategy otherwise holds too much cash, then that should be handled as:

- a hard investment constraint
- or an explicit target-cash band

Examples:

- minimum invested weight
- maximum cash weight
- target cash range around a policy cash buffer

This is a constraint design problem, not an objective-mixing problem.

### Role C: Pseudo-Risk Regularizer

If `target_deviation` was only stabilizing the optimizer, that role should be replaced by:

- proper total-risk aversion
- active-risk aversion
- or well-defined constraints

not by an unscaled penalty with unclear economics.

## Current v1 Recommendation

For the next implementation pass:

1. remove `target_deviation` from the economic objective
2. keep `tracking_error` only if a benchmark `w_b` is intentional
3. handle cash deployment outside the objective via constraints
4. run diagnostics on the continuous solution before touching the repair layer

## Worked Decision Test

The spec is only useful if one can answer a simple decision question by arithmetic.

Example:

- one stock has forecast next-period excess return = `+50 bps`
- buying it adds estimated one-shot transaction cost = `3 bps`
- incremental risk penalty = `12 bps`
- incremental active-risk penalty = `5 bps`

Then the optimizer sees:

```text
net incremental objective contribution
  = +50 bps
  -  3 bps
  - 12 bps
  -  5 bps
  = +30 bps
```

That trade is economically attractive before constraints.

If instead:

- forecast return = `+10 bps`
- cost = `3 bps`
- risk penalty = `12 bps`
- active-risk penalty = `5 bps`

then:

```text
net = -10 bps
```

and the trade should not be taken.

If the objective cannot be interpreted this way, the units are still wrong.

## Diagnostic Implications

After implementing this spec, the next ablation should be:

1. inspect continuous solver output before repair
2. verify whether trade sizes are now economically meaningful
3. only then ask whether:
   - constraints are too tight
   - cash deployment is under-specified
   - or repair truncation is still the main blocker

## Non-Goals

This spec does not yet decide:

- the final value of `lambda_risk`
- the final value of `lambda_active`
- whether the benchmark should be equal-weight, target-weight, or none
- the eventual multi-period architecture

Those come after the units and roles are corrected.

## Implementation Trigger

The next code change should be considered valid only if it satisfies all of the following:

1. alpha is passed to the optimizer as period expected return
2. transaction cost is consumed as NAV fraction, not raw currency
3. covariance inputs are on the same decision horizon
4. `target_deviation` is removed from the core objective
5. cash deployment, if needed, is handled by explicit constraints rather than by a hidden objective anchor
