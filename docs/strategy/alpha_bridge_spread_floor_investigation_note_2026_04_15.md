# Alpha Bridge Spread Floor Investigation Note

Date: 2026-04-15  
Status: investigation complete, implementation unchanged

## Question

What is the current `spread floor` in the Phase 1.5 alpha bridge actually doing, and how should it be interpreted before any further optimizer or time-series validation work?

## Facts

### 1. The floor is explicit in the original Phase 1.5 design

The Phase 1.5 design spec defines the mapping:

- `annualized_spread = max(spread_5d, 0.0) * (252.0 / 5.0)`

and gives the rationale:

- weak signals are shrunk toward zero
- the bridge "does not reverse signal direction when historical evidence is negative"

Source:

- [2026-04-01-phase-1-5-alpha-integration-design.md](/C:/Users/14574/Quant/PortfolioOS/docs/superpowers/specs/2026-04-01-phase-1-5-alpha-integration-design.md)

### 2. The current implementation matches that design exactly

The alpha bridge still computes:

```python
annualized_spread = float(
    max(float(history["top_bottom_spread"].mean()), 0.0)
    * (_ANNUALIZATION_FACTOR / float(_ACCEPTED_RECIPE.forward_horizon_days))
)
```

Source:

- [backtest_bridge.py](/C:/Users/14574/Quant/PortfolioOS/src/portfolio_os/alpha/backtest_bridge.py)

### 3. Existing repo docs already describe the observed behavior

The Phase 1.5 decision note states that signal-ready dates can still produce zero effective `expected_return` because the trailing top-bottom spread estimate was clipped to zero.

Source:

- [phase_1_5_alpha_decision_note.md](/C:/Users/14574/Quant/PortfolioOS/docs/phase_1_5_alpha_decision_note.md)

### 4. The latest structural-ablation diagnosis shows the floor is currently binding

On the corrected walk-forward diagnosis:

- total rebalance observations: `11`
- alpha-ready months: `4`
- non-zero alpha months: `1`
- `3` ready months were shut off by `spread_floor_to_zero`

Ready-month breakdown:

- `2025-10-31`: nonzero alpha
- `2025-11-28`: floor to zero
- `2025-12-31`: floor to zero
- `2026-01-30`: floor to zero

Source:

- [alpha_ready_vs_nonzero_diagnosis.md](/C:/Users/14574/Quant/PortfolioOS/outputs/objective_units_v1_structural_ablation_2026-04-15/alpha_ready_vs_nonzero_diagnosis.md)

## Interpretation

The current `spread floor` should be read as a **one-sided non-reversal regime guard**, not as an accidental leftover and not merely as a trivial long-only clip.

Why:

1. The behavior was specified up front in the original design rather than appearing later as an implementation artifact.
2. The design rationale explicitly says the bridge should **not reverse** when trailing historical evidence is negative.
3. The floor is applied to the shared trailing spread scalar before cross-sectional projection, not to individual ticker signs after projection.

This matters because the bridge can still emit negative per-ticker `expected_return` values when the trailing spread is positive and a ticker sits in the weak tail of the cross-section. The floor is therefore not simply "long-only cannot use negative values." It is a higher-level decision that negative trailing evidence should disable the alpha package rather than invert it.

## Current Judgment

Before any full time-series confirmation run of `risk_term = 0.3`, the spread-floor semantics should be treated as the next structural decision point.

Current recommendation:

- do **not** change the implementation yet
- do **not** treat the floor as a bug
- do **not** rerun broad optimizer confirmation under a semantic assumption that may change

Instead, treat the next question as:

> Should the alpha bridge keep its current non-reversal regime-guard semantics, or should it be replaced by a different explicit policy?

Possible policies to compare later in offline research:

- keep current `floor_to_zero`
- carry signed trailing spread through the bridge
- abstain with an explicit null / missing alpha state rather than zero-valued `expected_return`

## Not Decided Here

This note does **not** decide whether the current floor is economically optimal.

It only freezes the factual read:

- the floor is intentional
- the current implementation matches the original design
- it is materially binding in the recent sample
- any further optimizer validation should acknowledge that this alpha-bridge semantic choice comes before more risk-parameter confirmation
