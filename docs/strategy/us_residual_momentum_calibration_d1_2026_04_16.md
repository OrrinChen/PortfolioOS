# US Residual Momentum Calibration Family D1 (2026-04-16)

## Role

This is the calibration family, not the expected winner family.

Its purpose is to validate the discovery machine defined in `alpha_discovery_charter_v2_2026_04_16.md`.
The calibration family is successful if it tells us whether the discovery loop can:

- distinguish signal from artifact,
- distinguish mechanism-bearing expressions from cosmetic variants,
- and produce interpretable closeout logic under adversarial attack.

It is not successful merely because one residual momentum expression looks good.

## Family Boundary

The family under calibration is:

- `US residual momentum / residual reversal`

This includes expressions derived from:

- market residual returns,
- sector residual returns,
- beta-adjusted returns,
- and volatility-managed variants,

but only when those expressions can be mapped to a specific mechanism hypothesis.

This calibration family does **not** authorize a free-form cartesian search over transformations.

## Mechanism Hypotheses

### M1: Residual Continuation

Economic story:

- once broad market and sector effects are stripped away,
- idiosyncratic winner/loser information may continue to diffuse across the next rebalance window.

Prediction:

- residual winners continue,
- residual losers lag,
- and the effect is strongest at intermediate horizons rather than overnight.

### M2: Residual Reversal After Overextension

Economic story:

- after strong residual moves, overextension may dominate continuation,
- especially when prior residual momentum has already crowded into the same side of the book.

Prediction:

- the most extreme residual winners underperform,
- especially after stress or sharp sentiment reversal windows.

### M3: Volatility-Managed Residual Persistence

Economic story:

- the family may contain real continuation information,
- but raw exposure may be too crash-prone without volatility conditioning.

Prediction:

- volatility-managed versions outperform raw residual continuation on a risk-adjusted basis,
- especially in left-tail windows.

## Mechanism Signatures

| Mechanism | Expected horizon | Expected stress sign | Expected baseline overlap | Falsification trigger |
|---|---|---|---|---|
| M1 residual continuation | short-to-medium rebalance horizon | weakens but stays positive | moderate overlap with frozen momentum baselines | matched controls and placebo windows show the same continuation |
| M2 residual reversal | short horizon after overextension | sign flips in crash/reversal windows | lower overlap with standard momentum | reversal only appears after arbitrary parameter changes |
| M3 vol-managed persistence | same horizon as M1 but smoother left tail | less negative in stress than raw continuation | moderate overlap with residual momentum family | vol-management improves optics but not mechanism tests |

## Expression Ledger Admission Rule

Every expression must declare:

1. primary mechanism,
2. expected signature,
3. expected failure mode,
4. and why it is not just a cosmetic variation of an already-listed expression.

An expression may not enter the calibration ledger simply because it changes:

- lookback length,
- skip length,
- neutralization target,
- clipping,
- or volatility scaling.

Those changes are admissible only if they test a declared mechanism.

## Mandatory Negative Controls

### C1: Shuffled / Noise-Matched Placebo

Take the residual-return series or ranking backbone and replace the signal ordering with a shuffled or noise-matched analogue while preserving broad distributional shape.

Purpose:

- detect whether the discovery machine is over-responsive to generic rank dispersion rather than mechanism-bearing structure.

Expected result:

- no stable winner should survive this control.

### C2: Pre-Window Placebo

Apply the same signal logic to the same stocks in a window before the intended rebalance window.

Purpose:

- detect whether the measured family effect is already present before the supposed continuation/reversal mechanism should operate.

Expected result:

- the family effect should weaken materially or disappear.

### C3: Matched Baseline-Mimic Control

Construct a control expression designed to mimic the frozen baseline mechanically while matching broad rank shape.

Purpose:

- confirm that the discovery machine can tell apart a truly distinct residual expression from a momentum rewrite.

Expected result:

- orthogonality and residualization should expose the mimic as non-incremental.

## Intra-Family Orthogonality Rule

The top surviving expressions may not all be near-duplicates.

At calibration closeout, the family should produce one of two clean reads:

1. one cluster dominates and the rest are cosmetic,
2. or two clearly distinct mechanism-bearing clusters survive.

If the calibration family cannot separate these cases, the discovery loop is not well-calibrated enough to open the primary family.

## Existing Code Reuse

This family may reuse existing repo assets as inputs:

- `src/portfolio_os/alpha/qualification.py`
- `src/portfolio_os/alpha/long_horizon.py`
- `src/portfolio_os/alpha/signal_consistency_diagnostic.py`
- `scripts/run_us_long_horizon_signal_extension.py`

It must **not** reuse the old `US alpha core restart` winner-gate logic as the discovery decision rule.

The old restart artifacts are informative historical context only.
They are not the decisive evaluator for this calibration family.

## Existing Code Non-Reuse

This calibration packet explicitly rejects the following reuse patterns:

- no revival of the old frozen eight-candidate tournament,
- no narrow-sample Week 4 winner decision,
- no assumption that current platform qualification logic is the discovery machine,
- no use of the old restart sample as the primary evidence base.

## Calibration Success Read

The calibration family is considered successful if it produces all of the following:

1. negative controls behave as expected,
2. bootstrap ranking is interpretable rather than noisy or degenerate,
3. intra-family orthogonality can separate redundant from distinct expressions,
4. residualization versus frozen baselines produces a meaningful incremental read,
5. and the family-level closeout logic can cleanly distinguish:
   - family winner,
   - regime-contingent local pattern,
   - mechanism suggestive but unproven,
   - and no winner.

## Calibration Failure Read

The calibration family is considered a machine-calibration failure if any of the following occurs:

1. placebo expressions survive as easily as real expressions,
2. winner ranking collapses under mild bootstrap perturbation,
3. orthogonality logic cannot distinguish mimic expressions from genuine family variants,
4. residualization destroys every candidate in a way that suggests the machine only rediscovers the baseline,
5. or the closeout labels are not interpretable.

If calibration fails at this level, the correct next move is to repair the discovery machine, not to open the primary family.

## Immediate Next Gate

The next step after this D1 packet is not primary-family mining.

The next step is:

1. translate this packet into a calibration implementation plan,
2. run the calibration family,
3. write a calibration closeout,
4. and only then decide whether the primary family may open.
