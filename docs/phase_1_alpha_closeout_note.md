# Phase 1 Alpha Closeout Note

## Scope

Phase 1 goal was to build a deterministic US alpha research and acceptance workflow on a frozen snapshot, then close the phase with a machine-readable binary decision.

Acceptance source of truth:

- `data/risk_inputs_us_expanded/returns_long.csv`
- `data/risk_inputs_us_expanded/factor_exposure.csv`
- `data/risk_inputs_us_expanded/risk_inputs_manifest.json`

Primary runtime artifacts:

- `outputs/phase1_alpha_acceptance_us_expanded/alpha_sweep_summary.csv`
- `outputs/phase1_alpha_acceptance_us_expanded/alpha_sweep_manifest.json`
- `outputs/phase1_alpha_acceptance_us_expanded/alpha_acceptance_decision.json`
- `outputs/phase1_alpha_acceptance_us_expanded/alpha_acceptance_note.md`

## Gate Outcome

- Final status: `accepted`
- Acceptance mode: `accepted_by_relative_and_absolute_gates`
- Baseline recipe: `equal_weight_momentum_6_1`
- Accepted recipe: `alt_momentum_4_1`
- Stop reason: `accepted_challenger`
- Completed rounds: `1`

The implementation closed Phase 1 in round 1 without requiring automatic expansion into rounds 2 or 3.

## Accepted Recipe

`alt_momentum_4_1` parameters:

- reversal lookback: `21`
- momentum lookback: `84`
- momentum skip: `21`
- forward horizon: `5`
- reversal weight: `0.0`
- momentum weight: `1.0`
- quantiles: `5`
- min assets per date: `20`

Interpretation:

- The accepted signal is pure momentum-first.
- The reversal leg is not needed in the accepted Phase 1 recipe.
- The shorter `84`-day momentum lookback beat the `126`-day baseline on the frozen expanded-US sample.

## Holdout Metrics

Accepted holdout metrics:

- mean IC: `0.11077831637999969`
- mean rank IC: `0.10630492196878749`
- positive rank IC ratio: `0.775`
- mean top-bottom spread: `0.012255149986753346`
- evaluation date count: `40`
- mean monthly factor turnover: `0.35`

Baseline holdout comparison:

- `equal_weight_momentum_6_1` mean rank IC: `0.05506122448979591`
- `equal_weight_momentum_6_1` mean top-bottom spread: `0.008706683339869042`
- `equal_weight_momentum_6_1` mean monthly factor turnover: `0.25`

The accepted recipe cleared both gates:

- relative gate: it beat the baseline on holdout mean rank IC and mean top-bottom spread while keeping positive rank IC ratio within tolerance
- absolute gate: it cleared the minimum thresholds for holdout signal quality, evaluation coverage, and turnover control

## Statistical Health Warning

All five recipes exhibited a sign flip in mean rank IC between the development and holdout slices. The accepted recipe `alt_momentum_4_1` ranked last on development (mean rank IC = -0.040) and first on holdout (mean rank IC = +0.106). This pattern suggests a regime change within the 100-date evaluation window rather than a stable predictive signal.

Additional concerns:

- Holdout contains only 40 evaluation dates across 50 stocks
- The t-statistic for holdout mean rank IC is approximately 2.9, which is still only moderate evidence on a short sample
- The development-holdout inversion means the holdout result alone cannot confirm signal robustness

This caveat does not invalidate the Phase 1 gate outcome. The gate was designed to enforce minimum viability, not production robustness. However, Phase 1.5 should treat the accepted recipe as a provisional seed, not a validated alpha, and should include a longer-sample robustness check when more data becomes available.

## Phase 1 Deliverables

Implemented in code:

- deterministic alpha acceptance workflow in `src/portfolio_os/alpha/acceptance.py`
- public alpha returns loader for shared workflows
- markdown acceptance note rendering
- dedicated CLI surface: `portfolio-os-alpha-acceptance`
- regression coverage for acceptance logic, CLI, and empty-evaluation failure semantics

Verification:

- `python -m pytest tests/test_alpha_acceptance.py tests/test_alpha_research.py -q` -> `14 passed`
- `python -m pytest -q` -> `296 passed, 28 warnings`

## Decision

Phase 1 is complete.

Recommended next step:

- move into Phase 1.5 expected-return integration using `alt_momentum_4_1` as the accepted seed alpha

Important boundary:

- Phase 1 closed the research-and-gate loop only
- it did not yet connect alpha into portfolio construction
- optimizer integration should start from the accepted recipe, not from the rejected equal-weight blend
