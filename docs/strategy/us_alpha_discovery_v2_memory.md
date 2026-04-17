# US Alpha Discovery v2 Memory

Short branch-local operating note for the active discovery-v2 lane.

## Status

- Worktree: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze`
- Branch: `codex/us-alpha-week1-freeze`
- Last sync: `2026-04-16`
- Active lane: calibration-family validation only
- Old `US alpha core restart` sprint remains closed with no winner

## Canonical References

- Charter:
  - `docs/strategy/alpha_discovery_charter_v2_2026_04_16.md`
- Family selection memo:
  - `docs/strategy/alpha_discovery_family_selection_memo_2026_04_16.md`
- Calibration D1:
  - `docs/strategy/us_residual_momentum_calibration_d1_2026_04_16.md`
- Kickoff plan:
  - `docs/superpowers/plans/2026-04-16-alpha-discovery-v2-kickoff.md`
- Calibration setup note:
  - `docs/strategy/us_residual_momentum_calibration_setup_note_2026_04_16.md`
- Calibration failure taxonomy:
  - `docs/strategy/calibration_machine_failure_taxonomy_2026_04_16.md`

## Program Structure

- Calibration family:
  - `US residual momentum / residual reversal`
- Primary mining family:
  - `A-share state-transition microstructure`
- Hard serial rule:
  - no primary-family mining before the calibration-family closeout exists

## Current Calibration Read

- Harness state:
  - `3` live expressions
  - `3` controls
  - persisted `100`-seed shuffled null distribution
  - expression bootstrap ranking artifact
  - expression spread-correlation artifact
  - baseline-residualization artifact versus frozen baseline mimic
- Best current live expression:
  - `RM3_VOL_MANAGED`
  - `mean_rank_ic ~ 0.0329`
  - `rank_ic_t ~ 0.7371`
  - shuffled-null percentiles only `~70-72%`
  - bootstrap top-1 frequency only `~53.8%`
  - baseline-residualized `rank_ic_t ~ 1.03`
- Key interpretation:
  - a single shuffled placebo draw is too noisy to judge alone on this short sample
  - after moving to a null distribution, the stronger read is still negative:
    - no live expression yet separates cleanly from the null/control envelope
  - orthogonality is not the main issue:
    - live-expression spread correlations stay modest (`max abs ~0.40`)
  - baseline residualization now exposes a boundary behavior rather than a positive read:
    - `RM3` and `RM1` strengthen after baseline removal
    - `RM2` largely collapses
    - `RM3` live residualized `rank_ic_t ~ 1.03` only reaches about the `81st` percentile of the residualization placebo null
    - `19 / 100` placebo seeds match or exceed the live residualized `rank_ic_t`
    - residualized `RM3` strength is concentrated in the `mid/high` baseline-exposure terciles while the `low` tercile is negative
    - practical read:
      - the strengthening is still null-consistent
      - and is being treated as a residualization-calibration edge case, not as family evidence
  - the updated failure mode is:
    - no clear null separation,
    - no stable internal winner dominance,
    - and a residualization step that still shows null-consistent strengthening under `RM3_VOL_MANAGED`
  - this boundary behavior is now formalized as:
    - `FM-001: Null-Consistent Residualization Strengthening`
  - calibration machine is therefore live but not yet validated

## Active Constraints

- Do not open the primary family yet.
- Do not treat the calibration family as a winner search.
- Do not reopen Branch A / fixed candidate tournament logic.
- Do not escalate into optimizer redesign from this lane.

## Next Gate

- Calibration-family closeout only if one of these becomes true:
  - live expressions clearly outrun the null/control envelope, or
  - the calibration family honestly shows that the current discovery machine is not yet trustworthy

## Practical Next Work

1. Keep strengthening calibration, not primary mining.
2. Preserve the shuffled-null benchmark as the default reference; do not reason from one placebo draw.
3. Use `FM-001` as the first machine-taxonomy entry; classify similar future reads against it before making family claims.
4. Next slice order is now fixed:
   - adversarial-layer hardening first
   - residualization-layer review second
5. Keep calibration-family alpha conclusions out of scope unless a later slice pushes the residualized read outside the null envelope.
