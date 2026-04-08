# TASK_MEMORY

This file is the short handoff note for continuing PortfolioOS. It keeps only the current state, stable conclusions, and the next useful branch. Detailed artifacts remain in `docs/`, `outputs/`, and the external research workspaces.

## Current Snapshot

- PortfolioOS is a compliance-aware portfolio rebalance, scenario, approval, execution-simulation, backtest, TCA, and research CLI platform.
- Core platform buildout through Phase 12 is implemented and stable.
- Canonical orchestration path: `src/portfolio_os/workflow/single_run.py`.
- Historical backtests should run through library calls, not CLI subprocess chains.
- Default execution simulation mode: `impact_aware`.
- `participation_twap` is intentionally preserved in sample execution requests as the baseline comparison mode.
- Latest full regression on this machine: `python -m pytest -q` -> `310 passed, 38 warnings`.

## Active Worktree Topology

- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01` main research repo = home of the closed US WRDS method asset; read `docs/us_wrds_alpha_roadmap.md` and `docs/us_wrds_memory.md` there instead of treating US as an active worktree.
- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1` = the only active A-share research line.
- `C:\Users\14574\Quant\PortfolioOS` main repo = shared platform changes only; do not leave branch-local research copies here.

## Stable Platform State

### Data And Inputs

- Provider/builder infrastructure for `market.csv`, `reference.csv`, `target.csv`, and snapshot bundles is complete.
- Tushare permission-aware fallbacks are implemented.
- Because current Tushare `index_weight` access is still limited, client-provided `target.csv` remains the official fallback when live snapshot generation is incomplete.
- Frozen expanded-US research assets are the canonical US fundamentals workspace:
  - `data/universe/us_equity_expanded_tickers.txt`
  - `data/universe/us_universe_reference.csv`
  - `data/universe/us_universe_market_2026-03-27.csv`
  - `data/risk_inputs_us_expanded/`
  - `C:\Users\14574\Quant\fmp_data_freeze`
- Use the frozen FMP workspace for resumed US fundamentals or transcript work; do not reopen the old `yfinance` path.
- Important FMP caveat: frozen analyst-estimate payloads are not PIT-safe for analyst-revision research because they do not include historical snapshot metadata.
- WRDS bootstrap research infrastructure is now live in the external workspace under:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\run_w1.py`
  - `...\scripts\wrds_ibes_w3.py`
  - `...\scripts\wrds_eval_adapter.py`
  - `...\outputs\wrds_bootstrap\`
- Stable WRDS research notes:
  - `W1` dynamic universes were validated cleanly: `top_500_dynamic` stays at exactly `500` names and `rank_500_1500_dynamic` stays at exactly `1000`
  - `wrds.iclink` is not available on this account; the working IBES-CRSP link uses local `ibes.idsum` CUSIP matching with `sdates`-aware validity handling
  - for analyst and event research, WRDS is now the canonical PIT source; do not use the old FMP estimate history as a substitute

### Execution, TCA, And Costs

- Expanded-US replay/sample validation for `sample_us_04` to `sample_us_06` is already complete; the stack is healthy enough for further research work.
- Fill collection preserves `reference_price` end to end.
- BOM-tolerant JSON handling is required for some calibration and broker-state artifacts.
- The constrained fill batch generator already supports broker positions, buying-power limits, and audit manifests.
- Current account scale only supports realistic participation around `0-0.1%` on the frozen 50-name US universe.
- Low-participation TCA closure is done:
  - `overlay_readiness = sufficient`
  - `candidate_k = 3.498400399110418`
  - scope = paper overlay only
- Do not extrapolate the calibration above `0.1%` participation.
- Do not promote calibrated `k` into `config/us_expanded.yaml` yet.
- Calibrated research config exists at `config/us_expanded_tca_calibrated.yaml`.
- The main estimator follow-up is to include negative-signal eligible fills; useful, but not blocking the current low-participation closure.

### Optimizer And Risk Conclusions

- Cost-model sweeps show a structural problem, not just `k` tuning:
  - without a real alpha / expected-return term, the optimizer mainly differentiates itself by suppressing trades
  - under realistic costs, that suppression gives up more holding return than it saves
- Risk-aversion tooling is implemented (`portfolio-os-risk-sweep`), but no Sharpe-improving sweet spot was found.
- Conclusion: better alpha is the main bottleneck, not more optimizer tuning or a risk-aware default objective.

## US Research State

- Status: closed US WRDS methods chapter; do not treat as an active discovery queue.
- Last sync: `2026-04-08`
- Branch-local references:
  - roadmap = `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\docs\us_wrds_alpha_roadmap.md`
  - memory = `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\docs\us_wrds_memory.md`
- Stable read:
  - data quality and evaluation-horizon mismatch were both real bottlenecks
  - `announcement-timed SUE` is the main US event alpha
  - finalized event-aware `revision` is real, but fixed-horizon mainline ingestion does not recover most of its edge
  - naive hybrid-v1 fallback is already rejected
- Current active branch: none
- Next decision node:
  - if US is explicitly restarted, first gate is `SUE x revision` orthogonality audit for event-driven package qualification
  - otherwise default action is `no action`
- Do not reopen broad fixed-horizon retries, standalone `CAR3`, old `21d` carry evaluation, or optimizer / integration work from this state.

## A-Share Research State

- Status:
  - active as a branch-local research line; main-repo memory is now index-only for A-share
- Last sync:
  - `2026-04-08`
- Branch-local canonical docs:
  - memory:
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\ASHARE_MEMORY.md`
  - roadmap:
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\docs\ashare_alpha_roadmap.md`
  - ledger:
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\docs\superpowers\notes\2026-04-07-ashare-inefficiency-hypothesis-ledger.md`
- Stable read:
  - `anti_mom_21_5` remains the only audited A-share lead and is currently `partially_real`
  - `pivot 1a` is already triggered
  - A5 is closed as an execution / alpha-translation repair result, not as an alpha winner
  - the current A-share line is sequencing-driven, with multiple honest negatives, several inconclusives, and no nearby-family reopen path
- Current active branch:
  - none
- Next decision node:
  - lift one deferred path into an honest executable branch
  - or regenerate the post-pivot A-share ledger before opening a new branch
- Main-repo rule:
  - do not use this section as the operating note for A-share work
  - use the branch-local memory above

## Recommended Next Steps

1. Treat the US Phase 1 through Phase 4E arc as methodologically valuable but stage-closed on the primary deployment universe.
2. Keep the active research pivot on A-share.
3. Treat the core A-share single-factor stage as already informative:
   - `anti_mom_21_5` is the primary price signal
   - turnover is a related but distinct companion factor
   - the main conditional contrast is high-vol anti-momentum vs non-high-vol turnover
4. The next natural A-share stage is now explicitly gated by the `anti_mom_21_5` audit:
   - the signal is strong enough to keep
   - but it is only `partially_real`, not clean enough to justify open-ended A4 / A5 tuning on top of it
   - immediate next work should stay diagnostic and discipline-preserving:
     - Monte Carlo-hardening of the audit pipeline
     - Stage 0 / Stage 1 progression on the next ledger-selected hypothesis
     - synthetic-alpha toy translation tests for the repaired A5 path
   - freeze for now:
     - more anti-momentum subperiod / regime / lookback retuning
     - more A5 config sweeps around the real signal
     - `book_to_price` / PIT expansion work
5. The next distinct research branch to open after A5 closeout is US Phase 3.0:
   - universe:
     - `expanded_liquid_core`
   - PIT rule:
     - use `filingDate` anchored fundamentals from the FMP freeze path
   - first milestone:
     - build the strict PIT universe + staging + qlib-ready dataset
     - do **not** start with ensembles, multi-horizon models, or optimizer integration
   - kickoff status:
      - universe-manifest step is already live
      - latest manifest output:
        - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\outputs\phase3_0_us_universe_manifest\phase3_universe_manifest.json`
      - current primary count:
        - `expanded_liquid_core_count = 1989`
      - current PIT quality read on that universe:
        - `daily_history_min = 1250`
        - `pit_quarter_min = 20`
        - `pit_valid_core_quarter_median = 92`
5. Keep deterministic signals as the working baseline; do not promote `qlib_lgbm`.
6. Treat signal robustness and signal translation as the main bottleneck, ahead of optimizer tuning or RL execution work.
7. Treat the current TCA overlay as valid only for `0-0.1%` participation.
8. Do not promote calibrated `k = 3.498400399110418` into the default config.
9. If US research is reopened, prefer a clearly new branch:
   - lower-efficiency or mid-cap slices over more top-500 large-cap retries
   - event-driven announcement or transcript hypotheses rather than more monthly-carry or screen-tuning retries
   - objective liquid-universe definitions rather than canonical-300-specific framing
10. Keep the canonical 300-name list only as a continuity control; prefer `expanded_liquid_core` as the primary US research universe when a genuinely new hypothesis exists.
11. Do not resume optimizer-promotion or RL-execution work until alpha is signal-ready again.
12. If the WRDS US branch is resumed next, the best immediate path is staying inside the new announcement-driven evaluator:
   - keep the CAR3 confirmation line closed at `z(SUE) * sign(CAR3)` unless a new hypothesis clearly justifies reopening it
   - extend announcement-timed SUE mapping / short-window PEAD variants
   - revisit revision separately with event-aware labels rather than forcing it into the announcement-only frame

### Current SUE PEAD Read

- Announcement-timed `SUE` PEAD mapping on `rank_500_1500_dynamic` is now live across:
  - `[+2,+2]`
  - `[+2,+3]`
  - `[+2,+4]`
  - `[+2,+5]`
  - `[+2,+7]`
  - `[+2,+10]`
  - `[+2,+15]`
  - `[+2,+22]`
- Best clean / risk-adjusted ranking window is:
  - `[+2,+2]`
  - `rank_ic_t ~ 22.7`
- Best alpha-only t-stat window is:
  - `[+2,+3]`
  - `alpha_only_t ~ 12.3`
- Best mean alpha-only spread window is:
  - `[+2,+22]`
  - `mean_alpha_only_spread ~ 1.49%`
- Rank-IC decays as the window lengthens, but mean alpha-only spread remains economically meaningful through roughly the 2-to-3 week range:
  - practical read: the drift is front-loaded in purity / ranking power, but not fully exhausted immediately
- Breadth remains stable across the dense grid:
  - active months stay around `251-252`
  - total event count stays around `78k`
  - mean events per month stays around `312`
- Half-sample split stays directionally positive:
  - the result looks real
  - later-half t-stats are generally lower, so the strongest window numbers should be treated as strong empirical reads rather than fixed constants
- This reinforces the current US WRDS boundary:
  - the main edge is announcement-timed `SUE`
  - the CAR3 overlay is secondary and should stay closed as a modest delayed-entry confirmation result
  - the next distinct branch after SUE mapping remains event-aware `revision`

### Current Revision Event-Aware Read

- FY1 `revision_1m` now has its own narrow Phase C runner:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\scripts\run_revision_event_mapping.py`
- Phase C is now closed with a final representative spec:
  - signal date = `statpers`
  - entry = next trading day after `statpers`
  - label = **pure `to-next-announcement`**
  - representation = **full panel**, not latest-snapshot-per-event
  - aggregation = monthly cross-sectional evaluation by signal month on `rank_500_1500_dynamic`
- Final representative result:
  - `monthly_signal_count = 250`
  - `total_signal_count = 218,766`
  - `mean_signals_per_month = 875.1`
  - `mean_rank_ic ~ 0.0166`
  - `rank_ic_t ~ 3.61`
  - `mean_alpha_only_spread ~ 0.42%`
  - `alpha_only_t ~ 4.03`
- Shared-event overlap with announcement-timed `SUE` is low:
  - using the last revision snapshot before each earnings event, pooled rank correlation with `SUE` is only `~0.11`
  - practical read: revision is not just a noisy SUE shadow
- Closeout interpretation:
  - the main problem in early Phase C was **horizon misspecification**, not snapshot dilution
  - `last snapshot per event` did not win even after the horizon was corrected:
    - pure full panel: `rank_ic_t ~ 3.61`, `alpha_only_t ~ 4.03`
    - pure last snapshot: `rank_ic_t ~ 2.37`, `alpha_only_t ~ 0.79`
  - final representative spec is therefore **full-panel pure `to-next-announcement` revision**
  - announcement-timed `SUE` remains the primary event-driven alpha benchmark, but finalized revision is strong enough to treat as a real separate branch rather than a weak afterthought
  - if the US WRDS line continues, the next question is whether this finalized revision branch adds enough marginal value relative to announcement-timed `SUE`; Phase C itself should not be reopened

### Phase 3 Mainline Integration Pre-Registration

- Next cheap discriminating experiment:
  - wire finalized revision back into the Phase 3 Qlib / LightGBM mainline as a **feature**
  - keep the current fixed-horizon training label unchanged on the first pass
  - run a matching simple linear baseline in the same round
- Revision feature spec for the first-pass mainline test:
  - use **full-panel pure `to-next-announcement` revision** as the source signal
  - feature timestamp is still `statpers`, not `announcement_date`
  - align to the **next trading day** before the feature becomes visible in the daily model table
  - use **latest-value carry-forward** between revision snapshots until a newer snapshot arrives
  - practical caveat: this repeated carry-forward makes revision look more persistent in the training table than the number of truly independent revision events; do not over-interpret raw tree split counts
- Mainline comparison must stay locked across runs:
  - same universe
  - same label
  - same train/validation split
  - same random seeds
  - same feature-standardization fit window
  - same early-stopping criterion
  - compare **delta versus baseline**, not absolute one-off scores
- Seed discipline:
  - run at least `3` fixed seeds for baseline and `+revision`
  - judge the result on mean delta and dispersion, not on a single lucky seed
- Simple baseline definition for this round:
  - use **signal-level** cross-sectional equal-weight combination, not portfolio-level strategy ensembling
  - standardize features by **same-day cross-sectional z-score**
  - then average the standardized signals into one composite score
  - this is the sanity-check baseline that should be compared directly against LightGBM's score output
- Importance/readout discipline:
  - if revision appears important in LightGBM, cross-check with permutation importance or SHAP-style diagnostics
  - do not treat raw split / gain importance alone as decisive because carry-forward can overstate visual prominence
  - Pre-registered interpretation bands for the first-pass mainline test:
    - **Clearly positive**:
      - overall IC improves by `>= 15%`, or revision ranks stably in the top `5` features
      - interpretation: fixed-horizon mainline is already able to absorb most of the revision edge; keep stacking validated features before touching labels
  - **Intermediate**:
    - overall IC improves by `5%` to `< 15%`, or revision lands in the top `10` but not stably
    - interpretation: fixed-horizon mainline captures part of the edge; record the gain and keep label redesign as a later optimization path
    - **Near-zero**:
      - overall IC improves by `< 5%`
      - interpretation: fixed-horizon labeling is likely leaving most of the revision value on the table; event-aligned label redesign becomes the next justified engineering step

### Phase 3 Mainline Integration First-Pass Result

- The cheap discriminating integration test is now partially complete:
  - finalized WRDS revision was wired into the external Phase 3 Qlib mainline as `revision_1m_wrds`
  - implementation lives in:
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\scripts\staging_to_qlib.py`
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\scripts\run_qlib_lgbm_walkforward.py`
  - feature contract used:
    - source = finalized full-panel FY1 `revision_1m`
    - feature timestamp = `statpers`
    - visibility = next trading day
    - daily representation = latest-value carry-forward
- Fixed-horizon mainline read on `expanded_liquid_core`:
  - baseline seeds were all weak/negative:
    - seed `7`: `mean_rank_ic ~ 0.0010`, `rank_ic_t ~ 0.07`, `alpha_only_t ~ -1.42`
    - seed `17`: `mean_rank_ic ~ -0.0011`, `rank_ic_t ~ -0.08`, `alpha_only_t ~ -1.86`
    - seed `29`: `mean_rank_ic ~ -0.0015`, `rank_ic_t ~ -0.11`, `alpha_only_t ~ -2.48`
  - `+revision` seed `7` improved slightly but remained economically weak:
    - `mean_rank_ic ~ 0.0021`
    - `rank_ic_t ~ 0.15`
    - `alpha_only_t ~ -1.22`
  - practical read:
    - fixed-horizon Qlib does appear to ingest **some** revision information
    - but the realized lift is tiny relative to the size of the event-aware revision edge
    - this did **not** turn the mainline model into a live signal
- Simple linear sanity check reached the same conclusion:
  - same-day cross-sectional z-score equal-weight custom-feature composite without revision:
    - `rank_ic_t ~ -0.46`
    - `alpha_only_t ~ 0.57`
  - with revision added:
    - `rank_ic_t ~ -0.46`
    - `alpha_only_t ~ 0.60`
  - practical read:
    - adding revision helps only trivially in the fixed-horizon signal-level baseline too
- Important model-usage nuance:
  - on the completed `+revision` seed `7` run, `REVISION_1M_WRDS` ranked very high in raw tree usage:
    - gain rank = `2`
    - split rank = `1`
  - do **not** over-read that as proof the mainline solved revision
  - the carry-forward representation makes revision visually prominent in tree splits, but the realized holdout lift stayed small
- Updated decision boundary:
  - do **not** reopen a broad multi-seed fixed-horizon mainline sweep just for revision
  - do **not** promote the fixed-horizon Phase 3 Qlib branch on the back of this result
  - current best interpretation is:
    - fixed-horizon mainline absorbs a little of revision
    - most of the value remains tied to event-aware labeling
  - if the US WRDS line returns to mainline model work, the justified next step is **event-aligned or hybrid label design**, not more feature-only stacking on the old fixed-horizon target

## Key Paths And Docs

- Main docs:
  - `docs/execution_mode_decision_note.md`
  - `docs/cost_model_decision_note.md`
  - `docs/platform_ml_rl_roadmap.md`
  - `docs/phase_1_alpha_closeout_note.md`
  - `docs/phase_1_5_alpha_decision_note.md`
- Core alpha implementation:
  - `src/portfolio_os/alpha/`
- Alpha CLIs:
  - `portfolio-os-alpha-research`
  - `portfolio-os-alpha-acceptance`
- Canonical US freeze summary:
  - `C:\Users\14574\Quant\fmp_data_freeze\summary\fmp_coverage_summary.json`
- External research workspace:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01`
- Latest WRDS US research artifacts:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\outputs\wrds_bootstrap\`
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\notes\2026-04-06-car3-confirmation-closeout.md`
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\notes\2026-04-06-sue-pead-mapping-closeout.md`
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\notes\2026-04-07-revision-event-aware-initial.md`
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\notes\2026-04-07-revision-event-aware-closeout.md`

## Workflow Notes

- Commit every substantive change.
- Default full regression on this machine: `python -m pytest -q`.
- Generated artifacts under `outputs/` stay out of version control.

