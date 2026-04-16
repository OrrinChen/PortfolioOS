# TASK_MEMORY

This file is the short handoff note for continuing PortfolioOS. It keeps only the current state, stable conclusions, and the next useful branch. Detailed artifacts remain in `docs/`, `outputs/`, and the external research workspaces.

## Current Snapshot

- PortfolioOS is a compliance-aware portfolio rebalance, scenario, approval, execution-simulation, backtest, TCA, and research CLI platform.
- Core platform buildout through Phase 12 is implemented and stable.
- The current project-wide meta stage is `research convergence + promotion contract`, not repo merge.
- Project operating mode is now `paper-stage only`.
- Default rule: freeze new research, new integrations, and new optimization work unless they are directly required by the live paper-stage path.
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
- Paper calibration sprint is now live as a platform-validation lane:
  - dry-run contract path is implemented via `portfolio_os.api.cli paper-calibration`
  - first neutral Alpaca paper run completed on `2026-04-15`
  - canonical live calibration read is:
    - `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_live_2026-04-15_v3`
    - `fill_rate = 100%`, `partial_count = 0`, `rejected_count = 0`
    - dedicated pre-trade reference snapshot is now captured:
      - `captured_ticker_count = 1`
      - `fallback_reference_count = 0`
      - `quoted_mid_price = 697.33`
      - `quoted_spread_bps = 0.86`
    - `requested_notional = 697.33`
    - `filled_notional = 697.36`
    - `reconciliation matched_count = 12`, `mismatched_count = 0`
  - important scope rule:
    - treat this as platform calibration, not alpha validation
    - current sample is still tiny and not yet a stress / slippage distribution sample
  - repeated-sampling tooling now exists:
    - `paper-calibration --repeat N --interval-seconds X`
    - `paper-calibration-aggregate --input-root ... --output-dir ...`
  - current recommended next step on this lane:
    - collect a small repeated `SPY x 1 share` tranche (`~30-50` runs)
    - aggregate drift, half-spread scaling, latency slope, and time-of-day buckets
    - then decide whether the result is:
      - systematic staleness bias
      - microstructure noise floor
      - or paper-venue-specific quirk
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
- Project-level structural decision is now frozen in:
  - `docs/strategy/single_period_vs_multi_period_structure_decision_memo_2026_04_15.md`
- Current read from that memo:
  - immediate full multi-period portfolio-construction work is **not** approved
  - the present issue is a mix of alpha weakness, objective-shape limitations, and missing intertemporal allocation logic
  - reopen multi-period only if a real alpha package later proves that single-period myopia is the next binding loss source
- Immediate optimizer-interface design reference is now:
  - `docs/strategy/objective_function_units_spec_2026_04_15.md`
- Current implementation order from that spec:
  - align alpha, cost, and covariance to one rebalance-period horizon
  - remove `target_deviation` from the core economic objective
  - treat cash deployment as a constraint design problem, not as a hidden objective anchor
  - rerun continuous-solution ablations before reopening signal research or multi-period work
- As of `2026-04-15`, the first objective-units implementation pass is now in code:
  - default `transaction_cost_objective_mode` is `nav_fraction`
  - risk-enabled objective paths now use the economic core only:
    - `risk_term`
    - `tracking_error`
    - `transaction_cost`
    - `alpha_reward`
  - legacy `target_deviation` / fee / turnover / slippage penalties no longer re-enter the risk-enabled objective through `augment`
  - walk-forward alpha snapshots now deannualize `annualized_top_bottom_spread` into a rebalance-period `expected_return`
  - backtest alpha panels now carry:
    - `period_top_bottom_spread`
    - `decision_horizon_days`
  - risk covariance is now scaled from annualized space to the same decision horizon when `decision_horizon_days` is supplied in-universe
- First post-fix local sanity checks:
  - on the frozen US expanded alpha sample at rebalance date `2025-10-31` with next rebalance `2025-11-28` (`decision_horizon_days = 19`):
    - objective decomposition shares were approximately:
      - `transaction_cost = 45.4%`
      - `risk_term = 31.0%`
      - `alpha_reward = 16.5%`
      - `tracking_error = 7.2%`
    - solver continuous gross traded notional was about `$452k`
    - repair preserved `20` executable instructions
  - practical read:
    - the optimizer is no longer collapsing into near-zero continuous trades solely because of raw-currency cost vs annualized-alpha mismatch
    - any remaining quality issues should now be treated as genuine portfolio-construction or signal problems, not as the old objective-unit bug

## US Research State

- Status: stage-closed again after the narrow US roadmap-first Phase 1 restart failed its first package gate; still not an open discovery queue.
- Last sync: `2026-04-08`
- Branch-local references:
  - roadmap = `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\docs\us_wrds_alpha_roadmap.md`
  - memory = `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\docs\us_wrds_memory.md`
- Stable read:
  - data quality and evaluation-horizon mismatch were both real bottlenecks
  - `announcement-timed SUE` is the main US event alpha
  - finalized event-aware `revision` is real, but fixed-horizon mainline ingestion does not recover most of its edge
  - naive hybrid-v1 fallback is already rejected
- same-event package qualification also failed:
  - same-event `SUE x revision` correlation stayed low
  - but the simple `SUE + revision` package still underperformed pure `SUE`
  - redesigned event-aligned mainline delta was not admitted from that branch
- Current active branch:
  - none; frozen while paper stage is the only active project lane
- Next decision node:
  - none by default
  - only reopen if a genuinely new US objective is chosen
- Do not reopen broad fixed-horizon retries, standalone `CAR3`, old `21d` carry evaluation, or paper/execution work from this state.

## A-Share Research State

- Status:
  - frozen as a branch-local archive; main-repo memory is index-only for A-share
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
  - the current tranche has now exported its first promotion-contract example bundle instead of trying to merge research code into PortfolioOS
- Current active branch:
  - none; frozen while paper stage is the only active project lane
- Next decision node:
  - none by default
  - only reopen by explicit decision to start a new A-share tranche
- Main-repo rule:
  - do not use this section as the operating note for A-share work
  - use the branch-local memory above

## Recommended Next Steps

1. Treat `paper-stage only` as the active project rule.
2. Freeze new research discovery, new alpha integration, new A4/A5 tuning, and repo-merger work.
3. Keep the research promotion contract in place as a stable boundary:
   - `docs/research_promotion_contract.md`
4. Use existing research outputs only as reference material for the paper stage; do not reopen them by drift.
5. Reopen any research line only by explicit decision, not because a deferred branch or nearby variant exists.

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
  - `docs/strategy/single_period_vs_multi_period_structure_decision_memo_2026_04_15.md`
  - `docs/strategy/objective_function_units_spec_2026_04_15.md`
  - `docs/phase_1_5_alpha_decision_note.md`
  - `docs/paper_calibration_runbook.md`
  - `docs/paper_calibration_live_2026_04_15.md`
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

