# A-Share A5 Closeout And Phase 3.0 Kickoff

## A5 Closeout

`A5` is now closed as a research-debugging stage.

The closed question was:

- can the A-share optimizer be repaired from the old "`transaction_cost` on => no-trade" cliff into a stable cost-aware trading path?

The answer is `yes`.

The root cause was not A4 covariance scaling. It was objective-space cost normalization. The old live optimizer compared:

- weight-space alpha
- weight-space risk / tracking error
- raw currency-space transaction cost

That mismatch created the kill-switch behavior. The repair was:

- keep fee / slippage accounting in raw currency space for execution, TCA, and reporting
- switch only the optimizer comparison term to `transaction_cost_objective_mode = nav_fraction`
- keep concentrated active targets as the working construction layer

## Evidence Chain

### 1. White-box autopsy

Source outputs:

- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\outputs\ashare_a5_transaction_cost_autopsy\`

Key result on `2022-06-02 / top_k_50_long_only`:

- a `1bp` active move improved alpha by about `4.78e-06`
- the same move increased live transaction cost by about `1.57` in raw currency units
- the normalized turnover increase for that same move was only `0.0002`

That is the decisive unit mismatch.

### 2. Sample8 recovery

Source outputs:

- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\outputs\ashare_a5_target_concentration_topk50_navfraction_sample8\`

Key result:

- `tradeable_rebalance_share = 1.0`
- `mean_surviving_order_count = 178.875`

This showed the fix changed the shape from "cost on => dead" to "cost on => more selective but still alive".

### 3. Full-history confirmation

Stable source outputs:

- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\outputs\ashare_a5_target_concentration_topk50_navfraction_full\`
- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\outputs\ashare_a5_target_concentration_topk100_navfraction_full\`

`top_k_50_long_only`:

- `rebalance_count = 90`
- `tradeable_rebalance_share = 1.0`
- `mean_surviving_order_count = 117.18`
- `mean_surviving_turnover = 0.1538`
- `annualized_return = 3.25%`
- `sharpe = 0.143`
- `cost_drag_start_nav_bps = 276.27`

`top_k_100_long_only`:

- `rebalance_count = 90`
- `tradeable_rebalance_share = 1.0`
- `mean_surviving_order_count = 106.54`
- `mean_surviving_turnover = 0.1297`
- `annualized_return = 3.22%`
- `sharpe = 0.145`
- `cost_drag_start_nav_bps = 243.25`

The important conclusion is not the Sharpe level. It is that the repaired baseline now trades stably over the full history instead of collapsing to no-trade.

## Known Limits

- This is not an "alpha winner" closeout.
- This is an execution / alpha-translation repair closeout.
- Full-history concentrated baselines are only around `Sharpe ~ 0.14-0.15`.
- Cost drag remains material at about `243-276` bps on start NAV.
- There is no reason to keep sweeping `k` inside A5. `top_k_50` vs `top_k_100` already gives the relevant monotonic control.

## Scope Check For US / Overlay Paths

The `nav_fraction` repair is currently opt-in, not global.

Code boundary:

- shared config default remains `transaction_cost_objective_mode = raw_currency`
- shared optimizer supports both:
  - `raw_currency`
  - `nav_fraction`
- explicit A-share opt-in currently appears only in:
  - `run_ashare_a5_constrained_backtest.py`
- the white-box autopsy explicitly forces `raw_currency` to preserve the old failure mode for diagnosis

Practical consequence:

- existing US configs do not set `transaction_cost_objective_mode`
- they therefore keep the old `raw_currency` behavior
- the US TCA / overlay readiness path is not implicitly changed by this patch, because slippage calibration and overlay readiness live in execution-calibration code, not in the A-share A5 research script

So the correct current sentence is:

- this repair is scoped to the A-share A5 research baseline
- it does **not** by itself invalidate the current US `candidate_k` / `overlay_readiness` conclusions
- there is no immediate US revisit required solely because this patch landed

## Phase 3.0 Kickoff

The next branch should be `US Phase 3.0`, not more A5 tuning.

Locked decisions:

- universe:
  - `expanded_liquid_core`
- fundamentals timing:
  - strict PIT using `filingDate`
- first milestone:
  - build strict PIT universe manifest
  - build staging dataset
  - build qlib-ready dataset
- first modeling milestone:
  - one LightGBM cross-sectional model
  - report `IC`, `rank IC`, and `top-bottom spread`
- non-goals for Phase 3.0 first pass:
  - no ensembles
  - no multi-horizon stack
  - no optimizer integration yet

Existing reusable scripts already cover most of this pipeline skeleton:

- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\scripts\build_phase3_universes.py`
- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\scripts\fmp_to_staging.py`
- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\scripts\staging_to_qlib.py`

Kickoff is already started at the universe-manifest layer.

Current output:

- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\outputs\phase3_0_us_universe_manifest\phase3_universe_manifest.json`

Current read:

- `expanded_liquid_core_count = 1989`
- `daily_history_min = 1250`
- `pit_quarter_min = 20`
- `pit_valid_core_quarter_median = 92`

So the immediate next job is not inventing a new stack. It is wiring the already-valid universe manifest into:

1. `fmp_to_staging.py`
2. `staging_to_qlib.py`
3. a first strict-PIT LightGBM cross-sectional baseline
