# Quant Research Meta-Analysis

Date: 2026-04-08

## Purpose

This note is a cross-branch meta-analysis of the `Quant` workspace as of 2026-04-08.
It is not a chronological project history and it is not a single-branch handoff.
Its job is to answer a narrower question:

> Across all completed work so far, what has actually been learned, what is now stable enough to treat as a real conclusion, and where is the remaining research value?

## Scope

Primary source set:

- `PortfolioOS/TASK_MEMORY.md`
- `PortfolioOS/docs/phase_1_alpha_closeout_note.md`
- `PortfolioOS/docs/phase_1_5_alpha_decision_note.md`
- `PortfolioOS/docs/cost_model_decision_note.md`
- `PortfolioOS/docs/ashare_a5_closeout_and_phase3_kickoff_note.md`
- `qlib_spikes/portfolioos_signal_probe_01/docs/us_wrds_memory.md`
- `qlib_spikes/portfolioos_signal_probe_01/docs/us_wrds_alpha_roadmap.md`
- `qlib_spikes/portfolioos_signal_probe_01/docs/superpowers/notes/2026-04-06-sue-pead-mapping-closeout.md`
- `qlib_spikes/portfolioos_signal_probe_01/docs/superpowers/notes/2026-04-06-car3-confirmation-closeout.md`
- `qlib_spikes/portfolioos_signal_probe_01/docs/superpowers/notes/2026-04-07-revision-event-aware-closeout.md`
- `qlib_spikes/portfolioos_signal_probe_01/docs/superpowers/notes/2026-04-08-revision-event-mainline-poc-attribution.md`
- `qlib_spikes/portfolioos_signal_probe_01/docs/superpowers/notes/2026-04-08-hybrid-mainline-v1-closeout.md`
- `qlib_spikes/portfolioos_signal_probe_01/.worktrees/ashare-a1/ASHARE_MEMORY.md`
- `qlib_spikes/portfolioos_signal_probe_01/.worktrees/ashare-a1/docs/superpowers/notes/2026-04-07-ashare-inefficiency-hypothesis-ledger.md`
- `qlib_spikes/portfolioos_signal_probe_01/.worktrees/ashare-a1/docs/superpowers/notes/2026-04-08-ashare-stage1-diagnostic-closeout.md`

## Executive Summary

The strongest achievement of this workspace is not that it has already produced a universal deployable alpha.
The stronger claim is that it has produced a credible research system that now distinguishes between:

- real signal,
- data-quality illusion,
- evaluator or label misspecification,
- optimizer-structure failure,
- proxy-constrained inconclusive branches,
- and honest negatives that should stay closed.

That distinction is the main asset.

The current highest-confidence positive result is the US event-driven earnings family, especially announcement-timed `SUE`.
The current highest-confidence A-share lead is `anti_mom_21_5`, but it remains only `partially_real`.
The optimizer, TCA, and risk work have already shown that alpha quality is the primary bottleneck.
Further progress will come much more from better signal packaging, label design, and branch selection than from more cost tuning or generic model retries.

## What The Project Has Already Built

The workspace is no longer in an early prototype state.
Three durable foundations now exist.

### 1. A usable platform layer

`PortfolioOS` is already a compliance-aware rebalance, scenario, approval, execution-simulation, backtest, TCA, and research CLI platform.
The canonical orchestration path and regression suite exist, and the mainline memory now treats the core platform as stable rather than exploratory.

This matters because recent negative research results are increasingly hard to dismiss as "plumbing issues."
In practical terms, the platform has crossed the threshold where research failure is more likely to be a real research failure.

### 2. A reusable data layer

The project has moved beyond one-off pulls:

- the FMP freeze is a real reusable research asset,
- strict PIT universe manifest work now exists for the newer US branch,
- WRDS is established as the canonical PIT source for analyst and event research,
- A-share branch work now uses a more explicit feasibility / proxy-quality discipline.

This is one of the deepest accomplishments in the repo.
The project has learned that data quality is not a small implementation detail.
It is often the dominant variable.

### 3. A real evaluation discipline

The workspace now contains multiple examples where the original evaluator shape was materially wrong for the hypothesis under test.
The system has therefore evolved from "run a backtest and inspect the chart" to:

- choose a hypothesis family,
- choose a representation,
- choose the correct event or carry evaluator,
- run null calibration where needed,
- classify outcomes as positive, negative, or inconclusive,
- freeze nearby rescue variants unless a truly new mechanism exists.

That discipline is now one of the clearest differentiators of the project.

## Durable Empirical Findings

### A. Data quality changes the research answer

This point now appears across multiple branches and should be treated as fixed.

The clearest example is the US analyst and event work:

- FMP was sufficient for broad fundamentals coverage,
- but insufficient for clean point-in-time analyst revision history,
- WRDS materially changed the conclusions for both signal quality and evaluator design.

The project should therefore treat "better data changed the answer" as a first-order empirical finding, not as a mundane data-engineering footnote.

### B. Label and evaluator alignment matter more than extra model complexity

This is arguably the single strongest cross-branch conclusion in the entire workspace.

In the US WRDS line:

- announcement-timed `SUE` became decisively strong only under an announcement-driven evaluator,
- `revision` became clearly real only once the label matched the natural `to-next-announcement` event cycle,
- the same `revision` signal added only a tiny lift when injected into the old fixed-horizon mainline as a feature,
- a custom event-label PoC on the same event rows was strongly positive,
- a naive hybrid mainline that restored full coverage lost most of that edge again.

The practical reading is sharp:

- the main problem was not "LightGBM cannot use the feature,"
- the main problem was label mismatch,
- and the smallest successful restart direction is event-aligned label design rather than more feature stacking.

### C. The best US alpha is event-driven, not generic fixed-horizon ML

The US line is now best understood as a closed event-driven methods chapter rather than an active generic ML search.

Stable hierarchy:

- primary alpha: announcement-timed `SUE`
- secondary branch: event-aware `revision`
- tertiary overlay: `CAR3` as modest confirmation only

What failed:

- broad fixed-horizon LightGBM retries,
- naive feature-only revision ingestion into the fixed-horizon mainline,
- naive hybrid-v1 fallback design,
- older carry-style evaluation for announcement-driven signals.

This is not a small local lesson.
It says that the US line already found its strongest economic object:
earnings-event alpha with event-aware timing.

### D. The best A-share result is real enough to keep, but not strong enough to crown

`anti_mom_21_5` is the only audited A-share lead still standing.
It survives multiple stripping and audit layers and is therefore more than noise.
But later-half walk-forward weakness keeps it below a "clearly real" classification.

The right reading is:

- it is the current A-share lead,
- it is a useful anchor for cross-market comparison,
- but it is not yet the sort of robust foundation that justifies unlimited downstream tuning.

This distinction is important.
The project has avoided the common failure mode of treating a `partially_real` lead as a production result.

### E. A-share negative results are now informative, not embarrassing

The A-share branch has matured from "maybe this hypothesis just needs another proxy" into a more honest ledger system.

After the event-conditioned Stage 1 retrofit and `1000x` null calibration:

- some families are now credible negatives,
- some families are now clearly inconclusive under the current proxy stack,
- and several nearby rescue variants are explicitly frozen.

This is a genuine research asset.
It means the project is no longer paying the same tuition twice.

### F. Optimizer and TCA work have already identified the true bottleneck

The cost and risk work now support a strong structural statement:

- the optimizer can trade off cost, risk, and tracking mechanics,
- but without a meaningful alpha or expected-return input, it mostly differentiates itself by trading less.

This finding appears in more than one place:

- zero-cost probes,
- calibrated cost sweeps,
- augment and replace risk-aversion sweeps,
- Phase 1.5 alpha integration.

The result is consistent:
the platform has enough optimizer and TCA infrastructure for the current stage.
What it does not have is enough signal thickness to justify further optimizer promotion.

## Cross-Branch Patterns

The value of a meta-analysis is not the individual result list.
It is the recurring structure across results.

### 1. Data quality beats model complexity

The project has repeatedly gained more by improving the data source or timing semantics than by increasing model complexity.

Examples:

- WRDS versus FMP for analyst and earnings-event work
- strict event-aware labels versus fixed-horizon targets
- feasibility gates and proxy audits in A-share before escalating branch effort

This pattern argues for a general rule:
when a branch is weak, first suspect data semantics and timing alignment before assuming the next model class will rescue it.

### 2. Research framing beats parameter search

Several branches improved not by adding more knobs, but by reframing the question correctly.

Examples:

- `revision` improved when its lifecycle was respected
- `CAR3` became useful only after being demoted to a confirmation overlay
- A-share event diagnostics became clearer once Stage 1 was made event-conditioned and null-calibrated

The inverse is also true:
many low-value retries were correctly closed because they only mutated the same weak framing.

### 3. Universe choice matters

The project has already shown that large-cap and lower-efficiency slices can behave very differently.
US results on `top_500` and lower-efficiency / mid-cap style slices are not interchangeable.
A-share dynamic mid-cap has also been the key location where the current lead survives.

This argues against treating "the market" as a single object.
Many branches should be interpreted as conditional on market segment, not universal.

### 4. Honest negatives are a core output

This is one of the healthiest cultural features in the repo.
The project increasingly treats a branch as successful if it reaches a defensible classification, even if that classification is negative.

That matters because it changes capital allocation.
It becomes possible to stop.

### 5. The next unit of value is a package, not another isolated factor

This point is strongest on the US side, but it generalizes.

The biggest unresolved question is no longer:

> Is there any alpha somewhere?

It is now:

> Which small set of validated, sufficiently independent components forms a package strong enough to deserve translation into later phases?

That is a qualitatively different stage of research maturity.

## Current Boundaries

### What should stay closed

- Broad fixed-horizon US feature-only retries built on the same old target
- Standalone `CAR3`
- Naive hybrid-v1 mainline fallback as a solution concept
- More US optimizer / integration work before a stronger alpha package exists
- More A-share `anti_mom_21_5` mutation work
- More A-share rescue attempts inside already diagnosed repurchase, analyst, or H5-adjacent families
- More cost-model or risk-only sweeps as if they were still the primary bottleneck

### What remains live or conditionally live

- US event-driven package qualification, but only if explicitly restarted and only through the narrow roadmap gates already written
- A-share hypothesis-generation and branch selection, because that line is still active and still underdetermined
- Shared evaluation hardening, especially where null calibration and event-aware framing improve branch honesty
- Alpha-translation sanity work when it tests infrastructure rather than disguising further signal rescue

## Current Bottlenecks

The bottlenecks are now clearer than they were even a few days ago.

### 1. Package formation

The repo has several validated components, but very few validated packages.
That is now the key problem.

### 2. Label design for event-aware signals

US has already shown that this issue is real and economically large.
It may appear again in other branches.

### 3. Proxy quality in A-share event families

The event-stage diagnostic made this explicit.
Several A-share branches are not awaiting one more transformation.
They are blocked by proxy quality.

### 4. Research sequencing

The A-share line especially now needs disciplined branch selection, not more local optimization around the current lead.

## Final Assessment

The most honest overall assessment is:

1. The project has already succeeded as a research platform.
2. It has not yet succeeded in turning that research into a broad production-ready alpha stack.
3. The strongest validated alpha family is US event-driven earnings alpha, especially announcement-timed `SUE`, with event-aware `revision` as a credible companion.
4. The strongest active A-share result is `anti_mom_21_5`, but it remains a lead candidate rather than a crowned winner.
5. The optimizer, TCA, and risk work have already done their job for the current stage by proving that alpha quality is the bottleneck.
6. The workspace's biggest strategic asset is now methodological clarity:
   it knows more clearly what to stop doing.

That last point matters the most.
At this stage, the project should be judged less by whether it found one perfect chart and more by whether it built a research process that compounds real knowledge and stops compounding false hope.

By that standard, the workspace is already producing high-quality research capital.
