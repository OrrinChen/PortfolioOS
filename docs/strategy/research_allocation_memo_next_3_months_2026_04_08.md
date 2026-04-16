# Research Allocation Memo: Next 3 Months

Date: 2026-04-08

## Purpose

This memo turns the current `Quant` research state into an allocation decision for the next three months.
It assumes the goal is not "maximize the number of experiments," but:

- maximize durable research learning,
- preserve branch discipline,
- avoid paying twice for already-closed ideas,
- and increase the odds of forming at least one package-worthy signal family.

This memo is intentionally conservative.
The workspace already has enough evidence to show that undisciplined retries are expensive.

## Starting Position

Current state:

- `PortfolioOS` platform and research infrastructure are stable enough for continued work.
- US WRDS research is closed by default and now functions as a method asset.
- A-share is the only active discovery line, but it is post-pivot and must now be managed as a hypothesis portfolio rather than as a single-factor tuning exercise.
- Optimizer, TCA, and risk work are not the highest-value frontier right now.
- The strongest unresolved positive opportunity is package formation, not more isolated factor churn.

## Allocation Summary

Recommended allocation over the next three months:

- `50%` A-share branch generation, selection, and audit
- `20%` shared evaluation hardening and measurement infrastructure
- `15%` US event-driven package qualification reserve
- `10%` alpha-translation and execution sanity work
- `5%` documentation, memory upkeep, and platform maintenance

This is not a recommendation to actively spend all `15%` of the US reserve immediately.
That bucket is conditional.
If the US restart gate is not explicitly opened, that time should roll down to A-share branch work and shared evaluation hardening.

## Why This Allocation Makes Sense

### A-share gets the largest share because it is still the only active discovery line

The A-share branch has:

- one audited lead (`anti_mom_21_5`),
- multiple honest negatives,
- several inconclusives,
- a live hypothesis ledger,
- and a clear post-pivot need for better branch sequencing.

That is exactly where research time still compounds.
The highest-value next step is not polishing the current lead.
It is opening the next honest branch under the same discipline.

### Shared evaluation hardening deserves a real budget

This is not overhead.
Recent work already proved that evaluator shape and null calibration can materially change the interpretation of a branch.

The next three months should keep improving:

- event-conditioned evaluators,
- null calibration templates,
- overlap controls,
- walk-forward stability reads,
- and branch-level audit artifacts.

This bucket increases the value of every future branch.

### US should receive a reserve, not an open-ended restart budget

US has already produced a meaningful methods chapter.
The current question is not whether to restart broad discovery.
It is whether a narrow package-qualification branch is worth testing.

So the correct US allocation is a gated reserve:

- available if a restart trigger is chosen,
- unavailable for generic exploration,
- and automatically reallocated if the gate is not opened.

### Translation work should stay narrow and diagnostic

There is still value in translation work when it answers questions like:

- is the repaired optimizer infrastructure behaving sanely?
- does a synthetic-alpha control show the translation layer works?
- are cost terms causing unit mismatches again?

There is little value in translation work when it is used to disguise unresolved alpha weakness.

## Month-By-Month Plan

## Month 1: Rebuild the active research pipeline around discipline

Primary goal:
turn the current A-share line from a frozen lead plus a crowded ledger into a clean next-branch launch system.

### A-share allocation

Use most of the A-share budget on:

- Monte Carlo hardening of the existing audit pipeline where still missing
- maintaining the hypothesis ledger at a healthy live count
- selecting one next branch that is genuinely orthogonal to the current lead
- running that branch through Stage 0 and Stage 1 cleanly

Preferred branch types:

- a lifted deferred branch with a materially better data path
- or a structurally different post-pivot family

Avoid:

- nearby `anti_mom_21_5` mutations
- analyst-family rescue under the same metadata stack
- repurchase-family rescue under the same sparse proxy stack
- H5 sign-flip or similar local reinterpretations

### Shared evaluation allocation

Use this month to standardize:

- branch-specific null calibration outputs
- overlap-control reporting
- walk-forward half-split reporting
- event-conditioned versus carry-style evaluator templates

The goal is to make future closeouts cheaper and more comparable.

### Translation allocation

Do one narrow sanity path only:

- synthetic-alpha testing for the repaired A5 path

This should remain diagnostic.
Do not turn it into real-alpha tuning.

### US allocation

Default: no active US discovery in Month 1.

Only if the line is explicitly reopened:

- run `SUE x revision` orthogonality audit
- do not run broad model sweeps
- do not reopen `CAR3`
- do not reopen fixed-horizon feature stacking

## Month 2: Convert disciplined branch work into a second real candidate or a sharper no

Primary goal:
increase the number of well-classified signals, not the number of active threads.

### A-share allocation

By Month 2, one of two things should happen:

1. the Month 1 branch is promoted into deeper audit work, or
2. it is closed honestly and replaced by a structurally different branch

This month should emphasize:

- full audit for the most promising Month 1 branch if justified
- or immediate branch replacement if the first branch fails cleanly
- keeping the ledger populated above the minimum live threshold

The ideal outcome is not necessarily a new winner.
The ideal outcome is one more branch reaching a defensible final classification.

### Shared evaluation allocation

Use this month to integrate the lessons from the first new A-share branch back into the common toolkit:

- which evaluator shape matched the mechanism best,
- which null calibration form was most informative,
- and which branch artifacts should become standard.

### US reserve allocation

If the US reserve is activated and the orthogonality gate is favorable, run only the smallest package-qualification branch:

- compare pure `SUE` against the simplest justified `SUE + revision` package baseline
- judge the result on incremental value, not narrative appeal

Kill the branch quickly if:

- overlap is too high,
- the package does not beat pure `SUE` clearly,
- or the benefit is too thin to justify more packaging work.

If the US reserve is not activated, reallocate this time to A-share branch work or shared evaluation hardening.

## Month 3: Force a portfolio-level decision

Primary goal:
end the quarter with a clearer research portfolio, not with more half-open loops.

By the end of Month 3, the workspace should be able to answer:

- does A-share now contain at least two serious signal candidates from different families?
- is US worth a narrow package chapter, or should it remain closed?
- is any translation or integration work justified beyond synthetic and diagnostic checks?

### A-share decision target

Best-case outcome:

- one audited lead remains alive,
- one additional branch reaches at least serious-candidate status,
- and Stage 3 combination work becomes justified.

Acceptable outcome:

- no second winner appears,
- but several branches are cleanly closed,
- the ledger is regenerated around stronger post-pivot families,
- and the next quarter begins from a cleaner search space.

Bad outcome:

- the quarter ends with many live branch fragments but no new classification.

The memo's purpose is to prevent that bad outcome.

### US decision target

By the end of Month 3, US should be in one of only two states:

1. still closed, but with the existing event-driven chapter preserved cleanly, or
2. reopened narrowly because a package-qualification branch actually cleared its restart gate

It should not end the quarter half-reopened in a cloudy middle state.

### Translation and integration target

No real alpha-to-portfolio integration should be resumed unless a new package actually exists.

This means:

- no broad optimizer retuning
- no new cost-model promotion push
- no execution or RL expansion

unless the signal layer becomes materially stronger first.

## Explicit Spending Rules

The next three months should follow these rules.

### Spend more time on

- hypothesis selection quality
- evaluator alignment
- null calibration
- proxy-quality diagnosis
- overlap and independence checks
- package-level comparisons
- reusable audit artifacts

### Spend less time on

- near-identical factor retries
- additional fixed-horizon LightGBM runs on already-closed targets
- more `CAR3` work
- more A-share `anti_mom_21_5` tuning
- more repurchase or analyst rescue under unchanged proxy stacks
- more calibrated `k` or risk-only objective tuning

## Decision Gates

Use the following gates to protect allocation discipline.

### Gate 1: A-share next branch gate

Do not open the next A-share branch unless it is:

- meaningfully different from frozen nearby families,
- executable under the current data path,
- and equipped with a pre-registered kill condition.

### Gate 2: US restart gate

Do not spend the US reserve unless:

- the restart is explicit,
- the objective is package qualification rather than generic exploration,
- and the first step is the `SUE x revision` independence audit.

### Gate 3: Integration gate

Do not resume optimizer-promotion or broader integration work unless:

- there is a new package-worthy alpha result,
- and that result survives a more realistic translation read than the old fixed-horizon seeds.

## Recommended End-Of-Quarter Deliverables

At the end of the next three months, the target artifact set should be:

- one updated cross-branch memory note
- one refreshed A-share ledger with clear live / frozen / archived states
- one or two additional branch closeouts with honest final classification
- one shared audit template or toolkit upgrade
- optionally, one narrow US package-qualification closeout if and only if the restart gate was opened

The project should optimize for this artifact set rather than for raw experiment count.

## Bottom Line

The right portfolio for the next three months is:

- mostly A-share branch work,
- meaningfully more measurement discipline,
- a narrow conditional US reserve,
- only limited translation work,
- and almost no budget for already-diagnosed optimizer or feature-stack retries.

The reason is simple:
the best remaining upside is no longer hidden in extra tuning.
It is hidden in better branch selection, better label alignment, and the first small package that is strong enough to deserve the next layer of the stack.
