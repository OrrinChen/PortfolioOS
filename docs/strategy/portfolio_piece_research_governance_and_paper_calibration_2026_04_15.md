# Portfolio Piece: Research Governance And Paper Calibration

Date: 2026-04-15

## Why This Is A Real Project Asset

This project is strongest when it is described as a **quant research and execution-governance platform**, not as a claim that one alpha is already production-ready.

The two clearest portfolio pieces now available are:

1. a **research-promotion review surface** that turns archived research bundles into platform-readable candidate records
2. a **paper-calibration lane** that compares dedicated pre-trade reference snapshots with live Alpaca paper fills under a deliberately neutral strategy

Together, they show two different but complementary skills:

- how research results become auditable platform objects
- how execution assumptions are calibrated against reality without pretending that paper fills prove alpha

## Asset 1: Promotion Registry Review Surface

### Problem

The platform already had:

- branch-local research memories
- closeout notes
- a promotion contract
- a schema validator

But it did not yet have a reviewer-facing way to answer:

- what candidate packages exist
- what signals they contain
- whether they are blocked or promotable
- where the canonical memory and ledger artifacts live

### What Was Built

New platform surface:

- CLI:
  - `python -m portfolio_os.api.cli promotion-registry --input-root <bundle_root> --output-dir <review_output_dir>`
- outputs:
  - `promotion_registry.csv`
  - `promotion_registry_manifest.json`
  - `promotion_registry_summary.md`

Current design boundary:

- one row per promotion bundle / candidate package
- advisory only
- offline and manually triggered
- validates bundle shape, but does not recompute research thresholds

### Evidence

Implementation commit:

- `98567a3`
- `feat: add promotion registry review surface`

Fresh verification:

- `python -m pytest tests\\test_alpha_promotion_contract.py tests\\test_promotion_registry.py tests\\test_e2e_cli.py::test_promotion_registry_cli_produces_expected_outputs -q`
  - `5 passed`
- `python -m py_compile src\\portfolio_os\\alpha\\promotion_contract.py src\\portfolio_os\\workflow\\promotion_registry.py src\\portfolio_os\\api\\cli.py`
  - passed

Real archived-research smoke test:

- input bundle:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\outputs\ashare_stage3_promotion_bundle`
- output root:
  - `C:\Users\14574\Quant\PortfolioOS\outputs\promotion_registry_smoke_2026-04-15_ashare_stage3`

Observed reviewer-facing result:

- bundle id:
  - `ashare_stage3_candidate_2026-04-08`
- research line:
  - `ashare`
- candidate status:
  - `stage3_candidate_not_promoted`
- signals:
  - `anti_mom_21_5`
  - `institutional_crowding`
- Stage 4 eligibility:
  - `False`
- blocking reason:
  - shared-sample uplift was positive, but second-half IR did not beat the best single signal

### Why It Matters

This is a good hiring-market artifact because it demonstrates that research outcomes are not just notebooks or narrative notes.

They are:

- packaged
- validated
- reviewable
- and consumable by the platform without merging exploratory branch code into the main repo

That is a stronger story than "I wrote a backtest."

## Asset 2: Paper Calibration Sprint

### Problem

The platform had execution simulation, TCA, and an Alpaca paper adapter, but it still lacked a clean way to compare:

- a dedicated pre-trade market reference
- realized paper fills
- and reconciliation artifacts

Without that, any simulator-vs-paper discussion risked using broker-side state as both the benchmark and the realized result.

### What Was Built

Paper-calibration lane:

- dry-run and live neutral paper path via:
  - `python -m portfolio_os.api.cli paper-calibration ...`
- repeated tranche support via:
  - `--repeat N --interval-seconds X`
- offline drift aggregation via:
  - `python -m portfolio_os.api.cli paper-calibration-aggregate ...`
- pre-registered calibration note discipline for early samples

Strategy boundary:

- deliberately neutral
- `SPY`
- `1` share
- platform calibration only
- not an alpha test

### Evidence

Canonical single live read:

- root:
  - `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_live_2026-04-15_v3`

Observed:

- fill rate:
  - `100%`
- partial fills:
  - `0`
- rejected orders:
  - `0`
- dedicated pre-trade reference snapshot:
  - `captured_ticker_count = 1`
  - `fallback_reference_count = 0`
- quoted mid:
  - `697.33`
- fill price:
  - `697.36`
- reconciliation:
  - `matched_count = 12`
  - `mismatched_count = 0`

Repeated tranche proof-of-life:

- root:
  - `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-15_session1`
- aggregate:
  - `Run Count = 3`
  - `Observation Count = 3`
  - reference source mix stayed on the happy path
  - all fills remained complete

Interpretation boundary:

- session1 proves the repeated artifact chain works
- session1 does **not** justify any drift-regime conclusion yet

### Why It Matters

This is useful as a portfolio piece because it shows execution realism discipline:

- separate alpha validation from execution validation
- capture an independent pre-trade reference
- quantify drift only after the reference path is trustworthy
- keep early-sample interpretation constrained

That is exactly the kind of thing a buy-side reviewer can trust more than a backtest screenshot.

## Honest Boundaries

This project still does **not** claim:

- a production-ready alpha package
- a live promotion engine
- a calibrated execution model from only three paper fills
- a reason to start RL before the alpha layer clears its own gate

Those boundaries are part of the strength of the project, not a weakness.

## Five-Minute Interview Version

If this project needed to be explained quickly, the cleanest talk track is:

1. I built a portfolio and execution research platform, not just a backtest.
2. I separated exploratory research from platform promotion with a bundle-based contract.
3. I added a reviewer-facing registry so archived research can be inspected without merging branch-local code.
4. I also built a neutral paper-calibration lane so simulator assumptions can be checked against real paper fills without confusing execution quality with alpha.
5. The project is strongest in governance, evidence discipline, and honest negative preservation, not in overclaiming deployment readiness.

## Next Good Demonstrations

The next artifacts that would strengthen this story further are:

- a second real promotion bundle passing through the same registry surface
- a `paper-calibration` tranche reaching `N >= 30` with a first real drift-regime read
- a compact reviewer note that links research bundle state and execution-calibration state into one platform review pack
