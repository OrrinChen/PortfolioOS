# Typed Alpha View Contract

This project area defines the Phase 35 AlphaView contract.

AlphaView is a typed predictive claim. It is not a static `alpha_score`, an
order list, a broker artifact, a live-performance record, or a trading
recommendation.

## Contract Boundary

AlphaView records:

- mechanism type: event, state transition, fixed horizon, or residual factor
- signal, visibility, tradable, and optional event-anchor timestamps
- horizon type and holding-window semantics
- decay, coverage, confidence, capacity, and cost-sensitivity views
- explicit abstain policy
- PIT safety report
- provenance

The core semantic rule is:

```text
no_view != zero_alpha
```

`no_view` means the system has no valid predictive view for a symbol. It must
carry an explicit reason and cannot carry an expected-return value. A zero
expected-return value is allowed only as an `active_view` value when the model
has a real neutral view.

## Fixtures

Valid fixtures:

- `examples/valid/valid_event_sue_alpha_view.json`
- `examples/valid/valid_revision_to_next_announcement_alpha_view.json`
- `examples/valid/valid_residual_momentum_calibration_alpha_view.json`

Rejected fixture:

- `examples/rejected/rejected_forward_return_leakage_alpha_view.json`

The rejected fixture demonstrates why forward-return labels cannot appear in
AlphaView payloads.

Phase 36 event-aware fixtures:

- `examples/event_sue_pead_view.json`
- `examples/event_revision_view.json`

These fixtures feed the local event-evidence contract kernel. The kernel can
write deterministic evidence-plan artifacts for SUE event windows and analyst
revision to-next-announcement horizons, but it does not compute realized alpha
performance or generate trading instructions.

Phase 37 projection:

`src/portfolio_os/alpha/projection.py` can project these typed views into a
rebalance-period expected-return panel plus a projection manifest, diagnostics,
and explicit abstain report. Missing coverage is never emitted as a synthetic
zero expected return.

## Validation

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_view_contract.py -q
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_event_alpha_evaluation_contract.py -q
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_projection_bridge_v2.py -q
```
