# FD-Wide Candidate Design Manifest Audit

not alpha evidence
allocator entry: blocked
Q1 entry: blocked
Q2 entry: blocked
Alpha Registry update: blocked
production approval: not claimed

This audit scans Factor Discovery candidate output directories and requires a valid `candidate_design_manifest.json` beside each candidate or family decision artifact.

## Summary

- candidate directories: 3
- manifests found: 3
- valid manifests: 3
- blockers: 0
- decision: all_candidate_design_manifests_valid

## Blockers

- none

## Boundary

A passing FD-wide audit only allows candidate-family validation to continue inside Factor Discovery. It does not approve allocator entry, Q1, Promotion Gate, Q2, Alpha Registry updates, broker/order workflows, live trading, or production use.
