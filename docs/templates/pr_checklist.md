# PR Checklist (TDD + SDD)

Copy into PR description and fill all sections.

## 1. Change Summary

- What changed:
- Why this change is needed:

## 2. Spec / Contract

- [ ] Task spec exists (link or pasted section)
- [ ] CLI contract changes documented
- [ ] Output artifact/schema changes documented
- [ ] Backward compatibility impact evaluated

## 3. Test Evidence

- [ ] New tests added for new behavior
- [ ] Negative-path test added
- [ ] Boundary test added
- [ ] Cross-script passthrough test added (if relevant)
- [ ] Full pytest run completed and green

Commands run:

```bash
py -3 -m pytest -q
```

## 4. Reliability And Safety

- [ ] Subprocess Python calls use `sys.executable`
- [ ] Paths use `pathlib.Path`
- [ ] Timeout/retry semantics covered by tests (if relevant)
- [ ] Missing optional fields degrade gracefully (`N/A`), no crash

## 5. Rollout Notes

- Risk level: `low | medium | high`
- Monitoring artifacts to inspect after merge:
- Rollback plan:

## 6. Exceptions (if any)

- Which standard step was skipped:
- Why:
- Follow-up task and due date:

