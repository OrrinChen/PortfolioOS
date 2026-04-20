# Test Design Template

Use this when adding non-trivial behavior to L1/L2 code.

## Unit Under Test

- Module/file:
- Behavior under test:

## Risks

- Primary failure risk:
- Regression risk:
- Platform risk (Windows/Linux/macOS):

## Cases

1. Happy path  
Input:
Expected:

2. Failure path  
Input:
Expected:

3. Boundary path  
Input:
Expected:

## Mocks / Fixtures

- External dependencies mocked:
- Filesystem artifacts used:

## Determinism Notes

- Time/random/network controls:
- Why test is deterministic:

## Assertions

- Core functional assertions:
- Contract/schema assertions:
- Error semantics assertions:

