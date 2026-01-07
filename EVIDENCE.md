# Evidence expectations (CPSC 416)

The goal is correctness under a failure model. Polished artifacts are not sufficient evidence.

## What counts as evidence

- Signed git history that shows incremental construction.
- Minimal GenAI interaction log entries that capture decisions and deltas (not transcript dumps).
- Protocol evidence: failure model, invariants/safety claims, and liveness assumptions.
- Execution evidence: experiments, failure injections, logs/metrics/traces.
- Model evidence (preferred): a TLA+/PlusCal (or similar) spec that produced a counterexample or ruled something out.

## What is treated as low-signal

- One-shot “final” artifacts with no intermediate commits/experiments.
- Designs without a failure model, invariants, or an evaluation plan.
- AI transcript dumps with no decisions, deltas, or attribution.
- Claims like “it works” without a reproduction recipe and failure cases.

## Required artifacts (minimum)

- `evidence/ledger.jsonl` entries for major milestones.
- An AI log entry (and corresponding commit references) for each major design change.

