# CPSC 416 — Student work repo

This repository is the single locus of truth for your course work artifacts: designs, evidence, reflections, and (if applicable) capstone contributions.

## Required: signed commits

All commits that you push must be signed (`git commit -S`). Either GPG or SSH signing is acceptable.

This repo ships with a `pre-push` hook that rejects pushes containing unsigned commits.

## Evidence chain (anti-Goodhart)

Your grade depends on evidence-bearing iteration, not polished prose. Use:
- `evidence/ledger.jsonl` as an append-only index of evidence events (hash-chained).
- `ai/LOG.md` (or `ai/log.jsonl`) for a lightweight GenAI decision log that links to commits.

## Capstone team work

Team work lives in a separate team repo. This repo records your current team HEAD as part of your evidence chain:
- Use `capstone/team/` as a submodule (optional), or
- Put the team repo URL in `capstone/team_repo.txt`.

The `pre-commit` hook attempts to update `capstone/team-pointer.json` automatically.

## Setup (required)

1. Enable hooks:
   - `git config core.hooksPath githooks`
2. Make sure your commits are signed:
   - GPG: configure `gpg` signing (or)
   - SSH: configure SSH signing

## Directory map

- `ai/` — GenAI interaction log(s)
- `design/` — DP1/DP2 design artifacts + reflections
- `capstone/` — individual capstone contributions + team pointer
- `evidence/` — append-only evidence ledger
- `tools/` — helper scripts (append/verify evidence ledger)
- `githooks/` — hooks enforcing signed-push + auto pointer updates

