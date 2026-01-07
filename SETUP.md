# Setup checklist (CPSC 416 student repo)

## 0) Create the local environment (recommended)

```bash
uv sync
```

## 1) Enable hooks (required)

```bash
git config core.hooksPath githooks
```

What this does:
- Rejects pushes with unsigned commits.
- Updates `capstone/team-pointer.json` automatically when possible.

## 2) Enable commit signing (required)

Either GPG or SSH signing is acceptable; pick one and use it consistently.

### Option A: GPG signing

Configure Git to sign commits:

```bash
git config --global commit.gpgsign true
git config --global gpg.program gpg
```

Set your signing key:

```bash
git config --global user.signingkey <YOUR_GPG_KEY_ID>
```

### Option B: SSH signing

Configure Git to sign commits with SSH:

```bash
git config --global gpg.format ssh
git config --global commit.gpgsign true
git config --global user.signingkey ~/.ssh/<YOUR_SIGNING_KEY>
```

## 3) Use the evidence ledger (required)

Append an entry:

```bash
uv run python tools/evidence/append.py --kind dp1-design --notes "Initial DP1 design sketch" --artifacts design/dp1/design.md
```

Verify the chain:

```bash
uv run python tools/evidence/verify.py
```

## 4) Capstone team pointer (when applicable)

Choose one:
- Submodule path: `capstone/team/` (optional, higher Git complexity)
- URL path: put the team repo URL in `capstone/team_repo.txt`

Then the `pre-commit` hook updates `capstone/team-pointer.json` when it can determine the current team repo SHA.
