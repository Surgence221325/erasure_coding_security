# Git hooks (CPSC 416 template)

Enable hooks:

```bash
git config core.hooksPath githooks
```

Hooks provided:
- `pre-commit`: updates `capstone/team-pointer.json` when possible.
- `pre-push`: rejects pushes that include unsigned commits; optionally verifies the evidence ledger chain.

