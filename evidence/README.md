# Evidence ledger

`ledger.jsonl` is an append-only log of evidence events. Each line is a JSON object.

The ledger is hash-chained to make tampering obvious:
- each entry includes `prev_hash` (hash of previous entry)
- each entry includes `entry_hash` (hash of this entry)

Use `tools/evidence/append.py` to add entries and `tools/evidence/verify.py` to verify the chain.

## Hash convention (deterministic)

`entry_hash` is computed as:
- take the full entry fields *excluding* `entry_hash`
- serialize using the tool’s canonical JSON (`json.dumps(sort_keys=True, separators=(",", ":"))`)
- compute SHA-256 over that canonical JSON string

The first entry uses `prev_hash = "GENESIS"`.

## Sequencing convention

Each entry includes:
- `seq` (monotonically increasing integer, starting at `1`)
- `ledger_format_version` (currently `"1"`)
