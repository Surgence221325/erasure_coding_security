#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from pydantic import BaseModel, Field


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _canonical_json(obj: object) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class LedgerEntry(BaseModel):
    timestamp_utc: str
    ledger_format_version: str
    seq: int
    course_id: str | None = None
    assignment_id: str | None = None
    student: str | None = None
    tool_version: str
    kind: str
    commit: str
    parent_commit: str | None = None
    artifacts: list[str] = Field(default_factory=list)
    notes: str = ""
    prev_hash: str
    entry_hash: str


def main() -> None:
    ap = argparse.ArgumentParser(description="Verify hash chain in evidence/ledger.jsonl")
    ap.add_argument("--ledger", default="evidence/ledger.jsonl")
    args = ap.parse_args()

    ledger = Path(args.ledger)
    if not ledger.exists():
        print("OK (no ledger file)")
        return

    lines = [ln for ln in ledger.read_text(encoding="utf-8").splitlines() if ln.strip()]
    prev_hash = "GENESIS"
    expected_seq = 1
    for i, ln in enumerate(lines, start=1):
        entry = LedgerEntry.model_validate_json(ln)
        if entry.seq != expected_seq:
            raise SystemExit(f"FAIL line {i}: seq mismatch (got {entry.seq}, expected {expected_seq})")
        got_prev = entry.prev_hash
        if got_prev != prev_hash:
            raise SystemExit(f"FAIL line {i}: prev_hash mismatch (got {got_prev}, expected {prev_hash})")

        got_hash = entry.entry_hash

        tmp = entry.model_dump()
        tmp.pop("entry_hash", None)
        expected = _sha256_hex(_canonical_json(tmp))
        if got_hash != expected:
            raise SystemExit(f"FAIL line {i}: entry_hash mismatch (got {got_hash}, expected {expected})")

        prev_hash = got_hash
        expected_seq += 1

    print(f"OK ({len(lines)} entries)")


if __name__ == "__main__":
    main()
