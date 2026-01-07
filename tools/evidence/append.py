#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field


TOOL_VERSION = "cpsc416-evidence-ledger/1"
GENESIS = "GENESIS"
LEDGER_FORMAT_VERSION = "1"


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


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _canonical_json(obj: object) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _last_entry(path: Path) -> dict | None:
    if not path.exists():
        return None
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not lines:
        return None
    return json.loads(lines[-1])


def main() -> None:
    ap = argparse.ArgumentParser(description="Append a hash-chained entry to evidence/ledger.jsonl")
    ap.add_argument("--ledger", default="evidence/ledger.jsonl")
    ap.add_argument("--kind", required=True, help="e.g., dp1-design, dp1-review, dp2-impl, capstone-weekly")
    ap.add_argument("--notes", default="", help="Short note (one sentence).")
    ap.add_argument("--artifacts", nargs="*", default=[], help="Paths to artifacts in this commit/repo.")
    ap.add_argument("--commit", default="HEAD", help="Commit SHA to reference (default: HEAD).")
    ap.add_argument("--course-id", default=None, help="Optional course identifier for cross-term disambiguation.")
    ap.add_argument("--assignment-id", default=None, help="Optional assignment identifier (e.g., dp1-design, wr3).")
    ap.add_argument("--student", default=None, help="Optional stable student identifier (e.g., GitHub username).")
    args = ap.parse_args()

    ledger = Path(args.ledger)
    ledger.parent.mkdir(parents=True, exist_ok=True)

    commit = _git("rev-parse", args.commit)
    try:
        parent = subprocess.check_output(
            ["git", "rev-parse", f"{commit}^"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        parent = None
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    prev = _last_entry(ledger)
    prev_hash = prev.get("entry_hash") if prev else GENESIS
    prev_seq = int(prev.get("seq")) if prev and prev.get("seq") is not None else 0
    seq = prev_seq + 1

    student = args.student
    if student is None:
        # Best-effort fallback; students can override with --student.
        try:
            student = _git("config", "user.email") or None
        except subprocess.CalledProcessError:
            student = None

    entry_no_hash = LedgerEntry(
        timestamp_utc=now,
        ledger_format_version=LEDGER_FORMAT_VERSION,
        seq=seq,
        course_id=args.course_id,
        assignment_id=args.assignment_id,
        student=student,
        tool_version=TOOL_VERSION,
        kind=args.kind,
        commit=commit,
        parent_commit=parent,
        artifacts=list(args.artifacts),
        notes=args.notes,
        prev_hash=prev_hash,
        entry_hash="",
    )

    payload = entry_no_hash.model_dump()
    payload.pop("entry_hash", None)
    entry_hash = _sha256_hex(_canonical_json(payload))

    entry = entry_no_hash.model_copy(update={"entry_hash": entry_hash})

    with ledger.open("a", encoding="utf-8") as f:
        f.write(_canonical_json(entry.model_dump()) + "\n")

    print(f"Appended ledger entry kind={args.kind} commit={commit[:12]} hash={entry_hash[:12]}")


if __name__ == "__main__":
    main()
