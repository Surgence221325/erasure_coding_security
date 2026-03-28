# Capstone: Distributed KV Store with Erasure Coding & Raft Consensus

A distributed key-value store built on the dslabs framework. Features:
- **Reed-Solomon erasure coding** over GF(256) — data split into k+m fragments
- **Shamir secret sharing** — AES key split across regions (threshold reconstruction)
- **AES-128/CBC encryption** — per-version encryption with fresh keys
- **Challenge-response authentication** — HMAC-SHA256 handshake, session tokens
- **Per-key ownership** — first writer owns the key
- **Dynamic region membership** — regions join at runtime, m increases
- **Raft coordinator replication** — 3 coordinators, leader election, log replication, strict reads
- **Garbage collection** — old versions cleaned on commit, fire-and-forget region deletes
- **Deterministic search tests** — BFS/DFS model checking alongside run tests

## Requirements

- Java 17 (pinned via `gradle.properties`)
- Python 3 (for test runner)

## Build

```bash
cd capstone/project
./gradlew assemble
```

## Run Tests

```bash
# All tests (run + search, ~3min):
cd capstone/project/build/handout
python3 run-tests.py --lab capstone

# Run tests only (~1min):
python3 run-tests.py --lab capstone --no-search

# Search tests only (~2min):
python3 run-tests.py --lab capstone --no-run
```

**Note:** Raft failover tests (test37-44) occasionally flake (~5% of runs) due to non-deterministic message delivery timing in the framework. The election + client redirect + re-authentication chain sometimes exceeds the Thread.sleep window. The correctness of the Raft protocol is verified deterministically by the DFS search test (test45). If a Raft test fails, re-run.

## One-Liner

```bash
cd capstone/project && ./gradlew assemble && cd build/handout && python3 run-tests.py --lab capstone --no-search
```

## Test Suite (46 tests)

| Category | Tests | What's verified |
|---|---|---|
| Basic correctness | test01-03 | Put/get, multiple ops, overwrite |
| Fault tolerance | test04-08 | Partition, timeout, recovery |
| Multi-client | test09-10 | Concurrent keys, BUSY rejection |
| Stress & integrity | test11-13 | Large values, 10 versions, 5 clients × 10 ops |
| Auth & liveness | test14-21 | Ownership, dedup, heartbeat fast-fail, corruption, compound failure, wrong credentials |
| Dynamic membership | test22-25 | Region join, per-version decoding, improved fault tolerance, join auth |
| Search (BFS/DFS) | test26-30 | Exhaustive message ordering verification |
| Cost measurements | test31-32 | Message count (14.2/op), storage overhead (52% of replication) |
| Garbage collection | test33-36 | Version cleanup, uncommitted GC, GC across config changes |
| Raft consensus | test37-44 | Election, failover, redirect, split brain, consistency, dedup, stress |
| Raft search | test45 | DFS safety verification with 3 coordinators |
| Raft measurement | test46 | Message overhead: 18-32/op with Raft (vs 14.2 single) |

## Architecture

```
Clients ──→ Leader Coordinator ──→ Regions (fragments + key shares)
                 │
                 ├─��→ Follower Coordinator (metadata replica)
                 └──→ Follower Coordinator (metadata replica)
```

- **Coordinator** (server 1-3 in Raft mode): handles auth, encryption, erasure coding, version management, ownership, GC. Replicated via Raft for fault tolerance.
- **Regions** (server 4-6): passive storage of erasure-coded fragments and Shamir key shares. Don't participate in Raft.
- **Client**: authenticates via HMAC challenge-response, knows all coordinator addresses for Raft failover.

## Key Source Files

| File | Purpose |
|---|---|
| `CoordinatorNode.java` | Control plane: write/read paths, auth, Raft consensus, GC, dynamic membership |
| `RegionalNode.java` | Storage: fragment/key-share persistence, idempotent writes, corruption injection |
| `CapstoneClient.java` | Client: auth handshake, command retry with backoff, multi-coordinator Raft support |
| `Messages.java` | All message types (auth, client, region, heartbeat, Raft, GC, membership) |
| `Timers.java` | All timer types with exponential backoff and deterministic Raft election |
| `ErasureCoder.java` | Reed-Solomon over GF(256) with Vandermonde matrix |
| `ShamirSecretSharing.java` | Threshold secret sharing with Lagrange interpolation |
| `StateDelta.java` | Raft log entry payloads (WRITE_COMMIT, AUTH_SESSION, CONFIG_CHANGE) |
| `LogEntry.java` | Raft log entry (term, index, delta) |
| `VersionMetadata.java` | Per-version metadata (k, m, threshold, IV, checksums) |
| `CryptoUtil.java` | Shared HMAC-SHA256 utility |
