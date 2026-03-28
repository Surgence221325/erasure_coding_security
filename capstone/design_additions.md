# Design Additions: Garbage Collection & Raft Consensus

These features were added after the original design document. This document describes their design, the decisions made, and the tradeoffs accepted.

---

## 1. Garbage Collection

### Problem

Every write creates a new version. Without cleanup, the coordinator accumulates version metadata indefinitely, and regions accumulate orphaned fragments and key shares from overwritten or failed versions.

### Design

**When GC runs:**
- **On commit:** When version N+1 commits for a key, all versions < N+1 are deleted from the coordinator's `allVersions` map. Fire-and-forget `DeleteVersionData` messages are sent to regions.
- **On write timeout:** When a write fails (insufficient region acks), the uncommitted version is deleted from `allVersions` and regions are notified.

**What's deleted:**
- Coordinator: `VersionMetadata` entries (checksums, IV, k/m/threshold, commit status)
- Regions: stored fragment bytes and key share bytes for the deleted version

**Why fire-and-forget (no ack protocol):**

We evaluated four approaches:

| Approach | Pros | Cons |
|---|---|---|
| Fire-and-forget (chosen) | Simple, no blocking | Regions may keep orphaned data if delete message lost |
| Confirmed deletes (wait for acks) | Guarantees cleanup | Blocks if a region is down; trades memory leak for blocked GC |
| Region-initiated reconciliation | Self-healing | Complex (cursor management, rate limiting, coordinator load) |
| Piggyback on heartbeats | No new timers | Slow full scan, adds payload to every heartbeat |

We chose fire-and-forget because orphaned data on regions is **harmless**: without the coordinator's version metadata, no read will ever request those fragments. They waste storage but don't affect correctness or security.

**Why no in-flight read race:**

A key concern: what if we delete version 1 while a read for version 1 is in-flight? This can't happen because:
1. Per-key ownership means only one client writes/reads a key
2. The client sends one command at a time (sequential seqNums)
3. A read must complete (or timeout) before the client sends the overwriting write
4. The dslabs framework is single-threaded per node — no concurrent handler execution

In a multi-threaded production system, this would require MVCC with reference counting. Our version-based metadata design is compatible with this.

**Per-version GC with dynamic membership:**

After a region join, old keys have different k/m than new keys. GC handles this correctly: the `StateDelta` includes the list of old versions to delete. `applyLogEntry` removes them from `allVersions` and (leader only) sends deletes to the correct set of regions using the old version's k+m count, not the current global config.

**Verified by:** test33 (20 overwrites → 1 version retained, verified via `getVersionCount()`), test34 (uncommitted version cleaned on timeout), test36 (GC across config boundaries after dynamic join).

---

## 2. Raft Coordinator Replication

### Problem

The single coordinator is a single point of failure. If it crashes, all version metadata, ownership, auth sessions, and dedup state are lost. Fragments survive on regions but are unrecoverable without the coordinator's metadata.

### Design

**Architecture:**
```
Clients ──→ Leader Coordinator ──→ Regions (fragments + key shares)
                 │
                 ├──→ Follower Coordinator (metadata replica)
                 └──→ Follower Coordinator (metadata replica)
```

Three coordinator replicas. The leader handles all client requests and region communication. Followers maintain a replicated copy of the coordinator's metadata via the Raft log. If the leader crashes, a follower is elected as the new leader.

**What goes in the Raft log:**

State deltas (outcomes), not commands. The leader does all the work (encryption, erasure coding, region communication). When a write commits on regions, the leader creates a `StateDelta` describing the outcome and replicates it to followers via `AppendEntries`. Followers apply the delta to their local state without talking to regions.

This is necessary because followers can't re-execute writes — they would generate different AES keys and different fragments.

**Three delta types:**
- `WRITE_COMMIT`: key, version, VersionMetadata, owner, dedup entry, old versions to GC
- `AUTH_SESSION`: clientId, session token
- `CONFIG_CHANGE`: new region address, updated m value

**Write protocol (two-phase):**
```
1. Client → Leader: WriteRequest
2. Leader encrypts, erasure codes, fans out to regions
3. Regions ack (≥k fragments + ≥threshold shares)        ← Phase 1 (300ms timeout)
4. Leader creates StateDelta, sends AppendEntries to followers
5. Majority of coordinators ack                            ← Phase 2 (500ms timeout)
6. Leader applies to state machine, responds to client
```

Client response is deferred until BOTH phases complete. If the leader crashes between phases, the write is lost (regions have fragments but metadata isn't replicated). The client retries with the new leader.

**Read protocol (strict consistency):**

Before serving a read, the leader confirms it's still the leader by sending a Raft heartbeat round and waiting for majority acknowledgment. This prevents a partitioned old leader from serving stale reads. In single-node mode, verification is immediate (majority of 1).

**Leader election:**

Standard Raft election with one modification: **deterministic election timeouts** instead of randomized. Each coordinator gets `1000ms + 100ms × peer_index`. This avoids `Math.random()` which would break the framework's deterministic model checking. The 150ms heartbeat interval provides a 6.7× margin below the minimum election timeout.

**Log compaction:**

When the Raft log exceeds 100 entries, the leader takes a snapshot (serializes full coordinator state), discards old log entries, and sends the snapshot to followers that are too far behind via `InstallSnapshot`.

### Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Log content | State deltas, not commands | Followers can't re-execute (different AES keys) |
| Auth | Leader-only, replicated via log | Follower-issued sessions aren't replicated (Bug 11) |
| GC in Raft | Only leader sends region deletes | Followers apply to local state in `applyLogEntry` |
| Read consistency | Strict (leadership verification) | Prevents stale reads from partitioned old leader |
| Election timeout | Deterministic stagger | Framework compatibility (no Math.random) |
| Coordinator set | Fixed at startup | Joint consensus for membership changes too complex |
| Write timeout | Two-phase (300ms + 500ms) | DFS model checking found single timeout was insufficient (Bug 10) |
| Client discovery | All coordinator addresses, round-robin on failure | Client stuck on partitioned coordinator otherwise (Bug 12) |

### What Raft Changed About Existing Features

Adding Raft was not "bolt on consensus." It required rethinking how every existing feature interacts with replication:

- **Auth:** Moved to leader-only. Initially followers handled auth directly, creating sessions that weren't replicated (Bug 11).
- **GC:** Region deletes moved to `applyLogEntry` (post-Raft-commit), leader-only. Prevents deleting fragments before metadata is safely replicated.
- **Timeouts:** Write pipeline became two-phase. DFS model checking found that the single `WriteTimeoutTimer` couldn't cover both the region and Raft phases (Bug 10).
- **Client:** Now knows all coordinator addresses. Rotates after 3 consecutive auth failures. Handles `NotLeaderResponse` for fast redirect.
- **Dynamic membership:** Region join is now a Raft log entry (`CONFIG_CHANGE`). All coordinators must agree on the region list.

### Bugs Found During Raft Implementation

| Bug | How Found | Fix |
|---|---|---|
| Election instability (heartbeat/election ratio too narrow) | test37 (first Raft test) | Increased election timeout to 1000ms base (6.7× margin) |
| WriteTimeout fires before Raft commit | DFS model checking (test45) | Added separate RaftCommitTimeoutTimer (500ms) |
| Auth on follower creates unreplicable session | test42 (auth loop) | Added isLeader() check to handleAuthRequest |
| Client stuck on partitioned coordinator | test42 (timeout) | Client knows all coordinators, rotates on failure |

### Verified by

- test37: Basic election + write + read through 3 coordinators
- test38: Leader failover — new leader elected, writes continue
- test39: Client redirected from follower to leader
- test40: Partitioned leader can't make progress (no Raft majority)
- test41: Committed data readable after failover
- test42: Uncommitted write not visible on new leader (safety)
- test43: Dedup survives failover (replicated via Raft log)
- test44: Multi-write stress through Raft
- test45: DFS search — Raft safety under exhaustive message orderings
- test46: Message overhead: 18-32 msgs/op with Raft (vs 14.2 single coordinator)
