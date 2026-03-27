## Assumption hooks (EC + threshold-key KV store, CP model)

### Hook A1: Coordinator remains alive and trusted
- Assumption: The metadata coordinator does not fail or act maliciously in the base design.
- Why we need it: The coordinator is the control plane for version commit, fragment placement, authorization, and reconstruction. If it fails, clients cannot safely read or write.
- Failure-first trace if false:
  1) Client sends `PUT(k, v)` to coordinator
  2) Coordinator writes some fragments/key shares, then crashes before commit
  3) Some regions hold partial state, but no committed metadata exists
  4) Write outcome is unknown and reads must fail
- What to observe:
  - coordinator uptime / heartbeat
  - count of in-progress writes without final commit
  - metadata commit latency
  - aborted commit count

### Hook A2: At least k regions are reachable
- Assumption: At least `k` regional services are reachable for both fragments and key shares.
- Why we need it: Reads require `k` fragments and `k` key shares; writes require at least `k` successful fragment writes and `k` successful key-share writes to commit.
- Failure-first trace if false:
  1) System configured with threshold `k = 2`
  2) Two regions become unreachable, only one remains
  3) Coordinator cannot gather enough fragments or key shares
  4) Reads and writes fail
- What to observe:
  - number of live regions
  - fragment-availability count vs required `k`
  - key-share-availability count vs required `k`
  - threshold-failure logs

### Hook A3: Regional responses can be validated by metadata and integrity checks
- Assumption: Stale or corrupted fragment/key-share responses can be detected using version metadata, checksums, and commit state.
- Why we need it: Without validation, reads could mix versions, accept corrupted data, or decrypt the wrong object.
- Failure-first trace if false:
  1) Coordinator requests fragments for object version `v7`
  2) One region returns a stale fragment from `v6`
  3) Coordinator cannot distinguish it from `v7`
  4) Reconstruction fails or returns incorrect plaintext
- What to observe:
  - checksum mismatches
  - version mismatch counters
  - stale-response rejects by region
  - reconstruction failures after fragment verification

### Hook A4: No single region can reconstruct and decrypt alone
- Assumption: No single region stores enough material to both reconstruct ciphertext and recover the decryption key.
- Why we need it: This is the core confidentiality/security property of the design.
- Failure-first trace if false:
  1) Region A stores a valid ciphertext fragment and also enough key material
  2) Region A is seized or compromised
  3) Region A alone reconstructs and decrypts plaintext
  4) Confidentiality is violated
- What to observe:
  - per-region fragment assignment
  - per-region key-share assignment
  - policy checks ensuring one region never holds reconstructable ciphertext and full key access
  - audit logs for plaintext release attempts

### Hook A5: Duplicate client writes are deduplicated
- Assumption: The coordinator tracks `(clientId, requestId)` and treats repeated write attempts idempotently.
- Why we need it: Retries are expected under timing failures; without deduplication, the same logical write could be committed multiple times.
- Failure-first trace if false:
  1) Client sends `PUT(k, v)` with request ID `r1`
  2) Coordinator commits, but response is delayed or lost
  3) Client retries `PUT(k, v)` with the same logical request
  4) Coordinator commits again as a new write
- What to observe:
  - duplicate `(clientId, requestId)` detections
  - deduplicated write count
  - repeated commit attempts for the same client request
- Implementation note: Uses sliding-window dedup (one entry per client, latest seqNum only). Stale replays (seqNum < latest) return the cached response. test17 forces this code path by blocking coordinator→client link so retries hit the dedup cache.

### Hook A6: Clients authenticate via challenge-response before operating
- Assumption: Only authenticated clients with valid session tokens can read or write data.
- Why we need it: Without auth, any node (including a compromised region) could impersonate a client and read plaintext.
- Discovery: This was a **reactive discovery**, not a planned feature. The design doc said "clients are untrusted" but the initial implementation had zero auth. We realized mid-implementation that any node could send a ReadRequest and get full plaintext back.
- Failure-first trace if false:
  1) Malicious node sends `ReadRequest(key)` to coordinator
  2) Coordinator reconstructs ciphertext, gathers key shares, decrypts
  3) Coordinator returns plaintext to the malicious node
  4) Confidentiality violated without any authentication
- What we implemented: HMAC-SHA256 challenge-response handshake. Coordinator sends random nonce, client proves identity with `HMAC(sharedSecret, nonce)`, coordinator issues session token. All subsequent requests carry the token. Shared secret never travels over the wire.
- What to observe:
  - auth failures (wrong credentials permanently locked out — test21)
  - session token validation on every request
  - per-key ownership enforcement (test14, test15)

### Hook A7: Per-key ownership is permanent (first writer wins)
- Assumption: The first client to commit a write to a key becomes its permanent owner. No transfer, no revocation.
- Why we need it: Simplifies authorization — no need for ACLs, groups, or ownership transfer protocols.
- Failure-first trace if false:
  1) Client A writes key K, becomes owner
  2) Client A is decommissioned (crashes permanently)
  3) No other client can ever write or read key K again
  4) Data is permanently locked to a dead client
- What to observe:
  - ownership assignments in coordinator's keyOwner map
  - ACCESS_DENIED responses for non-owners
  - no mechanism to transfer or revoke ownership
- Open question: What happens when a client is permanently decommissioned? The key is locked forever. A production system would need admin-level ownership transfer.

### Hook A8: Region liveness is tracked deterministically
- Assumption: Region liveness uses a logical heartbeat counter, not wall-clock time.
- Why we need it: System.currentTimeMillis() is non-deterministic and breaks the dslabs framework's deterministic model checking (BFS/DFS). This was discovered when search tests produced inconsistent results.
- Failure-first trace if false:
  1) Search test (BFS) explores message orderings
  2) isRegionAlive() calls System.currentTimeMillis() — returns different values on each run
  3) BFS explores different states on each execution
  4) Model checking is non-deterministic — bugs may appear or disappear between runs
- What we implemented: `missedHeartbeats` counter per region. Incremented each heartbeat cycle, reset to 0 on reply. Region dead if counter >= 2. Fully deterministic.
- What to observe:
  - missedHeartbeats values per region
  - fast-fail when alive count < threshold (test18)
  - heartbeats disabled in search tests to prevent infinite state expansion

---

## Tradeoff hooks

### Hook T1: Availability sacrificed for consistency
- Tradeoff: We deliberately sacrificed availability to preserve strong consistency and atomic visibility.
- Why we made it: We do not want clients to observe partially written or unreconstructable object versions.
- Failure-first trace if we chose the opposite:
  1) Coordinator allows best-effort commit with fewer than `k` successes
  2) Metadata exposes the new version
  3) Later read cannot gather enough fragments or key shares
  4) The object is visible but unreadable
- What to observe:
  - rejected reads/writes during partitions
  - count of operations denied due to threshold failure
  - absence of committed-but-unreadable versions

### Hook T2: Simplicity sacrificed for confidentiality
- Tradeoff: We deliberately sacrificed architectural simplicity to preserve cross-region confidentiality.
- Why we made it: Full replication is simpler, but one region would then hold a complete object copy.
- Failure-first trace if we chose full replication:
  1) Object replicated in full to multiple regions
  2) One region is compromised or seized
  3) That region alone reads the full plaintext
  4) Confidentiality fails
- What to observe:
  - no single region stores full plaintext
  - fragment-only regional storage
  - distributed key-share placement

### Hook T3: Latency sacrificed for controlled plaintext release
- Tradeoff: We deliberately sacrificed read latency to preserve controlled plaintext release.
- Why we made it: Reads require both ciphertext reconstruction and threshold key-share recovery.
- Failure-first trace if we chose lower-latency local plaintext caching:
  1) A region caches or stores plaintext locally for fast reads
  2) That region is compromised
  3) A local attacker obtains plaintext without multi-region cooperation
  4) Confidentiality fails
- What to observe:
  - read latency split into fragment fetch / key-share fetch / decrypt stages
  - no plaintext at rest in a single region
  - audit logs for key-share access and plaintext release

### Hook T4: Fixed-region simplicity sacrificed repair flexibility (partially addressed)
- Tradeoff: Originally assumed a fixed set of regions. Later implemented dynamic join (add-only) to improve fault tolerance.
- Why we changed: Fixed regions mean fault tolerance can never improve after deployment. Dynamic join allows adding regions to increase parity (m) without re-encoding existing data.
- What we implemented: Region sends authenticated JoinRequest → coordinator pauses new writes (RECONFIGURING) → in-flight writes drain → m incremented, region added → writes resume with new config. Per-version metadata preserves backward compatibility (old keys decoded with original k/m). Reconfiguration timeout (2s) aborts join if writes don't drain.
- What we deliberately did NOT implement:
  - Region removal (would orphan fragments for keys written to the departed region)
  - Data re-encoding on join (would require background migration — read every key, re-encode, redistribute while serving requests)
  - Concurrent joins (sequential only — second join rejected with RECONFIG_IN_PROGRESS)
- Failure-first trace if permanent region loss occurs:
  1) A region permanently fails
  2) Surviving regions still satisfy threshold temporarily
  3) A new region can be added (dynamic join) to restore parity
  4) But old data on the failed region is NOT migrated to the new region
  5) Old keys have fewer parity fragments than new keys
- What to observe:
  - region count before/after join (test22: 3→4)
  - message cost increase after join (test35: 14.2→18.0 msgs/op, +4 per region)
  - old keys still readable after join with per-version decoding (test23)
  - fault tolerance improvement: survive 2 failures after join vs 1 before (test24)

### Hook T5: Write-time integrity sacrificed for reliability under message loss
- Tradeoff: We deliberately deferred integrity detection from write-time to read-time to preserve reliability in unreliable networks.
- Why we made it: The design doc said "regional service rejects duplicate writes." But when acks are lost and the coordinator retransmits, regions that already stored the fragment would reject the retransmission — preventing the coordinator from ever getting the ack it needs. We changed to idempotent acks (ack success if already stored).
- Failure-first trace showing the cost:
  1) Region stores a fragment, acks success
  2) Region silently corrupts the stored fragment (bit flip, disk error)
  3) Coordinator retransmits (ack was lost), region acks success again (idempotent — data "exists")
  4) Corruption is NOT detected at write time
  5) On later read, coordinator fetches fragment, SHA-256 checksum fails, fragment rejected
  6) If enough regions are corrupted (> m), data is unrecoverable
- What to observe:
  - checksum failures on read (test19: corrupted fragment detected and rejected)
  - compound failure: corruption + partition (test20: read fails gracefully when only 1 valid fragment < k)
  - absence of write-time integrity checks after idempotent ack change

### Hook T6: Coordinator confidentiality relaxed during in-flight writes
- Tradeoff: We temporarily store Shamir key shares and erasure-coded fragments in PendingWrite at the coordinator for retransmission.
- Why we made it: When acks are lost, the coordinator needs to re-send fragments/shares to unacked regions. Without storing them, the coordinator would need to re-encrypt and re-encode from scratch (but it already discarded the plaintext and AES key).
- Design doc contradiction: The design doc says "AES key never stored at coordinator." We relaxed this — the coordinator holds key shares (not the full key, but threshold shares are enough to reconstruct it) during the write window (~300ms). Shares are cleared on commit.
- Failure-first trace showing the cost:
  1) Write begins, coordinator stores fragments + key shares in PendingWrite
  2) Coordinator is compromised during the write window
  3) Attacker has threshold key shares + k fragments = can reconstruct and decrypt
  4) Confidentiality violated during the write window
- Mitigating factor: The coordinator already held the plaintext moments earlier (it encrypted it). The marginal exposure is extending from "one method call" to "until commit" (~300ms).
- What to observe:
  - PendingWrite.fragments/keyShares cleared on commit
  - retransmission to unacked regions on client retry

### Hook T7: Server-side request buffering sacrificed for simplicity
- Tradeoff: We chose BUSY rejection + client-side retry instead of coordinator-side request queuing.
- Why we made it: The design doc suggested "buffering client requests... respond busy when we reach a set limit." We evaluated this and rejected it.
- Why buffering was rejected:
  - Queue needs a size cap (unbounded = memory growth)
  - Queue needs timeout cleanup (what if the first write fails? drain queue with failures?)
  - Queue needs stale-entry detection (what if queued client disconnects?)
  - Queue interacts with ownership/auth checks
  - All this complexity to save one retry round-trip (~100ms)
- What we do instead: Client gets BUSY, ignores it (transient error), retry timer re-sends after 100ms (with exponential backoff). When the pending write commits, the next retry succeeds. The client IS the queue.
- What to observe:
  - BUSY responses for different-client same-key contention (test10)
  - same-client retries handled by re-fan-out, not BUSY

### Hook T8: Data re-encoding sacrificed for join simplicity
- Tradeoff: When a new region joins, old keys keep their original erasure coding config. No background migration.
- Why we made it: Re-encoding old data would require: read every key, re-encode with new k/m, redistribute fragments to all regions (including new one), handle concurrent reads/writes during migration. This is 200+ lines of complex, bug-prone code.
- What we do instead: VersionMetadata stores per-version k, m, keyThreshold. Old versions are decoded with their original ErasureCoder. New versions use the current (larger) config. The coordinator creates a temporary ErasureCoder if the version's config differs from the global config.
- Failure-first trace showing the cost:
  1) Key "foo" written with k=2, m=1 (3 regions)
  2) 4th region joins, config becomes k=2, m=2
  3) "foo" still has fragments on only 3 regions (m=1 parity)
  4) If 2 of those 3 original regions fail, "foo" is unrecoverable
  5) Meanwhile, a key written after the join has m=2 parity and would survive 2 failures
- What to observe:
  - per-version k/m in VersionMetadata
  - old keys readable after join (test23)
  - GC correctly handles mixed-config versions (test36)

### Hook T9: Search test configuration fidelity sacrificed for tractability
- Tradeoff: Search tests use k=1, m=1 (2 regions) instead of the real k=2, m=1 (3 regions), and disable all timers.
- Why we made it: BFS explores ALL message orderings. With k=2, m=1: each write generates 6 messages to regions + 6 acks = state space explodes exponentially. Heartbeat timers (every 500ms) create infinite branches. Auth retry timers add further noise.
- What we do instead:
  - Smaller config (k=1, m=1) reduces fan-out from 6 to 4 messages per direction
  - `deliverTimers(false)` eliminates timer-driven state explosion
  - Client proactively re-sends pending command on auth completion (no retry timer needed for progress)
  - DFS for larger workloads (write+read), BFS for smaller ones (single write)
  - One search test (test30) uses the REAL k=2, m=1 config with DFS to verify actual erasure coding
- Failure-first trace showing the cost:
  1) Search tests use k=1, m=1 — losing 1 fragment is fatal (no fault tolerance)
  2) A bug that only manifests under k=2 fault tolerance could be missed
  3) Timer-dependent bugs are not found by search (timers disabled)
- Mitigating factor: Run tests use k=2, m=1 with real timers and cover fault tolerance scenarios. Search tests verify protocol correctness under all orderings. Together they provide complementary coverage.
- What to observe:
  - search test state counts and completion times
  - BFS exhaustive for small workloads, DFS for larger ones

### Hook T10: Read deduplication sacrificed for simplicity
- Tradeoff: Writes have AMO (at-most-once) dedup. Reads do not.
- Why we made it: Reads are idempotent — executing a read twice returns the same result without changing state. The "exactly-once semantics" claim in the design doc applies to writes (where duplicate execution creates duplicate versions), not reads.
- Failure-first trace showing the cost:
  1) Client sends ReadRequest, coordinator starts PendingRead, fans out to regions
  2) ReadResponse is lost, client retries with same seqNum
  3) Coordinator sees pending read for same client/seqNum, re-requests from unresponsive regions (handled)
  4) OR: ReadTimeoutTimer fires, PendingRead removed, retry starts a NEW read (redundant work)
- What to observe:
  - redundant fragment fetch + reconstruction on retried reads (wasted computation, not a correctness issue)
  - no cached read responses (unlike write dedup)

### Hook T11: Region removal sacrificed for data safety
- Tradeoff: Dynamic membership supports only region addition, never removal.
- Why we made it: Removing a region would orphan fragments and key shares for every key that was written when that region was part of the configuration. Old VersionMetadata references the departed region's index. Without data migration to a replacement region, those keys lose a fragment permanently — potentially below the recovery threshold.
- Failure-first trace if we allowed removal:
  1) Key "foo" written to regions [R0, R1, R2] with k=2, m=1
  2) R2 is removed from the cluster
  3) "foo" now has fragments on only R0, R1 — exactly k=2, no parity
  4) If R0 or R1 fails, "foo" is unrecoverable
- What to observe:
  - only JoinRequest supported, no LeaveRequest
  - region list only grows, never shrinks

### Hook T12: Confirmed GC sacrificed for simplicity
- Tradeoff: Garbage collection uses fire-and-forget delete messages to regions, not a confirmed-delete protocol.
- Why we made it: Confirmed deletes (wait for ack from every region) would block if a region is down. We'd trade one memory leak (orphaned fragments) for a blocked GC process. The orphaned fragments are harmless — unreachable without coordinator metadata.
- Alternatives considered:
  - Confirmed deletes with acks: blocks on downed regions
  - Region-initiated reconciliation: region periodically asks "is this version still needed?"; adds cursor management, rate limiting, coordinator load
  - Piggyback on heartbeats: adds payload to every heartbeat, slow full scan
- What we do instead: Coordinator deletes metadata locally on commit/timeout, sends DeleteVersionData to regions. If lost, region keeps orphaned data.
- What to observe:
  - GC verified: 20 overwrites → 1 version retained (test33, getVersionCount())
  - uncommitted versions cleaned on timeout (test34)
  - GC across config changes after dynamic join (test36)

### Hook T13: Channel encryption sacrificed (framework limitation)
- Tradeoff: No TLS or channel encryption. Messages are plaintext on the dslabs network.
- Why: The dslabs framework uses an in-memory message bus with no wire protocol. There's no socket to wrap with TLS. Our HMAC challenge-response proves client identity at the application layer, but a passive observer can see message contents.
- Failure-first trace showing the cost:
  1) Client sends ReadRequest with valid session token
  2) Coordinator returns ReadResponse containing decrypted plaintext
  3) A passive observer on the network sees the plaintext in transit
  4) Confidentiality violated despite authentication
- What to observe:
  - session tokens transmitted in plaintext (could be stolen)
  - fragment data visible in transit between coordinator and regions
  - production would use mutual TLS on all channels

### Hook T14: Exponential backoff was initially skipped, added after observation
- Tradeoff: Initially used fixed 100ms retry intervals. Changed to exponential backoff (100ms→2000ms cap) after observing wasted messages during sustained failures.
- Why we changed: During heartbeat fast-fail testing (test18), the client retried every 100ms against dead regions for 1.5+ seconds. Each retry generated a RECONFIGURING or INSUFFICIENT_REGIONS response that the client ignored. Fixed retries at 10/sec create unnecessary coordinator load.
- What we implemented: Retry interval doubles each attempt: 100→200→400→800→1600→2000ms (cap). Resets to 100ms on each new command. Auth retry uses the same pattern.
- What to observe:
  - retry rate under sustained failure: 10/sec (fixed) → 0.5/sec (backoff) = 20× reduction
  - backoff progression visible in client logs ("backoff=200ms", "backoff=400ms", etc.)

### Hook T15: Blocking auth sacrificed for framework compatibility
- Tradeoff: Client sendCommand() originally blocked until authentication completed. Changed to optimistic send + retry.
- Why we changed: The dslabs framework calls sendCommand() synchronously during addClientWorker() setup, BEFORE the message delivery loop starts. Blocking on auth created a startup ordering deadlock: sendCommand waits for auth → auth needs message delivery → message delivery needs system start → system start needs setup complete → setup blocked in sendCommand.
- What we implemented: sendCommand() sends immediately with whatever token is available (null if unauthenticated). Coordinator rejects with AUTH_REQUIRED; client ignores this transient error. Client retry timer re-sends after auth completes. Additionally, client proactively re-sends pending command when handleAuthResultMsg sets authenticated=true.
- Failure-first trace of the original design:
  1) addClientWorker() creates client, calls init() (sends AuthRequest), then calls sendCommand()
  2) sendCommand() blocks: `while (!authenticated) wait()`
  3) Auth messages are queued but system hasn't started → no delivery
  4) Setup never completes → system never starts → deadlock
- What to observe:
  - AUTH_REQUIRED responses in early message logs (before auth completes)
  - proactive re-send visible in logs after "Authenticated, token=..."
  - no blocking in sendCommand()