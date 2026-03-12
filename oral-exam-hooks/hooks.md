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

### Hook T4: Fixed-region simplicity sacrificed repair flexibility
- Tradeoff: We deliberately assumed a fixed set of regions to keep the prototype understandable, at the cost of dynamic repair and rebalancing.
- Why we made it: Dynamic membership and fragment migration add major complexity.
- Failure-first trace if permanent region loss occurs:
  1) A region permanently fails
  2) Surviving regions still satisfy threshold temporarily
  3) No new region is added and no re-encoding occurs
  4) A later region loss makes the object unrecoverable
- What to observe:
  - live-region count over time
  - objects near minimum survivable threshold
  - repair backlog or repair-out-of-scope incidents