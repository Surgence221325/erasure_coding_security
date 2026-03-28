package dslabs.capstone;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * The coordinator — the replicated control plane of the distributed KV store.
 *
 * Extends dslabs.framework.Node. Message handlers follow the framework naming
 * convention (handleFoo / onFooTimer) and are dispatched via reflection.
 *
 * In single-node mode (no Raft peers), behaves as a standalone coordinator.
 * In multi-node mode (3+ coordinators), uses Raft consensus for replication.
 * Only the Raft leader handles client requests; followers redirect.
 *
 * ============================================================================
 *  CODE SECTIONS (in order of appearance)
 * ============================================================================
 *
 *  1. CONFIGURATION & STATE          — fields, constructors, init()
 *  2. AUTHENTICATION                 — challenge-response HMAC, session tokens
 *     (leader-only; replicated via Raft log as AUTH_SESSION)
 *  3. REGION LIVENESS                — deterministic heartbeat counter,
 *     fast-fail, isRegionAlive, countAliveRegions
 *  4. WRITE PATH                     — handleWriteRequest → region fan-out →
 *     tryCommitWrite → Raft replication → applyLogEntry → client response
 *     Two-phase timeout: WriteTimeoutTimer (300ms) for regions,
 *     RaftCommitTimeoutTimer (500ms) for Raft majority.
 *  5. READ PATH                      — handleReadRequest → leadership
 *     verification (strict reads) → proceedWithRead → region fan-out →
 *     per-version decoding (uses version's k/m, not global)
 *  6. REGION HEARTBEAT               — HeartbeatTimer, missedHeartbeats counter
 *  7. DYNAMIC MEMBERSHIP             — JoinRequest → reconfiguring pause →
 *     drain pending writes → Raft CONFIG_CHANGE → applyJoin
 *  8. RAFT CONSENSUS                 — election (RequestVote), log replication
 *     (AppendEntries), state machine application (applyLogEntry for
 *     WRITE_COMMIT / AUTH_SESSION / CONFIG_CHANGE), strict read verification,
 *     log compaction (snapshot + InstallSnapshot)
 *  9. TIMEOUTS                       — read/write/reconfig/Raft-commit timeouts
 * 10. AES ENCRYPTION                 — encrypt/decrypt helpers
 * 11. GARBAGE COLLECTION             — gcOldVersions, gcUncommittedVersion,
 *     fire-and-forget DeleteVersionData to regions (leader only)
 * 12. AMO DEDUP                      — sliding window per client, latest seqNum
 * 13. INTERNAL CLASSES               — PendingWrite, PendingRead, DedupEntry
 * 14. OBSERVABILITY                  — getVersionCount, getTotalVersionCount,
 *     getRegionCount (for test verification)
 *
 * ============================================================================
 *  KEY DESIGN DECISIONS
 * ============================================================================
 *
 *  - Raft consensus: state deltas (not commands) replicated. Followers don't
 *    re-execute writes — leader does all crypto/region work, replicates outcome.
 *  - Auth: leader-only. Sessions replicated via Raft log (AUTH_SESSION delta).
 *  - GC: only leader sends fire-and-forget DeleteVersionData to regions.
 *    Followers apply GC to local metadata in applyLogEntry.
 *  - Per-version metadata: each version stores its own k, m, keyThreshold.
 *    After dynamic region join, old keys decoded with original config.
 *  - Deterministic: no System.currentTimeMillis(), no Math.random().
 *    Region liveness uses logical heartbeat counter. Election timeout uses
 *    deterministic stagger (1000ms + 100ms × peer_index).
 *  - AES key shares temporarily stored in PendingWrite for retransmission
 *    to unacked regions. Cleared on commit. Tradeoff: relaxes confidentiality
 *    during write window to preserve reliability under message loss.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CoordinatorNode extends Node {

    private static final SecureRandom RNG = new SecureRandom();

    // --- Configuration (k is fixed; m and regions grow on dynamic join) ---
    private final java.util.List<Address> regions;  // ordered, grows on join
    private final int       k;                       // erasure coding: data fragments (fixed)
    private int             m;                       // erasure coding: parity fragments (grows)
    private int             keyThreshold;            // Shamir threshold (may grow)

    // Precomputed erasure coder (recreated when m changes)
    private ErasureCoder erasureCoder;

    // Whether to send periodic heartbeats (disabled during search tests
    // to prevent infinite state space expansion)
    private final boolean enableHeartbeats;

    // --- Authentication secrets (clientId -> pre-shared secret) ---
    private final Map<String, byte[]> clientSecrets;

    // --- Cluster secret for region join authentication ---
    private final byte[] clusterSecret;

    // --- Dynamic membership state ---
    private boolean reconfiguring;       // true while waiting for pending writes to drain
    private Address pendingJoinRegion;   // the region waiting to be added (null if none)

    // --- Version metadata ---
    // objectKey -> (versionNum -> VersionMetadata)
    private Map<String, Map<Integer, VersionMetadata>> allVersions;
    // objectKey -> latest committed version number
    private Map<String, Integer> latestCommitted;
    // objectKey -> next version number to assign
    private Map<String, Integer> nextVersion;

    // --- In-flight write state (one per key) ---
    private Map<String, PendingWrite> pendingWrites;

    // --- In-flight read state (one per key) ---
    private Map<String, PendingRead> pendingReads;

    // --- AMO deduplication for writes ---
    // Since each client processes commands sequentially with monotonically
    // increasing seqNums, we only need to store the LATEST (seqNum, response)
    // per client.  Any replay with seqNum <= lastProcessed is stale.
    // Reads are idempotent and don't need dedup — duplicate reads produce the
    // same result without state changes.
    // clientId -> (latestSeqNum, cachedResponse)
    private Map<String, DedupEntry> writeDedup;

    // --- Region liveness (logical heartbeat counter, fully deterministic) ---
    // Each heartbeat cycle increments a region's missed count. A heartbeat reply
    // resets it to 0. A region is considered dead if it has missed >= DEAD_THRESHOLD
    // consecutive heartbeats. This avoids System.currentTimeMillis() which would
    // break the framework's deterministic model checking.
    private static final int HEARTBEAT_DEAD_THRESHOLD = 2;
    private Map<Address, Integer> missedHeartbeats;

    // --- Authentication state ---
    // clientId -> nonce (pending challenge-response)
    private Map<String, byte[]> pendingAuths;
    // sessionToken -> clientId (validated sessions)
    private Map<String, String> validSessions;
    // clientId -> sessionToken (reverse lookup, O(1) instead of O(n) scan)
    private Map<String, String> clientToToken;

    // --- Per-key ownership (key -> owning clientId) ---
    private Map<String, String> keyOwner;

    // =========================================================================
    //  Raft consensus state
    // =========================================================================

    public enum RaftRole { LEADER, FOLLOWER, CANDIDATE }

    private final Address[] raftPeers;   // other coordinator addresses (empty = single-node)
    private RaftRole raftRole;
    private int      currentTerm;
    private Address  votedFor;           // who we voted for in currentTerm (null = nobody)
    private Address  raftLeaderAddress;  // current known leader (null if unknown)

    // Raft log (1-indexed: entry at position i has index i)
    private java.util.List<LogEntry> raftLog;

    // Index of highest log entry known to be committed (replicated on majority)
    private int commitIndex;
    // Index of highest log entry applied to state machine
    private int lastApplied;

    // Leader-only state (reinitialized on election)
    private Map<Address, Integer> nextIndex;   // per peer: next log index to send
    private Map<Address, Integer> matchIndex;  // per peer: highest replicated index

    // Votes received during current election (candidate only)
    private Set<Address> votesReceived;

    // Pending client responses waiting for Raft commit
    // logIndex -> (clientAddress, WriteResponse to send)
    private Map<Integer, PendingRaftResponse> pendingRaftResponses;

    private static final class PendingRaftResponse {
        final Address clientSender;
        final WriteResponse response;
        PendingRaftResponse(Address clientSender, WriteResponse response) {
            this.clientSender = clientSender;
            this.response     = response;
        }
    }

    // Pending auth responses waiting for Raft commit
    // clientId -> PendingAuthResponse
    private Map<String, PendingAuthResponse> pendingAuthResponses;

    private static final class PendingAuthResponse {
        final Address sender;
        final String  clientId;
        final String  token;
        PendingAuthResponse(Address sender, String clientId, String token) {
            this.sender   = sender;
            this.clientId = clientId;
            this.token    = token;
        }
    }

    // Pending reads waiting for Raft leadership verification.
    // We must confirm we're still leader (majority heartbeat ack) before serving reads.
    // readVerificationId -> PendingReadVerification
    private int nextReadVerificationId;
    private Map<Integer, PendingReadVerification> pendingReadVerifications;

    // Track heartbeat acks for the current verification round
    private int readVerifyAckCount;
    private int readVerifyTerm;  // term when verification started

    private static final class PendingReadVerification {
        final ReadRequest request;
        final Address sender;
        PendingReadVerification(ReadRequest request, Address sender) {
            this.request = request;
            this.sender  = sender;
        }
    }

    // Log compaction: snapshot state
    private int    snapshotLastIndex;
    private int    snapshotLastTerm;
    private byte[] snapshotData;

    // =========================================================================
    //  Constructors
    // =========================================================================

    public CoordinatorNode(Address address, Address[] regions, int k, int m,
                           int keyThreshold, Map<String, byte[]> clientSecrets) {
        this(address, regions, k, m, keyThreshold, clientSecrets, true, null, new Address[0]);
    }

    public CoordinatorNode(Address address, Address[] regions, int k, int m,
                           int keyThreshold, Map<String, byte[]> clientSecrets,
                           boolean enableHeartbeats) {
        this(address, regions, k, m, keyThreshold, clientSecrets, enableHeartbeats, null, new Address[0]);
    }

    public CoordinatorNode(Address address, Address[] regions, int k, int m,
                           int keyThreshold, Map<String, byte[]> clientSecrets,
                           boolean enableHeartbeats, byte[] clusterSecret) {
        this(address, regions, k, m, keyThreshold, clientSecrets, enableHeartbeats, clusterSecret, new Address[0]);
    }

    public CoordinatorNode(Address address, Address[] regions, int k, int m,
                           int keyThreshold, Map<String, byte[]> clientSecrets,
                           boolean enableHeartbeats, byte[] clusterSecret,
                           Address[] raftPeers) {
        super(address);

        // Validate configuration
        if (k < 1) throw new IllegalArgumentException("k must be >= 1, got " + k);
        if (m < 0) throw new IllegalArgumentException("m must be >= 0, got " + m);
        if (keyThreshold < 1 || keyThreshold > k + m)
            throw new IllegalArgumentException("keyThreshold must be in [1, k+m], got " + keyThreshold);
        if (regions.length != k + m)
            throw new IllegalArgumentException("regions.length must equal k+m=" + (k+m) + ", got " + regions.length);

        this.regions          = new java.util.ArrayList<>(java.util.Arrays.asList(regions));
        this.k                = k;
        this.m                = m;
        this.keyThreshold     = keyThreshold;
        this.erasureCoder     = new ErasureCoder(k, m);
        this.clientSecrets    = clientSecrets;
        this.enableHeartbeats = enableHeartbeats;
        this.clusterSecret    = clusterSecret;
        this.raftPeers        = raftPeers;
    }

    @Override
    public void init() {
        allVersions     = new HashMap<>();
        latestCommitted = new HashMap<>();
        nextVersion     = new HashMap<>();
        pendingWrites   = new HashMap<>();
        pendingReads    = new HashMap<>();
        writeDedup      = new HashMap<>();
        missedHeartbeats = new HashMap<>();
        pendingAuths    = new HashMap<>();
        validSessions   = new HashMap<>();
        clientToToken   = new HashMap<>();
        keyOwner        = new HashMap<>();
        reconfiguring   = false;
        pendingJoinRegion = null;
        pendingJoinAddress = null;

        // --- Raft state initialization ---
        raftLog               = new java.util.ArrayList<>();
        currentTerm           = 0;
        votedFor              = null;
        commitIndex           = 0;
        lastApplied           = 0;
        nextIndex             = new HashMap<>();
        matchIndex            = new HashMap<>();
        votesReceived         = new HashSet<>();
        pendingRaftResponses  = new HashMap<>();
        pendingAuthResponses  = new HashMap<>();
        nextReadVerificationId = 0;
        pendingReadVerifications = new HashMap<>();
        readVerifyAckCount = 0;
        readVerifyTerm     = 0;
        snapshotLastIndex     = 0;
        snapshotLastTerm      = 0;
        snapshotData          = null;

        if (raftPeers.length == 0) {
            // Single-node mode: start as leader immediately (degenerate Raft)
            raftRole           = RaftRole.LEADER;
            raftLeaderAddress  = address();
        } else {
            // Multi-node: start as follower, arm election timer.
            // Deterministic timeout based on our index among all coordinators.
            raftRole           = RaftRole.FOLLOWER;
            raftLeaderAddress  = null;
            int myIndex = 0;
            for (int i = 0; i < raftPeers.length; i++) {
                if (address().compareTo(raftPeers[i]) > 0) myIndex++;
            }
            set(new RaftElectionTimer(), RaftElectionTimer.timeoutForIndex(myIndex));
        }

        // Assume all regions alive at startup (missed count = 0).
        for (Address r : regions) missedHeartbeats.put(r, 0);

        // Start the recurring heartbeat timer (disabled during search tests
        // to prevent infinite BFS state expansion).
        if (enableHeartbeats) {
            set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_MILLIS);
        }
    }

    // =========================================================================
    //  AUTHENTICATION
    // =========================================================================

    private void handleAuthRequest(AuthRequest req, Address sender) {
        // Only the leader handles auth (followers redirect)
        if (!isLeader()) {
            send(new NotLeaderResponse(raftLeaderAddress), sender);
            return;
        }

        log("Auth request from " + req.clientId());

        if (!clientSecrets.containsKey(req.clientId())) {
            log("Unknown client: " + req.clientId());
            send(new AuthResultMsg(req.clientId(), null, false, "UNKNOWN_CLIENT"), sender);
            return;
        }

        // If already authenticated, re-send the token (handles lost AuthResultMsg)
        String existingToken = getSessionForClient(req.clientId());
        if (existingToken != null) {
            log("Already authenticated: " + req.clientId() + " — re-sending token");
            send(new AuthResultMsg(req.clientId(), existingToken, true, null), sender);
            return;
        }

        // If there's already a pending challenge, re-send the SAME nonce.
        // This prevents a race where a retried AuthRequest overwrites the nonce
        // that an in-flight AuthResponse is using.
        byte[] nonce = pendingAuths.get(req.clientId());
        if (nonce == null) {
            nonce = new byte[16];
            RNG.nextBytes(nonce);
            pendingAuths.put(req.clientId(), nonce);
        }
        send(new AuthChallenge(req.clientId(), nonce), sender);
        log("Sent auth challenge to " + req.clientId());
    }

    private void handleAuthResponse(AuthResponse resp, Address sender) {
        byte[] nonce = pendingAuths.get(resp.clientId());
        if (nonce == null) {
            send(new AuthResultMsg(resp.clientId(), null, false, "NO_PENDING_AUTH"), sender);
            return;
        }

        byte[] secret = clientSecrets.get(resp.clientId());
        byte[] expected = CryptoUtil.hmacSha256(secret, nonce);

        if (!Arrays.equals(expected, resp.hmac())) {
            log("Auth FAILED for " + resp.clientId() + " — HMAC mismatch");
            pendingAuths.remove(resp.clientId());
            send(new AuthResultMsg(resp.clientId(), null, false, "AUTH_FAILED"), sender);
            return;
        }

        // Revoke any previous session token for this client (prevents accumulation)
        String oldToken = getSessionForClient(resp.clientId());
        if (oldToken != null) {
            validSessions.remove(oldToken);
            log("Revoked old token for " + resp.clientId());
        }

        // Issue new session token
        byte[] tokenBytes = new byte[16];
        RNG.nextBytes(tokenBytes);
        StringBuilder sb = new StringBuilder();
        for (byte b : tokenBytes) sb.append(String.format("%02x", b & 0xff));
        String token = sb.toString();

        pendingAuths.remove(resp.clientId());

        // Replicate auth session via Raft — state update + AuthResultMsg
        // sent in applyLogEntry after Raft commit.
        StateDelta delta = StateDelta.authSession(resp.clientId(), token);
        // Store deferred auth response (reuse pendingRaftResponses isn't clean
        // since it expects WriteResponse, so store auth responses separately)
        pendingAuthResponses.put(resp.clientId(),
                new PendingAuthResponse(sender, resp.clientId(), token));
        replicateLogEntry(delta, null, null);
    }

    /**
     * Validate the session token and return the authenticated clientId,
     * or null if the token is missing or invalid.
     */
    private String validateSession(String sessionToken) {
        if (sessionToken == null) return null;
        return validSessions.get(sessionToken);
    }

    /** Reverse lookup: find the session token for a given clientId, or null. O(1). */
    private String getSessionForClient(String clientId) {
        return clientToToken.get(clientId);
    }

    /**
     * Validate authentication and authorization for a request.
     * Returns the authenticated clientId on success, or null if rejected
     * (in which case the error string is set in errorOut[0]).
     */
    private String validateAndAuthorize(String clientId, String sessionToken, String key,
                                        String[] errorOut) {
        String authClient = validateSession(sessionToken);
        if (authClient == null) {
            log("Rejected: no valid session for " + clientId);
            errorOut[0] = "AUTH_REQUIRED";
            return null;
        }
        if (!authClient.equals(clientId)) {
            log("Rejected: session/clientId mismatch for " + clientId);
            errorOut[0] = "IDENTITY_MISMATCH";
            return null;
        }
        String owner = keyOwner.get(key);
        if (owner != null && !owner.equals(authClient)) {
            log("Rejected: " + authClient + " is not owner of key=" + key);
            errorOut[0] = "ACCESS_DENIED";
            return null;
        }
        return authClient;
    }

    // =========================================================================
    //  Region liveness
    // =========================================================================

    private boolean isRegionAlive(Address region) {
        Integer missed = missedHeartbeats.get(region);
        if (missed == null) return false;
        return missed < HEARTBEAT_DEAD_THRESHOLD;
    }

    private int countAliveRegions() {
        int count = 0;
        for (Address r : regions) {
            if (isRegionAlive(r)) count++;
        }
        return count;
    }

    /** Minimum alive regions needed: must have enough for both fragments and key shares. */
    private int minRegionsRequired() {
        return Math.max(k, keyThreshold);
    }

    /** True if this coordinator is the Raft leader (or single-node degenerate). */
    private boolean isLeader() {
        return raftRole == RaftRole.LEADER;
    }

    /** Raft majority: more than half of the cluster (self + peers). */
    private int raftMajority() {
        return (raftPeers.length + 1) / 2 + 1;
    }

    /** Get the last log index (0 if empty). */
    private int lastLogIndex() {
        return raftLog.isEmpty() ? snapshotLastIndex : raftLog.get(raftLog.size() - 1).index();
    }

    /** Get the last log term (0 if empty). */
    private int lastLogTerm() {
        return raftLog.isEmpty() ? snapshotLastTerm : raftLog.get(raftLog.size() - 1).term();
    }

    /** Get log entry by index (null if not in log — may be in snapshot). */
    private LogEntry getLogEntry(int index) {
        if (index <= snapshotLastIndex || index > lastLogIndex()) return null;
        int offset = index - snapshotLastIndex - 1;
        if (offset < 0 || offset >= raftLog.size()) return null;
        return raftLog.get(offset);
    }

    /** Get the term of a log entry by index. */
    private int getLogTerm(int index) {
        if (index == 0) return 0;
        if (index == snapshotLastIndex) return snapshotLastTerm;
        LogEntry entry = getLogEntry(index);
        return (entry != null) ? entry.term() : -1;
    }

    // =========================================================================
    //  WRITE PATH
    // =========================================================================

    private void handleWriteRequest(WriteRequest req, Address sender) {
        // --- Raft: only the leader handles client requests ---
        if (!isLeader()) {
            send(new NotLeaderResponse(raftLeaderAddress), sender);
            return;
        }

        log("WriteRequest from " + req.clientId() + " seq=" + req.sequenceNum()
            + " key=" + req.key() + " token=" + (req.sessionToken() != null ? "present" : "MISSING"));

        // --- Authentication + authorization ---
        String[] errorOut = new String[1];
        String authClient = validateAndAuthorize(req.clientId(), req.sessionToken(), req.key(), errorOut);
        if (authClient == null) {
            send(new WriteResponse(req.clientId(), req.sequenceNum(), false, errorOut[0]), sender);
            return;
        }

        // --- AMO dedup check ---
        WriteResponse cached = getCachedWriteResponse(req.clientId(), req.sequenceNum());
        if (cached != null) {
            log("Dedup: replaying cached response for " + req.clientId() + " seq=" + req.sequenceNum());
            send(cached, sender);
            return;
        }

        // --- Reject new writes during reconfiguration (transient) ---
        if (reconfiguring) {
            log("Reconfiguring: rejecting write for key=" + req.key());
            send(new WriteResponse(req.clientId(), req.sequenceNum(), false, "RECONFIGURING"), sender);
            return;
        }

        // --- One pending write per key at a time ---
        if (pendingWrites.containsKey(req.key())) {
            PendingWrite pw = pendingWrites.get(req.key());
            if (pw.clientId.equals(req.clientId()) && pw.seqNum == req.sequenceNum()) {
                // Same client retrying its own in-progress write.
                // Re-send to regions that haven't acked yet to recover from
                // dropped messages in an unreliable network.
                log("Retry of in-progress write for key=" + req.key()
                    + " — re-sending to unacked regions"
                    + " (frags=" + pw.fragmentAcks.size() + "/" + k
                    + ", shares=" + pw.keyShareAcks.size() + "/" + keyThreshold + ")");
                for (int i = 0; i < k + m; i++) {
                    if (!isRegionAlive(regions.get(i))) continue;
                    if (!pw.fragmentAcks.contains(i)) {
                        send(new FragmentWrite(req.key(), pw.version, i,
                            pw.fragments[i], pw.checksums.get(i)), regions.get(i));
                    }
                    if (!pw.keyShareAcks.contains(i)) {
                        send(new KeyShareWrite(req.key(), pw.version, i,
                            pw.keyShares[i]), regions.get(i));
                    }
                }
                return;
            }
            log("Busy: write already in progress for key=" + req.key());
            send(new WriteResponse(req.clientId(), req.sequenceNum(), false, "BUSY"), sender);
            return;
        }

        // --- Fast-fail if insufficient alive regions ---
        int alive = countAliveRegions();
        if (alive < minRegionsRequired()) {
            log("Rejected: only " + alive + " regions alive, need " + minRegionsRequired());
            send(new WriteResponse(req.clientId(), req.sequenceNum(), false, "INSUFFICIENT_REGIONS"), sender);
            return;
        }

        // --- Assign new version ---
        int version = nextVersion.getOrDefault(req.key(), 1);
        nextVersion.put(req.key(), version + 1);

        // --- Encrypt value with a fresh AES-128/CBC key ---
        byte[] aesKey    = newAESKey();
        byte[] iv        = newIV();
        byte[] ciphertext = aesEncrypt(req.value(), aesKey, iv);
        log("Encrypted " + req.value().length + "B -> " + ciphertext.length + "B ciphertext");

        // --- Erasure-code the ciphertext into k+m fragments ---
        byte[][] frags = erasureCoder.encode(ciphertext);
        log("Erasure coded into " + (k + m) + " fragments of " + frags[0].length + "B (k=" + k + ",m=" + m + ")");

        // --- Split AES key into k+m Shamir shares (threshold = keyThreshold) ---
        byte[][] shares = ShamirSecretSharing.split(aesKey, k + m, keyThreshold);
        log("Split AES key into " + (k + m) + " shares (threshold=" + keyThreshold + ")");

        // --- Build version metadata ---
        Map<Integer, byte[]> checksums = new HashMap<>();
        for (int i = 0; i < k + m; i++) checksums.put(i, VersionMetadata.sha256(frags[i]));

        VersionMetadata meta = new VersionMetadata(
            req.key(), version, k, m, keyThreshold, iv, ciphertext.length, checksums);
        allVersions.computeIfAbsent(req.key(), x -> new HashMap<>()).put(version, meta);

        // --- Track the pending write (store fragments/shares for retransmission) ---
        PendingWrite pending = new PendingWrite(req.clientId(), req.sequenceNum(),
            req.key(), version, sender, frags, shares, checksums);
        pendingWrites.put(req.key(), pending);

        // --- Fan out: send fragment + key share to alive regions only ---
        int sent = 0;
        for (int i = 0; i < k + m; i++) {
            if (isRegionAlive(regions.get(i))) {
                send(new FragmentWrite(req.key(), version, i, frags[i], checksums.get(i)), regions.get(i));
                send(new KeyShareWrite(req.key(), version, i, shares[i]), regions.get(i));
                sent++;
            } else {
                log("Skipping dead region-" + i + " (" + regions.get(i) + ")");
            }
        }
        log("Sent to " + sent + "/" + (k + m) + " alive regions; waiting for acks");

        // --- Arm a timeout so the system doesn't block on a slow/down region ---
        set(new WriteTimeoutTimer(req.key(), version), WriteTimeoutTimer.WRITE_TIMEOUT_MILLIS);
    }

    private void handleFragmentAck(FragmentAck ack, Address sender) {
        PendingWrite pending = pendingWrites.get(ack.key());
        if (pending == null || pending.version != ack.version()) return;

        if (!ack.success()) {
            log("Fragment ack FAILED from " + sender + " key=" + ack.key());
            return;
        }
        log("Fragment ack from " + sender + " key=" + ack.key() + " v=" + ack.version());
        pending.fragmentAcks.add(ack.regionIndex());
        tryCommitWrite(pending);
    }

    private void handleKeyShareAck(KeyShareAck ack, Address sender) {
        PendingWrite pending = pendingWrites.get(ack.key());
        if (pending == null || pending.version != ack.version()) return;

        if (!ack.success()) {
            log("Key share ack FAILED from " + sender + " key=" + ack.key());
            return;
        }
        log("Key share ack from " + sender + " key=" + ack.key() + " v=" + ack.version());
        pending.keyShareAcks.add(ack.regionIndex());
        tryCommitWrite(pending);
    }

    /**
     * Commit the version once BOTH thresholds are met.
     *
     * Commit condition: >= k fragment acks AND >= keyThreshold key-share acks.
     *
     * This ensures the object is both reconstructable (k fragments) and
     * decryptable (keyThreshold key shares) for any future reader.
     *
     * With Raft: region commit is step 1. We then create a StateDelta and
     * replicate it via Raft. State updates (latestCommitted, keyOwner, dedup,
     * GC) happen in applyLogEntry() after Raft majority confirms. The client
     * response is deferred until Raft commit.
     */
    private void tryCommitWrite(PendingWrite pending) {
        if (pending.committed) return;
        if (pending.fragmentAcks.size() < k)            return;
        if (pending.keyShareAcks.size() < keyThreshold) return;

        pending.committed = true;

        log("*** REGION-COMMITTED key=" + pending.key + " v=" + pending.version
            + " (frags=" + pending.fragmentAcks.size() + "/" + k
            + ", shares=" + pending.keyShareAcks.size() + "/" + keyThreshold
            + ") — replicating via Raft ***");

        // Clear stored crypto material — no longer needed after region commit
        pending.fragments = null;
        pending.keyShares = null;
        pending.checksums = null;

        // Build the list of old versions to GC
        java.util.List<Integer> oldVersionsToDelete = null;
        Map<Integer, VersionMetadata> versions = allVersions.get(pending.key);
        if (versions != null) {
            oldVersionsToDelete = new java.util.ArrayList<>();
            for (int v : versions.keySet()) {
                if (v != pending.version) oldVersionsToDelete.add(v);
            }
            if (oldVersionsToDelete.isEmpty()) oldVersionsToDelete = null;
        }

        // Determine if this creates a new owner
        String ownerToSet = keyOwner.containsKey(pending.key) ? null : pending.clientId;

        // Get the version metadata (was pre-inserted in handleWriteRequest)
        VersionMetadata meta = versions.get(pending.version);

        // Create the state delta
        StateDelta delta = StateDelta.writeCommit(
                pending.key, pending.version, meta,
                ownerToSet, pending.clientId, pending.seqNum,
                oldVersionsToDelete);

        // Prepare deferred client response
        WriteResponse resp = new WriteResponse(pending.clientId, pending.seqNum, true, null);

        // Remove from pendingWrites BEFORE Raft replication
        pendingWrites.remove(pending.key);

        // Replicate via Raft — state updates + client response happen in applyLogEntry
        replicateLogEntry(delta, pending.clientSender, resp);

        // If reconfiguring and all writes drained, apply the pending join
        if (reconfiguring && pendingWrites.isEmpty()) {
            applyJoin();
        }
    }

    /**
     * Append a log entry and replicate to peers. In single-node mode,
     * immediately commits and applies.
     */
    private void replicateLogEntry(StateDelta delta, Address clientSender, WriteResponse resp) {
        int index = lastLogIndex() + 1;
        LogEntry entry = new LogEntry(currentTerm, index, delta);
        raftLog.add(entry);

        if (clientSender != null && resp != null) {
            pendingRaftResponses.put(index, new PendingRaftResponse(clientSender, resp));
        }

        if (raftPeers.length == 0) {
            // Single-node: immediately commit and apply
            commitIndex = index;
            applyCommittedEntries();
        } else {
            // Multi-node: send to peers, wait for majority
            sendRaftHeartbeats();
            // Arm Raft commit timeout — if majority doesn't ack in time, fail the operation
            set(new RaftCommitTimeoutTimer(index), RaftCommitTimeoutTimer.RAFT_COMMIT_TIMEOUT_MILLIS);
        }
    }

    // =========================================================================
    //  READ PATH
    // =========================================================================

    private void handleReadRequest(ReadRequest req, Address sender) {
        // --- Raft: only the leader handles client requests ---
        if (!isLeader()) {
            send(new NotLeaderResponse(raftLeaderAddress), sender);
            return;
        }

        log("ReadRequest from " + req.clientId() + " seq=" + req.sequenceNum()
            + " key=" + req.key() + " token=" + (req.sessionToken() != null ? "present" : "MISSING"));

        // --- Authentication + authorization ---
        String[] errorOut = new String[1];
        String authClient = validateAndAuthorize(req.clientId(), req.sessionToken(), req.key(), errorOut);
        if (authClient == null) {
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, errorOut[0]), sender);
            return;
        }

        // --- Raft: verify leadership before serving reads (strict consistency) ---
        // In multi-node mode, we must confirm we're still leader by getting a
        // majority heartbeat ack. In single-node mode, skip (majority of 1 = self).
        if (raftPeers.length > 0) {
            int verifyId = nextReadVerificationId++;
            pendingReadVerifications.put(verifyId, new PendingReadVerification(req, sender));
            // Trigger a heartbeat round — when majority acks come back,
            // processVerifiedReads() will proceed with the read
            readVerifyAckCount = 1;  // count self
            readVerifyTerm     = currentTerm;
            sendRaftHeartbeats();
            log("Raft: read queued for leadership verification (id=" + verifyId + ")");
            return;
        }

        // Single-node: proceed directly
        proceedWithRead(req, sender);
    }

    /** Execute a read after leadership has been verified (or single-node mode). */
    private void proceedWithRead(ReadRequest req, Address sender) {
        if (pendingReads.containsKey(req.key())) {
            PendingRead pr = pendingReads.get(req.key());
            if (pr.clientId.equals(req.clientId()) && pr.seqNum == req.sequenceNum()) {
                // Same client retrying its own in-progress read.
                // Re-request from regions that haven't replied yet.
                log("Retry of in-progress read for key=" + req.key()
                    + " — re-requesting from unresponsive regions"
                    + " (frags=" + pr.fragments.size() + "/" + k
                    + ", shares=" + pr.keyShares.size() + "/" + keyThreshold + ")");
                for (int i = 0; i < k + m; i++) {
                    if (!isRegionAlive(regions.get(i))) continue;
                    if (!pr.fragments.containsKey(i)) {
                        send(new FragmentReadRequest(req.key(), pr.version, i), regions.get(i));
                    }
                    if (!pr.keyShares.containsKey(i)) {
                        send(new KeyShareReadRequest(req.key(), pr.version, i), regions.get(i));
                    }
                }
                return;
            }
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, "READ_IN_PROGRESS"), sender);
            return;
        }

        Integer version = latestCommitted.get(req.key());
        if (version == null) {
            log("Key not found: " + req.key());
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, "KEY_NOT_FOUND"), sender);
            return;
        }

        // --- Per-version fast-fail: check alive regions against this version's thresholds ---
        VersionMetadata meta = allVersions.get(req.key()).get(version);
        int versionMinRegions = Math.max(meta.k, meta.keyThreshold);
        int alive = countAliveRegions();
        if (alive < versionMinRegions) {
            log("Rejected read: only " + alive + " regions alive, version needs " + versionMinRegions);
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, "INSUFFICIENT_REGIONS"), sender);
            return;
        }

        log("Reading key=" + req.key() + " at v=" + version);

        PendingRead pending = new PendingRead(req.clientId(), req.sequenceNum(), req.key(), version, sender);
        pendingReads.put(req.key(), pending);

        // Fan out to alive regions only — skip dead ones to save traffic.
        // Fan out to ALL regions (including those added after this version was written);
        // regions without the fragment will reply null, which is handled.
        int numRegions = meta.k + meta.m;  // version's region count (may be < current)
        int sent = 0;
        for (int i = 0; i < Math.min(numRegions, regions.size()); i++) {
            if (isRegionAlive(regions.get(i))) {
                send(new FragmentReadRequest(req.key(), version, i), regions.get(i));
                send(new KeyShareReadRequest(req.key(), version, i), regions.get(i));
                sent++;
            } else {
                log("Skipping dead region-" + i + " (" + regions.get(i) + ")");
            }
        }
        log("Sent fragment + key-share requests to " + sent + "/" + (k + m) + " alive regions");

        // Arm timeout so we fail gracefully if not enough regions reply
        set(new ReadTimeoutTimer(req.key(), version), ReadTimeoutTimer.READ_TIMEOUT_MILLIS);
    }

    private void handleFragmentReadReply(FragmentReadReply reply, Address sender) {
        PendingRead pending = pendingReads.get(reply.key());
        if (pending == null || pending.version != reply.version() || pending.completed) return;

        if (reply.fragment() == null) {
            log("Region-" + reply.regionIndex() + " has no fragment for key=" + reply.key());
            return;
        }

        // Bounds check: reject fragments from regions outside this version's config
        VersionMetadata meta = allVersions.get(reply.key()).get(reply.version());
        if (reply.regionIndex() >= meta.k + meta.m) {
            log("Ignoring fragment from region-" + reply.regionIndex()
                + " — outside version's k+m=" + (meta.k + meta.m));
            return;
        }

        // Integrity check: reject corrupted or tampered fragments
        if (!meta.verifyFragment(reply.regionIndex(), reply.fragment())) {
            log("*** Checksum FAILED for fragment from region-" + reply.regionIndex()
                + " key=" + reply.key() + " — rejecting ***");
            return;
        }

        pending.fragments.put(reply.regionIndex(), reply.fragment());
        log("Accepted fragment from region-" + reply.regionIndex()
            + " (have " + pending.fragments.size() + "/" + meta.k + ")");
        tryCompleteRead(pending);
    }

    private void handleKeyShareReadReply(KeyShareReadReply reply, Address sender) {
        PendingRead pending = pendingReads.get(reply.key());
        if (pending == null || pending.version != reply.version() || pending.completed) return;

        if (reply.keyShare() == null) {
            log("Region-" + reply.regionIndex() + " has no key share for key=" + reply.key());
            return;
        }

        // Bounds check: reject shares from regions outside this version's config
        VersionMetadata meta = allVersions.get(reply.key()).get(reply.version());
        if (reply.regionIndex() >= meta.k + meta.m) {
            log("Ignoring key share from region-" + reply.regionIndex()
                + " — outside version's k+m=" + (meta.k + meta.m));
            return;
        }

        pending.keyShares.put(reply.regionIndex(), reply.keyShare());
        log("Accepted key share from region-" + reply.regionIndex()
            + " (have " + pending.keyShares.size() + "/" + meta.keyThreshold + ")");
        tryCompleteRead(pending);
    }

    /**
     * Complete the read once we have both k fragments AND keyThreshold shares.
     *
     * Neither condition alone is sufficient — this enforces confidentiality:
     * ciphertext alone is useless without the key; key shares alone are useless
     * without the ciphertext.
     */
    private void tryCompleteRead(PendingRead pending) {
        if (pending.completed) return;

        // Use the version's stored k/m/keyThreshold — may differ from current
        // global config if the system was reconfigured after this version was written.
        VersionMetadata meta = allVersions.get(pending.key).get(pending.version);
        if (pending.fragments.size() < meta.k)            return;
        if (pending.keyShares.size() < meta.keyThreshold) return;

        pending.completed = true;

        // Reconstruct ciphertext using the version's erasure coding params
        ErasureCoder versionCoder = (meta.k == k && meta.m == m)
                ? erasureCoder  // reuse global if same config
                : new ErasureCoder(meta.k, meta.m);
        byte[][] allFrags = new byte[meta.k + meta.m][];
        pending.fragments.forEach((idx, frag) -> allFrags[idx] = frag);
        byte[] ciphertext = versionCoder.decode(allFrags, meta.originalCiphertextLength);
        log("Reconstructed ciphertext from " + pending.fragments.size()
            + " fragments (version k=" + meta.k + ", m=" + meta.m + ")");

        // Reconstruct AES key from key shares
        int n = pending.keyShares.size();
        byte[][] shares  = new byte[n][];
        int[]    indices = new int[n];
        int i = 0;
        for (Map.Entry<Integer, byte[]> e : pending.keyShares.entrySet()) {
            indices[i] = e.getKey();
            shares[i]  = e.getValue();
            i++;
        }
        byte[] aesKey = ShamirSecretSharing.recover(shares, indices);
        log("Reconstructed AES key from " + n + " shares");

        // Decrypt
        byte[] plaintext = aesDecrypt(ciphertext, aesKey, meta.iv);
        log("*** Decrypted " + plaintext.length + "B plaintext for key=" + pending.key + " ***");

        pendingReads.remove(pending.key);
        send(new ReadResponse(pending.clientId, pending.seqNum, plaintext, null), pending.clientSender);
    }

    // =========================================================================
    //  Timers
    // =========================================================================

    /**
     * Recurring heartbeat: ping all regions, then re-arm the timer.
     * The coordinator uses heartbeat replies to track region liveness.
     */
    private void onHeartbeatTimer(HeartbeatTimer t) {
        // Increment missed count for every region, then ping them.
        // Regions that reply will reset their count in handleHeartbeatReply.
        for (Address region : regions) {
            missedHeartbeats.merge(region, 1, Integer::sum);
            send(new HeartbeatMsg(), region);
        }
        set(t, HeartbeatTimer.HEARTBEAT_MILLIS);  // re-arm
    }

    private void handleHeartbeatReply(HeartbeatReply reply, Address sender) {
        missedHeartbeats.put(sender, 0);  // alive — reset counter
        log("Heartbeat reply from " + sender);
    }

    // =========================================================================
    //  DYNAMIC MEMBERSHIP
    // =========================================================================

    private void handleJoinRequest(JoinRequest req, Address sender) {
        log("JoinRequest from " + sender);

        // Verify cluster secret
        if (clusterSecret == null) {
            log("Rejected join: dynamic membership not enabled (no cluster secret)");
            send(new JoinResult(false, "MEMBERSHIP_DISABLED"), sender);
            return;
        }

        byte[] expected = CryptoUtil.hmacSha256(clusterSecret, sender.toString().getBytes());
        if (!Arrays.equals(expected, req.hmac())) {
            log("Rejected join: HMAC mismatch from " + sender);
            send(new JoinResult(false, "AUTH_FAILED"), sender);
            return;
        }

        // Check if already a member
        if (regions.contains(sender)) {
            log("Already a member: " + sender);
            send(new JoinResult(true, null), sender);
            return;
        }

        // Check if already reconfiguring
        if (reconfiguring) {
            log("Rejected join: reconfiguration already in progress");
            send(new JoinResult(false, "RECONFIG_IN_PROGRESS"), sender);
            return;
        }

        // Begin reconfiguration: stop accepting new writes until in-flight drain
        reconfiguring     = true;
        pendingJoinRegion = sender;
        log("*** RECONFIGURING: waiting for " + pendingWrites.size()
            + " pending writes to drain before adding " + sender + " ***");

        // If no pending writes, apply immediately
        if (pendingWrites.isEmpty()) {
            applyJoin();
        } else {
            // Arm timeout to abort if writes don't drain
            set(new ReconfigTimeoutTimer(), ReconfigTimeoutTimer.RECONFIG_TIMEOUT_MILLIS);
        }
    }

    /** Apply the pending region join: add region, increase m, recreate coder. */
    private void applyJoin() {
        Address newRegion = pendingJoinRegion;
        int newM = m + 1;

        // Replicate config change via Raft — actual state update happens in applyLogEntry
        StateDelta delta = StateDelta.configChange(newRegion, newM);
        pendingJoinAddress = newRegion;  // track for sending JoinResult after Raft commit
        reconfiguring     = false;
        pendingJoinRegion = null;

        replicateLogEntry(delta, null, null);
    }

    // Address of the region waiting for JoinResult after Raft commit
    private Address pendingJoinAddress;

    private void onReconfigTimeoutTimer(ReconfigTimeoutTimer t) {
        if (!reconfiguring) return;

        log("*** RECONFIG TIMEOUT: aborting join of " + pendingJoinRegion
            + " (" + pendingWrites.size() + " writes still pending) ***");
        Address failedRegion = pendingJoinRegion;
        reconfiguring     = false;
        pendingJoinRegion = null;
        send(new JoinResult(false, "RECONFIG_TIMEOUT"), failedRegion);
    }

    // =========================================================================
    //  RAFT CONSENSUS
    //
    //  Leader election: followers become candidates after election timeout,
    //  request votes, become leader on majority. Leaders send periodic
    //  heartbeats (empty AppendEntries) to suppress elections.
    // =========================================================================

    /**
     * Election timeout fired — no heartbeat from leader.
     * Become candidate, increment term, vote for self, request votes from peers.
     */
    private void onRaftElectionTimer(RaftElectionTimer t) {
        if (raftPeers.length == 0) return;  // single-node, no elections

        currentTerm++;
        raftRole          = RaftRole.CANDIDATE;
        votedFor          = address();
        raftLeaderAddress = null;
        votesReceived.clear();
        votesReceived.add(address());  // vote for self

        log("Raft: starting election for term " + currentTerm);

        for (Address peer : raftPeers) {
            send(new RaftRequestVote(currentTerm, address(), lastLogIndex(), lastLogTerm()), peer);
        }

        // Re-arm election timer (in case election doesn't complete)
        int myIndex = 0;
        for (int i = 0; i < raftPeers.length; i++) {
            if (address().compareTo(raftPeers[i]) > 0) myIndex++;
        }
        set(new RaftElectionTimer(), RaftElectionTimer.timeoutForIndex(myIndex));
    }

    /**
     * Received a vote request from a candidate.
     * Grant vote if: candidate's term >= ours, we haven't voted for someone else
     * in this term, and candidate's log is at least as up-to-date as ours.
     */
    private void handleRaftRequestVote(RaftRequestVote req, Address sender) {
        // If candidate's term > ours, update term and become follower
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }

        boolean logOk = req.lastLogTerm() > lastLogTerm()
                || (req.lastLogTerm() == lastLogTerm() && req.lastLogIndex() >= lastLogIndex());

        boolean grant = req.term() == currentTerm
                && (votedFor == null || votedFor.equals(req.candidateId()))
                && logOk;

        if (grant) {
            votedFor = req.candidateId();
            // Reset election timer — we granted a vote, give the candidate time
            int myIndex = 0;
            for (int i = 0; i < raftPeers.length; i++) {
                if (address().compareTo(raftPeers[i]) > 0) myIndex++;
            }
            set(new RaftElectionTimer(), RaftElectionTimer.timeoutForIndex(myIndex));
            log("Raft: granted vote to " + req.candidateId() + " for term " + req.term());
        }

        send(new RaftRequestVoteReply(currentTerm, grant), sender);
    }

    /**
     * Received a vote reply. If we have majority, become leader.
     */
    private void handleRaftRequestVoteReply(RaftRequestVoteReply reply, Address sender) {
        if (reply.term() > currentTerm) {
            stepDown(reply.term());
            return;
        }

        if (raftRole != RaftRole.CANDIDATE || reply.term() != currentTerm) return;

        if (reply.voteGranted()) {
            votesReceived.add(sender);
            log("Raft: received vote from " + sender + " (" + votesReceived.size()
                + "/" + raftMajority() + " needed)");

            if (votesReceived.size() >= raftMajority()) {
                becomeLeader();
            }
        }
    }

    /**
     * Transition to leader. Initialize nextIndex/matchIndex for all peers.
     * Start sending heartbeats.
     */
    private void becomeLeader() {
        raftRole          = RaftRole.LEADER;
        raftLeaderAddress = address();
        log("Raft: *** BECAME LEADER for term " + currentTerm + " ***");

        // Initialize leader state
        int lastIdx = lastLogIndex();
        for (Address peer : raftPeers) {
            nextIndex.put(peer, lastIdx + 1);
            matchIndex.put(peer, 0);
        }

        // Recompute nextVersion from committed state (may differ from old leader)
        for (Map.Entry<String, Map<Integer, VersionMetadata>> e : allVersions.entrySet()) {
            int maxVersion = 0;
            for (int v : e.getValue().keySet()) {
                if (v > maxVersion) maxVersion = v;
            }
            nextVersion.put(e.getKey(), maxVersion + 1);
        }

        // Send immediate heartbeat to assert leadership
        sendRaftHeartbeats();
        set(new RaftHeartbeatTimer(), RaftHeartbeatTimer.RAFT_HEARTBEAT_MILLIS);
    }

    /**
     * Step down to follower when we discover a higher term.
     */
    private void stepDown(int newTerm) {
        currentTerm       = newTerm;
        raftRole          = RaftRole.FOLLOWER;
        votedFor          = null;
        raftLeaderAddress = null;
        log("Raft: stepped down to follower, term " + currentTerm);

        // Reject any pending read verifications — we're no longer leader
        for (PendingReadVerification v : pendingReadVerifications.values()) {
            send(new NotLeaderResponse(raftLeaderAddress), v.sender);
        }
        pendingReadVerifications.clear();
        readVerifyAckCount = 0;

        // Re-arm election timer
        int myIndex = 0;
        for (int i = 0; i < raftPeers.length; i++) {
            if (address().compareTo(raftPeers[i]) > 0) myIndex++;
        }
        set(new RaftElectionTimer(), RaftElectionTimer.timeoutForIndex(myIndex));
    }

    /**
     * Leader heartbeat timer — send empty AppendEntries to all peers.
     */
    private void onRaftHeartbeatTimer(RaftHeartbeatTimer t) {
        if (raftRole != RaftRole.LEADER) return;
        sendRaftHeartbeats();
        set(t, RaftHeartbeatTimer.RAFT_HEARTBEAT_MILLIS);
    }

    /** Send AppendEntries (heartbeat or with entries) to all peers. */
    private void sendRaftHeartbeats() {
        for (Address peer : raftPeers) {
            int ni = nextIndex.getOrDefault(peer, lastLogIndex() + 1);

            // If follower is too far behind (needs entries we've compacted), send snapshot
            if (ni <= snapshotLastIndex && snapshotData != null) {
                send(new RaftInstallSnapshot(currentTerm, address(),
                        snapshotLastIndex, snapshotLastTerm, snapshotData), peer);
                continue;
            }

            int prevIdx = ni - 1;
            int prevTerm = getLogTerm(prevIdx);

            // Collect entries from nextIndex onwards
            java.util.List<LogEntry> entries = new java.util.ArrayList<>();
            for (int i = ni; i <= lastLogIndex(); i++) {
                LogEntry entry = getLogEntry(i);
                if (entry != null) entries.add(entry);
            }

            send(new RaftAppendEntries(currentTerm, address(), prevIdx, prevTerm,
                    entries, commitIndex), peer);
        }
    }

    /**
     * Follower receives AppendEntries from leader.
     * For Phase 3: handles heartbeats (empty entries) — resets election timer, updates term.
     * Full log replication will be added in Phase 4.
     */
    private void handleRaftAppendEntries(RaftAppendEntries req, Address sender) {
        if (req.term() < currentTerm) {
            send(new RaftAppendEntriesReply(currentTerm, false, 0), sender);
            return;
        }

        if (req.term() > currentTerm || raftRole == RaftRole.CANDIDATE) {
            stepDown(req.term());
        }

        // Valid leader — reset election timer
        raftRole          = RaftRole.FOLLOWER;
        raftLeaderAddress = req.leaderId();
        int myIndex = 0;
        for (int i = 0; i < raftPeers.length; i++) {
            if (address().compareTo(raftPeers[i]) > 0) myIndex++;
        }
        set(new RaftElectionTimer(), RaftElectionTimer.timeoutForIndex(myIndex));

        // Check prevLogIndex/prevLogTerm match
        if (req.prevLogIndex() > 0) {
            int localTerm = getLogTerm(req.prevLogIndex());
            if (localTerm != req.prevLogTerm()) {
                // Log mismatch — truncate divergent entries
                while (lastLogIndex() >= req.prevLogIndex()) {
                    raftLog.remove(raftLog.size() - 1);
                }
                send(new RaftAppendEntriesReply(currentTerm, false, lastLogIndex()), sender);
                return;
            }
        }

        // Append new entries (skip duplicates)
        for (LogEntry entry : req.entries()) {
            if (entry.index() <= lastLogIndex()) {
                // Already have this entry — check for conflict
                int existingTerm = getLogTerm(entry.index());
                if (existingTerm != entry.term()) {
                    // Conflict: truncate from here
                    while (lastLogIndex() >= entry.index()) {
                        raftLog.remove(raftLog.size() - 1);
                    }
                    raftLog.add(entry);
                }
                // Same term = same entry, skip
            } else {
                raftLog.add(entry);
            }
        }

        // Advance commitIndex
        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), lastLogIndex());
            applyCommittedEntries();
        }

        send(new RaftAppendEntriesReply(currentTerm, true, lastLogIndex()), sender);
    }

    /**
     * Leader receives AppendEntries reply from follower.
     */
    private void handleRaftAppendEntriesReply(RaftAppendEntriesReply reply, Address sender) {
        if (reply.term() > currentTerm) {
            stepDown(reply.term());
            return;
        }

        if (raftRole != RaftRole.LEADER || reply.term() != currentTerm) return;

        if (reply.success()) {
            matchIndex.put(sender, reply.matchIndex());
            nextIndex.put(sender, reply.matchIndex() + 1);
            advanceCommitIndex();

            // Check if this ack completes a read leadership verification
            if (readVerifyTerm == currentTerm && !pendingReadVerifications.isEmpty()) {
                readVerifyAckCount++;
                if (readVerifyAckCount >= raftMajority()) {
                    processVerifiedReads();
                }
            }
        } else {
            // Follower's log didn't match — decrement nextIndex and retry
            int ni = nextIndex.getOrDefault(sender, 1);
            nextIndex.put(sender, Math.max(1, ni - 1));
        }
    }

    /**
     * Leadership verified for pending reads — proceed with all queued reads.
     */
    private void processVerifiedReads() {
        log("Raft: leadership verified, processing " + pendingReadVerifications.size() + " pending reads");
        Map<Integer, PendingReadVerification> toProcess = new HashMap<>(pendingReadVerifications);
        pendingReadVerifications.clear();
        readVerifyAckCount = 0;

        for (PendingReadVerification v : toProcess.values()) {
            if (isLeader()) {
                proceedWithRead(v.request, v.sender);
            } else {
                // Lost leadership during verification — redirect
                send(new NotLeaderResponse(raftLeaderAddress), v.sender);
            }
        }
    }

    /**
     * Raft commit timeout: if a log entry wasn't committed by majority in time,
     * fail the pending client response. The log entry stays (may be committed
     * later or truncated by a new leader).
     */
    private void onRaftCommitTimeoutTimer(RaftCommitTimeoutTimer t) {
        if (t.logIndex() <= commitIndex) return;  // already committed, ignore

        PendingRaftResponse pending = pendingRaftResponses.remove(t.logIndex());
        if (pending != null) {
            log("Raft: commit timeout for log index " + t.logIndex()
                + " — failing client response");
            send(new WriteResponse(
                    pending.response.clientId(), pending.response.sequenceNum(),
                    false, "RAFT_COMMIT_TIMEOUT"), pending.clientSender);
        }

        // Also check for pending auth responses at this index
        // (auth entries don't have a direct index mapping, so this is best-effort)
    }

    /**
     * Leader advances commitIndex when a majority has replicated an entry.
     */
    private void advanceCommitIndex() {
        for (int n = lastLogIndex(); n > commitIndex; n--) {
            // Only commit entries from current term (Raft safety)
            if (getLogTerm(n) != currentTerm) continue;

            // Count replicas (self + peers with matchIndex >= n)
            int replicas = 1; // self
            for (Address peer : raftPeers) {
                if (matchIndex.getOrDefault(peer, 0) >= n) replicas++;
            }

            if (replicas >= raftMajority()) {
                commitIndex = n;
                applyCommittedEntries();
                break;
            }
        }
    }

    /**
     * Apply all committed but unapplied log entries to the state machine.
     */
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = getLogEntry(lastApplied);
            if (entry != null) {
                applyLogEntry(entry);
            }
        }
        // Check if log compaction is needed
        maybeCompactLog();
    }

    // =========================================================================
    //  LOG COMPACTION
    //
    //  When the log exceeds LOG_COMPACT_THRESHOLD entries, take a snapshot of
    //  the full state machine and discard old log entries. Followers that are
    //  too far behind receive the snapshot via InstallSnapshot instead of
    //  replaying the full log.
    // =========================================================================

    private static final int LOG_COMPACT_THRESHOLD = 100;

    private void maybeCompactLog() {
        if (raftLog.size() < LOG_COMPACT_THRESHOLD) return;

        // Snapshot at lastApplied
        snapshotLastIndex = lastApplied;
        snapshotLastTerm  = getLogTerm(lastApplied);

        try {
            snapshotData = serializeState();
        } catch (Exception e) {
            log("Raft: snapshot serialization failed: " + e.getMessage());
            return;
        }

        // Discard log entries up to snapshotLastIndex
        int entriesToRemove = 0;
        for (int i = 0; i < raftLog.size(); i++) {
            if (raftLog.get(i).index() <= snapshotLastIndex) {
                entriesToRemove++;
            } else {
                break;
            }
        }
        if (entriesToRemove > 0) {
            raftLog.subList(0, entriesToRemove).clear();
        }

        log("Raft: compacted log — snapshot at index " + snapshotLastIndex
            + ", " + raftLog.size() + " entries remaining");
    }

    /** Serialize the coordinator's replicated state for a snapshot. */
    private byte[] serializeState() {
        try {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(new java.util.HashMap<>(allVersions));
            oos.writeObject(new java.util.HashMap<>(latestCommitted));
            oos.writeObject(new java.util.HashMap<>(keyOwner));
            oos.writeObject(new java.util.HashMap<>(writeDedup));
            oos.writeObject(new java.util.HashMap<>(validSessions));
            oos.writeObject(new java.util.HashMap<>(clientToToken));
            oos.writeObject(new java.util.ArrayList<>(regions));
            oos.writeInt(m);
            oos.writeInt(keyThreshold);
            oos.flush();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Snapshot serialization failed", e);
        }
    }

    /** Restore coordinator state from a snapshot. */
    @SuppressWarnings("unchecked")
    private void deserializeState(byte[] data) {
        try {
            java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(data);
            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bis);
            allVersions     = (Map<String, Map<Integer, VersionMetadata>>) ois.readObject();
            latestCommitted = (Map<String, Integer>) ois.readObject();
            keyOwner        = (Map<String, String>) ois.readObject();
            writeDedup      = (Map<String, DedupEntry>) ois.readObject();
            validSessions   = (Map<String, String>) ois.readObject();
            clientToToken   = (Map<String, String>) ois.readObject();
            java.util.List<Address> restoredRegions = (java.util.List<Address>) ois.readObject();
            regions.clear();
            regions.addAll(restoredRegions);
            m             = ois.readInt();
            keyThreshold  = ois.readInt();
            erasureCoder  = new ErasureCoder(k, m);
            // Recompute nextVersion from allVersions
            nextVersion.clear();
            for (Map.Entry<String, Map<Integer, VersionMetadata>> e : allVersions.entrySet()) {
                int maxV = 0;
                for (int v : e.getValue().keySet()) {
                    if (v > maxV) maxV = v;
                }
                nextVersion.put(e.getKey(), maxV + 1);
            }
            log("Raft: restored state from snapshot");
        } catch (Exception e) {
            throw new RuntimeException("Snapshot deserialization failed", e);
        }
    }

    /**
     * Follower receives a snapshot from the leader (too far behind for log replay).
     */
    private void handleRaftInstallSnapshot(RaftInstallSnapshot req, Address sender) {
        if (req.term() < currentTerm) return;

        if (req.term() > currentTerm) {
            stepDown(req.term());
        }

        raftRole          = RaftRole.FOLLOWER;
        raftLeaderAddress = req.leaderId();

        // Reset election timer
        int myIndex = 0;
        for (int i = 0; i < raftPeers.length; i++) {
            if (address().compareTo(raftPeers[i]) > 0) myIndex++;
        }
        set(new RaftElectionTimer(), RaftElectionTimer.timeoutForIndex(myIndex));

        // Apply snapshot
        snapshotLastIndex = req.lastIncludedIndex();
        snapshotLastTerm  = req.lastIncludedTerm();
        snapshotData      = req.snapshotData();

        // Discard entire log (snapshot supersedes it)
        raftLog.clear();

        // Restore state from snapshot
        deserializeState(req.snapshotData());

        // Update Raft indices
        commitIndex = Math.max(commitIndex, snapshotLastIndex);
        lastApplied = Math.max(lastApplied, snapshotLastIndex);

        log("Raft: installed snapshot from " + req.leaderId()
            + " at index " + snapshotLastIndex);

        send(new RaftAppendEntriesReply(currentTerm, true, snapshotLastIndex), sender);
    }

    /**
     * Apply a single log entry to the coordinator state machine.
     * Called on both leader and followers when an entry is committed.
     * Must be deterministic — same delta produces same state on all replicas.
     */
    private void applyLogEntry(LogEntry entry) {
        StateDelta delta = entry.delta();
        switch (delta.type()) {
            case WRITE_COMMIT:
                // Apply version metadata
                allVersions.computeIfAbsent(delta.key(), x -> new HashMap<>())
                           .put(delta.version(), delta.metadata());
                latestCommitted.put(delta.key(), delta.version());
                if (delta.ownerClientId() != null) {
                    keyOwner.putIfAbsent(delta.key(), delta.ownerClientId());
                }
                // Apply dedup
                if (delta.dedupClientId() != null) {
                    writeDedup.put(delta.dedupClientId(),
                            new DedupEntry(delta.dedupSeqNum(),
                                    new WriteResponse(delta.dedupClientId(), delta.dedupSeqNum(), true, null)));
                }
                // GC old versions (only leader sends region deletes)
                if (delta.oldVersionsToDelete() != null) {
                    Map<Integer, VersionMetadata> versions = allVersions.get(delta.key());
                    for (int oldVersion : delta.oldVersionsToDelete()) {
                        VersionMetadata oldMeta = versions != null ? versions.remove(oldVersion) : null;
                        if (oldMeta != null && isLeader()) {
                            int regionCount = Math.min(oldMeta.k + oldMeta.m, regions.size());
                            for (int i = 0; i < regionCount; i++) {
                                send(new DeleteVersionData(delta.key(), oldVersion), regions.get(i));
                            }
                        }
                    }
                    if (versions != null && versions.isEmpty()) allVersions.remove(delta.key());
                }
                // Send deferred response (leader only)
                if (isLeader()) {
                    PendingRaftResponse pending = pendingRaftResponses.remove(entry.index());
                    if (pending != null) {
                        send(pending.response, pending.clientSender);
                    }
                }
                log("Raft: applied WRITE_COMMIT key=" + delta.key() + " v=" + delta.version());
                break;

            case AUTH_SESSION:
                validSessions.put(delta.sessionToken(), delta.authClientId());
                clientToToken.put(delta.authClientId(), delta.sessionToken());
                log("Raft: applied AUTH_SESSION for " + delta.authClientId());
                // Send deferred AuthResultMsg (leader only)
                if (isLeader()) {
                    PendingAuthResponse par = pendingAuthResponses.remove(delta.authClientId());
                    if (par != null) {
                        send(new AuthResultMsg(par.clientId, par.token, true, null), par.sender);
                        log("*** Authenticated " + par.clientId + " token=" + par.token + " ***");
                    }
                }
                break;

            case CONFIG_CHANGE:
                regions.add(delta.newRegionAddress());
                m = delta.newM();
                erasureCoder = new ErasureCoder(k, m);
                missedHeartbeats.put(delta.newRegionAddress(), 0);
                log("Raft: applied CONFIG_CHANGE, added " + delta.newRegionAddress()
                    + " (now " + regions.size() + " regions, m=" + m + ")");
                // Send JoinResult to the new region (leader only)
                if (isLeader() && pendingJoinAddress != null
                        && pendingJoinAddress.equals(delta.newRegionAddress())) {
                    send(new JoinResult(true, null), pendingJoinAddress);
                    log("*** JOIN COMPLETE: sent JoinResult to " + pendingJoinAddress + " ***");
                    pendingJoinAddress = null;
                }
                break;
        }
    }

    /**
     * Read timeout: fail the pending read if we haven't collected enough
     * fragments or key shares yet.
     */
    private void onReadTimeoutTimer(ReadTimeoutTimer t) {
        PendingRead pending = pendingReads.get(t.key());
        if (pending == null || pending.completed || pending.version != t.version()) return;

        log("Read TIMEOUT for key=" + t.key() + " v=" + t.version()
            + " (frags=" + pending.fragments.size() + "/" + k
            + ", shares=" + pending.keyShares.size() + "/" + keyThreshold + ")");

        pending.completed = true;
        pendingReads.remove(t.key());
        send(new ReadResponse(pending.clientId, pending.seqNum,
            null, "READ_TIMEOUT: insufficient fragments or key shares"), pending.clientSender);
    }

    /**
     * Write timeout: if the write hasn't committed by now, fail it.
     *
     * This prevents a stuck write from blocking the key indefinitely when
     * regions are slow or down and we can't reach the commit threshold.
     */
    private void onWriteTimeoutTimer(WriteTimeoutTimer t) {
        PendingWrite pending = pendingWrites.get(t.key());
        if (pending == null || pending.committed || pending.version != t.version()) return;

        log("Write TIMEOUT for key=" + t.key() + " v=" + t.version()
            + " (frags=" + pending.fragmentAcks.size() + "/" + k
            + ", shares=" + pending.keyShareAcks.size() + "/" + keyThreshold + ")");

        WriteResponse resp = new WriteResponse(pending.clientId, pending.seqNum,
            false, "WRITE_TIMEOUT: insufficient acks");
        send(resp, pending.clientSender);
        pendingWrites.remove(t.key());

        // GC: clean up the failed uncommitted version immediately
        gcUncommittedVersion(t.key(), t.version());

        // If reconfiguring and all writes drained, apply the pending join
        if (reconfiguring && pendingWrites.isEmpty()) {
            applyJoin();
        }
    }

    // =========================================================================
    //  AES-128/CBC  (javax.crypto — standard Java, not a 3rd-party library)
    // =========================================================================

    private static byte[] newAESKey() { byte[] k = new byte[16]; RNG.nextBytes(k); return k; }
    private static byte[] newIV()     { byte[] v = new byte[16]; RNG.nextBytes(v); return v; }

    private static byte[] aesEncrypt(byte[] data, byte[] key, byte[] iv) {
        try {
            Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding");
            c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv));
            return c.doFinal(data);
        } catch (Exception e) { throw new RuntimeException("AES encrypt failed", e); }
    }

    private static byte[] aesDecrypt(byte[] data, byte[] key, byte[] iv) {
        try {
            Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding");
            c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv));
            return c.doFinal(data);
        } catch (Exception e) { throw new RuntimeException("AES decrypt failed", e); }
    }

    // =========================================================================
    //  Garbage collection
    //
    //  Old committed versions and failed uncommitted versions are cleaned up
    //  to prevent unbounded memory growth. The coordinator deletes local
    //  metadata immediately and sends fire-and-forget DeleteVersionData to
    //  regions. If the delete message is lost, the region keeps orphaned data
    //  that is unreachable (no coordinator metadata points to it).
    // =========================================================================

    /**
     * Delete old versions of a key from coordinator metadata and send
     * fire-and-forget delete messages to regions.
     *
     * @param key         the object key
     * @param keepVersion the version to keep (latest committed); all others deleted
     */
    private void gcOldVersions(String key, int keepVersion) {
        Map<Integer, VersionMetadata> versions = allVersions.get(key);
        if (versions == null) return;

        java.util.List<Integer> toDelete = new java.util.ArrayList<>();
        for (Map.Entry<Integer, VersionMetadata> e : versions.entrySet()) {
            if (e.getKey() != keepVersion) {
                toDelete.add(e.getKey());
            }
        }

        for (int oldVersion : toDelete) {
            VersionMetadata oldMeta = versions.remove(oldVersion);
            // Send fire-and-forget deletes to regions that had this version's data.
            // Use the old version's k+m (may differ from current after dynamic join).
            int oldRegionCount = Math.min(oldMeta.k + oldMeta.m, regions.size());
            for (int i = 0; i < oldRegionCount; i++) {
                send(new DeleteVersionData(key, oldVersion), regions.get(i));
            }
            log("GC: deleted version " + oldVersion + " of key=" + key
                + " (sent deletes to " + oldRegionCount + " regions)");
        }
    }

    /**
     * Delete a single uncommitted version (from a failed/timed-out write).
     */
    private void gcUncommittedVersion(String key, int version) {
        Map<Integer, VersionMetadata> versions = allVersions.get(key);
        if (versions == null) return;

        VersionMetadata meta = versions.remove(version);
        if (meta == null) return;

        int regionCount = Math.min(meta.k + meta.m, regions.size());
        for (int i = 0; i < regionCount; i++) {
            send(new DeleteVersionData(key, version), regions.get(i));
        }
        log("GC: deleted uncommitted v=" + version + " of key=" + key);

        // Clean up the outer map if no versions remain
        if (versions.isEmpty()) allVersions.remove(key);
    }

    // =========================================================================
    //  AMO dedup helpers
    //
    //  Sliding window: only the latest (seqNum, response) per client is kept.
    //  Since clients send commands sequentially with monotonically increasing
    //  seqNums, a replay of seqNum <= lastProcessed is always stale.
    // =========================================================================

    private WriteResponse getCachedWriteResponse(String clientId, int seqNum) {
        DedupEntry entry = writeDedup.get(clientId);
        if (entry == null) return null;
        if (seqNum == entry.seqNum) return entry.response;
        if (seqNum < entry.seqNum) {
            // Stale replay — the client has moved on to a higher seqNum.
            // We only cache committed (successful) writes, so if this old seqNum
            // isn't cached, the original write may have failed. Return the latest
            // cached response as a signal to stop retrying — the client has already
            // progressed past this seqNum regardless.
            log("Dedup: stale replay from " + clientId + " seq=" + seqNum
                + " (latest=" + entry.seqNum + "), returning cached latest");
            return entry.response;
        }
        return null; // seqNum > lastProcessed → new command, not a replay
    }

    private void cacheWriteResponse(String clientId, int seqNum, WriteResponse resp) {
        writeDedup.put(clientId, new DedupEntry(seqNum, resp));
    }

    private static final class DedupEntry implements java.io.Serializable {
        final int           seqNum;
        final WriteResponse response;
        DedupEntry(int seqNum, WriteResponse response) {
            this.seqNum   = seqNum;
            this.response = response;
        }
    }

    // =========================================================================
    //  Internal state classes
    // =========================================================================

    private static final class PendingWrite {
        final String  clientId;
        final int     seqNum;
        final String  key;
        final int     version;
        final Address clientSender;
        final Set<Integer> fragmentAcks  = new HashSet<>();
        final Set<Integer> keyShareAcks  = new HashSet<>();
        boolean committed = false;

        // Stored for retransmission to unacked regions on client retry.
        // Cleared on commit to avoid holding crypto material longer than needed.
        byte[][]           fragments;
        byte[][]           keyShares;
        Map<Integer, byte[]> checksums;

        PendingWrite(String clientId, int seqNum, String key, int version,
                     Address clientSender, byte[][] fragments, byte[][] keyShares,
                     Map<Integer, byte[]> checksums) {
            this.clientId     = clientId;
            this.seqNum       = seqNum;
            this.key          = key;
            this.version      = version;
            this.clientSender = clientSender;
            this.fragments    = fragments;
            this.keyShares    = keyShares;
            this.checksums    = checksums;
        }
    }

    private static final class PendingRead {
        final String  clientId;
        final int     seqNum;
        final String  key;
        final int     version;
        final Address clientSender;
        final Map<Integer, byte[]> fragments = new HashMap<>();
        final Map<Integer, byte[]> keyShares = new HashMap<>();
        boolean completed = false;

        PendingRead(String clientId, int seqNum, String key, int version, Address clientSender) {
            this.clientId     = clientId;
            this.seqNum       = seqNum;
            this.key          = key;
            this.version      = version;
            this.clientSender = clientSender;
        }
    }

    // =========================================================================
    //  Observability (read-only diagnostics for tests)
    // =========================================================================

    /** Returns the number of versions stored for a key (for GC verification). */
    public int getVersionCount(String key) {
        Map<Integer, VersionMetadata> versions = allVersions.get(key);
        return (versions != null) ? versions.size() : 0;
    }

    /** Returns the total number of versions across all keys. */
    public int getTotalVersionCount() {
        int total = 0;
        for (Map<Integer, VersionMetadata> v : allVersions.values()) {
            total += v.size();
        }
        return total;
    }

    /** Returns the current number of regions. */
    public int getRegionCount() {
        return regions.size();
    }

    // =========================================================================
    //  Utility
    // =========================================================================

    private void log(String msg) {
        System.out.println("[coordinator] " + msg);
    }
}
