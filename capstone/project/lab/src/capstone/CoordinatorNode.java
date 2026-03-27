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
 * The coordinator — the control plane of the distributed KV store.
 *
 * Extends dslabs.framework.Node.  Message handlers follow the framework naming
 * convention (handleFoo / onFooTimer) and are dispatched via reflection.
 *
 * The coordinator is the only node clients talk to.  It:
 *
 *   WRITE PATH
 *   ----------
 *   1. Deduplicates the request via (clientId, seqNum).
 *   2. Creates a new version, encrypts with a fresh AES-128/CBC key.
 *   3. Erasure-codes the ciphertext into k+m fragments.
 *   4. Splits the AES key via Shamir (threshold = keyThreshold).
 *   5. Distributes one fragment + one key share to each region.
 *   6. Commits once >= k fragment acks AND >= keyThreshold key-share acks arrive.
 *   7. A WriteTimeoutTimer fires if acks don't arrive in time — fails the write.
 *
 *   READ PATH
 *   ---------
 *   1. Looks up the latest committed version.
 *   2. Fans out fragment + key-share requests to all regions simultaneously.
 *   3. Verifies each fragment's SHA-256 checksum before accepting it.
 *   4. Once k valid fragments + keyThreshold valid key shares are collected:
 *      reconstructs ciphertext → reconstructs AES key → decrypts → returns plaintext.
 *
 *   LIVENESS
 *   ---------
 *   A HeartbeatTimer fires periodically; the coordinator pings all regions and
 *   tracks reply timestamps.  Down regions are skipped in future decisions
 *   (though the bus already drops their messages — this is belt-and-suspenders).
 *
 * Design decisions:
 *   - Single coordinator, no replication (explicitly out of scope).
 *   - One pending write per key at a time.
 *   - One pending read per key at a time.
 *   - AES key never stored at coordinator — generated, distributed, discarded.
 *
 *   AUTHENTICATION
 *   ---------------
 *   Clients authenticate via a challenge-response handshake before issuing
 *   any commands.  The coordinator holds a pre-shared secret per client.
 *   1. Client sends AuthRequest(clientId).
 *   2. Coordinator replies with AuthChallenge(nonce) — a random 16-byte nonce.
 *   3. Client computes HMAC-SHA256(sharedSecret, nonce) and sends AuthResponse.
 *   4. Coordinator verifies the HMAC, issues a session token on success.
 *   All subsequent WriteRequest/ReadRequest carry the session token.
 *   Requests with missing or invalid tokens are rejected with AUTH_REQUIRED.
 *
 *   AUTHORIZATION (per-key ownership)
 *   ----------------------------------
 *   The first client to successfully commit a write to a key becomes its owner.
 *   Subsequent writes or reads from a different client are rejected with
 *   ACCESS_DENIED.  Ownership is permanent (no transfer or revocation).
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

    public CoordinatorNode(Address address, Address[] regions, int k, int m,
                           int keyThreshold, Map<String, byte[]> clientSecrets) {
        this(address, regions, k, m, keyThreshold, clientSecrets, true, null);
    }

    public CoordinatorNode(Address address, Address[] regions, int k, int m,
                           int keyThreshold, Map<String, byte[]> clientSecrets,
                           boolean enableHeartbeats) {
        this(address, regions, k, m, keyThreshold, clientSecrets, enableHeartbeats, null);
    }

    public CoordinatorNode(Address address, Address[] regions, int k, int m,
                           int keyThreshold, Map<String, byte[]> clientSecrets,
                           boolean enableHeartbeats, byte[] clusterSecret) {
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

        validSessions.put(token, resp.clientId());
        clientToToken.put(resp.clientId(), token);
        pendingAuths.remove(resp.clientId());
        log("*** Authenticated " + resp.clientId() + " token=" + token + " ***");
        send(new AuthResultMsg(resp.clientId(), token, true, null), sender);
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

    // =========================================================================
    //  WRITE PATH
    // =========================================================================

    private void handleWriteRequest(WriteRequest req, Address sender) {
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
     */
    private void tryCommitWrite(PendingWrite pending) {
        if (pending.committed) return;
        if (pending.fragmentAcks.size() < k)            return;
        if (pending.keyShareAcks.size() < keyThreshold) return;

        pending.committed = true;

        VersionMetadata meta = allVersions.get(pending.key).get(pending.version);
        meta.committed = true;
        latestCommitted.put(pending.key, pending.version);
        boolean newOwner = keyOwner.putIfAbsent(pending.key, pending.clientId) == null;

        log("*** COMMITTED key=" + pending.key + " v=" + pending.version
            + " (frags=" + pending.fragmentAcks.size() + "/" + k
            + ", shares=" + pending.keyShareAcks.size() + "/" + keyThreshold + ")"
            + (newOwner ? " owner=" + pending.clientId : "") + " ***");

        // Clear stored crypto material — no longer needed after commit
        pending.fragments = null;
        pending.keyShares = null;
        pending.checksums = null;

        WriteResponse resp = new WriteResponse(pending.clientId, pending.seqNum, true, null);
        cacheWriteResponse(pending.clientId, pending.seqNum, resp);
        send(resp, pending.clientSender);

        pendingWrites.remove(pending.key);

        // If reconfiguring and all writes drained, apply the pending join
        if (reconfiguring && pendingWrites.isEmpty()) {
            applyJoin();
        }
    }

    // =========================================================================
    //  READ PATH
    // =========================================================================

    private void handleReadRequest(ReadRequest req, Address sender) {
        log("ReadRequest from " + req.clientId() + " seq=" + req.sequenceNum()
            + " key=" + req.key() + " token=" + (req.sessionToken() != null ? "present" : "MISSING"));

        // --- Authentication + authorization ---
        String[] errorOut = new String[1];
        String authClient = validateAndAuthorize(req.clientId(), req.sessionToken(), req.key(), errorOut);
        if (authClient == null) {
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, errorOut[0]), sender);
            return;
        }

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
        regions.add(newRegion);
        m = m + 1;
        erasureCoder = new ErasureCoder(k, m);
        missedHeartbeats.put(newRegion, 0);
        reconfiguring     = false;
        pendingJoinRegion = null;

        log("*** JOIN COMPLETE: added " + newRegion
            + " — now " + regions.size() + " regions (k=" + k + ", m=" + m
            + ", keyThreshold=" + keyThreshold + ") ***");
        send(new JoinResult(true, null), newRegion);
    }

    private void onReconfigTimeoutTimer(ReconfigTimeoutTimer t) {
        if (!reconfiguring) return;

        log("*** RECONFIG TIMEOUT: aborting join of " + pendingJoinRegion
            + " (" + pendingWrites.size() + " writes still pending) ***");
        Address failedRegion = pendingJoinRegion;
        reconfiguring     = false;
        pendingJoinRegion = null;
        send(new JoinResult(false, "RECONFIG_TIMEOUT"), failedRegion);
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
        // Note: the partial version remains in allVersions but is uncommitted,
        // so it will never be visible to readers.  It can be GC'd later.

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

    private static final class DedupEntry {
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
    //  Utility
    // =========================================================================

    private void log(String msg) {
        System.out.println("[coordinator] " + msg);
    }
}
