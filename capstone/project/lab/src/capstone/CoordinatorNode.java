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
import javax.crypto.Mac;
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

    // --- Fixed configuration (set in constructor, immutable) ---
    private final Address[] regions;    // ordered list of region addresses
    private final int       k;          // erasure coding: data fragments
    private final int       m;          // erasure coding: parity fragments
    private final int       keyThreshold;

    // Precomputed erasure coder (stateless, safe to share)
    private final ErasureCoder erasureCoder;

    // --- Authentication secrets (clientId -> pre-shared secret, immutable) ---
    private final Map<String, byte[]> clientSecrets;

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
    // clientId -> (seqNum -> cached WriteResponse)
    private Map<String, Map<Integer, WriteResponse>> writeDedup;

    // --- Region liveness (region address -> last heartbeat reply time ms) ---
    private Map<Address, Long> lastHeartbeat;

    // --- Authentication state ---
    // clientId -> nonce (pending challenge-response)
    private Map<String, byte[]> pendingAuths;
    // sessionToken -> clientId (validated sessions)
    private Map<String, String> validSessions;

    // --- Per-key ownership (key -> owning clientId) ---
    private Map<String, String> keyOwner;

    public CoordinatorNode(Address address, Address[] regions, int k, int m,
                           int keyThreshold, Map<String, byte[]> clientSecrets) {
        super(address);
        this.regions       = regions;
        this.k             = k;
        this.m             = m;
        this.keyThreshold  = keyThreshold;
        this.erasureCoder  = new ErasureCoder(k, m);
        this.clientSecrets = clientSecrets;
    }

    @Override
    public void init() {
        allVersions     = new HashMap<>();
        latestCommitted = new HashMap<>();
        nextVersion     = new HashMap<>();
        pendingWrites   = new HashMap<>();
        pendingReads    = new HashMap<>();
        writeDedup      = new HashMap<>();
        lastHeartbeat   = new HashMap<>();
        pendingAuths    = new HashMap<>();
        validSessions   = new HashMap<>();
        keyOwner        = new HashMap<>();

        // Assume all regions alive at startup; they'll confirm via heartbeat.
        long now = System.currentTimeMillis();
        for (Address r : regions) lastHeartbeat.put(r, now);

        // Start the recurring heartbeat timer.
        set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_MILLIS);
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

        // Generate nonce and send challenge
        byte[] nonce = new byte[16];
        RNG.nextBytes(nonce);
        pendingAuths.put(req.clientId(), nonce);
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
        byte[] expected = hmacSha256(secret, nonce);

        if (!Arrays.equals(expected, resp.hmac())) {
            log("Auth FAILED for " + resp.clientId() + " — HMAC mismatch");
            pendingAuths.remove(resp.clientId());
            send(new AuthResultMsg(resp.clientId(), null, false, "AUTH_FAILED"), sender);
            return;
        }

        // Issue session token
        byte[] tokenBytes = new byte[16];
        RNG.nextBytes(tokenBytes);
        StringBuilder sb = new StringBuilder();
        for (byte b : tokenBytes) sb.append(String.format("%02x", b & 0xff));
        String token = sb.toString();

        validSessions.put(token, resp.clientId());
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

    // =========================================================================
    //  WRITE PATH
    // =========================================================================

    private void handleWriteRequest(WriteRequest req, Address sender) {
        log("WriteRequest from " + req.clientId() + " seq=" + req.sequenceNum()
            + " key=" + req.key() + " token=" + (req.sessionToken() != null ? "present" : "MISSING"));

        // --- Session authentication check ---
        String authClient = validateSession(req.sessionToken());
        if (authClient == null) {
            log("Rejected: no valid session for " + req.clientId());
            send(new WriteResponse(req.clientId(), req.sequenceNum(), false, "AUTH_REQUIRED"), sender);
            return;
        }
        if (!authClient.equals(req.clientId())) {
            log("Rejected: session/clientId mismatch for " + req.clientId());
            send(new WriteResponse(req.clientId(), req.sequenceNum(), false, "IDENTITY_MISMATCH"), sender);
            return;
        }

        // --- Per-key ownership check ---
        String owner = keyOwner.get(req.key());
        if (owner != null && !owner.equals(authClient)) {
            log("Rejected: " + authClient + " is not owner of key=" + req.key());
            send(new WriteResponse(req.clientId(), req.sequenceNum(), false, "ACCESS_DENIED"), sender);
            return;
        }

        // --- AMO dedup check ---
        WriteResponse cached = getCachedWriteResponse(req.clientId(), req.sequenceNum());
        if (cached != null) {
            log("Dedup: replaying cached response for " + req.clientId() + " seq=" + req.sequenceNum());
            send(cached, sender);
            return;
        }

        // --- One pending write per key at a time ---
        if (pendingWrites.containsKey(req.key())) {
            log("Busy: write already in progress for key=" + req.key());
            send(new WriteResponse(req.clientId(), req.sequenceNum(), false, "BUSY"), sender);
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

        // --- Track the pending write ---
        PendingWrite pending = new PendingWrite(req.clientId(), req.sequenceNum(), req.key(), version, sender);
        pendingWrites.put(req.key(), pending);

        // --- Fan out: send fragment + key share to every region ---
        for (int i = 0; i < k + m; i++) {
            send(new FragmentWrite(req.key(), version, i, frags[i], checksums.get(i)), regions[i]);
            send(new KeyShareWrite(req.key(), version, i, shares[i]), regions[i]);
        }
        log("Sent to " + (k + m) + " regions; waiting for acks");

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

        WriteResponse resp = new WriteResponse(pending.clientId, pending.seqNum, true, null);
        cacheWriteResponse(pending.clientId, pending.seqNum, resp);
        send(resp, pending.clientSender);

        pendingWrites.remove(pending.key);
    }

    // =========================================================================
    //  READ PATH
    // =========================================================================

    private void handleReadRequest(ReadRequest req, Address sender) {
        log("ReadRequest from " + req.clientId() + " seq=" + req.sequenceNum()
            + " key=" + req.key() + " token=" + (req.sessionToken() != null ? "present" : "MISSING"));

        // --- Session authentication check ---
        String authClient = validateSession(req.sessionToken());
        if (authClient == null) {
            log("Rejected: no valid session for " + req.clientId());
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, "AUTH_REQUIRED"), sender);
            return;
        }
        if (!authClient.equals(req.clientId())) {
            log("Rejected: session/clientId mismatch for " + req.clientId());
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, "IDENTITY_MISMATCH"), sender);
            return;
        }

        // --- Per-key ownership check ---
        String owner = keyOwner.get(req.key());
        if (owner != null && !owner.equals(authClient)) {
            log("Rejected: " + authClient + " is not owner of key=" + req.key());
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, "ACCESS_DENIED"), sender);
            return;
        }

        if (pendingReads.containsKey(req.key())) {
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, "READ_IN_PROGRESS"), sender);
            return;
        }

        Integer version = latestCommitted.get(req.key());
        if (version == null) {
            log("Key not found: " + req.key());
            send(new ReadResponse(req.clientId(), req.sequenceNum(), null, "KEY_NOT_FOUND"), sender);
            return;
        }

        log("Reading key=" + req.key() + " at v=" + version);

        PendingRead pending = new PendingRead(req.clientId(), req.sequenceNum(), req.key(), version, sender);
        pendingReads.put(req.key(), pending);

        // Fan out to all regions simultaneously — use whichever k fragments +
        // keyThreshold shares respond first.
        for (int i = 0; i < k + m; i++) {
            send(new FragmentReadRequest(req.key(), version, i), regions[i]);
            send(new KeyShareReadRequest(req.key(), version, i), regions[i]);
        }
        log("Sent fragment + key-share requests to " + (k + m) + " regions");

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

        // Integrity check: reject corrupted or tampered fragments
        VersionMetadata meta = allVersions.get(reply.key()).get(reply.version());
        if (!meta.verifyFragment(reply.regionIndex(), reply.fragment())) {
            log("*** Checksum FAILED for fragment from region-" + reply.regionIndex()
                + " key=" + reply.key() + " — rejecting ***");
            return;
        }

        pending.fragments.put(reply.regionIndex(), reply.fragment());
        log("Accepted fragment from region-" + reply.regionIndex()
            + " (have " + pending.fragments.size() + "/" + k + ")");
        tryCompleteRead(pending);
    }

    private void handleKeyShareReadReply(KeyShareReadReply reply, Address sender) {
        PendingRead pending = pendingReads.get(reply.key());
        if (pending == null || pending.version != reply.version() || pending.completed) return;

        if (reply.keyShare() == null) {
            log("Region-" + reply.regionIndex() + " has no key share for key=" + reply.key());
            return;
        }

        pending.keyShares.put(reply.regionIndex(), reply.keyShare());
        log("Accepted key share from region-" + reply.regionIndex()
            + " (have " + pending.keyShares.size() + "/" + keyThreshold + ")");
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
        if (pending.fragments.size() < k)            return;
        if (pending.keyShares.size() < keyThreshold) return;

        pending.completed = true;

        VersionMetadata meta = allVersions.get(pending.key).get(pending.version);

        // Reconstruct ciphertext from k fragments
        byte[][] allFrags = new byte[k + m][];
        pending.fragments.forEach((idx, frag) -> allFrags[idx] = frag);
        byte[] ciphertext = erasureCoder.decode(allFrags, meta.originalCiphertextLength);
        log("Reconstructed ciphertext from " + pending.fragments.size() + " fragments");

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
        for (Address region : regions) send(new HeartbeatMsg(), region);
        set(t, HeartbeatTimer.HEARTBEAT_MILLIS);  // re-arm
    }

    private void handleHeartbeatReply(HeartbeatReply reply, Address sender) {
        lastHeartbeat.put(sender, System.currentTimeMillis());
        log("Heartbeat reply from " + sender);
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
    //  HMAC-SHA256
    // =========================================================================

    private static byte[] hmacSha256(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception e) { throw new RuntimeException("HMAC failed", e); }
    }

    // =========================================================================
    //  AMO dedup helpers
    // =========================================================================

    private WriteResponse getCachedWriteResponse(String clientId, int seqNum) {
        Map<Integer, WriteResponse> m = writeDedup.get(clientId);
        return (m != null) ? m.get(seqNum) : null;
    }

    private void cacheWriteResponse(String clientId, int seqNum, WriteResponse resp) {
        writeDedup.computeIfAbsent(clientId, x -> new HashMap<>()).put(seqNum, resp);
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

        PendingWrite(String clientId, int seqNum, String key, int version, Address clientSender) {
            this.clientId     = clientId;
            this.seqNum       = seqNum;
            this.key          = key;
            this.version      = version;
            this.clientSender = clientSender;
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
