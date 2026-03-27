package dslabs.capstone;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A regional node in the distributed KV store.
 *
 * Extends dslabs.framework.Node — message handlers are named handleFoo(Foo, Address)
 * and are dispatched via reflection, exactly as in the Paxos lab.
 *
 * Responsibilities:
 *   1. Fragment store: durably holds one ciphertext fragment per (key, version).
 *   2. Key-share holder: durably holds one Shamir share of the AES key per (key, version).
 *
 * The region knows nothing about other regions.  It cannot reconstruct the full
 * ciphertext or the full AES key alone.
 *
 * Region indices are learned per-message (from FragmentWrite.regionIndex), so a
 * single RegionalNode class serves all positions in the erasure coding scheme.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class RegionalNode extends Node {

    // fragment storage:   objectKey -> (version -> fragment bytes)
    private Map<String, Map<Integer, byte[]>> fragments;

    // key share storage:  objectKey -> (version -> share bytes)
    private Map<String, Map<Integer, byte[]>> keyShares;

    // If true, flip a byte in every fragment returned on read.
    // Used in corruption injection tests to verify coordinator checksum detection.
    private final boolean corruptFragmentsOnRead;

    // --- Dynamic membership: if set, region sends JoinRequest on init ---
    private final Address coordinatorAddress;
    private final byte[]  clusterSecret;

    public RegionalNode(Address address) {
        this(address, false, null, null);
    }

    public RegionalNode(Address address, boolean corruptFragmentsOnRead) {
        this(address, corruptFragmentsOnRead, null, null);
    }

    public RegionalNode(Address address, boolean corruptFragmentsOnRead,
                        Address coordinatorAddress, byte[] clusterSecret) {
        super(address);
        this.corruptFragmentsOnRead = corruptFragmentsOnRead;
        this.coordinatorAddress     = coordinatorAddress;
        this.clusterSecret          = clusterSecret;
    }

    @Override
    public void init() {
        fragments = new HashMap<>();
        keyShares = new HashMap<>();

        // If configured for dynamic join, send JoinRequest to coordinator
        if (coordinatorAddress != null && clusterSecret != null) {
            byte[] hmac = CryptoUtil.hmacSha256(clusterSecret, address().toString().getBytes());
            send(new JoinRequest(hmac), coordinatorAddress);
            log("Sent JoinRequest to " + coordinatorAddress);
        }
    }

    // -------------------------------------------------------------------------
    //  Write handlers
    // -------------------------------------------------------------------------

    /**
     * Store the ciphertext fragment for this (key, version).
     *
     * Duplicate writes for the same version are rejected.  The coordinator
     * only sends one FragmentWrite per region per version, so a duplicate
     * arriving here indicates either a retransmission (message duplication)
     * or a logic error.  Rejecting is safe: the coordinator won't count this
     * ack toward its threshold.
     */
    private void handleFragmentWrite(FragmentWrite m, Address sender) {
        log("Received " + m);

        Map<Integer, byte[]> byVersion =
            fragments.computeIfAbsent(m.key(), k -> new HashMap<>());

        if (byVersion.containsKey(m.version())) {
            // Already stored — idempotent ack. This handles retransmissions
            // where the original ack was lost in an unreliable network.
            log("Duplicate FragmentWrite for key=" + m.key() + " v=" + m.version() + " — acking success");
            send(new FragmentAck(m.key(), m.version(), m.regionIndex(), true), sender);
            return;
        }

        byVersion.put(m.version(), Arrays.copyOf(m.fragment(), m.fragment().length));
        log("Stored fragment for key=" + m.key() + " v=" + m.version()
            + " (" + m.fragment().length + " bytes)");

        send(new FragmentAck(m.key(), m.version(), m.regionIndex(), true), sender);
    }

    /**
     * Store the Shamir key share for this (key, version).
     * Same rejection logic as handleFragmentWrite.
     */
    private void handleKeyShareWrite(KeyShareWrite m, Address sender) {
        log("Received " + m);

        Map<Integer, byte[]> byVersion =
            keyShares.computeIfAbsent(m.key(), k -> new HashMap<>());

        if (byVersion.containsKey(m.version())) {
            log("Duplicate KeyShareWrite for key=" + m.key() + " v=" + m.version() + " — acking success");
            send(new KeyShareAck(m.key(), m.version(), m.regionIndex(), true), sender);
            return;
        }

        byVersion.put(m.version(), Arrays.copyOf(m.keyShare(), m.keyShare().length));
        log("Stored key share for key=" + m.key() + " v=" + m.version());

        send(new KeyShareAck(m.key(), m.version(), m.regionIndex(), true), sender);
    }

    // -------------------------------------------------------------------------
    //  Read handlers
    // -------------------------------------------------------------------------

    /**
     * Return the stored fragment for (key, version).
     * Replies with fragment=null if not found (stale version request, or
     * region was down when the write happened).
     */
    private void handleFragmentReadRequest(FragmentReadRequest req, Address sender) {
        log("Received " + req);

        byte[] fragment = null;
        Map<Integer, byte[]> byVersion = fragments.get(req.key());
        if (byVersion != null && byVersion.containsKey(req.version())) {
            byte[] stored = byVersion.get(req.version());
            fragment = Arrays.copyOf(stored, stored.length);
        }

        if (fragment == null) {
            log("Fragment not found for key=" + req.key() + " v=" + req.version());
        } else if (corruptFragmentsOnRead) {
            // Flip a byte to simulate storage corruption or a malicious region
            fragment[0] = (byte) (fragment[0] ^ 0xFF);
            log("*** CORRUPTING fragment for key=" + req.key() + " v=" + req.version() + " ***");
        }

        send(new FragmentReadReply(req.key(), req.version(), req.regionIndex(), fragment), sender);
    }

    /**
     * Return the stored key share for (key, version).
     */
    private void handleKeyShareReadRequest(KeyShareReadRequest req, Address sender) {
        log("Received " + req);

        byte[] share = null;
        Map<Integer, byte[]> byVersion = keyShares.get(req.key());
        if (byVersion != null && byVersion.containsKey(req.version())) {
            byte[] stored = byVersion.get(req.version());
            share = Arrays.copyOf(stored, stored.length);
        }

        if (share == null)
            log("Key share not found for key=" + req.key() + " v=" + req.version());

        send(new KeyShareReadReply(req.key(), req.version(), req.regionIndex(), share), sender);
    }

    // -------------------------------------------------------------------------
    //  Heartbeat
    // -------------------------------------------------------------------------

    private void handleHeartbeatMsg(HeartbeatMsg m, Address sender) {
        // Just reply — the reply itself is what the coordinator needs.
        send(new HeartbeatReply(), sender);
    }

    // -------------------------------------------------------------------------
    //  Dynamic membership
    // -------------------------------------------------------------------------

    private void handleJoinResult(JoinResult result, Address sender) {
        if (result.success()) {
            log("*** Successfully joined cluster ***");
        } else {
            log("Join rejected: " + result.error());
        }
    }

    // -------------------------------------------------------------------------
    //  Garbage collection
    // -------------------------------------------------------------------------

    private void handleDeleteVersionData(DeleteVersionData req, Address sender) {
        // Idempotent: deleting non-existent data is a no-op.
        boolean deletedFragment = false;
        boolean deletedShare = false;

        Map<Integer, byte[]> fragByVersion = fragments.get(req.key());
        if (fragByVersion != null) {
            deletedFragment = fragByVersion.remove(req.version()) != null;
            if (fragByVersion.isEmpty()) fragments.remove(req.key());
        }

        Map<Integer, byte[]> shareByVersion = keyShares.get(req.key());
        if (shareByVersion != null) {
            deletedShare = shareByVersion.remove(req.version()) != null;
            if (shareByVersion.isEmpty()) keyShares.remove(req.key());
        }

        if (deletedFragment || deletedShare) {
            log("GC: deleted v=" + req.version() + " for key=" + req.key()
                + " (fragment=" + deletedFragment + ", share=" + deletedShare + ")");
        }
    }

    // -------------------------------------------------------------------------
    //  Utility
    // -------------------------------------------------------------------------

    private void log(String msg) {
        System.out.println("[" + address() + "] " + msg);
    }
}
