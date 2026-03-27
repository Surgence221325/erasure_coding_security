package dslabs.capstone;

import dslabs.framework.Message;
import java.util.Arrays;
import lombok.Data;
import lombok.NonNull;

/*
 * All message types in the capstone protocol.
 *
 * Every class implements dslabs.framework.Message (which extends Serializable).
 * @Data gives us equals(), hashCode(), toString() automatically — a requirement
 * of the dslabs framework.
 *
 * Note: @Data won't generate correct equals/hashCode for byte[] fields since
 * arrays use reference equality by default.  For messages carrying byte arrays
 * we override equals/hashCode manually, or we accept reference equality for
 * the demo (the framework's correctness checks don't inspect message payloads).
 *
 * Groups:
 *   0. Authentication (challenge-response handshake)
 *   1. Client <-> Coordinator
 *   2. Coordinator -> Region  (writes)
 *   3. Coordinator -> Region  (reads)
 *   4. Region -> Coordinator  (acks and replies)
 *   5. Heartbeat
 */

// =============================================================================
//  0. Authentication (challenge-response)
// =============================================================================

/** Client initiates authentication. */
@Data
final class AuthRequest implements Message {
    @NonNull private final String clientId;
}

/** Coordinator sends a random nonce for the client to prove identity. */
@Data
final class AuthChallenge implements Message {
    @NonNull private final String clientId;
    @NonNull private final byte[] nonce;

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AuthChallenge)) return false;
        AuthChallenge that = (AuthChallenge) o;
        return clientId.equals(that.clientId) && Arrays.equals(nonce, that.nonce);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(clientId, Arrays.hashCode(nonce));
    }
}

/** Client proves identity by returning HMAC(sharedSecret, nonce). */
@Data
final class AuthResponse implements Message {
    @NonNull private final String clientId;
    @NonNull private final byte[] hmac;

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AuthResponse)) return false;
        AuthResponse that = (AuthResponse) o;
        return clientId.equals(that.clientId) && Arrays.equals(hmac, that.hmac);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(clientId, Arrays.hashCode(hmac));
    }
}

/** Coordinator grants (or denies) a session token after verifying the HMAC. */
@Data
final class AuthResultMsg implements Message {
    @NonNull private final String clientId;
    private final String          sessionToken;  // null on failure
    private final boolean         success;
    private final String          error;         // null on success
}

// =============================================================================
//  1. Client <-> Coordinator
// =============================================================================

/** Client requests a write. Carries (clientId, seqNum) for AMO dedup and sessionToken for auth. */
@Data
final class WriteRequest implements Message {
    @NonNull private final String clientId;
    private final int             sequenceNum;
    @NonNull private final String key;
    @NonNull private final byte[] value;  // plaintext
    private final String          sessionToken;

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WriteRequest)) return false;
        WriteRequest that = (WriteRequest) o;
        return sequenceNum == that.sequenceNum
            && clientId.equals(that.clientId)
            && key.equals(that.key)
            && Arrays.equals(value, that.value)
            && java.util.Objects.equals(sessionToken, that.sessionToken);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(clientId, sequenceNum, key,
            Arrays.hashCode(value), sessionToken);
    }
}

/** Coordinator's reply to a write request. */
@Data
final class WriteResponse implements Message {
    @NonNull private final String clientId;
    private final int             sequenceNum;
    private final boolean         success;
    private final String          error;   // null on success
}

/** Client requests a read. */
@Data
final class ReadRequest implements Message {
    @NonNull private final String clientId;
    private final int             sequenceNum;
    @NonNull private final String key;
    private final String          sessionToken;
}

/** Coordinator returns decrypted plaintext (or error) to the client. */
@Data
final class ReadResponse implements Message {
    @NonNull private final String clientId;
    private final int             sequenceNum;
    private final byte[]          value;   // null on error
    private final String          error;   // null on success

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReadResponse)) return false;
        ReadResponse that = (ReadResponse) o;
        return sequenceNum == that.sequenceNum
            && clientId.equals(that.clientId)
            && Arrays.equals(value, that.value)
            && java.util.Objects.equals(error, that.error);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(clientId, sequenceNum, Arrays.hashCode(value), error);
    }
}

// =============================================================================
//  2. Coordinator -> Region  (write phase)
// =============================================================================

/** Coordinator sends a ciphertext fragment to a region for storage. */
@Data
final class FragmentWrite implements Message {
    @NonNull private final String key;
    private final int             version;
    private final int             regionIndex;
    @NonNull private final byte[] fragment;
    @NonNull private final byte[] checksum;   // SHA-256 of fragment

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FragmentWrite)) return false;
        FragmentWrite that = (FragmentWrite) o;
        return version == that.version && regionIndex == that.regionIndex
            && key.equals(that.key)
            && Arrays.equals(fragment, that.fragment)
            && Arrays.equals(checksum, that.checksum);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(key, version, regionIndex,
            Arrays.hashCode(fragment), Arrays.hashCode(checksum));
    }
}

/** Coordinator sends a Shamir key share to a region for storage. */
@Data
final class KeyShareWrite implements Message {
    @NonNull private final String key;
    private final int             version;
    private final int             regionIndex;
    @NonNull private final byte[] keyShare;

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyShareWrite)) return false;
        KeyShareWrite that = (KeyShareWrite) o;
        return version == that.version && regionIndex == that.regionIndex
            && key.equals(that.key)
            && Arrays.equals(keyShare, that.keyShare);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(key, version, regionIndex, Arrays.hashCode(keyShare));
    }
}

// =============================================================================
//  3. Coordinator -> Region  (read phase)
// =============================================================================

/** Coordinator requests a region's stored ciphertext fragment. */
@Data
final class FragmentReadRequest implements Message {
    @NonNull private final String key;
    private final int             version;
    private final int             regionIndex;
}

/** Coordinator requests a region's stored key share. */
@Data
final class KeyShareReadRequest implements Message {
    @NonNull private final String key;
    private final int             version;
    private final int             regionIndex;
}

// =============================================================================
//  4. Region -> Coordinator  (acks and replies)
// =============================================================================

/** Region acknowledges (or rejects) a fragment write. */
@Data
final class FragmentAck implements Message {
    @NonNull private final String key;
    private final int             version;
    private final int             regionIndex;
    private final boolean         success;
}

/** Region acknowledges (or rejects) a key share write. */
@Data
final class KeyShareAck implements Message {
    @NonNull private final String key;
    private final int             version;
    private final int             regionIndex;
    private final boolean         success;
}

/** Region returns its stored ciphertext fragment (null if not found). */
@Data
final class FragmentReadReply implements Message {
    @NonNull private final String key;
    private final int             version;
    private final int             regionIndex;
    private final byte[]          fragment;  // null = not found

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FragmentReadReply)) return false;
        FragmentReadReply that = (FragmentReadReply) o;
        return version == that.version && regionIndex == that.regionIndex
            && key.equals(that.key)
            && Arrays.equals(fragment, that.fragment);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(key, version, regionIndex, Arrays.hashCode(fragment));
    }
}

/** Region returns its stored key share (null if not found). */
@Data
final class KeyShareReadReply implements Message {
    @NonNull private final String key;
    private final int             version;
    private final int             regionIndex;
    private final byte[]          keyShare;  // null = not found

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyShareReadReply)) return false;
        KeyShareReadReply that = (KeyShareReadReply) o;
        return version == that.version && regionIndex == that.regionIndex
            && key.equals(that.key)
            && Arrays.equals(keyShare, that.keyShare);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(key, version, regionIndex, Arrays.hashCode(keyShare));
    }
}

// =============================================================================
//  5. Heartbeat
// =============================================================================

/** Coordinator pings a region to confirm it's alive. */
@Data
final class HeartbeatMsg implements Message {
    // No payload needed — the ping itself is the signal.
}

/** Region's response to a heartbeat ping. */
@Data
final class HeartbeatReply implements Message {
    // No payload needed — the reply itself confirms liveness.
}

// =============================================================================
//  6. Dynamic Membership
// =============================================================================

/** Region requests to join the cluster. Includes HMAC proof of cluster secret. */
@Data
final class JoinRequest implements Message {
    @NonNull private final byte[] hmac;  // HMAC(clusterSecret, regionAddress)

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinRequest)) return false;
        return Arrays.equals(hmac, ((JoinRequest) o).hmac);
    }
    @Override public int hashCode() {
        return Arrays.hashCode(hmac);
    }
}

/** Coordinator's response to a JoinRequest. */
@Data
final class JoinResult implements Message {
    private final boolean success;
    private final String  error;  // null on success
}
