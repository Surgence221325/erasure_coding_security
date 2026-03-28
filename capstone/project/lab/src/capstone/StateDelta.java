package dslabs.capstone;

import java.io.Serializable;
import java.util.List;
import lombok.Data;

/**
 * Describes a state machine mutation to be applied when a Raft log entry commits.
 *
 * Each delta represents one of:
 *   - WRITE_COMMIT: a new version committed (key, version, metadata, owner, dedup)
 *   - AUTH_SESSION: a new client session established (clientId, sessionToken)
 *   - CONFIG_CHANGE: a region joined (new region address, updated m)
 *
 * Followers apply these deltas to their local state in applyLogEntry().
 * The leader creates them after completing the operation (e.g., after region acks).
 */
@Data
public class StateDelta implements Serializable {

    public enum Type { WRITE_COMMIT, AUTH_SESSION, CONFIG_CHANGE }

    private final Type type;

    // --- WRITE_COMMIT fields ---
    private final String          key;
    private final int             version;
    private final VersionMetadata metadata;       // null for non-write deltas
    private final String          ownerClientId;  // null if ownership unchanged
    private final String          dedupClientId;  // for AMO
    private final int             dedupSeqNum;
    private final List<Integer>   oldVersionsToDelete;  // versions to GC (null if none)

    // --- AUTH_SESSION fields ---
    private final String          authClientId;
    private final String          sessionToken;

    // --- CONFIG_CHANGE fields ---
    private final dslabs.framework.Address newRegionAddress;
    private final int             newM;           // updated m after join

    /** Create a write-commit delta. */
    public static StateDelta writeCommit(String key, int version, VersionMetadata metadata,
                                         String ownerClientId, String dedupClientId, int dedupSeqNum,
                                         List<Integer> oldVersionsToDelete) {
        return new StateDelta(Type.WRITE_COMMIT, key, version, metadata, ownerClientId,
                dedupClientId, dedupSeqNum, oldVersionsToDelete, null, null, null, 0);
    }

    /** Create an auth-session delta. */
    public static StateDelta authSession(String clientId, String sessionToken) {
        return new StateDelta(Type.AUTH_SESSION, null, 0, null, null,
                null, 0, null, clientId, sessionToken, null, 0);
    }

    /** Create a config-change delta (region join). */
    public static StateDelta configChange(dslabs.framework.Address newRegion, int newM) {
        return new StateDelta(Type.CONFIG_CHANGE, null, 0, null, null,
                null, 0, null, null, null, newRegion, newM);
    }
}
