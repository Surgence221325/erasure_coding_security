package dslabs.capstone;

import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.Arrays;
import lombok.Data;
import lombok.NonNull;

/*
 * Application-level command and result types for the capstone KV store.
 *
 * These implement dslabs.framework.Command and dslabs.framework.Result,
 * which the Client interface uses to interact with the system.
 *
 * CapstoneWrite / CapstoneRead are what the demo passes to client.sendCommand().
 * CapstoneWriteResult / CapstoneReadResult are what client.getResult() returns.
 */

// =============================================================================
//  Commands
// =============================================================================

/** Write (put) a value for the given key. */
@Data
final class CapstoneWrite implements Command {
    @NonNull private final String key;
    @NonNull private final byte[] value;  // plaintext bytes to store

    @Override public boolean readOnly() { return false; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CapstoneWrite)) return false;
        CapstoneWrite that = (CapstoneWrite) o;
        return key.equals(that.key) && Arrays.equals(value, that.value);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(key, Arrays.hashCode(value));
    }
}

/** Read (get) the value for the given key. */
@Data
final class CapstoneRead implements Command {
    @NonNull private final String key;

    @Override public boolean readOnly() { return true; }
}

// =============================================================================
//  Results
// =============================================================================

/** Result of a write operation. */
@Data
final class CapstoneWriteResult implements Result {
    private final boolean success;
    private final String  error;   // null on success
}

/** Result of a read operation. */
@Data
final class CapstoneReadResult implements Result {
    private final byte[] value;  // null on failure
    private final String error;  // null on success

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CapstoneReadResult)) return false;
        CapstoneReadResult that = (CapstoneReadResult) o;
        return Arrays.equals(value, that.value)
            && java.util.Objects.equals(error, that.error);
    }
    @Override public int hashCode() {
        return java.util.Objects.hash(Arrays.hashCode(value), error);
    }

    /** Convenience: decode value as a UTF-8 string, or null if absent. */
    public String valueAsString() {
        return value != null ? new String(value) : null;
    }
}
