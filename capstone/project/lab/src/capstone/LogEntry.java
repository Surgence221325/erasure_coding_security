package dslabs.capstone;

import java.io.Serializable;
import lombok.Data;

/**
 * A single entry in the Raft replicated log.
 *
 * Each entry records the term in which it was created, its position in the log,
 * and a StateDelta describing the state machine mutation to apply on commit.
 */
@Data
public class LogEntry implements Serializable {
    private final int        term;
    private final int        index;
    private final StateDelta delta;
}
