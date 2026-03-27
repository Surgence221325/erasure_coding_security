package dslabs.capstone;

import dslabs.framework.Timer;
import lombok.Data;

/*
 * Timer types for the capstone protocol.
 *
 * Timers are set via Node.set(timer, millis) and delivered back to the same
 * node via the onFoo(FooTimer) handler naming convention — exactly like in the
 * Paxos lab.
 *
 * The framework delivers a timer by calling the handler even if the node's
 * state has changed since the timer was set.  Each handler should check
 * whether the timer is still relevant before acting on it.
 */

/**
 * Client retry timer.
 * If the client hasn't received a response for its pending request, it
 * re-broadcasts the request.  Carries the sequence number so we can
 * ignore stale timer fires from a previous request.
 */
@Data
final class ClientRetryTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;
    private final int sequenceNum;
}

/**
 * Coordinator heartbeat timer.
 * Fires periodically; the coordinator sends a HeartbeatMsg to each region
 * and re-sets this timer to keep it recurring.
 */
@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_MILLIS = 50;
}

/**
 * Coordinator write timeout timer.
 * Set when a write begins.  If we haven't committed by the time this fires
 * (because some region never acked), we fail the write gracefully rather than
 * blocking forever.
 * Carries (key, version) so stale fires from old write attempts are ignored.
 */
@Data
final class WriteTimeoutTimer implements Timer {
    static final int WRITE_TIMEOUT_MILLIS = 300;
    private final String key;
    private final int    version;
}

/**
 * Coordinator read timeout timer.
 * If we can't collect k valid fragments + keyThreshold shares within this window,
 * fail the read gracefully rather than blocking forever.
 */
@Data
final class ReadTimeoutTimer implements Timer {
    static final int READ_TIMEOUT_MILLIS = 300;
    private final String key;
    private final int    version;
}
