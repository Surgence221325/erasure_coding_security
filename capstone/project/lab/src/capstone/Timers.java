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
 *
 * Client retry timers use exponential backoff: the interval doubles each
 * retry up to a cap (MAX_BACKOFF_MILLIS).  This reduces wasted messages
 * during sustained failures while still recovering quickly from transient ones.
 */

/**
 * Client auth retry timer.
 * If the client hasn't received an AuthResult, it re-sends AuthRequest.
 * Uses exponential backoff: 100 → 200 → 400 → ... → 2000ms cap.
 */
@Data
final class AuthRetryTimer implements Timer {
    static final int INITIAL_MILLIS     = 100;
    static final int MAX_BACKOFF_MILLIS = 2000;
    private final int backoffMillis;

    AuthRetryTimer() { this.backoffMillis = INITIAL_MILLIS; }
    AuthRetryTimer(int backoffMillis) {
        this.backoffMillis = backoffMillis;
    }

    /** Return a new timer with doubled backoff (capped). */
    AuthRetryTimer nextBackoff() {
        return new AuthRetryTimer(Math.min(backoffMillis * 2, MAX_BACKOFF_MILLIS));
    }
}

/**
 * Client retry timer.
 * If the client hasn't received a response for its pending request, it
 * re-broadcasts the request.  Carries the sequence number so we can
 * ignore stale timer fires from a previous request.
 * Uses exponential backoff: 100 → 200 → 400 → ... → 2000ms cap.
 */
@Data
final class ClientRetryTimer implements Timer {
    static final int INITIAL_MILLIS     = 100;
    static final int MAX_BACKOFF_MILLIS = 2000;
    private final int sequenceNum;
    private final int backoffMillis;

    ClientRetryTimer(int sequenceNum) {
        this.sequenceNum   = sequenceNum;
        this.backoffMillis = INITIAL_MILLIS;
    }
    ClientRetryTimer(int sequenceNum, int backoffMillis) {
        this.sequenceNum   = sequenceNum;
        this.backoffMillis = backoffMillis;
    }

    /** Return a new timer with doubled backoff (capped). */
    ClientRetryTimer nextBackoff() {
        return new ClientRetryTimer(sequenceNum,
                Math.min(backoffMillis * 2, MAX_BACKOFF_MILLIS));
    }
}

/**
 * Coordinator heartbeat timer.
 * Fires periodically; the coordinator sends a HeartbeatMsg to each region
 * and re-sets this timer to keep it recurring.
 */
@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_MILLIS = 500;
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
