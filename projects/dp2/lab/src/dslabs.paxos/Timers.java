package dslabs.paxos;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    private final int sequenceNum;
}

@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_MILLIS = 25;
}

@Data
final class HeartbeatCheckTimer implements Timer {
    static final int HEARTBEAT_CHECK_MILLIS = 100;
}