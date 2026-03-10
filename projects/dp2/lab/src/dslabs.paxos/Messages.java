package dslabs.paxos;

import dslabs.framework.Message;
import java.util.Map;
import lombok.Data;
import lombok.NonNull;

@Data
final class P1A implements Message {
    @NonNull private final Ballot ballot;
}

@Data
final class P1B implements Message {
    @NonNull private final Ballot ballot;
    @NonNull private final Map<Integer, Pvalue> accepted;
}

@Data
final class P2A implements Message {
    @NonNull private final Ballot ballot;
    @NonNull private final int slotNum;
    @NonNull private final PaxosRequest value;
}

@Data
final class P2B implements Message {
    @NonNull private final Ballot ballot;
    @NonNull private final int slotNum;
    private final boolean accepted;
}

@Data
final class Heartbeat implements Message {
    @NonNull private final Ballot ballot;
    private final int firstNonCleared;
    private final int slotOut;
}

@Data
final class HeartbeatReply implements Message {
    @NonNull private final Ballot ballot;
    private final int slotOut;
}

@Data
final class Decision implements Message {
    private final int slotNum;
    @NonNull private final PaxosRequest value;
}