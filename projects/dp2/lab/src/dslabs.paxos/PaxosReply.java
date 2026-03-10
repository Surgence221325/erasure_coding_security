package dslabs.paxos;

import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;
import lombok.NonNull;

@Data
public final class PaxosReply implements Message {
    @NonNull private final AMOResult result;
}