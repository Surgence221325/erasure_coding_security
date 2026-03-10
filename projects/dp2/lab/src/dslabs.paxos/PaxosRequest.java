package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import lombok.Data;
import lombok.NonNull;

@Data
public final class PaxosRequest implements Message {
    @NonNull private final AMOCommand command;
}
