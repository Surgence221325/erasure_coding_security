package dslabs.paxos;

import dslabs.framework.Command;
import dslabs.framework.Message;
import lombok.Data;

/**
 * Please see {@link PaxosRequest} for illustration.
 */

@Data
public class PaxosDecision implements Message {
    private final String id;
    private final int sequenceNum;
    private final Command command;
}
