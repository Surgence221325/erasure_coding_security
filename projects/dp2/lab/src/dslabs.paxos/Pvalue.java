package dslabs.paxos;

import java.io.Serializable;
import lombok.Data;

@Data
public final class Pvalue implements Serializable {
    private final int slot_num;
    private final Ballot ballot;
    private final PaxosRequest paxosRequest;
}
