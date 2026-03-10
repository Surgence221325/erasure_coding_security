package dslabs.paxos;

import dslabs.framework.Address;
import java.io.Serializable;
import lombok.Data;
import lombok.NonNull;

@Data
public final class Ballot implements Serializable, Comparable<Ballot> {
    @NonNull private final int sequenceNum;
    @NonNull private final Address address;

    @Override
    public int compareTo(Ballot ballot) {
        if (sequenceNum > ballot.sequenceNum)
            return 1;
        if (sequenceNum < ballot.sequenceNum)
            return -1;
        return address.compareTo(ballot.address);
    }

    @Override
    public String toString() {
        return String.format("Ballot(%s, %s)", sequenceNum, address);
    }
}
