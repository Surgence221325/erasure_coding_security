package dslabs.atmostonce;

import dslabs.framework.Result;
import lombok.Data;
import lombok.NonNull;

@Data
public final class AMOResult implements Result {
    @NonNull private final Result result;
    private final int sequenceNum;
}