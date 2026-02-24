package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;
import lombok.NonNull;

@Data
public final class AMOCommand implements Command {
    @NonNull private final Command command;
    @NonNull private final Address clientAddress;
    private final int sequenceNum;

    @Override
    public boolean readOnly() {
        return command.readOnly();
    }
}