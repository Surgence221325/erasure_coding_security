package dslabs.atmostonce;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    // Per-client latest executed request/result
    private final Map<dslabs.framework.Address, Integer> lastSeq = new HashMap<>();
    private final Map<dslabs.framework.Address, AMOResult> lastResult = new HashMap<>();

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        if (alreadyExecuted(amoCommand)) {
            return lastResult.get(amoCommand.clientAddress());
        }

        Result res = application.execute(amoCommand.command());
        AMOResult amoRes = new AMOResult(res, amoCommand.sequenceNum());

        lastSeq.put(amoCommand.clientAddress(), amoCommand.sequenceNum());
        lastResult.put(amoCommand.clientAddress(), amoRes);

        return amoRes;
    }

    public Result executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }

        if (command instanceof AMOCommand) {
            return execute(command);
        }

        return application.execute(command);
    }

    public boolean alreadyExecuted(AMOCommand amoCommand) {
        Integer seen = lastSeq.get(amoCommand.clientAddress());
        return seen != null && amoCommand.sequenceNum() <= seen;
    }
}