package dslabs.capstone;

import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.framework.testing.Workload;
import java.nio.charset.StandardCharsets;

public final class CapstoneWorkload {
    private CapstoneWorkload() {}

    public static Command write(String key, String value) {
        return new CapstoneWrite(key, value.getBytes(StandardCharsets.UTF_8));
    }

    public static Command read(String key) {
        return new CapstoneRead(key);
    }

    public static Result writeOk() {
        return new CapstoneWriteResult(true, null);
    }

    public static Result readResult(String value) {
        return new CapstoneReadResult(
                value.getBytes(StandardCharsets.UTF_8), null);
    }

    public static Result readNotFound() {
        return new CapstoneReadResult(null, "NOT_FOUND");
    }

    public static Workload emptyWorkload() {
        return Workload.builder().commands().build();
    }

    public static Workload putGetWorkload(String key, String value) {
        return Workload.builder()
                .commands(write(key, value), read(key))
                .results(writeOk(), readResult(value))
                .build();
    }
}
