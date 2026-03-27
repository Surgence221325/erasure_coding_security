package dslabs.kvstore;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;


@ToString
@EqualsAndHashCode
public class KVStore implements Application {

    public interface KVStoreCommand extends Command {
    }

    public interface SingleKeyCommand extends KVStoreCommand {
        String key();
    }

    @Data
    public static final class Get implements SingleKeyCommand {
        @NonNull private final String key;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    @Data
    public static final class Put implements SingleKeyCommand {
        @NonNull private final String key, value;
    }

    @Data
    public static final class Append implements SingleKeyCommand {
        @NonNull private final String key, value;
    }

    public interface KVStoreResult extends Result {
    }

    @Data
    public static final class GetResult implements KVStoreResult {
        @NonNull private final String value;
    }

    @Data
    public static final class KeyNotFound implements KVStoreResult {
    }

    @Data
    public static final class PutOk implements KVStoreResult {
    }

    @Data
    public static final class AppendResult implements KVStoreResult {
        @NonNull private final String value;
    }

    private final Map<String, String> kv = new HashMap<>();

    @Override
    public KVStoreResult execute(Command command) {
        if (command instanceof Get) {
            Get g = (Get) command;
            if (!kv.containsKey(g.key())) {
                return new KeyNotFound();
            }
            return new GetResult(kv.get(g.key()));
        }

        if (command instanceof Put) {
            Put p = (Put) command;
            kv.put(p.key(), p.value());
            return new PutOk();
        }

        if (command instanceof Append) {
            Append a = (Append) command;
            String oldVal = kv.getOrDefault(a.key(), "");
            String newVal = oldVal + a.value();
            kv.put(a.key(), newVal);
            return new AppendResult(newVal);
        }

        throw new IllegalArgumentException();
    }
}