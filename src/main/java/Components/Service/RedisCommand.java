package Components.Service;

import java.util.HashMap;
import java.util.Map;

public enum RedisCommand {
    PING(false, false, true),
    ECHO(false, false, false),
    SET(true, false, false),
    GET(false, false, false),
    INCR(true, false, false),
    DEL(true, false, false),
    INFO(false, false, false),
    REPLCONF(false, false, false),
    PSYNC(false, false, false),
    WAIT(false, false, false),
    MULTI(false, false, false),
    EXEC(false, true, false),
    DISCARD(false, true, false),
    WATCH(false, true, false),
    UNWATCH(false, false, false),
    SUBSCRIBE(false, false, true),
    UNSUBSCRIBE(false, false, true),
    PSUBSCRIBE(false, false, true),
    PUNSUBSCRIBE(false, false, true),
    PUBLISH(false, false, false),
    RPUSH(true, false, false),
    LPUSH(true, false, false),
    LRANGE(false, false, false),
    LLEN(false, false, false),
    LPOP(true, false, false),
    BLPOP(true, false, false),
    QUIT(false, false, true),
    UNKNOWN(false, false, false);

    private final boolean writeCommand;
    private final boolean transactionalControl;
    private final boolean subscribeEligible;

    private static final Map<String, RedisCommand> LOOKUP = new HashMap<>();

    static {
        for (RedisCommand cmd : values()) {
            LOOKUP.put(cmd.name().toLowerCase(), cmd);
        }
    }

    RedisCommand(boolean writeCommand, boolean transactionalControl, boolean subscribeEligible) {
        this.writeCommand = writeCommand;
        this.transactionalControl = transactionalControl;
        this.subscribeEligible = subscribeEligible;
    }

    public boolean isWriteCommand() {
        return writeCommand;
    }

    public boolean isTransactionalControl() {
        return transactionalControl;
    }

    public boolean isSubscribeEligible() {
        return subscribeEligible;
    }

    public static RedisCommand fromString(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return UNKNOWN;
        }
        return LOOKUP.getOrDefault(raw.trim().toLowerCase(), UNKNOWN);
    }
}

