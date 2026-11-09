package Components.Infra;

import Components.Repository.Store;
import Components.Repository.Value;
import Components.Repository.ValueType;
import Components.Service.CommandHandler;
import Components.Service.RedisCommand;
import Components.Service.RespSerializer;
import Config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Component
public class AofManager {
    private static final Logger log = LoggerFactory.getLogger(AofManager.class);

    private final RedisConfig redisConfig;
    private final RespSerializer respSerializer;
    private final Object writeLock = new Object();
    private volatile boolean isReplaying = false;

    @Autowired
    public AofManager(RedisConfig redisConfig, RespSerializer respSerializer) {
        this.redisConfig = redisConfig;
        this.respSerializer = respSerializer;
    }

    public Path getAofPath() {
        return Path.of(redisConfig.getDir(), redisConfig.getAofFileName());
    }

    public void append(String[] command) {
        if (!redisConfig.isAofEnabled() || isReplaying || command == null || command.length == 0) {
            return;
        }
        RedisCommand redisCommand = RedisCommand.fromString(command[0]);
        // Only append write/mutating commands
        if (!redisCommand.isWriteCommand()) {
            return;
        }

        String resp = respSerializer.respArray(command);
        byte[] bytes = resp.getBytes(StandardCharsets.UTF_8);

        synchronized (writeLock) {
            try {
                Path path = getAofPath();
                if (path.getParent() != null) {
                    Files.createDirectories(path.getParent());
                }
                Files.write(path, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                log.error("Failed to append to AOF file: {}", e.getMessage());
            }
        }
    }

    public int loadAndReplay(CommandHandler commandHandler) {
        if (!redisConfig.isAofEnabled()) {
            return 0;
        }
        Path path = getAofPath();
        if (!Files.exists(path)) {
            log.info("No existing AOF file found at {}", path.toAbsolutePath());
            return 0;
        }

        isReplaying = true;
        int commandCount = 0;
        try {
            byte[] allBytes = Files.readAllBytes(path);
            if (allBytes.length == 0) {
                return 0;
            }
            List<String[]> commands = respSerializer.deseriaize(allBytes);
            for (String[] command : commands) {
                if (command == null || command.length == 0) continue;
                RedisCommand cmd = RedisCommand.fromString(command[0]);
                switch (cmd) {
                    case SET:
                        commandHandler.set(command);
                        commandCount++;
                        break;
                    case INCR:
                        commandHandler.incr(command);
                        commandCount++;
                        break;
                    case RPUSH:
                        commandHandler.rpush(command);
                        commandCount++;
                        break;
                    case LPUSH:
                        commandHandler.lpush(command);
                        commandCount++;
                        break;
                    case LPOP:
                        commandHandler.lpop(command);
                        commandCount++;
                        break;
                    default:
                        break;
                }
            }
            log.info("Successfully recovered {} command(s) from AOF ({})", commandCount, path.toAbsolutePath());
        } catch (IOException e) {
            log.error("Error reading AOF file during replay: {}", e.getMessage());
        } finally {
            isReplaying = false;
        }
        return commandCount;
    }

    public boolean rewrite(Store store) {
        if (!redisConfig.isAofEnabled()) {
            return false;
        }
        Path aofPath = getAofPath();
        Path tempPath = Path.of(redisConfig.getDir(), redisConfig.getAofFileName() + ".tmp");

        synchronized (writeLock) {
            try {
                if (tempPath.getParent() != null) {
                    Files.createDirectories(tempPath.getParent());
                }
                StringBuilder sb = new StringBuilder();
                LocalDateTime now = LocalDateTime.now();

                for (String key : store.getKeys()) {
                    Value val = store.getValue(key);
                    if (val == null) continue;
                    if (val.expiresAt != null && val.expiresAt.isBefore(now)) continue;

                    if (val.type == ValueType.STRING && val.val != null) {
                        if (val.expiresAt != null && !val.expiresAt.equals(LocalDateTime.MAX)) {
                            long remainingMillis = Duration.between(now, val.expiresAt).toMillis();
                            if (remainingMillis > 0) {
                                sb.append(respSerializer.respArray(new String[]{"SET", key, val.val, "PX", String.valueOf(remainingMillis)}));
                            }
                        } else {
                            sb.append(respSerializer.respArray(new String[]{"SET", key, val.val}));
                        }
                    } else if (val.type == ValueType.LIST && val.list != null && !val.list.isEmpty()) {
                        String[] items = val.list.toArray(new String[0]);
                        String[] rpushCmd = new String[2 + items.length];
                        rpushCmd[0] = "RPUSH";
                        rpushCmd[1] = key;
                        System.arraycopy(items, 0, rpushCmd, 2, items.length);
                        sb.append(respSerializer.respArray(rpushCmd));
                    }
                }

                Files.writeString(tempPath, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                Files.move(tempPath, aofPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                log.info("AOF rewrite completed successfully at {}", aofPath.toAbsolutePath());
                return true;
            } catch (IOException e) {
                log.error("Failed to rewrite AOF: {}", e.getMessage());
                return false;
            }
        }
    }
}
