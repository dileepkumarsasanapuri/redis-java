package Components.Infra;

import Components.Repository.Store;
import Components.Service.CommandHandler;
import Components.Service.RedisCommand;
import Components.Service.RespSerializer;
import Config.RedisConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class AofManagerTest {

    @TempDir
    Path tempDir;

    private RedisConfig redisConfig;
    private RespSerializer respSerializer;
    private Store store;
    private CommandHandler commandHandler;
    private AofManager aofManager;

    @BeforeEach
    void setUp() {
        redisConfig = new RedisConfig();
        redisConfig.setDir(tempDir.toString());
        redisConfig.setAofFileName("test_appendonly.aof");
        redisConfig.setAofEnabled(true);

        respSerializer = new RespSerializer();
        store = new Store(respSerializer);
        commandHandler = new CommandHandler();
        commandHandler.setRespSerializer(respSerializer);
        commandHandler.setStore(store);
        commandHandler.setRedisConfig(redisConfig);

        aofManager = new AofManager(redisConfig, respSerializer);
    }

    @AfterEach
    void tearDown() throws IOException {
        Path aofPath = aofManager.getAofPath();
        Files.deleteIfExists(aofPath);
    }

    @Test
    void testRedisCommandEnumProperties() {
        assertEquals(RedisCommand.SET, RedisCommand.fromString("SET"));
        assertEquals(RedisCommand.SET, RedisCommand.fromString("set"));
        assertEquals(RedisCommand.GET, RedisCommand.fromString("get"));
        assertEquals(RedisCommand.UNKNOWN, RedisCommand.fromString("nonexistent_command_xyz"));

        assertTrue(RedisCommand.SET.isWriteCommand());
        assertTrue(RedisCommand.INCR.isWriteCommand());
        assertFalse(RedisCommand.GET.isWriteCommand());

        assertTrue(RedisCommand.EXEC.isTransactionalControl());
        assertTrue(RedisCommand.DISCARD.isTransactionalControl());
        assertFalse(RedisCommand.SET.isTransactionalControl());

        assertTrue(RedisCommand.SUBSCRIBE.isSubscribeEligible());
        assertTrue(RedisCommand.PING.isSubscribeEligible());
        assertFalse(RedisCommand.GET.isSubscribeEligible());
    }

    @Test
    void testAofAppendAndReplay() {
        // Append a SET command
        String[] setCmd = new String[]{"SET", "site", "Antigravity"};
        aofManager.append(setCmd);
        commandHandler.set(setCmd);

        // Append an RPUSH command
        String[] rpushCmd = new String[]{"RPUSH", "frameworks", "Spring", "Redis"};
        aofManager.append(rpushCmd);
        commandHandler.rpush(rpushCmd);

        assertTrue(Files.exists(aofManager.getAofPath()), "AOF file should exist on disk");

        // Clear in-memory store
        store.map.clear();
        assertNull(store.getValue("site"));
        assertEquals(0, store.llen("frameworks"));

        // Replay AOF
        int replayed = aofManager.loadAndReplay(commandHandler);
        assertEquals(2, replayed, "Should have replayed 2 commands from AOF");

        // Verify recovered state
        assertEquals("Antigravity", store.getValue("site").val);
        assertEquals(2, store.llen("frameworks"));
        assertArrayEquals(new String[]{"Spring", "Redis"}, store.lrange("frameworks", 0, -1));
    }
}
