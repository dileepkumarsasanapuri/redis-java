package Components.Service;

import Components.Server.TcpServer;
import Config.AppConfig;
import Config.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest(classes = AppConfig.class)
class CommandHandlerTest {

    @Autowired
    public CommandHandler commandHandler;

    @BeforeAll
    public static void setup(@Autowired ApplicationContext context) throws InterruptedException {
        RedisConfig redisConfig=context.getBean(RedisConfig.class);
        redisConfig.setRole("master");
        redisConfig.setPort(6379);
        TcpServer app=context.getBean(TcpServer.class);

        CompletableFuture.runAsync(() -> app.startServer(6379));

        Thread.sleep(1000);
    }

    @Test
    public void testInfo(){
        String res=commandHandler.info(new String[]{"INFO","replication"});
        assertEquals("$11\r\nrole:master\r\n",res);
    }
}