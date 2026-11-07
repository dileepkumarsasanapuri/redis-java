package Components.Repository;

import Components.Service.RespSerializer;
import Config.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest(classes = AppConfig.class)
class StoreTest {

    @Autowired
    private Store store;
    @Autowired
    private RespSerializer respSerializer;

    @BeforeEach
    public void setup(){
        store.map.clear();
    }
    @Test
    public void testSetAndGet(){
        String key="testKey";
        String val="testValue";
        String setResponse=store.set(key,val);
        assertEquals("+OK\r\n",setResponse);
        String getResponse=store.get(key);
        assertEquals(respSerializer.serializeBulkString(val),getResponse);
    }

    @Test
    public void testSetAndGetWithExpiry() throws InterruptedException {
        String key="testKey";
        String val="testValue";
        int expiry=1000; // 1 second
        String setResponse=store.set(key,val,expiry);
        assertEquals("+OK\r\n",setResponse);
        String getResponse=store.get(key);
        assertEquals(respSerializer.serializeBulkString(val),getResponse);

        Thread.sleep(1500);
        String getResponseAfterExpiry=store.get(key);
        assertEquals("$-1\r\n",getResponseAfterExpiry);
    }

    @Test
    public void testSetAndGetExpiryReset() throws InterruptedException {
        String key="testKey";
        String val="testValue";
        String newVal="newValue";
        int expiry=1000; // 1 second

        String setResponse=store.set(key,val,expiry);
        assertEquals("+OK\r\n",setResponse);

        String getResponse=store.get(key);
        assertEquals(respSerializer.serializeBulkString(val),getResponse);

        String setResponseNew=store.set(key,newVal,expiry*2);
        assertEquals("+OK\r\n",setResponseNew);

        String getResponseNew=store.get(key);
        assertEquals(respSerializer.serializeBulkString(newVal),getResponseNew);

        Thread.sleep(1500);
        String getResponseAfterExpiry=store.get(key);
        assertEquals(respSerializer.serializeBulkString(newVal),getResponseAfterExpiry);
    }

    @Test
    public void testConcurrentSetting() throws InterruptedException {
        List<CompletableFuture<Void>> l=new ArrayList<>();
        for(int i=0;i<10;i++){
            int finalI = i;
            CompletableFuture<Void> future=CompletableFuture.runAsync(()->{
               for(int j=0;j<100;j++){
                   String key="key"+finalI+"_"+j;
                   String val="value"+finalI+"_"+j;
                   store.set(key,val);
               }
            });
            l.add(future);
        }
        CompletableFuture.allOf(l.toArray(new CompletableFuture[0])).join();
        assertEquals(1000,store.getKeys().size());
    }
}