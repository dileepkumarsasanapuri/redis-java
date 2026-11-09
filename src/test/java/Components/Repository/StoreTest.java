package Components.Repository;

import Components.Service.RespSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class StoreTest {

    private Store store;
    private RespSerializer respSerializer;

    @BeforeEach
    public void setup(){
        respSerializer = new RespSerializer();
        store = new Store(respSerializer);
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
        int expiry=100; // 100ms
        String setResponse=store.set(key,val,expiry);
        assertEquals("+OK\r\n",setResponse);
        String getResponse=store.get(key);
        assertEquals(respSerializer.serializeBulkString(val),getResponse);

        Thread.sleep(150);
        String getResponseAfterExpiry=store.get(key);
        assertEquals("$-1\r\n",getResponseAfterExpiry);
    }

    @Test
    public void testSetAndGetExpiryReset() throws InterruptedException {
        String key="testKey";
        String val="testValue";
        String newVal="newValue";
        int expiry=100; // 100ms

        String setResponse=store.set(key,val,expiry);
        assertEquals("+OK\r\n",setResponse);

        String getResponse=store.get(key);
        assertEquals(respSerializer.serializeBulkString(val),getResponse);

        String setResponseNew=store.set(key,newVal,expiry*4);
        assertEquals("+OK\r\n",setResponseNew);

        String getResponseNew=store.get(key);
        assertEquals(respSerializer.serializeBulkString(newVal),getResponseNew);

        Thread.sleep(150);
        String getResponseAfterExpiry=store.get(key);
        assertEquals(respSerializer.serializeBulkString(newVal),getResponseAfterExpiry);
    }

    @Test
    public void testActiveTtlPurge() throws InterruptedException {
        store.set("tempKey", "val", 50);
        assertEquals(1, store.getKeys().size());
        Thread.sleep(80);
        store.purgeExpiredKeys();
        assertEquals(0, store.getKeys().size());
    }

    @Test
    public void testListOperations() {
        assertEquals(1, store.rpush("mylist", "a"));
        assertEquals(2, store.rpush("mylist", "b"));
        assertEquals(3, store.lpush("mylist", "first"));
        assertEquals(3, store.llen("mylist"));

        String[] range = store.lrange("mylist", 0, -1);
        assertArrayEquals(new String[]{"first", "a", "b"}, range);

        assertEquals("first", store.lpop("mylist"));
        assertEquals(2, store.llen("mylist"));
    }

    @Test
    public void testConcurrentSetting() {
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