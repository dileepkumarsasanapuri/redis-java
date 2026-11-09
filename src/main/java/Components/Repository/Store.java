package Components.Repository;

import Components.Infra.Client;
import Components.Service.RespSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;


@Component
public class Store {
    private final ReentrantReadWriteLock rwLock=new ReentrantReadWriteLock(true);
    public ConcurrentHashMap<String , Value> map;
    public ConcurrentHashMap<String, Set<Integer>>  watchingClientsListForKeys;
    public Set<Integer> failTransactionFor;

    private static final Logger log =  LoggerFactory.getLogger(Store.class);
    @Autowired
    private RespSerializer respSerializer;

    public Store() {
        this(new RespSerializer());
    }

    public Store(RespSerializer respSerializer) {
        this.map = new ConcurrentHashMap<>();
        this.watchingClientsListForKeys = new ConcurrentHashMap<>();
        this.failTransactionFor = ConcurrentHashMap.newKeySet();
        this.respSerializer = respSerializer;
    }

    public void purgeExpiredKeys() {
        LocalDateTime now = LocalDateTime.now();
        for (Map.Entry<String, Value> entry : map.entrySet()) {
            Value val = entry.getValue();
            if (val != null && val.expiresAt != null && val.expiresAt.isBefore(now)) {
                rwLock.writeLock().lock();
                try {
                    Value current = map.get(entry.getKey());
                    if (current != null && current.expiresAt != null && current.expiresAt.isBefore(now)) {
                        map.remove(entry.getKey());
                    }
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
        }
    }

    public boolean isWatchedKey(String key){
        return watchingClientsListForKeys.containsKey(key);
    }

    public void addToFailTransactionForExcept(String key,Client curClient){
        Set<Integer> watchers= watchingClientsListForKeys.getOrDefault(key,new HashSet<>());
        for(int id:watchers){
            if(id!=curClient.id){
                this.failTransactionFor.add(id);
            }
        }
    }

    public void addToFailTransactionFor(String key){
        Set<Integer> watchers= watchingClientsListForKeys.getOrDefault(key,new HashSet<>());
        for(int id:watchers){
            this.failTransactionFor.add(id);
        }
    }

    public Set<String> getKeys() {
        rwLock.readLock().lock();
        try {
            return map.keySet();
        } finally {
           rwLock.readLock().unlock();
        }
    }
    public String set(String key,String val){
        rwLock.writeLock().lock();
        LocalDateTime createdAt=LocalDateTime.now();
        LocalDateTime expiresAt=LocalDateTime.MAX;
        Value value=new Value(val,createdAt,expiresAt);
        try {
            map.put(key, value);
            return "+OK\r\n";
        } catch (Exception e) {
            log.error(e.getMessage());
            return "$-1\r\n";
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public String set(String key,String val, int ttl){
        rwLock.writeLock().lock();
        LocalDateTime createdAt=LocalDateTime.now();
        LocalDateTime expiresAt=createdAt.plus(Duration.ofMillis(ttl));
        Value value=new Value(val,createdAt,expiresAt);
        try {
            map.put(key, value);
            return "+OK\r\n";
        } catch (Exception e) {
            log.error(e.getMessage());
            return "$-1\r\n";
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public String get(String key){
        rwLock.readLock().lock();
        try{
            Value value = map.get(key);
            LocalDateTime now=LocalDateTime.now();
            if(value !=null && value.expiresAt.isBefore(now)){
                map.remove(key);
                return "$-1\r\n";
            }
            return respSerializer.serializeBulkString(value.val);
        } catch (Exception e) {
            log.error(e.getMessage());
            return "$-1\r\n";
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Value getValue(String key) {
        rwLock.readLock().lock();
        try{
            Value value = map.get(key);
            LocalDateTime now=LocalDateTime.now();
            if(value !=null && value.expiresAt.isBefore(now)){
                map.remove(key);
                return null;
            }
            return value;
        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void executeTransaction(
            Client client,
            BiFunction<String [], Map<String, Value>,String> transactionCacheApplier
    ){
        rwLock.writeLock().lock();
        Map<String, Value> localCache= new HashMap<>();
        List<String> localResponses = new ArrayList<>();
        try {
            while (!client.commandQueue.isEmpty()){
                String [] command= client.commandQueue.poll();
                String res = transactionCacheApplier.apply(command, localCache);
               localResponses.add(res);
            }
            for(Map.Entry<String,Value> entry:localCache.entrySet()){
                String key=entry.getKey();
                Value val=entry.getValue();
                if(val.valueTobeDeleted){
                    this.map.remove(key);
                }else{
                    this.map.put(key,val);
                }
            }
            if(client.watchSet!=null){
                for(String key:client.watchSet){
                    removeWatcherForKey(key,client);
                }
            }
            client.transactionResponse.addAll(localResponses);
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public boolean addWatcherForKey(String key,Client watcher){
        rwLock.writeLock().lock();
        try{
            Set<Integer> watchers= watchingClientsListForKeys.getOrDefault(key,null);
            if(watchers==null){
                watchers=new HashSet<>();
            }
            watchers.add(watcher.id);
            watchingClientsListForKeys.put(key,watchers);
            return true;
        } catch (Exception e) {
            return false;
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public boolean removeWatcherForKey(String key,Client client){
        rwLock.writeLock().lock();
        try{
            Set<Integer> watchers = watchingClientsListForKeys.get(key);
            if(watchers==null){
               return true;
            }
            watchers.remove(client.id);
            if(watchers.isEmpty()) {
                watchingClientsListForKeys.put(key, watchers);
            }else{
                watchingClientsListForKeys.put(key,watchers);
            }
            return true;
        } catch (Exception e) {
            return false;
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public int rpush(String key, String val) {
        rwLock.writeLock().lock();
        try{
            Value value=map.get(key);
            if(value==null){
                value= new Value(ValueType.LIST,LocalDateTime.now(),LocalDateTime.MAX);
                map.put(key,value);
            } else if (value.type!=ValueType.LIST) {
                return -1;
            }
            value.list.add(val);
            return value.list.size();
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public int lpush(String key, String val) {
        rwLock.writeLock().lock();
        try{
            Value value=map.get(key);
            if(value==null){
                value= new Value(ValueType.LIST,LocalDateTime.now(),LocalDateTime.MAX);
                map.put(key,value);
            } else if (value.type!=ValueType.LIST) {
                return -1;
            }
            value.list.addFirst(val);
            return value.list.size();
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public String[] lrange(String key, int st, int stop) {
        rwLock.readLock().lock();
        try {
            Value value= map.get(key);
            if(value==null){
                return new String[0];
            }else if(value.type!=ValueType.LIST){
                throw new RuntimeException("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
            }
            int len=value.list.size();

            int stPositive = st<0 ? st+len : st;
            int stopPositive = stop<0 ? stop+len:stop;
            if(stPositive > stopPositive ){
                return new String[0];
            }
            int stopExclusive = stopPositive+1;
            if(stPositive >= len) {
                return new String[0];
            }
            stopExclusive = Math.min(stopExclusive,len);
            stPositive = Math.max(stPositive,0);
            List<String> snapshot = new ArrayList<>(value.list);
            return snapshot.subList(stPositive,stopExclusive).toArray(new String[0]);
        }finally {
            rwLock.readLock().unlock();
        }
    }

    public int llen(String key) {
        rwLock.readLock().lock();
        try{
            Value value=map.get(key);
            if(value==null){
                return 0;
            }
            return value.list.size();
        }finally {
            rwLock.readLock().unlock();
        }
    }

    public String lpop(String key) {
        rwLock.writeLock().lock();
        try{
            Value value=map.get(key);
            if(value==null){
                return "";
            }
            return value.list.removeFirst();
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public String blpop(String key) throws InterruptedException {
        rwLock.writeLock().lock();
        LinkedBlockingDeque<String> deque;
        try{
            Value value=map.get(key);
            if(value==null){
                value=new Value(ValueType.LIST,LocalDateTime.now(),LocalDateTime.MAX);
                map.put(key,value);
            }
            deque=value.list;
        }finally {
            rwLock.writeLock().unlock();
        }
        return deque.takeFirst();
    }

    public String blpop_timeout(String key, long timeOutMs) throws InterruptedException {
        rwLock.writeLock().lock();
        LinkedBlockingDeque<String> deque;
        try{
            Value value=map.get(key);
            if(value==null){
                value=new Value(ValueType.LIST,LocalDateTime.now(),LocalDateTime.MAX);
                map.put(key,value);
            }
            deque=value.list;
        }finally {
            rwLock.writeLock().unlock();
        }
        String popped = deque.pollFirst(timeOutMs, TimeUnit.MILLISECONDS);
        if (popped == null) {
            rwLock.writeLock().lock();
            try {
                Value val = map.get(key);
                if (val != null && val.list != null && val.list.isEmpty()) {
                    map.remove(key);
                }
            } finally {
                rwLock.writeLock().unlock();
            }
        }
        return popped;
    }
}
