package Components.Repository;

import Components.Infra.Client;
import Components.Service.RespSerializer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;


@Component
public class Store {
    private final ReentrantReadWriteLock rwLock=new ReentrantReadWriteLock();
    public ConcurrentHashMap<String , Value> map;
    public ConcurrentHashMap<String, Set<Integer>>  watchingClientsListForKeys;
    public Set<Integer> failTransactionFor;

    private static final Logger log =  LoggerFactory.getLogger(Store.class);
    @Autowired
    private RespSerializer respSerializer;

    public Store() {
        this.map = new ConcurrentHashMap<>();
        watchingClientsListForKeys= new ConcurrentHashMap<>();
        failTransactionFor = ConcurrentHashMap.newKeySet();
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
            return respSerializer.serialize(value.val);
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
}
