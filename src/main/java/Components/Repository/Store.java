package Components.Repository;

import Components.Service.RespSerializer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


@Component
public class Store {
    public ConcurrentHashMap<String , Value> map;
    private static final Logger log =
            LoggerFactory.getLogger(Store.class);
    @Autowired
    private RespSerializer respSerializer;

    public Store() {
        this.map = new ConcurrentHashMap<>();
    }

    public Set<String> getKeys() {
        return map.keySet();
    }
    public String set(String key,String val){
        LocalDateTime createdAt=LocalDateTime.now();
        LocalDateTime expiresAt=LocalDateTime.MAX;
        Value value=new Value(val,createdAt,expiresAt);
        try {
            map.put(key, value);
            return "+OK\r\n";
        } catch (Exception e) {
            log.error(e.getMessage());
            return "$-1\r\n";
        }
    }

    public String set(String key,String val, int ttl){
        LocalDateTime createdAt=LocalDateTime.now();
        LocalDateTime expiresAt=createdAt.plus(Duration.ofMillis(ttl));
        Value value=new Value(val,createdAt,expiresAt);
        try {
            map.put(key, value);
            return "+OK\r\n";
        } catch (Exception e) {
            log.error(e.getMessage());
            return "$-1\r\n";
        }
    }

    public String get(String key){
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
        }
    }

    public Value getValue(String key) {
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
        }
    }
}
