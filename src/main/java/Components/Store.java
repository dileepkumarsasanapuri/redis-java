package Components;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class Store {
    public ConcurrentHashMap<String , Value> map;

    @Autowired
    private RespSerializer respSerializer;

    public Store() {
        this.map = new ConcurrentHashMap<>();
    }

    public Set<String> getKeys() {
        return map.keySet();
    }

    public String set(String key,String val, int ttl){
        LocalDateTime createdAt=LocalDateTime.now();
        LocalDateTime expiresAt=createdAt.plusSeconds(ttl);
        Value value=new Value(val,createdAt,expiresAt);
        try {
            map.put(key, value);
            return "+OK\r\n";
        } catch (Exception e) {
            return "$-1\r\n";
        }
    }

    public String get(String key){
        Value val=map.get(key);
        if(val==null){
            return "$-1\r\n";
        }
        LocalDateTime now=LocalDateTime.now();
        if(val.expiresAt.isAfter(now)){
            return respSerializer.serialize(val.val);
        }
        else{
            map.remove(key);
            return "$-1\r\n";
        }
    }
}
