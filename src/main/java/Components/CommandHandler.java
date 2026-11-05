package Components;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CommandHandler {

    @Autowired
    private RespSerializer respSerializer;

    @Autowired
    private Store store;

    public String ping(String []command){
        return "+PONG\r\n";
    }
    public String echo(String []command){
       return respSerializer.serialize(command[1]);
    }
    public String set(String []command){
        String key= null;
        String val= null;
        try {
            key = command[1];
            val = command[2];
        } catch (Exception e) {
            return "$-1\r\n";
        }
        return store.set(key,val,50000);
    }
    public String get(String []command){
        try {
            return store.get(command[1]);
        } catch (Exception e) {
            return "$-1\r\n";
        }
    }
}
