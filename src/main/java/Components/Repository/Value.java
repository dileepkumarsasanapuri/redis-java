package Components.Repository;



import java.time.LocalDateTime;
import java.util.concurrent.LinkedBlockingDeque;


public class Value {
    public ValueType type;
    public LinkedBlockingDeque<String> list;
    public String val;
    public LocalDateTime createdAt;
    public LocalDateTime expiresAt;
    public boolean valueTobeDeleted;
    public Value(String val, LocalDateTime createdAt, LocalDateTime expiresAt) {
        this.val = val;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.valueTobeDeleted=false;
        this.list=null;
        this.type= ValueType.STRING;
    }

    @Override
    public String toString(){
        return val;
    }
}
