package Components.Repository;



import java.time.LocalDateTime;


public class Value {
    public String val;
    public LocalDateTime createdAt;
    public LocalDateTime expiresAt;
    public boolean valueTobeDeleted;
    public Value(String val, LocalDateTime createdAt, LocalDateTime expiresAt) {
        this.val = val;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.valueTobeDeleted=false;
    }
}
