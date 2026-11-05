package Components;

import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class Value {
    public String val;
    public LocalDateTime createdAt;
    public LocalDateTime expiresAt;

    public Value(String val, LocalDateTime createdAt, LocalDateTime expiresAt) {
        this.val = val;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
    }
}
