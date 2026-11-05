package Config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;



@Component
public class RedisConfig {
    public String role;
    public int port;

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
