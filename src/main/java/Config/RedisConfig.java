package Config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.UUID;


@Component
public class RedisConfig {
    private String role;
    private int port;
    private String masterHost;
    private int masterPort;
    private String masterReplId = null;
    private Long masterReplOffset = null;
    private String dir = ".";
    private String dbFileName = "dump.rdb";
    private boolean aofEnabled = true;
    private String aofFileName = "appendonly.aof";

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getDbFileName() {
        return dbFileName;
    }

    public void setDbFileName(String dbFileName) {
        this.dbFileName = dbFileName;
    }

    public boolean isAofEnabled() {
        return aofEnabled;
    }

    public void setAofEnabled(boolean aofEnabled) {
        this.aofEnabled = aofEnabled;
    }

    public String getAofFileName() {
        return aofFileName;
    }

    public void setAofFileName(String aofFileName) {
        this.aofFileName = aofFileName;
    }

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

    public String getMasterHost() {
        return masterHost;
    }

    public void setMasterHost(String masterHost) {
        this.masterHost = masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public String getMasterReplId() {
        if(masterReplId==null){
            masterReplId = UUID.randomUUID().toString().replace("-", "")
                    + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        }
        return masterReplId;
    }

    public void setMasterReplId(String masterReplId) {
        this.masterReplId = masterReplId;
    }

    public Long getMasterReplOffset() {
        if(masterReplOffset==null){
            masterReplOffset = 0L;
        }
        return masterReplOffset;
    }

    public void setMasterReplOffset(Long masterReplOffset) {

        this.masterReplOffset = masterReplOffset;
    }
}
