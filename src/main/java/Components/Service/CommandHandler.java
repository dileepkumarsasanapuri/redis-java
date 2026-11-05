package Components.Service;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.Store;
import Config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;


@Component
public class CommandHandler {
    private static final Logger log =
            LoggerFactory.getLogger(CommandHandler.class);
    @Autowired
    private RespSerializer respSerializer;

    @Autowired
    private Store store;

    @Autowired
    public RedisConfig redisConfig;

    @Autowired
    public ConnectionPool connectionPool;

    public String ping(String []command){
        return "+PONG\r\n";
    }
    public String echo(String []command){
       return respSerializer.serialize(command[1]);
    }
    public String set(String []command){
        String key= null;
        String val= null;
        int ttl=0;
        try {
            key = command[1];
            val = command[2];
            boolean expiryFlag= Arrays.stream(command)
                    .anyMatch(s-> s.equalsIgnoreCase("PX") ||
                                         s.equalsIgnoreCase("EX"));
            if(expiryFlag){
                ttl = Integer.parseInt(command[3].equalsIgnoreCase("PX")? command[4]: command[4]+"000");
                return store.set(key,val,ttl);
            }
            else{
                return store.set(key,val);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            return "$-1\r\n";
        }
    }
    public String get(String []command){
        try {
            return store.get(command[1]);
        } catch (Exception e) {
            log.error(e.getMessage());
            return "$-1\r\n";
        }
    }

    public String info(String[] command) {
        int replication = Arrays.stream(command)
                .anyMatch(s -> s.equalsIgnoreCase("replication")) ? 1 : 0;
        if(replication == 1){
            String role= "role:"+ redisConfig.getRole();
            String masterReplID= "master_replid:"+ redisConfig.getMasterReplId();
            String masterReplOffset= "master_repl_offset:"+ redisConfig.getMasterReplOffset();
            String [] info= new String[]{role,masterReplID,masterReplOffset};
            String replicationInfo= String.join("\r\n",info);
            return respSerializer.serialize(replicationInfo);
        }
        return "";
    }

    public String replconf(String[] command, Client client) {
        switch (command[1].toLowerCase()){
            case "listening-port":
                connectionPool.removeClient(client);
                Slave sl=new Slave(client);
                connectionPool.addSlave(sl);
                return "+OK\r\n";
            case "capa":
                Slave slave=null;
                for(Slave s:connectionPool.getSlaves()){
                    if(s.connection.equals(client)){
                        slave=s;
                        break;
                    }
                }
                for(int i=0;i<command.length;i++){
                    if(command[i].equalsIgnoreCase("capa")){
                        slave.capabilities.add(command[i+1]);
                    }
                }
                return "+OK\r\n";

//            case "ack":
//                client.ack=Long.parseLong(command[2]);
//                break;
        }
        return "+OK\r\n";
    }

    public String psync(String[] command, Client client) {
        if(command[1].equalsIgnoreCase("?") && command[2].equalsIgnoreCase("-1")){
            String res= "+FULLRESYNC "+redisConfig.getMasterReplId()+" "+redisConfig.getMasterReplOffset()+"\r\n";
            return res;
        }
        else{
            return "+CONTINUE\r\n";
        }
    }
}
