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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;


@Component
public class CommandHandler {
    private static final Logger log = LoggerFactory.getLogger(CommandHandler.class);
    private static final String emptyRDBFile = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

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
            case "getack":
                String []replconfAck= new String[]{"REPLCONF","ACK",redisConfig.getMasterReplOffset()+""};
                return respSerializer.respArray(replconfAck);
            case "ack":
                int ackResponse=Integer.parseInt(command[2]);
                connectionPool.slaveAck(ackResponse);
                break;

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

    public byte[] concatenate(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
    public ResponseDto psync(String[] command) {
        if(command[1].equals("?") && command[2].equals("-1")){
            String res= "+FULLRESYNC "+redisConfig.getMasterReplId()+" "+redisConfig.getMasterReplOffset()+"\r\n";

            byte [] rdbFile= Base64.getDecoder().decode(emptyRDBFile);
            String  len=rdbFile.length+"";
            String  fullResHeader= "$"+len+"\r\n";
            byte [] header= fullResHeader.getBytes();

            connectionPool.slavesThatareUp++;
            return new ResponseDto(res,concatenate(header,rdbFile));
        }
        else{
            return new ResponseDto("+CONTINUE\r\n",null);
        }
    }

    public String wait(String[] command, Instant now) {
        String [] getAcArr=new String[]{"REPLCONF","GETACK","*"};
        String getAck=respSerializer.respArray(getAcArr);
        byte []getAckBytes=getAck.getBytes();
        int bufferSize= getAckBytes.length;

        int required=Integer.parseInt(command[1]);
        int time = Integer.parseInt(command[2]);

        for (Slave sl:connectionPool.getSlaves()){
            CompletableFuture.runAsync(()->{
                try {
                    sl.connection.send(getAckBytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        int res=0;
        while(true){
            if(Duration.between(now,Instant.now()).toMillis()>=time){
                break;
            }
            if(res>=required){
                break;
            }
            res=connectionPool.slavesThatareUp;
        }
        connectionPool.bytesSentToSlaves+=bufferSize;
        if(res> required){
            return respSerializer.respInteger(required);
        }
        return respSerializer.respInteger(res);
    }
}
