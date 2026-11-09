package Components.Service;

import Components.Infra.Client;
import Components.Infra.ClientChannelPool;
import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.OptimisticLockException;
import Components.Repository.Store;
import Components.Repository.Value;
import Config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;


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

    @Autowired
    public ClientChannelPool clientChannelPool;

    public CommandHandler() {}

    public CommandHandler(RespSerializer respSerializer, Store store, RedisConfig redisConfig, ConnectionPool connectionPool, ClientChannelPool clientChannelPool) {
        this.respSerializer = respSerializer;
        this.store = store;
        this.redisConfig = redisConfig;
        this.connectionPool = connectionPool;
        this.clientChannelPool = clientChannelPool;
    }

    public void setRespSerializer(RespSerializer respSerializer) {
        this.respSerializer = respSerializer;
    }

    public void setStore(Store store) {
        this.store = store;
    }

    public void setRedisConfig(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    public String ping(String []command){
        return "+PONG\r\n";
    }
    public String echo(String []command){
       return respSerializer.serializeBulkString(command[1]);
    }
    public String set(String []command){
        String key= null;
        String val= null;
        int ttl=0;
        try {
            key = command[1];
            val = command[2];
            if(this.store.isWatchedKey(key)){
                this.store.addToFailTransactionFor(key);
            }
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
            return respSerializer.serializeBulkString(replicationInfo);
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
                return "";

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

            connectionPool.setSlavesThatAreUp(connectionPool.getSlavesThatAreUp() + 1);
            return new ResponseDto(res,concatenate(header,rdbFile));
        }
        else{
            return new ResponseDto("+CONTINUE\r\n",null);
        }
    }

    public String wait(String[] command, Instant now) {
        String [] getAcArr=new String[]{"REPLCONF","GETACK","*"};
        String getAck=respSerializer.respArray(getAcArr);
        byte []getAckBytes=getAck.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int bufferSize= getAckBytes.length;

        int required=Integer.parseInt(command[1]);
        long time = Long.parseLong(command[2]);

        for (Slave sl:connectionPool.getSlaves()){
            CompletableFuture.runAsync(()->{
                try {
                    sl.connection.send(getAckBytes);
                } catch (IOException e) {
                    log.error("Failed to send GETACK to slave: {}", e.getMessage());
                }
            });
        }
        int res=0;
        long deadline = System.currentTimeMillis() + time;
        while(true){
            res = connectionPool.getSlavesThatAreUp();
            if(res >= required){
                break;
            }
            if(System.currentTimeMillis() >= deadline){
                break;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        connectionPool.addBytesSentToSlaves(bufferSize);
        return respSerializer.respInteger(Math.min(res, required));
    }

    public String incr(String[] command) {
        String key=command[1];
        String res="";
        try{
            if(this.store.isWatchedKey(key)){
                this.store.addToFailTransactionFor(key);
            }
            Value value=store.getValue(key);
            if(value==null){
                store.set(key,"0");
                value = store.getValue(key);
            }

            int val= Integer.parseInt(value.val);
            val++;
            value.val=val+"";

            res=respSerializer.respInteger(val);
        }catch (Exception e){
            res= "-ERR value is not an integer or out of range\r\n";
        }
        return res;
    }

    public BiFunction<String [], Map<String,Value>,String> getTransactionaCommandCacheApplier(Client client){
        final RespSerializer localSerializer = this.respSerializer;
        final Store localStore= this.store;
        return (String [] command, Map<String, Value> map) -> {
            isOptimisticLockViolated(client);
            String res="";
            String key=command[1];
            RedisCommand cmd = RedisCommand.fromString(command[0]);
            switch (cmd){
                case SET:
                    if(this.store.isWatchedKey(key)){
                        this.store.addToFailTransactionForExcept(key,client);
                    }
                    res = handleSetCommandTransactional(command,map,localSerializer,localStore);
                    break;
                case GET:
                    res = handleGetCommandTransactional(command,map,localSerializer,localStore);
                    break;
                case INCR:
                    if(this.store.isWatchedKey(key)){
                        this.store.addToFailTransactionForExcept(key,client);
                    }
                    res = handleIncrCommandTransactional(command,map,localSerializer,localStore);
                    break;
                case DEL:
                    if(this.store.isWatchedKey(key)){
                        this.store.addToFailTransactionForExcept(key,client);
                    }
                    res = handleDelCommandTransactional(command,map,localSerializer,localStore);
                    break;
                default:
                    res= "-ERR unknown command "+command[0]+"\r\n";
                    break;
            }
            return res;
        };
    }

    private void isOptimisticLockViolated(Client client){
        if(this.store.failTransactionFor.contains(client.id)){
            this.store.failTransactionFor.remove(client.id);
            client.endTransaction();
            throw new OptimisticLockException();
        }
    }
    private String handleDelCommandTransactional(String[] command, Map<String, Value> map, RespSerializer localSerializer, Store localStore) {
        String key=command[1];
        Value value;
        Value cachedValue= map.getOrDefault(key,null);
        if(cachedValue==null){
            Value storeValue= localStore.getValue(key);
            if(storeValue==null){
               return "-ERR deleting an Invalid key\r\n";
            }else{
                value= new Value(storeValue.val,storeValue.createdAt,storeValue.expiresAt);
            }
        }else{
            value=cachedValue;
        }
        value.valueTobeDeleted=true;
        map.put(key,value);
        return "+OK\r\n";
    }

    private String handleSetCommandTransactional(
            String[] command,
            Map<String, Value> map,
            RespSerializer localSerializer,
            Store localStore) {
        String key=command[1];
        Value newValue = new Value(command[2], LocalDateTime.now(),LocalDateTime.MAX);
        map.put(key,newValue);
        return "+OK\r\n";
    }

    private String handleIncrCommandTransactional(
            String[] command,
            Map<String, Value> map,
            RespSerializer localSerializer,
            Store localStore) {
        try {
            String key=command[1];
            Value value;
            Value cachedValue= map.getOrDefault(key,null);
            if(cachedValue==null){
                Value storeValue= localStore.getValue(key);
                if(storeValue==null){
                    value= new Value("0",LocalDateTime.now(),LocalDateTime.MAX);
                }else{
                    value= new Value(storeValue.val,storeValue.createdAt,storeValue.expiresAt);
                }
            }else{
                value=cachedValue;
            }
            int valInt= Integer.parseInt(value.val);
            valInt++;
            value.val=String.valueOf(valInt);
            map.put(key,value);
            return respSerializer.respInteger(valInt);
        } catch (Exception e) {
            return "-ERR value is not an integer or out of range\r\n";
        }


    }

    private String handleGetCommandTransactional(
            String[] command,
            Map<String, Value> map,
            RespSerializer localSerializer,
            Store localStore) {
        String key=command[1];
        Value value;
        Value cachedValue= map.getOrDefault(key,null);
        if(cachedValue==null){
            Value storeValue= localStore.getValue(key);
            if(storeValue==null){
                return localStore.get(key);
            }else {
                value = new Value(storeValue.val,storeValue.createdAt,storeValue.expiresAt);
                map.put(key,value);
            }
        }else{
            value=cachedValue;
        }
        return respSerializer.serializeBulkString(value.val);
    }

    public String watch(String[] command, Client client) {
        for(int i=1;i<command.length;i++){
            client.addToWatchSet(command[i]);
            store.addWatcherForKey(command[i],client);
        }
        return "+OK\r\n";
    }

    public String subscribe(String []command,Client client){
        String channelId= command[1];
        int numberOfChannels= clientChannelPool.subscribe(client,channelId);
        String numberOfChannelsResp=respSerializer.respInteger(numberOfChannels);
        String response = "*3\r\n$9\r\nsubscribe\r\n"
                + respSerializer.serializeBulkString(channelId)
                + numberOfChannelsResp;

        client.isSubscribed=true;
        return response;
    }

    public String unsubscribe(String []command,Client client){
        String channelId= command[1];
        int numberOfChannels= clientChannelPool.unsubscribe(client,channelId);
        String numberOfChannelsResp=respSerializer.respInteger(numberOfChannels);
        String response = "*3\r\n$11\r\nunsubscribe\r\n"
                + respSerializer.serializeBulkString(channelId)
                + numberOfChannelsResp;

        client.isSubscribed=false;
        return response;
    }

    public String publish(String [] command){
        String channelId=command[1];
        String msg=command[2];
        int res= clientChannelPool.publish(channelId,msg);
        return respSerializer.respInteger(res);
    }

    public String rpush(String[] command) {
        String key= command[1];
        int len=0;
        for(int i=2;i<command.length;i++){
            String val=command[i];
            len=store.rpush(key,val);
        }
        return respSerializer.respInteger(len);
    }

    public String lpush(String[] command) {
        String key= command[1];
        int len=0;
        for(int i=2;i<command.length;i++){
            String val=command[i];
            len=store.lpush(key,val);
        }
        return respSerializer.respInteger(len);
    }

    public String lrange(String[] command) {
        int st= Integer.parseInt(command[2]);
        int stop= Integer.parseInt(command[3]);
        String key=command[1];
        String []list=store.lrange(key, st,stop);
        if(list.length==0){
            return "*0\r\n";
        }
        return respSerializer.respArray(list);
    }

    public String llen(String[] command) {
        String key=command[1];
        return respSerializer.respInteger(store.llen(key));
    }

    public String lpop(String[] command) {
        String key=command[1];
        if(command.length==2){
            return respSerializer.serializeBulkString(store.lpop(key));
        }
        int popTimes= Integer.parseInt(command[2]);
        String []pops= new String[popTimes];
        for(int i=0;i<popTimes;i++){
            pops[i]=store.lpop(key);
        }
        return respSerializer.respArray(pops);
    }

    public String blpop(String[] command) {
        String key=command[1];
        double timeOut= Double.parseDouble(command[2]);
        long timeOutMs=(long)(timeOut*1000);
        try{
            if(command.length==2 || timeOut==0){
                System.out.println("--- indefinite block -----");
                String e= store.blpop(key);
                if(e==null){
                    System.out.println("---- retunring null array---");
                    return "*-1\r\n"; // timeout expired, nothing popped;
                }
                String res[]= new String[2];
                res[0]=key;
                res[1]=e;
                return respSerializer.respArray(res);
            }
            System.out.println("--- blocking for "+timeOutMs+"----");
            String e=store.blpop_timeout(key,timeOutMs);
            if(e==null){
                System.out.println("---- retunring null array---");
                return "*-1\r\n"; // timeout expired, nothing popped;
            }
            String res[]= new String[2];
            res[0]=key;
            res[1]=e;
            return respSerializer.respArray(res);
        } catch (Exception e) {
            return "*-1\r\n";
        }
    }
}
