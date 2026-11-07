package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.OptimisticLockException;
import Components.Repository.Store;
import Components.Repository.Value;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Components.Service.ResponseDto;
import Config.RedisConfig;
import Components.Infra.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;


@Component
public class MasterTcpServer {

    @Autowired
    private RespSerializer respSerializer;
    @Autowired
    private CommandHandler commandHandler;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private Store store;

    private static final Logger log = LoggerFactory.getLogger(MasterTcpServer.class);


    public void startServer(){

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        ExecutorService executor= Executors.newCachedThreadPool();
        int port = redisConfig.getPort();
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            int id=0;
            while(true) {
                clientSocket = serverSocket.accept();
                id++;
                Socket finalClientSocket = clientSocket;
                InputStream inputStream= finalClientSocket.getInputStream();
                OutputStream outputStream=finalClientSocket.getOutputStream();
                Client client=new Client(finalClientSocket,inputStream,outputStream,id);
                executor.submit(() -> {
                    try {
                        handleClients(client);
                    } catch (IOException e) {
                        log.error(e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            try {
                executor.shutdown();
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
    }

    public void handleClients(Client client) throws IOException {
        connectionPool.addClient(client);
        while(true){
            byte [] buffer=new byte[client.socket.getReceiveBufferSize()];
            int bytesRead=client.inputStream.read(buffer);
            if(bytesRead==-1){
                log.info("Client disconnected: "+client.id);
                break;
            }
            if(bytesRead > 0){
                byte [] recieved= Arrays.copyOf(buffer,bytesRead);
                List<String [] > commands= respSerializer.deseriaize(recieved);
                for( String [] command:commands){
                   if(client.isSubscribed){
                       handleCommandSubscribed(command,client);
                   }else {
                       handleCommand(command, client);
                   }
                }
            }
        }
        connectionPool.removeClient(client);
        connectionPool.removeSlave(client);
    }

    private void handleCommandSubscribed(String[] command, Client client) throws IOException {
        if(!isCommandSubscribeModelEligible(command[0])){
            String errMsg="-ERR Can't execute '"+command[0].toLowerCase()+"':only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n";
            client.send(errMsg);
            return;
        }
        String res="";
        switch (command[0].toLowerCase()){
            case "subscribe":
                res=commandHandler.subscribe(command,client);
                break;
            case "unsubscribe":
                res=commandHandler.unsubscribe(command,client);
                break;
            case "ping":
                res="*2\r\n$4\r\npong\r\n$0\r\n\r\n";
                break;
            default:
                res=command[0];
                break;
        }
        client.send(res);
    }

    private boolean isCommandSubscribeModelEligible(String command){
        return switch (command.toLowerCase()){
            case "subscribe","unsubscribe","psubscribe","punsubscribe","ping","quit" -> true;
            default -> false;
        };
    }
    private void handleCommand(String[] command, Client client) throws IOException {
        if(!client.getTransactionalContext()){
            ResponseDto responseDto= caseHandler(command,client);
            client.send(responseDto);
        }
        else if(!isTransactionalControlCommand(command[0])){ // not exec or not discard
            addCommandToTransaction(command,client);
        }else{
            transactionalController(command,client);
        }
    }

    private void transactionalController(String[] command, Client client) throws IOException {
        switch (command[0].toLowerCase()){
            case "watch":
                String res="-ERR WATCH inside MULTI is not allowed\r\n";
                client.send(res);
                break;
            case "exec":
                if(client.commandQueue==null || client.commandQueue.isEmpty()){
                    client.send("*0\r\n");
                    client.endTransaction();
                    return;
                }
                Queue<String []> commands= new LinkedList<>(client.commandQueue);

                BiFunction<String [], Map<String, Value>,String> transactionCacheApplier= commandHandler.getTransactionaCommandCacheApplier(client);
                try{
                    store.executeTransaction(client,transactionCacheApplier);
                    while(!commands.isEmpty()) {
                        String[] commandToPropagate = commands.poll();
                        String respArr = respSerializer.respArray(commandToPropagate);
                        byte[] bytes = respArr.getBytes();
                        connectionPool.bytesSentToSlaves += bytes.length;
                        CompletableFuture.runAsync(() -> propagateToSlaves(command));
                    }
                    String response= respSerializer.respArray(client.transactionResponse);
                    client.send(response);
                }catch (OptimisticLockException e) {
                    client.send("*-1\r\n");
                }finally {
                    client.endTransaction();
                }
                break;
            case "discard":
                if(client.watchSet!=null){
                    for(String key:client.watchSet){
                        store.removeWatcherForKey(key,client);
                    }
                }
                if(store.failTransactionFor.contains(client.id)){
                    store.failTransactionFor.remove(client.id);
                }
                client.endTransaction();
                client.send("+OK\r\n");
                break;
        }
    }

    private void addCommandToTransaction(String[] command, Client client) throws IOException {
        client.commandQueue.offer(command);
        client.send("+QUEUED\r\n");
    }

    private boolean isTransactionalControlCommand(String s) {
        return (s.equalsIgnoreCase("exec")
                || s.equalsIgnoreCase("discard")
                || s.equalsIgnoreCase("watch"));
    }

    public ResponseDto caseHandler(String[] command,Client client){
        String res="";
        byte [] data=null;
        switch (command[0].toLowerCase()){
            case "ping":
                res=commandHandler.ping(command);
                break;
            case "echo":
                res=commandHandler.echo(command);
                break;
            case "set":
                res=commandHandler.set(command);
                String respArr= respSerializer.respArray(command);
                byte [] bytes=respArr.getBytes();
                connectionPool.bytesSentToSlaves+=bytes.length;
                try {
                    CompletableFuture.runAsync(() -> propagateToSlaves(command));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                break;
            case "get":
                res=commandHandler.get(command);
                break;
            case "info":
                res=commandHandler.info(command);
                break;
            case "replconf":
                res=commandHandler.replconf(command,client);
                break;
            case "psync":
                ResponseDto resDto=commandHandler.psync(command);
                res=resDto.response;
                data= resDto.data;
                break;
            case "wait":
                if(connectionPool.bytesSentToSlaves==0){
                    res= respSerializer.respInteger(connectionPool.slavesThatareUp);
                    break;
                }
                Instant now=Instant.now();
                res= commandHandler.wait(command, now);
                connectionPool.slavesThatareUp=0;
                break;
            case "incr":
                res= commandHandler.incr(command);
                break;
            case "multi":
                client.beginTransaction();
                res="+OK\r\n";
                break;
            case "exec":
                res = "-ERR EXEC without MULTI\r\n";
                break;
            case "discard":
                res="-ERR DISCARD without MULTI\r\n";
                break;
            case "watch":
                res=commandHandler.watch(command,client);
                break;
            case "unwatch":
                if(client.watchSet!=null){
                    for(String key:client.watchSet){
                        store.removeWatcherForKey(key,client);
                    }
                }
                if(store.failTransactionFor.contains(client.id)){
                    store.failTransactionFor.remove(client.id);
                }
                client.watchSet=null;
                res="+OK\r\n";
                break;
            case "subscribe":
                res=commandHandler.subscribe(command,client);
                break;
            case "unsubscribe":
                res=commandHandler.unsubscribe(command,client);
                break;
            case "publish":
                res=commandHandler.publish(command);
                break;
            case "rpush":
                res = commandHandler.rpush(command);
                String commandRespStringRpush=respSerializer.respArray(command);
                byte[] toCountRpush = commandRespStringRpush.getBytes();
                connectionPool.bytesSentToSlaves+=toCountRpush.length;
                CompletableFuture.runAsync(()-> propagateToSlaves(command));
                break;
            case "lpush":
                res = commandHandler.lpush(command);
                String commandRespStringLpush=respSerializer.respArray(command);
                byte[] toCountLpush = commandRespStringLpush.getBytes();
                connectionPool.bytesSentToSlaves+=toCountLpush.length;
                CompletableFuture.runAsync(()-> propagateToSlaves(command));
                break;
            case "lrange":
                res= commandHandler.lrange(command);
                break;
            case "llen":
                res=commandHandler.llen(command);
                break;
            case "lpop":
                res=commandHandler.lpop(command);
                break;
            case "blpop":
                res=commandHandler.blpop(command);
                break;



        }
        return new ResponseDto(res,data);
    }
    private void propagateToSlaves(String[] command) {
        String commandRespStr= respSerializer.respArray(command);
          try{
                for(Slave slave:connectionPool.getSlaves()){
                    slave.send(commandRespStr.getBytes());
                }
        } catch (Exception e) {
              log.error( "Error propagating command to slaves: " + e.getMessage());
          }
    }
}
