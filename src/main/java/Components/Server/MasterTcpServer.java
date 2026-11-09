package Components.Server;

import Components.Infra.AofManager;
import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.OptimisticLockException;
import Components.Repository.Store;
import Components.Repository.Value;
import Components.Service.CommandHandler;
import Components.Service.RedisCommand;
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
    @Autowired
    private AofManager aofManager;

    private static final Logger log = LoggerFactory.getLogger(MasterTcpServer.class);


    public void startServer(){

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        ExecutorService executor= Executors.newCachedThreadPool();
        int port = redisConfig.getPort();
        try {
            aofManager.loadAndReplay(commandHandler);
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
        RedisCommand cmd = RedisCommand.fromString(command[0]);
        if (!cmd.isSubscribeEligible()) {
            String errMsg = "-ERR Can't execute '" + command[0].toLowerCase() + "':only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n";
            client.send(errMsg);
            return;
        }
        String res = "";
        switch (cmd) {
            case SUBSCRIBE:
                res = commandHandler.subscribe(command, client);
                break;
            case UNSUBSCRIBE:
                res = commandHandler.unsubscribe(command, client);
                break;
            case PING:
                res = "*2\r\n$4\r\npong\r\n$0\r\n\r\n";
                break;
            default:
                res = command[0];
                break;
        }
        client.send(res);
    }

    private void handleCommand(String[] command, Client client) throws IOException {
        RedisCommand cmd = RedisCommand.fromString(command[0]);
        if (!client.getTransactionalContext()) {
            ResponseDto responseDto = caseHandler(cmd, command, client);
            client.send(responseDto);
        } else if (!cmd.isTransactionalControl()) {
            addCommandToTransaction(command, client);
        } else {
            transactionalController(cmd, command, client);
        }
    }

    private void transactionalController(RedisCommand cmd, String[] command, Client client) throws IOException {
        switch (cmd) {
            case WATCH:
                client.send("-ERR WATCH inside MULTI is not allowed\r\n");
                break;
            case EXEC:
                if (client.commandQueue == null || client.commandQueue.isEmpty()) {
                    client.send("*0\r\n");
                    client.endTransaction();
                    return;
                }
                Queue<String[]> commands = new LinkedList<>(client.commandQueue);

                BiFunction<String[], Map<String, Value>, String> transactionCacheApplier = commandHandler.getTransactionaCommandCacheApplier(client);
                try {
                    store.executeTransaction(client, transactionCacheApplier);
                    while (!commands.isEmpty()) {
                        String[] commandToPropagate = commands.poll();
                        aofManager.append(commandToPropagate);
                        String respArr = respSerializer.respArray(commandToPropagate);
                        byte[] bytes = respArr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        connectionPool.addBytesSentToSlaves(bytes.length);
                        CompletableFuture.runAsync(() -> propagateToSlaves(commandToPropagate));
                    }
                    String response = respSerializer.respArray(client.transactionResponse);
                    client.send(response);
                } catch (OptimisticLockException e) {
                    client.send("*-1\r\n");
                } finally {
                    client.endTransaction();
                }
                break;
            case DISCARD:
                if (client.watchSet != null) {
                    for (String key : client.watchSet) {
                        store.removeWatcherForKey(key, client);
                    }
                }
                if (store.failTransactionFor.contains(client.id)) {
                    store.failTransactionFor.remove(client.id);
                }
                client.endTransaction();
                client.send("+OK\r\n");
                break;
            default:
                break;
        }
    }

    private void addCommandToTransaction(String[] command, Client client) throws IOException {
        client.commandQueue.offer(command);
        client.send("+QUEUED\r\n");
    }

    public ResponseDto caseHandler(String[] command, Client client) {
        return caseHandler(RedisCommand.fromString(command[0]), command, client);
    }

    public ResponseDto caseHandler(RedisCommand cmd, String[] command, Client client) {
        String res = "";
        byte[] data = null;
        switch (cmd) {
            case PING:
                res = commandHandler.ping(command);
                break;
            case ECHO:
                res = commandHandler.echo(command);
                break;
            case SET:
                res = commandHandler.set(command);
                propagateWriteCommand(command);
                break;
            case GET:
                res = commandHandler.get(command);
                break;
            case INFO:
                res = commandHandler.info(command);
                break;
            case REPLCONF:
                res = commandHandler.replconf(command, client);
                break;
            case PSYNC:
                ResponseDto resDto = commandHandler.psync(command);
                res = resDto.response;
                data = resDto.data;
                break;
            case WAIT:
                if (connectionPool.getBytesSentToSlaves() == 0) {
                    res = respSerializer.respInteger(connectionPool.getSlavesThatAreUp());
                    break;
                }
                Instant now = Instant.now();
                res = commandHandler.wait(command, now);
                connectionPool.setSlavesThatAreUp(0);
                break;
            case INCR:
                res = commandHandler.incr(command);
                if (!res.startsWith("-ERR")) {
                    propagateWriteCommand(command);
                }
                break;
            case MULTI:
                client.beginTransaction();
                res = "+OK\r\n";
                break;
            case EXEC:
                res = "-ERR EXEC without MULTI\r\n";
                break;
            case DISCARD:
                res = "-ERR DISCARD without MULTI\r\n";
                break;
            case WATCH:
                res = commandHandler.watch(command, client);
                break;
            case UNWATCH:
                if (client.watchSet != null) {
                    for (String key : client.watchSet) {
                        store.removeWatcherForKey(key, client);
                    }
                }
                if (store.failTransactionFor.contains(client.id)) {
                    store.failTransactionFor.remove(client.id);
                }
                client.watchSet = null;
                res = "+OK\r\n";
                break;
            case SUBSCRIBE:
                res = commandHandler.subscribe(command, client);
                break;
            case UNSUBSCRIBE:
                res = commandHandler.unsubscribe(command, client);
                break;
            case PUBLISH:
                res = commandHandler.publish(command);
                break;
            case RPUSH:
                res = commandHandler.rpush(command);
                propagateWriteCommand(command);
                break;
            case LPUSH:
                res = commandHandler.lpush(command);
                propagateWriteCommand(command);
                break;
            case LRANGE:
                res = commandHandler.lrange(command);
                break;
            case LLEN:
                res = commandHandler.llen(command);
                break;
            case LPOP:
                res = commandHandler.lpop(command);
                if (!res.equals("$-1\r\n") && !res.equals("")) {
                    propagateWriteCommand(command);
                }
                break;
            case BLPOP:
                res = commandHandler.blpop(command);
                break;
            default:
                res = "-ERR unknown command '" + command[0] + "'\r\n";
                break;
        }
        return new ResponseDto(res, data);
    }

    private void propagateWriteCommand(String[] command) {
        aofManager.append(command);
        String respArr = respSerializer.respArray(command);
        byte[] bytes = respArr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        connectionPool.addBytesSentToSlaves(bytes.length);
        CompletableFuture.runAsync(() -> propagateToSlaves(command));
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
