package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
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
        ExecutorService executor= Executors.newFixedThreadPool(10);
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
        while(client.socket.isConnected()){
            byte [] buffer=new byte[client.socket.getReceiveBufferSize()];
            int bytesRead=client.inputStream.read(buffer);
            if(bytesRead==-1){
                log.info("Client disconnected: "+client.id);
                connectionPool.removeClient(client);
                break;
            }
            if(bytesRead > 0){
                byte [] recieved= Arrays.copyOf(buffer,bytesRead);
                List<String [] > commands= respSerializer.deseriaize(recieved);
                for( String [] command:commands){
                    handleCommand(command,client);
                }
            }
        }
        connectionPool.removeClient(client);
        connectionPool.removeSlave(client);
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
            case "exec":
                if(client.commandQueue==null || client.commandQueue.isEmpty()){
                    client.send("*0\r\n");
                    client.endTransaction();
                    return;
                }
                Queue<String []> commands= new LinkedList<>(client.commandQueue);

                BiFunction<String [], Map<String, Value>,String> transactionCacheApplier= commandHandler.getTransactionaCommandCacheApplier();

                store.executeTransaction(client,transactionCacheApplier);

                client.endTransaction();
                while(!commands.isEmpty()){
                    String [] commandToPropagate= commands.poll();
                    String respArr= respSerializer.respArray(commandToPropagate);
                    byte [] bytes=respArr.getBytes();
                    connectionPool.bytesSentToSlaves+=bytes.length;
                    try {
                        CompletableFuture.runAsync(() -> propagateToSlaves(command));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                String response = respSerializer.respArray(client.transactionResponse);
                client.send(response);
                break;
            case "discard":
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
        return (s.equalsIgnoreCase("exec") || s.equalsIgnoreCase("discard"));
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
                for(Slave s:connectionPool.getSlaves()){

                }
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
