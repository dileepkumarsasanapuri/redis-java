package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


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
                   res= respSerializer.respInteger(connectionPool.getSlavesThatareUp());
               }
                Instant now=Instant.now();
                res= commandHandler.wait(command, now);
                connectionPool.slavesThatareUp=0;
                break;
        }

        client.send(res,data);

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
