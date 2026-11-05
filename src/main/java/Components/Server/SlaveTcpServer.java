package Components.Server;

import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Config.RedisConfig;
import Infra.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import static java.rmi.server.LogStream.log;


@Component
public class SlaveTcpServer {

    @Autowired
    private RespSerializer respSerializer;
    @Autowired
    private CommandHandler commandHandler;
    private static final Logger log =LoggerFactory.getLogger(SlaveTcpServer.class);

    @Autowired
    private RedisConfig redisConfig;

    public void startServer(){

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        ExecutorService executor= Executors.newFixedThreadPool(10);
        int port= redisConfig.getPort();
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            CompletableFuture<Void> slaveConnectionFuture = CompletableFuture.runAsync(this::initiateSlaveConnection);
            slaveConnectionFuture.thenRun(()->{
                System.out.println("Slave connection established successfully.");
            });

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

    private void initiateSlaveConnection() {
        try(Socket master= new Socket(redisConfig.getMasterHost(), redisConfig.getMasterPort())) {
            InputStream inputStream = master.getInputStream();
            OutputStream outputStream = master.getOutputStream();
            byte[] inputBuffer = new byte[1024];
            byte [] data="*1\r\n$4\r\nPING\r\n".getBytes();
            outputStream.write(data);

            int bytesRead = inputStream.read(inputBuffer,0,inputBuffer.length);
            String  resp=new String(inputBuffer,0,bytesRead, StandardCharsets.UTF_8);
            log("Received response from master: " + resp);
        } catch (IOException e) {
            log.error("Error establishing connection to master: " + e.getMessage());
        }
    }


    public void handleClients(Client client) throws IOException {
        while(client.socket.isConnected()){
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
                    handleCommand(command,client);
                }
            }

        }
    }

    private void handleCommand(String[] command, Client client) throws IOException {
        String res="";
        switch (command[0].toLowerCase()){
            case "ping":
                res=commandHandler.ping(command);
                break;
            case "echo":
                res=commandHandler.echo(command);
                break;
            case "set":
                res="-READONLY You can't write against a read only slave.\r\n";
                break;
            case "get":
                res=commandHandler.get(command);
                break;
            case "info":
                res=commandHandler.info(command);
                break;
        }
        if(res !=null && !res.equals(""))
            client.outputStream.write(res.getBytes());

    }
}
