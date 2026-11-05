package Components.Server;

import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Infra.Client;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Component
public class TcpServer {

    @Autowired
    private RespSerializer respSerializer;
    @Autowired
    private CommandHandler commandHandler;
    private static final Logger log =
            LoggerFactory.getLogger(TcpServer.class);
    public void startServer(int port){

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        ExecutorService executor= Executors.newFixedThreadPool(10);
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
        while(client.socket.isConnected()){
            byte [] buffer=new byte[client.socket.getReceiveBufferSize()];
            int bytesRead=client.inputStream.read(buffer);
            if(bytesRead > 0){
                List<String [] > commands= respSerializer.deseriaize(buffer);
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
                res=commandHandler.set(command);
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
