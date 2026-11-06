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
import java.lang.ref.Cleaner;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.rmi.server.LogStream.log;


@Component
public class SlaveTcpServer {

    @Autowired
    private RespSerializer respSerializer;
    @Autowired
    private CommandHandler commandHandler;
    private static final Logger log =LoggerFactory.getLogger(SlaveTcpServer.class);
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private RedisConfig redisConfig;

    public void startServer(){

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        ExecutorService executor= Executors.newCachedThreadPool();
        int port= redisConfig.getPort();
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            executor.submit(()->{
                initiateSlaveConnection();
                System.out.println("Replication Completed");
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
                executor.shutdown();
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
            // HANDSHAKE - part - 1: send the slave's port to the master
            byte[] inputBuffer = new byte[1024];
            byte [] data="*1\r\n$4\r\nPING\r\n".getBytes();
            outputStream.write(data);
            int bytesRead = inputStream.read(inputBuffer,0,inputBuffer.length);
            String  resp=new String(inputBuffer,0,bytesRead, StandardCharsets.UTF_8);
            log("Received response from master: " + resp);

            // HANDSHAKE - part - 2: send the slave's port to the master
            String replconf= "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$"
                    +(String.valueOf(redisConfig.getPort()).length()+"")+"\r\n"
                    +(redisConfig.getPort()+"")+"\r\n";

            data = replconf.getBytes();
            outputStream.write(data);
            bytesRead = inputStream.read(inputBuffer,0,inputBuffer.length);
            resp=new String(inputBuffer,0,bytesRead, StandardCharsets.UTF_8);
            log("Received response from master: " + resp);

            // HANDSHAKE - part - 2(b) : send the slave's protocol info to the master

            replconf= "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$"
                    +String.valueOf("psync2".length())+"\r\n"
                    +"psync2"+"\r\n";
            data = replconf.getBytes();
            outputStream.write(data);
            bytesRead = inputStream.read(inputBuffer,0,inputBuffer.length);
            resp=new String(inputBuffer,0,bytesRead, StandardCharsets.UTF_8);
            log("Received response from master: " + resp);

            // HANDSHAKE - part - 3: sending PSYNC command to the master
            String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$" + String.valueOf("?".length()) + "\r\n?\r\n$" + String.valueOf("-1".length()) + "\r\n-1\r\n";
            data = psyncCommand.getBytes();
            outputStream.write(data);

            List<Integer> res=handlePsyncResponse( inputStream);
            while(master.isConnected()){
                int offset=1;
                StringBuilder commandBuilder = new StringBuilder();
                List<Byte> commandBytes = new ArrayList<>();
                while (true) {
                    int byteRead = inputStream.read();
                    if (byteRead == '*') {
                        break; // End of stream
                    }
                    offset++;
                    commandBytes.add((byte) byteRead);
                    if(inputStream.available()<=0){
                        break;
                    }
                }

                for(Byte b:commandBytes){
                    commandBuilder.append((char)(b.byteValue() & 0xFF));
                }
                if(commandBytes.isEmpty())
                    continue;
                String commandString = commandBuilder.toString();
                String []parts=commandString.split("\r\n");
                if(commandString.equals("+OK\r\n")){
                    log.info("Received +OK from master");
                    continue;
                }
                String []commandArray=respSerializer.parseArray(parts);
                Client masterClient= new Client(master,master.getInputStream(),master.getOutputStream(),-1);
                String commandResult = handleCommandFromMaster(commandArray,masterClient);

                if(commandArray.length>=2 && commandArray[0].equalsIgnoreCase("replconf") && commandArray[1].equalsIgnoreCase("getack")){
                    if(!commandResult.equals("") && commandResult!=null)
                       outputStream.write(commandResult.getBytes());
                    offset++;
                    List<Byte> leftOverBytes=new ArrayList<>();
                    while (true){
                        if(inputStream.available()<=0){
                            break;
                        }
                        byte b=(byte)inputStream.read();
                        leftOverBytes.add(b);
                        if((int)b==(int)'*')
                            break;
                        offset++;
                    }
                    StringBuilder leftOverStr=new StringBuilder();
                    for(Byte b:leftOverBytes){
                        leftOverStr.append((char)(b.byteValue() & 0xFF));
                    }
                }
                redisConfig.setMasterReplOffset((long)offset+redisConfig.getMasterReplOffset());
            }


        } catch (IOException e) {
            log.error("Error establishing connection to master: " + e.getMessage());
        }
    }

    private String handleCommandFromMaster(String[] commandArray, Client master) {
        String cmd=commandArray[0].toLowerCase();
        String res="";
        switch (cmd) {
            case "set":
                commandHandler.set(commandArray);
                String commandRespString= respSerializer.respArray(commandArray);
                byte [] toCount= commandRespString.getBytes();
                connectionPool.bytesSentToSlaves+=toCount.length;
                CompletableFuture.runAsync(() -> propagateToSlaves(commandArray));
                break;

            case "replconf":
                res=commandHandler.replconf(commandArray,master);
        }
        return res;
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

    private List<Integer> handlePsyncResponse(InputStream inputStream) throws IOException {
        List<Integer> res= new ArrayList<>();
        while (true){
            if(inputStream.available() <=0)
                continue;
            int b=inputStream.read();
            res.add(b);
            if(b==(int)'*'){
                break;
            }
        }

        return res;
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
        connectionPool.removeSlave(client);
        connectionPool.removeClient(client);
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
                res="-READONLY You can't write against a read only slave.\r\n";
                break;
            case "get":
                res=commandHandler.get(command);
                break;
            case "info":
                res=commandHandler.info(command);
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
            case "subscribe":
                res=commandHandler.subscribe(command,client);
                break;
            case "unsubscribe":
                res=commandHandler.unsubscribe(command,client);
                break;

        }
      client.send(new ResponseDto(res,data));
    }
}
