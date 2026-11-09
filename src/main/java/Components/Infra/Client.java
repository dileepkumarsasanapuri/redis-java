package Components.Infra;

import Components.Service.ResponseDto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;

public class Client {
    public Socket socket;
    public InputStream inputStream;
    public OutputStream outputStream;
    public int id;
    public boolean isSubscribed;

    private boolean transactionalContext;
    public Queue<String []> commandQueue;
    public List<String> transactionResponse; // strings already in rest format
    public Set<String> watchSet;

    public Client(Socket socket, InputStream inputStream, OutputStream outputStream, int id) {
        this.socket = socket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.id = id;
        this.isSubscribed=false;
    }

    public boolean isWatchSetInitiated(){
        return this.watchSet!=null;
    }

    public void addToWatchSet(String key){
        if(!isWatchSetInitiated()){
            this.watchSet=new HashSet<>();
        }
        this.watchSet.add(key);
    }

    public boolean getTransactionalContext(){
        return transactionalContext;
    }

    public boolean beginTransaction(){
        if(transactionalContext){
            return false;
        }
        transactionalContext=true;
        transactionResponse=new ArrayList<>();
        commandQueue=new LinkedList<>();
        return transactionalContext;
    }

    public boolean endTransaction(){
        commandQueue=null;
        transactionalContext=false;
        watchSet=null;
        return transactionalContext;
    }


//    public void send(String res, byte[] data) throws IOException {
//        if(res !=null && !res.equals(""))
//            outputStream.write(res.getBytes());
//
//        if(data!=null){
//            outputStream.write(data);
//        }
//    }

    public synchronized void send(ResponseDto res) throws IOException {
        if(res.response != null && !res.response.isEmpty()) {
            outputStream.write(res.response.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        if(res.data != null) {
            outputStream.write(res.data);
        }
        outputStream.flush();
    }

    public synchronized void send(byte[] data) throws IOException {
        if(data != null) {
            outputStream.write(data);
            outputStream.flush();
        }
    }

    public synchronized void send(String data) throws IOException {
        if(data != null && !data.isEmpty()) {
            outputStream.write(data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            outputStream.flush();
        }
    }
}
