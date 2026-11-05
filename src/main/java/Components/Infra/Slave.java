package Components.Infra;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Slave {
    public Client connection;
    public List<String> capabilities;
    public Slave(Client client){
        this.connection=client;
        this.capabilities=new ArrayList<>();
    }

    public void send(byte [] cmd) throws IOException {
        if(cmd!=null){
            this.connection.outputStream.write(cmd);
        }
    }
}
