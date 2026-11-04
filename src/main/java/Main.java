import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class Main {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          serverSocket.setReuseAddress(true);

          while(true) {
              clientSocket = serverSocket.accept();
              Socket finalClientSocket = clientSocket;
              CompletableFuture.runAsync(() -> {
                  handleClients(finalClientSocket);
              });
          }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (clientSocket != null) {
              clientSocket.close();
            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
  public static void handleClients(Socket clientSocket) {
    String resp= "+PONG";
    try(Socket socket=clientSocket;
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out=new PrintWriter(socket.getOutputStream(),true);
    ){
        String msg;
        while ((msg = in.readLine())!=null){
            if( msg.startsWith("*")){
                in.readLine();
                in.readLine();
                out.print(resp+"/r/n");
                out.flush();
            }else{
                break;
            }
        }
    } catch (IOException e) {
        System.out.println("client disconnected: "+e.getMessage());
    }

  }
}
