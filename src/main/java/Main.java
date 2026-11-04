import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
      ExecutorService executor= Executors.newFixedThreadPool(10);
        try {
          serverSocket = new ServerSocket(port);
          serverSocket.setReuseAddress(true);

          while(true) {
              clientSocket = serverSocket.accept();
              Socket finalClientSocket = clientSocket;
              executor.submit(() -> handleClients(finalClientSocket));
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
      try (Socket socket = clientSocket;
           Scanner sc = new Scanner(socket.getInputStream());
           OutputStream output = socket.getOutputStream()
      ) {
          while (sc.hasNextLine()) {
              String command = sc.nextLine();
              System.out.println("Received: " + command);
              if (command.contains("PING")) {
                  output.write("+PONG\r\n".getBytes());
                  output.flush();
              }
              if(command.contains("ECHO")){
                  String respHeader=sc.nextLine();
                  String respBody=sc.nextLine();
                  String respp= respHeader+"\r\n"+respBody+"\r\n";
                  output.write(respp.getBytes());
              }
          }
    } catch (IOException e) {
        System.out.println("client disconnected: "+e.getMessage());
    }

  }
}
