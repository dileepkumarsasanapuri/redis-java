import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
                  try {
                      handleClients(finalClientSocket);
                  } catch (IOException e) {
                      throw new RuntimeException(e);
                  }
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
  public static void handleClients(Socket clientSocket) throws IOException {
      InputStream inputStream=clientSocket.getInputStream();
      Scanner sc=new Scanner(inputStream);
      while(sc.hasNextLine()){
          String nextLine=sc.nextLine();
          if(nextLine.contains("PING")) {
              clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
          }
      }
  }
}
