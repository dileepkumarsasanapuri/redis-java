import Components.TcpServer;
import Config.AppConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {
  public static void main(String[] args){
      AnnotationConfigApplicationContext context=new AnnotationConfigApplicationContext(AppConfig.class);
      TcpServer app=context.getBean(TcpServer.class);
      int port=6379;
      for(int i=0;i<args.length;i++){
          if(args[i].equals("--port")){
              port=Integer.parseInt(args[++i]);
          }
      }
      app.startServer(port);
  }
}
