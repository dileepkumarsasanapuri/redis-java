import Components.TcpServer;
import Config.AppConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {
  public static void main(String[] args){
      AnnotationConfigApplicationContext context=new AnnotationConfigApplicationContext(AppConfig.class);
      TcpServer app=context.getBean(TcpServer.class);
      app.startServer();
  }
}
