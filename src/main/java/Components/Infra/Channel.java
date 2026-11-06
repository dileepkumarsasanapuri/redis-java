package Components.Infra;

import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

@Component
public class Channel {
    public  String id;
    public Queue<String > events;
    public Set<Client> clients;
    public Channel(String id){
        this.id=id;
        this.clients=new HashSet<>();
        this.events=new LinkedList<>();
    }
}
