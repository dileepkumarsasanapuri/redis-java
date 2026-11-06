package Components.Infra;

import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class ConnectionPool {
    private  Set<Client> clients;
    private Set<Slave> slaves;
    public int slavesThatareUp=0;
    public int bytesSentToSlaves=0;


    public ConnectionPool(Set<Client> clients, Set<Slave> slaves) {
        this.clients = clients;
        this.slaves = slaves;
    }

    public void slaveAck(int ackResponse){
        if(this.bytesSentToSlaves==ackResponse){
            slavesThatareUp++;
        }
    }

    public Set<Client> getClients() {
        return clients;
    }

    public Set<Slave> getSlaves() {
        return slaves;
    }

    public void addClient(Client client) {
        clients.add(client);
    }

    public boolean removeClient(Client client) {
        return clients.remove(client);
    }

    public void addSlave(Slave slave) {
        slaves.add(slave);
    }

    public boolean removeSlave(Slave slave) {
        return slaves.remove(slave);
    }

    public boolean removeSlave(Client client){
        Slave toRemove = null;
        for (Slave s : slaves) {
            if(s.connection.equals(client)){
                toRemove  = s;
                break;
            }
        }
        return slaves.remove(toRemove);
    }
}
