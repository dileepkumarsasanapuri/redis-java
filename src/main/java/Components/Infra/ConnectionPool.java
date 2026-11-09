package Components.Infra;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ConnectionPool {
    private final Set<Client> clients;
    private final Set<Slave> slaves;
    private final AtomicInteger slavesThatAreUp = new AtomicInteger(0);
    private final AtomicInteger bytesSentToSlaves = new AtomicInteger(0);

    public ConnectionPool() {
        this.clients = ConcurrentHashMap.newKeySet();
        this.slaves = ConcurrentHashMap.newKeySet();
    }

    public ConnectionPool(Set<Client> clients, Set<Slave> slaves) {
        this.clients = ConcurrentHashMap.newKeySet();
        if (clients != null) this.clients.addAll(clients);
        this.slaves = ConcurrentHashMap.newKeySet();
        if (slaves != null) this.slaves.addAll(slaves);
    }

    public void slaveAck(int ackResponse) {
        if (this.bytesSentToSlaves.get() == ackResponse) {
            slavesThatAreUp.incrementAndGet();
        }
    }

    public int getSlavesThatAreUp() {
        return slavesThatAreUp.get();
    }

    public void setSlavesThatAreUp(int val) {
        slavesThatAreUp.set(val);
    }

    public int getBytesSentToSlaves() {
        return bytesSentToSlaves.get();
    }

    public void addBytesSentToSlaves(int bytes) {
        bytesSentToSlaves.addAndGet(bytes);
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

    public boolean removeSlave(Client client) {
        return slaves.removeIf(s -> s.connection.equals(client));
    }
}
