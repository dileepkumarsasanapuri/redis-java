package Components.Infra;

import Components.Service.RespSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class ClientChannelPool {

    @Autowired
    private RespSerializer respSerializer;
    ConcurrentHashMap<String,Channel> channelByChannelId;
    ConcurrentHashMap<Integer, Set<String>> channelIdByClientIds;

    private final ReentrantReadWriteLock rwLock=new ReentrantReadWriteLock();
    public ClientChannelPool(){
        this.channelByChannelId=new ConcurrentHashMap<>();
        this.channelIdByClientIds=new ConcurrentHashMap<>();
    }

    public int subscribe(Client client,String channelId){
        rwLock.writeLock().lock();
        try{
            if(!channelByChannelId.containsKey(channelId)){
                Channel newChannel=new Channel(channelId);
                channelByChannelId.put(channelId,newChannel);
            }
            if(!channelIdByClientIds.containsKey(client.id)){
                channelIdByClientIds.put(client.id,new HashSet<>());
            }

            Channel channel= channelByChannelId.get(channelId);
            channelIdByClientIds.get(client.id).add(channelId);
            channel.clients.add(client);

            return channelIdByClientIds.get(client.id).size();

        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public int unsubscribe(Client client,String channelId){
        rwLock.writeLock().lock();
        try{
            if(!channelByChannelId.containsKey(channelId)){
              return -1;
            }
            Channel channel= channelByChannelId.get(channelId);
            channelIdByClientIds.get(client.id).remove(channelId);
            channel.clients.remove(client);

            return channelIdByClientIds.get(client.id).size();

        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public boolean isClientSubscirbed(Client client){
        rwLock.readLock().lock();
        try {
            Set<String> channelIds= channelIdByClientIds.getOrDefault(client.id,new HashSet<>());
            return channelIds.size()>0;
        }finally {
            rwLock.readLock().unlock();
        }
    }
    public int publish(String channelId, String msg) {
        Set<Client> clientsSnapshot;
        rwLock.readLock().lock();
        try {
            Channel channel= channelByChannelId.get(channelId);
            if(channel==null){
                return 0;
            }
            clientsSnapshot= new HashSet<>(channel.clients);
        }finally {
            rwLock.readLock().unlock();
        }
        int res=0;
        String [] msgToSend= new String[3];
        msgToSend[0]="message";
        msgToSend[1]=channelId;
        msgToSend[2]=msg;
        String msgToSendResp= respSerializer.respArray(msgToSend);
        try {
            System.out.println("SENDING: "+msgToSendResp.replace("\r","\\r")+msgToSendResp.replace("\n","\\n"));
            for(Client client:clientsSnapshot){
                client.send(msgToSendResp);
                res++;
            }

        } catch (Exception e) {
            System.err.println("Sent to "+res+" but needed to send to "+clientsSnapshot.size());
            return res;
        }
        return clientsSnapshot.size();
    }
}
