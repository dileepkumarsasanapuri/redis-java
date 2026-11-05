package Components;


import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RespSerializerTest {
    private final RespSerializer respSerializer=new RespSerializer();
    @Test
    public void testDeserialization(){
        String ping = "*1\r\n$4\r\nping\r\n";
        List<String []> commands= respSerializer.deseriaize(ping.getBytes(StandardCharsets.UTF_8));
        for(String []s:commands){
            System.out.println("==========");
            for (String ss:s){
                System.out.print(ss+" ");
            }
        }
        assertEquals(1,commands.size());
        assertEquals(1,commands.get(0).length);
        assertEquals("PING",commands.get(0)[0]);
    }

    @Test
    public void testMultipleCommands(){
        String multipleCommands="*2\r\n*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nDileep\r\n";
        List<String []> commands=respSerializer.deseriaize(multipleCommands.getBytes(StandardCharsets.UTF_8));
        for(String [] s:commands){
            System.out.println();
            for(String ss:s){
                System.out.print(ss+" ");
            }
        }
        assertEquals(2,commands.size());
        assertEquals(3,commands.get(0).length);
        assertEquals(3,commands.get(1).length);

        assertEquals("set", commands.get(0)[0]);
        assertEquals("key", commands.get(0)[1]);
        assertEquals("value", commands.get(0)[2]);

        assertEquals("set", commands.get(1)[0]);
        assertEquals("key", commands.get(1)[1]);
        assertEquals("value", commands.get(1)[2]);
    }
}