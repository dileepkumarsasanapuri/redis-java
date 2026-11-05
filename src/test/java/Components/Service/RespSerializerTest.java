package Components.Service;


import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
        assertEquals("ping",commands.get(0)[0]);
    }

    @Test
    void testMultipleCommands() {
        String input ="*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nDileep\r\n" +
                   "*2\r\n$3\r\nGET\r\n$4\r\nname\r\n";
        List<String[]> commands = respSerializer.deseriaize(input.getBytes(StandardCharsets.UTF_8));
        assertEquals(2,commands.size());
        assertArrayEquals(new String[]{"SET", "name", "Dileep"}, commands.get(0));

        assertArrayEquals(new String[]{"GET", "name"}, commands.get(1));
    }
}