package Components.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


@Component
public class RespSerializer {
    private static final Logger log = LoggerFactory.getLogger(RespSerializer.class);

    public String serializeBulkString(String command){
        if(command == null){
            return "$-1\r\n";
        }
        if(command.isEmpty()){
            return "$0\r\n\r\n";
        }
        byte[] bytes = command.getBytes(StandardCharsets.UTF_8);
        return "$" + bytes.length + "\r\n" + command + "\r\n";
    }

    public int getParts(char []dataArr, int i, String []subArray){
        int j=0;
        while(i<dataArr.length && j<subArray.length){
            if(dataArr[i]=='$'){
                // bulk string
                i++;
                String partLen="";
                while(i<dataArr.length && Character.isDigit(dataArr[i])){
                    partLen+=dataArr[i];
                    i++;
                }
                i+=2;
                String part="";
                for(int k=0;k<Integer.parseInt(partLen);k++){
                    part+=dataArr[i++];
                }
                i+=2;
                subArray[j++]=part;
            }
        }
        return i;
    }

    public List<String[]> deseriaize(byte[] command){
        List<String[]> res= null;
        try {
            String data= new String(command, StandardCharsets.UTF_8);
            char [] dataArr= data.toCharArray();
            res = new ArrayList<>();
            int i=0;
            while(i<dataArr.length){
                char cur=dataArr[i];
                if(cur=='\u0000')
                    break;
                if(cur=='*'){
                    String arrLen="";
                    i++;
                    while(i<dataArr.length && Character.isDigit(dataArr[i])){
                        arrLen+=dataArr[i++];
                    }
                    i+=2;
                    if(dataArr[i]=='*'){
                        for(int t=0;t<Integer.parseInt(arrLen);t++){
                            String nested="";
                            i++;
                            while(i< dataArr.length && Character.isDigit(dataArr[i])){
                                nested+=dataArr[i++];
                            }
                            i+=2;
                            String []subArray= new String[Integer.parseInt(nested)];
                            i=getParts(dataArr,i,subArray);
                            res.add(subArray);
                        }
                    }else{
                        String []subArray= new String[Integer.parseInt(arrLen)];
                        i=getParts(dataArr,i,subArray);
                        res.add(subArray);
                    }
                }
            }
            return res;
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return  new ArrayList<>();
    }

    public String respArray(String []command) {
        if (command == null) {
            return "*-1\r\n";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(command.length).append("\r\n");
        for (String s : command) {
            if (s == null) {
                sb.append("$-1\r\n");
            } else {
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                sb.append("$").append(bytes.length).append("\r\n").append(s).append("\r\n");
            }
        }
        return sb.toString();
    }

    public String respArray(List<String> command) {
        List<String> res= new ArrayList<>();
        int  len=command.size();
        res.add("*"+len+"\r\n");
        res.addAll(command);
        return String.join("",res);
    }

    public String[] parseArray(String[] parts) {
        String len=parts[0];
        int length=Integer.parseInt(len);
        String command[]= new String[length];
        command[0]=parts[2];
        int idx=1;
        for(int i=4;i<parts.length;i+=2){
            command[idx++]=parts[i];
        }
        return command;
    }


    public String respInteger(int i){
        StringBuilder sb=new StringBuilder();
        sb.append(":");
        sb.append(i);
        sb.append("\r\n");
        return sb.toString();
    }
}
