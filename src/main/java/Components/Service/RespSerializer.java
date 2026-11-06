package Components.Service;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


@Component
public class RespSerializer {
    private static final Logger log =
            LoggerFactory.getLogger(RespSerializer.class);
    public String serialize(String command){
        StringBuilder sb=new StringBuilder();
        sb.append("$"+command.length()+"\r\n");
        sb.append(command+"\r\n");
        return sb.toString();
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
        List<String> res= new ArrayList<>();
        int  len=command.length;
        res.add("*"+len);
        for(String s:command){
            len=s.length();
            res.add("$"+len);
            res.add(s);
        }
        return String.join("\r\n",res)+"\r\n";
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
