package Components;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Component
public class RespSerializer {

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
                i++;
                subArray[j++]=part;
            }
        }
        return i;
    }

    public List<String[]> deseriaize(byte[] command){
        String data= new String(command, StandardCharsets.UTF_8);
        char [] dataArr= data.toCharArray();
        List<String[]> res=new ArrayList<>();
        int i=0;
        while(i<dataArr.length){
            char cur=dataArr[i];
            if(cur=='\u0000')
                break;
            if(cur=='*'){
                String arrLen="";
                i++;
                while(i<dataArr.length && Character.isDigit(dataArr[i])){
                    arrLen+=dataArr[i];
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
        return  res;
    }
}
