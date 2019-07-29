package flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text,FlowBean>{
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phone = text.toString();
        String num = phone.substring(0,3);
        int result = 4;
        if(num.equals("135")){
            result = 0;
        }else if(num.equals("136")){
            result = 1;
        }else if(num.equals("137")){
            result = 2;
        }else if(num.equals("138")){
            result = 3;
        }
        return result;
    }
}
