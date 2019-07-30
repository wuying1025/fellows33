package table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean>{
    TableBean t = new TableBean();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        FileSplit fs  = (FileSplit) context.getInputSplit();
        String name = fs.getPath().getName();//获取文件名称
        if("order.txt".equals(name)){
            t.setOrderId(words[0]);
            t.setPid(words[1]);
            t.setAmount(Integer.parseInt(words[2]));
            t.setPname("");
            t.setType("0");
        }else{
            t.setOrderId("");
            t.setPid(words[0]);
            t.setAmount(0);
            t.setPname(words[1]);
            t.setType("1");
        }
        v.set(t.getPid());
        context.write(v,t);

    }
}
