package log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 2 解析日志是否合法
        boolean b = pressLog(line);
        if(!b){
            context.getCounter("log", "false").increment(1);
            return;
        }
        context.getCounter("log", "true").increment(1);
        context.write(value,NullWritable.get());
    }

    private boolean pressLog(String line) {
        String[] split = line.split(" ");
        if(split.length <= 11){
            return false;
        }
        return true;
    }

}
