package etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    private Text t = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String s = ETLUtil.oriString2ETLString(line);
        if(s == null){
            return ;
        }
        t.set(s);
        context.write(t,NullWritable.get());
    }
}
