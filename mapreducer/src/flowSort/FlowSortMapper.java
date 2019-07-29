package flowSort;

import flow.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable,Text,FlowSortBean,Text>{
    FlowSortBean fb = new FlowSortBean();
    Text t = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        fb.setUpFlow(Long.parseLong(words[1]));
        fb.setDownFlow(Long.parseLong(words[2]));
        fb.setSumFlow(Long.parseLong(words[1]),Long.parseLong(words[2]));
        t.set(words[0]);
        context.write(fb,t);
    }
}
