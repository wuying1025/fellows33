package Whole;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    private Text filenameKey;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
        Path path = split.getPath();
        filenameKey = new Text(path.toString());//c:www/ww/ww/log.txt
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(filenameKey,value);
    }
}
