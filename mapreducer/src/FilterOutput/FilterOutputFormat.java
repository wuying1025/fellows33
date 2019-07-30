package FilterOutput;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterOutputFormat extends FileOutputFormat{
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new FileterRecordWriter(job);
    }
}
