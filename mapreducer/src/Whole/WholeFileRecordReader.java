package Whole;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private BytesWritable value = new BytesWritable();
    private FileSplit split = null;
    private Configuration configuration = null;
    private boolean processed = false;//代表没结束
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        split = (FileSplit)inputSplit;
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            // 1 定义缓存
            byte[] contents = new byte[(int) split.getLength()];
            // 2 获取文件系统
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(configuration);
            // 3 读取内容
            FSDataInputStream fis = null;
            try {
                // 3.1 打开输入流
                fis = fs.open(path);
                // 3.2 读取文件内容
                IOUtils.readFully(fis, contents, 0, contents.length);
                // 3.3 输出文件内容
                value.set(contents, 0, contents.length);
            } catch (Exception e) {

            } finally {
                IOUtils.closeStream(fis);
            }
            processed = true;
            return true;
        }
        return false;

    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }
}
