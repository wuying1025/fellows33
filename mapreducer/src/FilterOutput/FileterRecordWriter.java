package FilterOutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FileterRecordWriter extends RecordWriter<Text,NullWritable> {
    private FSDataOutputStream weichuangOut=null;
    private FSDataOutputStream otherOut=null;
    public FileterRecordWriter(TaskAttemptContext job) throws IOException {
        // 1 获取文件系统
        FileSystem fs;
        try {
            fs = FileSystem.get(job.getConfiguration());
            // 2 创建输出文件路径
            Path weichuangPath = new Path("d://output/weichuang.log");
            Path otherPath = new Path("d://output/other.log");

            // 3 创建输出流
            weichuangOut = fs.create(weichuangPath);
            otherOut = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String s = text.toString();
        if(s.contains("weichuang")){
            weichuangOut.write(s.getBytes());
        }else{
            otherOut.write(s.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        weichuangOut.close();
        otherOut.close();
    }
}
