import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class IOHdfs {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {

        Configuration config = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop100:8020"), config, "root");
        //输入流
        FileInputStream fi = new FileInputStream("d:/xiyou.txt");

        FSDataOutputStream fo = fileSystem.create(new Path("/user/weichuang/input/xiyou3.txt"));

        IOUtils.copyBytes(fi,fo,config);
        //5关闭
        IOUtils.closeStream(fi);
        IOUtils.closeStream(fo);
    }

    @Test
    public void seek1() throws URISyntaxException, IOException, InterruptedException {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:8020"), configuration, "weichuang");
        // 2 获取输入流路径
        Path path = new Path("/user/weichuang/input/hadoop-2.7.2.tar.gz");
        // 3 打开输入流
        FSDataInputStream fis = fs.open(path);
        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream("d:/hadoop-2.7.2.tar.gz.part1");
        // 5 流对接
        byte[] buf = new byte[1024];
        // 1024 * 1024 * 128
        for (int i = 0; i < 128 * 1024; i++) {
            fis.read(buf);
            fos.write(buf);
        }
        // 6 关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
    @Test
    public void seek2() throws URISyntaxException, IOException, InterruptedException {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:8020"), configuration, "weichuang");
        // 2 获取输入流路径
        Path path = new Path("/user/weichuang/input/hadoop-2.7.2.tar.gz");
        // 3 打开输入流
        FSDataInputStream fis = fs.open(path);
        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream("d:/hadoop-2.7.2.tar.gz.part2");
        // 5 定位偏移量（第二块的首位）
        fis.seek(1024 * 1024 * 128);
        // 6 流对接
        IOUtils.copyBytes(fis, fos, configuration );
        // 6 关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    @Test
    public void putFileToHDFS() throws Exception{
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:8020"),configuration, "root");
        // 2 创建输出流
        FSDataOutputStream fos = fs.create(new Path("/user/weichuang/input/hello6.txt"));
        // 3 写数据
        fos.write("hello".getBytes());
        // 4 一致性刷新
        fos.hflush();
        fos.close();
    }

}
