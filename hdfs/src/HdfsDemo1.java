import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsDemo1 {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration config = new Configuration();
//        config.set("fs.defaultFS","hdfs://hadoop100:8020");
        //1.获取文件系统
//        FileSystem fileSystem = FileSystem.get(config);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop100:8020"), config, "root");
        fileSystem.copyFromLocalFile(new Path("D:/xiyou.txt"),new Path("/user/weichuang/input/xiyou4.txt"));
        fileSystem.close();
    }
}
