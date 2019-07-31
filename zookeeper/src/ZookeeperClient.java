import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperClient {
    private static String connectString =
            "hadoop100:2181,hadoop101:2181,hadoop102:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zooKeeper = null;
    @Before
    public void init() throws Exception {
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("===============");
            }
        });
    }
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        zooKeeper.create("/xiyou","baigujing".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    @Test
    public void getChildren() throws Exception {
        List<String> children = zooKeeper.getChildren("/xiyou", true);
        for (String c : children) {
            System.out.printf(c);
        }
        Thread.sleep(Integer.MAX_VALUE);
    }

    // 判断znode是否存在
    @Test
    public void exist() throws Exception {
        Stat stat = zooKeeper.exists("/weichuang", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}
