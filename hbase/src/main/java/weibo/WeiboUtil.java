package weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class WeiboUtil {
    private static Configuration conf = null;
    private static Connection connection = null;
    private static Admin admin = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.100");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void createNameSpace(String ns) throws Exception {
        NamespaceDescriptor nsd = NamespaceDescriptor.create(ns).build();
        admin.createNamespace(nsd);
    }

    public static void createTable(String tableName,int versions,String... cf) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String s : cf) {
            HColumnDescriptor chd = new HColumnDescriptor(s);
            chd.setMaxVersions(versions);
            hTableDescriptor.addFamily(chd);
        }
        admin.createTable(hTableDescriptor);
    }

    public static void main(String[] args) throws Exception {
        createNameSpace(Constant.WEIBO);
        //用户关系表
        createTable(Constant.WEIBO_RELATIONS,1,"attends","fans");
        createTable(Constant.WEIBO_CONTENT,1,"info");
        createTable(Constant.WEIBO_RECEIVE_CONTENT_EMAIL,3,"info");
    }

}
