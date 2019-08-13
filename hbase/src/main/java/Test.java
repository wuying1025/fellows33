import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class Test {
    static private Admin admin = null;
    static private Connection connection = null;
    static {
        Configuration conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.100");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static boolean isExist(String tableName) throws Exception {
        Configuration conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.100");
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        TableName table =TableName.valueOf(tableName);
        boolean b = admin.tableExists(table);
        return b;
    }

    public static void createTable(String tableName,String... fc) throws Exception {
        TableName table =TableName.valueOf(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        for (String s : fc) {
            hTableDescriptor.addFamily(new HColumnDescriptor(s));
        }
        admin.createTable(hTableDescriptor);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(isExist("student"));
        System.out.println(isExist("student1"));
        createTable("student1","info");
        System.out.println(isExist("student1"));
    }
}
