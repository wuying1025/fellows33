package weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    public static void putContent(String uid,String content) throws Exception {
        Table conTable = connection.getTable(TableName.valueOf(Constant.WEIBO_CONTENT));
        Table relTable = connection.getTable(TableName.valueOf(Constant.WEIBO_RELATIONS));
        Table recTable = connection.getTable(TableName.valueOf(Constant.WEIBO_RECEIVE_CONTENT_EMAIL));
        //1.插入微博
        long l = System.currentTimeMillis();
        String rowKey = uid + "_" + l;
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("content"),Bytes.toBytes(content));
        conTable.put(put);
        //2.找用户的关注人
        Get g = new Get(Bytes.toBytes(uid));
        g.addFamily(Bytes.toBytes("fans"));
        Result result = relTable.get(g);
        Cell[] cells = result.rawCells();
        List<Put> lp = new ArrayList<Put>();
        for (Cell cell : cells) {
            byte[] qualifier = CellUtil.cloneQualifier(cell);//粉丝 B
            //3.加入微博收件箱表
            Put p = new Put(qualifier);
            p.addColumn(Bytes.toBytes("info"),Bytes.toBytes(uid),Bytes.toBytes(rowKey));
            lp.add(p);
        }
        recTable.put(lp);
    }

    public static void main(String[] args) throws Exception {
        createNameSpace(Constant.WEIBO);
        //用户关系表
        createTable(Constant.WEIBO_RELATIONS,1,"attends","fans");
        createTable(Constant.WEIBO_CONTENT,1,"info");
        createTable(Constant.WEIBO_RECEIVE_CONTENT_EMAIL,3,"info");
    }

}
