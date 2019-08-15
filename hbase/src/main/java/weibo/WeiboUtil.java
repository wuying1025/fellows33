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
        if(cells.length <= 0){
            return;
        }
        for (Cell cell : cells) {
            byte[] qualifier = CellUtil.cloneQualifier(cell);//粉丝 B
            //3.加入微博收件箱表
            Put p = new Put(qualifier);
            p.addColumn(Bytes.toBytes("info"),Bytes.toBytes(uid),Bytes.toBytes(rowKey));
            lp.add(p);
        }
        recTable.put(lp);
    }

    public static void attendUser(String fans,String attend) throws Exception {
        Table conTable = connection.getTable(TableName.valueOf(Constant.WEIBO_CONTENT));
        Table relTable = connection.getTable(TableName.valueOf(Constant.WEIBO_RELATIONS));
        Table recTable = connection.getTable(TableName.valueOf(Constant.WEIBO_RECEIVE_CONTENT_EMAIL));
        //1.添加关系表
        List<Put> lp = new ArrayList<Put>();
        Put pfans = new Put(Bytes.toBytes(fans));
        pfans.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(attend),Bytes.toBytes(attend));
        lp.add(pfans);
        Put pattend = new Put(Bytes.toBytes(attend));
        pattend.addColumn(Bytes.toBytes("fans"),Bytes.toBytes(fans),Bytes.toBytes(fans));
        lp.add(pattend);
        relTable.put(lp);
        //2.查找发布的微博内容
        Scan sc = new Scan(Bytes.toBytes(attend),Bytes.toBytes(attend+"|"));
        ResultScanner scanner = conTable.getScanner(sc);
        if(!scanner.iterator().hasNext()){
            return;
        }
        List<Put> reclp = new ArrayList<Put>();
        for (Result result : scanner) {
            byte[] row = result.getRow();// 1003_4314314321
            String[] split = Bytes.toString(row).split("_");
            String ts = split[1];
            //3.插入收件箱表
            Put put = new Put(Bytes.toBytes(fans));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(attend),Long.parseLong(ts),row);
            reclp.add(put);
        }
        recTable.put(reclp);
    }


    public static void remove(String uid,String attend) throws Exception {
        Table conTable = connection.getTable(TableName.valueOf(Constant.WEIBO_CONTENT));
        Table relTable = connection.getTable(TableName.valueOf(Constant.WEIBO_RELATIONS));
        Table recTable = connection.getTable(TableName.valueOf(Constant.WEIBO_RECEIVE_CONTENT_EMAIL));
        Delete del = new Delete(uid.getBytes());
        del.addColumn("attends".getBytes(),attend.getBytes());
        relTable.delete(del);

        Delete del2 = new Delete(attend.getBytes());
        del2.addColumn("fans".getBytes(),uid.getBytes());
        relTable.delete(del2);

        Delete del3 = new Delete(uid.getBytes());
        del3.addColumn("info".getBytes(),attend.getBytes());
        recTable.delete(del3);

    }
    public static void getAll(String uid) throws Exception {
        Table recTable = connection.getTable(TableName.valueOf(Constant.WEIBO_RECEIVE_CONTENT_EMAIL));
        Table conTable = connection.getTable(TableName.valueOf(Constant.WEIBO_CONTENT));

        Get g = new Get(uid.getBytes());
        g.setMaxVersions(3);
        Result result = recTable.get(g);
        for (Cell cell : result.rawCells()) {
            byte[] value = CellUtil.cloneValue(cell);
            Get get = new Get(value);
            get.addColumn("info".getBytes(), "content".getBytes());
            Result result1 = conTable.get(get);
            byte[] value1 = result1.getValue("info".getBytes(), "content".getBytes());
            System.out.println("发布人："+Bytes.toString(CellUtil.cloneQualifier(cell))+",微博："
                    +Bytes.toString(value1));
        }
    }
    public static void main(String[] args) throws Exception {
//        createNameSpace(Constant.WEIBO);
//        //用户关系表
//        createTable(Constant.WEIBO_RELATIONS,1,"attends","fans");
//        createTable(Constant.WEIBO_CONTENT,1,"info");
//        createTable(Constant.WEIBO_RECEIVE_CONTENT_EMAIL,3,"info");
//        putContent("1009","123");
//        putContent("1009","456");
//        putContent("1009","789");
//        putContent("1009","abc");
//        putContent("1009","xxx");
//        attendUser("1001","1009");
//        remove("1001","1003");
        getAll("1002");
    }

}
