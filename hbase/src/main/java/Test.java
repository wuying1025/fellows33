import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Test {
    static private Admin admin = null;
    static private Connection connection = null;
    static private Configuration conf = null;
    static {
        conf =  HBaseConfiguration.create();
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
        if(isExist(tableName)){
            return;
        }
        TableName table =TableName.valueOf(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        for (String s : fc) {
            hTableDescriptor.addFamily(new HColumnDescriptor(s));
        }
        admin.createTable(hTableDescriptor);
    }
    public static void deleteTable(String tableName) throws Exception {
        if(!isExist(tableName)){
           return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    public static void insertData(String tableName,String rowkey,String cf,String name,String value) throws IOException {
        HTable table = new HTable(conf,tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(name),Bytes.toBytes(value));
        table.put(put);
    }

    public static void delData(String tableName,String... rowkey) throws IOException {
        HTable table = new HTable(conf,tableName);
        List<Delete> ld = new ArrayList<Delete>();
        for (String s : rowkey) {
            Delete del = new Delete(Bytes.toBytes(s));
            ld.add(del);
        }
        table.delete(ld);
    }
    public static void get(String tableName) throws IOException {
        HTable table = new HTable(conf,tableName);
        Scan sc = new Scan();
        ResultScanner scanner = table.getScanner(sc);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))+",");
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell))+",");
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell))+",");
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
    public static void getRow(String tableName,String rowkey) throws IOException {
        HTable table = new HTable(conf,tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ",");
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + ",");
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)) + ",");
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void getRowcf(String tableName,String rowkey,String cf,String name) throws IOException {
        HTable table = new HTable(conf,tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(name));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ",");
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + ",");
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)) + ",");
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }


    public static void main(String[] args) throws Exception {
//        System.out.println(isExist("student"));
//        System.out.println(isExist("student1"));
//        createTable("student1","info");
//        System.out.println(isExist("student1"));
//        deleteTable("student1");
//        insertData("student","1001","info","name","zs");
//        insertData("student","1002","info","name","lisi");
//        insertData("student","1001","info","age","13");
//        delData("student","1001","1000");
//        get("student");
//        getRow("student","1001");
        getRowcf("student","1001","info","age");
    }
}
