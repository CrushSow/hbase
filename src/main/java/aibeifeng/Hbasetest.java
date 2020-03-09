package aibeifeng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

/**
 * PACKAGE_NAMW   aibeifeng
 * DATE      10
 * Author     Crush
 */
public class Hbasetest {

    private static ThreadLocal<Connection> connectionThreadLocal=new ThreadLocal<Connection>();


    static Admin admin = null;


    static {
        //获取hbase的配置文件
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection=connectionThreadLocal.get();
        try {
            connection = ConnectionFactory.createConnection(configuration);
            connectionThreadLocal.set(connection);
            admin = connection.getAdmin();
            //HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(Admin admin, Connection connection,ThreadLocal threadLocal) throws IOException {
        if (connection != null) {
            connection.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (threadLocal!=null){
            threadLocal.remove();
        }
    }


    //判断表是不是存在
    public static boolean isTableexist(String tableanem) throws IOException {
        return admin.tableExists(TableName.valueOf(tableanem));
    }

    //创建表格
    public static void createTable(String tablename, String... columnFamily) throws IOException {
        // 判断表格是不是存在
        if (isTableexist(tablename)) {
            System.out.println(tablename + "is exist");
            System.exit(0);
        } else {
            //创建表的属性对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));
            //协处理器
            hTableDescriptor.addCoprocessor("aibeifeng.InserCrushCoprocessor");
            // 创建多个数组
            for (String column : columnFamily) {
                hTableDescriptor.addFamily(new HColumnDescriptor(column));
            }

            admin.createTable(hTableDescriptor);
            System.out.println(" CREATE TABLE SUCCESS");
        }

    }


    //删除表格
    public static void deleteTable(String tablename) throws IOException {
        if (isTableexist(tablename) == false) {
            System.out.println(" TABLE IS NOT EXIST");
        } else {
            admin.disableTable(TableName.valueOf(tablename));

            admin.deleteTable(TableName.valueOf(tablename));
        }
    }

    //改写数据和插入数据是一个样子的
    public static void addRowData(String tablename, String rowkey, String coloumnFamily, String column, String value) throws IOException {
        //创建Htable对象 然后对table进行操作
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = connectionThreadLocal.get();
        Table table = connection.getTable(TableName.valueOf(tablename));
        // 旧的spi
        //HTable hTable = new HTable(configuration, TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowkey));
        Put add = put.add(Bytes.toBytes(coloumnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

        table.put(put);
        table.close();
        System.out.println("INSERT  DATA OR UPDATE  SUCCESS");

    }

    //删除多行数据
    public static void deletemuchRow(String tablename,String...rows) throws IOException {
        Connection connection = connectionThreadLocal.get();
        Table table = connection.getTable(TableName.valueOf(tablename));
        List<Delete> deletes = new ArrayList<Delete>();

        for (String row : rows) {
            deletes.add(new Delete(Bytes.toBytes(row)));
        }


        table.delete(deletes);
        System.out.println("DELETE MUCH ROWS SUCCESS");
    }


    // 获取所有行的数据
    public static void getAllrows(String tablename) throws IOException {
        Connection connection = connectionThreadLocal.get();
        Table table = connection.getTable(TableName.valueOf(tablename));
        //获取scan对象
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {

                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));

            }
        }


    }

    // 获取某一行的数据
    public static  void getrowDate(String tablename,String row) throws IOException {
        Connection connection = connectionThreadLocal.get();
        Table table = connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(row));

        //get.getMaxVersions();
        //get.setTimeStamp()
        Result result = table.get(get);
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            System.out.println(result.getRow()); //行键
            System.out.println(CellUtil.cloneFamily(cell));
            System.out.println(CellUtil.cloneQualifier(cell));
            System.out.println(CellUtil.cloneValue(cell));
        }


    }

    public static void getRowFamilyQualifier(String tablename,String row,String family,String qualifer) throws IOException {
        Connection connection = connectionThreadLocal.get();
        Table table = connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(row));

        get.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifer));
        Result result = table.get(get);

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(CellUtil.cloneRow(cell));
            System.out.println(CellUtil.cloneFamily(cell));
            System.out.println(CellUtil.cloneQualifier(cell));
            System.out.println(CellUtil.cloneValue(cell));
        }

    }


    public static void main(String[] args) {

    }
}
