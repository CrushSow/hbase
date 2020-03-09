package aibeifeng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import java.util.List;

public class TestHbase {
    static Connection connection=null;
    static Admin admin=null;
    static {
        //获取hbase的配置信息
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop");

        //获取admin
        try {
           connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public  static  void close(Connection connection,Admin admin) throws IOException {
        if (connection!=null){
            connection.close();
        }
        if (admin!=null){
            admin.close();
        }
    }
    //判断表是否存在  old API
    public static boolean tableExist(String tablename) throws IOException {
        //获取hbase的配置信息
        HBaseConfiguration configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum","hadoop");
        //获取admin
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        return hBaseAdmin.tableExists(tablename);
    }
    /**
     * new API
    */
    public static  boolean tableExist1(String tablename) throws IOException {
        //获取hbase的配置信息

        return admin.tableExists(TableName.valueOf(tablename));

    }

    /**
     * 创建表格
     * @param tablename
     * @param
     */
    private  static void createTable(String tablename, List<String> columnFamilys) throws IOException {
        if (tableExist1(tablename)){
            System.out.println("表格" + tablename + "已经存在了");
            return;
        }
        //创建表的描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));
        for (String cf : columnFamilys) {
            //创建列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        try {
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void deleteTable(String tableName) throws IOException {
        if (tableExist1(tableName)){
            System.out.println("表格" + tableName + "不存在");
            return;
        }
        //让表格不可用
        admin.disableTable(TableName.valueOf(tableName));
        //删除表格
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 改写和插入数据是一样的操作
     * @param tableName
     * @param rowkey
     * @param cf
     * @param cn
     * @param version
     * @throws IOException
     */
    private static void putData(String tableName,String rowkey,String cf,
                                String cn,String version) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建put  朝put 里面出添加东西
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(version));
        //执行put操作
        table.put(put);
        table.close();
    }
    //获取一条数据
    private  static void getData(String tableName,String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        //打印出来获取的数据
        for (Cell cell : cells) {
            System.out.println("rowkey"+ Bytes.toString(CellUtil.cloneRow(cell))
            +"cf"+ Bytes.toString(CellUtil.cloneFamily(cell))
            +"cn"+ Bytes.toString(CellUtil.cloneQualifier(cell))
            +"value"+ Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //关闭
        table.close();

    }
    //获取一行数据（指定列簇：列）
    private static void getDataByCN(String tablename,String rowkey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        //打印出来获取的数据
        for (Cell cell : cells) {
            System.out.println("rowkey"+ Bytes.toString(CellUtil.cloneRow(cell))
                    +"cf"+ Bytes.toString(CellUtil.cloneFamily(cell))
                    +"cn"+ Bytes.toString(CellUtil.cloneQualifier(cell))
                    +"value"+ Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //关闭
        table.close();
    }
    //全表扫描
    private static void scanData(String tablename) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("rowkey"+ Bytes.toString(CellUtil.cloneRow(cell))
                        +"cf"+ Bytes.toString(CellUtil.cloneFamily(cell))
                        +"cn"+ Bytes.toString(CellUtil.cloneQualifier(cell))
                        +"value"+ Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }
    //删除一条数据
    private static void deleteData(String tableName,String rowkey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowkey));
        //给delete对象添加具体的列簇：列
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        table.delete(delete);

        table.close();
    }
    //删除多条数据

    public static void main(String[] args) throws IOException {
       // System.out.println(tableExist("student"));
        //System.out.println(tableExist1("gazige"));
        //createTable("staff",Collections.singletonList("f1"));

        //插入一条数据
       // putData("staff","1001","f1","name","gazige");
        //获取一条数据
        //getData("staff","1001");
        getDataByCN("student","1001","f1","name");
        close(connection,admin);
    }
}
